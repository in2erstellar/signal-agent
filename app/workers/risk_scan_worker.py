"""Consumers for ``risk_scan_queue`` and ``risk_heavy_queue``.

The fast consumer reads mint, freeze, and LP, then acks. Holder
concentration, the funder walk, and creator-cluster distribution run on the
heavy consumer. Both use the scan semaphore passed into this worker. The
Helius client is the one inside the funder and scanner factories.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, cast

import structlog
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import (
    MAX_FUNDER_LOOKUP_RETRIES,
    POST_MIGRATION_RETRY_DELAYS_SECONDS,
    RISK_CHECK_WEIGHTS,
    RISK_HEAVY_GROUP,
    RISK_HEAVY_QUEUE,
    RISK_SCAN_GROUP,
    RISK_SCAN_PRIORITY_GROUP,
    RISK_SCAN_PRIORITY_QUEUE,
    RISK_SCAN_QUEUE,
)
from app.db.models import FunderCluster, Token, TrackedWallet, WalletTx
from app.observability import bind_trace, new_trace_id, record_risk_scan_latency
from app.services.creator_cluster import measure_distribution
from app.services.funder_graph import FunderLookupFailure
from app.services.lp_unlock import LpCustody, classify_lp_custody
from app.services.outcome_jobs import record_listing, record_lp_unlock
from app.services.risk_scanner import RiskReport, RiskScanner
from app.services.token_store import (
    FUNDER_GRAPH_BUILT,
    FUNDER_GRAPH_COMPLETED_POST,
    FUNDER_GRAPH_FAILED,
    FUNDER_GRAPH_PENDING_POST,
    FUNDER_GRAPH_SKIPPED,
    HEAVY_SCAN_COMPLETE,
    HEAVY_SCAN_FAILED,
    HEAVY_SCAN_PENDING,
    HEAVY_SCAN_SKIPPED,
)
from app.workers.signal_fusion import composite_score, composite_without_wallet, emits_signal

logger = structlog.get_logger(__name__)

# Best wallet score fusion can compute: every tracked buyer at a 100% win rate.
_BEST_WALLET_SCORE = Decimal(100)

_RETRY_KEY_TTL_SECONDS = 3600
_PROTECTED_FUNDER_STATUSES = frozenset(
    {
        FUNDER_GRAPH_SKIPPED,
        FUNDER_GRAPH_COMPLETED_POST,
        FUNDER_GRAPH_BUILT,
        FUNDER_GRAPH_FAILED,
    }
)

class _RetryFastScan(Exception):
    """Leave the message pending and rescan after ``delay`` seconds."""

    def __init__(self, attempt: int, delay: float, reason: str) -> None:
        super().__init__(reason)
        self.attempt = attempt
        self.delay = delay
        self.reason = reason


FunderFactory = Callable[[AsyncSession], Any]
ScannerFactory = Callable[[AsyncSession, list[FunderCluster]], RiskScanner]

FAST_CHECKS = (
    "check_mint_authority",
    "check_freeze_authority",
    "check_lp_lock",
)
HEAVY_CHECKS = (
    "check_holder_concentration",
    "check_funder_cluster_exposure",
)


class PrebuiltClusters:
    """FunderClusters port over a graph that was just built.

    ``built`` is false when the worker skipped the walk. An empty list then
    means the graph does not exist, not that no cluster was found.
    """

    def __init__(self, clusters: list[FunderCluster], *, built: bool = True) -> None:
        self._clusters = clusters
        self.built = built

    async def build_graph_for_token(
        self,
        token_mint: str,
        top_n_holders: int = 20,
    ) -> list[FunderCluster]:
        return list(self._clusters)


def mark_funder_graph_unbuilt(scanner: object) -> None:
    """Tell the scanner this scan has no funder graph to read."""
    funder = getattr(scanner, "_funder_graph", None)
    if isinstance(funder, PrebuiltClusters):
        funder.built = False


class RiskScanWorker:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        redis: Redis,
        funder_factory: FunderFactory,
        scanner_factory: ScannerFactory,
        *,
        stop: Any | None = None,
        consumer_name: str = "risk-scan",
        retry_backoff_seconds: float = 2.0,
        fast_slots: asyncio.Semaphore | None = None,
        heavy_slots: asyncio.Semaphore | None = None,
    ) -> None:
        self._sessions = sessions
        self._redis = redis
        self._funder_factory = funder_factory
        self._scanner_factory = scanner_factory
        self._stop = stop
        self._consumer = consumer_name
        self._group_ready = False
        self._retry_backoff_seconds = retry_backoff_seconds
        # Independent of heavy_slots. The funder-graph builder must not acquire
        # either one: asyncio.Semaphore is not reentrant.
        self._fast_slots = fast_slots
        self._heavy_slots = heavy_slots
        self._heavy_group_ready = False
        self._priority_group_ready = False

    async def run_once(self, *, block_ms: int | None = None) -> tuple[int, int]:
        """One priority read, then the qualify stream.

        A non-empty priority batch does not skip qualify. The numbered plan
        said to read the background stream only when priority was empty. That
        would let one unacked priority message hide qualify on every later
        pass. The same plan forbids that, so qualify is read after the single
        priority pass. The 5s block is used only when priority returned nothing.
        """
        priority = await self._read_available(
            RISK_SCAN_PRIORITY_QUEUE,
            RISK_SCAN_PRIORITY_GROUP,
            self._ensure_priority_group,
            block_ms=None,
        )
        priority_processed, priority_failed = await self._dispatch(
            RISK_SCAN_PRIORITY_QUEUE,
            RISK_SCAN_PRIORITY_GROUP,
            priority,
            self._fast_slots,
            self._handle_message_body,
        )
        background = await self._read_available(
            RISK_SCAN_QUEUE,
            RISK_SCAN_GROUP,
            self._ensure_group,
            block_ms=None if priority else block_ms,
        )
        processed, failed = await self._dispatch(
            RISK_SCAN_QUEUE,
            RISK_SCAN_GROUP,
            background,
            self._fast_slots,
            self._handle_message_body,
        )
        processed += priority_processed
        failed += priority_failed
        logger.info("risk_scan_cycle", processed=processed, failed=failed)
        return processed, failed

    async def run_heavy_once(self, *, block_ms: int | None = None) -> tuple[int, int]:
        return await self._run_queue(
            RISK_HEAVY_QUEUE,
            RISK_HEAVY_GROUP,
            self._ensure_heavy_group,
            self._handle_heavy_body,
            slots=self._heavy_slots,
            block_ms=block_ms,
            cycle_event="risk_heavy_cycle",
        )

    async def move_undelivered_post_migrations(self) -> int:
        """Copy not-yet-delivered post_migration entries onto the priority stream.

        PEL entries stay where the consumer already claimed them. A second call
        finds nothing to copy: the first call deleted those stream ids.
        """
        await self._ensure_group()
        await self._ensure_priority_group()
        last_id = "0-0"
        for group in await self._redis.xinfo_groups(RISK_SCAN_QUEUE):
            if str(group.get("name")) == RISK_SCAN_GROUP:
                last_id = str(group.get("last-delivered-id") or "0-0")
                break
        moved = 0
        start = f"({last_id}"
        while True:
            rows = await self._redis.xrange(RISK_SCAN_QUEUE, min=start, max="+", count=100)
            if not rows:
                break
            for raw_id, raw_fields in rows:
                if raw_id is None or raw_fields is None:
                    continue
                message_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
                start = "(" + message_id
                payload: dict[str, str] = {}
                for key, value in raw_fields.items():
                    field = key.decode() if isinstance(key, bytes) else str(key)
                    payload[field] = value.decode() if isinstance(value, bytes) else str(value)
                if payload.get("reason") != "post_migration":
                    continue
                await self._redis.xadd(RISK_SCAN_PRIORITY_QUEUE, cast(Any, payload))
                await self._redis.xdel(RISK_SCAN_QUEUE, message_id)
                moved += 1
            if len(rows) < 100:
                break
        return moved

    async def _run_queue(
        self,
        queue: str,
        group: str,
        ensure: Callable[[], Any],
        handle: Callable[..., Any],
        *,
        slots: asyncio.Semaphore | None,
        block_ms: int | None,
        cycle_event: str,
    ) -> tuple[int, int]:
        messages = await self._read_available(queue, group, ensure, block_ms=block_ms)
        processed, failed = await self._dispatch(queue, group, messages, slots, handle)
        logger.info(cycle_event, processed=processed, failed=failed)
        return processed, failed

    async def _read_available(
        self,
        queue: str,
        group: str,
        ensure: Callable[[], Any],
        *,
        block_ms: int | None,
    ) -> list[tuple[str, dict[str, str]]]:
        """This consumer's PEL, then new messages. PEL is not read again."""
        await ensure()
        messages = await self._read(queue, group, "0", block_ms=None)
        messages.extend(await self._read(queue, group, ">", block_ms=block_ms))
        return messages

    async def _dispatch(
        self,
        queue: str,
        group: str,
        messages: list[tuple[str, dict[str, str]]],
        slots: asyncio.Semaphore | None,
        handle: Callable[..., Any],
    ) -> tuple[int, int]:
        async def call(message_id: str, fields: dict[str, str]) -> tuple[int, int]:
            outcome: tuple[int, int] = await handle(message_id, fields, queue=queue, group=group)
            return outcome

        if slots is None:
            outcomes: list[tuple[int, int]] = []
            for message_id, fields in messages:
                if self._stopping():
                    break
                outcomes.append(await call(message_id, fields))
        else:
            tasks = [
                asyncio.create_task(self._in_slot(slots, call, message_id, fields))
                for message_id, fields in messages
                if not self._stopping()
            ]
            outcomes = list(await asyncio.gather(*tasks))
        if not outcomes:
            return 0, 0
        return sum(item[0] for item in outcomes), sum(item[1] for item in outcomes)

    async def _in_slot(
        self,
        slots: asyncio.Semaphore | None,
        handle: Callable[[str, dict[str, str]], Any],
        message_id: str,
        fields: dict[str, str],
    ) -> tuple[int, int]:
        if self._stopping():
            return 0, 0
        if slots is None:
            return await handle(message_id, fields)
        async with slots:
            if self._stopping():
                return 0, 0
            return await handle(message_id, fields)

    async def _handle_message_body(
        self,
        message_id: str,
        fields: dict[str, str],
        *,
        queue: str,
        group: str,
    ) -> tuple[int, int]:
        mint = str(fields.get("mint") or "")
        trace_id = str(fields.get("trace_id") or "") or new_trace_id()
        try:
            if not mint:
                await self._redis.xack(queue, group, message_id)
                with bind_trace(trace_id):
                    logger.warning(
                        "risk_scan_failed",
                        reason="missing_mint",
                        message_id=message_id,
                    )
                return 0, 1
            if await self._in_backoff(message_id):
                return 0, 0
            started = time.perf_counter()
            try:
                with bind_trace(trace_id):
                    enqueue = await self._scan(mint, trace_id)
            finally:
                record_risk_scan_latency(time.perf_counter() - started)
            # Ack only after the heavy message is stored. A failed XADD leaves
            # this message pending so the next cycle tries the enqueue again.
            if enqueue:
                await self._redis.xadd(
                    RISK_HEAVY_QUEUE,
                    {"mint": mint, "trace_id": trace_id},
                )
            await self._redis.xack(queue, group, message_id)
            return 1, 0
        except _RetryFastScan as retry:
            await self._arm_delay(message_id, retry.delay)
            with bind_trace(trace_id):
                logger.info(
                    "risk_scan_post_migration_retry",
                    mint=mint,
                    attempt=retry.attempt,
                    retry_in_seconds=retry.delay,
                    reason=retry.reason,
                )
            return 0, 0
        except Exception as exc:
            with bind_trace(trace_id):
                logger.warning(
                    "risk_scan_failed",
                    mint=mint,
                    message_id=message_id,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
            return 0, 1

    async def _handle_heavy_body(
        self,
        message_id: str,
        fields: dict[str, str],
        *,
        queue: str,
        group: str,
    ) -> tuple[int, int]:
        del queue, group
        mint = str(fields.get("mint") or "")
        trace_id = str(fields.get("trace_id") or "") or new_trace_id()
        try:
            if not mint:
                await self._redis.xack(RISK_HEAVY_QUEUE, RISK_HEAVY_GROUP, message_id)
                with bind_trace(trace_id):
                    logger.warning(
                        "risk_heavy_failed",
                        reason="missing_mint",
                        message_id=message_id,
                    )
                return 0, 1
            if await self._in_backoff(message_id):
                return 0, 0
            with bind_trace(trace_id):
                await self._scan_heavy(mint, trace_id)
            await self._redis.xack(RISK_HEAVY_QUEUE, RISK_HEAVY_GROUP, message_id)
            return 1, 0
        except FunderLookupFailure as exc:
            with bind_trace(trace_id):
                await self._handle_lookup_failure(mint, message_id, exc)
            return 0, 1
        except Exception as exc:
            with bind_trace(trace_id):
                logger.warning(
                    "risk_heavy_failed",
                    mint=mint,
                    message_id=message_id,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
            return 0, 1

    async def _scan(self, mint: str, trace_id: str) -> bool:
        """Write the fast checks and return whether a heavy message is required.

        ``False`` means the funder walk already failed for good, or the fast
        checks make a signal impossible (``heavy_scan_status = skipped``). The
        caller acks without enqueueing. Distribution columns are not written here.
        """
        async with self._sessions() as session:
            async with session.begin():
                token = await session.scalar(select(Token).where(Token.mint == mint))
                prior = token.funder_graph_status if token is not None else None
                if prior == FUNDER_GRAPH_FAILED:
                    logger.info(
                        "funder_graph_skipped",
                        mint=mint,
                        reason=FUNDER_GRAPH_FAILED,
                    )
                    return False
                scanner = self._scanner_factory(session, [])
                report = await scanner.scan_fast(mint)
                if token is None:
                    token = Token(mint=mint, first_seen_at=report.scanned_at)
                    session.add(token)
                if report.liquidity_usd is not None:
                    token.liquidity_usd = report.liquidity_usd
                token.risk_score = None
                token.risk_flags = _fast_flags(report)
                token.heavy_scan_status = HEAVY_SCAN_PENDING
                token.mint_authority_renounced = _passed(report, "check_mint_authority")
                token.freeze_authority_renounced = _passed(report, "check_freeze_authority")
                token.updated_at = report.scanned_at
                token.trace_id = trace_id
                logger.info(
                    "risk_scan_stored",
                    mint=mint,
                    risk_score=None,
                    risk_flags=token.risk_flags,
                    heavy_scan_status=HEAVY_SCAN_PENDING,
                )
                seen_at = report.scanned_at
                await record_listing(
                    session,
                    self._redis,
                    token,
                    pool_status=report.pool_status,
                    price_usd=report.price_usd,
                    seen_at=seen_at,
                )
                lp_book = getattr(scanner, "lp_book", None)
                rows = (
                    []
                    if lp_book is None
                    else [
                        LpCustody(row.is_burn_address, row.is_lock_program, row.amount_raw)
                        for row in lp_book.rows
                    ]
                )
                await record_lp_unlock(session, token, classify_lp_custody(rows))
                await session.flush()
                reason = heavy_skip_reason(
                    list(token.risk_flags),
                    wallet_history=await _has_wallet_history(session, token),
                )
                if (
                    reason is not None
                    and reason.startswith("fast_check_unavailable")
                    and prior == FUNDER_GRAPH_PENDING_POST
                ):
                    attempt = await self._post_migration_attempt(mint)
                    if attempt <= len(POST_MIGRATION_RETRY_DELAYS_SECONDS):
                        # Raising rolls this transaction back; the rescan writes again.
                        raise _RetryFastScan(
                            attempt, POST_MIGRATION_RETRY_DELAYS_SECONDS[attempt - 1], reason
                        )
                if reason is not None:
                    token.heavy_scan_status = HEAVY_SCAN_SKIPPED
                    logger.info("risk_heavy_skipped", mint=mint, reason=reason)
                    return False
        return True

    async def _scan_heavy(self, mint: str, trace_id: str) -> None:
        async with self._sessions() as session:
            async with session.begin():
                token = await session.scalar(select(Token).where(Token.mint == mint))
                if token is not None and token.heavy_scan_status in {
                    HEAVY_SCAN_COMPLETE,
                    HEAVY_SCAN_FAILED,
                }:
                    logger.info(
                        "risk_heavy_already_finished",
                        mint=mint,
                        heavy_scan_status=token.heavy_scan_status,
                    )
                    return
                prior = token.funder_graph_status if token is not None else None
                if prior == FUNDER_GRAPH_FAILED:
                    await self._store_heavy_failure(session, token)
                    return
                if prior == FUNDER_GRAPH_SKIPPED:
                    clusters: list[FunderCluster] = []
                    logger.info(
                        "funder_graph_skipped",
                        mint=mint,
                        reason=FUNDER_GRAPH_SKIPPED,
                    )
                else:
                    clusters = list(await self._funder_factory(session).build_graph_for_token(mint))
                    token = await session.scalar(select(Token).where(Token.mint == mint))
                    if token is not None and prior == FUNDER_GRAPH_PENDING_POST:
                        token.funder_graph_status = FUNDER_GRAPH_COMPLETED_POST
                        logger.info(
                            "funder_graph_completed_post_migration",
                            mint=mint,
                        )
                    elif token is not None and prior is None:
                        token.funder_graph_status = FUNDER_GRAPH_BUILT
                scanner = self._scanner_factory(session, list(clusters))
                if prior == FUNDER_GRAPH_SKIPPED:
                    mark_funder_graph_unbuilt(scanner)
                report = await scanner.scan_heavy(mint)
                if token is None:
                    token = Token(mint=mint, first_seen_at=report.scanned_at)
                    session.add(token)
                    await session.flush()
                flags = _flags_after_heavy(token.risk_flags, report)
                token.risk_flags = flags
                token.risk_score = Decimal(score_from_flags(flags))
                token.heavy_scan_status = HEAVY_SCAN_COMPLETE
                # Freshness for fusion is this write, not the fast ack. A mint
                # that waited in risk_heavy_queue is inside the 5-minute window
                # once risk_score and updated_at land together.
                token.updated_at = report.scanned_at
                token.trace_id = trace_id
                logger.info(
                    "risk_heavy_stored",
                    mint=mint,
                    risk_score=str(token.risk_score),
                    risk_flags=token.risk_flags,
                    heavy_scan_status=HEAVY_SCAN_COMPLETE,
                )
                builder = self._funder_factory(session)
                dev_root = None
                if token.creator_address and hasattr(builder, "root_for_wallet"):
                    dev_root = await builder.root_for_wallet(token.creator_address)
                await measure_distribution(
                    session,
                    token,
                    getattr(scanner, "holder_book", None),
                    dev_root=dev_root,
                )

    async def _handle_lookup_failure(
        self,
        mint: str,
        message_id: str,
        failure: FunderLookupFailure,
    ) -> None:
        """Ack a permanent funded-by failure. Leave transient ones for a later cycle.

        HTTP 403 (and any other non-429 4xx) is permanent on the first failure:
        the free plan returns 403 for funded-by, and repeating it does not
        change the body. 429 and transport errors count per wallet and status
        up to ``MAX_FUNDER_LOOKUP_RETRIES``, then the same terminal status.
        """
        attempts = 1
        if not failure.permanent:
            attempts = await self._failure_count(mint, failure)
            if attempts < MAX_FUNDER_LOOKUP_RETRIES:
                await self._arm_backoff(message_id, attempts)
                logger.info(
                    "funder_lookup_retry",
                    mint=mint,
                    wallet=failure.wallet_address,
                    status_code=failure.status_code,
                    attempt=attempts,
                    message_id=message_id,
                )
                return
        await self._close_heavy_as_failed(mint)
        await self._redis.xack(RISK_HEAVY_QUEUE, RISK_HEAVY_GROUP, message_id)
        logger.warning(
            "funder_lookup_failed_permanently",
            mint=mint,
            wallet=failure.wallet_address,
            status_code=failure.status_code,
            attempts=attempts,
            message_id=message_id,
            error=str(failure),
        )

    async def _failure_count(self, mint: str, failure: FunderLookupFailure) -> int:
        code = failure.status_code if failure.status_code is not None else "transport"
        key = f"funder_retry:{mint}:{failure.wallet_address}:{code}"
        count = int(await self._redis.incr(key))
        await self._redis.expire(key, _RETRY_KEY_TTL_SECONDS)
        return count

    async def _post_migration_attempt(self, mint: str) -> int:
        key = f"post_migration_retry:{mint}"
        count = int(await self._redis.incr(key))
        await self._redis.expire(key, _RETRY_KEY_TTL_SECONDS)
        return count

    async def _arm_backoff(self, message_id: str, attempt: int) -> None:
        await self._arm_delay(message_id, self._retry_backoff_seconds * (2 ** (attempt - 1)))

    async def _arm_delay(self, message_id: str, delay: float) -> None:
        if delay <= 0:
            return
        await self._redis.set(
            f"funder_backoff:{message_id}",
            str(time.time() + delay),
            ex=_RETRY_KEY_TTL_SECONDS,
        )

    async def _in_backoff(self, message_id: str) -> bool:
        raw = await self._redis.get(f"funder_backoff:{message_id}")
        if raw is None:
            return False
        try:
            deadline = float(raw)
        except ValueError:
            return False
        return deadline > time.time()

    async def _close_heavy_as_failed(self, mint: str) -> None:
        """Score the checks that already resolved. Leave the heavy flags unavailable."""
        async with self._sessions() as session:
            async with session.begin():
                token = await session.scalar(select(Token).where(Token.mint == mint))
                if token is None:
                    return
                await self._store_heavy_failure(session, token)

    async def _store_heavy_failure(self, session: AsyncSession, token: Token) -> None:
        if token.funder_graph_status not in _PROTECTED_FUNDER_STATUSES:
            token.funder_graph_status = FUNDER_GRAPH_FAILED
        flags = _ensure_heavy_unavailable(
            [str(flag) for flag in token.risk_flags] if isinstance(token.risk_flags, list) else []
        )
        token.risk_flags = flags
        token.risk_score = Decimal(score_from_flags(flags))
        token.heavy_scan_status = HEAVY_SCAN_FAILED
        token.updated_at = datetime.now(timezone.utc)
        logger.warning(
            "risk_heavy_stored",
            mint=token.mint,
            risk_score=str(token.risk_score),
            risk_flags=flags,
            heavy_scan_status=HEAVY_SCAN_FAILED,
        )

    async def _ensure_group(self) -> None:
        if self._group_ready:
            return
        try:
            await self._redis.xgroup_create(
                RISK_SCAN_QUEUE,
                RISK_SCAN_GROUP,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._group_ready = True

    async def _ensure_heavy_group(self) -> None:
        if self._heavy_group_ready:
            return
        try:
            await self._redis.xgroup_create(
                RISK_HEAVY_QUEUE,
                RISK_HEAVY_GROUP,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._heavy_group_ready = True

    async def _ensure_priority_group(self) -> None:
        if self._priority_group_ready:
            return
        try:
            await self._redis.xgroup_create(
                RISK_SCAN_PRIORITY_QUEUE,
                RISK_SCAN_PRIORITY_GROUP,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._priority_group_ready = True

    async def _read(
        self,
        queue: str,
        group: str,
        stream_id: str,
        *,
        block_ms: int | None,
    ) -> list[tuple[str, dict[str, str]]]:
        rows = await self._redis.xreadgroup(
            groupname=group,
            consumername=self._consumer,
            streams={queue: stream_id},
            count=20,
            block=block_ms,
        )
        messages: list[tuple[str, dict[str, str]]] = []
        for _stream, entries in rows or []:
            for message_id, fields in entries:
                messages.append(
                    (str(message_id), {str(key): str(value) for key, value in fields.items()})
                )
        return messages

    def _stopping(self) -> bool:
        return self._stop is not None and self._stop.is_set()


def _passed(report: RiskReport, name: str) -> bool | None:
    for check in report.checks:
        if check.name == name:
            return check.passed
    return None


def _risk_flags(report: RiskReport) -> list[str]:
    flags: list[str] = []
    for check in report.checks:
        if check.passed is False:
            flags.append(check.name)
        elif check.passed is None:
            flags.append(f"{check.name}:unavailable")
    return flags


def _fast_flags(report: RiskReport) -> list[str]:
    flags = [flag for flag in _risk_flags(report) if flag.split(":", 1)[0] in FAST_CHECKS]
    flags.extend(f"{name}:unavailable" for name in HEAVY_CHECKS)
    return flags


def _flags_after_heavy(stored: object, heavy: RiskReport) -> list[str]:
    kept: list[str] = []
    if isinstance(stored, list):
        for flag in stored:
            text = str(flag)
            if text.split(":", 1)[0] in FAST_CHECKS:
                kept.append(text)
    present = {check.name for check in heavy.checks}
    kept.extend(_risk_flags(heavy))
    for name in HEAVY_CHECKS:
        if name not in present:
            kept.append(f"{name}:unavailable")
    return kept


def _ensure_heavy_unavailable(flags: list[str]) -> list[str]:
    kept = [flag for flag in flags if flag.split(":", 1)[0] not in HEAVY_CHECKS]
    kept.extend(f"{name}:unavailable" for name in HEAVY_CHECKS)
    return kept


async def _has_wallet_history(session: AsyncSession, token: Token) -> bool:
    """Same rule as fusion: a tracked buyer of this mint with a known win rate."""
    found = await session.scalar(
        select(TrackedWallet.id)
        .join(WalletTx, WalletTx.wallet_id == TrackedWallet.id)
        .where(
            WalletTx.token_id == token.id,
            WalletTx.action == "buy",
            TrackedWallet.win_rate.is_not(None),
        )
        .limit(1)
    )
    return found is not None


def heavy_skip_reason(fast_flags: list[str], *, wallet_history: bool) -> str | None:
    """Why the heavy half cannot lead to a signal, or None when it still can.

    Fusion drops a mint with any unavailable check, so an unavailable fast
    check ends it here. Otherwise every heavy check is assumed to pass and the
    bait penalty to be zero, the best case fusion could see; if even that
    composite does not clear the threshold, the walk would only spend credits.
    """
    unavailable = sorted(
        flag.split(":", 1)[0]
        for flag in fast_flags
        if flag.endswith(":unavailable") and flag.split(":", 1)[0] in FAST_CHECKS
    )
    if unavailable:
        return f"fast_check_unavailable:{','.join(unavailable)}"
    failed = {flag for flag in fast_flags if flag in FAST_CHECKS}
    best_risk = Decimal(sum(w for name, w in RISK_CHECK_WEIGHTS.items() if name not in failed))
    if wallet_history:
        best = composite_score(best_risk, _BEST_WALLET_SCORE, Decimal(0))
    else:
        best = composite_without_wallet(best_risk, Decimal(0))
    if emits_signal(best):
        return None
    return f"threshold_unreachable:best_composite={best}"


def score_from_flags(flags: list[str]) -> int:
    """Sum weights of checks that are not failed and not unavailable."""
    blocked = {flag.split(":", 1)[0] for flag in flags}
    return sum(weight for name, weight in RISK_CHECK_WEIGHTS.items() if name not in blocked)
