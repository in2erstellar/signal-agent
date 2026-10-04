"""Consumer for ``risk_scan_queue``.

Builds the funder graph once, then scores the mint. The scanner's cluster
check reads that result instead of calling Helius a second time.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import structlog
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import MAX_FUNDER_LOOKUP_RETRIES, RISK_SCAN_GROUP, RISK_SCAN_QUEUE
from app.db.models import FunderCluster, Token
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
)

logger = structlog.get_logger(__name__)

_RETRY_KEY_TTL_SECONDS = 3600
_PROTECTED_FUNDER_STATUSES = frozenset(
    {
        FUNDER_GRAPH_SKIPPED,
        FUNDER_GRAPH_COMPLETED_POST,
        FUNDER_GRAPH_BUILT,
        FUNDER_GRAPH_FAILED,
    }
)

FunderFactory = Callable[[AsyncSession], Any]
ScannerFactory = Callable[[AsyncSession, list[FunderCluster]], RiskScanner]


class PrebuiltClusters:
    """FunderClusters port over a graph that was just built."""

    def __init__(self, clusters: list[FunderCluster]) -> None:
        self._clusters = clusters

    async def build_graph_for_token(
        self,
        token_mint: str,
        top_n_holders: int = 20,
    ) -> list[FunderCluster]:
        return list(self._clusters)


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
        scan_slots: asyncio.Semaphore | None = None,
    ) -> None:
        self._sessions = sessions
        self._redis = redis
        self._funder_factory = funder_factory
        self._scanner_factory = scanner_factory
        self._stop = stop
        self._consumer = consumer_name
        self._group_ready = False
        self._retry_backoff_seconds = retry_backoff_seconds
        # Whole scan, not a second acquire inside the funder-graph builder.
        self._scan_slots = scan_slots

    async def run_once(self, *, block_ms: int | None = None) -> tuple[int, int]:
        await self._ensure_group()
        messages = await self._read("0", block_ms=None)
        messages.extend(await self._read(">", block_ms=block_ms))
        if self._scan_slots is None:
            outcomes: list[tuple[int, int]] = []
            for message_id, fields in messages:
                if self._stopping():
                    break
                outcomes.append(await self._handle_message_body(message_id, fields))
        else:
            tasks = [
                asyncio.create_task(self._handle_message(message_id, fields))
                for message_id, fields in messages
                if not self._stopping()
            ]
            outcomes = list(await asyncio.gather(*tasks))
        processed = sum(item[0] for item in outcomes)
        failed = sum(item[1] for item in outcomes)
        logger.info("risk_scan_cycle", processed=processed, failed=failed)
        return processed, failed

    async def _handle_message(self, message_id: str, fields: dict[str, str]) -> tuple[int, int]:
        if self._stopping():
            return 0, 0
        if self._scan_slots is None:
            return await self._handle_message_body(message_id, fields)
        async with self._scan_slots:
            if self._stopping():
                return 0, 0
            return await self._handle_message_body(message_id, fields)

    async def _handle_message_body(self, message_id: str, fields: dict[str, str]) -> tuple[int, int]:
        mint = str(fields.get("mint") or "")
        trace_id = str(fields.get("trace_id") or "") or new_trace_id()
        try:
            if not mint:
                await self._redis.xack(RISK_SCAN_QUEUE, RISK_SCAN_GROUP, message_id)
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
                    await self._scan(mint, trace_id)
            finally:
                record_risk_scan_latency(time.perf_counter() - started)
            await self._redis.xack(RISK_SCAN_QUEUE, RISK_SCAN_GROUP, message_id)
            return 1, 0
        except FunderLookupFailure as exc:
            with bind_trace(trace_id):
                await self._handle_lookup_failure(mint, message_id, exc)
            return 0, 1
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

    async def _scan(self, mint: str, trace_id: str) -> RiskReport | None:
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
                    return None
                if prior == FUNDER_GRAPH_SKIPPED:
                    # Bonding-curve holders were not verified. The curve PDA is
                    # not a Raydium LP, so a graph walk would treat it as a wallet.
                    clusters: list[FunderCluster] = []
                    logger.info(
                        "funder_graph_skipped",
                        mint=mint,
                        reason=FUNDER_GRAPH_SKIPPED,
                    )
                else:
                    # build_graph_for_token calls getTokenLargestAccounts now.
                    # Post-migration scans must not reuse a pre-migration list.
                    clusters = list(
                        await self._funder_factory(session).build_graph_for_token(mint)
                    )
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
                report = await scanner.scan_token(mint)
                if token is None:
                    token = Token(mint=mint, first_seen_at=report.scanned_at)
                    session.add(token)
                if report.liquidity_usd is not None:
                    token.liquidity_usd = report.liquidity_usd
                token.risk_score = Decimal(report.risk_score)
                token.risk_flags = _risk_flags(report)
                token.mint_authority_renounced = _passed(report, "check_mint_authority")
                token.freeze_authority_renounced = _passed(report, "check_freeze_authority")
                token.updated_at = report.scanned_at
                token.trace_id = trace_id
                logger.info(
                    "risk_scan_stored",
                    mint=mint,
                    risk_score=report.risk_score,
                    risk_flags=token.risk_flags,
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
                rows = [] if lp_book is None else [
                    LpCustody(row.is_burn_address, row.is_lock_program, row.amount_raw)
                    for row in lp_book.rows
                ]
                await record_lp_unlock(session, token, classify_lp_custody(rows))
        return report

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
        await self._mark_failed_permanently(mint)
        await self._redis.xack(RISK_SCAN_QUEUE, RISK_SCAN_GROUP, message_id)
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

    async def _arm_backoff(self, message_id: str, attempt: int) -> None:
        delay = self._retry_backoff_seconds * (2 ** (attempt - 1))
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

    async def _mark_failed_permanently(self, mint: str) -> None:
        async with self._sessions() as session:
            async with session.begin():
                token = await session.scalar(select(Token).where(Token.mint == mint))
                if token is None:
                    return
                if token.funder_graph_status in _PROTECTED_FUNDER_STATUSES:
                    return
                token.funder_graph_status = FUNDER_GRAPH_FAILED

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

    async def _read(self, stream_id: str, *, block_ms: int | None) -> list[tuple[str, dict[str, str]]]:
        rows = await self._redis.xreadgroup(
            groupname=RISK_SCAN_GROUP,
            consumername=self._consumer,
            streams={RISK_SCAN_QUEUE: stream_id},
            count=20,
            block=block_ms,
        )
        messages: list[tuple[str, dict[str, str]]] = []
        for _stream, entries in rows or []:
            for message_id, fields in entries:
                messages.append((str(message_id), {str(key): str(value) for key, value in fields.items()}))
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
