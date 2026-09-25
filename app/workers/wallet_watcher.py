"""Helius enhanced-webhook handler for tracked-wallet swaps.

Helius POSTs a JSON array of enhanced transactions and echoes the configured
``authHeader`` in the ``Authorization`` header. A buy is a SWAP whose
``events.swap`` shows native SOL in and a non-WSOL token out for that wallet.
The same shape is read from ``nativeTransfers`` and ``tokenTransfers`` when
the swap event is absent.

``wallet_txs`` is a hypertable, so the unique key is
``(tx_signature, block_time)``. Retries of the same delivery hit
``ON CONFLICT DO NOTHING`` on that constraint. A token still waiting for its
first risk score is queued again, which covers a retry after Redis was down.
"""

from __future__ import annotations

import hmac
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Literal

import structlog
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import BAIT_CHECK_QUEUE, BAIT_RECHECK_DELAY_MINUTES, RISK_SCAN_QUEUE
from app.db.models import TrackedWallet, WalletTx
from app.observability import bind_trace, new_trace_id, record_webhook
from app.services.bait_detector import PendingBaitCheck
from app.services.token_store import insert_token
from app.workers.queues import bait_member, score_for

logger = structlog.get_logger(__name__)

WSOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS = Decimal("1000000000")
Clock = Callable[[], datetime]


class WebhookRejected(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class ParsedSwap:
    signature: str
    wallet_address: str
    mint: str
    action: Literal["buy", "sell"]
    amount_sol: Decimal
    block_time: datetime


@dataclass(frozen=True)
class WatchResult:
    stored: int
    duplicated: int
    skipped: int
    risk_enqueued: int
    bait_enqueued: int


class WalletWatcher:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        redis: Redis,
        webhook_secret: str,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._sessions = sessions
        self._redis = redis
        self._secret = webhook_secret
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def handle(self, payload: Any, authorization: str | None) -> WatchResult:
        self._authorize(authorization)
        swaps = parse_enhanced_payload(payload)
        now = self._clock()
        async with self._sessions() as session:
            async with session.begin():
                stored = await self._persist(session, swaps, now)
        await self._enqueue(stored.jobs)
        result = WatchResult(
            stored=stored.stored,
            duplicated=stored.duplicated,
            skipped=stored.skipped,
            risk_enqueued=stored.risk_jobs,
            bait_enqueued=stored.bait_jobs,
        )
        record_webhook()
        summary_trace = stored.jobs[0].trace_id if len({job.trace_id for job in stored.jobs}) == 1 else None
        with bind_trace(summary_trace):
            logger.info(
                "webhook_handled",
                stored=result.stored,
                duplicated=result.duplicated,
                skipped=result.skipped,
                risk_enqueued=result.risk_enqueued,
                bait_enqueued=result.bait_enqueued,
            )
        for job in stored.jobs:
            with bind_trace(job.trace_id):
                logger.info(
                    "webhook_buy_traced",
                    mint=job.mint,
                    risk_enqueued=job.risk,
                    signature=job.bait.buy_signature if job.bait is not None else None,
                )
        return result

    def _authorize(self, authorization: str | None) -> None:
        if not self._secret:
            raise WebhookRejected(503, "webhook secret is not configured")
        supplied = authorization or ""
        try:
            match = hmac.compare_digest(supplied, self._secret)
        except ValueError:
            match = False
        if not match:
            raise WebhookRejected(401, "unauthorized")

    async def _persist(
        self,
        session: AsyncSession,
        swaps: list[ParsedSwap],
        now: datetime,
    ) -> _PersistResult:
        stored = 0
        duplicated = 0
        skipped = 0
        jobs: list[_Job] = []
        risk_mints: set[str] = set()
        for swap in swaps:
            wallet = await session.scalar(
                select(TrackedWallet).where(TrackedWallet.address == swap.wallet_address)
            )
            if wallet is None:
                skipped += 1
                continue
            token, created = await insert_token(session, mint=swap.mint, first_seen_at=now)
            inserted = await _insert_tx(session, wallet_id=wallet.id, token_id=token.id, swap=swap)
            if inserted:
                stored += 1
            else:
                duplicated += 1
            if swap.action != "buy":
                continue
            # A row with no score still needs a scan, including a retry after
            # the insert committed and the Redis write did not.
            enqueue_risk = (created or token.risk_score is None) and swap.mint not in risk_mints
            if enqueue_risk:
                risk_mints.add(swap.mint)
            scheduled_at = now + timedelta(minutes=BAIT_RECHECK_DELAY_MINUTES)
            jobs.append(
                _Job(
                    mint=swap.mint,
                    risk=enqueue_risk,
                    trace_id=new_trace_id(),
                    bait=PendingBaitCheck(
                        wallet_address=swap.wallet_address,
                        token_mint=swap.mint,
                        buy_signature=swap.signature,
                        buy_amount_sol=swap.amount_sol,
                        buy_block_time=swap.block_time,
                        scheduled_at=scheduled_at,
                        created_at=now,
                    ),
                )
            )
        return _PersistResult(
            stored=stored,
            duplicated=duplicated,
            skipped=skipped,
            jobs=jobs,
            risk_jobs=len(risk_mints),
            bait_jobs=len(jobs),
        )

    async def _enqueue(self, jobs: list[_Job]) -> None:
        risk_mints: list[str] = []
        for job in jobs:
            if job.risk and job.mint not in risk_mints:
                risk_mints.append(job.mint)
                await self._redis.xadd(
                    RISK_SCAN_QUEUE,
                    {"mint": job.mint, "trace_id": job.trace_id},
                )
            if job.bait is not None:
                pending = job.bait
                await self._redis.zadd(
                    BAIT_CHECK_QUEUE,
                    {bait_member(pending): score_for(pending.scheduled_at)},
                    nx=True,
                )


@dataclass
class _Job:
    mint: str
    risk: bool
    bait: PendingBaitCheck | None
    trace_id: str


@dataclass
class _PersistResult:
    stored: int
    duplicated: int
    skipped: int
    jobs: list[_Job]
    risk_jobs: int
    bait_jobs: int


def parse_enhanced_payload(payload: Any) -> list[ParsedSwap]:
    if isinstance(payload, dict):
        items = [payload]
    elif isinstance(payload, list):
        items = payload
    else:
        raise WebhookRejected(400, "payload must be a JSON array of enhanced transactions")
    swaps: list[ParsedSwap] = []
    for item in items:
        if isinstance(item, dict):
            swaps.extend(_parse_transaction(item))
    return swaps


def _parse_transaction(tx: dict[str, Any]) -> list[ParsedSwap]:
    if tx.get("transactionError"):
        return []
    signature = tx.get("signature")
    timestamp = tx.get("timestamp")
    if not isinstance(signature, str) or not signature or not isinstance(timestamp, int):
        return []
    block_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    if tx.get("type") not in (None, "SWAP") and not _swap_event(tx):
        return []
    from_event = _from_swap_event(tx, signature, block_time)
    if from_event:
        return from_event
    if tx.get("type") != "SWAP":
        return []
    return _from_transfers(tx, signature, block_time)


def _swap_event(tx: dict[str, Any]) -> dict[str, Any]:
    events = tx.get("events")
    if not isinstance(events, dict):
        return {}
    swap = events.get("swap")
    return swap if isinstance(swap, dict) else {}


def _from_swap_event(
    tx: dict[str, Any],
    signature: str,
    block_time: datetime,
) -> list[ParsedSwap]:
    swap = _swap_event(tx)
    if not swap:
        return []
    parsed: list[ParsedSwap] = []
    native_in = swap.get("nativeInput") if isinstance(swap.get("nativeInput"), dict) else None
    native_out = swap.get("nativeOutput") if isinstance(swap.get("nativeOutput"), dict) else None
    outputs = _records(swap.get("tokenOutputs"))
    inputs = _records(swap.get("tokenInputs"))
    if native_in is not None:
        wallet = native_in.get("account")
        mint = _first_mint(outputs, user=wallet if isinstance(wallet, str) else None)
        if isinstance(wallet, str) and mint is not None:
            parsed.append(
                ParsedSwap(
                    signature=signature,
                    wallet_address=wallet,
                    mint=mint,
                    action="buy",
                    amount_sol=_lamports(native_in.get("amount")),
                    block_time=block_time,
                )
            )
    if native_out is not None:
        wallet = native_out.get("account")
        mint = _first_mint(inputs, user=wallet if isinstance(wallet, str) else None)
        if isinstance(wallet, str) and mint is not None:
            parsed.append(
                ParsedSwap(
                    signature=signature,
                    wallet_address=wallet,
                    mint=mint,
                    action="sell",
                    amount_sol=_lamports(native_out.get("amount")),
                    block_time=block_time,
                )
            )
    return parsed


def _from_transfers(
    tx: dict[str, Any],
    signature: str,
    block_time: datetime,
) -> list[ParsedSwap]:
    received: dict[tuple[str, str], None] = {}
    sent: dict[tuple[str, str], None] = {}
    for transfer in _records(tx.get("tokenTransfers")):
        mint = transfer.get("mint")
        if not isinstance(mint, str) or mint == WSOL_MINT:
            continue
        receiver = transfer.get("toUserAccount")
        sender = transfer.get("fromUserAccount")
        if isinstance(receiver, str) and receiver:
            received[(receiver, mint)] = None
        if isinstance(sender, str) and sender:
            sent[(sender, mint)] = None
    parsed: list[ParsedSwap] = []
    for wallet, mint in received:
        if (wallet, mint) in sent:
            continue
        parsed.append(
            ParsedSwap(
                signature=signature,
                wallet_address=wallet,
                mint=mint,
                action="buy",
                amount_sol=_sol_from_native(tx, wallet, outgoing=True),
                block_time=block_time,
            )
        )
    for wallet, mint in sent:
        if (wallet, mint) in received:
            continue
        parsed.append(
            ParsedSwap(
                signature=signature,
                wallet_address=wallet,
                mint=mint,
                action="sell",
                amount_sol=_sol_from_native(tx, wallet, outgoing=False),
                block_time=block_time,
            )
        )
    return parsed


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _first_mint(records: list[dict[str, Any]], user: str | None) -> str | None:
    for record in records:
        mint = record.get("mint")
        owner = record.get("userAccount")
        if not isinstance(mint, str) or mint == WSOL_MINT:
            continue
        if user is not None and owner not in (None, user):
            continue
        return mint
    return None


def _lamports(value: Any) -> Decimal:
    if isinstance(value, bool) or value is None:
        return Decimal(0)
    if isinstance(value, int):
        raw = Decimal(value)
    elif isinstance(value, str) and value.strip():
        raw = Decimal(value)
    else:
        return Decimal(0)
    return raw / LAMPORTS


def _sol_from_native(tx: dict[str, Any], wallet: str, *, outgoing: bool) -> Decimal:
    total = Decimal(0)
    for transfer in _records(tx.get("nativeTransfers")):
        side = transfer.get("fromUserAccount") if outgoing else transfer.get("toUserAccount")
        if side == wallet:
            total += _lamports(transfer.get("amount"))
    return total


async def _insert_tx(
    session: AsyncSession,
    *,
    wallet_id: Any,
    token_id: Any,
    swap: ParsedSwap,
) -> bool:
    inserted = await session.scalar(
        pg_insert(WalletTx)
        .values(
            wallet_id=wallet_id,
            token_id=token_id,
            action=swap.action,
            amount_sol=swap.amount_sol,
            tx_signature=swap.signature,
            block_time=swap.block_time,
        )
        .on_conflict_do_nothing(constraint="uq_wallet_txs_tx_signature_block_time")
        .returning(WalletTx.id)
    )
    return inserted is not None
