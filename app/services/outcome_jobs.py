"""Delayed price snapshots. One Redis sorted set, one row shape.

``listing`` is written when a scan first sees the pool (or sees that there
is no pool). ``1h``, ``24h``, and ``7d`` are the same kind of row, scored
by due time. Only a pool that exists is followed up. A token that never
leaves the bonding curve stays ``no_pool`` and is not polled forever.
A pool seen without a price gets its ``listing`` row read once more later.

The accuracy tracker is not published from these rows yet. The rows are
already there so that tracker does not need a second queue.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

import structlog
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.pumpswap import PumpSwapAccountError
from app.adapters.raydium import PoolQuote, RaydiumError
from app.config import OUTCOME_HORIZONS, OUTCOME_QUEUE
from app.db.models import OutcomeCheck, Token
from app.services.lp_unlock import LpCustody, UnlockReading, lp_is_locked
from app.workers.queues import score_for

logger = structlog.get_logger(__name__)

Clock = Callable[[], datetime]

# DexScreener lists a PumpSwap pair one to three minutes after migration
# (live check 2026-10-16), so a scan right after migration has no price yet.
LISTING_RETRY_SECONDS = 180


class PoolSource(Protocol):
    async def pool_quote(self, mint: str) -> PoolQuote: ...


class LpCustodySource(Protocol):
    async def lp_custody(self, lp_mint: str) -> Any: ...


def outcome_member(token_id: UUID, horizon: str) -> str:
    return f"{token_id}|{horizon}"


def parse_outcome_member(member: str) -> tuple[UUID, str] | None:
    token_text, separator, horizon = member.partition("|")
    if not separator or horizon not in {"listing", *(name for name, _seconds in OUTCOME_HORIZONS)}:
        return None
    try:
        return UUID(token_text), horizon
    except ValueError:
        return None


def quote_observation(quote: PoolQuote) -> tuple[str, Decimal | None, str | None]:
    """Map one pool quote to status, price, reason. No silent zero price."""
    if quote.lp_mint is None:
        return "unavailable", None, "no_pool"
    if quote.price_usd is None:
        return "unavailable", None, "price_lookup_failed"
    return "observed", quote.price_usd, None


async def record_listing(
    session: AsyncSession,
    redis: Redis,
    token: Token,
    *,
    pool_status: str,
    price_usd: Decimal | None,
    seen_at: datetime,
) -> None:
    """Persist the scan-time price reading and enqueue follow-ups when a pool exists."""
    if pool_status == "no_pool":
        await _upsert_check(
            session,
            token.id,
            "listing",
            status="unavailable",
            price_usd=None,
            price_reason="no_pool",
            scheduled_at=seen_at,
            observed_at=seen_at,
        )
        logger.info(
            "price_snapshot_unavailable", mint=token.mint, horizon="listing", reason="no_pool"
        )
        return
    if pool_status != "observed":
        await _upsert_check(
            session,
            token.id,
            "listing",
            status="unavailable",
            price_usd=None,
            price_reason="price_lookup_failed",
            scheduled_at=seen_at,
            observed_at=seen_at,
        )
        logger.warning(
            "price_snapshot_unavailable",
            mint=token.mint,
            horizon="listing",
            reason="price_lookup_failed",
        )
        return
    if price_usd is None:
        retry_at = seen_at + timedelta(seconds=LISTING_RETRY_SECONDS)
        await _upsert_check(
            session,
            token.id,
            "listing",
            status="pending",
            price_usd=None,
            price_reason=None,
            scheduled_at=retry_at,
            observed_at=None,
        )
        await session.flush()
        await redis.zadd(OUTCOME_QUEUE, {outcome_member(token.id, "listing"): score_for(retry_at)})
        logger.info(
            "price_snapshot_deferred",
            mint=token.mint,
            horizon="listing",
            retry_in_seconds=LISTING_RETRY_SECONDS,
        )
    else:
        await _upsert_check(
            session,
            token.id,
            "listing",
            status="observed",
            price_usd=price_usd,
            price_reason=None,
            scheduled_at=seen_at,
            observed_at=seen_at,
        )
        logger.info("price_snapshot_observed", mint=token.mint, horizon="listing")
    for horizon, seconds in OUTCOME_HORIZONS:
        await _enqueue_followup(
            session,
            redis,
            token.id,
            horizon,
            due_at=seen_at + timedelta(seconds=seconds),
        )


async def record_lp_unlock(session: AsyncSession, token: Token, reading: UnlockReading) -> None:
    from app.db.models import LpUnlock

    row = await session.scalar(select(LpUnlock).where(LpUnlock.token_id == token.id))
    if row is not None and row.status == "alerted":
        return
    if row is None:
        row = LpUnlock(token_id=token.id, status=reading.status, reason=reading.reason)
        session.add(row)
    else:
        row.status = reading.status
        row.reason = reading.reason
    if reading.status != "scheduled":
        row.unlock_at = None
    logger.info("lp_unlock_reading", mint=token.mint, status=reading.status, reason=reading.reason)


class OutcomeWorker:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        redis: Redis,
        pools: PoolSource,
        *,
        lp_custody: LpCustodySource | None = None,
        clock: Clock | None = None,
        stop: Any | None = None,
    ) -> None:
        self._sessions = sessions
        self._redis = redis
        self._pools = pools
        self._lp_custody = lp_custody
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._stop = stop

    async def run_once(self) -> tuple[int, int]:
        now = self._clock()
        members = await self._redis.zrangebyscore(OUTCOME_QUEUE, "-inf", score_for(now))
        processed = 0
        failed = 0
        for raw_member in members:
            member = raw_member if isinstance(raw_member, str) else str(raw_member)
            if self._stop is not None and self._stop.is_set() and processed > 0:
                break
            parsed = parse_outcome_member(member)
            if parsed is None:
                await self._redis.zrem(OUTCOME_QUEUE, member)
                failed += 1
                continue
            token_id, horizon = parsed
            try:
                done = await self._capture(token_id, horizon, now)
            except Exception as exc:
                failed += 1
                logger.warning(
                    "outcome_capture_failed",
                    token_id=str(token_id),
                    horizon=horizon,
                    error_type=type(exc).__name__,
                )
                continue
            if done:
                await self._redis.zrem(OUTCOME_QUEUE, member)
                processed += 1
        return processed, failed

    async def _capture(self, token_id: UUID, horizon: str, now: datetime) -> bool:
        async with self._sessions() as session:
            async with session.begin():
                token = await session.get(Token, token_id)
                row = await session.scalar(
                    select(OutcomeCheck).where(
                        OutcomeCheck.token_id == token_id,
                        OutcomeCheck.horizon == horizon,
                    )
                )
                if token is None or row is None:
                    return False
                if row.status != "pending":
                    return True
                try:
                    quote = await self._pools.pool_quote(token.mint)
                except (RaydiumError, PumpSwapAccountError) as exc:
                    row.status = "unavailable"
                    row.price_reason = "price_lookup_failed"
                    row.observed_at = now
                    logger.warning(
                        "price_snapshot_unavailable",
                        mint=token.mint,
                        horizon=horizon,
                        reason="price_lookup_failed",
                        error_type=type(exc).__name__,
                    )
                    if horizon == "1h":
                        # The quote failed, so there is no LP mint to re-read.
                        # A second pools/info/mint call would only repeat the error.
                        logger.info(
                            "lp_followup_skipped",
                            mint=token.mint,
                            reason="price_lookup_failed",
                        )
                    return True
                status, price, reason = quote_observation(quote)
                row.status = status
                row.price_usd = price
                row.price_reason = reason
                row.observed_at = now
                if status == "observed":
                    logger.info("price_snapshot_observed", mint=token.mint, horizon=horizon)
                else:
                    logger.info(
                        "price_snapshot_unavailable",
                        mint=token.mint,
                        horizon=horizon,
                        reason=reason,
                    )
                if horizon == "1h":
                    # Reuse this quote's LP mint. Custody is Helius-only.
                    row.lp_locked = await self._lp_locked(token.mint, quote.lp_mint)
        return True

    async def _lp_locked(self, mint: str, lp_mint: str | None) -> bool | None:
        if lp_mint is None or self._lp_custody is None:
            return None
        try:
            book = await self._lp_custody.lp_custody(lp_mint)
        except Exception as exc:
            logger.warning("lp_followup_unavailable", mint=mint, error_type=type(exc).__name__)
            return None
        rows = [
            LpCustody(row.is_burn_address, row.is_lock_program, row.amount_raw) for row in book.rows
        ]
        locked = lp_is_locked(book.supply_raw, rows)
        if locked is None:
            logger.warning("lp_followup_unavailable", mint=mint, reason="supply_unreadable")
        return locked


async def _enqueue_followup(
    session: AsyncSession,
    redis: Redis,
    token_id: UUID,
    horizon: str,
    *,
    due_at: datetime,
) -> None:
    existing = await session.scalar(
        select(OutcomeCheck.id).where(
            OutcomeCheck.token_id == token_id,
            OutcomeCheck.horizon == horizon,
        )
    )
    if existing is not None:
        return
    session.add(
        OutcomeCheck(
            token_id=token_id,
            horizon=horizon,
            status="pending",
            scheduled_at=due_at,
        )
    )
    await session.flush()
    await redis.zadd(OUTCOME_QUEUE, {outcome_member(token_id, horizon): score_for(due_at)})


async def _upsert_check(
    session: AsyncSession,
    token_id: UUID,
    horizon: str,
    *,
    status: str,
    price_usd: Decimal | None,
    price_reason: str | None,
    scheduled_at: datetime,
    observed_at: datetime | None,
) -> None:
    row = await session.scalar(
        select(OutcomeCheck).where(
            OutcomeCheck.token_id == token_id,
            OutcomeCheck.horizon == horizon,
        )
    )
    if row is None:
        session.add(
            OutcomeCheck(
                token_id=token_id,
                horizon=horizon,
                status=status,
                price_usd=price_usd,
                price_reason=price_reason,
                scheduled_at=scheduled_at,
                observed_at=observed_at,
            )
        )
        return
    if row.status == "observed":
        return
    row.status = status
    row.price_usd = price_usd
    row.price_reason = price_reason
    row.observed_at = observed_at


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
