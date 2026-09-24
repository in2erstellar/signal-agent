"""Poll ``bait_check_queue`` for jobs whose score is due, then persist flags.

The sorted set is the delay. A job stays in the set until the write commits,
so a crash mid-check is retried on the next poll. A payload that cannot be
parsed is removed; retrying it would fail the same way.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import structlog
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import BAIT_CHECK_QUEUE
from app.db.models import BaitFlag as BaitFlagRow
from app.db.models import Token, TrackedWallet
from app.services.bait_detector import BaitDetector, PendingBaitCheck
from app.services.bait_sources import SessionClusterBook, SessionSwapLedger
from app.workers.queues import parse_bait_member, score_for

logger = structlog.get_logger(__name__)

DetectorFactory = Callable[[AsyncSession], BaitDetector]
Clock = Callable[[], datetime]


class PipelineHolderCensus:
    """Top holders from Helius, plus tracked wallets that already traded the mint.

    ``getTokenLargestAccounts`` does not reveal a hidden holder. A wallet is
    treated as holding only when it is in that top list or in ``wallet_txs``.
    """

    def __init__(self, session: AsyncSession, holders: Any) -> None:
        self._session = session
        self._holders = holders

    async def public_top_holders(self, token_mint: str) -> set[str]:
        owners = await self._holders.get_top_holder_owners(token_mint, 20)
        return set(owners)

    async def holders(self, token_mint: str) -> set[str]:
        from app.db.models import WalletTx

        rows = await self._session.scalars(
            select(TrackedWallet.address)
            .join(WalletTx, WalletTx.wallet_id == TrackedWallet.id)
            .join(Token, WalletTx.token_id == Token.id)
            .where(Token.mint == token_mint)
        )
        return set(rows) | await self.public_top_holders(token_mint)


class BaitCheckWorker:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        redis: Redis,
        detector_factory: DetectorFactory,
        *,
        clock: Clock | None = None,
        stop: Any | None = None,
    ) -> None:
        self._sessions = sessions
        self._redis = redis
        self._detector_factory = detector_factory
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._stop = stop

    async def run_once(self) -> tuple[int, int]:
        now = self._clock()
        members = await self._redis.zrangebyscore(BAIT_CHECK_QUEUE, "-inf", score_for(now))
        processed = 0
        failed = 0
        for member in members:
            if self._stop is not None and self._stop.is_set():
                break
            try:
                await self._check(str(member))
            except _PoisonPayload as exc:
                failed += 1
                await self._redis.zrem(BAIT_CHECK_QUEUE, member)
                logger.warning("bait_check_failed", reason="poison_payload", error_type=type(exc.__cause__).__name__)
            except Exception as exc:
                failed += 1
                logger.warning("bait_check_failed", error_type=type(exc).__name__)
            else:
                await self._redis.zrem(BAIT_CHECK_QUEUE, member)
                processed += 1
        logger.info(
            "bait_check_cycle",
            processed=processed,
            failed=failed,
            due=len(members),
        )
        return processed, failed

    async def _check(self, member: str) -> None:
        try:
            pending = parse_bait_member(member)
        except ValidationError as exc:
            raise _PoisonPayload() from exc
        async with self._sessions() as session:
            async with session.begin():
                flags = await self._detector_factory(session).evaluate_mature_buy(pending)
                await _store_flags(session, pending, flags)
                logger.info(
                    "bait_check_stored",
                    wallet_address=pending.wallet_address,
                    token_mint=pending.token_mint,
                    buy_signature=pending.buy_signature,
                    flags=[flag.flag_type for flag in flags],
                )


class _PoisonPayload(Exception):
    pass


async def _store_flags(
    session: AsyncSession,
    pending: PendingBaitCheck,
    flags: list[Any],
) -> None:
    if not flags:
        return
    wallet = await session.scalar(
        select(TrackedWallet).where(TrackedWallet.address == pending.wallet_address)
    )
    token = await session.scalar(select(Token).where(Token.mint == pending.token_mint))
    if wallet is None or token is None:
        raise LookupError("tracked wallet or token is missing for a bait check")
    for flag in flags:
        statement = pg_insert(BaitFlagRow).values(
            wallet_id=wallet.id,
            token_id=token.id,
            flag_type=flag.flag_type,
            confidence=flag.confidence,
            detected_at=flag.detected_at,
            evidence=flag.evidence,
        )
        statement = statement.on_conflict_do_update(
            constraint="uq_bait_flags_wallet_token_type",
            set_={
                "confidence": statement.excluded.confidence,
                "detected_at": statement.excluded.detected_at,
                "evidence": statement.excluded.evidence,
            },
        )
        await session.execute(statement)


def detector_for_session(session: AsyncSession, holders: Any, clock: Clock) -> BaitDetector:
    from app.services.bait_detector import PendingBaitCheck as _Pending

    class _ClosedQueue:
        async def enqueue(self, check: _Pending) -> None:
            raise RuntimeError("bait_check_queue owns scheduling")

    return BaitDetector(
        SessionSwapLedger(session),
        SessionClusterBook(session),
        PipelineHolderCensus(session, holders),
        _ClosedQueue(),
        clock=clock,
    )
