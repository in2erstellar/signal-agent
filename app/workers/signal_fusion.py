"""Fold a fresh risk scan, wallet quality, and bait flags into one signal.

``wallet_score`` is the mean ``tracked_wallets.win_rate`` of wallets that
bought the mint, scaled from a 0–1 fraction to 0–100. Missing win rates
contribute nothing. ``bait_flag_penalty`` is the sum of confidences on
``bait_flags`` for that mint.

A composite equal to ``SIGNAL_THRESHOLD`` does not create a row. The scan
must be newer than ``SIGNAL_RISK_SCAN_MAX_AGE_MINUTES``. One signal per
token is enforced by ``uq_signals_token_id``.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import exists, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import (
    SIGNAL_BAIT_WEIGHT,
    SIGNAL_RISK_SCAN_MAX_AGE_MINUTES,
    SIGNAL_RISK_WEIGHT,
    SIGNAL_THRESHOLD,
    SIGNAL_WALLET_WEIGHT,
)
from app.db.models import BaitFlag, Signal, Token, TrackedWallet, WalletTx
from app.observability import bind_trace

logger = structlog.get_logger(__name__)

Clock = Callable[[], datetime]


def composite_score(
    risk_score: Decimal,
    wallet_score: Decimal,
    bait_flag_penalty: Decimal,
) -> Decimal:
    return (
        risk_score * SIGNAL_RISK_WEIGHT
        + wallet_score * SIGNAL_WALLET_WEIGHT
        - bait_flag_penalty * SIGNAL_BAIT_WEIGHT
    )


def emits_signal(score: Decimal) -> bool:
    return score > SIGNAL_THRESHOLD


def wallet_score_from_win_rates(win_rates: list[Decimal | None]) -> Decimal:
    usable = [rate for rate in win_rates if rate is not None]
    if not usable:
        return Decimal(0)
    mean = sum(usable, Decimal(0)) / Decimal(len(usable))
    scaled = mean * Decimal(100)
    if scaled < 0:
        return Decimal(0)
    if scaled > 100:
        return Decimal(100)
    return scaled


class SignalFusion:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        clock: Clock | None = None,
        stop: Any | None = None,
    ) -> None:
        self._sessions = sessions
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._stop = stop

    async def run_once(self) -> tuple[int, int, int]:
        now = self._clock()
        cutoff = now - timedelta(minutes=SIGNAL_RISK_SCAN_MAX_AGE_MINUTES)
        processed = 0
        failed = 0
        created = 0
        async with self._sessions() as session:
            async with session.begin():
                tokens = list(
                    await session.scalars(
                        select(Token).where(
                            Token.updated_at.is_not(None),
                            Token.updated_at >= cutoff,
                            Token.risk_score.is_not(None),
                            ~exists(select(Signal.id).where(Signal.token_id == Token.id)),
                        )
                    )
                )
                for token in tokens:
                    if self._stop is not None and self._stop.is_set() and processed > 0:
                        break
                    processed += 1
                    try:
                        async with session.begin_nested():
                            if await self._consider(session, token, now):
                                created += 1
                    except Exception as exc:
                        failed += 1
                        logger.warning(
                            "signal_fusion_failed",
                            mint=token.mint,
                            error_type=type(exc).__name__,
                        )
        logger.info(
            "signal_fusion_cycle",
            processed=processed,
            failed=failed,
            created=created,
        )
        return processed, failed, created

    async def _consider(self, session: AsyncSession, token: Token, now: datetime) -> bool:
        win_rates = [
            rate
            for _wallet_id, rate in (
                await session.execute(
                    select(TrackedWallet.id, TrackedWallet.win_rate)
                    .join(WalletTx, WalletTx.wallet_id == TrackedWallet.id)
                    .where(WalletTx.token_id == token.id, WalletTx.action == "buy")
                    .distinct()
                )
            ).all()
        ]
        wallet_score = wallet_score_from_win_rates(win_rates)
        penalty = await session.scalar(
            select(func.coalesce(func.sum(BaitFlag.confidence), 0)).where(
                BaitFlag.token_id == token.id
            )
        )
        risk = token.risk_score if token.risk_score is not None else Decimal(0)
        score = composite_score(risk, wallet_score, Decimal(penalty or 0))
        if not emits_signal(score):
            with bind_trace(token.trace_id):
                logger.info(
                    "signal_below_threshold",
                    mint=token.mint,
                    composite_score=str(score),
                    threshold=str(SIGNAL_THRESHOLD),
                )
            return False
        inserted_id = await session.scalar(
            pg_insert(Signal)
            .values(
                token_id=token.id,
                wallet_score=wallet_score,
                risk_score=risk,
                composite_score=score,
                triggered_at=now,
                notified=False,
                trace_id=token.trace_id,
            )
            .on_conflict_do_nothing(constraint="uq_signals_token_id")
            .returning(Signal.id)
        )
        inserted = inserted_id is not None
        if inserted:
            with bind_trace(token.trace_id):
                logger.info(
                    "signal_created",
                    mint=token.mint,
                    composite_score=str(score),
                    risk_score=str(risk),
                    wallet_score=str(wallet_score),
                    bait_flag_penalty=str(penalty or 0),
                )
        return inserted
