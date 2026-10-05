"""Fold a fresh risk scan, wallet quality, and bait flags into one signal.

``wallet_score`` is the mean ``tracked_wallets.win_rate`` of wallets that
bought the mint, scaled from a 0–1 fraction to 0–100. A stored win rate of
0 is a known result and keeps the full formula. If every buyer has a null
win rate, the wallet term is unknown: it is left empty and the risk and
bait weights are scaled to the same 0–100 range. Treating that case as 0
would cap a perfect scan at 50, which does not clear the threshold.

``bait_flag_penalty`` is the sum of confidences on ``bait_flags`` for that
mint. A composite equal to ``SIGNAL_THRESHOLD`` does not create a row. The
scan must be newer than ``SIGNAL_RISK_SCAN_MAX_AGE_MINUTES``. One signal
per token is enforced by ``uq_signals_token_id``.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Protocol

import structlog
from redis.asyncio import Redis
from sqlalchemy import exists, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import config
from app.adapters.ai_review import TokenReviewInput, estimate_review_tokens
from app.config import (
    AI_REVIEW_QUEUE,
    SIGNAL_BAIT_WEIGHT,
    SIGNAL_RISK_SCAN_MAX_AGE_MINUTES,
    SIGNAL_RISK_WEIGHT,
    SIGNAL_THRESHOLD,
    SIGNAL_WALLET_WEIGHT,
)
from app.db.models import BaitFlag, Signal, Token, TrackedWallet, WalletTx
from app.observability import bind_trace
from app.services.ai_budget import release_review, reserve_review, review_counts, tokens_used
from app.services.creator_cluster import load_creator_context
from app.services.risk_scanner import unavailable_fields
from app.services.token_store import AI_REVIEW_COMPLETED, AI_REVIEW_PENDING, AI_REVIEW_TRUNCATED

# A creator-cluster read that could have resolved and did not. Young-database
# history, the median-sell ledger limit, ``distribution_not_run``, and
# ``no_holder_balances`` are not in this set.
CREATOR_CLUSTER_DEGRADED_REASONS = frozenset(
    {
        "holder_funder_missing",
        "creator_funder_missing",
        "creator_missing",
        "holder_book_missing",
    }
)

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


def wallet_history_known(win_rates: list[Decimal | None]) -> bool:
    return any(rate is not None for rate in win_rates)


def creator_cluster_degraded_reason(context: dict[str, Any] | None) -> str | None:
    """Return a distribution reason that blocks a signal, or None.

    Only the distribution line is read. Price-drop history, LP history, and
    the constant median-sell line never block here.
    """
    if not context or context.get("distribution_status") != "unavailable":
        return None
    reason = context.get("distribution_reason")
    if reason in CREATOR_CLUSTER_DEGRADED_REASONS:
        return str(reason)
    return None


class AiReviewCandidate(Protocol):
    liquidity_usd: Decimal | None
    risk_score: Decimal | None
    bonding_curve_liquidity_sol: Decimal | None


def _review_liquidity_ok(token: AiReviewCandidate) -> bool:
    """Post-migration TVL when it is stored, otherwise the bonding-curve SOL snapshot.

    A null ``liquidity_usd`` means the Raydium pool is not there yet. It does
    not mean the curve is empty. The two thresholds are independent.
    """
    liquidity = token.liquidity_usd
    if liquidity is not None:
        return liquidity >= config.AI_REVIEW_MIN_LIQUIDITY
    curve = token.bonding_curve_liquidity_sol
    if curve is None:
        return False
    return curve >= config.AI_REVIEW_MIN_BONDING_CURVE_LIQUIDITY_SOL


def qualifies_for_ai_review(
    token: AiReviewCandidate,
    *,
    reviews_this_hour: int,
    reviews_today: int,
    tokens_today: int = 0,
    estimated_tokens: int = 0,
) -> bool:
    """True only inside the score band, with enough liquidity, and under every cap.

    The daily call cap is checked on its own. A quiet hour does not bypass it,
    and a day that still has call room does not bypass the hourly cap or the
    token cap. ``estimated_tokens`` is chars/4 of this review's prompt.
    """
    if not _review_liquidity_ok(token):
        return False
    score = token.risk_score
    if score is None or score < config.AI_REVIEW_SCORE_MIN or score > config.AI_REVIEW_SCORE_MAX:
        return False
    if reviews_this_hour >= config.AI_REVIEW_HOURLY_BUDGET:
        return False
    if reviews_today >= config.AI_REVIEW_DAILY_BUDGET:
        return False
    if tokens_today + max(0, estimated_tokens) > config.AI_REVIEW_DAILY_TOKEN_BUDGET:
        return False
    return True


def composite_without_wallet(risk_score: Decimal, bait_flag_penalty: Decimal) -> Decimal:
    """Risk and bait only, scaled so a perfect scan with no penalty is 100."""
    weight = SIGNAL_RISK_WEIGHT + SIGNAL_BAIT_WEIGHT
    raw = (risk_score * SIGNAL_RISK_WEIGHT - bait_flag_penalty * SIGNAL_BAIT_WEIGHT) / weight
    return raw.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


class SignalFusion:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        redis: Redis | None = None,
        *,
        reviews_enabled: bool = False,
        clock: Clock | None = None,
        stop: Any | None = None,
    ) -> None:
        self._sessions = sessions
        self._redis = redis
        self._reviews_enabled = reviews_enabled and redis is not None
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
        penalty = await session.scalar(
            select(func.coalesce(func.sum(BaitFlag.confidence), 0)).where(
                BaitFlag.token_id == token.id
            )
        )
        missing = unavailable_fields(token.risk_flags)
        if missing:
            with bind_trace(token.trace_id):
                logger.warning(
                    "signal_data_incomplete",
                    mint=token.mint,
                    missing=missing,
                    reason="scan_fields_unavailable",
                )
            return False
        risk = token.risk_score if token.risk_score is not None else Decimal(0)
        bait_penalty = Decimal(penalty or 0)
        wallet_score: Decimal | None
        if wallet_history_known(win_rates):
            wallet_score = wallet_score_from_win_rates(win_rates)
            score = composite_score(risk, wallet_score, bait_penalty)
        else:
            wallet_score = None
            score = composite_without_wallet(risk, bait_penalty)
        if not emits_signal(score):
            with bind_trace(token.trace_id):
                logger.info(
                    "signal_below_threshold",
                    mint=token.mint,
                    composite_score=str(score),
                    threshold=str(SIGNAL_THRESHOLD),
                )
            return False
        context = await load_creator_context(session, token)
        degraded = creator_cluster_degraded_reason(context)
        if degraded:
            with bind_trace(token.trace_id):
                logger.warning(
                    "creator_cluster_data_quality",
                    mint=token.mint,
                    flag="creator_cluster_data_quality:degraded",
                    reason=f"degraded: {degraded}",
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
                context=context,
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
                    wallet_score=str(wallet_score) if wallet_score is not None else None,
                    bait_flag_penalty=str(penalty or 0),
                )
            await self._enqueue_review(token, now, score)
        return inserted

    async def _enqueue_review(self, token: Token, now: datetime, score: Decimal) -> None:
        redis = self._redis
        if not self._reviews_enabled or redis is None:
            return
        if token.ai_review_status == AI_REVIEW_PENDING:
            return
        if _review_is_fresh(token, now):
            return
        hour, day = await review_counts(redis, now)
        spent_tokens = await tokens_used(redis, now)
        estimate = estimate_review_tokens(_review_input(token, score))
        if not qualifies_for_ai_review(
            token,
            reviews_this_hour=hour,
            reviews_today=day,
            tokens_today=spent_tokens,
            estimated_tokens=estimate,
        ):
            logger.info(
                "ai_review_skipped",
                mint=token.mint,
                reviews_this_hour=hour,
                reviews_today=day,
                tokens_today=spent_tokens,
                estimated_tokens=estimate,
                liquidity_usd=None if token.liquidity_usd is None else str(token.liquidity_usd),
                bonding_curve_liquidity_sol=(
                    None
                    if token.bonding_curve_liquidity_sol is None
                    else str(token.bonding_curve_liquidity_sol)
                ),
            )
            return
        if not await reserve_review(redis, now, tokens=estimate):
            logger.info("ai_review_budget_exhausted", mint=token.mint)
            return
        token.ai_review_status = AI_REVIEW_PENDING
        try:
            await redis.xadd(
                AI_REVIEW_QUEUE,
                {
                    "mint": token.mint,
                    "trace_id": token.trace_id or "",
                    "reserved_at": now.isoformat(),
                    "reserved_tokens": str(estimate),
                },
            )
        except Exception:
            await release_review(redis, now, tokens=estimate)
            token.ai_review_status = None
            raise
        logger.info("ai_review_enqueued", mint=token.mint, trace_id=token.trace_id)


def _review_input(token: Token, score: Decimal) -> TokenReviewInput:
    flags = token.risk_flags if isinstance(token.risk_flags, list) else []
    return TokenReviewInput(
        mint=token.mint,
        symbol=token.symbol,
        name=token.name,
        risk_score=None if token.risk_score is None else str(token.risk_score),
        risk_flags=tuple(str(flag) for flag in flags),
        liquidity_usd=None if token.liquidity_usd is None else str(token.liquidity_usd),
        composite_score=str(score),
    )


def _review_is_fresh(token: Token, now: datetime) -> bool:
    if token.ai_review_status == AI_REVIEW_TRUNCATED:
        return _review_clock_is_fresh(token, now)
    if token.ai_review_status != AI_REVIEW_COMPLETED or not token.ai_review:
        return False
    return _review_clock_is_fresh(token, now)


def _review_clock_is_fresh(token: Token, now: datetime) -> bool:
    reviewed = token.ai_review_at
    if reviewed is None:
        return False
    if reviewed.tzinfo is None:
        reviewed = reviewed.replace(tzinfo=timezone.utc)
    return reviewed >= now - timedelta(hours=config.AI_REVIEW_STALE_HOURS)
