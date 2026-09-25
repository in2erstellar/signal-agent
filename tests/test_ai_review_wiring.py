"""Signals that pass the review gate are the only ones placed on ai_review_queue."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from html import escape

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.ai_review import AiReviewError, TokenReview, TokenReviewInput
from app.config import AI_REVIEW_DAILY_BUDGET, AI_REVIEW_QUEUE
from app.db.models import Signal, Token
from app.services.ai_budget import day_key, review_counts
from app.services.token_store import AI_REVIEW_COMPLETED, AI_REVIEW_FAILED, AI_REVIEW_PENDING
from app.templates import DISCLAIMER, alert_text
from app.workers.ai_review_worker import AiReviewWorker
from app.workers.signal_fusion import SignalFusion
from tests.factories import make_token
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

NOW = datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc)


class _Review:
    def __init__(self, text: str = "Pool TVL is 12000 USD. Risk score is 80.") -> None:
        self.text = text
        self.calls = 0

    async def generate_token_review(self, token: TokenReviewInput) -> TokenReview:
        self.calls += 1
        assert token.mint
        return TokenReview(text=self.text)


class _Permanent:
    async def generate_token_review(self, token: TokenReviewInput) -> TokenReview:
        del token
        raise AiReviewError("rejected", status_code=400, retryable=False)


def test_alert_text_puts_an_escaped_ai_block_before_the_disclaimer() -> None:
    review = "TVL < 9000 & still listed"
    text = alert_text(
        symbol="ELIZABETH",
        mint="CMdp4kwo65eeNV9RP4NWGf7ZRXeZnqeszVfJQV2Tpump",
        composite_score=Decimal("57.14"),
        risk_score=Decimal("80"),
        risk_flags=["check_lp_lock"],
        bait_warning=False,
        ai_review=review,
    )
    assert "<b>AI review</b>" in text
    assert escape(review) in text
    bold = alert_text(
        symbol="ASI",
        mint="AJzWtzVh77uQxK1aLPt4STMtFnHVQKNkNPRA4ebfpump",
        composite_score=Decimal("53.57"),
        risk_score=Decimal("75"),
        risk_flags=["check_lp_lock"],
        bait_warning=False,
        ai_review="**Token:** ASI <test>\n**Mint:** AJzW  ",
    )
    assert "<b>Token:</b> ASI &lt;test&gt;" in bold
    assert "**" not in bold
    assert text.endswith(DISCLAIMER)
    assert text.index("<b>AI review</b>") < text.index(DISCLAIMER)
    quiet = alert_text(
        symbol="ELIZABETH",
        mint="CMdp4kwo65eeNV9RP4NWGf7ZRXeZnqeszVfJQV2Tpump",
        composite_score=Decimal("57.14"),
        risk_score=Decimal("80"),
        risk_flags=["check_lp_lock"],
        bait_warning=False,
    )
    assert "AI review" not in quiet


async def test_only_a_qualifying_signal_is_enqueued(pipeline_infra: object) -> None:
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    redis = Redis.from_url(pipeline_infra.redis_url, decode_responses=True)  # type: ignore[attr-defined]
    await truncate_pipeline(engine)
    await redis.flushdb()
    try:
        async with sessions() as session:
            async with session.begin():
                session.add_all(
                    [
                        make_token(
                            mint="mint-review",
                            risk_score=Decimal("80"),
                            liquidity_usd=Decimal("12000"),
                            updated_at=NOW,
                        ),
                        make_token(
                            mint="mint-thin",
                            risk_score=Decimal("80"),
                            liquidity_usd=Decimal("1000"),
                            updated_at=NOW,
                        ),
                        make_token(
                            mint="mint-high",
                            risk_score=Decimal("100"),
                            liquidity_usd=Decimal("12000"),
                            updated_at=NOW,
                        ),
                    ]
                )
        fusion = SignalFusion(sessions, redis, reviews_enabled=True, clock=lambda: NOW)
        processed, failed, created = await fusion.run_once()
        assert (processed, failed, created) == (3, 0, 3)
        assert await redis.xlen(AI_REVIEW_QUEUE) == 1
        fields = (await redis.xrange(AI_REVIEW_QUEUE))[0][1]
        assert fields["mint"] == "mint-review"
        hour, day = await review_counts(redis, NOW)
        assert (hour, day) == (1, 1)

        async with sessions() as session:
            review = await session.scalar(select(Token).where(Token.mint == "mint-review"))
            thin = await session.scalar(select(Token).where(Token.mint == "mint-thin"))
            high = await session.scalar(select(Token).where(Token.mint == "mint-high"))
            assert review is not None and review.ai_review_status == AI_REVIEW_PENDING
            assert thin is not None and thin.ai_review_status is None
            assert high is not None and high.ai_review_status is None
            assert await session.scalar(select(Signal).where(Signal.token_id == thin.id)) is not None
            assert await session.scalar(select(Signal).where(Signal.token_id == high.id)) is not None

        await redis.set(day_key(NOW), AI_REVIEW_DAILY_BUDGET)
        async with sessions() as session:
            async with session.begin():
                session.add(
                    make_token(
                        mint="mint-capped",
                        risk_score=Decimal("80"),
                        liquidity_usd=Decimal("12000"),
                        updated_at=NOW,
                    )
                )
        _processed, _failed, created_capped = await fusion.run_once()
        assert created_capped == 1
        assert await redis.xlen(AI_REVIEW_QUEUE) == 1
    finally:
        await truncate_pipeline(engine)
        await redis.flushdb()
        await redis.aclose()
        await engine.dispose()


async def test_worker_stores_the_review_and_refunds_a_permanent_failure(
    pipeline_infra: object,
) -> None:
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    redis = Redis.from_url(pipeline_infra.redis_url, decode_responses=True)  # type: ignore[attr-defined]
    await truncate_pipeline(engine)
    await redis.flushdb()
    try:
        async with sessions() as session:
            async with session.begin():
                session.add(
                    make_token(
                        mint="mint-review",
                        symbol="ELIZABETH",
                        risk_score=Decimal("80"),
                        liquidity_usd=Decimal("12000"),
                        updated_at=NOW,
                        risk_flags=["check_lp_lock"],
                    )
                )
        fusion = SignalFusion(sessions, redis, reviews_enabled=True, clock=lambda: NOW)
        await fusion.run_once()
        client = _Review("Noted. <do not buy>")
        worker = AiReviewWorker(sessions, redis, client, clock=lambda: NOW)
        processed, failed = await worker.run_once()
        assert (processed, failed) == (1, 0)
        assert client.calls == 1
        async with sessions() as session:
            token = await session.scalar(select(Token).where(Token.mint == "mint-review"))
        assert token is not None
        assert token.ai_review_status == AI_REVIEW_COMPLETED
        assert token.ai_review == "Noted. <do not buy>"
        assert token.ai_review_at == NOW
        processed, failed = await worker.run_once()
        assert (processed, failed) == (0, 0)
        assert client.calls == 1

        async with sessions() as session:
            async with session.begin():
                session.add(
                    make_token(
                        mint="mint-bad",
                        risk_score=Decimal("80"),
                        liquidity_usd=Decimal("12000"),
                        updated_at=NOW + timedelta(seconds=1),
                    )
                )
        later = NOW + timedelta(seconds=1)
        fusion_later = SignalFusion(sessions, redis, reviews_enabled=True, clock=lambda: later)
        await fusion_later.run_once()
        assert (await review_counts(redis, later))[1] == 2
        bad = AiReviewWorker(sessions, redis, _Permanent(), clock=lambda: later, consumer_name="ai-bad")
        processed, failed = await bad.run_once()
        assert (processed, failed) == (1, 0)
        async with sessions() as session:
            token = await session.scalar(select(Token).where(Token.mint == "mint-bad"))
        assert token is not None and token.ai_review_status == AI_REVIEW_FAILED
        assert (await review_counts(redis, later))[1] == 1
    finally:
        await truncate_pipeline(engine)
        await redis.flushdb()
        await redis.aclose()
        await engine.dispose()
