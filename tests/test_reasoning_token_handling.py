"""A reply cut off at max_tokens is stored as truncated and stays off the card."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.ai_review import TokenReview, TokenReviewInput
from app.db.models import Token
from app.services.token_store import AI_REVIEW_TRUNCATED
from app.templates import alert_text
from app.workers.ai_review_worker import AiReviewWorker
from app.workers.alert_dispatcher import _completed_review
from app.workers.signal_fusion import SignalFusion
from tests.factories import make_token
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

NOW = datetime(2026, 9, 25, 17, 30, tzinfo=timezone.utc)
CUT_OFF = "Note that the risk"


class _CutOff:
    def __init__(self) -> None:
        self.calls = 0

    async def generate_token_review(self, token: TokenReviewInput) -> TokenReview:
        self.calls += 1
        assert token.mint
        return TokenReview(text=CUT_OFF, finish_reason="length", total_tokens=616)


async def test_length_finish_is_truncated_and_omitted_from_the_card(
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
                        mint="mint-cut",
                        symbol="ELIZABETH",
                        risk_score=Decimal("80"),
                        liquidity_usd=Decimal("12000"),
                        creator_address="creator-known",
                        updated_at=NOW,
                        risk_flags=["check_lp_lock"],
                    )
                )
        fusion = SignalFusion(sessions, redis, reviews_enabled=True, clock=lambda: NOW)
        await fusion.run_once()
        client = _CutOff()
        worker = AiReviewWorker(sessions, redis, client, clock=lambda: NOW)
        processed, failed = await worker.run_once()
        assert (processed, failed) == (1, 0)
        assert client.calls == 1
        async with sessions() as session:
            token = await session.scalar(select(Token).where(Token.mint == "mint-cut"))
        assert token is not None
        assert token.ai_review_status == AI_REVIEW_TRUNCATED
        assert token.ai_review is None
        card = alert_text(
            symbol="ELIZABETH",
            mint="mint-cut",
            composite_score=Decimal("57.14"),
            risk_score=Decimal("80"),
            risk_flags=["check_lp_lock"],
            bait_warning=False,
            ai_review=_completed_review(token),
        )
        assert CUT_OFF not in card
        assert "AI review" not in card
    finally:
        await truncate_pipeline(engine)
        await redis.flushdb()
        await redis.aclose()
        await engine.dispose()
