"""RPM 429 is retried. A daily token 429 is not, and it is not a generic failure."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import httpx
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.ai_review import (
    AiReviewClient,
    AiReviewError,
    TokenReviewInput,
    classify_rate_limit,
)
from app.db.models import Token
from app.services.ai_budget import review_counts, tokens_used
from app.services.token_store import AI_REVIEW_SKIPPED_BUDGET
from app.workers.ai_review_worker import AiReviewWorker
from app.workers.signal_fusion import SignalFusion
from tests.factories import make_token
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

NOW = datetime(2026, 9, 25, 16, 0, tzinfo=timezone.utc)
SAMPLE = TokenReviewInput(
    mint="mint",
    symbol="ELIZABETH",
    name="The Amazon Monkey",
    risk_score="80",
    risk_flags=("check_lp_lock",),
    liquidity_usd="12000",
    composite_score="57.14",
)
# Shape from Groq's error object (message + type) and from captured TPD text:
# "on tokens per day (TPD): Limit 100000, Used 99787, Requested 1616".
TPD_MESSAGE = (
    "Rate limit reached for model `openai/gpt-oss-120b` in organization `org_test` "
    "on tokens per day (TPD): Limit 100000, Used 99787, Requested 1616. "
    "Please try again in 20m12s."
)
RPM_MESSAGE = (
    "Rate limit reached for model `openai/gpt-oss-120b` in organization `org_test` "
    "on requests per minute (RPM): Limit 30, Used 30, Requested 1. "
    "Please try again in 2s."
)


def test_daily_remainder_is_taken_from_the_message_not_the_minute_header() -> None:
    response = httpx.Response(
        429,
        json={
            "error": {
                "message": (
                    "Rate limit reached for model `openai/gpt-oss-120b` "
                    "on tokens per day (TPD): Limit 100,000, Used 99,787, Requested 1,616."
                ),
                "type": "tokens",
            }
        },
        headers={
            "retry-after": "72000",
            "x-ratelimit-remaining-tokens": "17997",
            "x-ratelimit-remaining-requests": "900",
        },
    )
    quota = classify_rate_limit(response)
    assert quota.kind == "tpd"
    assert quota.retryable is False
    assert quota.limit == 100_000
    assert quota.used == 99_787
    assert quota.remaining == 213
    assert quota.remaining != 17_997


async def test_rpm_429_retries_with_backoff_and_tpd_429_does_not() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                429,
                json={"error": {"message": RPM_MESSAGE, "type": "requests"}},
                headers={"retry-after": "2", "x-ratelimit-remaining-tokens": "17997"},
            )
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": "Short note."}}],
                "usage": {"prompt_tokens": 120, "completion_tokens": 20, "total_tokens": 140},
            },
        )

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = AiReviewClient(provider="groq", api_key="gsk-test", client=http, sleep=sleep)
        review = await client.generate_token_review(SAMPLE)
    assert review.text == "Short note."
    assert review.total_tokens == 140
    assert calls["n"] == 2
    assert sleeps == [2.0]

    tpd_calls = {"n": 0}

    def tpd_handler(request: httpx.Request) -> httpx.Response:
        del request
        tpd_calls["n"] += 1
        return httpx.Response(
            429,
            json={"error": {"message": TPD_MESSAGE, "type": "tokens"}},
            headers={"retry-after": "72000", "x-ratelimit-remaining-tokens": "17997"},
        )

    tpd_sleeps: list[float] = []

    async def tpd_sleep(delay: float) -> None:
        tpd_sleeps.append(delay)

    transport = httpx.MockTransport(tpd_handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = AiReviewClient(provider="groq", api_key="gsk-test", client=http, sleep=tpd_sleep)
        try:
            await client.generate_token_review(SAMPLE)
        except AiReviewError as exc:
            assert exc.retryable is False
            assert exc.budget_exceeded is True
            assert exc.limit_kind == "tpd"
            assert exc.quota_remaining == 213
            assert exc.quota_used == 99_787
        else:
            raise AssertionError("daily token limit was retried or ignored")
    assert tpd_calls["n"] == 1
    assert tpd_sleeps == []


class _BudgetStop:
    async def generate_token_review(self, token: TokenReviewInput):
        del token
        raise AiReviewError(
            "daily quota is exhausted",
            status_code=429,
            retryable=False,
            budget_exceeded=True,
            limit_kind="tpd",
            quota_limit=100_000,
            quota_used=99_787,
            quota_remaining=213,
        )


async def test_tpd_429_marks_skipped_budget_exceeded(pipeline_infra: object) -> None:
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
                        risk_score=Decimal("80"),
                        liquidity_usd=Decimal("12000"),
                        updated_at=NOW,
                    )
                )
        fusion = SignalFusion(sessions, redis, reviews_enabled=True, clock=lambda: NOW)
        await fusion.run_once()
        worker = AiReviewWorker(sessions, redis, _BudgetStop(), clock=lambda: NOW)
        processed, failed = await worker.run_once()
        assert (processed, failed) == (1, 0)
        async with sessions() as session:
            token = await session.scalar(select(Token).where(Token.mint == "mint-review"))
        assert token is not None
        assert token.ai_review_status == AI_REVIEW_SKIPPED_BUDGET
        assert await tokens_used(redis, NOW) == 99_787
        hour, day = await review_counts(redis, NOW)
        assert (hour, day) == (0, 0)
    finally:
        await truncate_pipeline(engine)
        await redis.flushdb()
        await redis.aclose()
        await engine.dispose()
