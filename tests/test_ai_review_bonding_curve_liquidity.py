"""A null Raydium TVL does not hide a pre-migration mint that has curve SOL."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.pumpportal import NewTokenEvent
from app.config import AI_REVIEW_MIN_BONDING_CURVE_LIQUIDITY_SOL, AI_REVIEW_MIN_LIQUIDITY
from app.db.models import Token
from app.workers.signal_fusion import qualifies_for_ai_review
from app.workers.token_discovery import TokenDiscovery
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

CREATOR = "7mHmNh6BFNWpFp5qh2VXRNzsK63zZ5ZQGQY77khANBQW"
CURVE = "yDh3gJdZbFpmsLDEswWKr8BiNHZpdedZP5oeaN67hw8"
MINT = "CMdp4kwo65eeNV9RP4NWGf7ZRXeZnqeszVfJQV2Tpump"
T0 = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)


class _Token:
    def __init__(
        self,
        liquidity: Decimal | None,
        curve: Decimal | None,
        score: Decimal | None = Decimal("50"),
    ) -> None:
        self.liquidity_usd = liquidity
        self.bonding_curve_liquidity_sol = curve
        self.risk_score = score


def test_curve_sol_passes_when_usd_liquidity_is_missing() -> None:
    assert AI_REVIEW_MIN_BONDING_CURVE_LIQUIDITY_SOL == Decimal("1")
    assert AI_REVIEW_MIN_LIQUIDITY == Decimal("5000")
    curve = Decimal("30.097774556784593")
    assert (
        qualifies_for_ai_review(
            _Token(None, curve),
            reviews_this_hour=0,
            reviews_today=0,
        )
        is True
    )
    assert (
        qualifies_for_ai_review(
            _Token(None, AI_REVIEW_MIN_BONDING_CURVE_LIQUIDITY_SOL),
            reviews_this_hour=0,
            reviews_today=0,
        )
        is True
    )
    assert (
        qualifies_for_ai_review(
            _Token(None, AI_REVIEW_MIN_BONDING_CURVE_LIQUIDITY_SOL - Decimal("0.01")),
            reviews_this_hour=0,
            reviews_today=0,
        )
        is False
    )
    assert (
        qualifies_for_ai_review(
            _Token(None, None),
            reviews_this_hour=0,
            reviews_today=0,
        )
        is False
    )


def test_usd_liquidity_stays_the_post_migration_gate() -> None:
    curve = Decimal("30")
    assert (
        qualifies_for_ai_review(
            _Token(Decimal("4999"), curve),
            reviews_this_hour=0,
            reviews_today=0,
        )
        is False
    )
    assert (
        qualifies_for_ai_review(
            _Token(Decimal("5000"), None),
            reviews_this_hour=0,
            reviews_today=0,
        )
        is True
    )


@pytest.fixture
async def discovery_stack(pipeline_infra: object) -> AsyncIterator[tuple]:
    database_url = pipeline_infra.database_url  # type: ignore[attr-defined]
    redis_url = pipeline_infra.redis_url  # type: ignore[attr-defined]
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    redis = Redis.from_url(redis_url, decode_responses=True)
    await truncate_pipeline(engine)
    await redis.flushdb()
    try:
        yield sessions, redis
    finally:
        await truncate_pipeline(engine)
        await redis.flushdb()
        await redis.aclose()
        await engine.dispose()


async def test_create_stores_curve_sol_apart_from_liquidity_usd(discovery_stack: tuple) -> None:
    sessions, redis = discovery_stack
    discovery = TokenDiscovery(sessions, redis, _Holders(), clock=lambda: T0)
    created = await discovery.ingest_create(
        NewTokenEvent(
            mint=MINT,
            creator_address=CREATOR,
            bonding_curve_key=CURVE,
            signature="sig",
            name="The Amazon Monkey",
            symbol="ELIZABETH",
            pool="pump",
            bonding_curve_liquidity_sol=Decimal("30.097774556784593"),
        )
    )
    assert created is True
    async with sessions() as session:
        token = await session.scalar(select(Token).where(Token.mint == MINT))
    assert token is not None
    assert token.liquidity_usd is None
    assert token.bonding_curve_liquidity_sol == Decimal("30.097774556784593")


class _Holders:
    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]:
        del mint, top_n
        return []
