"""Holder checks reach mints from the last hour before the older backlog."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

import pytest
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.pumpportal import NewTokenEvent
from app.workers.token_discovery import TokenDiscovery
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

CREATOR = "7mHmNh6BFNWpFp5qh2VXRNzsK63zZ5ZQGQY77khANBQW"
CURVE = "yDh3gJdZbFpmsLDEswWKr8BiNHZpdedZP5oeaN67hw8"
T0 = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now


class Holders:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]:
        del top_n
        self.calls.append(mint)
        return [CREATOR, CURVE]


def _event(mint: str) -> NewTokenEvent:
    return NewTokenEvent(
        mint=mint,
        creator_address=CREATOR,
        bonding_curve_key=CURVE,
        signature="sig",
        name="name",
        symbol="SYM",
        pool="pump",
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
    clock = Clock()
    try:
        yield sessions, redis, clock
    finally:
        await truncate_pipeline(engine)
        await redis.flushdb()
        await redis.aclose()
        await engine.dispose()


async def test_fresh_mints_are_checked_before_the_backlog(discovery_stack: tuple) -> None:
    sessions, redis, clock = discovery_stack
    holders = Holders()
    discovery = TokenDiscovery(sessions, redis, holders, clock=clock, pending_batch=2)
    now = T0 + timedelta(hours=3)

    clock.now = now - timedelta(hours=3)
    await discovery.ingest_create(_event("mint-old"))
    clock.now = now - timedelta(minutes=40)
    await discovery.ingest_create(_event("mint-mid"))
    clock.now = now - timedelta(minutes=10)
    await discovery.ingest_create(_event("mint-new"))
    clock.now = now

    first = await discovery.review_pending()
    assert first.checked == 2
    assert holders.calls == ["mint-new", "mint-mid"]

    second = await discovery.review_pending()
    assert second.checked == 1
    assert holders.calls == ["mint-new", "mint-mid", "mint-old"]
