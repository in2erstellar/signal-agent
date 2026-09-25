"""A migration event clears skipped_pre_migration and rebuilds the graph."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.adapters.pumpportal import MigrationEvent, NewTokenEvent
from app.config import RISK_SCAN_QUEUE
from app.db.models import Token
from app.services.risk_scanner import CheckResult, RiskReport
from app.services.token_store import (
    FUNDER_GRAPH_BUILT,
    FUNDER_GRAPH_COMPLETED_POST,
    FUNDER_GRAPH_PENDING_POST,
    FUNDER_GRAPH_SKIPPED,
    insert_token,
)
from app.workers.risk_scan_worker import RiskScanWorker
from app.workers.token_discovery import TokenDiscovery
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

CREATOR = "7mHmNh6BFNWpFp5qh2VXRNzsK63zZ5ZQGQY77khANBQW"
CURVE = "yDh3gJdZbFpmsLDEswWKr8BiNHZpdedZP5oeaN67hw8"
BUYER = "Buyer1111111111111111111111111111111111111"
MINT = "CMdp4kwo65eeNV9RP4NWGf7ZRXeZnqeszVfJQV2Tpump"
T0 = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)


class _Holders:
    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]:
        del mint, top_n
        return [CREATOR, CURVE, BUYER]


class _Graph:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def build_graph_for_token(self, token_mint: str, top_n_holders: int = 20) -> list[object]:
        del top_n_holders
        self.calls.append(token_mint)
        return []


class _Scanner:
    async def scan_token(self, mint: str) -> RiskReport:
        return RiskReport(
            mint=mint,
            risk_score=40,
            checks=[CheckResult(name="check_mint_authority", passed=True, detail="renounced")],
            scanned_at=T0,
            liquidity_usd=Decimal("9000"),
        )


def _create(mint: str = MINT) -> NewTokenEvent:
    return NewTokenEvent(
        mint=mint,
        creator_address=CREATOR,
        bonding_curve_key=CURVE,
        signature="sig",
        name="The Amazon Monkey",
        symbol="ELIZABETH",
        pool="pump",
    )


def _migration(mint: str = MINT) -> MigrationEvent:
    return MigrationEvent(mint=mint, signature="mig-sig", pool="pump-amm", tx_type="migrate")


@pytest.fixture
async def stack(pipeline_infra: object) -> AsyncIterator[tuple]:
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


async def _token(sessions: async_sessionmaker[AsyncSession], mint: str = MINT) -> Token | None:
    async with sessions() as session:
        return await session.scalar(select(Token).where(Token.mint == mint))


async def test_migration_clears_skip_and_requeues_once(stack: tuple) -> None:
    sessions, redis = stack
    discovery = TokenDiscovery(sessions, redis, _Holders(), clock=lambda: T0)
    await discovery.ingest_create(_create())
    assert (await _token(sessions)).funder_graph_status == FUNDER_GRAPH_SKIPPED  # type: ignore[union-attr]

    assert await discovery.handle_migration(_migration()) is True
    token = await _token(sessions)
    assert token is not None
    assert token.funder_graph_status == FUNDER_GRAPH_PENDING_POST
    assert await redis.xlen(RISK_SCAN_QUEUE) == 1
    fields = (await redis.xrange(RISK_SCAN_QUEUE))[0][1]
    assert fields["mint"] == MINT
    assert fields["reason"] == "post_migration"
    assert fields["trace_id"]

    assert await discovery.handle_migration(_migration()) is False
    assert await redis.xlen(RISK_SCAN_QUEUE) == 1
    assert await discovery.handle_migration(_migration("missing-mint")) is False
    assert await redis.xlen(RISK_SCAN_QUEUE) == 1

    async with sessions() as session:
        async with session.begin():
            await insert_token(session, mint="wallet-mint", first_seen_at=T0)
            row = await session.scalar(select(Token).where(Token.mint == "wallet-mint"))
            assert row is not None
            row.funder_graph_status = FUNDER_GRAPH_BUILT
    assert await discovery.handle_migration(_migration("wallet-mint")) is False
    assert await redis.xlen(RISK_SCAN_QUEUE) == 1


async def test_post_migration_scan_builds_a_fresh_graph(stack: tuple) -> None:
    sessions, redis = stack
    discovery = TokenDiscovery(sessions, redis, _Holders(), clock=lambda: T0)
    await discovery.ingest_create(_create())
    assert (await discovery.review_pending()).qualified == 1
    graph = _Graph()
    worker = RiskScanWorker(
        sessions,
        redis,
        lambda _session: graph,
        lambda _session, _clusters: _Scanner(),
    )
    processed, failed = await worker.run_once()
    assert (processed, failed) == (1, 0)
    assert graph.calls == []
    assert (await _token(sessions)).funder_graph_status == FUNDER_GRAPH_SKIPPED  # type: ignore[union-attr]

    assert await discovery.handle_migration(_migration()) is True
    processed, failed = await worker.run_once()
    assert (processed, failed) == (1, 0)
    assert graph.calls == [MINT]
    token = await _token(sessions)
    assert token is not None
    assert token.funder_graph_status == FUNDER_GRAPH_COMPLETED_POST
    assert token.liquidity_usd == Decimal("9000")
