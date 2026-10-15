"""Pending Pump.fun creates qualify only after a non-creator holder appears."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

import pytest
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.adapters.pumpportal import NewTokenEvent
from app.config import RISK_SCAN_QUEUE
from app.db.models import Token
from app.services.risk_scanner import CheckResult, RiskReport
from app.services.token_store import (
    DISCOVERY_DEAD,
    DISCOVERY_PENDING,
    DISCOVERY_QUALIFIED,
    FUNDER_GRAPH_BUILT,
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
# 10_400 creates/day is about 433 in one hour. The foreign-buy count below is
# injected, not measured from chain holders.
SYNTHETIC_HOUR_CREATES = 433
SYNTHETIC_HOUR_FOREIGN = 4


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now


class Holders:
    def __init__(self, owners: dict[str, list[str]]) -> None:
        self.owners = owners
        self.calls: list[str] = []

    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]:
        assert top_n == 20
        self.calls.append(mint)
        return list(self.owners.get(mint, [CREATOR, CURVE]))


def _event(mint: str = MINT) -> NewTokenEvent:
    return NewTokenEvent(
        mint=mint,
        creator_address=CREATOR,
        bonding_curve_key=CURVE,
        signature="sig",
        name="The Amazon Monkey",
        symbol="ELIZABETH",
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


async def _token(sessions: async_sessionmaker[AsyncSession], mint: str = MINT) -> Token | None:
    async with sessions() as session:
        return await session.scalar(select(Token).where(Token.mint == mint))


async def test_create_stays_pending_until_a_non_creator_holder(
    discovery_stack: tuple,
) -> None:
    sessions, redis, clock = discovery_stack
    holders = Holders({MINT: [CREATOR, CURVE]})
    discovery = TokenDiscovery(sessions, redis, holders, clock=clock)

    assert await discovery.ingest_create(_event()) is True
    assert await discovery.ingest_create(_event()) is False
    assert await redis.xlen(RISK_SCAN_QUEUE) == 0
    token = await _token(sessions)
    assert token is not None
    assert token.discovery_status == DISCOVERY_PENDING
    assert token.symbol == "ELIZABETH"
    assert token.creator_address == CREATOR
    assert token.bonding_curve_key == CURVE
    assert token.first_seen_at == T0
    assert token.funder_graph_status == FUNDER_GRAPH_SKIPPED

    quiet = await discovery.review_pending()
    assert quiet.qualified == 0
    assert quiet.dead == 0
    assert await redis.xlen(RISK_SCAN_QUEUE) == 0
    assert (await _token(sessions)).discovery_status == DISCOVERY_PENDING  # type: ignore[union-attr]

    holders.owners[MINT] = [CREATOR, CURVE, BUYER]
    qualified = await discovery.review_pending()
    assert qualified.qualified == 1
    token = await _token(sessions)
    assert token is not None
    assert token.discovery_status == DISCOVERY_QUALIFIED
    assert token.funder_graph_status == FUNDER_GRAPH_SKIPPED
    assert await redis.xlen(RISK_SCAN_QUEUE) == 1
    messages = await redis.xrange(RISK_SCAN_QUEUE)
    assert messages[0][1]["mint"] == MINT
    assert messages[0][1]["trace_id"]

    again = await discovery.review_pending()
    assert again.qualified == 0
    assert await redis.xlen(RISK_SCAN_QUEUE) == 1


async def test_pending_older_than_24_hours_is_dead_and_not_queued(
    discovery_stack: tuple,
) -> None:
    sessions, redis, clock = discovery_stack
    holders = Holders({MINT: [BUYER]})
    discovery = TokenDiscovery(sessions, redis, holders, clock=clock)
    await discovery.ingest_create(_event())

    clock.now = T0 + timedelta(hours=24) - timedelta(seconds=1)
    still = await discovery.review_pending()
    assert still.dead == 0
    assert still.qualified == 1

    await redis.flushdb()
    clock.now = T0
    await discovery.ingest_create(_event("mint-aged"))
    clock.now = T0 + timedelta(hours=24)
    aged = await discovery.review_pending()
    assert aged.dead == 1
    assert aged.qualified == 0
    assert await redis.xlen(RISK_SCAN_QUEUE) == 0
    token = await _token(sessions, "mint-aged")
    assert token is not None
    assert token.discovery_status == DISCOVERY_DEAD
    async with sessions() as session:
        count = await session.scalar(select(func.count()).select_from(Token))
    assert count == 2


async def test_synthetic_hour_qualifies_only_the_injected_second_buyers(
    discovery_stack: tuple,
) -> None:
    sessions, redis, clock = discovery_stack
    foreign = {f"mint-{index:04d}" for index in range(SYNTHETIC_HOUR_FOREIGN)}
    owners = {
        mint: [CREATOR, CURVE, BUYER] if mint in foreign else [CREATOR, CURVE]
        for mint in (f"mint-{index:04d}" for index in range(SYNTHETIC_HOUR_CREATES))
    }
    discovery = TokenDiscovery(
        sessions,
        redis,
        Holders(owners),
        clock=clock,
        pending_batch=SYNTHETIC_HOUR_CREATES,
    )
    for index in range(SYNTHETIC_HOUR_CREATES):
        await discovery.ingest_create(_event(f"mint-{index:04d}"))

    result = await discovery.review_pending()
    assert result.qualified == SYNTHETIC_HOUR_FOREIGN
    assert result.dead == 0
    assert result.checked == SYNTHETIC_HOUR_CREATES
    assert await redis.xlen(RISK_SCAN_QUEUE) == SYNTHETIC_HOUR_FOREIGN
    async with sessions() as session:
        pending = await session.scalar(
            select(func.count()).select_from(Token).where(Token.discovery_status == DISCOVERY_PENDING)
        )
        qualified = await session.scalar(
            select(func.count()).select_from(Token).where(Token.discovery_status == DISCOVERY_QUALIFIED)
        )
    assert pending == SYNTHETIC_HOUR_CREATES - SYNTHETIC_HOUR_FOREIGN
    assert qualified == SYNTHETIC_HOUR_FOREIGN


class _Scanner:
    async def scan_fast(self, mint: str) -> RiskReport:
        return RiskReport(
            mint=mint,
            risk_score=70,
            checks=[
                CheckResult(name="check_mint_authority", passed=True, detail="renounced"),
                CheckResult(name="check_freeze_authority", passed=True, detail="renounced"),
                CheckResult(name="check_lp_lock", passed=True, detail="locked"),
            ],
            scanned_at=T0,
            pool_status="no_pool",
        )

    async def scan_heavy(self, mint: str) -> RiskReport:
        return RiskReport(
            mint=mint,
            risk_score=30,
            checks=[
                CheckResult(name="check_holder_concentration", passed=True, detail="spread"),
                CheckResult(name="check_funder_cluster_exposure", passed=True, detail="none"),
            ],
            scanned_at=T0,
        )


async def test_pre_migration_scan_skips_funder_graph_and_wallet_rows_do_not(
    discovery_stack: tuple,
) -> None:
    sessions, redis, clock = discovery_stack
    discovery = TokenDiscovery(sessions, redis, Holders({MINT: [BUYER]}), clock=clock)
    await discovery.ingest_create(_event())
    await discovery.review_pending()

    calls: list[str] = []

    class _Boom:
        async def build_graph_for_token(self, token_mint: str, top_n_holders: int = 20) -> list[object]:
            del top_n_holders
            calls.append(token_mint)
            raise AssertionError("pre-migration token must not build a funder graph")

    worker = RiskScanWorker(
        sessions,
        redis,
        lambda _session: _Boom(),
        lambda _session, clusters: _Scanner(),
    )
    processed, failed = await worker.run_once()
    assert (processed, failed) == (1, 0)
    assert calls == []
    token = await _token(sessions)
    assert token is not None
    assert token.risk_score is None
    assert token.funder_graph_status == FUNDER_GRAPH_SKIPPED
    processed, failed = await worker.run_heavy_once()
    assert (processed, failed) == (1, 0)
    assert calls == []
    token = await _token(sessions)
    assert token is not None
    assert token.risk_score is not None
    assert token.funder_graph_status == FUNDER_GRAPH_SKIPPED

    async with sessions() as session:
        async with session.begin():
            await insert_token(session, mint="wallet-mint", first_seen_at=T0)
    await redis.xadd(RISK_SCAN_QUEUE, {"mint": "wallet-mint", "trace_id": "trace-wallet"})

    class _Recorder:
        async def build_graph_for_token(self, token_mint: str, top_n_holders: int = 20) -> list[object]:
            del top_n_holders
            calls.append(token_mint)
            return []

    worker = RiskScanWorker(
        sessions,
        redis,
        lambda _session: _Recorder(),
        lambda _session, clusters: _Scanner(),
    )
    processed, failed = await worker.run_once()
    assert (processed, failed) == (1, 0)
    assert calls == []
    processed, failed = await worker.run_heavy_once()
    assert (processed, failed) == (1, 0)
    assert calls == ["wallet-mint"]
    wallet = await _token(sessions, "wallet-mint")
    assert wallet is not None
    assert wallet.funder_graph_status == FUNDER_GRAPH_BUILT
    assert wallet.discovery_status is None


async def test_post_migration_only_mode_never_polls_holders(discovery_stack: tuple) -> None:
    sessions, redis, clock = discovery_stack
    holders = Holders({MINT: [CREATOR, CURVE, BUYER]})
    discovery = TokenDiscovery(
        sessions, redis, holders, clock=clock, qualify_pre_migration=False
    )
    await discovery.ingest_create(_event())

    counts = await discovery.review_pending()

    assert counts.checked == 0
    assert counts.qualified == 0
    assert holders.calls == []
    assert await redis.xlen(RISK_SCAN_QUEUE) == 0
    async with sessions() as session:
        token = await session.scalar(select(Token).where(Token.mint == MINT))
    assert token is not None
    assert token.discovery_status == DISCOVERY_PENDING


async def test_post_migration_only_mode_still_marks_old_creates_dead(
    discovery_stack: tuple,
) -> None:
    sessions, redis, clock = discovery_stack
    discovery = TokenDiscovery(
        sessions, redis, Holders({}), clock=clock, qualify_pre_migration=False
    )
    await discovery.ingest_create(_event())
    clock.now = T0 + timedelta(hours=25)

    counts = await discovery.review_pending()

    assert counts.dead == 1
