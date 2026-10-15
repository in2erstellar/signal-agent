"""Fast risk ack, then the heavy queue. Fusion still waits for a score."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.adapters.token_security import HolderBook, LpBook, LpHolderRow, MintView
from app.config import (
    POST_MIGRATION_RETRY_DELAYS_SECONDS,
    RISK_CHECK_WEIGHTS,
    RISK_HEAVY_GROUP,
    RISK_HEAVY_QUEUE,
    RISK_SCAN_GROUP,
    RISK_SCAN_PRIORITY_GROUP,
    RISK_SCAN_PRIORITY_QUEUE,
    RISK_SCAN_QUEUE,
)
from app.db.models import Signal, Token
from app.services.funder_graph import FunderLookupFailure
from app.services.risk_scanner import RiskScanner
from app.services.token_store import (
    FUNDER_GRAPH_PENDING_POST,
    FUNDER_GRAPH_SKIPPED,
    HEAVY_SCAN_COMPLETE,
    HEAVY_SCAN_FAILED,
    HEAVY_SCAN_PENDING,
    HEAVY_SCAN_SKIPPED,
    insert_token,
)
from app.workers.risk_scan_worker import PrebuiltClusters, RiskScanWorker
from app.workers.signal_fusion import SignalFusion
from tests.factories import make_tracked_wallet, make_wallet_funder, make_wallet_tx
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

MINT = "SplitMint111111111111111111111111111111111"
CREATOR = "Creator11111111111111111111111111111111111"
BURN = "1nc1nerator11111111111111111111111111111111"
T0 = datetime(2026, 10, 6, 8, 0, tzinfo=timezone.utc)
LATER = T0 + timedelta(hours=3)


class _Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class _Facts:
    def __init__(self) -> None:
        self.holder_calls = 0
        self.lp_calls = 0

    async def mint_view(self, mint: str) -> MintView:
        del mint
        return MintView(None, None, 1_000_000)

    async def holder_book(self, mint: str) -> HolderBook:
        del mint
        self.holder_calls += 1
        return HolderBook(1_000_000, [])

    async def lp_book(self, mint: str) -> LpBook:
        del mint
        self.lp_calls += 1
        return LpBook(
            "lp-mint",
            1_000,
            [LpHolderRow(BURN, 1_000, is_burn_address=True, is_lock_program=False)],
            liquidity_usd=Decimal("12000"),
            price_usd=Decimal("0.01"),
        )


class _Graph:
    def __init__(self, failure: FunderLookupFailure | None = None) -> None:
        self.failure = failure
        self.calls: list[str] = []

    async def build_graph_for_token(self, token_mint: str, top_n_holders: int = 20) -> list[object]:
        del top_n_holders
        self.calls.append(token_mint)
        if self.failure is not None:
            raise self.failure
        return []

    async def root_for_wallet(self, wallet: str) -> str | None:
        del wallet
        return "root-funder"


class _BoomHeavy:
    """Redis that refuses the heavy enqueue and forwards every other command."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    def __getattr__(self, name: str) -> object:
        return getattr(self._redis, name)

    async def xadd(self, name: str, fields: dict[str, str]) -> str:
        if name == RISK_HEAVY_QUEUE:
            raise ConnectionError("heavy enqueue failed")
        return await self._redis.xadd(name, fields)


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
        yield sessions, redis, engine
    finally:
        await truncate_pipeline(engine)
        await redis.flushdb()
        await redis.aclose()
        await engine.dispose()


def _worker(
    sessions: async_sessionmaker[AsyncSession],
    redis: Redis,
    facts: _Facts,
    graph: _Graph,
    clock: _Clock,
) -> RiskScanWorker:
    def scanner_factory(_session: object, clusters: list[object]) -> RiskScanner:
        return RiskScanner(
            facts,
            PrebuiltClusters(clusters),  # type: ignore[arg-type]
            clock=clock,
        )

    return RiskScanWorker(sessions, redis, lambda _session: graph, scanner_factory)


async def _token(sessions: async_sessionmaker[AsyncSession]) -> Token:
    async with sessions() as session:
        token = await session.scalar(select(Token).where(Token.mint == MINT))
    assert token is not None
    return token


async def _seed(
    sessions: async_sessionmaker[AsyncSession], redis: Redis, **overrides: object
) -> None:
    async with sessions() as session:
        async with session.begin():
            await insert_token(
                session,
                mint=MINT,
                first_seen_at=T0,
                creator_address=CREATOR,
                funder_graph_status=overrides.get("funder_graph_status"),  # type: ignore[arg-type]
            )
            token = await session.scalar(select(Token).where(Token.mint == MINT))
            assert token is not None
            session.add(
                make_wallet_funder(
                    wallet_address=CREATOR, funder_address="root-funder", hop_depth=1
                )
            )
    await redis.xadd(RISK_SCAN_QUEUE, {"mint": MINT, "trace_id": "trace-split"})


async def test_fast_scan_acks_only_after_the_heavy_message_is_stored(stack: tuple) -> None:
    sessions, redis, _engine = stack
    facts = _Facts()
    graph = _Graph()
    clock = _Clock(T0)
    await _seed(sessions, redis)
    worker = _worker(sessions, redis, facts, graph, clock)

    processed, failed = await worker.run_once()
    assert (processed, failed) == (1, 0)
    assert facts.holder_calls == 0
    assert facts.lp_calls == 1
    assert graph.calls == []

    token = await _token(sessions)
    assert token.risk_score is None
    assert token.heavy_scan_status == HEAVY_SCAN_PENDING
    assert token.dev_distribution_status is None
    assert token.dev_distribution_share is None
    assert token.dev_distribution_reason is None
    flags = list(token.risk_flags or [])
    assert "check_holder_concentration:unavailable" in flags
    assert "check_funder_cluster_exposure:unavailable" in flags
    assert "check_mint_authority:unavailable" not in flags
    assert "check_freeze_authority:unavailable" not in flags
    assert "check_lp_lock:unavailable" not in flags
    assert token.mint_authority_renounced is True
    assert token.freeze_authority_renounced is True

    risk_pending = await redis.xpending(RISK_SCAN_QUEUE, RISK_SCAN_GROUP)
    assert int(risk_pending["pending"]) == 0
    assert await redis.xlen(RISK_HEAVY_QUEUE) == 1

    fusion = SignalFusion(sessions, clock=lambda: T0)
    seen, fusion_failed, created = await fusion.run_once()
    assert (seen, fusion_failed, created) == (0, 0, 0)


async def test_failed_heavy_enqueue_leaves_the_fast_message_pending(stack: tuple) -> None:
    sessions, redis, _engine = stack
    facts = _Facts()
    graph = _Graph()
    clock = _Clock(T0)
    await _seed(sessions, redis)
    worker = _worker(sessions, _BoomHeavy(redis), facts, graph, clock)  # type: ignore[arg-type]

    processed, failed = await worker.run_once()
    assert (processed, failed) == (0, 1)
    assert await redis.xlen(RISK_HEAVY_QUEUE) == 0
    pending = await redis.xpending(RISK_SCAN_QUEUE, RISK_SCAN_GROUP)
    assert int(pending["pending"]) == 1
    token = await _token(sessions)
    assert token.risk_score is None
    assert token.dev_distribution_status is None


async def test_heavy_completion_is_fresh_for_fusion_even_after_a_long_wait(stack: tuple) -> None:
    sessions, redis, _engine = stack
    facts = _Facts()
    graph = _Graph()
    clock = _Clock(T0)
    await _seed(sessions, redis)
    worker = _worker(sessions, redis, facts, graph, clock)
    assert (await worker.run_once())[0] == 1

    clock.now = LATER
    processed, failed = await worker.run_heavy_once()
    assert (processed, failed) == (1, 0)
    assert facts.holder_calls == 1
    assert graph.calls == [MINT]

    token = await _token(sessions)
    assert token.risk_score == Decimal(100)
    assert token.heavy_scan_status == HEAVY_SCAN_COMPLETE
    assert token.updated_at == LATER
    flags = list(token.risk_flags or [])
    assert "check_holder_concentration:unavailable" not in flags
    assert "check_funder_cluster_exposure:unavailable" not in flags
    assert token.dev_distribution_status == "unavailable"
    assert token.dev_distribution_reason == "no_holder_balances"

    # updated_at is the heavy write. The 5-minute fusion window starts then,
    # so the hours spent on risk_heavy_queue do not make the row stale.
    fusion = SignalFusion(sessions, clock=lambda: LATER)
    seen, fusion_failed, created = await fusion.run_once()
    assert (seen, fusion_failed, created) == (1, 0, 1)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Signal)) == 1
    heavy_pending = await redis.xpending(RISK_HEAVY_QUEUE, RISK_HEAVY_GROUP)
    assert int(heavy_pending["pending"]) == 0


async def test_permanent_funder_failure_scores_known_checks_and_fusion_blocks(stack: tuple) -> None:
    sessions, redis, _engine = stack
    facts = _Facts()
    failure = FunderLookupFailure(
        "holder-wallet",
        status_code=403,
        permanent=True,
        message="funded-by failed",
    )
    graph = _Graph(failure)
    clock = _Clock(T0)
    await _seed(sessions, redis, funder_graph_status=FUNDER_GRAPH_PENDING_POST)
    async with sessions() as session:
        async with session.begin():
            token = await _token(sessions)
            buyer = make_tracked_wallet(address="buyer-split", win_rate=Decimal("1"))
            session.add(buyer)
            await session.flush()
            session.add(
                make_wallet_tx(
                    wallet_id=buyer.id,
                    token_id=token.id,
                    action="buy",
                    amount_sol=Decimal("1"),
                    block_time=T0,
                    tx_signature="buy-split",
                )
            )
    worker = _worker(sessions, redis, facts, graph, clock)
    assert (await worker.run_once()) == (1, 0)
    processed, failed = await worker.run_heavy_once()
    assert (processed, failed) == (0, 1)

    token = await _token(sessions)
    known = (
        RISK_CHECK_WEIGHTS["check_mint_authority"]
        + RISK_CHECK_WEIGHTS["check_freeze_authority"]
        + RISK_CHECK_WEIGHTS["check_lp_lock"]
    )
    assert token.risk_score == Decimal(known)
    assert token.heavy_scan_status == HEAVY_SCAN_FAILED
    flags = list(token.risk_flags or [])
    assert "check_holder_concentration:unavailable" in flags
    assert "check_funder_cluster_exposure:unavailable" in flags
    assert token.dev_distribution_status is None
    assert token.funder_graph_status == "failed_permanently"

    fusion = SignalFusion(sessions, clock=lambda: T0)
    seen, fusion_failed, created = await fusion.run_once()
    assert seen == 1
    assert created == 0
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Signal)) == 0


async def test_skipped_pre_migration_keeps_funder_exposure_unavailable(stack: tuple) -> None:
    sessions, redis, _engine = stack
    facts = _Facts()
    graph = _Graph()
    clock = _Clock(T0)
    await _seed(sessions, redis, funder_graph_status=FUNDER_GRAPH_SKIPPED)
    worker = _worker(sessions, redis, facts, graph, clock)
    assert (await worker.run_once()) == (1, 0)
    processed, failed = await worker.run_heavy_once()
    assert (processed, failed) == (1, 0)
    assert graph.calls == []
    assert facts.holder_calls == 1

    token = await _token(sessions)
    assert token.funder_graph_status == FUNDER_GRAPH_SKIPPED
    assert token.heavy_scan_status == HEAVY_SCAN_COMPLETE
    assert token.risk_score is not None
    flags = list(token.risk_flags or [])
    assert "check_funder_cluster_exposure:unavailable" in flags
    assert "check_holder_concentration:unavailable" not in flags
    assert token.heavy_scan_status != HEAVY_SCAN_PENDING


MIGRATION_MINT = "MigrateMint11111111111111111111111111111111"


async def test_post_migration_is_scanned_before_an_earlier_qualify(stack: tuple) -> None:
    sessions, redis, _engine = stack
    facts = _Facts()
    seen: list[str] = []

    async def mint_view(mint: str) -> MintView:
        seen.append(mint)
        return MintView(None, None, 1_000_000)

    facts.mint_view = mint_view  # type: ignore[method-assign]
    graph = _Graph()
    clock = _Clock(T0)
    await _seed(sessions, redis)
    async with sessions() as session:
        async with session.begin():
            await insert_token(
                session,
                mint=MIGRATION_MINT,
                first_seen_at=T0,
                creator_address=CREATOR,
                funder_graph_status=FUNDER_GRAPH_PENDING_POST,
            )
    await redis.xadd(
        RISK_SCAN_PRIORITY_QUEUE,
        {"mint": MIGRATION_MINT, "trace_id": "trace-migration", "reason": "post_migration"},
    )
    worker = _worker(sessions, redis, facts, graph, clock)

    processed, failed = await worker.run_once()
    assert (processed, failed) == (2, 0)
    # mint and freeze each read the mint account, so the migration occupies
    # the first two reads. Qualify does not start until that batch returns.
    assert seen[:3] == [MIGRATION_MINT, MIGRATION_MINT, MINT]
    priority_pending = await redis.xpending(RISK_SCAN_PRIORITY_QUEUE, RISK_SCAN_PRIORITY_GROUP)
    qualify_pending = await redis.xpending(RISK_SCAN_QUEUE, RISK_SCAN_GROUP)
    assert int(priority_pending["pending"]) == 0
    assert int(qualify_pending["pending"]) == 0


async def test_priority_message_acks_its_own_stream_and_enqueues_heavy(stack: tuple) -> None:
    sessions, redis, _engine = stack
    facts = _Facts()
    graph = _Graph()
    clock = _Clock(T0)
    async with sessions() as session:
        async with session.begin():
            await insert_token(
                session,
                mint=MIGRATION_MINT,
                first_seen_at=T0,
                creator_address=CREATOR,
                funder_graph_status=FUNDER_GRAPH_PENDING_POST,
            )
    await redis.xadd(
        RISK_SCAN_PRIORITY_QUEUE,
        {"mint": MIGRATION_MINT, "trace_id": "trace-migration", "reason": "post_migration"},
    )
    worker = _worker(sessions, redis, facts, graph, clock)

    processed, failed = await worker.run_once()
    assert (processed, failed) == (1, 0)
    assert facts.holder_calls == 0
    assert graph.calls == []
    assert await redis.xlen(RISK_SCAN_QUEUE) == 0
    assert await redis.xlen(RISK_HEAVY_QUEUE) == 1
    priority_pending = await redis.xpending(RISK_SCAN_PRIORITY_QUEUE, RISK_SCAN_PRIORITY_GROUP)
    assert int(priority_pending["pending"]) == 0

    async with sessions() as session:
        token = await session.scalar(select(Token).where(Token.mint == MIGRATION_MINT))
    assert token is not None
    assert token.risk_score is None
    assert token.heavy_scan_status == HEAVY_SCAN_PENDING
    flags = list(token.risk_flags or [])
    assert "check_holder_concentration:unavailable" in flags
    assert "check_funder_cluster_exposure:unavailable" in flags
    assert "check_mint_authority:unavailable" not in flags
    assert "check_freeze_authority:unavailable" not in flags
    assert "check_lp_lock:unavailable" not in flags


async def test_startup_moves_undelivered_post_migrations_and_leaves_the_pel(stack: tuple) -> None:
    sessions, redis, _engine = stack
    claimed = await redis.xadd(
        RISK_SCAN_QUEUE,
        {"mint": "claimed-migration", "trace_id": "t-claimed", "reason": "post_migration"},
    )
    await redis.xadd(RISK_SCAN_QUEUE, {"mint": MINT, "trace_id": "t-qualify"})
    waiting = await redis.xadd(
        RISK_SCAN_QUEUE,
        {"mint": MIGRATION_MINT, "trace_id": "t-waiting", "reason": "post_migration"},
    )
    await redis.xgroup_create(RISK_SCAN_QUEUE, RISK_SCAN_GROUP, id="0", mkstream=True)
    delivered = await redis.xreadgroup(
        groupname=RISK_SCAN_GROUP,
        consumername="risk-scan",
        streams={RISK_SCAN_QUEUE: ">"},
        count=1,
    )
    assert delivered[0][1][0][0] == claimed

    worker = _worker(sessions, redis, _Facts(), _Graph(), _Clock(T0))
    assert await worker.move_undelivered_post_migrations() == 1
    assert await worker.move_undelivered_post_migrations() == 0

    entries = await redis.xrange(RISK_SCAN_QUEUE)
    assert [item[1]["mint"] for item in entries] == ["claimed-migration", MINT]
    assert waiting not in [item[0] for item in entries]
    priority = await redis.xrange(RISK_SCAN_PRIORITY_QUEUE)
    assert len(priority) == 1
    assert priority[0][1]["mint"] == MIGRATION_MINT
    assert priority[0][1]["reason"] == "post_migration"
    assert priority[0][1]["trace_id"] == "t-waiting"
    pending = await redis.xpending(RISK_SCAN_QUEUE, RISK_SCAN_GROUP)
    assert int(pending["pending"]) == 1
    assert pending["min"] == claimed


async def _seed_migration(sessions: async_sessionmaker[AsyncSession], redis: Redis) -> None:
    async with sessions() as session:
        async with session.begin():
            await insert_token(
                session,
                mint=MIGRATION_MINT,
                first_seen_at=T0,
                creator_address=CREATOR,
                funder_graph_status=FUNDER_GRAPH_PENDING_POST,
            )
    await redis.xadd(
        RISK_SCAN_PRIORITY_QUEUE,
        {"mint": MIGRATION_MINT, "trace_id": "trace-migration", "reason": "post_migration"},
    )


async def _clear_backoff(redis: Redis) -> None:
    for key in await redis.keys("funder_backoff:*"):
        await redis.delete(key)


async def test_post_migration_pool_not_yet_visible_is_rescanned(stack: tuple) -> None:
    sessions, redis, _engine = stack
    facts = _Facts()
    visible = False
    real_lp_book = facts.lp_book

    async def lp_book(mint: str) -> LpBook:
        if not visible:
            return LpBook(None, None, [])
        return await real_lp_book(mint)

    facts.lp_book = lp_book  # type: ignore[method-assign]
    await _seed_migration(sessions, redis)
    worker = _worker(sessions, redis, facts, _Graph(), _Clock(T0))

    assert await worker.run_once() == (0, 0)
    pending = await redis.xpending(RISK_SCAN_PRIORITY_QUEUE, RISK_SCAN_PRIORITY_GROUP)
    assert int(pending["pending"]) == 1
    assert await redis.xlen(RISK_HEAVY_QUEUE) == 0
    # Still inside the delay: the pending message is not rescanned.
    assert await worker.run_once() == (0, 0)

    visible = True
    await _clear_backoff(redis)
    assert await worker.run_once() == (1, 0)
    pending = await redis.xpending(RISK_SCAN_PRIORITY_QUEUE, RISK_SCAN_PRIORITY_GROUP)
    assert int(pending["pending"]) == 0
    assert await redis.xlen(RISK_HEAVY_QUEUE) == 1


async def test_post_migration_retries_end_in_skipped(stack: tuple) -> None:
    sessions, redis, _engine = stack
    facts = _Facts()

    async def lp_book(mint: str) -> LpBook:
        del mint
        return LpBook(None, None, [])

    facts.lp_book = lp_book  # type: ignore[method-assign]
    await _seed_migration(sessions, redis)
    worker = _worker(sessions, redis, facts, _Graph(), _Clock(T0))

    for _ in range(len(POST_MIGRATION_RETRY_DELAYS_SECONDS)):
        assert await worker.run_once() == (0, 0)
        await _clear_backoff(redis)
    assert await worker.run_once() == (1, 0)

    async with sessions() as session:
        token = await session.scalar(select(Token).where(Token.mint == MIGRATION_MINT))
    assert token is not None
    assert token.heavy_scan_status == HEAVY_SCAN_SKIPPED
    assert await redis.xlen(RISK_HEAVY_QUEUE) == 0
