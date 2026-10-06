"""A repeated funded-by 403 is acked once. The same wallet is not retried forever."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone

import pytest
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.adapters.helius import HeliusError, HeliusRateLimitError, HeliusResponseError
from app.config import RISK_HEAVY_GROUP, RISK_HEAVY_QUEUE, RISK_SCAN_GROUP, RISK_SCAN_QUEUE
from app.db.models import Token
from app.services.funder_graph import FunderGraphBuilder, FunderLookupFailure, funder_lookup_failure
from app.services.risk_scanner import CheckResult, RiskReport
from app.services.token_store import FUNDER_GRAPH_FAILED, FUNDER_GRAPH_PENDING_POST, insert_token
from app.workers.risk_scan_worker import RiskScanWorker
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

WALLET = "7ExZPpQWWXBuf92bVGGser6S1ANE28po75rwwZ1TnG4g"
MINT = "2VeuwcJYtu9P1g27a1UahFsJmVZRBWfBvLJcuzvApump"
T0 = datetime(2026, 9, 25, 20, 38, tzinfo=timezone.utc)


class _FastScanner:
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


class _Graph:
    def __init__(self, failure: FunderLookupFailure) -> None:
        self.failure = failure
        self.calls = 0

    async def build_graph_for_token(self, token_mint: str, top_n_holders: int = 20) -> list[object]:
        del token_mint, top_n_holders
        self.calls += 1
        raise self.failure


class _Helius403:
    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]:
        del mint, top_n
        return [WALLET]

    async def find_first_funding_tx(self, wallet_address: str) -> None:
        raise HeliusResponseError(403, f"funded-by failed for {wallet_address} with HTTP 403")


def test_403_is_permanent_and_429_is_transient() -> None:
    forbidden = funder_lookup_failure(
        WALLET,
        HeliusResponseError(403, f"funded-by failed for {WALLET} with HTTP 403"),
    )
    assert forbidden is not None
    assert forbidden.permanent is True
    assert forbidden.wallet_address == WALLET
    assert forbidden.status_code == 403

    limited = funder_lookup_failure(WALLET, HeliusRateLimitError("Helius rate limited GET"))
    assert limited is not None
    assert limited.permanent is False
    assert limited.status_code == 429

    timed_out = funder_lookup_failure(WALLET, HeliusError("Helius GET failed after 3 attempts"))
    assert timed_out is not None
    assert timed_out.permanent is False
    assert timed_out.status_code is None


class _NoCache:
    async def scalar(self, statement: object) -> None:
        del statement
        return None


async def test_builder_wraps_funded_by_403_with_the_wallet() -> None:
    builder = FunderGraphBuilder(_NoCache(), _Helius403())  # type: ignore[arg-type]
    with pytest.raises(FunderLookupFailure) as caught:
        await builder.build_graph_for_token(MINT)
    assert caught.value.wallet_address == WALLET
    assert caught.value.permanent is True
    assert caught.value.status_code == 403


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


async def _enqueue(sessions: async_sessionmaker[AsyncSession], redis: Redis) -> None:
    async with sessions() as session:
        async with session.begin():
            await insert_token(
                session,
                mint=MINT,
                first_seen_at=T0,
                funder_graph_status=FUNDER_GRAPH_PENDING_POST,
            )
    await redis.xadd(RISK_SCAN_QUEUE, {"mint": MINT, "trace_id": "trace-funder"})


async def _status(sessions: async_sessionmaker[AsyncSession]) -> str | None:
    async with sessions() as session:
        token = await session.scalar(select(Token).where(Token.mint == MINT))
    assert token is not None
    return token.funder_graph_status


async def _pending(
    redis: Redis, queue: str = RISK_HEAVY_QUEUE, group: str = RISK_HEAVY_GROUP
) -> int:
    info = await redis.xpending(queue, group)
    return int(info["pending"])


async def test_repeated_403_is_acked_and_not_retried(stack: tuple) -> None:
    sessions, redis = stack
    graph = _Graph(
        FunderLookupFailure(
            WALLET,
            status_code=403,
            permanent=True,
            message=f"funded-by failed for {WALLET} with HTTP 403",
        )
    )
    worker = RiskScanWorker(
        sessions,
        redis,
        lambda _session: graph,
        lambda _session, _clusters: _FastScanner(),
    )
    await _enqueue(sessions, redis)

    processed, failed = await worker.run_once()
    assert (processed, failed) == (1, 0)
    assert graph.calls == 0
    assert await _pending(redis, RISK_SCAN_QUEUE, RISK_SCAN_GROUP) == 0
    processed, failed = await worker.run_heavy_once()
    assert (processed, failed) == (0, 1)
    assert graph.calls == 1
    assert await _status(sessions) == FUNDER_GRAPH_FAILED
    assert await _pending(redis) == 0

    await redis.xadd(RISK_SCAN_QUEUE, {"mint": MINT, "trace_id": "trace-again"})
    processed, failed = await worker.run_once()
    assert (processed, failed) == (1, 0)
    assert graph.calls == 1
    assert await _status(sessions) == FUNDER_GRAPH_FAILED
    assert await _pending(redis, RISK_SCAN_QUEUE, RISK_SCAN_GROUP) == 0


async def test_transient_429_retries_then_stops(stack: tuple) -> None:
    sessions, redis = stack
    graph = _Graph(
        FunderLookupFailure(
            WALLET,
            status_code=429,
            permanent=False,
            message="Helius rate limited GET after 3 attempts",
        )
    )
    worker = RiskScanWorker(
        sessions,
        redis,
        lambda _session: graph,
        lambda _session, _clusters: _FastScanner(),
        retry_backoff_seconds=0,
    )
    await _enqueue(sessions, redis)

    processed, failed = await worker.run_once()
    assert (processed, failed) == (1, 0)
    assert graph.calls == 0
    processed, failed = await worker.run_heavy_once()
    assert (processed, failed) == (0, 1)
    assert graph.calls == 1
    assert await _status(sessions) == FUNDER_GRAPH_PENDING_POST
    assert await _pending(redis) == 1

    processed, failed = await worker.run_heavy_once()
    assert (processed, failed) == (0, 1)
    assert graph.calls == 2
    assert await _status(sessions) == FUNDER_GRAPH_FAILED
    assert await _pending(redis) == 0

    await redis.xadd(RISK_SCAN_QUEUE, {"mint": MINT, "trace_id": "trace-after-cap"})
    processed, failed = await worker.run_once()
    assert graph.calls == 2
    assert (processed, failed) == (1, 0)
