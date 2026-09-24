"""Funder-graph tests. Helius is mocked; nothing here opens a socket to api.helius.xyz."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.adapters.helius import (
    FundingInfo,
    HeliusClient,
    HeliusRateLimitError,
    parse_holder_owners,
)
from app.db.models import FunderCluster, WalletFunder
from app.services.funder_graph import DisjointSet, FunderGraphBuilder, cluster_holders

ROOT = Path(__file__).resolve().parents[1]
TEST_DATABASE = "signalforge_funder_test"


def _admin_and_test_urls() -> tuple[str, str]:
    raw = os.environ.get(
        "DATABASE_URL",
        "postgresql+asyncpg://signalforge:signalforge@127.0.0.1:5432/signalforge",
    )
    parsed = make_url(raw)
    admin = parsed.set(database="postgres").render_as_string(hide_password=False)
    test = parsed.set(database=TEST_DATABASE).render_as_string(hide_password=False)
    return admin, test


async def _recreate_database(admin_url: str, name: str) -> None:
    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as connection:
        await connection.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :name AND pid <> pg_backend_pid()"
            ),
            {"name": name},
        )
        await connection.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
        await connection.execute(text(f'CREATE DATABASE "{name}"'))
    await engine.dispose()


async def _drop_database(admin_url: str, name: str) -> None:
    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as connection:
        await connection.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :name AND pid <> pg_backend_pid()"
            ),
            {"name": name},
        )
        await connection.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
    await engine.dispose()


def _alembic(database_url: str, *args: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")
    env.setdefault("ENVIRONMENT", "dev")
    env.setdefault("LOG_LEVEL", "INFO")
    subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=ROOT,
        env=env,
        check=True,
    )


@pytest.fixture(scope="session")
def funder_database_url() -> Iterator[str]:
    admin_url, test_url = _admin_and_test_urls()
    asyncio.run(_recreate_database(admin_url, TEST_DATABASE))
    _alembic(test_url, "upgrade", "head")
    yield test_url
    asyncio.run(_drop_database(admin_url, TEST_DATABASE))


@pytest.fixture
async def session(funder_database_url: str) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(funder_database_url)
    async with engine.connect() as connection:
        transaction = await connection.begin()
        factory = async_sessionmaker(
            bind=connection,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )
        async with factory() as db_session:
            yield db_session
        await transaction.rollback()
    await engine.dispose()


def test_disjoint_set_splits_two_clusters() -> None:
    """Синтетический граф: 5 кошельков, два явных кластера, без Helius.

    w1-w2-w3 связаны между собой, w4-w5 — отдельно. w3 не должен
    оказаться в одном множестве с w4.
    """
    wallets = ["w1", "w2", "w3", "w4", "w5"]
    clusters = DisjointSet(wallets)
    clusters.union("w1", "w2")
    clusters.union("w2", "w3")
    clusters.union("w4", "w5")

    assert clusters.same("w1", "w3")
    assert clusters.same("w4", "w5")
    assert not clusters.same("w3", "w4")
    assert not clusters.same("w1", "w5")
    assert sorted(tuple(group) for group in clusters.components()) == [
        ("w1", "w2", "w3"),
        ("w4", "w5"),
    ]


def test_cluster_holders_joins_different_hop1_funders_through_one_hop2_root() -> None:
    """Пять холдеров. a/b/c вышли из разных промежуточных кошельков, но из одного корня.

    d и e делят только свой hop-1 фандер. Их двое — кластер не пишется.
    """
    clusters = cluster_holders(
        ["a", "b", "c", "d", "e"],
        {"a": "mid-1", "b": "mid-2", "c": "mid-3", "d": "mid-4", "e": "mid-4"},
        {"mid-1": "ROOT", "mid-2": "ROOT", "mid-3": "ROOT", "mid-4": "OTHER"},
    )
    assert [(cluster.root_funder, cluster.members) for cluster in clusters] == [
        ("ROOT", ("a", "b", "c")),
    ]


def test_parse_holder_owners_keeps_balance_order_and_drops_duplicates() -> None:
    """Фикстура ответа RPC, не живой Helius. Два ATA одного владельца схлопываются."""
    owners = parse_holder_owners(
        ["ata-large", "ata-same-owner", "ata-other"],
        [
            {"data": {"parsed": {"info": {"owner": "WalletA"}}}},
            {"data": {"parsed": {"info": {"owner": "WalletA"}}}},
            {"data": {"parsed": {"info": {"owner": "WalletB"}}}},
        ],
    )
    assert owners == ["WalletA", "WalletB"]


class _FakeHelius:
    """Явный двойник HeliusClient. Сеть не открывает."""

    def __init__(self, funding: dict[str, FundingInfo | None]) -> None:
        self._funding = funding
        self.holder_calls: list[tuple[str, int]] = []
        self.funding_calls: list[str] = []

    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]:
        self.holder_calls.append((mint, top_n))
        return ["H1", "H2", "H3", "H4", "H5"]

    async def get_first_funding_tx(self, wallet_address: str) -> FundingInfo | None:
        self.funding_calls.append(wallet_address)
        return self._funding.get(wallet_address)


def _funding(wallet: str, funder: str) -> FundingInfo:
    return FundingInfo(
        wallet_address=wallet,
        funder=funder,
        amount_sol=Decimal("1.5"),
        signature=f"sig-{wallet}",
        funded_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        slot=10,
    )


async def test_build_graph_keeps_cluster_of_three_and_drops_pair(
    session: AsyncSession,
) -> None:
    """Три холдера с общим корнем дают кластер. Два холдера с другим фандером — нет."""
    helius = _FakeHelius(
        {
            "H1": _funding("H1", "F_SHARED"),
            "H2": _funding("H2", "F_SHARED"),
            "H3": _funding("H3", "F_SHARED"),
            "H4": _funding("H4", "F_SMALL"),
            "H5": _funding("H5", "F_SMALL"),
            "F_SHARED": _funding("F_SHARED", "ROOT"),
            "F_SMALL": None,
        }
    )
    clusters = await FunderGraphBuilder(session, helius).build_graph_for_token(
        "mint-under-test",
        top_n_holders=20,
    )

    assert helius.holder_calls == [("mint-under-test", 20)]
    assert len(clusters) == 1
    assert clusters[0].root_funder == "ROOT"
    assert clusters[0].cluster_size == 3
    assert clusters[0].member_wallets == ["H1", "H2", "H3"]
    assert clusters[0].confidence == Decimal("0.3")

    stored = (await session.scalars(select(FunderCluster))).all()
    assert [row.member_wallets for row in stored] == [["H1", "H2", "H3"]]

    edges = {
        (row.wallet_address, row.funder_address, row.hop_depth)
        for row in (await session.scalars(select(WalletFunder))).all()
    }
    assert edges == {
        ("H1", "F_SHARED", 1),
        ("H2", "F_SHARED", 1),
        ("H3", "F_SHARED", 1),
        ("H4", "F_SMALL", 1),
        ("H5", "F_SMALL", 1),
        ("F_SHARED", "ROOT", 2),
    }


async def test_rate_limit_retries_with_retry_after() -> None:
    """Первый ответ — фикстура 429. Клиент ждёт Retry-After и повторяет, а не падает."""
    calls = 0
    slept: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.headers["X-Api-Key"] == "test-key"
        assert "api-key" not in str(request.url)
        if calls == 1:
            return httpx.Response(
                429,
                headers={"Retry-After": "2"},
                json={"error": "RATE_LIMIT_EXCEEDED", "code": 429},
            )
        return httpx.Response(
            200,
            json={
                "funder": "FunderA",
                "amountRaw": "500000000",
                "decimals": 9,
                "signature": "sig-1",
                "timestamp": 1700000000,
                "slot": 99,
            },
        )

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = HeliusClient(
            api_key="test-key",
            api_base_url="https://api.helius.xyz",
            rpc_url="https://mainnet.helius-rpc.com",
            client=http,
            sleep=fake_sleep,
        )
        info = await client.get_first_funding_tx("HolderA")

    assert calls == 2
    assert slept == [2.0]
    assert info is not None
    assert info.funder == "FunderA"
    assert info.amount_sol == Decimal("0.5")


async def test_rate_limit_uses_exponential_backoff_without_retry_after() -> None:
    calls = 0
    slept: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls < 3:
            return httpx.Response(429, json={"error": "RATE_LIMIT_EXCEEDED", "code": 429})
        return httpx.Response(
            200,
            json={
                "funder": "FunderB",
                "amount": "0.25",
                "signature": "sig-2",
                "timestamp": 1700000001,
            },
        )

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = HeliusClient(
            api_key="test-key",
            api_base_url="https://api.helius.xyz",
            rpc_url="https://mainnet.helius-rpc.com",
            client=http,
            sleep=fake_sleep,
        )
        info = await client.get_first_funding_tx("HolderB")

    assert calls == 3
    assert slept == [1.0, 2.0]
    assert info is not None
    assert info.funder == "FunderB"


async def test_rate_limit_gives_up_after_three_attempts() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "1"}, json={"error": "RATE_LIMIT_EXCEEDED"})

    async def fake_sleep(_delay: float) -> None:
        return None

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = HeliusClient(
            api_key="test-key",
            api_base_url="https://api.helius.xyz",
            rpc_url="https://mainnet.helius-rpc.com",
            client=http,
            sleep=fake_sleep,
        )
        with pytest.raises(HeliusRateLimitError):
            await client.get_first_funding_tx("HolderC")

    assert calls == 3


async def test_missing_funding_is_none_and_is_not_retried() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404, json={"error": "No funding transaction found", "code": 404})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = HeliusClient(
            api_key="test-key",
            api_base_url="https://api.helius.xyz",
            rpc_url="https://mainnet.helius-rpc.com",
            client=http,
            sleep=_no_sleep,
        )
        info = await client.get_first_funding_tx("HolderD")

    assert info is None
    assert calls == 1


async def _no_sleep(_delay: float) -> None:
    return None
