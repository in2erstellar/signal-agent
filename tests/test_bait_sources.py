"""Session readers for the bait detector. Postgres only; no Helius or Redis."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.services.bait_detector import BaitDetector, PendingBaitCheck
from app.services.bait_sources import SessionClusterBook, SessionSwapLedger
from tests.factories import (
    make_funder_cluster,
    make_token,
    make_tracked_wallet,
    make_wallet_funder,
    make_wallet_tx,
)
from tests.test_bait_detector import (
    HIDDEN,
    MINT,
    OTHER,
    PUBLIC,
    ROOT,
    T0,
    WALLET,
    MapCensus,
    MemoryQueue,
)

ROOT_DIR = Path(__file__).resolve().parents[1]
TEST_DATABASE = "signalforge_bait_test"


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
        cwd=ROOT_DIR,
        env=env,
        check=True,
    )


@pytest.fixture(scope="module")
def migrated_url() -> Iterator[str]:
    admin_url, test_url = _admin_and_test_urls()
    asyncio.run(_recreate_database(admin_url, TEST_DATABASE))
    _alembic(test_url, "upgrade", "head")
    yield test_url
    asyncio.run(_drop_database(admin_url, TEST_DATABASE))


@pytest.fixture
async def session(migrated_url: str) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(migrated_url)
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


async def test_session_ledger_recheck_sees_a_3_1x_dump(session: AsyncSession) -> None:
    token = make_token(mint=MINT)
    tracked = make_tracked_wallet(address=WALLET)
    other = make_tracked_wallet(address=OTHER)
    session.add_all([token, tracked, other])
    await session.flush()
    session.add_all(
        [
            make_wallet_tx(
                wallet_id=other.id,
                token_id=token.id,
                action="buy",
                amount_sol=Decimal("120"),
                block_time=T0 - timedelta(minutes=30),
                tx_signature="baseline",
            ),
            make_wallet_tx(
                wallet_id=tracked.id,
                token_id=token.id,
                action="buy",
                amount_sol=Decimal("10"),
                block_time=T0,
                tx_signature="buy-sig",
            ),
            make_wallet_tx(
                wallet_id=other.id,
                token_id=token.id,
                action="buy",
                amount_sol=Decimal("31"),
                block_time=T0 + timedelta(minutes=5),
                tx_signature="influx",
            ),
            make_wallet_tx(
                wallet_id=tracked.id,
                token_id=token.id,
                action="sell",
                amount_sol=Decimal("5"),
                block_time=T0 + timedelta(minutes=15),
                tx_signature="sell",
            ),
        ]
    )
    await session.flush()

    detector = BaitDetector(
        SessionSwapLedger(session),
        SessionClusterBook(session),
        MapCensus(set(), set()),
        MemoryQueue(),
        clock=lambda: T0 + timedelta(minutes=60),
    )
    flags = await detector.complete_pending_check(
        PendingBaitCheck(
            wallet_address=WALLET,
            token_mint=MINT,
            buy_signature="buy-sig",
            buy_amount_sol=Decimal("10"),
            buy_block_time=T0,
            scheduled_at=T0 + timedelta(minutes=60),
            created_at=T0,
        )
    )
    assert len(flags) == 1
    assert flags[0].confidence == Decimal("0.5")
    assert flags[0].evidence["volume_multiple"] == "3.1"


async def test_session_cluster_book_includes_hop2_and_hidden_holder(
    session: AsyncSession,
) -> None:
    token = make_token(mint=MINT)
    tracked = make_tracked_wallet(address=WALLET)
    public = make_tracked_wallet(address=PUBLIC)
    session.add_all([token, tracked, public])
    await session.flush()
    session.add_all(
        [
            make_funder_cluster(
                token_id=token.id,
                root_funder=ROOT,
                member_wallets=[WALLET, PUBLIC],
                cluster_size=2,
                confidence=Decimal("0.8"),
            ),
            make_wallet_funder(
                wallet_address=HIDDEN,
                funder_address=ROOT,
                hop_depth=1,
            ),
            make_wallet_funder(
                wallet_address="Intermediate",
                funder_address=ROOT,
                hop_depth=2,
            ),
            make_wallet_funder(
                wallet_address="Hop2Hidden",
                funder_address="Intermediate",
                hop_depth=1,
            ),
        ]
    )
    await session.flush()

    cohorts = await SessionClusterBook(session).cohorts_for(WALLET, MINT)
    assert len(cohorts) == 1
    assert cohorts[0].funded_by_root == ("Hop2Hidden", HIDDEN)

    detector = BaitDetector(
        SessionSwapLedger(session),
        SessionClusterBook(session),
        MapCensus(holders={WALLET, PUBLIC, HIDDEN}, top={WALLET, PUBLIC}),
        MemoryQueue(),
        clock=lambda: T0,
    )
    sell = make_wallet_tx(
        wallet_id=tracked.id,
        token_id=token.id,
        action="sell",
        amount_sol=Decimal("1"),
        block_time=T0,
        tx_signature="not-a-buy",
    )
    flags = await detector.analyze_wallet_action(WALLET, MINT, sell)
    by_type = {flag.flag_type: flag for flag in flags}
    assert set(by_type) == {"hidden_dual_position", "funder_cluster_member"}
    assert by_type["hidden_dual_position"].evidence["hidden_wallets"] == [HIDDEN]
    assert by_type["hidden_dual_position"].confidence == Decimal("0.5")
    assert by_type["funder_cluster_member"].confidence == Decimal("0.8")
