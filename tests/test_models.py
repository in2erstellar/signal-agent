"""Persist one row per phase-1 table and check foreign-key behavior.

The session fixture migrates a throwaway database with Alembic. Schema is not
created via ``create_all()``.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from sqlalchemy import delete, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.db.models import (
    BaitFlag,
    FunderCluster,
    Signal,
    Token,
    TrackedWallet,
    User,
    WalletFunder,
    WalletTx,
)
from tests.factories import (
    make_bait_flag,
    make_funder_cluster,
    make_signal,
    make_token,
    make_tracked_wallet,
    make_user,
    make_wallet_funder,
    make_wallet_tx,
)

ROOT = Path(__file__).resolve().parents[1]
TEST_DATABASE = "signalforge_model_test"


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
def migrated_url() -> Iterator[str]:
    admin_url, test_url = _admin_and_test_urls()
    asyncio.run(_recreate_database(admin_url, TEST_DATABASE))
    _alembic(test_url, "upgrade", "head")
    yield test_url
    asyncio.run(_drop_database(admin_url, TEST_DATABASE))


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


async def test_each_table_accepts_a_row_and_foreign_keys_resolve(
    session: AsyncSession,
) -> None:
    token = make_token()
    wallet = make_tracked_wallet()
    user = make_user()
    session.add_all([token, wallet, user])
    await session.flush()

    wallet_tx = make_wallet_tx(wallet_id=wallet.id, token_id=token.id)
    funder = make_wallet_funder(wallet_address=wallet.address)
    cluster = make_funder_cluster(token_id=token.id)
    bait_flag = make_bait_flag(wallet_id=wallet.id, token_id=token.id)
    signal = make_signal(token_id=token.id)
    session.add_all([wallet_tx, funder, cluster, bait_flag, signal])
    await session.flush()

    await session.refresh(
        token,
        attribute_names=["transactions", "bait_flags", "signals", "funder_clusters"],
    )
    await session.refresh(wallet, attribute_names=["transactions", "bait_flags"])

    assert wallet_tx.wallet_id == wallet.id
    assert wallet_tx.token_id == token.id
    assert bait_flag.wallet_id == wallet.id
    assert bait_flag.token_id == token.id
    assert signal.token_id == token.id
    assert cluster.token_id == token.id
    assert {row.id for row in token.transactions} == {wallet_tx.id}
    assert {row.id for row in token.bait_flags} == {bait_flag.id}
    assert {row.id for row in token.signals} == {signal.id}
    assert {row.id for row in token.funder_clusters} == {cluster.id}
    assert {row.id for row in wallet.transactions} == {wallet_tx.id}
    assert {row.id for row in wallet.bait_flags} == {bait_flag.id}

    stored_user = await session.get(User, user.id)
    stored_funder = await session.get(WalletFunder, funder.id)
    assert stored_user is not None
    assert stored_user.telegram_id == user.telegram_id
    assert stored_funder is not None
    assert stored_funder.wallet_address == wallet.address
    assert isinstance(stored_user, User)
    assert isinstance(wallet_tx, WalletTx)
    assert isinstance(signal, Signal)
    assert isinstance(cluster, FunderCluster)
    assert isinstance(bait_flag, BaitFlag)
    assert isinstance(token, Token)
    assert isinstance(wallet, TrackedWallet)


async def test_wallet_tx_restricts_deleting_token_or_wallet(
    session: AsyncSession,
) -> None:
    token = make_token()
    wallet = make_tracked_wallet()
    session.add_all([token, wallet])
    await session.flush()
    session.add(make_wallet_tx(wallet_id=wallet.id, token_id=token.id))
    await session.flush()

    with pytest.raises(IntegrityError):
        async with session.begin_nested():
            await session.execute(delete(Token).where(Token.id == token.id))

    with pytest.raises(IntegrityError):
        async with session.begin_nested():
            await session.execute(delete(TrackedWallet).where(TrackedWallet.id == wallet.id))


async def test_bait_flag_and_signal_cascade_with_parent(session: AsyncSession) -> None:
    token = make_token()
    wallet = make_tracked_wallet()
    session.add_all([token, wallet])
    await session.flush()
    bait_flag = make_bait_flag(wallet_id=wallet.id, token_id=token.id)
    signal = make_signal(token_id=token.id)
    session.add_all([bait_flag, signal])
    await session.flush()
    bait_flag_id = bait_flag.id
    signal_id = signal.id
    wallet_id = wallet.id
    token_id = token.id

    await session.execute(delete(TrackedWallet).where(TrackedWallet.id == wallet_id))
    await session.flush()
    session.expire_all()
    assert await session.get(BaitFlag, bait_flag_id) is None
    assert await session.get(Signal, signal_id) is not None

    await session.execute(delete(Token).where(Token.id == token_id))
    await session.flush()
    session.expire_all()
    assert await session.get(Signal, signal_id) is None


async def test_funder_cluster_token_link_is_set_null_on_token_delete(
    session: AsyncSession,
) -> None:
    token = make_token()
    session.add(token)
    await session.flush()
    cluster = make_funder_cluster(token_id=token.id)
    session.add(cluster)
    await session.flush()
    cluster_id = cluster.id

    await session.execute(delete(Token).where(Token.id == token.id))
    await session.flush()
    session.expire_all()

    stored = await session.get(FunderCluster, cluster_id)
    assert stored is not None
    assert stored.token_id is None


def test_alembic_upgrade_and_downgrade_on_a_clean_database() -> None:
    admin_url, _ = _admin_and_test_urls()
    name = "signalforge_migration_roundtrip"
    parsed = make_url(admin_url)
    database_url = parsed.set(database=name).render_as_string(hide_password=False)
    asyncio.run(_recreate_database(admin_url, name))
    try:
        _alembic(database_url, "upgrade", "head")
        asyncio.run(_assert_wallet_txs_is_hypertable(database_url))
        _alembic(database_url, "downgrade", "base")
        asyncio.run(_assert_user_tables_are_gone(database_url))
        _alembic(database_url, "upgrade", "head")
    finally:
        asyncio.run(_drop_database(admin_url, name))


async def _assert_wallet_txs_is_hypertable(database_url: str) -> None:
    engine = create_async_engine(database_url)
    async with engine.connect() as connection:
        found = await connection.scalar(
            text(
                "SELECT 1 FROM timescaledb_information.hypertables "
                "WHERE hypertable_name = 'wallet_txs'"
            )
        )
    await engine.dispose()
    assert found == 1


async def _assert_user_tables_are_gone(database_url: str) -> None:
    engine = create_async_engine(database_url)
    async with engine.connect() as connection:
        rows = await connection.scalars(
            text(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename <> 'alembic_version'"
            )
        )
        names = list(rows)
    await engine.dispose()
    assert names == []
