"""Upgrade and downgrade 0015 on a database that already has rows.

There is no production dump in this environment. The fixture is a migrated
copy with a token, a signal, an outcome row, and an LP-unlock row.
The revision does not touch Redis; this test checks the schema round trip.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

ROOT = Path(__file__).resolve().parents[1]
TEST_DATABASE = "signalforge_migration_0015"
PRIOR = "0014_funder_cache_signature"


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


@pytest.fixture(scope="module")
def migrated_url() -> Iterator[str]:
    admin_url, test_url = _admin_and_test_urls()
    asyncio.run(_recreate_database(admin_url, TEST_DATABASE))
    try:
        yield test_url
    finally:
        asyncio.run(_drop_database(admin_url, TEST_DATABASE))


async def _tables(url: str) -> set[str]:
    engine = create_async_engine(url)
    async with engine.connect() as connection:
        rows = await connection.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        )
        names = {row[0] for row in rows}
    await engine.dispose()
    return names


async def _columns(url: str, table: str) -> set[str]:
    engine = create_async_engine(url)
    async with engine.connect() as connection:
        rows = await connection.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :table"
            ),
            {"table": table},
        )
        names = {row[0] for row in rows}
    await engine.dispose()
    return names


def test_readme_rollback_stops_the_worker_before_downgrade() -> None:
    readme = (ROOT / "README.md").read_text()
    rollback = readme.split("## Откат ревизии 0015", 1)[1]
    stop = rollback.index("stop worker")
    delete = rollback.index("DEL signalforge:outcome:pending")
    downgrade = rollback.index("alembic downgrade 0014_funder_cache_signature")
    assert stop < delete < downgrade


async def test_upgrade_downgrade_upgrade_keeps_existing_rows(migrated_url: str) -> None:
    _alembic(migrated_url, "upgrade", PRIOR)
    engine = create_async_engine(migrated_url)
    async with engine.begin() as connection:
        token_id = await connection.scalar(
            text(
                "INSERT INTO tokens (mint, first_seen_at) "
                "VALUES ('mint-before', NOW()) RETURNING id"
            )
        )
        await connection.execute(
            text(
                "INSERT INTO signals (token_id, composite_score, triggered_at) "
                "VALUES (:token_id, 60, NOW())"
            ),
            {"token_id": token_id},
        )
    await engine.dispose()

    _alembic(migrated_url, "upgrade", "head")
    assert "outcome_checks" in await _tables(migrated_url)
    assert "lp_unlocks" in await _tables(migrated_url)
    assert "context" in await _columns(migrated_url, "signals")
    assert "dev_distribution_status" in await _columns(migrated_url, "tokens")

    engine = create_async_engine(migrated_url)
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO outcome_checks "
                "(token_id, horizon, status, scheduled_at) "
                "VALUES (:token_id, '24h', 'pending', NOW())"
            ),
            {"token_id": token_id},
        )
        await connection.execute(
            text(
                "INSERT INTO lp_unlocks (token_id, status, reason) "
                "VALUES (:token_id, 'unavailable', 'lock_schedule_unreadable')"
            ),
            {"token_id": token_id},
        )
        await connection.execute(
            text("UPDATE signals SET context = '{\"creator\": \"dev\"}'::jsonb")
        )
    await engine.dispose()

    _alembic(migrated_url, "downgrade", PRIOR)
    tables = await _tables(migrated_url)
    assert "outcome_checks" not in tables
    assert "lp_unlocks" not in tables
    assert "context" not in await _columns(migrated_url, "signals")
    assert "dev_distribution_share" not in await _columns(migrated_url, "tokens")

    engine = create_async_engine(migrated_url)
    async with engine.connect() as connection:
        mint = await connection.scalar(text("SELECT mint FROM tokens"))
        score = await connection.scalar(text("SELECT composite_score FROM signals"))
    await engine.dispose()
    assert mint == "mint-before"
    assert score == 60

    _alembic(migrated_url, "upgrade", "head")
    assert "outcome_checks" in await _tables(migrated_url)
    engine = create_async_engine(migrated_url)
    async with engine.connect() as connection:
        mint = await connection.scalar(text("SELECT mint FROM tokens"))
        outcomes = await connection.scalar(text("SELECT count(*) FROM outcome_checks"))
    await engine.dispose()
    assert mint == "mint-before"
    assert outcomes == 0
