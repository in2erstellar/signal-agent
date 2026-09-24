"""Session-scoped Timescale and Redis containers for pipeline tests."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.core.container import DockerContainer

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class PipelineInfra:
    database_url: str
    redis_url: str


def _alembic(database_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["REDIS_URL"] = "redis://127.0.0.1:6379/0"
    env.setdefault("ENVIRONMENT", "dev")
    env.setdefault("LOG_LEVEL", "INFO")
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        check=True,
    )


async def _wait_postgres(url: str) -> None:
    last_error: Exception | None = None
    for _ in range(60):
        engine = create_async_engine(url)
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(1)
        finally:
            await engine.dispose()
    raise RuntimeError(f"postgres container did not become ready: {last_error}")


async def _wait_redis(url: str) -> None:
    from redis.asyncio import Redis

    last_error: Exception | None = None
    for _ in range(60):
        client = Redis.from_url(url, socket_connect_timeout=1, socket_timeout=1)
        try:
            if await client.ping():
                return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.5)
        finally:
            await client.aclose()
    raise RuntimeError(f"redis container did not become ready: {last_error}")


async def truncate_pipeline(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "TRUNCATE TABLE signals, bait_flags, wallet_txs, funder_clusters, "
                "wallet_funders, tokens, tracked_wallets RESTART IDENTITY CASCADE"
            )
        )


@pytest.fixture(scope="session")
def pipeline_infra() -> Iterator[PipelineInfra]:
    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
    postgres = (
        DockerContainer("timescale/timescaledb:2.30.1-pg16-oss")
        .with_env("POSTGRES_USER", "signalforge")
        .with_env("POSTGRES_PASSWORD", "signalforge")
        .with_env("POSTGRES_DB", "signalforge")
        .with_exposed_ports(5432)
    )
    redis = DockerContainer("redis:7").with_exposed_ports(6379)
    postgres.start()
    redis.start()
    try:
        database_url = (
            "postgresql+asyncpg://signalforge:signalforge@"
            f"{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/signalforge"
        )
        redis_url = f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"
        asyncio.run(_wait_postgres(database_url))
        asyncio.run(_wait_redis(redis_url))
        _alembic(database_url)
        yield PipelineInfra(database_url=database_url, redis_url=redis_url)
    finally:
        redis.stop()
        postgres.stop()
