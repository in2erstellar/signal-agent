"""Engine factory stays async and does not open a connection."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.db.base import create_engine, create_sessionmaker


def _settings() -> Settings:
    return Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://signalforge:signalforge@localhost:5432/signalforge",
        redis_url="redis://localhost:6379/0",
    )


async def test_create_engine_uses_asyncpg_without_connecting() -> None:
    engine = create_engine(_settings())
    try:
        assert engine.url.drivername == "postgresql+asyncpg"
        assert engine.pool.checkedout() == 0
        create_sessionmaker(engine)
    finally:
        await engine.dispose()


def test_sync_driver_url_is_rejected() -> None:
    with pytest.raises(ValueError, match="postgresql\\+asyncpg"):
        Settings(
            _env_file=None,
            database_url="postgresql://localhost/signalforge",
            redis_url="redis://localhost:6379/0",
        )
