"""Health endpoint tests.

Live Postgres and Redis are not required. Fixtures below are explicit mocks:
they stand in for the drivers and never pretend to be a real integration.
The running stack is checked with curl against docker-compose.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import OperationalError

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://signalforge:signalforge@localhost:5432/signalforge",
)
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("ENVIRONMENT", "dev")
os.environ.setdefault("LOG_LEVEL", "INFO")

from app.health import probe_database, probe_redis  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture
async def api_client():
    """HTTP client against the ASGI app with the real lifespan running.

    The lifespan opens driver objects but does not connect. Probe tests that
    need a stand-in replace ``probe_database`` / ``probe_redis`` with an
    explicit mock before the request.
    """
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.fixture
def mock_connected_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit mock of both probes returning a live connection."""

    async def _connected(_resource: object) -> str:
        return "connected"

    monkeypatch.setattr("app.main.probe_database", _connected)
    monkeypatch.setattr("app.main.probe_redis", _connected)


@pytest.fixture
def mock_engine() -> MagicMock:
    """Explicit mock of an async SQLAlchemy connection."""
    connection = AsyncMock()
    connection.execute = AsyncMock()
    context = AsyncMock()
    context.__aenter__.return_value = connection
    context.__aexit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = context
    engine.connection = connection
    return engine


async def test_health_reports_connected_dependencies(
    api_client: AsyncClient,
    mock_connected_probes: None,
) -> None:
    response = await api_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "db": "connected",
        "redis": "connected",
    }


async def test_health_degrades_when_database_probe_fails(
    api_client: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _db_down(_engine: object) -> str:
        return "disconnected"

    async def _redis_up(_client: object) -> str:
        return "connected"

    monkeypatch.setattr("app.main.probe_database", _db_down)
    monkeypatch.setattr("app.main.probe_redis", _redis_up)

    response = await api_client.get("/health")

    assert response.status_code == 503
    assert response.json() == {
        "status": "degraded",
        "db": "disconnected",
        "redis": "connected",
    }


async def test_probe_database_runs_select_one(mock_engine: MagicMock) -> None:
    state = await probe_database(mock_engine)

    assert state == "connected"
    statement = mock_engine.connection.execute.await_args.args[0]
    assert str(statement) == "SELECT 1"


async def test_probe_database_reports_disconnected_on_driver_error(
    mock_engine: MagicMock,
) -> None:
    mock_engine.connection.execute.side_effect = OperationalError(
        "SELECT 1", {}, Exception("connection refused")
    )

    assert await probe_database(mock_engine) == "disconnected"


async def test_probe_redis_requires_ping() -> None:
    client = AsyncMock()
    client.ping = AsyncMock(return_value=True)

    assert await probe_redis(client) == "connected"
    client.ping.assert_awaited()


async def test_probe_redis_reports_disconnected_when_ping_fails() -> None:
    client = AsyncMock()
    client.ping = AsyncMock(side_effect=ConnectionError("redis down"))

    assert await probe_redis(client) == "disconnected"
