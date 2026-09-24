"""Live connectivity probes for the health endpoint.

These functions open a real connection. Unit tests must pass an explicit mock
client; they must not pretend a skipped call was a successful probe.
"""

from typing import Literal

import structlog
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

ConnectionState = Literal["connected", "disconnected"]

logger = structlog.get_logger(__name__)


async def probe_database(engine: AsyncEngine) -> ConnectionState:
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning("database_health_failed", error_type=type(exc).__name__)
        return "disconnected"
    return "connected"


async def probe_redis(client: Redis) -> ConnectionState:
    try:
        pong = await client.ping()
    except Exception as exc:
        logger.warning("redis_health_failed", error_type=type(exc).__name__)
        return "disconnected"
    if not pong:
        logger.warning("redis_health_failed", error_type="PingRejected")
        return "disconnected"
    return "connected"
