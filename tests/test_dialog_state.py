"""Redis dialog state expires through SET EX, not a stored timestamp."""

from __future__ import annotations

import asyncio

from redis.asyncio import Redis

from app.services.dialog_state import clear_pending_input, get_pending_input, set_pending_input

pytest_plugins = ["tests.pipeline_containers"]

TELEGRAM_ID = 8700246141


async def test_set_get_and_clear(pipeline_infra: object) -> None:
    redis = Redis.from_url(pipeline_infra.redis_url)  # type: ignore[attr-defined]
    try:
        await set_pending_input(redis, TELEGRAM_ID, "awaiting_wallet_address")
        assert await get_pending_input(redis, TELEGRAM_ID) == "awaiting_wallet_address"
        await clear_pending_input(redis, TELEGRAM_ID)
        assert await get_pending_input(redis, TELEGRAM_ID) is None
    finally:
        await redis.aclose()


async def test_pending_input_expires(pipeline_infra: object) -> None:
    redis = Redis.from_url(pipeline_infra.redis_url)  # type: ignore[attr-defined]
    try:
        await set_pending_input(redis, TELEGRAM_ID, "awaiting_wallet_address", ttl_seconds=1)
        assert await get_pending_input(redis, TELEGRAM_ID) == "awaiting_wallet_address"
        await asyncio.sleep(1.2)
        assert await get_pending_input(redis, TELEGRAM_ID) is None
    finally:
        await redis.aclose()
