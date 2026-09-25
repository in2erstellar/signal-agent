"""Short-lived dialog state in Redis.

The key expires through Redis itself. Callers do not store a timestamp.
"""

from __future__ import annotations

from redis.asyncio import Redis


def _key(telegram_id: int) -> str:
    return f"dialog:{telegram_id}"


async def set_pending_input(
    redis: Redis,
    telegram_id: int,
    state: str,
    ttl_seconds: int = 300,
) -> None:
    await redis.set(_key(telegram_id), state, ex=ttl_seconds)


async def get_pending_input(redis: Redis, telegram_id: int) -> str | None:
    raw = await redis.get(_key(telegram_id))
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode()
    return str(raw)


async def clear_pending_input(redis: Redis, telegram_id: int) -> None:
    await redis.delete(_key(telegram_id))
