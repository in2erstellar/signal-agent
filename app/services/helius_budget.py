"""UTC-day request counter for the Helius free plan.

Each send reserves one request. The reserve script increments the day counter
and rolls it back when the cap would be exceeded, so concurrent callers cannot
overshoot. A refused reservation means the day's budget is spent: the client
raises instead of sending, and the next UTC day starts from zero.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from redis.asyncio import Redis

_DAY_TTL_SECONDS = 60 * 60 * 24 * 3

_RESERVE = """
local day = redis.call('INCR', KEYS[1])
if day == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
if tonumber(day) > tonumber(ARGV[2]) then
  redis.call('DECR', KEYS[1])
  return 0
end
return 1
"""

Clock = Callable[[], datetime]


def day_key(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    stamp = moment.astimezone(timezone.utc).strftime("%Y%m%d")
    return f"helius:requests:{stamp}"


async def reserve_request(redis: Redis, moment: datetime, *, daily_limit: int) -> bool:
    reserved = await redis.eval(
        _RESERVE,
        1,
        day_key(moment),
        str(_DAY_TTL_SECONDS),
        str(daily_limit),
    )
    return int(reserved) == 1


async def requests_used(redis: Redis, moment: datetime) -> int:
    raw = await redis.get(day_key(moment))
    return int(raw) if raw is not None else 0


def daily_budget(
    redis: Redis,
    *,
    daily_limit: int,
    clock: Clock = lambda: datetime.now(timezone.utc),
) -> Callable[[], Awaitable[bool]]:
    """A ``reserve`` callable for ``HeliusClient(budget=...)``."""
    if daily_limit < 1:
        raise ValueError("daily_limit must be at least 1")

    async def reserve() -> bool:
        return await reserve_request(redis, clock(), daily_limit=daily_limit)

    return reserve
