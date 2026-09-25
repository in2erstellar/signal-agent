"""UTC hour, day, and token counters for AI reviews.

The reserve script increments the call windows and the estimated tokens, then
rolls all three back when any cap would be exceeded. A quiet hour cannot spend
past the daily call cap, and a day with call room cannot spend past the token cap.
"""

from __future__ import annotations

from datetime import datetime, timezone

from redis.asyncio import Redis

from app.config import (
    AI_REVIEW_DAILY_BUDGET,
    AI_REVIEW_DAILY_TOKEN_BUDGET,
    AI_REVIEW_HOURLY_BUDGET,
)

_HOUR_TTL_SECONDS = 60 * 60 * 2
_DAY_TTL_SECONDS = 60 * 60 * 24 * 3

_RESERVE = """
local hour = redis.call('INCR', KEYS[1])
if hour == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
local day = redis.call('INCR', KEYS[2])
if day == 1 then redis.call('EXPIRE', KEYS[2], ARGV[2]) end
local tokens = redis.call('INCRBY', KEYS[3], ARGV[5])
if tonumber(tokens) == tonumber(ARGV[5]) then redis.call('EXPIRE', KEYS[3], ARGV[2]) end
if tonumber(hour) > tonumber(ARGV[3]) or tonumber(day) > tonumber(ARGV[4]) or tonumber(tokens) > tonumber(ARGV[6]) then
  redis.call('DECR', KEYS[1])
  redis.call('DECR', KEYS[2])
  redis.call('DECRBY', KEYS[3], ARGV[5])
  return 0
end
return 1
"""

_RELEASE = """
local hour = tonumber(redis.call('GET', KEYS[1]) or '0')
if hour > 0 then redis.call('DECR', KEYS[1]) end
local day = tonumber(redis.call('GET', KEYS[2]) or '0')
if day > 0 then redis.call('DECR', KEYS[2]) end
local tokens = tonumber(redis.call('GET', KEYS[3]) or '0')
local delta = tonumber(ARGV[1])
if delta > tokens then delta = tokens end
if delta > 0 then redis.call('DECRBY', KEYS[3], delta) end
return 1
"""

_RAISE_FLOOR = """
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
local floor = tonumber(ARGV[1])
if floor > cur then
  redis.call('SET', KEYS[1], floor)
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return tonumber(redis.call('GET', KEYS[1]) or '0')
"""

_RECONCILE = """
local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
local delta = tonumber(ARGV[1])
local next = cur + delta
if next < 0 then next = 0 end
redis.call('SET', KEYS[1], next)
redis.call('EXPIRE', KEYS[1], ARGV[2])
return next
"""


def _aware(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def hour_key(moment: datetime) -> str:
    stamp = _aware(moment).strftime("%Y%m%d%H")
    return f"ai_review:hour:{stamp}"


def day_key(moment: datetime) -> str:
    stamp = _aware(moment).strftime("%Y%m%d")
    return f"ai_review:day:{stamp}"


def tokens_key(moment: datetime) -> str:
    stamp = _aware(moment).strftime("%Y%m%d")
    return f"ai_review:tokens:{stamp}"


async def review_counts(redis: Redis, moment: datetime) -> tuple[int, int]:
    hour_raw, day_raw = await redis.mget(hour_key(moment), day_key(moment))
    return _count(hour_raw), _count(day_raw)


async def tokens_used(redis: Redis, moment: datetime) -> int:
    raw = await redis.get(tokens_key(moment))
    return _count(raw)


async def reserve_review(redis: Redis, moment: datetime, *, tokens: int) -> bool:
    reserved = await redis.eval(
        _RESERVE,
        3,
        hour_key(moment),
        day_key(moment),
        tokens_key(moment),
        str(_HOUR_TTL_SECONDS),
        str(_DAY_TTL_SECONDS),
        str(AI_REVIEW_HOURLY_BUDGET),
        str(AI_REVIEW_DAILY_BUDGET),
        str(max(0, tokens)),
        str(AI_REVIEW_DAILY_TOKEN_BUDGET),
    )
    return int(reserved) == 1


async def release_review(redis: Redis, moment: datetime, *, tokens: int) -> None:
    await redis.eval(
        _RELEASE,
        3,
        hour_key(moment),
        day_key(moment),
        tokens_key(moment),
        str(max(0, tokens)),
    )


async def raise_token_floor(redis: Redis, moment: datetime, used: int) -> int:
    """Move the daily token counter up to ``used`` when the provider reports it."""
    stored = await redis.eval(
        _RAISE_FLOOR,
        1,
        tokens_key(moment),
        str(max(0, used)),
        str(_DAY_TTL_SECONDS),
    )
    return int(stored)


async def raise_call_floor(redis: Redis, moment: datetime, used: int) -> int:
    """Move the daily call counter up to ``used`` when the provider reports RPD."""
    stored = await redis.eval(
        _RAISE_FLOOR,
        1,
        day_key(moment),
        str(max(0, used)),
        str(_DAY_TTL_SECONDS),
    )
    return int(stored)


async def reconcile_tokens(redis: Redis, moment: datetime, reserved: int, actual: int) -> int:
    """Replace a pre-call estimate with the provider's total_tokens."""
    stored = await redis.eval(
        _RECONCILE,
        1,
        tokens_key(moment),
        str(actual - reserved),
        str(_DAY_TTL_SECONDS),
    )
    return int(stored)


def _count(raw: object) -> int:
    if raw is None:
        return 0
    try:
        return int(str(raw))
    except ValueError:
        return 0
