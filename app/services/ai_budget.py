"""UTC hour and day counters for AI reviews.

The reserve script increments both windows and rolls both back when either
cap would be exceeded, so a quiet hour cannot spend past the daily cap.
"""

from __future__ import annotations

from datetime import datetime, timezone

from redis.asyncio import Redis

from app.config import AI_REVIEW_DAILY_BUDGET, AI_REVIEW_HOURLY_BUDGET

_HOUR_TTL_SECONDS = 60 * 60 * 2
_DAY_TTL_SECONDS = 60 * 60 * 24 * 3

_RESERVE = """
local hour = redis.call('INCR', KEYS[1])
if hour == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
local day = redis.call('INCR', KEYS[2])
if day == 1 then redis.call('EXPIRE', KEYS[2], ARGV[2]) end
if tonumber(hour) > tonumber(ARGV[3]) or tonumber(day) > tonumber(ARGV[4]) then
  redis.call('DECR', KEYS[1])
  redis.call('DECR', KEYS[2])
  return 0
end
return 1
"""

_RELEASE = """
local hour = tonumber(redis.call('GET', KEYS[1]) or '0')
if hour > 0 then redis.call('DECR', KEYS[1]) end
local day = tonumber(redis.call('GET', KEYS[2]) or '0')
if day > 0 then redis.call('DECR', KEYS[2]) end
return 1
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


async def review_counts(redis: Redis, moment: datetime) -> tuple[int, int]:
    hour_raw, day_raw = await redis.mget(hour_key(moment), day_key(moment))
    return _count(hour_raw), _count(day_raw)


async def reserve_review(redis: Redis, moment: datetime) -> bool:
    reserved = await redis.eval(
        _RESERVE,
        2,
        hour_key(moment),
        day_key(moment),
        str(_HOUR_TTL_SECONDS),
        str(_DAY_TTL_SECONDS),
        str(AI_REVIEW_HOURLY_BUDGET),
        str(AI_REVIEW_DAILY_BUDGET),
    )
    return int(reserved) == 1


async def release_review(redis: Redis, moment: datetime) -> None:
    await redis.eval(
        _RELEASE,
        2,
        hour_key(moment),
        day_key(moment),
    )


def _count(raw: object) -> int:
    if raw is None:
        return 0
    try:
        return int(str(raw))
    except ValueError:
        return 0
