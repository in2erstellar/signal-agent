"""Daily Helius request budget: the Redis counter and the client refusing to send."""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest
from redis.asyncio import Redis

from app.adapters.helius import HeliusBudgetExceeded, HeliusClient
from app.services.helius_budget import daily_budget, day_key, requests_used, reserve_request

pytest_plugins = ["tests.pipeline_containers"]

DAY = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
NEXT_DAY = datetime(2026, 10, 16, 0, 0, 1, tzinfo=timezone.utc)


class _CountingTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        del request
        self.calls += 1
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"ok": True}})


def _client(http: httpx.AsyncClient, allowed: list[bool]) -> HeliusClient:
    async def budget() -> bool:
        return allowed.pop(0)

    return HeliusClient(
        api_key="test-key",
        api_base_url="https://api.helius.xyz",
        rpc_url="https://mainnet.helius-rpc.com",
        client=http,
        budget=budget,
    )


async def test_client_sends_while_the_budget_allows_and_then_refuses() -> None:
    transport = _CountingTransport()
    async with httpx.AsyncClient(transport=transport) as http:
        client = _client(http, [True, False])
        await client.rpc("getHealth", [])
        with pytest.raises(HeliusBudgetExceeded, match="getHealth"):
            await client.rpc("getHealth", [])
    assert transport.calls == 1


def test_day_key_is_the_utc_date() -> None:
    assert day_key(DAY) == "helius:requests:20261015"
    assert day_key(datetime(2026, 10, 15, 23, 30)) == "helius:requests:20261015"


async def test_reserve_stops_at_the_limit_and_resets_next_day(pipeline_infra: object) -> None:
    redis = Redis.from_url(pipeline_infra.redis_url, decode_responses=True)  # type: ignore[attr-defined]
    try:
        await redis.delete(day_key(DAY), day_key(NEXT_DAY))
        results = [await reserve_request(redis, DAY, daily_limit=3) for _ in range(5)]
        assert results == [True, True, True, False, False]
        assert await requests_used(redis, DAY) == 3
        assert await redis.ttl(day_key(DAY)) > 0
        assert await reserve_request(redis, NEXT_DAY, daily_limit=3) is True
    finally:
        await redis.delete(day_key(DAY), day_key(NEXT_DAY))
        await redis.aclose()


async def test_daily_budget_uses_its_clock(pipeline_infra: object) -> None:
    redis = Redis.from_url(pipeline_infra.redis_url, decode_responses=True)  # type: ignore[attr-defined]
    try:
        await redis.delete(day_key(DAY))
        reserve = daily_budget(redis, daily_limit=1, clock=lambda: DAY)
        assert await reserve() is True
        assert await reserve() is False
    finally:
        await redis.delete(day_key(DAY))
        await redis.aclose()


def test_daily_budget_rejects_a_zero_limit() -> None:
    with pytest.raises(ValueError, match="daily_limit"):
        daily_budget(object(), daily_limit=0)  # type: ignore[arg-type]
