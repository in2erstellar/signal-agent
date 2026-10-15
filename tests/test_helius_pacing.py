"""Helius client paces sends below the plan's requests-per-second limit."""

from __future__ import annotations

import httpx
import pytest

from app.adapters.helius import HeliusClient
from app.config import HELIUS_MAX_REQUESTS_PER_SECOND, Settings


class _OkTransport(httpx.AsyncBaseTransport):
    def __init__(self, clock: list[float]) -> None:
        self.clock = clock
        self.sent_at: list[float] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        del request
        self.sent_at.append(self.clock[0])
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"ok": True}})


def _paced_client(
    http: httpx.AsyncClient,
    clock: list[float],
    sleeps: list[float],
    rps: float | None,
) -> HeliusClient:
    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay

    client = HeliusClient(
        api_key="test-key",
        api_base_url="https://api.helius.xyz",
        rpc_url="https://mainnet.helius-rpc.com",
        client=http,
        sleep=fake_sleep,
        max_requests_per_second=rps,
    )
    client._now = lambda: clock[0]  # type: ignore[method-assign]
    return client


async def test_back_to_back_sends_are_spaced_by_the_interval() -> None:
    clock = [100.0]
    sleeps: list[float] = []
    transport = _OkTransport(clock)
    async with httpx.AsyncClient(transport=transport) as http:
        client = _paced_client(http, clock, sleeps, rps=4.0)
        for _ in range(3):
            await client.rpc("getHealth", [])
    assert transport.sent_at == pytest.approx([100.0, 100.25, 100.5])
    assert sleeps == pytest.approx([0.25, 0.25])


async def test_a_send_after_an_idle_gap_does_not_wait() -> None:
    clock = [100.0]
    sleeps: list[float] = []
    transport = _OkTransport(clock)
    async with httpx.AsyncClient(transport=transport) as http:
        client = _paced_client(http, clock, sleeps, rps=4.0)
        await client.rpc("getHealth", [])
        clock[0] += 5.0
        await client.rpc("getHealth", [])
    assert transport.sent_at == pytest.approx([100.0, 105.0])
    assert sleeps == []


async def test_no_limit_sends_without_waiting() -> None:
    clock = [100.0]
    sleeps: list[float] = []
    transport = _OkTransport(clock)
    async with httpx.AsyncClient(transport=transport) as http:
        client = _paced_client(http, clock, sleeps, rps=None)
        for _ in range(3):
            await client.rpc("getHealth", [])
    assert sleeps == []


def test_a_non_positive_limit_is_rejected() -> None:
    with pytest.raises(ValueError, match="max_requests_per_second"):
        HeliusClient(
            api_key="test-key",
            api_base_url="https://api.helius.xyz",
            rpc_url="https://mainnet.helius-rpc.com",
            max_requests_per_second=0,
        )


async def test_from_settings_paces_below_the_free_plan_limit() -> None:
    settings = Settings(helius_api_key="test-key")  # type: ignore[call-arg]
    client = HeliusClient.from_settings(settings)
    try:
        assert client._min_send_interval == pytest.approx(1 / HELIUS_MAX_REQUESTS_PER_SECOND)
        assert HELIUS_MAX_REQUESTS_PER_SECOND < 10
    finally:
        await client.aclose()
