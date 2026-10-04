"""Shared Helius 429 pause and the single in-flight send lock."""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from app.adapters.helius import HeliusClient


class _HoldingTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.inflight = 0
        self.max_inflight = 0
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        del request
        self.calls += 1
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        self.entered.set()
        await self.release.wait()
        self.inflight -= 1
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": 1, "result": {"ok": True}},
        )


class _RateLimitTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        del request
        self.calls += 1
        if self.calls == 1:
            return httpx.Response(429, headers={"Retry-After": "5"})
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": 1, "result": {"ok": True}},
        )


def _client(http: httpx.AsyncClient, sleep: object) -> HeliusClient:
    return HeliusClient(
        api_key="test-key",
        api_base_url="https://api.helius.xyz",
        rpc_url="https://mainnet.helius-rpc.com",
        client=http,
        sleep=sleep,  # type: ignore[arg-type]
    )


async def test_send_lock_keeps_one_http_call_in_flight() -> None:
    transport = _HoldingTransport()
    async with httpx.AsyncClient(transport=transport) as http:
        client = _client(http, asyncio.sleep)
        first = asyncio.create_task(client.rpc("getHealth", []))
        await transport.entered.wait()
        second = asyncio.create_task(client.rpc("getHealth", []))
        await asyncio.sleep(0)
        assert transport.calls == 1
        assert transport.max_inflight == 1
        transport.release.set()
        await asyncio.gather(first, second)
    assert transport.calls == 2
    assert transport.max_inflight == 1


async def test_one_429_holds_every_caller_until_the_shared_deadline() -> None:
    transport = _RateLimitTransport()
    sleeps: list[float] = []
    sleeping = 0
    peak_sleepers = 0
    release = asyncio.Event()
    paused = asyncio.Event()

    async def sleep(delay: float) -> None:
        nonlocal sleeping, peak_sleepers
        sleeps.append(delay)
        sleeping += 1
        peak_sleepers = max(peak_sleepers, sleeping)
        paused.set()
        await release.wait()
        sleeping -= 1

    async with httpx.AsyncClient(transport=transport) as http:
        client = _client(http, sleep)

        async def call() -> dict[str, object]:
            return await client.rpc("getHealth", [])

        first = asyncio.create_task(call())
        await paused.wait()
        peers = [asyncio.create_task(call()) for _ in range(3)]
        for _ in range(20):
            if peak_sleepers >= 4:
                break
            await asyncio.sleep(0)
        assert transport.calls == 1
        assert peak_sleepers == 4
        assert sleeps == pytest.approx([5.0, 5.0, 5.0, 5.0], abs=0.05)
        release.set()
        results = await asyncio.gather(first, *peers)
    assert results == [{"ok": True}] * 4
    assert transport.calls == 5


async def test_later_retry_after_keeps_the_shared_deadline_open() -> None:
    async def noop(_delay: float) -> None:
        return None

    async with httpx.AsyncClient() as http:
        client = _client(http, noop)
        await asyncio.gather(
            client._extend_pause(1),
            client._extend_pause(5),
            client._extend_pause(2),
        )
        remaining = client._paused_until - time.monotonic()
    assert remaining == pytest.approx(5, abs=0.1)
