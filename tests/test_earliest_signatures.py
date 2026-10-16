"""Oldest-first signature paging used by the launch-trades line. Fixtures only."""

from __future__ import annotations

import json

import httpx
import pytest

from app.adapters.helius import HeliusClient, HistoryTooLong

ADDRESS = "Curve11111111111111111111111111111111111111"


def _rows(start: int, count: int) -> list[dict[str, object]]:
    # Newest first, as the RPC returns them.
    return [
        {"signature": f"sig-{n}", "slot": n, "err": None}
        for n in range(start + count - 1, start - 1, -1)
    ]


class _Pages:
    def __init__(self, pages: list[list[dict[str, object]]]) -> None:
        self.pages = list(pages)
        self.befores: list[str | None] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["method"] == "getSignaturesForAddress"
        self.befores.append(body["params"][1].get("before"))
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": self.pages.pop(0)})


async def _no_sleep(_delay: float) -> None:
    return None


async def _earliest(pages: _Pages, max_signatures: int) -> list[dict[str, object]]:
    async with httpx.AsyncClient(transport=httpx.MockTransport(pages.handler)) as http:
        client = HeliusClient(
            api_key="test-key",
            api_base_url="https://api.helius.xyz",
            rpc_url="https://mainnet.helius-rpc.com",
            client=http,
            sleep=_no_sleep,
        )
        return [
            dict(row)
            for row in await client.earliest_signatures(ADDRESS, max_signatures=max_signatures)
        ]


async def test_pages_back_to_the_start_and_returns_oldest_first() -> None:
    pages = _Pages([_rows(500, 1000), _rows(0, 500)])
    rows = await _earliest(pages, max_signatures=5000)
    assert pages.befores == [None, "sig-500"]
    assert len(rows) == 1500
    assert rows[0]["slot"] == 0
    assert rows[-1]["slot"] == 1499


async def test_history_longer_than_the_cap_is_an_error_not_a_partial_start() -> None:
    pages = _Pages([_rows(1000, 1000), _rows(0, 1000)])
    with pytest.raises(HistoryTooLong):
        await _earliest(pages, max_signatures=1000)
