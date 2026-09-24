"""Raydium pool lookup. Used only to discover an LP mint, not to judge safety.

Endpoint: ``GET /pools/info/mint`` on the public API v3.
https://docs.raydium.io/reference/program-addresses points at
``https://api-v3.raydium.io`` as the live API host.
"""

from __future__ import annotations

from typing import Any

import httpx

REQUEST_TIMEOUT_SECONDS = 10.0


class RaydiumError(Exception):
    """The Raydium HTTP API did not return a usable pool list."""


class RaydiumPools:
    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = httpx.Timeout(timeout_seconds)
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=self._timeout)

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def lp_mint_for_token(self, mint: str) -> str | None:
        """LP mint of the highest-liquidity pool that includes ``mint``, if any."""
        response = await self._client.get(
            f"{self._base_url}/pools/info/mint",
            params={
                "mint1": mint,
                "poolType": "all",
                "poolSortField": "liquidity",
                "sortType": "desc",
                "pageSize": "1",
                "page": "1",
            },
            timeout=self._timeout,
        )
        if response.status_code != 200:
            raise RaydiumError(f"Raydium pool lookup failed with HTTP {response.status_code}")
        payload = response.json()
        return _lp_mint(payload)


def _lp_mint(payload: Any) -> str | None:
    if not isinstance(payload, dict) or payload.get("success") is False:
        raise RaydiumError("Raydium pool lookup returned an error payload")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise RaydiumError("Raydium pool lookup is missing data")
    pools = data.get("data")
    if not pools:
        return None
    if not isinstance(pools, list):
        raise RaydiumError("Raydium pool list has an unexpected shape")
    lp_mint = pools[0].get("lpMint") if isinstance(pools[0], dict) else None
    if isinstance(lp_mint, dict) and isinstance(lp_mint.get("address"), str):
        return str(lp_mint["address"])
    if isinstance(lp_mint, str) and lp_mint:
        return lp_mint
    raise RaydiumError("Raydium pool is missing lpMint")
