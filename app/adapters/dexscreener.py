"""DexScreener pair lookup. Used only for a USD price and liquidity reading.

Endpoint: ``GET /tokens/v1/{chainId}/{tokenAddresses}`` on the public API
(https://docs.dexscreener.com/api/reference, no key, 300 requests/min).
It lists PumpSwap pairs, which the Raydium API does not.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx

REQUEST_TIMEOUT_SECONDS = 10.0
SOLANA_CHAIN_ID = "solana"
# The tokens endpoint takes at most this many comma-separated addresses.
MAX_TOKENS_PER_REQUEST = 30


class DexScreenerError(Exception):
    """The DexScreener HTTP API did not return a usable pair list."""


@dataclass(frozen=True)
class DexQuote:
    """The highest-liquidity pair where the token is the base token."""

    pair_address: str
    dex_id: str | None
    price_usd: Decimal | None
    liquidity_usd: Decimal | None
    # ``txns.m5``; None when the pair omits it.
    buys_5m: int | None = None
    sells_5m: int | None = None
    # Links from a DexScreener token profile, when one was submitted.
    social_links: tuple[str, ...] = ()
    # ``marketCap``, or ``fdv`` when the pair omits it.
    market_cap_usd: Decimal | None = None


class DexScreenerPrices:
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

    async def token_quote(self, mint: str) -> DexQuote | None:
        """``None`` when DexScreener lists no pair. HTTP failures raise ``DexScreenerError``."""
        return _best_pair(await self._pairs([mint]), mint)

    async def token_quotes(self, mints: list[str]) -> dict[str, DexQuote]:
        """Best pair per mint, one request per 30 mints. Mints with no pair are left out."""
        quotes: dict[str, DexQuote] = {}
        for start in range(0, len(mints), MAX_TOKENS_PER_REQUEST):
            chunk = mints[start : start + MAX_TOKENS_PER_REQUEST]
            payload = await self._pairs(chunk)
            for mint in chunk:
                quote = _best_pair(payload, mint)
                if quote is not None:
                    quotes[mint] = quote
        return quotes

    async def _pairs(self, mints: list[str]) -> Any:
        try:
            response = await self._client.get(
                f"{self._base_url}/tokens/v1/{SOLANA_CHAIN_ID}/{','.join(mints)}",
                timeout=self._timeout,
            )
        except httpx.HTTPError as exc:
            raise DexScreenerError(f"DexScreener request failed: {type(exc).__name__}") from exc
        if response.status_code != 200:
            raise DexScreenerError(f"DexScreener lookup failed with HTTP {response.status_code}")
        return response.json()


def _best_pair(payload: Any, mint: str) -> DexQuote | None:
    if not isinstance(payload, list):
        raise DexScreenerError("DexScreener pair list has an unexpected shape")
    quotes: list[DexQuote] = []
    for pair in payload:
        if not isinstance(pair, dict):
            continue
        base = pair.get("baseToken")
        if not isinstance(base, dict) or base.get("address") != mint:
            continue
        address = pair.get("pairAddress")
        if not isinstance(address, str) or not address:
            continue
        liquidity = pair.get("liquidity")
        dex_id = pair.get("dexId")
        buys, sells = _five_minute_trades(pair.get("txns"))
        quotes.append(
            DexQuote(
                pair_address=address,
                dex_id=dex_id if isinstance(dex_id, str) else None,
                price_usd=_positive_decimal(pair.get("priceUsd")),
                liquidity_usd=_positive_decimal(
                    liquidity.get("usd") if isinstance(liquidity, dict) else None
                ),
                buys_5m=buys,
                sells_5m=sells,
                social_links=_profile_links(pair.get("info")),
                market_cap_usd=_positive_decimal(pair.get("marketCap"))
                or _positive_decimal(pair.get("fdv")),
            )
        )
    if not quotes:
        return None
    return max(quotes, key=lambda quote: quote.liquidity_usd or Decimal(0))


def _five_minute_trades(raw: object) -> tuple[int | None, int | None]:
    window = raw.get("m5") if isinstance(raw, dict) else None
    if not isinstance(window, dict):
        return None, None
    return _count(window.get("buys")), _count(window.get("sells"))


def _count(raw: object) -> int | None:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        return None
    return raw


def _profile_links(raw: object) -> tuple[str, ...]:
    if not isinstance(raw, dict):
        return ()
    links: list[str] = []
    for key in ("websites", "socials"):
        for item in raw.get(key) or []:
            url = item.get("url") if isinstance(item, dict) else None
            if isinstance(url, str) and url:
                links.append(url)
    return tuple(links)


def _positive_decimal(raw: object) -> Decimal | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float, str)):
        return None
    try:
        value = Decimal(str(raw))
    except Exception:
        return None
    if not value.is_finite() or value <= 0:
        return None
    return value
