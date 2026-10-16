"""DexScreener price reading and the PumpSwap pool quote built from it. Fixtures only."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest

from app.adapters.dexscreener import DexQuote, DexScreenerError, DexScreenerPrices
from app.adapters.token_security import HeliusTokenSecurity
from tests.test_pumpswap import (
    KNOWN_MINT,
    LP_MINT,
    EmptyRaydium,
    QueueHelius,
    _account,
    _encode_pool,
)

BASE_URL = "https://dex.test"


def _pair(
    address: str,
    *,
    base: str = KNOWN_MINT,
    price: object = "0.0000021",
    liquidity: object = 2109.5,
) -> dict:
    return {
        "chainId": "solana",
        "dexId": "pumpswap",
        "pairAddress": address,
        "baseToken": {"address": base, "symbol": "TEST"},
        "priceUsd": price,
        "liquidity": {"usd": liquidity},
    }


def _prices(handler) -> DexScreenerPrices:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return DexScreenerPrices(BASE_URL, client=client)


async def test_highest_liquidity_base_pair_wins() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(
            200,
            json=[
                _pair("small", liquidity=10),
                _pair("quote-side", base="Other111", liquidity=99_999),
                _pair("big", liquidity=5000),
            ],
        )

    quote = await _prices(handler).token_quote(KNOWN_MINT)
    assert seen == [f"{BASE_URL}/tokens/v1/solana/{KNOWN_MINT}"]
    assert quote == DexQuote(
        pair_address="big",
        dex_id="pumpswap",
        price_usd=Decimal("0.0000021"),
        liquidity_usd=Decimal("5000"),
    )


async def test_empty_list_is_no_pair() -> None:
    quote = await _prices(lambda request: httpx.Response(200, json=[])).token_quote(KNOWN_MINT)
    assert quote is None


async def test_zero_price_is_null_not_zero() -> None:
    handler = lambda request: httpx.Response(200, json=[_pair("p", price="0", liquidity=None)])  # noqa: E731
    quote = await _prices(handler).token_quote(KNOWN_MINT)
    assert quote is not None
    assert quote.price_usd is None
    assert quote.liquidity_usd is None


@pytest.mark.parametrize(
    "response",
    [httpx.Response(429, json={}), httpx.Response(200, json={"pairs": []})],
)
async def test_bad_response_raises(response: httpx.Response) -> None:
    with pytest.raises(DexScreenerError):
        await _prices(lambda request: response).token_quote(KNOWN_MINT)


async def test_transport_error_raises_dexscreener_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("slow", request=request)

    with pytest.raises(DexScreenerError, match="ConnectTimeout"):
        await _prices(handler).token_quote(KNOWN_MINT)


class FixedPrices:
    def __init__(self, result: DexQuote | None | Exception) -> None:
        self._result = result

    async def token_quote(self, mint: str) -> DexQuote | None:
        if isinstance(self._result, Exception):
            raise self._result
        return self._result


async def test_pumpswap_pool_quote_takes_price_from_dexscreener() -> None:
    helius = QueueHelius([_account(_encode_pool())])
    prices = FixedPrices(DexQuote("pair", "pumpswap", Decimal("0.5"), Decimal("2100")))
    facts = HeliusTokenSecurity(
        helius,  # type: ignore[arg-type]
        EmptyRaydium(),  # type: ignore[arg-type]
        _pumpswap(helius),
        prices,  # type: ignore[arg-type]
    )
    quote = await facts.pool_quote(KNOWN_MINT)
    assert quote.lp_mint == LP_MINT
    assert quote.price_usd == Decimal("0.5")
    assert quote.tvl_usd == Decimal("2100")


async def test_dexscreener_failure_keeps_the_pool_with_a_null_price() -> None:
    helius = QueueHelius([_account(_encode_pool())])
    facts = HeliusTokenSecurity(
        helius,  # type: ignore[arg-type]
        EmptyRaydium(),  # type: ignore[arg-type]
        _pumpswap(helius),
        FixedPrices(DexScreenerError("HTTP 429")),  # type: ignore[arg-type]
    )
    quote = await facts.pool_quote(KNOWN_MINT)
    assert quote.lp_mint == LP_MINT
    assert quote.price_usd is None
    assert quote.tvl_usd is None


async def test_no_pool_does_not_ask_dexscreener() -> None:
    helius = QueueHelius([{"value": None}])
    facts = HeliusTokenSecurity(
        helius,  # type: ignore[arg-type]
        EmptyRaydium(),  # type: ignore[arg-type]
        _pumpswap(helius),
        FixedPrices(AssertionError("must not be called")),  # type: ignore[arg-type]
    )
    quote = await facts.pool_quote(KNOWN_MINT)
    assert quote.lp_mint is None
    assert quote.price_usd is None


def _pumpswap(helius: QueueHelius):
    from app.adapters.pumpswap import PumpSwapPools

    return PumpSwapPools(helius)  # type: ignore[arg-type]
