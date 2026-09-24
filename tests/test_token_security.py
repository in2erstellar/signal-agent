"""Adapter shape tests. RPC and Raydium responses are fixtures, not live calls."""

from __future__ import annotations

import httpx
import pytest

from app.adapters.helius import HeliusResponseError
from app.adapters.raydium import RaydiumError, RaydiumPools, _lp_mint
from app.adapters.token_security import (
    INCINERATOR,
    RAYDIUM_AMM_V4,
    RAYDIUM_AMM_V4_AUTHORITY,
    RAYDIUM_LP_LOCK,
    HeliusTokenSecurity,
    _mint_view,
)

MINT = "So11111111111111111111111111111111111111112"
LP = "8HoQnePLqPj4M7PUDzfw8e3Ymdwgc7NLGnaTUapubyvu"


class QueueHelius:
    def __init__(self, results: list[dict]) -> None:
        self._results = list(results)
        self.calls: list[tuple[str, list]] = []

    async def rpc(self, method: str, params: list) -> dict:
        self.calls.append((method, params))
        if not self._results:
            raise AssertionError(f"unexpected rpc {method}")
        return self._results.pop(0)


def _mint_account(
    *,
    mint_authority: str | None,
    freeze_authority: str | None,
    supply: str,
) -> dict:
    return {
        "data": {
            "parsed": {
                "info": {
                    "mintAuthority": mint_authority,
                    "freezeAuthority": freeze_authority,
                    "supply": supply,
                }
            }
        }
    }


def test_mint_view_reads_null_authorities() -> None:
    view = _mint_view(_mint_account(mint_authority=None, freeze_authority=None, supply="42"))
    assert view.mint_authority is None
    assert view.freeze_authority is None
    assert view.supply_raw == 42


def test_mint_view_rejects_an_unparsed_account() -> None:
    with pytest.raises(HeliusResponseError):
        _mint_view({"data": {"parsed": {"info": {}}}})


def test_lp_mint_reads_address_object_and_empty_list() -> None:
    payload = {
        "success": True,
        "data": {"data": [{"lpMint": {"address": LP, "programId": "token"}}]},
    }
    assert _lp_mint(payload) == LP
    assert _lp_mint({"success": True, "data": {"data": []}}) is None


def test_lp_mint_rejects_error_payload() -> None:
    with pytest.raises(RaydiumError):
        _lp_mint({"success": False, "data": {}})


async def test_raydium_client_uses_mint_query_without_a_socket() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["mint1"] = request.url.params["mint1"]
        return httpx.Response(
            200,
            json={"success": True, "data": {"data": [{"lpMint": {"address": LP}}]}},
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    pools = RaydiumPools("https://api-v3.raydium.io", client=client)
    assert await pools.lp_mint_for_token(MINT) == LP
    assert seen == {"path": "/pools/info/mint", "mint1": MINT}
    await client.aclose()


async def test_holder_book_marks_amm_vault_and_lp_book_marks_burn_and_lock() -> None:
    helius = QueueHelius(
        [
            {"value": _mint_account(mint_authority=None, freeze_authority=None, supply="1000")},
            {"value": [{"address": "vault-ata", "amount": "400"}, {"address": "user-ata", "amount": "100"}]},
            {
                "value": [
                    {"data": {"parsed": {"info": {"owner": RAYDIUM_AMM_V4_AUTHORITY}}}},
                    {"data": {"parsed": {"info": {"owner": "HolderA"}}}},
                ]
            },
            {
                "value": [
                    {"owner": RAYDIUM_AMM_V4},
                    {"owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
                ]
            },
            {"value": _mint_account(mint_authority=None, freeze_authority=None, supply="1000")},
            {
                "value": [
                    {"address": "burn-ata", "amount": "900"},
                    {"address": "lock-ata", "amount": "50"},
                ]
            },
            {
                "value": [
                    {"data": {"parsed": {"info": {"owner": INCINERATOR}}}},
                    {"data": {"parsed": {"info": {"owner": "LockAccount"}}}},
                ]
            },
            {"value": [{"owner": "11111111111111111111111111111111"}, {"owner": RAYDIUM_LP_LOCK}]},
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"success": True, "data": {"data": [{"lpMint": {"address": LP}}]}},
        )

    raydium = RaydiumPools(
        "https://api-v3.raydium.io",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    facts = HeliusTokenSecurity(helius, raydium)  # type: ignore[arg-type]

    holders = await facts.holder_book(MINT)
    assert holders.supply_raw == 1000
    assert [(row.owner, row.amount_raw, row.is_lp_pool) for row in holders.rows] == [
        (RAYDIUM_AMM_V4_AUTHORITY, 400, True),
        ("HolderA", 100, False),
    ]

    lp_book = await facts.lp_book(MINT)
    assert lp_book.lp_mint == LP
    assert lp_book.supply_raw == 1000
    assert [(row.owner, row.is_burn_address, row.is_lock_program) for row in lp_book.rows] == [
        (INCINERATOR, True, False),
        ("LockAccount", False, True),
    ]
    await raydium.aclose()
