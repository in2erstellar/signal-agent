"""RPC first-funding lookup. The transport is a fake, not Helius."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import pytest

from app.adapters.helius import (
    FundingDepthExceeded,
    FundingInfo,
    HeliusClient,
    funding_from_transaction,
)
from app.config import FUNDER_LOOKUP_METHOD, MAX_FUNDING_LOOKUP_SIGNATURES

WALLET = "Wallet11111111111111111111111111111111111111"
SENDER = "Sender11111111111111111111111111111111111111"


def _sig(name: str, *, block_time: int = 1_700_000_000) -> dict[str, object]:
    return {"signature": name, "slot": 10, "err": None, "blockTime": block_time}


def _transfer(signature: str, *, block_time: int = 1_700_000_000) -> dict[str, object]:
    gain = 500_000_000
    fee = 5_000
    sender_pre = 1_000_000_000
    return {
        "slot": 42,
        "blockTime": block_time,
        "transaction": {
            "signatures": [signature],
            "message": {
                "accountKeys": [
                    {"pubkey": SENDER, "signer": True, "writable": True},
                    {"pubkey": WALLET, "signer": False, "writable": True},
                ]
            },
        },
        "meta": {
            "err": None,
            "fee": fee,
            "preBalances": [sender_pre, 0],
            "postBalances": [sender_pre - gain - fee, gain],
        },
    }


class _Rpc:
    def __init__(self, pages: list[list[dict[str, object]]]) -> None:
        self.pages = list(pages)
        self.calls: list[tuple[str, dict[str, object]]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        method = str(body["method"])
        params = body["params"]
        config = params[1] if len(params) > 1 and isinstance(params[1], dict) else {}
        self.calls.append((method, config))
        if method == "getSignaturesForAddress":
            page = self.pages.pop(0) if self.pages else []
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": page})
        if method == "getTransaction":
            signature = str(params[0])
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": 1, "result": _transfer(signature)},
            )
        raise AssertionError(method)


async def _lookup(rpc: _Rpc, **kwargs: object) -> FundingInfo | None:
    transport = httpx.MockTransport(rpc.handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = HeliusClient(
            api_key="test-key",
            api_base_url="https://api.helius.xyz",
            rpc_url="https://mainnet.helius-rpc.com",
            client=http,
            sleep=_no_sleep,
            **kwargs,  # type: ignore[arg-type]
        )
        return await client.find_first_funding_tx(WALLET)


async def _no_sleep(_delay: float) -> None:
    return None


def test_defaults_use_rpc_and_cap_signatures() -> None:
    assert FUNDER_LOOKUP_METHOD == "rpc"
    assert MAX_FUNDING_LOOKUP_SIGNATURES == 5000


async def test_oldest_signature_on_one_page_is_the_funding_transfer() -> None:
    rpc = _Rpc([[_sig("sig-new", block_time=1_700_000_100), _sig("sig-old")]])
    info = await _lookup(rpc)
    assert info is not None
    assert info.funder == SENDER
    assert info.signature == "sig-old"
    assert info.amount_sol == Decimal("0.5")
    assert info.funded_at == datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)
    assert info.slot == 42
    assert [method for method, _config in rpc.calls] == [
        "getSignaturesForAddress",
        "getTransaction",
    ]
    assert rpc.calls[1][1]["encoding"] == "jsonParsed"
    assert "before" not in rpc.calls[0][1]


async def test_pagination_walks_before_to_the_oldest_page() -> None:
    rpc = _Rpc(
        [
            [_sig("sig-new"), _sig("sig-mid")],
            [_sig("sig-old", block_time=1_600_000_000)],
        ]
    )
    info = await _lookup(rpc, signature_page_size=2, max_funding_signatures=10)
    assert info is not None
    assert info.signature == "sig-old"
    signature_calls = [config for method, config in rpc.calls if method == "getSignaturesForAddress"]
    assert "before" not in signature_calls[0]
    assert signature_calls[1]["before"] == "sig-mid"
    assert [method for method, _config in rpc.calls].count("getTransaction") == 1
    assert rpc.calls[-1][0] == "getTransaction"


async def test_signature_cap_is_not_an_empty_history() -> None:
    rpc = _Rpc(
        [
            [_sig("sig-1"), _sig("sig-2")],
            [_sig("sig-3"), _sig("sig-4")],
            [_sig("sig-5"), _sig("sig-6")],
        ]
    )
    with pytest.raises(FundingDepthExceeded) as caught:
        await _lookup(rpc, signature_page_size=2, max_funding_signatures=4)
    assert caught.value.wallet_address == WALLET
    assert caught.value.scanned == 4
    assert [method for method, _config in rpc.calls] == [
        "getSignaturesForAddress",
        "getSignaturesForAddress",
    ]
    assert rpc.pages


async def test_no_signatures_means_there_is_no_funder() -> None:
    rpc = _Rpc([[]])
    assert await _lookup(rpc) is None
    assert [method for method, _config in rpc.calls] == ["getSignaturesForAddress"]


def test_balance_delta_names_the_sender() -> None:
    info = funding_from_transaction(WALLET, "sig-old", _transfer("sig-old"))
    assert info is not None
    assert info.funder == SENDER
    assert info.amount_sol == Decimal("0.5")


async def test_paid_flag_calls_funded_by_instead_of_rpc() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path.endswith(f"/v1/wallet/{WALLET}/funded-by")
        return httpx.Response(
            200,
            json={
                "funder": SENDER,
                "amount": "1.25",
                "signature": "paid-sig",
                "timestamp": 1_700_000_000,
                "slot": 7,
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        client = HeliusClient(
            api_key="test-key",
            api_base_url="https://api.helius.xyz",
            rpc_url="https://mainnet.helius-rpc.com",
            client=http,
            funder_lookup_method="helius_funded_by",
        )
        info = await client.find_first_funding_tx(WALLET)
    assert info is not None
    assert info.funder == SENDER
    assert info.signature == "paid-sig"
