"""Helius webhook address edits, and the database rollback when the edit fails.

The HTTP client is a mock. No request leaves the test process.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.helius import HeliusClient, HeliusError
from app.adapters.telegram_bot import cmd_unwatch, cmd_watch
from app.db.models import TrackedWallet
from tests.factories import make_token, make_wallet_tx
from tests.pipeline_containers import truncate_pipeline
from tests.test_telegram_bot import TELEGRAM_ID, _command, _context

pytest_plugins = ["tests.pipeline_containers"]

WEBHOOK_ID = "wh_test"
ADDRESS = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
KEPT = "So11111111111111111111111111111111111111112"


def _payload(addresses: list[str]) -> dict[str, object]:
    return {
        "webhookID": WEBHOOK_ID,
        "webhookURL": "https://example.test/webhooks/helius",
        "webhookType": "enhanced",
        "accountAddresses": addresses,
        "transactionTypes": ["SWAP", "TRANSFER"],
        "authHeader": "secret-header",
        "txnStatus": "all",
    }


async def _noop_sleep(_seconds: float) -> None:
    return None


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> HeliusClient:
    return HeliusClient(
        api_key="test-key",
        api_base_url="https://api.helius.xyz",
        rpc_url="https://mainnet.helius-rpc.com",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        sleep=_noop_sleep,
        max_attempts=1,
    )


async def test_add_address_puts_the_existing_config_plus_the_new_address() -> None:
    seen: list[tuple[str, dict[str, object] | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        seen.append((request.method, body))
        assert request.url.params["api-key"] == "test-key"
        if request.method == "GET":
            return httpx.Response(200, json=_payload([KEPT]))
        assert body is not None
        assert body["webhookURL"] == "https://example.test/webhooks/helius"
        assert body["webhookType"] == "enhanced"
        assert body["transactionTypes"] == ["SWAP", "TRANSFER"]
        assert body["authHeader"] == "secret-header"
        assert body["txnStatus"] == "all"
        assert body["accountAddresses"] == [KEPT, ADDRESS]
        saved = dict(body)
        saved["webhookID"] = WEBHOOK_ID
        return httpx.Response(200, json=saved)

    client = _client(handler)
    assert await client.add_address_to_webhook(WEBHOOK_ID, ADDRESS) is True
    assert [method for method, _body in seen] == ["GET", "PUT"]


async def test_add_address_does_not_duplicate_an_address_already_on_the_webhook() -> None:
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        return httpx.Response(200, json=_payload([ADDRESS]))

    client = _client(handler)
    assert await client.add_address_to_webhook(WEBHOOK_ID, ADDRESS) is True
    assert methods == ["GET"]


async def test_remove_address_keeps_the_other_addresses() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=_payload([KEPT, ADDRESS]))
        body = json.loads(request.content)
        assert body["accountAddresses"] == [KEPT]
        saved = dict(body)
        saved["webhookID"] = WEBHOOK_ID
        return httpx.Response(200, json=saved)

    client = _client(handler)
    assert await client.remove_address_from_webhook(WEBHOOK_ID, ADDRESS) is True


async def test_network_error_raises_and_watch_does_not_keep_the_wallet(
    pipeline_infra: object,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down")

    client = _client(handler)
    with pytest.raises(HeliusError):
        await client.add_address_to_webhook(WEBHOOK_ID, ADDRESS)

    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        context = _context(sessions)
        context.bot_data["helius"] = _FailingWebhooks()
        context.bot_data["webhook_id"] = WEBHOOK_ID
        update = _command(TELEGRAM_ID)
        context.args = [ADDRESS, "desk"]
        await cmd_watch(update, context)
        assert "не удалось подписаться" in update.message.reply_text.await_args.args[0].lower()
        async with sessions() as session:
            row = await session.scalar(select(TrackedWallet).where(TrackedWallet.address == ADDRESS))
        assert row is None
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


async def test_unwatch_keeps_the_wallet_when_helius_fails(pipeline_infra: object) -> None:
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        async with sessions() as session:
            async with session.begin():
                session.add(
                    TrackedWallet(
                        address=ADDRESS,
                        label="desk",
                        first_seen_at=datetime.now(timezone.utc),
                    )
                )
        context = _context(sessions)
        context.bot_data["helius"] = _FailingWebhooks()
        context.bot_data["webhook_id"] = WEBHOOK_ID
        update = _command(TELEGRAM_ID)
        context.args = [ADDRESS]
        await cmd_unwatch(update, context)
        assert "кошелёк оставлен" in update.message.reply_text.await_args.args[0].lower()
        async with sessions() as session:
            row = await session.scalar(select(TrackedWallet).where(TrackedWallet.address == ADDRESS))
        assert row is not None
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


async def test_unwatch_with_trades_does_not_call_helius(pipeline_infra: object) -> None:
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    hooks = _Recording()
    try:
        async with sessions() as session:
            async with session.begin():
                token = make_token()
                wallet = TrackedWallet(address=ADDRESS, label="desk")
                session.add_all([token, wallet])
                await session.flush()
                session.add(
                    make_wallet_tx(
                        wallet_id=wallet.id,
                        token_id=token.id,
                        action="buy",
                    )
                )
        context = _context(sessions)
        context.bot_data["helius"] = hooks
        context.bot_data["webhook_id"] = WEBHOOK_ID
        update = _command(TELEGRAM_ID)
        context.args = [ADDRESS]
        await cmd_unwatch(update, context)
        assert "сделки" in update.message.reply_text.await_args.args[0]
        assert hooks.removed == []
        async with sessions() as session:
            row = await session.scalar(select(TrackedWallet).where(TrackedWallet.address == ADDRESS))
        assert row is not None
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


class _FailingWebhooks:
    async def add_address_to_webhook(self, webhook_id: str, address: str) -> bool:
        raise HeliusError("down")

    async def remove_address_from_webhook(self, webhook_id: str, address: str) -> bool:
        raise HeliusError("down")


class _Recording:
    def __init__(self) -> None:
        self.removed: list[tuple[str, str]] = []

    async def remove_address_from_webhook(self, webhook_id: str, address: str) -> bool:
        self.removed.append((webhook_id, address))
        return True
