"""Helius enhanced-webhook parsing and idempotent persistence."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import BAIT_CHECK_QUEUE, BAIT_RECHECK_DELAY_MINUTES, RISK_SCAN_QUEUE
from app.db.models import Token, WalletTx
from app.main import app
from app.workers.wallet_watcher import (
    WalletWatcher,
    WebhookRejected,
    parse_enhanced_payload,
)
from tests.factories import make_token, make_tracked_wallet
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

WALLET = "8cRrU1NzNpjL3k2BwjW3VixAcX6VFc29KHr4KZg8cs2Y"
MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SIGNATURE = "4jzQxVTaJ4Fe4Fct9y1aaT9hmVyEjpCqE2bL8JMnuLZbzHZwaL4kZZvNEZ6bEj6fGmiAdCPjmNQHCf8v994PAgDf"
SECRET = "Bearer test-webhook-secret"
T0 = datetime(2026, 9, 24, 15, 0, tzinfo=timezone.utc)


def helius_buy(signature: str = SIGNATURE) -> list[dict[str, object]]:
    """Enhanced SWAP shape from the Helius transaction schema."""
    return [
        {
            "description": f"{WALLET} swapped 1.5 SOL for tokens on Jupiter",
            "type": "SWAP",
            "source": "JUPITER",
            "fee": 5000,
            "feePayer": WALLET,
            "signature": signature,
            "slot": 148277128,
            "timestamp": 1656442333,
            "nativeTransfers": [
                {
                    "fromUserAccount": WALLET,
                    "toUserAccount": "PoolAddress11111111111111111111111111111111",
                    "amount": 1_500_000_000,
                }
            ],
            "tokenTransfers": [
                {
                    "fromUserAccount": "PoolAddress11111111111111111111111111111111",
                    "toUserAccount": WALLET,
                    "fromTokenAccount": "PoolATA",
                    "toTokenAccount": "UserATA",
                    "tokenAmount": 225.5,
                    "mint": MINT,
                }
            ],
            "events": {
                "swap": {
                    "nativeInput": {"account": WALLET, "amount": "1500000000"},
                    "nativeOutput": None,
                    "tokenInputs": [],
                    "tokenOutputs": [
                        {
                            "userAccount": WALLET,
                            "tokenAccount": "UserATA",
                            "mint": MINT,
                            "rawTokenAmount": {"tokenAmount": "225500000", "decimals": 6},
                        }
                    ],
                }
            },
        }
    ]


def test_parse_buy_uses_native_input_lamports() -> None:
    swaps = parse_enhanced_payload(helius_buy())
    assert len(swaps) == 1
    swap = swaps[0]
    assert swap.action == "buy"
    assert swap.wallet_address == WALLET
    assert swap.mint == MINT
    assert swap.amount_sol == Decimal("1.5")
    assert swap.signature == SIGNATURE
    assert swap.block_time == datetime.fromtimestamp(1656442333, tz=timezone.utc)


def test_parse_sell_and_ignores_failed_or_non_swap() -> None:
    sell = {
        "type": "SWAP",
        "signature": "sell-sig",
        "timestamp": 1656442400,
        "events": {
            "swap": {
                "nativeOutput": {"account": WALLET, "amount": "2000000000"},
                "tokenInputs": [{"userAccount": WALLET, "mint": MINT}],
            }
        },
    }
    failed = {"type": "SWAP", "signature": "bad", "timestamp": 1656442400, "transactionError": {"error": "x"}}
    transfer = {
        "type": "TRANSFER",
        "signature": "xfer",
        "timestamp": 1656442400,
        "nativeTransfers": [{"fromUserAccount": WALLET, "toUserAccount": "other", "amount": 10}],
    }
    swaps = parse_enhanced_payload([sell, failed, transfer])
    assert [(swap.action, swap.amount_sol) for swap in swaps] == [("sell", Decimal("2"))]


def test_parse_rejects_a_non_object_payload() -> None:
    with pytest.raises(WebhookRejected) as caught:
        parse_enhanced_payload("nope")
    assert caught.value.status_code == 400


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


@pytest.fixture
async def watcher_stack(pipeline_infra: object):
    infra = pipeline_infra
    engine = create_async_engine(infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    redis = Redis.from_url(infra.redis_url, decode_responses=True)  # type: ignore[attr-defined]
    await truncate_pipeline(engine)
    await redis.flushdb()
    clock = Clock(T0)
    watcher = WalletWatcher(sessions, redis, SECRET, clock=clock)
    yield watcher, sessions, redis, clock
    await truncate_pipeline(engine)
    await redis.flushdb()
    await redis.aclose()
    await engine.dispose()


async def test_webhook_stores_buy_and_second_delivery_does_not_duplicate(watcher_stack: tuple) -> None:
    watcher, sessions, redis, clock = watcher_stack
    async with sessions() as session:
        async with session.begin():
            session.add(make_tracked_wallet(address=WALLET, win_rate=Decimal("1")))

    first = await watcher.handle(helius_buy(), SECRET)
    second = await watcher.handle(helius_buy(), SECRET)

    assert first.stored == 1
    assert first.risk_enqueued == 1
    assert first.bait_enqueued == 1
    assert second.stored == 0
    assert second.duplicated == 1
    async with sessions() as session:
        count = await session.scalar(select(func.count()).select_from(WalletTx))
        token = await session.scalar(select(Token).where(Token.mint == MINT))
    assert count == 1
    assert token is not None
    assert token.risk_score is None
    assert await redis.xlen(RISK_SCAN_QUEUE) == 2
    messages = await redis.xrange(RISK_SCAN_QUEUE)
    assert messages[0][1]["trace_id"]
    assert messages[1][1]["trace_id"]
    assert await redis.zcard(BAIT_CHECK_QUEUE) == 1
    members = await redis.zrange(BAIT_CHECK_QUEUE, 0, -1, withscores=True)
    assert members[0][1] == (clock.now + timedelta(minutes=BAIT_RECHECK_DELAY_MINUTES)).timestamp()


async def test_known_token_with_a_score_does_not_enqueue_another_scan(watcher_stack: tuple) -> None:
    watcher, sessions, redis, _clock = watcher_stack
    async with sessions() as session:
        async with session.begin():
            session.add(make_tracked_wallet(address=WALLET))
            session.add(make_token(mint=MINT, risk_score=Decimal("40"), updated_at=T0))

    result = await watcher.handle(helius_buy(), SECRET)
    assert result.stored == 1
    assert result.risk_enqueued == 0
    assert result.bait_enqueued == 1
    assert await redis.xlen(RISK_SCAN_QUEUE) == 0


async def test_untracked_wallet_is_skipped(watcher_stack: tuple) -> None:
    watcher, sessions, _redis, _clock = watcher_stack
    result = await watcher.handle(helius_buy(), SECRET)
    assert result.skipped == 1
    assert result.stored == 0
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(WalletTx)) == 0


async def test_route_rejects_a_bad_authorization_header() -> None:
    class Stub:
        async def handle(self, payload: object, authorization: str | None) -> None:
            raise WebhookRejected(401, "unauthorized")

    app.state.watcher = Stub()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/webhooks/helius",
            json=helius_buy(),
            headers={"Authorization": "Bearer wrong"},
        )
    assert response.status_code == 401
    assert response.json()["error"] == "unauthorized"


async def test_route_returns_handler_counts() -> None:
    class Stub:
        async def handle(self, payload: object, authorization: str | None):
            from app.workers.wallet_watcher import WatchResult

            assert authorization == SECRET
            assert isinstance(payload, list)
            return WatchResult(stored=1, duplicated=0, skipped=0, risk_enqueued=1, bait_enqueued=1)

    app.state.watcher = Stub()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/webhooks/helius",
            json=helius_buy(),
            headers={"Authorization": SECRET},
        )
    assert response.status_code == 200
    assert response.json()["stored"] == 1


