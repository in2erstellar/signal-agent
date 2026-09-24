"""Public REST API. Postgres and Redis are real; Helius is not called."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://signalforge:signalforge@localhost:5432/signalforge",
)
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("ENVIRONMENT", "dev")
os.environ.setdefault("LOG_LEVEL", "INFO")

import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import get_settings
from app.db.models import Token
from app.main import app
from tests.factories import (
    make_bait_flag,
    make_funder_cluster,
    make_signal,
    make_token,
    make_tracked_wallet,
    make_user,
    make_wallet_tx,
)
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

API_KEY = "test-api-key"
WALLET = "8cRrU1NzNpjL3k2BwjW3VixAcX6VFc29KHr4KZg8cs2Y"
MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
T0 = datetime(2026, 9, 24, 18, 0, tzinfo=timezone.utc)


@pytest.fixture
async def api(pipeline_infra: object, monkeypatch: pytest.MonkeyPatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "public_api_key", SecretStr(API_KEY))
    monkeypatch.setattr(settings, "api_rate_limit_per_minute", 60)
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    redis = Redis.from_url(pipeline_infra.redis_url, decode_responses=True)  # type: ignore[attr-defined]
    await truncate_pipeline(engine)
    await redis.flushdb()
    async with app.router.lifespan_context(app):
        original_engine = app.state.engine
        original_redis = app.state.redis
        app.state.engine = engine
        app.state.session_factory = sessions
        app.state.redis = redis
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            client.headers["X-API-Key"] = API_KEY
            try:
                yield client, sessions
            finally:
                app.state.engine = original_engine
                app.state.redis = original_redis
                app.state.session_factory = async_sessionmaker(original_engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    await redis.flushdb()
    await redis.aclose()
    await engine.dispose()


def test_openapi_and_docs_render() -> None:
    with TestClient(app) as client:
        docs = client.get("/docs")
        spec = client.get("/openapi.json")
    assert docs.status_code == 200
    assert "swagger" in docs.text.lower()
    assert spec.status_code == 200
    body = spec.json()
    paths = body["paths"]
    assert "/watchlist/wallet" in paths
    assert "/watchlist/wallet/{address}" in paths
    assert "/signals" in paths
    assert "/signals/{signal_id}" in paths
    assert "/token/{mint}/risk" in paths
    assert "/users/{telegram_id}/settings" in paths
    schemas = body["components"]["schemas"]
    for name in (
        "WalletOut",
        "WalletList",
        "WalletCreate",
        "SignalOut",
        "SignalList",
        "SignalDetail",
        "TokenRiskResponse",
        "RiskReport",
        "UserSettingsOut",
        "SettingsPatch",
        "ErrorBody",
    ):
        assert name in schemas
    assert "APIKeyHeader" in body["components"]["securitySchemes"]


async def test_missing_api_key_is_401_on_every_route(api: tuple) -> None:
    _client, _sessions = api
    bare = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    cases = [
        ("get", "/watchlist/wallet"),
        ("post", "/watchlist/wallet"),
        ("delete", f"/watchlist/wallet/{WALLET}"),
        ("get", "/signals"),
        ("get", "/signals/00000000-0000-0000-0000-000000000001"),
        ("get", f"/token/{MINT}/risk"),
        ("get", "/users/1/settings"),
        ("patch", "/users/1/settings"),
    ]
    async with bare:
        for method, path in cases:
            response = await bare.request(method, path)
            assert response.status_code == 401, path
            assert response.json()["detail"] == "missing or invalid API key"


async def test_watchlist_add_list_and_reject_duplicate(api: tuple) -> None:
    client, _sessions = api
    created = await client.post("/watchlist/wallet", json={"address": WALLET, "label": "desk"})
    assert created.status_code == 201
    body = created.json()
    assert body["address"] == WALLET
    assert body["label"] == "desk"
    assert body["is_flagged_baiter"] is False

    listed = await client.get("/watchlist/wallet")
    assert listed.status_code == 200
    page = listed.json()
    assert page["total"] == 1
    assert page["limit"] == 50
    assert page["offset"] == 0
    assert page["items"][0]["address"] == WALLET

    duplicate = await client.post("/watchlist/wallet", json={"address": WALLET})
    assert duplicate.status_code == 409


async def test_watchlist_empty_and_invalid_address(api: tuple) -> None:
    client, _sessions = api
    empty = await client.get("/watchlist/wallet")
    assert empty.status_code == 200
    assert empty.json()["items"] == []
    assert empty.json()["total"] == 0

    invalid = await client.post("/watchlist/wallet", json={"address": "not-valid"})
    assert invalid.status_code == 422

    missing = await client.delete(f"/watchlist/wallet/{WALLET}")
    assert missing.status_code == 404


async def test_delete_wallet_and_refuse_when_it_has_transactions(api: tuple) -> None:
    client, sessions = api
    await client.post("/watchlist/wallet", json={"address": WALLET, "label": "desk"})
    deleted = await client.delete(f"/watchlist/wallet/{WALLET}")
    assert deleted.status_code == 200
    assert deleted.json()["address"] == WALLET
    gone = await client.get("/watchlist/wallet")
    assert gone.json()["total"] == 0

    async with sessions() as session:
        async with session.begin():
            wallet = make_tracked_wallet(address=WALLET)
            token = make_token(mint=MINT)
            session.add_all([wallet, token])
            await session.flush()
            session.add(make_wallet_tx(wallet_id=wallet.id, token_id=token.id))
    blocked = await client.delete(f"/watchlist/wallet/{WALLET}")
    assert blocked.status_code == 409


async def test_signal_feed_filters_sorts_and_pages(api: tuple) -> None:
    client, sessions = api
    async with sessions() as session:
        async with session.begin():
            older = make_token(mint=MINT, symbol="BONK", risk_score=Decimal("85"))
            newer = make_token(symbol="LOW", risk_score=Decimal("10"))
            session.add_all([older, newer])
            await session.flush()
            session.add_all(
                [
                    make_signal(
                        token_id=older.id,
                        composite_score=Decimal("72.5"),
                        risk_score=Decimal("85"),
                        triggered_at=T0,
                    ),
                    make_signal(
                        token_id=newer.id,
                        composite_score=Decimal("40"),
                        risk_score=Decimal("10"),
                        triggered_at=T0 + timedelta(minutes=5),
                    ),
                ]
            )
    feed = await client.get("/signals", params={"min_score": "50"})
    assert feed.status_code == 200
    page = feed.json()
    assert page["total"] == 1
    assert page["items"][0]["mint"] == MINT
    assert page["items"][0]["composite_score"] == "72.5"

    ordered = await client.get("/signals")
    stamps = [item["triggered_at"] for item in ordered.json()["items"]]
    assert stamps == sorted(stamps, reverse=True)

    past = await client.get("/signals", params={"offset": 50})
    assert past.status_code == 200
    assert past.json()["items"] == []
    assert past.json()["total"] == 2

    invalid = await client.get("/signals", params={"limit": 201})
    assert invalid.status_code == 422


async def test_signal_detail_includes_flags_and_unknown_id_is_404(api: tuple) -> None:
    client, sessions = api
    async with sessions() as session:
        async with session.begin():
            token = make_token(
                mint=MINT,
                symbol="BONK",
                risk_score=Decimal("85"),
                risk_flags=["check_funder_cluster_exposure"],
            )
            wallet = make_tracked_wallet(address=WALLET)
            session.add_all([token, wallet])
            await session.flush()
            signal = make_signal(
                token_id=token.id,
                composite_score=Decimal("72.5"),
                risk_score=Decimal("85"),
                triggered_at=T0,
            )
            session.add(signal)
            session.add(make_bait_flag(wallet_id=wallet.id, token_id=token.id))
            await session.flush()
            signal_id = signal.id
    detail = await client.get(f"/signals/{signal_id}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["risk_flags"] == ["check_funder_cluster_exposure"]
    assert body["bait_flags"][0]["wallet_address"] == WALLET
    assert body["bait_flags"][0]["flag_type"] == "funder_cluster_member"
    missing = await client.get("/signals/00000000-0000-0000-0000-000000000001")
    assert missing.status_code == 404
    assert (await client.get("/signals/not-a-uuid")).status_code == 422


async def test_token_risk_returns_stored_report_and_cluster(api: tuple) -> None:
    client, sessions = api
    async with sessions() as session:
        async with session.begin():
            token = make_token(
                mint=MINT,
                symbol="BONK",
                risk_score=Decimal("85"),
                risk_flags=["check_funder_cluster_exposure", "check_lp_lock:unavailable"],
                updated_at=T0,
            )
            session.add(token)
            await session.flush()
            session.add(
                make_funder_cluster(
                    token_id=token.id,
                    root_funder="RootFunder11111111111111111111111111111111",
                    member_wallets=[WALLET],
                    cluster_size=6,
                    confidence=Decimal("0.6"),
                )
            )
    response = await client.get(f"/token/{MINT}/risk")
    assert response.status_code == 200
    body = response.json()
    assert body["report"]["mint"] == MINT
    assert body["report"]["risk_score"] == 85
    by_name = {check["name"]: check for check in body["report"]["checks"]}
    assert by_name["check_funder_cluster_exposure"]["passed"] is False
    assert by_name["check_lp_lock"]["passed"] is None
    assert by_name["check_mint_authority"]["passed"] is True
    assert body["funder_clusters"][0]["cluster_size"] == 6
    assert body["funder_clusters"][0]["member_wallets"] == [WALLET]
    assert "not a claim that the token is safe" in body["disclaimer"]

    unknown = await client.get("/token/So11111111111111111111111111111111111111112/risk")
    assert unknown.status_code == 404

    async with sessions() as session:
        async with session.begin():
            session.add(Token(mint="9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin", first_seen_at=T0))
    unscanned = await client.get("/token/9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin/risk")
    assert unscanned.status_code == 404


async def test_user_settings_round_trip_and_missing_user(api: tuple) -> None:
    client, sessions = api
    async with sessions() as session:
        async with session.begin():
            session.add(make_user(telegram_id=4242, settings={"min_score": "50", "max_risk": "100"}))
    current = await client.get("/users/4242/settings")
    assert current.status_code == 200
    assert current.json()["min_score"] == "50"
    assert current.json()["active"] is True

    patched = await client.patch("/users/4242/settings", json={"min_score": "70", "max_risk": "40"})
    assert patched.status_code == 200
    assert patched.json()["min_score"] == "70"
    assert patched.json()["max_risk"] == "40"

    invalid = await client.patch("/users/4242/settings", json={"min_score": 150})
    assert invalid.status_code == 422
    missing = await client.get("/users/999/settings")
    assert missing.status_code == 404


async def test_rate_limit_returns_429_after_the_budget(api: tuple, monkeypatch: pytest.MonkeyPatch) -> None:
    client, _sessions = api
    monkeypatch.setattr(get_settings(), "api_rate_limit_per_minute", 2)
    first = await client.get("/watchlist/wallet")
    second = await client.get("/watchlist/wallet")
    third = await client.get("/watchlist/wallet")
    assert first.status_code == 200
    assert second.status_code == 200
    assert third.status_code == 429
    assert third.headers["retry-after"]
    assert third.json()["detail"] == "rate limit exceeded"


async def test_wrong_api_key_is_401(api: tuple) -> None:
    client, _sessions = api
    response = await client.get("/watchlist/wallet", headers={"X-API-Key": "nope"})
    assert response.status_code == 401
