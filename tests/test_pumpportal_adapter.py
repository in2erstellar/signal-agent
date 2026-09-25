"""PumpPortal socket guard and reconnect. The socket is a fake, not the live host."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from structlog.testing import capture_logs

from app.adapters.pumpportal import (
    DISCOVERY_DOWN_MESSAGE,
    SUBSCRIBE_MIGRATION,
    SUBSCRIBE_NEW_TOKEN,
    MigrationEvent,
    NewTokenEvent,
    PumpPortalAlreadyConnected,
    PumpPortalClient,
    parse_migration,
    parse_new_token,
    pumpportal_ws_url,
)
from app.config import Settings

# Captured by scripts/inspect_pumpportal_schema.py from subscribeNewToken.
LIVE_CREATE = (
    '{"signature":"2BmWbjYdZFTbhWaENUwtZzcEFmosuexrceJ3F5xzTceqyWR3puqgVVZoZng2mmdJ21ZcNxwbCnj8xanqLw2inZrq",'
    '"mint":"CMdp4kwo65eeNV9RP4NWGf7ZRXeZnqeszVfJQV2Tpump",'
    '"traderPublicKey":"7mHmNh6BFNWpFp5qh2VXRNzsK63zZ5ZQGQY77khANBQW",'
    '"txType":"create",'
    '"initialBuy":3485709.524202,'
    '"solAmount":0.097774557,'
    '"bondingCurveKey":"yDh3gJdZbFpmsLDEswWKr8BiNHZpdedZP5oeaN67hw8",'
    '"vTokensInBondingCurve":1069514290.475798,'
    '"vSolInBondingCurve":30.097774556784593,'
    '"marketCapSol":28.14153567166915,'
    '"name":"The Amazon Monkey",'
    '"symbol":"ELIZABETH",'
    '"uri":"https://ipfs.io/ipfs/QmWhabDrLmJ695bntu18rcpEcMwXU5SPYAhJ7eRJErxuKN",'
    '"is_mayhem_mode":true,'
    '"pool":"pump"}'
)
LIVE_ACK = '{"message":"Successfully subscribed to token creation events."}'
# Captured by scripts/inspect_migration_schema.py from subscribeMigration.
LIVE_MIGRATION = (
    '{"signature":"35fwAoDRzr7BBALA4AXYgEMixHdugY1R82GNvjiXgtpvmgBBQNhgpCdDeigo4NZWMgRgy3VrH87ZFB2kjiYQ3dGq",'
    '"mint":"HXWP9B1a7xRpCSrdU53GoGgvTftdHFxYtWvpP6gbpump",'
    '"txType":"migrate",'
    '"pool":"pump-amm"}'
)
LIVE_MIGRATION_ACK = '{"message":"Subscribed to \'migration\' events."}'


class ScriptedSocket:
    def __init__(self, frames: list[str | Exception]) -> None:
        self._frames = list(frames)
        self.sent: list[str] = []
        self._closed = False

    async def send(self, text: str) -> None:
        self.sent.append(text)

    async def recv(self) -> str:
        if not self._frames:
            raise ConnectionError("eof")
        frame = self._frames.pop(0)
        if isinstance(frame, Exception):
            raise frame
        return frame

    async def close(self) -> None:
        self._closed = True

    def closed(self) -> bool:
        return self._closed


@pytest.fixture(autouse=True)
def _reset_pumpportal_slot() -> object:
    PumpPortalClient.reset_for_tests()
    yield
    PumpPortalClient.reset_for_tests()


def test_parse_live_create_and_ignore_ack() -> None:
    event = parse_new_token(LIVE_CREATE)
    assert event == NewTokenEvent(
        mint="CMdp4kwo65eeNV9RP4NWGf7ZRXeZnqeszVfJQV2Tpump",
        creator_address="7mHmNh6BFNWpFp5qh2VXRNzsK63zZ5ZQGQY77khANBQW",
        bonding_curve_key="yDh3gJdZbFpmsLDEswWKr8BiNHZpdedZP5oeaN67hw8",
        signature="2BmWbjYdZFTbhWaENUwtZzcEFmosuexrceJ3F5xzTceqyWR3puqgVVZoZng2mmdJ21ZcNxwbCnj8xanqLw2inZrq",
        name="The Amazon Monkey",
        symbol="ELIZABETH",
        pool="pump",
        bonding_curve_liquidity_sol=Decimal("30.097774556784593"),
    )
    assert parse_new_token(LIVE_ACK) is None
    assert parse_new_token('{"txType":"buy","mint":"mint"}') is None
    assert parse_new_token("{not-json") is None
    assert parse_new_token('{"txType":"create","mint":"mint","traderPublicKey":"creator"}') is None
    assert parse_new_token(LIVE_MIGRATION) is None


def test_parse_live_migration_and_ignore_its_ack() -> None:
    event = parse_migration(LIVE_MIGRATION)
    assert event == MigrationEvent(
        mint="HXWP9B1a7xRpCSrdU53GoGgvTftdHFxYtWvpP6gbpump",
        signature="35fwAoDRzr7BBALA4AXYgEMixHdugY1R82GNvjiXgtpvmgBBQNhgpCdDeigo4NZWMgRgy3VrH87ZFB2kjiYQ3dGq",
        pool="pump-amm",
        tx_type="migrate",
    )
    assert parse_migration(LIVE_MIGRATION_ACK) is None
    assert parse_migration(LIVE_CREATE) is None
    assert parse_migration('{"txType":"migrate"}') is None


def test_url_appends_a_key_only_when_one_is_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://signalforge:signalforge@db:5432/signalforge",
    )
    monkeypatch.setenv("REDIS_URL", "redis://cache:6379/0")
    monkeypatch.delenv("PUMPPORTAL_API_KEY", raising=False)
    plain = Settings(_env_file=None)
    assert pumpportal_ws_url(plain) == "wss://pumpportal.fun/api/data"

    monkeypatch.setenv("PUMPPORTAL_API_KEY", "test-key")
    with_key = Settings(_env_file=None)
    assert pumpportal_ws_url(with_key) == "wss://pumpportal.fun/api/data?api-key=test-key"


async def test_second_connect_is_refused_while_the_socket_is_open() -> None:
    opens = 0

    async def opener(_url: str) -> ScriptedSocket:
        nonlocal opens
        opens += 1
        return ScriptedSocket([])

    client = PumpPortalClient("wss://pumpportal.fun/api/data", opener=opener)
    await client.connect()
    with pytest.raises(PumpPortalAlreadyConnected):
        await client.connect()
    other = PumpPortalClient("wss://pumpportal.fun/api/data", opener=opener)
    with pytest.raises(PumpPortalAlreadyConnected):
        await other.connect()
    assert opens == 1
    sent = [json.loads(frame) for frame in client._socket.sent]  # type: ignore[union-attr]
    assert sent == [SUBSCRIBE_NEW_TOKEN, SUBSCRIBE_MIGRATION]
    assert "subscribeTokenTrade" not in json.dumps(sent)
    assert "subscribeAccountTrade" not in json.dumps(sent)

    await client.close()
    await other.connect()
    assert opens == 2
    await other.close()


async def test_reconnect_uses_backoff_then_yields_the_next_create() -> None:
    sockets = [
        ScriptedSocket([ConnectionError("drop")]),
        ScriptedSocket([LIVE_ACK, LIVE_CREATE]),
    ]
    sleeps: list[float] = []

    async def opener(_url: str) -> ScriptedSocket:
        return sockets.pop(0)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    client = PumpPortalClient(
        "wss://pumpportal.fun/api/data",
        opener=opener,
        sleep=sleep,
    )
    stop = asyncio.Event()
    found: list[NewTokenEvent] = []
    async for event in client.iter_events(stop):
        found.append(event)
        stop.set()

    assert sleeps == [1]
    assert [event.mint for event in found] == ["CMdp4kwo65eeNV9RP4NWGf7ZRXeZnqeszVfJQV2Tpump"]


async def test_reconnect_subscribes_to_creates_and_migrations_on_one_socket() -> None:
    first = ScriptedSocket([ConnectionError("drop")])
    second = ScriptedSocket([LIVE_CREATE])
    pending = [first, second]

    async def opener(_url: str) -> ScriptedSocket:
        return pending.pop(0)

    async def sleep(_delay: float) -> None:
        return None

    client = PumpPortalClient(
        "wss://pumpportal.fun/api/data",
        opener=opener,
        sleep=sleep,
    )
    stop = asyncio.Event()
    async for _event in client.iter_events(stop):
        stop.set()

    for socket in (first, second):
        assert [json.loads(frame) for frame in socket.sent] == [
            SUBSCRIBE_NEW_TOKEN,
            SUBSCRIBE_MIGRATION,
        ]
        assert "subscribeTokenTrade" not in json.dumps(socket.sent)


async def test_outage_longer_than_five_minutes_logs_critical_and_alerts_once() -> None:
    clock_now = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)
    sleeps: list[float] = []
    alerts: list[str] = []

    def clock() -> datetime:
        return clock_now

    async def opener(_url: str) -> ScriptedSocket:
        raise ConnectionError("refused")

    async def sleep(delay: float) -> None:
        nonlocal clock_now
        sleeps.append(delay)
        clock_now += timedelta(seconds=delay)
        if len(sleeps) > 20:
            stop.set()

    stop = asyncio.Event()

    async def notify(text: str) -> None:
        alerts.append(text)
        stop.set()

    client = PumpPortalClient(
        "wss://pumpportal.fun/api/data",
        opener=opener,
        clock=clock,
        sleep=sleep,
        notifier=notify,
        down_after_seconds=300,
    )
    with capture_logs() as logs:
        async for _event in client.iter_events(stop):
            pass

    assert sleeps == [1, 2, 4, 8, 16, 32, 60, 60, 60, 60]
    assert sum(sleeps) >= 300
    assert alerts == [DISCOVERY_DOWN_MESSAGE]
    critical = [entry for entry in logs if entry["event"] == "pumpportal_down"]
    assert len(critical) == 1
    assert critical[0]["log_level"] == "critical"
