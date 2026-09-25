"""One PumpPortal websocket for the two free methods.

https://pumpportal.fun/data-api/real-time/ sends ``subscribeNewToken`` and
``subscribeMigration`` as two JSON messages on the same socket. A second
connection is banned for an hour, so this process keeps a single live socket
and ``connect()`` refuses to open another while that socket is still up.

``subscribeTokenTrade`` and ``subscribeAccountTrade`` are metered. This
adapter never sends them. Creation events do not include later trades; the
discovery worker reads holders from Helius instead.

Create fields were taken from a live frame: ``txType`` ``create``, ``mint``,
``traderPublicKey``, ``bondingCurveKey``, plus optional ``symbol``, ``name``,
``pool``, ``signature``, and ``vSolInBondingCurve``. A live ``subscribeMigration`` frame used ``txType``
``migrate``, ``mint``, ``signature``, and ``pool`` (``pump-amm``).
Subscription acknowledgements have ``message`` and no ``mint`` and are ignored.
"""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

import structlog

from app.config import DISCOVERY_DOWN_ALERT_SECONDS, Settings

logger = structlog.get_logger(__name__)

SUBSCRIBE_NEW_TOKEN = {"method": "subscribeNewToken"}
SUBSCRIBE_MIGRATION = {"method": "subscribeMigration"}
DISCOVERY_DOWN_MESSAGE = (
    "Discovery stream is down: PumpPortal has been disconnected for more than 5 minutes."
)

Sleep = Callable[[float], Awaitable[None]]
Clock = Callable[[], datetime]
Notifier = Callable[[str], Awaitable[None]]
_registry_lock = threading.Lock()


class PumpPortalAlreadyConnected(RuntimeError):
    """A second socket would risk an hour-long ban. The caller must not retry it."""


class PumpSocket(Protocol):
    async def send(self, text: str) -> None: ...

    async def recv(self) -> str: ...

    async def close(self) -> None: ...

    def closed(self) -> bool: ...


Opener = Callable[[str], Awaitable[PumpSocket]]


@dataclass(frozen=True)
class NewTokenEvent:
    mint: str
    creator_address: str
    bonding_curve_key: str
    signature: str | None
    name: str | None
    symbol: str | None
    pool: str | None
    # Virtual SOL reserve on the bonding curve at this create. Not AMM TVL.
    bonding_curve_liquidity_sol: Decimal | None = None


@dataclass(frozen=True)
class MigrationEvent:
    """A mint that left the bonding curve. Holders must be read again after this."""

    mint: str
    signature: str | None
    pool: str | None
    tx_type: str


def pumpportal_ws_url(settings: Settings) -> str:
    """Base socket URL. An API key is appended only when one is configured."""
    base = settings.pumpportal_ws_url
    key = settings.pumpportal_api_key.get_secret_value().strip()
    if not key:
        return base
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}api-key={key}"


def parse_new_token(payload: str | Mapping[str, Any]) -> NewTokenEvent | None:
    """Return a create event, or None for acks and any other frame."""
    data = _object(payload)
    if data is None or data.get("txType") != "create":
        return None
    mint = _text(data.get("mint"))
    creator = _text(data.get("traderPublicKey"))
    curve = _text(data.get("bondingCurveKey"))
    if mint is None or creator is None or curve is None:
        return None
    return NewTokenEvent(
        mint=mint,
        creator_address=creator,
        bonding_curve_key=curve,
        signature=_text(data.get("signature")),
        name=_text(data.get("name")),
        symbol=_text(data.get("symbol")),
        pool=_text(data.get("pool")),
        bonding_curve_liquidity_sol=_sol_amount(data.get("vSolInBondingCurve")),
    )


def parse_migration(payload: str | Mapping[str, Any]) -> MigrationEvent | None:
    """Return a migration event.

    ``txType`` ``migrate`` is the value from a live ``subscribeMigration``
    frame. Acks and creates return None.
    """
    data = _object(payload)
    if data is None:
        return None
    tx_type = _text(data.get("txType"))
    if tx_type not in _MIGRATION_TX_TYPES:
        return None
    mint = _text(data.get("mint"))
    if mint is None:
        return None
    return MigrationEvent(
        mint=mint,
        signature=_text(data.get("signature")),
        pool=_text(data.get("pool")),
        tx_type=tx_type,
    )


# Observed on a live subscribeMigration frame. Do not add values that were not seen.
_MIGRATION_TX_TYPES: frozenset[str] = frozenset({"migrate"})


def _object(payload: str | Mapping[str, Any]) -> dict[str, Any] | None:
    if isinstance(payload, str):
        try:
            data: Any = json.loads(payload)
        except json.JSONDecodeError:
            return None
    else:
        data = payload
    if isinstance(data, dict):
        return data
    return None


def _sol_amount(value: Any) -> Decimal | None:
    """Parse ``vSolInBondingCurve``. Missing or non-numeric stays None."""
    if isinstance(value, bool) or value is None:
        return None
    if not isinstance(value, (int, float, str, Decimal)):
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not amount.is_finite() or amount < 0:
        return None
    return amount


def _text(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _is_ack(raw: str) -> bool:
    data = _object(raw)
    return data is not None and "message" in data and "mint" not in data


def _clip(raw: str) -> str:
    if len(raw) <= 2000:
        return raw
    return raw[:2000]


class _WebsocketsTransport:
    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._closed = False

    async def send(self, text: str) -> None:
        await self._connection.send(text)

    async def recv(self) -> str:
        message: Any = await self._connection.recv()
        if isinstance(message, bytes):
            return message.decode()
        if isinstance(message, str):
            return message
        raise ConnectionError("unexpected websocket frame")

    async def close(self) -> None:
        self._closed = True
        await self._connection.close()

    def closed(self) -> bool:
        return self._closed


async def open_pumpportal_socket(url: str) -> PumpSocket:
    import websockets

    connection: Any = await websockets.connect(
        url,
        open_timeout=20,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
        max_size=1_048_576,
    )
    return _WebsocketsTransport(connection)


class PumpPortalClient:
    """Process-wide single socket with reconnect backoff.

    ``connect()`` raises ``PumpPortalAlreadyConnected`` when this client, or
    any other, already has a socket open or a connect in flight. Reconnect
    closes the dead socket first, then connects again.
    """

    _holder: PumpPortalClient | None = None

    def __init__(
        self,
        url: str,
        *,
        opener: Opener | None = None,
        clock: Clock | None = None,
        sleep: Sleep | None = None,
        notifier: Notifier | None = None,
        down_after_seconds: float = DISCOVERY_DOWN_ALERT_SECONDS,
        initial_backoff_seconds: float = 1,
        backoff_cap_seconds: float = 60,
    ) -> None:
        self._url = url
        self._opener = opener or open_pumpportal_socket
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleep = sleep or asyncio.sleep
        self._notifier = notifier
        self._down_after = down_after_seconds
        self._initial_backoff = initial_backoff_seconds
        self._backoff_cap = backoff_cap_seconds
        self._socket: PumpSocket | None = None
        self._opening = False
        self._down_since: datetime | None = None
        self._alerted = False

    @classmethod
    def reset_for_tests(cls) -> None:
        with _registry_lock:
            cls._holder = None

    def is_connected(self) -> bool:
        socket = self._socket
        return socket is not None and not socket.closed()

    async def connect(self) -> None:
        self._reserve()
        socket: PumpSocket | None = None
        try:
            socket = await self._opener(self._url)
            await socket.send(json.dumps(SUBSCRIBE_NEW_TOKEN))
            await socket.send(json.dumps(SUBSCRIBE_MIGRATION))
        except Exception:
            if socket is not None:
                await _close_quietly(socket)
            self._release()
            raise
        self._socket = socket
        self._opening = False
        logger.info("pumpportal_subscribed")

    async def close(self) -> None:
        await self._drop_socket()

    async def iter_events(
        self, stop: asyncio.Event
    ) -> AsyncIterator[NewTokenEvent | MigrationEvent]:
        """Yield create and migration events until ``stop`` is set."""
        backoff = self._initial_backoff
        try:
            while not stop.is_set():
                try:
                    await self.connect()
                    backoff = self._initial_backoff
                    while not stop.is_set():
                        raw = await self._recv_or_stop(stop)
                        if raw is None:
                            return
                        self._mark_recovered()
                        created = parse_new_token(raw)
                        if created is not None:
                            yield created
                            continue
                        migrated = parse_migration(raw)
                        if migrated is not None:
                            yield migrated
                            continue
                        if not _is_ack(raw):
                            logger.info("pumpportal_unparsed", payload=_clip(raw))
                except PumpPortalAlreadyConnected:
                    raise
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "pumpportal_disconnected",
                        error_type=type(exc).__name__,
                    )
                    await self._drop_socket()
                    await self._note_outage()
                    if await self._wait_backoff(stop, backoff):
                        return
                    backoff = min(backoff * 2, self._backoff_cap)
        finally:
            await self._drop_socket()

    def _reserve(self) -> None:
        with _registry_lock:
            holder = PumpPortalClient._holder
            if holder is not None and holder is not self and _busy(holder):
                raise PumpPortalAlreadyConnected(
                    "a PumpPortal socket is already open in this process"
                )
            if self.is_connected() or self._opening:
                raise PumpPortalAlreadyConnected(
                    "connect() called while the previous socket is still open"
                )
            PumpPortalClient._holder = self
            self._opening = True

    def _release(self) -> None:
        self._opening = False
        self._socket = None
        with _registry_lock:
            if PumpPortalClient._holder is self:
                PumpPortalClient._holder = None

    async def _drop_socket(self) -> None:
        socket = self._socket
        self._release()
        if socket is not None:
            await _close_quietly(socket)

    async def _recv_or_stop(self, stop: asyncio.Event) -> str | None:
        socket = self._socket
        if socket is None:
            raise ConnectionError("socket is closed")
        recv_task = asyncio.create_task(socket.recv())
        stop_task = asyncio.create_task(stop.wait())
        done, pending = await asyncio.wait(
            {recv_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if recv_task not in done:
            return None
        return recv_task.result()

    def _mark_recovered(self) -> None:
        self._down_since = None
        self._alerted = False

    async def _note_outage(self) -> None:
        now = self._clock()
        if self._down_since is None:
            self._down_since = now
        elapsed = (now - self._down_since).total_seconds()
        if elapsed < self._down_after or self._alerted:
            return
        self._alerted = True
        logger.critical("pumpportal_down", down_seconds=elapsed)
        if self._notifier is None:
            return
        try:
            await self._notifier(DISCOVERY_DOWN_MESSAGE)
        except Exception as exc:
            logger.critical(
                "pumpportal_down_alert_failed",
                error_type=type(exc).__name__,
            )

    async def _wait_backoff(self, stop: asyncio.Event, delay: float) -> bool:
        if stop.is_set():
            return True
        await self._sleep(delay)
        return stop.is_set()


def _busy(holder: PumpPortalClient) -> bool:
    return holder.is_connected() or holder._opening


async def _close_quietly(socket: PumpSocket) -> None:
    try:
        await socket.close()
    except Exception as exc:
        logger.warning("pumpportal_close_failed", error_type=type(exc).__name__)
