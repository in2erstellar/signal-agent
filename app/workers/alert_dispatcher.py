"""Send unnotified signals to subscribers whose settings accept them.

A signal is marked notified only after every matching subscriber has a
delivery row or has blocked the bot. A transport error leaves the signal
unnotified so the next cycle retries the subscribers who were not stored.
The unique key on (user_id, signal_id) makes that retry insert nothing
for people who already received it.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol
from uuid import UUID

import structlog
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram.error import Forbidden, TelegramError

from app.db.models import AlertDelivery, BaitFlag, Signal, Token, User
from app.observability import bind_trace, record_alert_sent
from app.services.subscriber_settings import accepts_alert, parse_settings, with_updates
from app.templates import alert_text

logger = structlog.get_logger(__name__)

Clock = Callable[[], datetime]


class AlertSender(Protocol):
    async def send(self, chat_id: int | str, text: str) -> None: ...


@dataclass(frozen=True)
class _Recipient:
    user_id: UUID
    telegram_id: int


@dataclass(frozen=True)
class _Snapshot:
    signal_id: UUID
    symbol: str | None
    mint: str
    composite_score: Decimal
    risk_score: Decimal | None
    risk_flags: list[str]
    bait_warning: bool
    trace_id: str | None
    recipients: list[_Recipient]


@dataclass(frozen=True)
class _Outcome:
    sent: int
    failed: int


class AlertDispatcher:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        sender: AlertSender,
        *,
        clock: Clock | None = None,
        stop: object | None = None,
        owners: frozenset[int] | None = None,
    ) -> None:
        self._sessions = sessions
        self._sender = sender
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._stop = stop
        self._owners = owners

    async def run_once(self) -> tuple[int, int, int]:
        if self._owners is not None and not self._owners:
            logger.info("alert_dispatch_skipped", reason="no_owners")
            return 0, 0, 0
        processed = 0
        failed = 0
        sent = 0
        for signal_id in await self._pending_ids():
            if _stopped(self._stop):
                break
            try:
                outcome = await self._dispatch(signal_id)
            except Exception:
                logger.exception("alert_dispatch_failed", signal_id=str(signal_id))
                failed += 1
                continue
            processed += 1
            sent += outcome.sent
            failed += outcome.failed
        logger.info(
            "alert_dispatch_cycle",
            processed=processed,
            failed=failed,
            sent=sent,
        )
        return processed, failed, sent

    async def _pending_ids(self) -> list[UUID]:
        async with self._sessions() as session:
            rows = await session.scalars(
                select(Signal.id)
                .where(Signal.notified.is_(False))
                .order_by(Signal.triggered_at, Signal.id)
            )
            return list(rows)

    async def _dispatch(self, signal_id: UUID) -> _Outcome:
        snapshot = await self._load(signal_id)
        if snapshot is None:
            return _Outcome(0, 0)
        sent = 0
        failed = 0
        delivered: list[UUID] = []
        deactivated: list[UUID] = []
        text = alert_text(
            symbol=snapshot.symbol,
            mint=snapshot.mint,
            composite_score=snapshot.composite_score,
            risk_score=snapshot.risk_score,
            risk_flags=snapshot.risk_flags,
            bait_warning=snapshot.bait_warning,
        )
        with bind_trace(snapshot.trace_id):
            for recipient in snapshot.recipients:
                try:
                    await self._sender.send(recipient.telegram_id, text)
                except Forbidden:
                    deactivated.append(recipient.user_id)
                    logger.info(
                        "alert_user_deactivated",
                        telegram_id=recipient.telegram_id,
                        signal_id=str(signal_id),
                    )
                    continue
                except TelegramError as exc:
                    failed += 1
                    logger.warning(
                        "alert_send_failed",
                        telegram_id=recipient.telegram_id,
                        error=type(exc).__name__,
                        signal_id=str(signal_id),
                    )
                    continue
                delivered.append(recipient.user_id)
                sent += 1
                record_alert_sent()
                logger.info(
                    "alert_sent",
                    telegram_id=recipient.telegram_id,
                    signal_id=str(signal_id),
                )
        await self._persist(
            signal_id,
            delivered,
            deactivated,
            mark_notified=failed == 0,
        )
        return _Outcome(sent, failed)

    async def _load(self, signal_id: UUID) -> _Snapshot | None:
        async with self._sessions() as session:
            signal = await session.get(Signal, signal_id)
            if signal is None or signal.notified:
                return None
            token = await session.get(Token, signal.token_id)
            if token is None:
                return None
            bait_count = await session.scalar(
                select(func.count())
                .select_from(BaitFlag)
                .where(BaitFlag.token_id == token.id)
            )
            delivered = set(
                await session.scalars(
                    select(AlertDelivery.user_id).where(AlertDelivery.signal_id == signal.id)
                )
            )
            users = list(await session.scalars(select(User)))
            recipients: list[_Recipient] = []
            for user in users:
                if user.id in delivered:
                    continue
                prefs = parse_settings(user.settings)
                if self._owners is not None and user.telegram_id not in self._owners:
                    continue
                if not accepts_alert(
                    prefs,
                    composite_score=signal.composite_score,
                    risk_score=signal.risk_score,
                ):
                    continue
                recipients.append(_Recipient(user.id, user.telegram_id))
            flags = token.risk_flags if isinstance(token.risk_flags, list) else []
            return _Snapshot(
                signal_id=signal.id,
                symbol=token.symbol,
                mint=token.mint,
                composite_score=signal.composite_score,
                risk_score=signal.risk_score,
                risk_flags=[str(flag) for flag in flags],
                bait_warning=bool(bait_count),
                trace_id=signal.trace_id,
                recipients=recipients,
            )

    async def _persist(
        self,
        signal_id: UUID,
        delivered: list[UUID],
        deactivated: list[UUID],
        *,
        mark_notified: bool,
    ) -> None:
        now = self._clock()
        async with self._sessions() as session:
            async with session.begin():
                for user_id in delivered:
                    await session.execute(
                        pg_insert(AlertDelivery)
                        .values(user_id=user_id, signal_id=signal_id, sent_at=now)
                        .on_conflict_do_nothing(
                            constraint="uq_alert_deliveries_user_id_signal_id"
                        )
                    )
                if deactivated:
                    rows = list(
                        await session.scalars(select(User).where(User.id.in_(deactivated)))
                    )
                    for row in rows:
                        row.settings = with_updates(row.settings, active=False)
                if mark_notified:
                    await session.execute(
                        update(Signal).where(Signal.id == signal_id).values(notified=True)
                    )


def _stopped(stop: object | None) -> bool:
    if stop is None:
        return False
    is_set = getattr(stop, "is_set", None)
    return bool(is_set()) if callable(is_set) else False
