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
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

import structlog
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram.error import Forbidden, TelegramError

from app.config import AI_REVIEW_ALERT_WAIT_SECONDS, UNLOCK_ALERT_LEAD_HOURS
from app.db.models import AlertDelivery, BaitFlag, LpUnlock, Signal, Token, User
from app.observability import bind_trace, record_alert_sent
from app.services.creator_cluster import share_above_limit
from app.services.lp_unlock import unlock_alert_due
from app.services.risk_scanner import unavailable_fields
from app.services.subscriber_settings import accepts_alert, parse_settings, with_updates
from app.services.token_store import AI_REVIEW_COMPLETED, AI_REVIEW_PENDING
from app.templates import alert_text, data_incomplete_text, unlock_alert_text

logger = structlog.get_logger(__name__)

Clock = Callable[[], datetime]


class AlertSender(Protocol):
    async def send(self, chat_id: int | str, text: str) -> None: ...


@dataclass(frozen=True)
class _Recipient:
    user_id: UUID
    telegram_id: int
    max_dev_cluster_share: Decimal


@dataclass(frozen=True)
class _Snapshot:
    signal_id: UUID
    symbol: str | None
    mint: str
    composite_score: Decimal
    risk_score: Decimal | None
    risk_flags: list[str]
    bait_warning: bool
    ai_review: str | None
    defer: bool
    trace_id: str | None
    recipients: list[_Recipient]
    context: dict[str, Any] | None


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
    ) -> None:
        self._sessions = sessions
        self._sender = sender
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._stop = stop

    async def run_once(self) -> tuple[int, int, int]:
        await self._send_unlock_alerts()
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

    async def _send_unlock_alerts(self) -> None:
        now = self._clock()
        lead = timedelta(hours=UNLOCK_ALERT_LEAD_HOURS)
        async with self._sessions() as session:
            rows = list(await session.scalars(select(LpUnlock)))
        due = [
            row
            for row in rows
            if unlock_alert_due(
                status=row.status,
                unlock_at=row.unlock_at,
                alerted_at=row.alerted_at,
                now=now,
                lead=lead,
            )
        ]
        for row in due:
            async with self._sessions() as session:
                token = await session.get(Token, row.token_id)
                signal = await session.scalar(select(Signal).where(Signal.token_id == row.token_id))
                if token is None or signal is None:
                    continue
                user_ids = list(
                    await session.scalars(
                        select(AlertDelivery.user_id).where(AlertDelivery.signal_id == signal.id)
                    )
                )
                users = list(
                    await session.scalars(select(User).where(User.id.in_(user_ids)))
                ) if user_ids else []
            if row.unlock_at is None:
                continue
            text = unlock_alert_text(symbol=token.symbol, mint=token.mint, unlock_at=row.unlock_at)
            delivered = False
            for user in users:
                try:
                    await self._sender.send(user.telegram_id, text)
                    delivered = True
                except TelegramError:
                    logger.warning("unlock_alert_failed", mint=token.mint)
            if delivered:
                async with self._sessions() as session:
                    async with session.begin():
                        stored = await session.get(LpUnlock, row.id)
                        if stored is not None and stored.alerted_at is None:
                            stored.status = "alerted"
                            stored.alerted_at = now
                logger.info("lp_unlock_alert_sent", mint=token.mint)

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
        if snapshot.defer:
            logger.info("alert_waiting_for_ai_review", signal_id=str(signal_id), mint=snapshot.mint)
            return _Outcome(0, 0)
        sent = 0
        failed = 0
        delivered: list[UUID] = []
        deactivated: list[UUID] = []
        missing = unavailable_fields(snapshot.risk_flags)
        if missing:
            logger.warning(
                "alert_data_incomplete",
                mint=snapshot.mint,
                missing=missing,
                reason="scan_fields_unavailable",
            )
        with bind_trace(snapshot.trace_id):
            for recipient in snapshot.recipients:
                if missing:
                    text = data_incomplete_text(mint=snapshot.mint, missing=missing)
                else:
                    text = alert_text(
                        symbol=snapshot.symbol,
                        mint=snapshot.mint,
                        composite_score=snapshot.composite_score,
                        risk_score=snapshot.risk_score,
                        risk_flags=snapshot.risk_flags,
                        bait_warning=snapshot.bait_warning,
                        ai_review=snapshot.ai_review,
                        context=snapshot.context,
                        creator_over_limit=share_above_limit(
                            snapshot.context,
                            recipient.max_dev_cluster_share,
                        ),
                    )
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
                select(func.count()).select_from(BaitFlag).where(BaitFlag.token_id == token.id)
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
                if not accepts_alert(
                    prefs,
                    composite_score=signal.composite_score,
                    risk_score=signal.risk_score,
                ):
                    continue
                recipients.append(
                    _Recipient(user.id, user.telegram_id, prefs.max_dev_cluster_share)
                )
            flags = token.risk_flags if isinstance(token.risk_flags, list) else []
            review = _completed_review(token)
            waiting = (
                review is None
                and token.ai_review_status == AI_REVIEW_PENDING
                and signal.triggered_at
                >= self._clock() - timedelta(seconds=AI_REVIEW_ALERT_WAIT_SECONDS)
            )
            return _Snapshot(
                signal_id=signal.id,
                symbol=token.symbol,
                mint=token.mint,
                composite_score=signal.composite_score,
                risk_score=signal.risk_score,
                risk_flags=[str(flag) for flag in flags],
                bait_warning=bool(bait_count),
                ai_review=review,
                defer=waiting,
                trace_id=signal.trace_id,
                recipients=recipients,
                context=signal.context if isinstance(signal.context, dict) else None,
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
                        .on_conflict_do_nothing(constraint="uq_alert_deliveries_user_id_signal_id")
                    )
                if deactivated:
                    rows = list(await session.scalars(select(User).where(User.id.in_(deactivated))))
                    for row in rows:
                        row.settings = with_updates(row.settings, active=False)
                if mark_notified:
                    await session.execute(
                        update(Signal).where(Signal.id == signal_id).values(notified=True)
                    )


def _completed_review(token: Token) -> str | None:
    if token.ai_review_status != AI_REVIEW_COMPLETED:
        return None
    text = token.ai_review
    if not isinstance(text, str) or not text.strip():
        return None
    return text


def _stopped(stop: object | None) -> bool:
    if stop is None:
        return False
    is_set = getattr(stop, "is_set", None)
    return bool(is_set()) if callable(is_set) else False
