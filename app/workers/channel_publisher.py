"""Public track-record channel: one post per signal, then result replies.

The first post goes out when the signal is sent, so Telegram's own post time
is the proof. 1h, 24h, and 7d results are replies to it, computed only from
stored market-cap readings. A result without a reading in time is recorded
as skipped, so it is not retried forever and is never filled in. A failed
send leaves no row and is tried again on the next cycle.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Protocol

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from app.config import (
    CHANNEL_BASELINE_MAX_MINUTES,
    CHANNEL_RESULT_GRACE_MINUTES,
    CHANNEL_RESULT_HORIZONS,
    CHANNEL_SIGNAL_MAX_AGE_MINUTES,
    CHANNEL_SIGNAL_WAIT_FOR_CAP_SECONDS,
)
from app.db.models import ChannelPost, MarketPoint, Signal, Token
from app.services.channel_results import horizon_result, offsets
from app.templates import CHANNEL_CARD_BUTTON, channel_result_text, channel_signal_text

logger = structlog.get_logger(__name__)

Clock = Callable[[], datetime]
LinkBuilder = Callable[[Signal], str | None]


class ChannelSender(Protocol):
    async def post(
        self,
        text: str,
        *,
        reply_to: int | None = None,
        link: tuple[str, str] | None = None,
    ) -> int: ...


class ChannelPublisher:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        sender: ChannelSender,
        *,
        card_link: LinkBuilder,
        clock: Clock | None = None,
    ) -> None:
        self._sessions = sessions
        self._sender = sender
        self._card_link = card_link
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def run_once(self) -> int:
        now = self._clock()
        posted = await self._post_signals(now)
        for kind, minutes, tolerance in CHANNEL_RESULT_HORIZONS:
            posted += await self._post_results(now, kind, minutes, tolerance)
        return posted

    async def _post_signals(self, now: datetime) -> int:
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(Signal, Token)
                    .join(Token, Token.id == Signal.token_id)
                    .where(
                        Signal.triggered_at
                        >= now - timedelta(minutes=CHANNEL_SIGNAL_MAX_AGE_MINUTES),
                        ~select(ChannelPost.id)
                        .where(ChannelPost.signal_id == Signal.id, ChannelPost.kind == "signal")
                        .exists(),
                    )
                    .order_by(Signal.triggered_at)
                )
            ).all()
        posted = 0
        for signal, token in rows:
            context = signal.context if isinstance(signal.context, dict) else {}
            if not context.get("quality_checks"):
                continue
            cap = await self._baseline_cap(token.id, signal.triggered_at)
            waited = (now - signal.triggered_at).total_seconds()
            if cap is None and waited < CHANNEL_SIGNAL_WAIT_FOR_CAP_SECONDS:
                continue
            text = channel_signal_text(
                symbol=token.symbol,
                mint=token.mint,
                passed=int(context.get("quality_passed") or 0),
                total=int(context.get("quality_total") or len(context["quality_checks"])),
                triggered_at=signal.triggered_at,
                market_cap=cap,
            )
            url = self._card_link(signal)
            message_id = await self._send(
                text, link=None if url is None else (CHANNEL_CARD_BUTTON, url), mint=token.mint
            )
            if message_id is None:
                continue
            await self._record(signal.id, "signal", message_id, now)
            posted += 1
        return posted

    async def _post_results(self, now: datetime, kind: str, minutes: int, tolerance: int) -> int:
        due_before = now - timedelta(minutes=minutes)
        oldest = due_before - timedelta(minutes=CHANNEL_RESULT_GRACE_MINUTES)
        parent = aliased(ChannelPost)
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(Signal, Token, parent.message_id)
                    .join(Token, Token.id == Signal.token_id)
                    .join(
                        parent,
                        (parent.signal_id == Signal.id)
                        & (parent.kind == "signal")
                        & (parent.status == "posted"),
                    )
                    .where(
                        Signal.triggered_at <= due_before,
                        Signal.triggered_at >= oldest - timedelta(days=1),
                        ~select(ChannelPost.id)
                        .where(ChannelPost.signal_id == Signal.id, ChannelPost.kind == kind)
                        .exists(),
                    )
                )
            ).all()
        posted = 0
        for signal, token, parent_id in rows:
            points = await self._points(token.id, signal.triggered_at, minutes)
            result = horizon_result(points, minutes=minutes, tolerance=tolerance)
            if result is None:
                if signal.triggered_at < oldest:
                    await self._record(signal.id, kind, None, now, status="skipped")
                    logger.info("channel_result_skipped", mint=token.mint, kind=kind)
                continue
            text = channel_result_text(symbol=token.symbol, horizon=kind, result=result)
            message_id = await self._send(text, reply_to=parent_id, mint=token.mint)
            if message_id is None:
                continue
            await self._record(signal.id, kind, message_id, now)
            posted += 1
        return posted

    async def _baseline_cap(self, token_id: object, triggered_at: datetime) -> Decimal | None:
        async with self._sessions() as session:
            cap: Decimal | None = await session.scalar(
                select(MarketPoint.market_cap_usd)
                .where(
                    MarketPoint.token_id == token_id,
                    MarketPoint.observed_at >= triggered_at,
                    MarketPoint.observed_at
                    <= triggered_at + timedelta(minutes=CHANNEL_BASELINE_MAX_MINUTES),
                )
                .order_by(MarketPoint.observed_at)
                .limit(1)
            )
        return cap

    async def _points(
        self, token_id: object, triggered_at: datetime, minutes: int
    ) -> list[tuple[float, Decimal]]:
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(MarketPoint.observed_at, MarketPoint.market_cap_usd).where(
                        MarketPoint.token_id == token_id,
                        MarketPoint.observed_at >= triggered_at,
                        MarketPoint.observed_at <= triggered_at + timedelta(minutes=minutes),
                    )
                )
            ).all()
        return offsets([(observed, cap) for observed, cap in rows], triggered_at)

    async def _send(
        self,
        text: str,
        *,
        mint: str,
        reply_to: int | None = None,
        link: tuple[str, str] | None = None,
    ) -> int | None:
        try:
            return await self._sender.post(text, reply_to=reply_to, link=link)
        except Exception as exc:
            logger.warning("channel_post_failed", mint=mint, error_type=type(exc).__name__)
            return None

    async def _record(
        self,
        signal_id: object,
        kind: str,
        message_id: int | None,
        now: datetime,
        *,
        status: str = "posted",
    ) -> None:
        async with self._sessions() as session:
            async with session.begin():
                session.add(
                    ChannelPost(
                        signal_id=signal_id,
                        kind=kind,
                        message_id=message_id,
                        status=status,
                        posted_at=now,
                    )
                )
        if status == "posted":
            logger.info("channel_posted", kind=kind, signal_id=str(signal_id))
