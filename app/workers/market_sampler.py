"""Market-cap readings after each signal, for channel charts and results.

Readings come from DexScreener in batches of 30 mints: no Helius credits.
How often a signal is read depends on its age (``MARKET_SAMPLE_SCHEDULE_MINUTES``).
A mint DexScreener does not list, or a pair without a market cap, gets no row
for that cycle. Nothing is filled in.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.dexscreener import DexQuote
from app.config import MARKET_SAMPLE_SCHEDULE_MINUTES
from app.db.models import MarketPoint, Signal, Token

logger = structlog.get_logger(__name__)

Clock = Callable[[], datetime]


class QuoteSource(Protocol):
    async def token_quotes(self, mints: list[str]) -> dict[str, DexQuote]: ...


def sample_interval(age: timedelta) -> timedelta | None:
    """Minutes between readings for a signal this old, or ``None`` past the schedule."""
    minutes = age.total_seconds() / 60
    for limit, step in MARKET_SAMPLE_SCHEDULE_MINUTES:
        if minutes <= limit:
            return timedelta(minutes=step)
    return None


def is_due(age: timedelta, last_reading_age: timedelta | None) -> bool:
    interval = sample_interval(age)
    if interval is None:
        return False
    # A little slack so a 5-minute step does not slip to 6 on a 60-second poll.
    return last_reading_age is None or last_reading_age >= interval - timedelta(seconds=30)


class MarketSampler:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        quotes: QuoteSource,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._sessions = sessions
        self._quotes = quotes
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def run_once(self) -> int:
        now = self._clock()
        oldest = max(limit for limit, _step in MARKET_SAMPLE_SCHEDULE_MINUTES)
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(
                        Token.id,
                        Token.mint,
                        Signal.triggered_at,
                        select(func.max(MarketPoint.observed_at))
                        .where(MarketPoint.token_id == Token.id)
                        .scalar_subquery(),
                    )
                    .join(Signal, Signal.token_id == Token.id)
                    .where(Signal.triggered_at >= now - timedelta(minutes=oldest))
                )
            ).all()
        due = {
            mint: token_id
            for token_id, mint, triggered_at, last in rows
            if is_due(now - triggered_at, None if last is None else now - last)
        }
        if not due:
            return 0
        try:
            quotes = await self._quotes.token_quotes(sorted(due))
        except Exception as exc:
            logger.warning("market_sample_failed", mints=len(due), error_type=type(exc).__name__)
            return 0
        stored = 0
        async with self._sessions() as session:
            async with session.begin():
                for mint, token_id in due.items():
                    quote = quotes.get(mint)
                    if quote is None or quote.market_cap_usd is None:
                        continue
                    session.add(_point(token_id, now, quote))
                    stored += 1
        logger.info("market_sample_cycle", due=len(due), stored=stored)
        return stored


def _point(token_id: Any, now: datetime, quote: DexQuote) -> MarketPoint:
    return MarketPoint(
        token_id=token_id,
        observed_at=now,
        market_cap_usd=quote.market_cap_usd,
        price_usd=quote.price_usd,
        liquidity_usd=quote.liquidity_usd,
    )
