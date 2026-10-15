"""Pump.fun creates stay pending until a wallet other than the creator holds.

The creation stream does not include later trades. ``subscribeTokenTrade``
is paid, so this worker does not subscribe to it. A non-creator buy is a
``getTokenLargestAccounts`` owner that is neither ``traderPublicKey`` nor
``bondingCurveKey``. A higher curve SOL balance is not that proof: the
creator can buy again.

Rows still pending after 24 hours are marked ``dead``. They stay in
``tokens`` and are not queued. Qualified rows are written to
``risk_scan_queue`` with ``funder_graph_status = skipped_pre_migration``.
The funder graph has not been verified on bonding-curve holders: largest
accounts include the curve PDA, and LP exclusion only knows Raydium.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

import structlog
from redis.asyncio import Redis
from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.pumpportal import MigrationEvent, NewTokenEvent
from app.config import (
    DISCOVERY_FRESH_WINDOW_HOURS,
    DISCOVERY_PENDING_BATCH,
    DISCOVERY_PENDING_MAX_AGE_HOURS,
    RISK_SCAN_PRIORITY_QUEUE,
    RISK_SCAN_QUEUE,
)
from app.db.models import Token
from app.observability import new_trace_id
from app.services.token_store import (
    DISCOVERY_DEAD,
    DISCOVERY_PENDING,
    DISCOVERY_QUALIFIED,
    FUNDER_GRAPH_PENDING_POST,
    FUNDER_GRAPH_SKIPPED,
    insert_token,
)

logger = structlog.get_logger(__name__)

Clock = Callable[[], datetime]
_HOLDER_TOP_N = 20


class HolderLookup(Protocol):
    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]: ...


@dataclass(frozen=True)
class ReviewCounts:
    qualified: int
    dead: int
    checked: int


@dataclass(frozen=True)
class _Pending:
    mint: str
    creator_address: str
    bonding_curve_key: str


def _copy_pending(rows: list[Token]) -> list[_Pending]:
    pending: list[_Pending] = []
    for row in rows:
        if not row.creator_address or not row.bonding_curve_key:
            continue
        pending.append(
            _Pending(
                mint=row.mint,
                creator_address=row.creator_address,
                bonding_curve_key=row.bonding_curve_key,
            )
        )
    return pending


class TokenDiscovery:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        redis: Redis,
        holders: HolderLookup,
        *,
        clock: Clock | None = None,
        pending_batch: int = DISCOVERY_PENDING_BATCH,
        pending_max_age: timedelta | None = None,
        fresh_window: timedelta | None = None,
        qualify_pre_migration: bool = True,
    ) -> None:
        self._sessions = sessions
        self._qualify_pre_migration = qualify_pre_migration
        self._redis = redis
        self._holders = holders
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._batch = pending_batch
        self._max_age = pending_max_age or timedelta(hours=DISCOVERY_PENDING_MAX_AGE_HOURS)
        self._fresh_window = fresh_window or timedelta(hours=DISCOVERY_FRESH_WINDOW_HOURS)
        # Newest-first cursor inside the fresh window. Cleared when that pass ends.
        self._fresh_before: tuple[datetime, str] | None = None
        # Oldest-first cursor for rows older than the fresh window.
        self._backlog_after: tuple[datetime, str] | None = None
        self.creates = 0
        self.qualified_total = 0
        self.migrations_queued = 0
        self.holder_failures = 0

    async def ingest_create(self, event: NewTokenEvent) -> bool:
        """Store minimal create metadata. Does not touch the risk queue."""
        now = self._clock()
        async with self._sessions() as session:
            async with session.begin():
                _token, created = await insert_token(
                    session,
                    mint=event.mint,
                    first_seen_at=now,
                    symbol=event.symbol,
                    name=event.name,
                    creator_address=event.creator_address,
                    bonding_curve_key=event.bonding_curve_key,
                    bonding_curve_liquidity_sol=event.bonding_curve_liquidity_sol,
                    discovery_status=DISCOVERY_PENDING,
                    funder_graph_status=FUNDER_GRAPH_SKIPPED,
                )
        if created:
            self.creates += 1
            logger.info(
                "token_discovery_pending",
                mint=event.mint,
                symbol=event.symbol,
                creates=self.creates,
            )
        return created

    async def handle_migration(self, event: MigrationEvent) -> bool:
        """Re-queue the funder graph after the mint leaves the bonding curve.

        Only ``skipped_pre_migration`` changes. A mint that never migrates
        stays skipped. The risk worker reads ``getTokenLargestAccounts`` when
        it builds the graph, so this does not pass any pre-migration holder list.
        """
        trace_id = new_trace_id()
        async with self._sessions() as session:
            async with session.begin():
                token = await session.scalar(select(Token).where(Token.mint == event.mint))
                if token is None or token.funder_graph_status != FUNDER_GRAPH_SKIPPED:
                    logger.info(
                        "token_discovery_migration_ignored",
                        mint=event.mint,
                        status=None if token is None else token.funder_graph_status,
                    )
                    return False
                token.funder_graph_status = FUNDER_GRAPH_PENDING_POST
                token.trace_id = trace_id
                if token.discovery_status in {DISCOVERY_PENDING, DISCOVERY_DEAD}:
                    # A completed curve is proof of foreign buyers.
                    token.discovery_status = DISCOVERY_QUALIFIED
                await self._redis.xadd(
                    RISK_SCAN_PRIORITY_QUEUE,
                    {"mint": event.mint, "trace_id": trace_id, "reason": "post_migration"},
                )
        self.migrations_queued += 1
        logger.info(
            "token_discovery_migration_queued",
            mint=event.mint,
            trace_id=trace_id,
            pool=event.pool,
            migrations_queued=self.migrations_queued,
        )
        return True

    async def review_pending(self) -> ReviewCounts:
        now = self._clock()
        dead = await self._mark_dead(now)
        if not self._qualify_pre_migration:
            # Only the 24h dead path runs. Migration events queue the scans.
            logger.info(
                "token_discovery_review",
                checked=0,
                qualified=0,
                dead=dead,
                creates=self.creates,
                migrations_queued=self.migrations_queued,
                mode="post_migration_only",
            )
            return ReviewCounts(qualified=0, dead=dead, checked=0)
        batch, fresh_checked, backlog_checked = await self._next_batch(now)
        qualified = 0
        for item in batch:
            if await self._qualify_if_foreign_buy(item):
                qualified += 1
        logger.info(
            "token_discovery_review",
            checked=len(batch),
            fresh_checked=fresh_checked,
            backlog_checked=backlog_checked,
            qualified=qualified,
            dead=dead,
            creates=self.creates,
            qualified_total=self.qualified_total,
            migrations_queued=self.migrations_queued,
            holder_failures=self.holder_failures,
        )
        return ReviewCounts(qualified=qualified, dead=dead, checked=len(batch))

    async def _mark_dead(self, now: datetime) -> int:
        cutoff = now - self._max_age
        async with self._sessions() as session:
            async with session.begin():
                result = await session.execute(
                    update(Token)
                    .where(
                        Token.discovery_status == DISCOVERY_PENDING,
                        Token.first_seen_at <= cutoff,
                    )
                    .values(discovery_status=DISCOVERY_DEAD)
                )
        count = int(result.rowcount or 0)
        if count:
            logger.info("token_discovery_marked_dead", count=count)
        return count

    async def _next_batch(self, now: datetime) -> tuple[list[_Pending], int, int]:
        """Fresh window first, then leftover slots for the older backlog.

        The live hour checked 1780 oldest pending rows and zero creates from
        that hour. A second buy, when it happens, is usually in the first
        minutes, so those creates have to be reached before the tail.

        Newest-first inside the fresh window uses a cursor. A full page does
        not restart on the same twenty mints. When the page is short or the
        cursor is exhausted, this cycle does not wrap back to the newest mint:
        the open slots go to older pending rows, oldest first, so the 24h
        dead path still moves. The next cycle starts the fresh window again.
        """
        dead_cutoff = now - self._max_age
        fresh_after = now - self._fresh_window
        fresh_floor = fresh_after if fresh_after > dead_cutoff else dead_cutoff
        async with self._sessions() as session:
            fresh_rows = await self._select_window(
                session,
                lower_exclusive=fresh_floor,
                upper_inclusive=None,
                cursor=self._fresh_before,
                newest_first=True,
                limit=self._batch,
            )
            if not fresh_rows and self._fresh_before is not None:
                # End of the fresh pass. Leave this cycle for the backlog.
                self._fresh_before = None
            elif len(fresh_rows) < self._batch:
                self._fresh_before = None
            else:
                last = fresh_rows[-1]
                self._fresh_before = (last.first_seen_at, last.mint)

            need = self._batch - len(fresh_rows)
            backlog_rows: list[Token] = []
            if need > 0:
                backlog_rows = await self._select_window(
                    session,
                    lower_exclusive=dead_cutoff,
                    upper_inclusive=fresh_after,
                    cursor=self._backlog_after,
                    newest_first=False,
                    limit=need,
                )
                if not backlog_rows and self._backlog_after is not None:
                    self._backlog_after = None
                    backlog_rows = await self._select_window(
                        session,
                        lower_exclusive=dead_cutoff,
                        upper_inclusive=fresh_after,
                        cursor=None,
                        newest_first=False,
                        limit=need,
                    )
                if len(backlog_rows) < need:
                    self._backlog_after = None
                elif backlog_rows:
                    last_old = backlog_rows[-1]
                    self._backlog_after = (last_old.first_seen_at, last_old.mint)
        pending = _copy_pending([*fresh_rows, *backlog_rows])
        return pending, len(fresh_rows), len(backlog_rows)

    async def _select_window(
        self,
        session: AsyncSession,
        *,
        lower_exclusive: datetime,
        upper_inclusive: datetime | None,
        cursor: tuple[datetime, str] | None,
        newest_first: bool,
        limit: int,
    ) -> list[Token]:
        stmt = select(Token).where(
            Token.discovery_status == DISCOVERY_PENDING,
            Token.first_seen_at > lower_exclusive,
        )
        if upper_inclusive is not None:
            stmt = stmt.where(Token.first_seen_at <= upper_inclusive)
        if newest_first:
            stmt = stmt.order_by(Token.first_seen_at.desc(), Token.mint.desc())
        else:
            stmt = stmt.order_by(Token.first_seen_at.asc(), Token.mint.asc())
        if cursor is not None:
            seen, mint = cursor
            if newest_first:
                stmt = stmt.where(
                    or_(
                        Token.first_seen_at < seen,
                        and_(Token.first_seen_at == seen, Token.mint < mint),
                    )
                )
            else:
                stmt = stmt.where(
                    or_(
                        Token.first_seen_at > seen,
                        and_(Token.first_seen_at == seen, Token.mint > mint),
                    )
                )
        stmt = stmt.limit(limit)
        return list(await session.scalars(stmt))

    async def _qualify_if_foreign_buy(self, item: _Pending) -> bool:
        try:
            owners = await self._holders.get_top_holder_owners(item.mint, _HOLDER_TOP_N)
        except Exception as exc:
            self.holder_failures += 1
            logger.warning(
                "token_discovery_holders_failed",
                mint=item.mint,
                error_type=type(exc).__name__,
                error=str(exc),
                holder_failures=self.holder_failures,
            )
            return False
        excluded = {item.creator_address, item.bonding_curve_key}
        if not any(owner and owner not in excluded for owner in owners):
            return False
        trace_id = new_trace_id()
        async with self._sessions() as session:
            async with session.begin():
                token = await session.scalar(select(Token).where(Token.mint == item.mint))
                if token is None or token.discovery_status != DISCOVERY_PENDING:
                    return False
                token.discovery_status = DISCOVERY_QUALIFIED
                token.funder_graph_status = FUNDER_GRAPH_SKIPPED
                token.trace_id = trace_id
                await self._redis.xadd(
                    RISK_SCAN_QUEUE,
                    {"mint": item.mint, "trace_id": trace_id},
                )
        self.qualified_total += 1
        logger.info(
            "token_discovery_qualified",
            mint=item.mint,
            trace_id=trace_id,
            qualified_total=self.qualified_total,
        )
        return True
