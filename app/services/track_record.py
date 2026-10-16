"""Public track record: what happened to the price after each signal.

A change is measured only from an observed listing price to an observed
price at the horizon. A signal without both is left out, never counted as
zero or as a loss.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from statistics import median

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.config import OUTCOME_HORIZONS
from app.db.models import OutcomeCheck, Signal

COLLAPSE_RATIO = Decimal("0.10")
DOUBLE_RATIO = Decimal("2")


@dataclass(frozen=True)
class HorizonStats:
    horizon: str
    measured: int
    collapsed: int
    doubled: int
    median_change: Decimal | None


@dataclass(frozen=True)
class TrackRecord:
    signals: int
    first_signal_at: datetime | None
    horizons: tuple[HorizonStats, ...]


def horizon_stats(horizon: str, ratios: list[Decimal]) -> HorizonStats:
    """``ratios`` are horizon price over listing price, one per measured signal."""
    return HorizonStats(
        horizon=horizon,
        measured=len(ratios),
        collapsed=sum(1 for ratio in ratios if ratio <= COLLAPSE_RATIO),
        doubled=sum(1 for ratio in ratios if ratio >= DOUBLE_RATIO),
        median_change=(median(ratios) - 1) if ratios else None,
    )


async def load_track_record(session: AsyncSession) -> TrackRecord:
    count, first = (
        await session.execute(select(func.count(Signal.id), func.min(Signal.triggered_at)))
    ).one()
    listing = aliased(OutcomeCheck)
    later = aliased(OutcomeCheck)
    stats: list[HorizonStats] = []
    for horizon, _seconds in OUTCOME_HORIZONS:
        rows = (
            await session.execute(
                select(listing.price_usd, later.price_usd)
                .select_from(Signal)
                .join(listing, listing.token_id == Signal.token_id)
                .join(later, later.token_id == Signal.token_id)
                .where(
                    listing.horizon == "listing",
                    listing.status == "observed",
                    listing.price_usd > 0,
                    later.horizon == horizon,
                    later.status == "observed",
                    later.price_usd.is_not(None),
                )
            )
        ).all()
        ratios = [Decimal(after) / Decimal(before) for before, after in rows]
        stats.append(horizon_stats(horizon, ratios))
    return TrackRecord(signals=int(count or 0), first_signal_at=first, horizons=tuple(stats))
