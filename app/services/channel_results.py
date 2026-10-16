"""What a signal's market cap did by 1h, 24h, and 7d, from stored readings.

The baseline is the first reading taken within ``CHANNEL_BASELINE_MAX_MINUTES``
of the signal. The end is the last reading at or before the horizon and not
older than its tolerance. Without both there is no result, never a guess.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from app.config import CHANNEL_BASELINE_MAX_MINUTES
from app.services.ascii_chart import render

DOUBLED = Decimal("1")
COLLAPSED = Decimal("-0.9")

_X_LABELS = {
    60: ("0m", "30m", "60m"),
    24 * 60: ("0h", "12h", "24h"),
    7 * 24 * 60: ("0d", "3.5d", "7d"),
}


@dataclass(frozen=True)
class HorizonResult:
    base: Decimal
    end: Decimal
    change: Decimal
    peak: Decimal
    peak_minutes: float
    chart: str | None


def offsets(
    points: list[tuple[datetime, Decimal]], triggered_at: datetime
) -> list[tuple[float, Decimal]]:
    return sorted(
        ((observed - triggered_at).total_seconds() / 60, value) for observed, value in points
    )


def baseline(points: list[tuple[float, Decimal]]) -> Decimal | None:
    for offset, value in points:
        if 0 <= offset <= CHANNEL_BASELINE_MAX_MINUTES:
            return value
    return None


def horizon_result(
    points: list[tuple[float, Decimal]],
    *,
    minutes: int,
    tolerance: int,
) -> HorizonResult | None:
    base = baseline(points)
    window = [(offset, value) for offset, value in points if 0 <= offset <= minutes]
    ends = [value for offset, value in window if offset >= minutes - tolerance]
    if base is None or not ends:
        return None
    end = ends[-1]
    peak_minutes, peak = max(window, key=lambda item: item[1])
    labels = _X_LABELS.get(minutes, ("0", "", f"{minutes}m"))
    return HorizonResult(
        base=base,
        end=end,
        change=end / base - 1,
        peak=peak,
        peak_minutes=peak_minutes,
        chart=render(window, span=minutes, x_labels=labels),
    )


def verdict_icon(change: Decimal) -> str:
    if change >= DOUBLED:
        return "🚀"
    if change > 0:
        return "📈"
    if change > COLLAPSED:
        return "📉"
    return "💀"
