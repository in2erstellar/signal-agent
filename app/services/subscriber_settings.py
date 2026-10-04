"""Subscriber alert filters stored in users.settings.

``min_score`` is inclusive: a composite equal to the setting still alerts.
``max_risk`` is the highest ``100 - risk_score`` the subscriber accepts.
Raising it admits more tokens. A missing ``risk_score`` counts as 100
points of risk, so only ``max_risk`` of 100 lets it through.
``active`` is false after Telegram reports that the user blocked the bot.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.config import DEFAULT_MAX_DEV_CLUSTER_SHARE, DEFAULT_MAX_RISK, DEFAULT_MIN_SCORE

# name -> (score floor, risk cap). Buttons on the settings screen.
ALERT_PRESETS: dict[str, tuple[Decimal, Decimal]] = {
    "strict": (Decimal("70"), Decimal("25")),
    "balanced": (Decimal("50"), Decimal("60")),
    "wide": (Decimal("30"), Decimal("100")),
}


@dataclass(frozen=True)
class AlertPrefs:
    min_score: Decimal
    max_risk: Decimal
    active: bool
    max_dev_cluster_share: Decimal = DEFAULT_MAX_DEV_CLUSTER_SHARE


def parse_settings(raw: dict[str, Any] | None) -> AlertPrefs:
    data = raw or {}
    return AlertPrefs(
        min_score=_decimal(data.get("min_score"), DEFAULT_MIN_SCORE),
        max_risk=_decimal(data.get("max_risk"), DEFAULT_MAX_RISK),
        active=bool(data.get("active", True)),
        max_dev_cluster_share=_decimal(
            data.get("max_dev_cluster_share"),
            DEFAULT_MAX_DEV_CLUSTER_SHARE,
        ),
    )


def accepts_alert(
    prefs: AlertPrefs,
    *,
    composite_score: Decimal,
    risk_score: Decimal | None,
) -> bool:
    if not prefs.active:
        return False
    if composite_score < prefs.min_score:
        return False
    return risk_points(risk_score) <= prefs.max_risk


def risk_points(risk_score: Decimal | None) -> Decimal:
    if risk_score is None:
        return Decimal(100)
    return Decimal(100) - risk_score


def with_updates(raw: dict[str, Any] | None, **changes: object) -> dict[str, Any]:
    merged = dict(raw or {})
    for key, value in changes.items():
        if isinstance(value, Decimal):
            merged[key] = str(value)
        else:
            merged[key] = value
    return merged


def _decimal(value: object, default: Decimal) -> Decimal:
    if value is None or value == "":
        return default
    return Decimal(str(value))
