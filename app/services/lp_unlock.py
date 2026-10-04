"""LP unlock warnings.

Burn custody has no vesting clock: status ``no_schedule``, no alert.
Raydium's Burn & Earn program escrow does not publish an unlock timestamp
in the account layout this process can verify, so custody there is
``unavailable`` (``lock_schedule_unreadable``), not a guessed date.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from app.config import LP_LOCKED_MIN_SHARE


@dataclass(frozen=True)
class LpCustody:
    is_burn_address: bool
    is_lock_program: bool
    amount_raw: int


@dataclass(frozen=True)
class UnlockReading:
    status: str
    reason: str | None


def lp_is_locked(supply_raw: int | None, rows: list[LpCustody]) -> bool | None:
    """Whether burned or escrowed LP meets the same share the scanner uses.

    ``None`` means the follow-up could not measure supply. That is not "unlocked".
    """
    if supply_raw is None or supply_raw <= 0:
        return None
    locked = sum(row.amount_raw for row in rows if row.is_burn_address or row.is_lock_program)
    return Decimal(locked) / Decimal(supply_raw) >= LP_LOCKED_MIN_SHARE


def classify_lp_custody(rows: list[LpCustody]) -> UnlockReading:
    if not rows:
        return UnlockReading("unavailable", "no_lp_holders")
    in_program = any(row.is_lock_program and row.amount_raw > 0 for row in rows)
    burned = any(row.is_burn_address and row.amount_raw > 0 for row in rows)
    if in_program:
        return UnlockReading("unavailable", "lock_schedule_unreadable")
    if burned:
        return UnlockReading("no_schedule", "burn_address")
    return UnlockReading("no_schedule", "lp_not_locked")


def unlock_alert_due(
    *,
    status: str,
    unlock_at: datetime | None,
    alerted_at: datetime | None,
    now: datetime,
    lead: timedelta,
) -> bool:
    """True only for a known unlock time inside the lead window, once."""
    if status != "scheduled" or unlock_at is None or alerted_at is not None:
        return False
    return now >= unlock_at - lead
