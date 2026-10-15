"""The fast scan skips the heavy half only when no heavy result could reach a signal."""

from __future__ import annotations

from app.workers.risk_scan_worker import HEAVY_CHECKS, heavy_skip_reason

HEAVY_PENDING = [f"{name}:unavailable" for name in HEAVY_CHECKS]


def test_clean_fast_checks_keep_the_heavy_half() -> None:
    assert heavy_skip_reason(HEAVY_PENDING, wallet_history=False) is None


def test_an_unavailable_fast_check_skips_it() -> None:
    flags = ["check_lp_lock:unavailable", *HEAVY_PENDING]
    reason = heavy_skip_reason(flags, wallet_history=False)
    assert reason == "fast_check_unavailable:check_lp_lock"


def test_one_failed_fast_check_still_leaves_room_without_wallets() -> None:
    # 80 risk at best: 80 * 0.5 / 0.7 = 57.14, above the 50 threshold.
    flags = ["check_mint_authority", *HEAVY_PENDING]
    assert heavy_skip_reason(flags, wallet_history=False) is None


def test_two_failed_fast_checks_make_the_threshold_unreachable_without_wallets() -> None:
    # 55 risk at best: 55 * 0.5 / 0.7 = 39.29, not above 50.
    flags = ["check_mint_authority", "check_freeze_authority", *HEAVY_PENDING]
    reason = heavy_skip_reason(flags, wallet_history=False)
    assert reason is not None
    assert reason.startswith("threshold_unreachable")


def test_wallet_history_widens_the_bound() -> None:
    # 55 * 0.5 + 100 * 0.3 = 57.5 with the best wallet score.
    flags = ["check_mint_authority", "check_freeze_authority", *HEAVY_PENDING]
    assert heavy_skip_reason(flags, wallet_history=True) is None


def test_every_fast_check_failed_is_unreachable_even_with_wallets() -> None:
    # 30 * 0.5 + 30 = 45.
    flags = ["check_mint_authority", "check_freeze_authority", "check_lp_lock", *HEAVY_PENDING]
    assert heavy_skip_reason(flags, wallet_history=True) is not None
