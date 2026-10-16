"""The AI note reads the card's checklist; the card keeps the creator block short."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.adapters.ai_review import TokenReviewInput, _user_prompt
from app.services.quality_checks import checklist_for_review
from app.templates import alert_text
from app.workers.ai_review_worker import SIGNAL_WAIT, AiReviewWorker

NOW = datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc)
MINT = "Mint1111111111111111111111111111111111111"
ROWS = [
    {
        "key": "mint_authority",
        "group": "contract",
        "status": "pass",
        "text": "Mint authority revoked",
    },
    {
        "key": "launch_trades",
        "group": "holders",
        "status": "fail",
        "text": "Trades in the first 2 blocks: 8 (max 3)",
    },
    {"key": "socials", "group": "market", "status": "unavailable", "text": "Socials: no data"},
]


def test_review_lines_keep_status_and_reading() -> None:
    assert checklist_for_review({"quality_checks": ROWS}) == (
        "pass: Mint authority revoked",
        "fail: Trades in the first 2 blocks: 8 (max 3)",
        "no data: Socials: no data",
    )
    assert checklist_for_review(None) == ()
    assert checklist_for_review({"distribution_status": "measured"}) == ()


def test_prompt_uses_the_checklist_instead_of_the_old_score() -> None:
    prompt = _user_prompt(
        TokenReviewInput(
            mint=MINT,
            symbol="TWMW",
            name="The Wojak Market Wizard",
            risk_score="85",
            risk_flags=("check_holder_concentration",),
            liquidity_usd=None,
            composite_score="75.00",
            checklist=checklist_for_review({"quality_checks": ROWS}),
        )
    )
    assert "checks passed: 1 of 3" in prompt
    assert "- fail: Trades in the first 2 blocks: 8 (max 3)" in prompt
    assert "checks_passed_score" not in prompt
    assert "liquidity_usd" not in prompt
    assert "composite_score" not in prompt


def test_review_waits_for_the_signal_row_then_gives_up() -> None:
    clock = {"now": NOW}
    worker = AiReviewWorker(None, None, None, clock=lambda: clock["now"])  # type: ignore[arg-type]
    fields = {"reserved_at": NOW.isoformat()}
    assert worker._signal_can_still_arrive(fields)
    clock["now"] = NOW + SIGNAL_WAIT + timedelta(seconds=1)
    assert not worker._signal_can_still_arrive(fields)
    assert not worker._signal_can_still_arrive({})


def _card(context: dict[str, object]) -> str:
    return alert_text(
        symbol="TWMW",
        mint=MINT,
        composite_score=Decimal("75"),
        risk_score=Decimal("85"),
        risk_flags=[],
        bait_warning=False,
        context={"quality_checks": ROWS, **context},
    )


def test_young_creator_gets_one_line() -> None:
    text = _card(
        {
            "creator": "CreatorWallet",
            "launches_since_tracking": 0,
            "price_drop_shown": "unavailable (no earlier launches)",
            "median_sell_shown": "unavailable (not in the tracked-wallet ledger)",
            "distribution_status": "measured",
            "distribution_share": "0",
        }
    )
    assert "👤 Creator: <code>CreatorWallet</code>" in text
    assert "Creator cluster" not in text
    assert "Supply on the creator's funder" not in text
    assert "Previous launches" not in text
    assert "tracked-wallet ledger" not in text


def test_creator_with_history_shows_the_measured_lines() -> None:
    text = _card(
        {
            "creator": "CreatorWallet",
            "launches_since_tracking": 3,
            "price_drop_shown": "2 of 3",
            "lp_stayed_shown": "unavailable (no follow-up yet)",
        }
    )
    assert "Previous launches since tracking started: <code>3</code>" in text
    assert "First-hour drop over 90%: <code>2 of 3</code>" in text
    assert "LP stayed locked" not in text


def test_counts_read_as_english() -> None:
    from app.services.quality_checks import _buy_pressure_check, _clone_check
    from tests.test_quality_checks import _dex

    assert _clone_check(1, "TWMW").text == "1 other token named TWMW in 24h"
    assert _clone_check(2, "TWMW").text == "2 other tokens named TWMW in 24h"
    assert _buy_pressure_check(_dex(buys_5m=1, sells_5m=1)).text == "Last 5 min: 1 buy / 1 sell"
