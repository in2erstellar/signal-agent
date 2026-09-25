"""AI review gate: liquidity, score band, and independent hourly and daily caps."""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.config import (
    AI_REVIEW_DAILY_BUDGET,
    AI_REVIEW_HOURLY_BUDGET,
    AI_REVIEW_MIN_LIQUIDITY,
    AI_REVIEW_SCORE_MAX,
    AI_REVIEW_SCORE_MIN,
)
from app.workers.signal_fusion import qualifies_for_ai_review


class _Token:
    def __init__(self, liquidity: Decimal | None, score: Decimal | None) -> None:
        self.liquidity_usd = liquidity
        self.risk_score = score


def test_budget_constants_let_the_daily_cap_bind_first() -> None:
    assert AI_REVIEW_HOURLY_BUDGET == 10
    assert AI_REVIEW_DAILY_BUDGET == 150
    assert AI_REVIEW_DAILY_BUDGET < AI_REVIEW_HOURLY_BUDGET * 24
    assert AI_REVIEW_MIN_LIQUIDITY == Decimal("5000")
    assert AI_REVIEW_SCORE_MIN == Decimal("25")
    assert AI_REVIEW_SCORE_MAX == Decimal("85")


@pytest.mark.parametrize(
    ("liquidity", "score", "hour", "day", "expected"),
    [
        (None, Decimal("50"), 0, 0, False),
        (Decimal("4999.99"), Decimal("50"), 0, 0, False),
        (Decimal("5000"), Decimal("50"), 0, 0, True),
        (Decimal("5000"), None, 0, 0, False),
        (Decimal("5000"), Decimal("24.99"), 0, 0, False),
        (Decimal("5000"), Decimal("25"), 0, 0, True),
        (Decimal("5000"), Decimal("85"), 0, 0, True),
        (Decimal("5000"), Decimal("85.01"), 0, 0, False),
        (Decimal("5000"), Decimal("50"), 9, 149, True),
        (Decimal("5000"), Decimal("50"), 10, 0, False),
        (Decimal("5000"), Decimal("50"), 0, 150, False),
        (Decimal("5000"), Decimal("50"), 9, 150, False),
        (Decimal("5000"), Decimal("50"), 10, 149, False),
    ],
)
def test_qualifies_for_ai_review_boundaries(
    liquidity: Decimal | None,
    score: Decimal | None,
    hour: int,
    day: int,
    expected: bool,
) -> None:
    assert (
        qualifies_for_ai_review(
            _Token(liquidity, score),
            reviews_this_hour=hour,
            reviews_today=day,
        )
        is expected
    )
