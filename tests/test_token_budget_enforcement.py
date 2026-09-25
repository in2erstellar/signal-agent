"""The token cap stops a review even when the call counters still have room."""

from __future__ import annotations

from decimal import Decimal

from app.adapters.ai_review import TokenReviewInput, estimate_review_tokens
from app.config import AI_REVIEW_DAILY_TOKEN_BUDGET
from app.workers.signal_fusion import qualifies_for_ai_review


class _Token:
    def __init__(self) -> None:
        self.liquidity_usd = Decimal("12000")
        self.risk_score = Decimal("50")


def test_chars_over_four_is_the_prompt_estimate() -> None:
    review = TokenReviewInput(
        mint="CMdp4kwo65eeNV9RP4NWGf7ZRXeZnqeszVfJQV2Tpump",
        symbol="ELIZABETH",
        name="The Amazon Monkey",
        risk_score="80",
        risk_flags=("check_lp_lock",),
        liquidity_usd="12000",
        composite_score="57.14",
    )
    from app.adapters.ai_review import _SYSTEM, _user_prompt

    assert estimate_review_tokens(review) == (len(_SYSTEM) + len(_user_prompt(review))) // 4
    assert AI_REVIEW_DAILY_TOKEN_BUDGET == 95_000


def test_exhausted_token_budget_blocks_while_call_counters_allow() -> None:
    token = _Token()
    blocked = qualifies_for_ai_review(
        token,
        reviews_this_hour=0,
        reviews_today=0,
        tokens_today=94_900,
        estimated_tokens=101,
    )
    allowed = qualifies_for_ai_review(
        token,
        reviews_this_hour=0,
        reviews_today=0,
        tokens_today=94_900,
        estimated_tokens=100,
    )
    calls_still_bind = qualifies_for_ai_review(
        token,
        reviews_this_hour=0,
        reviews_today=150,
        tokens_today=0,
        estimated_tokens=10,
    )
    assert blocked is False
    assert allowed is True
    assert calls_still_bind is False
