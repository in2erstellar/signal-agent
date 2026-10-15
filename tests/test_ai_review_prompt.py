"""The review prompt states that the stored score counts passed checks."""

from __future__ import annotations

from app.adapters.ai_review import TokenReviewInput, _user_prompt


def _input(**overrides: object) -> TokenReviewInput:
    values: dict[str, object] = {
        "mint": "Mint1111111111111111111111111111111111111",
        "symbol": "VRAXPHIL",
        "name": "VRAX PHIL",
        "risk_score": "85",
        "risk_flags": ("check_funder_cluster_exposure",),
        "liquidity_usd": None,
        "composite_score": "60.71",
    }
    values.update(overrides)
    return TokenReviewInput(**values)  # type: ignore[arg-type]


def test_prompt_names_the_score_by_what_it_counts() -> None:
    prompt = _user_prompt(_input())
    assert "checks_passed_score: 85 out of 100" in prompt
    assert "Higher is better" in prompt
    assert "risk_score" not in prompt


def test_prompt_lists_failed_checks_by_name() -> None:
    prompt = _user_prompt(_input())
    assert "failed_checks: check_funder_cluster_exposure" in prompt
    assert "failed_checks: none" in _user_prompt(_input(risk_flags=()))
