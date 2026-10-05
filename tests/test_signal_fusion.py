"""Composite score formula, including the exact-threshold boundary."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from structlog.testing import capture_logs

from app.config import SIGNAL_THRESHOLD
from app.db.models import OutcomeCheck, Signal, Token
from app.services.creator_cluster import MEDIAN_SELL_REASON
from app.workers.signal_fusion import (
    SignalFusion,
    composite_score,
    composite_without_wallet,
    creator_cluster_degraded_reason,
    emits_signal,
    wallet_score_from_win_rates,
)
from tests.factories import make_token, make_tracked_wallet, make_wallet_tx
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

NOW = datetime(2026, 9, 24, 16, 0, tzinfo=timezone.utc)


def test_composite_formula_and_exact_threshold() -> None:
    assert composite_score(Decimal("100"), Decimal("0"), Decimal("0")) == SIGNAL_THRESHOLD
    assert emits_signal(SIGNAL_THRESHOLD) is False

    above = composite_score(Decimal("100"), Decimal("1"), Decimal("0"))
    assert above == Decimal("50.3")
    assert emits_signal(above) is True

    # 100*0.5 + 100*0.3 - 150*0.2 == 50 exactly, so the penalty does not emit.
    on_threshold = composite_score(Decimal("100"), Decimal("100"), Decimal("150"))
    assert on_threshold == SIGNAL_THRESHOLD
    assert emits_signal(on_threshold) is False

    just_over = composite_score(Decimal("100"), Decimal("100"), Decimal("149"))
    assert just_over == Decimal("50.2")
    assert emits_signal(just_over) is True

    assert wallet_score_from_win_rates([Decimal("0.01"), None]) == Decimal("1")
    assert wallet_score_from_win_rates([None]) == Decimal(0)
    # A null win rate is unknown, so the wallet term is dropped and rescaled.
    unknown = composite_without_wallet(Decimal("100"), Decimal("0"))
    assert unknown == Decimal("71.43")
    assert emits_signal(unknown) is True


async def test_fusion_skips_the_exact_threshold_and_writes_just_above(
    pipeline_infra: object,
) -> None:
    infra = pipeline_infra
    engine = create_async_engine(infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        async with sessions() as session:
            async with session.begin():
                flat = make_token(mint="mint-flat", risk_score=Decimal("100"), updated_at=NOW)
                hot = make_token(
                    mint="mint-hot",
                    risk_score=Decimal("100"),
                    creator_address="creator-known",
                    updated_at=NOW,
                )
                quiet = make_tracked_wallet(address="wallet-quiet", win_rate=Decimal("0"))
                strong = make_tracked_wallet(address="wallet-strong", win_rate=Decimal("0.01"))
                session.add_all([flat, hot, quiet, strong])
                await session.flush()
                session.add_all(
                    [
                        make_wallet_tx(
                            wallet_id=quiet.id,
                            token_id=flat.id,
                            action="buy",
                            amount_sol=Decimal("1"),
                            block_time=NOW,
                            tx_signature="buy-flat",
                        ),
                        make_wallet_tx(
                            wallet_id=strong.id,
                            token_id=hot.id,
                            action="buy",
                            amount_sol=Decimal("1"),
                            block_time=NOW,
                            tx_signature="buy-hot",
                        ),
                    ]
                )

        fusion = SignalFusion(sessions, clock=lambda: NOW)
        processed, failed, created = await fusion.run_once()
        assert (processed, failed, created) == (2, 0, 1)

        async with sessions() as session:
            rows = list(await session.scalars(select(Signal)))
            hot_token = await session.scalar(select(Token).where(Token.mint == "mint-hot"))
            assert len(rows) == 1
            assert hot_token is not None
            assert rows[0].token_id == hot_token.id
            assert rows[0].composite_score == Decimal("50.3")
            assert rows[0].wallet_score == Decimal("1")
            assert await session.scalar(select(func.count()).select_from(Signal)) == 1

        fresh = make_token(
            mint="mint-new",
            risk_score=Decimal("100"),
            creator_address="creator-known",
            updated_at=NOW,
        )
        blank = make_tracked_wallet(address="wallet-blank", win_rate=None)
        async with sessions() as session:
            async with session.begin():
                session.add_all([fresh, blank])
                await session.flush()
                session.add(
                    make_wallet_tx(
                        wallet_id=blank.id,
                        token_id=fresh.id,
                        action="buy",
                        amount_sol=Decimal("1"),
                        block_time=NOW,
                        tx_signature="buy-new",
                    )
                )
        processed_new, failed_new, created_new = await fusion.run_once()
        assert (processed_new, failed_new, created_new) == (2, 0, 1)
        async with sessions() as session:
            new_token = await session.scalar(select(Token).where(Token.mint == "mint-new"))
            assert new_token is not None
            created_row = await session.scalar(
                select(Signal).where(Signal.token_id == new_token.id)
            )
            assert created_row is not None
            assert created_row.wallet_score is None
            assert created_row.composite_score == Decimal("71.43")

        incomplete = make_token(
            mint="mint-incomplete",
            risk_score=Decimal("75"),
            risk_flags=["check_lp_lock:unavailable"],
            updated_at=NOW,
        )
        buyer = make_tracked_wallet(address="wallet-incomplete", win_rate=Decimal("1"))
        async with sessions() as session:
            async with session.begin():
                session.add_all([incomplete, buyer])
                await session.flush()
                session.add(
                    make_wallet_tx(
                        wallet_id=buyer.id,
                        token_id=incomplete.id,
                        action="buy",
                        amount_sol=Decimal("1"),
                        block_time=NOW,
                        tx_signature="buy-incomplete",
                    )
                )
        held, failed_held, created_held = await fusion.run_once()
        assert created_held == 0
        assert failed_held == 0
        assert held >= 1
        async with sessions() as session:
            blocked = await session.scalar(select(Token).where(Token.mint == "mint-incomplete"))
            assert blocked is not None
            assert (
                await session.scalar(select(Signal).where(Signal.token_id == blocked.id))
            ) is None

        again, failed_again, created_again = await fusion.run_once()
        # The below-threshold mint and the incomplete scan stay inside the
        # window with no signal, so both are considered again.
        assert (again, failed_again, created_again) == (2, 0, 0)
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(Signal)) == 2
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


def test_degraded_gate_ignores_history_limits_and_unlisted_distribution_reasons() -> None:
    for reason in (
        "holder_funder_missing",
        "creator_funder_missing",
        "creator_missing",
        "holder_book_missing",
    ):
        assert (
            creator_cluster_degraded_reason(
                {"distribution_status": "unavailable", "distribution_reason": reason}
            )
            == reason
        )
    for reason in ("distribution_not_run", "no_holder_balances", "no_earlier_launches"):
        assert (
            creator_cluster_degraded_reason(
                {"distribution_status": "unavailable", "distribution_reason": reason}
            )
            is None
        )
    measured = {
        "distribution_status": "measured",
        "distribution_reason": None,
        "price_drop_reason": "price_lookup_failed",
        "lp_stayed_reason": "no_follow_up_yet",
        "median_sell_reason": MEDIAN_SELL_REASON,
    }
    assert creator_cluster_degraded_reason(measured) is None


async def _fuse_one(
    pipeline_infra: object, token: Token
) -> tuple[list[dict[str, object]], Signal | None]:
    infra = pipeline_infra
    engine = create_async_engine(infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        async with sessions() as session:
            async with session.begin():
                session.add(token)
        fusion = SignalFusion(sessions, clock=lambda: NOW)
        with capture_logs() as logs:
            await fusion.run_once()
        async with sessions() as session:
            stored = await session.scalar(select(Token).where(Token.mint == token.mint))
            assert stored is not None
            row = await session.scalar(select(Signal).where(Signal.token_id == stored.id))
        return logs, row
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


def _passing_token(mint: str, **overrides: object) -> Token:
    values: dict[str, object] = {
        "mint": mint,
        "risk_score": Decimal("100"),
        "risk_flags": ["check_lp_lock"],
        "creator_address": "creator-known",
        "dev_distribution_status": "measured",
        "dev_distribution_share": Decimal("0.1"),
        "dev_distribution_reason": None,
        "updated_at": NOW,
    }
    values.update(overrides)
    return make_token(**values)


@pytest.mark.parametrize(
    "reason",
    ["creator_funder_missing", "creator_missing", "holder_book_missing", "holder_funder_missing"],
)
async def test_degraded_distribution_does_not_create_a_signal(
    pipeline_infra: object, reason: str
) -> None:
    creator = None if reason == "creator_missing" else "creator-known"
    logs, row = await _fuse_one(
        pipeline_infra,
        _passing_token(
            f"mint-{reason}",
            creator_address=creator,
            dev_distribution_status="unavailable",
            dev_distribution_share=None,
            dev_distribution_reason=reason,
        ),
    )
    assert row is None
    blocked = [event for event in logs if event.get("event") == "creator_cluster_data_quality"]
    assert len(blocked) == 1
    assert blocked[0]["flag"] == "creator_cluster_data_quality:degraded"
    assert blocked[0]["reason"] == f"degraded: {reason}"
    assert not any(event.get("event") == "signal_data_incomplete" for event in logs)


async def test_no_earlier_launches_still_creates_a_signal(pipeline_infra: object) -> None:
    logs, row = await _fuse_one(pipeline_infra, _passing_token("mint-young"))
    assert row is not None
    assert row.context["price_drop_reason"] == "no_earlier_launches"
    assert row.context["lp_stayed_reason"] == "no_earlier_launches"
    assert row.context["median_sell_reason"] == MEDIAN_SELL_REASON
    assert row.context["distribution_status"] == "measured"
    assert not any(event.get("event") == "creator_cluster_data_quality" for event in logs)


async def test_price_lookup_failed_on_earlier_launches_still_creates_a_signal(
    pipeline_infra: object,
) -> None:
    infra = pipeline_infra
    engine = create_async_engine(infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        earlier = make_token(
            mint="mint-earlier",
            creator_address="creator-known",
            risk_score=None,
            updated_at=NOW - timedelta(days=2),
        )
        current = _passing_token("mint-lookup-failed")
        async with sessions() as session:
            async with session.begin():
                session.add_all([earlier, current])
                await session.flush()
                session.add_all(
                    [
                        OutcomeCheck(
                            token_id=earlier.id,
                            horizon="listing",
                            status="unavailable",
                            price_reason="price_lookup_failed",
                            scheduled_at=NOW - timedelta(days=2),
                        ),
                        OutcomeCheck(
                            token_id=earlier.id,
                            horizon="1h",
                            status="unavailable",
                            price_reason="price_lookup_failed",
                            scheduled_at=NOW - timedelta(days=2, hours=-1),
                        ),
                    ]
                )
        fusion = SignalFusion(sessions, clock=lambda: NOW)
        with capture_logs() as logs:
            _processed, failed, created = await fusion.run_once()
        assert (failed, created) == (0, 1)
        async with sessions() as session:
            stored = await session.scalar(select(Token).where(Token.mint == "mint-lookup-failed"))
            assert stored is not None
            row = await session.scalar(select(Signal).where(Signal.token_id == stored.id))
        assert row is not None
        assert row.context["price_drop_reason"] == "price_lookup_failed"
        assert row.context["median_sell_reason"] == MEDIAN_SELL_REASON
        assert not any(event.get("event") == "creator_cluster_data_quality" for event in logs)
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()
