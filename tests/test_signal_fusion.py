"""Composite score formula, including the exact-threshold boundary."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import SIGNAL_THRESHOLD
from app.db.models import Signal, Token
from app.workers.signal_fusion import (
    SignalFusion,
    composite_score,
    composite_without_wallet,
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


async def test_fusion_skips_the_exact_threshold_and_writes_just_above(pipeline_infra: object) -> None:
    infra = pipeline_infra
    engine = create_async_engine(infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        async with sessions() as session:
            async with session.begin():
                flat = make_token(mint="mint-flat", risk_score=Decimal("100"), updated_at=NOW)
                hot = make_token(mint="mint-hot", risk_score=Decimal("100"), updated_at=NOW)
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

        fresh = make_token(mint="mint-new", risk_score=Decimal("100"), updated_at=NOW)
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

        again, failed_again, created_again = await fusion.run_once()
        # The below-threshold mint is still inside the scan window and has no
        # signal, so it is considered again. The mint that already has a
        # signal is not.
        assert (again, failed_again, created_again) == (1, 0, 0)
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(Signal)) == 2
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()
