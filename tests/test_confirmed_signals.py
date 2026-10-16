"""Confirmed signals: a token whose price held 1h after listing, and its track record."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import CONFIRM_MAX_DELAY_MINUTES, SIGNAL_KIND_CONFIRMED, SIGNAL_KIND_EARLY
from app.db.models import OutcomeCheck, Signal, Token
from app.services.quality_checks import ExternalReadings
from app.services.track_record import load_track_record
from app.templates import alert_text, channel_signal_text, track_record_text
from app.workers.signal_fusion import SignalFusion
from tests.pipeline_containers import truncate_pipeline
from tests.test_quality_checks import MINT, NOW, FakeReader, _checks, _dex, _readings, _token

pytest_plugins = ["tests.pipeline_containers"]

LISTING = Decimal("0.0001")


async def _confirm(
    pipeline_infra: object,
    reader: FakeReader,
    *,
    hour_price: Decimal = Decimal("0.00012"),
    hour_age_minutes: int = 5,
    early_signal: bool = False,
) -> list[Signal]:
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        token = _token("mint-held", updated_at=NOW - timedelta(hours=1))
        async with sessions() as session:
            async with session.begin():
                session.add(token)
                await session.flush()
                session.add_all(_outcomes(token, hour_price, hour_age_minutes))
                if early_signal:
                    session.add(
                        Signal(
                            token_id=token.id,
                            composite_score=Decimal("75"),
                            triggered_at=NOW - timedelta(hours=1),
                            context={"kind": SIGNAL_KIND_EARLY},
                        )
                    )
        fusion = SignalFusion(sessions, clock=lambda: NOW, quality=reader)  # type: ignore[arg-type]
        await fusion.confirm_once()
        async with sessions() as session:
            return list(await session.scalars(select(Signal)))
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


def _outcomes(token: Token, hour_price: Decimal, hour_age_minutes: int) -> list[OutcomeCheck]:
    listed = NOW - timedelta(minutes=60 + hour_age_minutes)
    return [
        OutcomeCheck(
            token_id=token.id,
            horizon="listing",
            status="observed",
            price_usd=LISTING,
            scheduled_at=listed,
            observed_at=listed,
        ),
        OutcomeCheck(
            token_id=token.id,
            horizon="1h",
            status="observed",
            price_usd=hour_price,
            scheduled_at=listed + timedelta(hours=1),
            observed_at=NOW - timedelta(minutes=hour_age_minutes),
        ),
    ]


async def test_a_price_that_held_an_hour_is_confirmed(pipeline_infra: object) -> None:
    # Creator share has no data and launch trades fail: the 9-of-12 rule is not used.
    reader = FakeReader(_readings(dex=_dex(price_usd=Decimal("0.00011")), launch_trades=50))
    signals = await _confirm(pipeline_infra, reader)
    assert reader.calls == 1
    assert len(signals) == 1
    context = signals[0].context
    assert context is not None
    assert context["kind"] == SIGNAL_KIND_CONFIRMED
    assert context["confirmation"] == {
        "listing_price_usd": "0.0001",
        "hour_price_usd": "0.00012",
        "price_usd": "0.00011",
    }
    assert len(context["quality_checks"]) == 12


async def test_a_price_below_listing_after_an_hour_is_not_read(pipeline_infra: object) -> None:
    reader = FakeReader(_readings())
    assert await _confirm(pipeline_infra, reader, hour_price=Decimal("0.000099")) == []
    assert reader.calls == 0


async def test_a_price_that_fell_since_the_hour_reading_is_not_confirmed(
    pipeline_infra: object,
) -> None:
    reader = FakeReader(_readings(dex=_dex(price_usd=Decimal("0.00009"))))
    assert await _confirm(pipeline_infra, reader) == []
    assert reader.calls == 1


async def test_thin_liquidity_blocks_a_confirmation(pipeline_infra: object) -> None:
    reader = FakeReader(_readings(dex=_dex(liquidity_usd=Decimal("5000"))))
    assert await _confirm(pipeline_infra, reader) == []


async def test_no_confirmation_without_a_current_price(pipeline_infra: object) -> None:
    reader = FakeReader(ExternalReadings())
    assert await _confirm(pipeline_infra, reader) == []


async def test_an_old_hour_reading_or_an_early_signal_skips_the_token(
    pipeline_infra: object,
) -> None:
    reader = FakeReader(_readings())
    late = await _confirm(pipeline_infra, reader, hour_age_minutes=CONFIRM_MAX_DELAY_MINUTES + 1)
    assert late == []
    signals = await _confirm(pipeline_infra, reader, early_signal=True)
    assert [s.context["kind"] for s in signals] == [SIGNAL_KIND_EARLY]  # type: ignore[index]
    assert reader.calls == 0


def _confirmed_context() -> dict[str, object]:
    checks = _checks()
    return {
        "kind": SIGNAL_KIND_CONFIRMED,
        "quality_checks": [check.as_dict() for check in checks],
        "quality_passed": 12,
        "quality_total": 12,
        "confirmation": {
            "listing_price_usd": "0.0001",
            "hour_price_usd": "0.00012",
            "price_usd": "0.00015",
        },
    }


def test_confirmed_card_and_post_carry_the_badge_and_their_own_rule() -> None:
    context = _confirmed_context()
    card = alert_text(
        symbol="MOO",
        mint=MINT,
        composite_score=Decimal("100"),
        risk_score=Decimal("100"),
        risk_flags=[],
        bait_warning=False,
        context=context,
    )
    assert "<b>CONFIRMED</b> · held 1h after listing (+50% since listing)" in card
    assert "Confirmed signal: the price 1h after listing" in card
    assert "Early signal:" not in card
    rows = context["quality_checks"]
    assert isinstance(rows, list)
    post = channel_signal_text(
        symbol="MOO",
        mint=MINT,
        checks=rows,
        triggered_at=NOW,
        market_cap=None,
        context=context,
    )
    assert "<b>CONFIRMED</b>" in post
    assert "Confirmed: 12:00 UTC" in post
    early = channel_signal_text(
        symbol="MOO", mint=MINT, checks=rows, triggered_at=NOW, market_cap=None
    )
    assert "CONFIRMED" not in early
    assert "Signal: 12:00 UTC" in early


async def test_track_record_measures_confirmed_signals_from_the_confirmation_price(
    pipeline_infra: object,
) -> None:
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        early_token = _token("mint-early")
        confirmed_token = _token("mint-confirmed")
        async with sessions() as session:
            async with session.begin():
                session.add_all([early_token, confirmed_token])
                await session.flush()
                session.add_all(
                    [
                        Signal(
                            token_id=early_token.id,
                            composite_score=Decimal("75"),
                            triggered_at=NOW,
                            context={"kind": SIGNAL_KIND_EARLY},
                        ),
                        Signal(
                            token_id=confirmed_token.id,
                            composite_score=Decimal("75"),
                            triggered_at=NOW,
                            context=_confirmed_context(),
                        ),
                        *_day_outcomes(early_token, Decimal("0.0002")),
                        *_day_outcomes(confirmed_token, Decimal("0.0003")),
                    ]
                )
        async with sessions() as session:
            early = await load_track_record(session)
            confirmed = await load_track_record(session, SIGNAL_KIND_CONFIRMED)
        assert (early.signals, confirmed.signals) == (1, 1)
        early_day = next(h for h in early.horizons if h.horizon == "24h")
        assert early_day.median_change == Decimal("1")  # 0.0002 / 0.0001 listing
        assert [h.horizon for h in confirmed.horizons] == ["24h", "7d"]
        assert confirmed.horizons[0].median_change == Decimal("1")  # 0.0003 / 0.00015
        text = track_record_text(early, confirmed)
        assert "<b>Early signals</b>" in text
        assert "Confirmed signals</b>" in text
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


def _day_outcomes(token: Token, day_price: Decimal) -> list[OutcomeCheck]:
    return [
        OutcomeCheck(
            token_id=token.id,
            horizon="listing",
            status="observed",
            price_usd=LISTING,
            scheduled_at=NOW,
            observed_at=NOW,
        ),
        OutcomeCheck(
            token_id=token.id,
            horizon="24h",
            status="observed",
            price_usd=day_price,
            scheduled_at=NOW + timedelta(days=1),
            observed_at=NOW + timedelta(days=1),
        ),
    ]
