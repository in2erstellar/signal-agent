"""Command handlers with a mocked Update. The database is Postgres."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.telegram_bot import cmd_settings, cmd_start, cmd_status, on_pref
from app.db.models import AlertDelivery, User
from app.services.subscriber_settings import parse_settings
from app.templates import BAIT_WARNING, DISCLAIMER, alert_text, welcome_text
from tests.factories import make_signal, make_token, make_user
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

T0 = datetime(2026, 9, 24, 18, 0, tzinfo=timezone.utc)
TELEGRAM_ID = 424242


@pytest.fixture
async def sessions(pipeline_infra: object):
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    factory = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    yield factory
    await truncate_pipeline(engine)
    await engine.dispose()


def _context(sessions: object) -> MagicMock:
    context = MagicMock()
    context.bot_data = {"sessions": sessions, "clock": lambda: T0}
    return context


def _command(telegram_id: int = TELEGRAM_ID) -> MagicMock:
    update = MagicMock()
    update.effective_user.id = telegram_id
    update.message.reply_text = AsyncMock()
    update.callback_query = None
    return update


async def test_start_registers_once_and_sends_the_welcome(sessions: object) -> None:
    update = _command()
    context = _context(sessions)
    await cmd_start(update, context)
    await cmd_start(update, context)

    async with sessions() as session:  # type: ignore[operator]
        rows = list(await session.scalars(select(User)))
    assert len(rows) == 1
    assert rows[0].telegram_id == TELEGRAM_ID
    prefs = parse_settings(rows[0].settings)
    assert prefs.min_score == Decimal("50")
    assert prefs.max_risk == Decimal("100")
    assert prefs.active is True
    text = update.message.reply_text.await_args_list[-1].args[0]
    assert text == welcome_text()
    assert "SignalForge" in text
    assert DISCLAIMER in text


async def test_start_reactivates_a_blocked_user(sessions: object) -> None:
    async with sessions() as session:  # type: ignore[operator]
        async with session.begin():
            session.add(
                make_user(
                    telegram_id=TELEGRAM_ID,
                    settings={"min_score": "70", "max_risk": "40", "active": False},
                )
            )
    await cmd_start(_command(), _context(sessions))
    async with sessions() as session:  # type: ignore[operator]
        row = await session.scalar(select(User).where(User.telegram_id == TELEGRAM_ID))
    assert row is not None
    prefs = parse_settings(row.settings)
    assert prefs.active is True
    assert prefs.min_score == Decimal("70")
    assert prefs.max_risk == Decimal("40")


async def test_settings_buttons_persist_min_score(sessions: object) -> None:
    context = _context(sessions)
    await cmd_start(_command(), context)
    opened = _command()
    await cmd_settings(opened, context)
    markup = opened.message.reply_text.await_args.kwargs["reply_markup"]
    labels = [button.text for row in markup.inline_keyboard for button in row]
    assert any(label.startswith("● min 50") for label in labels)
    assert any("risk 100" in label for label in labels)

    chosen = MagicMock()
    chosen.effective_user.id = TELEGRAM_ID
    chosen.callback_query.data = "pref:min_score:70"
    chosen.callback_query.answer = AsyncMock()
    chosen.callback_query.edit_message_text = AsyncMock()
    await on_pref(chosen, context)

    async with sessions() as session:  # type: ignore[operator]
        row = await session.scalar(select(User).where(User.telegram_id == TELEGRAM_ID))
    assert row is not None
    assert parse_settings(row.settings).min_score == Decimal("70")
    edited = chosen.callback_query.edit_message_text.await_args.args[0]
    assert "min_score: 70" in edited


async def test_status_counts_deliveries_in_the_last_day(sessions: object) -> None:
    context = _context(sessions)
    await cmd_start(_command(), context)
    async with sessions() as session:  # type: ignore[operator]
        async with session.begin():
            user = await session.scalar(select(User).where(User.telegram_id == TELEGRAM_ID))
            assert user is not None
            token = make_token()
            session.add(token)
            await session.flush()
            signal = make_signal(token_id=token.id, triggered_at=T0)
            session.add(signal)
            await session.flush()
            session.add(
                AlertDelivery(user_id=user.id, signal_id=signal.id, sent_at=T0 - timedelta(hours=1))
            )
        async with session.begin():
            older = make_token()
            session.add(older)
            await session.flush()
            old_signal = make_signal(token_id=older.id, triggered_at=T0 - timedelta(days=3))
            session.add(old_signal)
            await session.flush()
            session.add(
                AlertDelivery(
                    user_id=user.id,
                    signal_id=old_signal.id,
                    sent_at=T0 - timedelta(hours=25),
                )
            )

    status = _command()
    await cmd_status(status, context)
    text = status.message.reply_text.await_args.args[0]
    assert "min_score: 50" in text
    assert "max_risk: 100" in text
    assert "Алертов за 24 часа: 1" in text


async def test_status_without_start_asks_to_register(sessions: object) -> None:
    update = _command()
    await cmd_status(update, _context(sessions))
    assert update.message.reply_text.await_args.args[0].startswith("Сначала отправьте /start")


async def test_alert_text_shows_the_score_the_mint_link_and_the_bait_warning() -> None:
    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    text = alert_text(
        symbol="BONK",
        mint=mint,
        composite_score=Decimal("72.5"),
        risk_score=Decimal("85"),
        risk_flags=["check_funder_cluster_exposure"],
        bait_warning=True,
    )
    assert "<b>BONK</b>" in text
    assert "<b>72.5</b>" in text
    assert f"https://solscan.io/token/{mint}" in text
    assert "EPjF…Dt1v" in text
    assert "risk_score: 85" in text
    assert "check_funder_cluster_exposure" in text
    assert f"<b>{BAIT_WARNING}</b>" in text
    assert text.endswith(DISCLAIMER)
    assert "купить" not in text.lower()
    quiet = alert_text(
        symbol="BONK",
        mint=mint,
        composite_score=Decimal("72.5"),
        risk_score=Decimal("85"),
        risk_flags=["check_funder_cluster_exposure"],
        bait_warning=False,
    )
    assert BAIT_WARNING not in quiet
