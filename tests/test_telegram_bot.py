"""Command handlers with a mocked Update. The database is Postgres."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.telegram_bot import (
    admin_ids,
    cmd_risk,
    cmd_settings,
    cmd_signals,
    cmd_start,
    cmd_status,
    cmd_unwatch,
    cmd_wallets,
    cmd_watch,
    on_callback,
)
from app.db.models import AlertDelivery, Token, TrackedWallet, User
from app.services.subscriber_settings import parse_settings
from app.templates import (
    BAIT_WARNING,
    DISCLAIMER,
    alert_text,
    help_text,
    user_welcome_text,
    welcome_text,
)
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


def _context(sessions: object, admins: frozenset[int] | None = None) -> MagicMock:
    context = MagicMock()
    context.args = []
    context.bot_data = {
        "sessions": sessions,
        "clock": lambda: T0,
        "admins": frozenset({TELEGRAM_ID}) if admins is None else admins,
    }
    return context


def _callback_data(call: object) -> set[str]:
    markup = call.kwargs["reply_markup"]  # type: ignore[attr-defined]
    return {button.callback_data for row in markup.inline_keyboard for button in row}


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
    assert rows[0].role == "admin"
    assert rows[0].tier == "free"
    prefs = parse_settings(rows[0].settings)
    assert prefs.min_score == Decimal("50")
    assert prefs.max_risk == Decimal("100")
    assert prefs.active is True
    texts = [call.args[0] for call in update.message.reply_text.await_args_list]
    assert texts[0] == welcome_text()
    assert "Parse" in texts[0]
    assert DISCLAIMER not in texts[0]
    assert "m:wallets" in _callback_data(update.message.reply_text.await_args_list[-1])


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
    assert "− 5" in labels
    assert "+ 5" in labels
    assert "50" in labels
    assert "100" in labels
    assert "🔒 Strict" in labels

    chosen = MagicMock()
    chosen.effective_user.id = TELEGRAM_ID
    chosen.callback_query.data = "set:min:+"
    chosen.callback_query.answer = AsyncMock()
    chosen.callback_query.edit_message_text = AsyncMock()
    await on_callback(chosen, context)

    async with sessions() as session:  # type: ignore[operator]
        row = await session.scalar(select(User).where(User.telegram_id == TELEGRAM_ID))
    assert row is not None
    assert parse_settings(row.settings).min_score == Decimal("55")
    edited = chosen.callback_query.edit_message_text.await_args.args[0]
    assert "Score floor</b>  55" in edited


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
    assert "Score floor: 50" in text
    assert "Risk cap: 100" in text
    assert "Last 24 hours: 1" in text


async def test_status_without_start_asks_to_register(sessions: object) -> None:
    update = _command()
    await cmd_status(update, _context(sessions))
    assert update.message.reply_text.await_args.args[0].startswith("Send /start")


def test_admin_ids_parse_a_list_and_reject_junk() -> None:
    assert admin_ids("") == frozenset()
    assert admin_ids(" 424242, 7 ") == frozenset({424242, 7})
    with pytest.raises(ValueError):
        admin_ids("424242, me")


async def grant_admin(sessions: object, telegram_id: int = TELEGRAM_ID) -> None:
    async with sessions() as session:  # type: ignore[operator]
        async with session.begin():
            session.add(
                User(
                    telegram_id=telegram_id,
                    tier="free",
                    role="admin",
                    settings={"min_score": "50", "max_risk": "100", "active": True},
                    created_at=T0,
                )
            )


async def test_stranger_can_register(sessions: object) -> None:
    update = _command(999)
    await cmd_start(update, _context(sessions))
    assert update.message.reply_text.await_args_list[0].args[0] == user_welcome_text()
    async with sessions() as session:  # type: ignore[operator]
        rows = list(await session.scalars(select(User)))
    assert len(rows) == 1
    assert rows[0].role == "user"
    assert rows[0].tier == "free"


class _RecordingWebhooks:
    def __init__(self) -> None:
        self.added: list[tuple[str, str]] = []
        self.removed: list[tuple[str, str]] = []

    async def add_address_to_webhook(self, webhook_id: str, address: str) -> bool:
        self.added.append((webhook_id, address))
        return True

    async def remove_address_from_webhook(self, webhook_id: str, address: str) -> bool:
        self.removed.append((webhook_id, address))
        return True


async def test_watch_lists_and_removes_a_wallet(sessions: object) -> None:
    await grant_admin(sessions)
    context = _context(sessions)
    hooks = _RecordingWebhooks()
    context.bot_data["helius"] = hooks
    context.bot_data["webhook_id"] = "wh_test"
    address = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    add = _command()
    context.args = [address, "desk"]
    await cmd_watch(add, context)
    assert "Helius subscription is active" in add.message.reply_text.await_args.args[0]
    assert hooks.added == [("wh_test", address)]

    listed = _command()
    await cmd_wallets(listed, context)
    assert address in listed.message.reply_text.await_args.args[0]
    assert "desk" in listed.message.reply_text.await_args.args[0]

    remove = _command()
    context.args = [address]
    await cmd_unwatch(remove, context)
    assert address in remove.message.reply_text.await_args.args[0]
    assert hooks.removed == [("wh_test", address)]
    async with sessions() as session:  # type: ignore[operator]
        left = await session.scalar(select(TrackedWallet).where(TrackedWallet.address == address))
    assert left is None


async def test_watch_rejects_a_bad_address(sessions: object) -> None:
    await grant_admin(sessions)
    context = _context(sessions)
    update = _command()
    context.args = ["not-an-address"]
    await cmd_watch(update, context)
    assert "base58" in update.message.reply_text.await_args.args[0]


async def test_signals_and_stored_risk_are_readable(sessions: object) -> None:
    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    async with sessions() as session:  # type: ignore[operator]
        async with session.begin():
            token = make_token(
                mint=mint,
                symbol="BONK",
                risk_score=Decimal("80"),
                risk_flags=["check_lp_lock"],
            )
            session.add(token)
            await session.flush()
            session.add(
                make_signal(
                    token_id=token.id,
                    composite_score=Decimal("61"),
                    risk_score=Decimal("80"),
                    triggered_at=T0,
                )
            )
    context = _context(sessions)
    signals = _command()
    await cmd_signals(signals, context)
    text = signals.message.reply_text.await_args.args[0]
    assert "BONK" in text
    assert "61" in text
    assert signals.message.reply_text.await_args.kwargs["reply_markup"] is not None

    risk = _command()
    context.args = [mint]
    await cmd_risk(risk, context)
    body = risk.message.reply_text.await_args.args[0]
    assert "risk_score: <code>80</code>" in body
    assert "check_lp_lock" in body
    assert "does not mean the token is safe" in body
    async with sessions() as session:  # type: ignore[operator]
        stored = await session.scalar(select(Token).where(Token.mint == mint))
    assert stored is not None

    incomplete_mint = "So11111111111111111111111111111111111111112"
    async with sessions() as session:  # type: ignore[operator]
        async with session.begin():
            session.add(
                make_token(
                    mint=incomplete_mint,
                    symbol="GAP",
                    risk_score=Decimal("75"),
                    risk_flags=["check_lp_lock:unavailable"],
                )
            )
    incomplete = _command()
    context.args = [incomplete_mint]
    await cmd_risk(incomplete, context)
    notice = incomplete.message.reply_text.await_args.args[0]
    assert "data incomplete: check_lp_lock" in notice
    assert "risk_score" not in notice


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
    assert "<b><code>72.5</code></b>" in text
    assert f"https://solscan.io/token/{mint}" in text
    assert f"<code>{mint}</code>" in text
    assert ">Solscan</a>" in text
    assert "risk_score: <code>85</code>" in text
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


def test_alert_card_groups_ready_data_without_rewriting_reasons() -> None:
    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    text = alert_text(
        symbol="BONK <dev>",
        mint=mint,
        composite_score=Decimal("72.5"),
        risk_score=Decimal("60"),
        risk_flags=["check_lp_lock:unavailable", "check_holder_concentration"],
        bait_warning=False,
        creator_over_limit=True,
        context={
            "creator": "Wallet<A>&B",
            "launches_since_tracking": 3,
            "lp_stayed_shown": "2 of 3",
            "price_drop_shown": "unavailable (no pool yet)",
            "median_sell_shown": "unavailable (not in the tracked-wallet ledger)",
            "distribution_status": "unavailable",
            "distribution_reason": "holder_funder_missing",
        },
    )
    assert "🟡 <b>BONK &lt;dev&gt;</b>" in text
    assert f"<code>{mint}</code>" in text
    assert ">Solscan</a>" in text
    assert "https://solscan.io/token/" in text
    assert "<code>72.5</code>" in text
    assert "<code>60</code>" in text
    assert "👤 <b>Creator cluster</b>" in text
    assert "<code>Wallet&lt;A&gt;&amp;B</code>" in text
    assert "⚪ unavailable (no pool yet)" in text
    assert "⚪ unavailable (not in the tracked-wallet ledger)" in text
    assert "⚪ unavailable (holder_funder_missing)" in text
    assert "⚪ check_lp_lock:unavailable" in text
    assert "⚠️ Creator-funder supply is above your limit." in text
    assert "LP stayed locked, since tracking started: <code>2 of 3</code>" in text
    assert "🔒 LP locked" not in text
    assert text.index("risk_score:") < text.index("Creator cluster") < text.index("<b>LP</b>")
    help_body = help_text()
    assert "⚪ <b>unavailable (no pool yet)</b>" in help_body
    assert "⚙️ /settings" in help_body
    assert "📡 /signals" in help_body
    assert "🛡 /risk mint" in help_body
