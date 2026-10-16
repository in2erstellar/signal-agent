"""Inline keyboards: one callback router, wallet confirmation, signal pages."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy import select

from app.adapters.telegram_bot import (
    cmd_signals,
    cmd_wallets,
    on_callback,
    on_wallet_text,
    parse_callback,
)
from app.db.models import TrackedWallet, User
from app.services.subscriber_settings import parse_settings, with_updates
from app.templates import BAIT_WARNING, DISCLAIMER, stale_menu_text
from tests.factories import make_bait_flag, make_signal, make_token, make_tracked_wallet
from tests.test_telegram_bot import TELEGRAM_ID, _command, _context, grant_admin

pytest_plugins = ["tests.pipeline_containers", "tests.test_telegram_bot"]

ADDRESS = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


class _MemoryRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        del ex
        self.values[key] = value

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)


class _Hooks:
    def __init__(self) -> None:
        self.added: list[tuple[str, str]] = []
        self.removed: list[tuple[str, str]] = []

    async def add_address_to_webhook(self, webhook_id: str, address: str) -> bool:
        self.added.append((webhook_id, address))
        return True

    async def remove_address_from_webhook(self, webhook_id: str, address: str) -> bool:
        self.removed.append((webhook_id, address))
        return True


def _press(data: str, telegram_id: int = TELEGRAM_ID) -> MagicMock:
    update = MagicMock()
    update.effective_user.id = telegram_id
    update.message = None
    update.callback_query = MagicMock()
    update.callback_query.data = data
    update.callback_query.answer = AsyncMock()
    update.callback_query.edit_message_text = AsyncMock()
    return update


def test_parse_callback_rejects_an_unknown_prefix() -> None:
    assert parse_callback("set:min:+") == ("set", ("min", "+"))
    assert parse_callback("nope") is None
    assert parse_callback("") is None


async def test_unknown_callback_is_answered_and_does_not_raise(sessions: object) -> None:
    pressed = _press("pref:min_score:70")
    await on_callback(pressed, _context(sessions))
    pressed.callback_query.answer.assert_awaited()
    assert pressed.callback_query.edit_message_text.await_args.args[0] == stale_menu_text()

    broken = _press("w:del:not-a-uuid")
    await on_callback(broken, _context(sessions))
    assert broken.callback_query.edit_message_text.await_args.args[0] == stale_menu_text()


async def test_settings_step_does_not_cross_zero_or_one_hundred(sessions: object) -> None:
    context = _context(sessions)
    await grant_admin(sessions)
    async with sessions() as session:  # type: ignore[operator]
        async with session.begin():
            row = await session.scalar(select(User).where(User.telegram_id == TELEGRAM_ID))
            assert row is not None
            row.settings = with_updates(
                row.settings, min_score=Decimal("0"), max_risk=Decimal("100")
            )

    down = _press("set:min:-")
    await on_callback(down, context)
    down.callback_query.answer.assert_awaited()
    down.callback_query.edit_message_text.assert_not_awaited()

    up = _press("set:max:+")
    await on_callback(up, context)
    up.callback_query.edit_message_text.assert_not_awaited()

    async with sessions() as session:  # type: ignore[operator]
        row = await session.scalar(select(User).where(User.telegram_id == TELEGRAM_ID))
    assert row is not None
    prefs = parse_settings(row.settings)
    assert prefs.min_score == Decimal("0")
    assert prefs.max_risk == Decimal("100")


async def test_settings_preset_and_alert_switch(sessions: object) -> None:
    context = _context(sessions)
    await grant_admin(sessions)
    preset = _press("set:go:strict")
    await on_callback(preset, context)
    paused = _press("set:live:off")
    await on_callback(paused, context)
    async with sessions() as session:  # type: ignore[operator]
        row = await session.scalar(select(User).where(User.telegram_id == TELEGRAM_ID))
    assert row is not None
    prefs = parse_settings(row.settings)
    assert prefs.min_score == Decimal("70")
    assert prefs.max_risk == Decimal("25")
    assert prefs.active is False
    body = paused.callback_query.edit_message_text.await_args.args[0]
    assert "paused" in body


async def test_wallet_delete_asks_before_it_removes_the_row(sessions: object) -> None:
    await grant_admin(sessions)
    async with sessions() as session:  # type: ignore[operator]
        async with session.begin():
            wallet = make_tracked_wallet(address=ADDRESS, label="whale1")
            session.add(wallet)
            await session.flush()
            wallet_id = wallet.id
    context = _context(sessions)
    hooks = _Hooks()
    context.bot_data["helius"] = hooks
    context.bot_data["webhook_id"] = "wh_test"

    listed = _command()
    await cmd_wallets(listed, context)
    callbacks = {
        button.callback_data
        for row in listed.message.reply_text.await_args.kwargs["reply_markup"].inline_keyboard
        for button in row
    }
    assert f"w:del:{wallet_id}" in callbacks
    assert "w:add" in callbacks

    confirm = _press(f"w:del:{wallet_id}")
    await on_callback(confirm, context)
    body = confirm.callback_query.edit_message_text.await_args.args[0]
    assert "Delete monitoring EPjF...Dt1v (label: whale1)?" in body
    labels = [
        button.text
        for row in confirm.callback_query.edit_message_text.await_args.kwargs[
            "reply_markup"
        ].inline_keyboard
        for button in row
    ]
    assert any(label.endswith("Yes, delete") for label in labels)
    async with sessions() as session:  # type: ignore[operator]
        still = await session.get(TrackedWallet, wallet_id)
    assert still is not None

    done = _press(f"w:yes:{wallet_id}")
    await on_callback(done, context)
    assert hooks.removed == [("wh_test", ADDRESS)]
    async with sessions() as session:  # type: ignore[operator]
        gone = await session.get(TrackedWallet, wallet_id)
    assert gone is None


async def test_add_wallet_button_reads_the_next_message(sessions: object) -> None:
    await grant_admin(sessions)
    context = _context(sessions)
    redis = _MemoryRedis()
    context.bot_data["redis"] = redis
    context.bot_data["helius"] = _Hooks()
    context.bot_data["webhook_id"] = "wh_test"

    asked = _press("w:add")
    await on_callback(asked, context)
    assert "address label" in asked.callback_query.edit_message_text.await_args.args[0]
    assert redis.values

    typed = _command()
    typed.message.text = f"{ADDRESS} desk"
    await on_wallet_text(typed, context)
    assert "Helius subscription is active" in typed.message.reply_text.await_args.args[0]
    assert redis.values == {}
    async with sessions() as session:  # type: ignore[operator]
        row = await session.scalar(select(TrackedWallet).where(TrackedWallet.address == ADDRESS))
    assert row is not None
    assert row.label == "desk"


async def test_signals_page_and_open_the_bait_card(sessions: object) -> None:
    from tests.test_telegram_bot import T0

    bait_id = None
    async with sessions() as session:  # type: ignore[operator]
        async with session.begin():
            wallet = make_tracked_wallet()
            session.add(wallet)
            await session.flush()
            for index in range(6):
                token = make_token(
                    symbol="BAIT" if index == 0 else f"T{index}",
                    mint=f"{index}xKXmint{index:02d}gAsU{index}111111111111111111111",
                    risk_score=Decimal("20"),
                    risk_flags=["check_lp_lock"],
                )
                session.add(token)
                await session.flush()
                signal = make_signal(
                    token_id=token.id,
                    composite_score=Decimal("72.5"),
                    risk_score=Decimal("20"),
                    triggered_at=T0 - timedelta(minutes=index),
                )
                session.add(signal)
                await session.flush()
                if index == 0:
                    session.add(make_bait_flag(wallet_id=wallet.id, token_id=token.id))
                    bait_id = signal.id

    context = _context(sessions)
    opened = _command()
    await cmd_signals(opened, context)
    text = opened.message.reply_text.await_args.args[0]
    assert "Page 1 of 2" in text
    markup = opened.message.reply_text.await_args.kwargs["reply_markup"]
    open_buttons = [
        button
        for row in markup.inline_keyboard
        for button in row
        if str(button.callback_data).startswith("s:open:")
    ]
    assert len(open_buttons) == 5
    nav = [button.callback_data for button in markup.inline_keyboard[-2]]
    assert nav[0] == "s:hold"
    assert nav[2] == "s:page:1"

    page = _press("s:page:1")
    await on_callback(page, context)
    assert "Page 2 of 2" in page.callback_query.edit_message_text.await_args.args[0]

    card = _press(f"s:open:{bait_id}:0")
    await on_callback(card, context)
    body = card.callback_query.edit_message_text.await_args.args[0]
    assert "🔴" in body
    assert "<b>BAIT</b>" in body
    assert "composite_score" not in body
    assert f"<b>{BAIT_WARNING}</b>" in body
    assert body.endswith(DISCLAIMER)
    assert "https://solscan.io/token/" in body
