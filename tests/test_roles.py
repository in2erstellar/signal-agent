"""Open registration and the admin gate on wallet commands."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.adapters.telegram_bot import cmd_menu, cmd_start, cmd_wallets, cmd_watch, on_callback
from app.db.models import TrackedWallet, User
from app.templates import admin_restricted_text, user_welcome_text
from tests.pipeline_containers import truncate_pipeline
from tests.test_telegram_bot import TELEGRAM_ID, _command, _context, grant_admin

pytest_plugins = ["tests.pipeline_containers"]

T0 = datetime(2026, 9, 24, 18, 0, tzinfo=timezone.utc)


@pytest.fixture
async def sessions(
    pipeline_infra: object,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    factory = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    yield factory
    await truncate_pipeline(engine)
    await engine.dispose()


USER_ID = 999
PREVIOUS_OWNER = 8700246141
ROOT = Path(__file__).resolve().parents[1]


async def test_start_registers_a_user_and_is_idempotent(sessions: object) -> None:
    update = _command(USER_ID)
    context = _context(sessions, admins=frozenset({TELEGRAM_ID}))
    await cmd_start(update, context)
    await cmd_start(update, context)

    text_out = update.message.reply_text.await_args_list[0].args[0]
    assert text_out == user_welcome_text()
    assert "/settings" in text_out
    assert "/watch" not in text_out
    async with sessions() as session:  # type: ignore[operator]
        rows = list(await session.scalars(select(User)))
    assert len(rows) == 1
    assert rows[0].telegram_id == USER_ID
    assert rows[0].role == "user"
    assert rows[0].tier == "free"


async def test_user_role_is_refused_wallet_commands(sessions: object) -> None:
    context = _context(sessions, admins=frozenset())
    await cmd_start(_command(USER_ID), context)
    hooks_address = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    context.bot_data["helius"] = object()
    context.bot_data["webhook_id"] = "wh_test"

    watch = _command(USER_ID)
    context.args = [hooks_address, "desk"]
    await cmd_watch(watch, context)
    assert watch.message.reply_text.await_args.args[0] == admin_restricted_text()

    listed = _command(USER_ID)
    await cmd_wallets(listed, context)
    assert listed.message.reply_text.await_args.args[0] == admin_restricted_text()

    async with sessions() as session:  # type: ignore[operator]
        stored = await session.scalar(select(TrackedWallet))
    assert stored is None


async def test_admin_role_can_open_wallets(sessions: object) -> None:
    await grant_admin(sessions)
    context = _context(sessions)
    listed = _command()
    await cmd_wallets(listed, context)
    assert listed.message.reply_text.await_args.args[0] != admin_restricted_text()


async def test_menu_hides_wallets_from_users(sessions: object) -> None:
    context = _context(sessions, admins=frozenset())
    await cmd_start(_command(USER_ID), context)
    opened = _command(USER_ID)
    await cmd_menu(opened, context)
    callbacks = {
        button.callback_data
        for row in opened.message.reply_text.await_args.kwargs["reply_markup"].inline_keyboard
        for button in row
    }
    assert "m:wallets" not in callbacks
    assert "m:signals" in callbacks

    pressed = _command(USER_ID)
    pressed.callback_query = MagicMock()
    pressed.callback_query.answer = AsyncMock()
    pressed.callback_query.data = "m:wallets"
    pressed.callback_query.edit_message_text = AsyncMock()
    await on_callback(pressed, context)
    pressed.callback_query.answer.assert_awaited()
    assert pressed.callback_query.edit_message_text.await_args.args[0] == admin_restricted_text()


async def test_menu_shows_wallets_to_admins(sessions: object) -> None:
    await grant_admin(sessions)
    opened = _command()
    await cmd_menu(opened, _context(sessions))
    callbacks = {
        button.callback_data
        for row in opened.message.reply_text.await_args.kwargs["reply_markup"].inline_keyboard
        for button in row
    }
    assert "m:wallets" in callbacks


async def test_data_migration_promotes_the_previous_owner_list(pipeline_infra: object) -> None:
    statement = "UPDATE users SET role = 'admin' WHERE telegram_id IN (8700246141)"
    source = (ROOT / "alembic/versions/0008_promote_admins.py").read_text(encoding="utf-8")
    assert statement in source

    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    factory = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        async with factory() as session:
            async with session.begin():
                session.add(
                    User(
                        telegram_id=PREVIOUS_OWNER,
                        tier="free",
                        role="user",
                        settings={},
                        created_at=T0,
                    )
                )
                session.add(
                    User(
                        telegram_id=USER_ID,
                        tier="free",
                        role="user",
                        settings={},
                        created_at=T0,
                    )
                )
        async with engine.begin() as connection:
            await connection.execute(text(statement))
        async with factory() as session:
            owner = await session.scalar(select(User).where(User.telegram_id == PREVIOUS_OWNER))
            other = await session.scalar(select(User).where(User.telegram_id == USER_ID))
        assert owner is not None and owner.role == "admin"
        assert other is not None and other.role == "user"
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()
