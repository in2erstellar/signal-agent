"""Telegram bot commands and the per-chat send limiter.

The token comes from Settings. This module does not embed it.
Polling is started by the worker process, not by import.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol

import structlog
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from app.adapters.helius import HeliusError
from app.api.schemas import WalletCreate
from app.api.token_risk import report_from_token
from app.config import (
    ALERT_CHAT_INTERVAL_SECONDS,
    DEFAULT_MAX_RISK,
    DEFAULT_MIN_SCORE,
    MAX_RISK_CHOICES,
    MIN_SCORE_CHOICES,
)
from app.db.models import AlertDelivery, Signal, Token, TrackedWallet, User, WalletTx
from app.services.subscriber_settings import AlertPrefs, parse_settings, with_updates
from app.templates import (
    admin_restricted_text,
    max_risk_label,
    menu_text,
    min_score_label,
    risk_missing_text,
    risk_text,
    settings_text,
    signals_text,
    stale_menu_text,
    status_text,
    unregistered_text,
    unwatch_blocked_text,
    unwatch_done_text,
    unwatch_missing_text,
    unwatch_unsubscribe_failed_text,
    user_welcome_text,
    wallets_text,
    watch_duplicate_text,
    watch_invalid_text,
    watch_saved_text,
    watch_subscribe_failed_text,
    watch_usage_text,
    watch_webhook_missing_text,
    welcome_text,
)

logger = structlog.get_logger(__name__)

Clock = Callable[[], datetime]
Sleep = Callable[[float], Awaitable[None]]
Monotonic = Callable[[], float]


class PerChatRateLimiter:
    """At most one outgoing message per chat per interval.

    Each chat has its own lock, which is a semaphore of one, and a
    next-allowed timestamp. ``wait`` sleeps until that timestamp and then
    reserves the next interval.
    """

    def __init__(
        self,
        *,
        interval_seconds: float = ALERT_CHAT_INTERVAL_SECONDS,
        sleep: Sleep | None = None,
        clock: Monotonic | None = None,
    ) -> None:
        self._interval = interval_seconds
        self._sleep = sleep or asyncio.sleep
        self._clock = clock or time.monotonic
        self._locks: dict[int | str, asyncio.Lock] = {}
        self._next_at: dict[int | str, float] = {}

    async def wait(self, chat_id: int | str) -> None:
        lock = self._locks.setdefault(chat_id, asyncio.Lock())
        async with lock:
            now = self._clock()
            ready_at = self._next_at.get(chat_id, now)
            delay = ready_at - now
            if delay > 0:
                await self._sleep(delay)
                now = self._clock()
            self._next_at[chat_id] = now + self._interval


class AlertBot(Protocol):
    """The slice of ``telegram.Bot.send_message`` the dispatcher uses.

    Typed as a protocol so tests can pass a fake without importing a live bot.
    """

    async def send_message(
        self,
        *,
        chat_id: int | str,
        text: str,
        parse_mode: str,
        link_preview_options: LinkPreviewOptions,
    ) -> Any: ...


class TelegramAlertSender:
    """Send one HTML alert through the bot, after the per-chat limiter."""

    def __init__(self, bot: AlertBot, limiter: PerChatRateLimiter | None = None) -> None:
        self._bot = bot
        self._limiter = limiter or PerChatRateLimiter()

    async def send(self, chat_id: int | str, text: str) -> None:
        await self._limiter.wait(chat_id)
        await self._bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )


def telegram_configured(token: str) -> bool:
    cleaned = token.strip()
    return bool(cleaned) and cleaned != "replace-me"


def admin_ids(raw: str) -> frozenset[int]:
    """Numeric Telegram ids. Empty means the admin list is empty."""
    text = raw.strip()
    if not text:
        return frozenset()
    found: list[int] = []
    for part in text.split(","):
        piece = part.strip()
        if not piece.isdigit():
            raise ValueError(
                "TELEGRAM_ADMIN_IDS must be a comma-separated list of numeric Telegram ids"
            )
        found.append(int(piece))
    return frozenset(found)


def build_application(
    token: str,
    sessions: async_sessionmaker[AsyncSession],
    *,
    clock: Clock | None = None,
    admins: frozenset[int] | None = None,
) -> Application[Any, Any, Any, Any, Any, Any]:
    application = Application.builder().token(token).build()
    application.bot_data["sessions"] = sessions
    application.bot_data["clock"] = clock or (lambda: datetime.now(timezone.utc))
    application.bot_data["admins"] = admins if admins is not None else frozenset()
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("menu", cmd_menu))
    application.add_handler(CommandHandler("settings", cmd_settings))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("watch", cmd_watch))
    application.add_handler(CommandHandler("unwatch", cmd_unwatch))
    application.add_handler(CommandHandler("wallets", cmd_wallets))
    application.add_handler(CommandHandler("signals", cmd_signals))
    application.add_handler(CommandHandler("risk", cmd_risk))
    application.add_handler(CallbackQueryHandler(on_pref, pattern=r"^pref:"))
    application.add_handler(CallbackQueryHandler(on_menu, pattern=r"^menu:"))
    return application


def _admins(context: ContextTypes.DEFAULT_TYPE) -> frozenset[int]:
    raw = context.bot_data.get("admins")
    if isinstance(raw, frozenset):
        return raw
    return frozenset()


async def _role_for(context: ContextTypes.DEFAULT_TYPE, telegram_id: int) -> str | None:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        row = await session.scalar(select(User).where(User.telegram_id == telegram_id))
        if row is None:
            return None
        return row.role


async def _require_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Wallet commands are admin-only. Other commands do not call this."""
    user = update.effective_user
    message = update.message
    if user is None or message is None:
        return False
    if await _role_for(context, user.id) == "admin":
        return True
    await message.reply_text(admin_restricted_text())
    return False


def menu_markup(*, admin: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("Signals", callback_data="menu:signals")],
    ]
    if admin:
        rows.append([InlineKeyboardButton("Wallets", callback_data="menu:wallets")])
    rows.append([InlineKeyboardButton("Settings", callback_data="menu:settings")])
    rows.append([InlineKeyboardButton("Status", callback_data="menu:status")])
    return InlineKeyboardMarkup(rows)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if user is None or message is None:
        return
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    clock: Clock = context.bot_data["clock"]
    async with sessions() as session:
        async with session.begin():
            row = await _register(session, user.id, clock(), admins=_admins(context))
            role = row.role
    if role == "admin":
        await message.reply_text(welcome_text())
        return
    await message.reply_text(user_welcome_text())


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if user is None or message is None:
        return
    role = await _role_for(context, user.id)
    if role is None:
        await message.reply_text(unregistered_text())
        return
    admin = role == "admin"
    await message.reply_text(menu_text(admin=admin), reply_markup=menu_markup(admin=admin))


async def on_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return
    await query.answer()
    action = (query.data or "").split(":")
    if len(action) != 2 or action[0] != "menu":
        await query.edit_message_text(stale_menu_text())
        return
    name = action[1]
    role = await _role_for(context, user.id)
    if name == "wallets":
        if role != "admin":
            await query.edit_message_text(admin_restricted_text())
            return
        await query.edit_message_text(await _wallets_body(context))
        return
    if name == "signals":
        await query.edit_message_text(await _signals_body(context))
        return
    if name == "settings":
        prefs = await _prefs_for(context, user.id)
        if prefs is None:
            await query.edit_message_text(unregistered_text())
            return
        await query.edit_message_text(
            settings_text(min_score=prefs.min_score, max_risk=prefs.max_risk),
            reply_markup=_markup(prefs.min_score, prefs.max_risk),
        )
        return
    if name == "status":
        text = await _status_body(context, user.id)
        await query.edit_message_text(text if text is not None else unregistered_text())
        return
    await query.edit_message_text(stale_menu_text())


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if user is None or message is None:
        return
    prefs = await _prefs_for(context, user.id)
    if prefs is None:
        await message.reply_text(unregistered_text())
        return
    await message.reply_text(
        settings_text(min_score=prefs.min_score, max_risk=prefs.max_risk),
        reply_markup=_markup(prefs.min_score, prefs.max_risk),
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if user is None or message is None:
        return
    text = await _status_body(context, user.id)
    await message.reply_text(text if text is not None else unregistered_text())


async def on_pref(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None or not query.data:
        return
    await query.answer()
    parts = query.data.split(":")
    if len(parts) != 3:
        return
    _, kind, raw = parts
    value = _allowed(kind, raw)
    if value is None:
        return
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    prefs = None
    async with sessions() as session:
        async with session.begin():
            row = await session.scalar(select(User).where(User.telegram_id == user.id))
            if row is not None:
                row.settings = with_updates(row.settings, **{kind: value})
                prefs = parse_settings(row.settings)
    if prefs is None:
        await query.edit_message_text(unregistered_text())
        return
    await query.edit_message_text(
        settings_text(min_score=prefs.min_score, max_risk=prefs.max_risk),
        reply_markup=_markup(prefs.min_score, prefs.max_risk),
    )


def _webhook_id(context: ContextTypes.DEFAULT_TYPE) -> str | None:
    raw = context.bot_data.get("webhook_id")
    if not isinstance(raw, str):
        return None
    cleaned = raw.strip()
    return cleaned or None


async def _add_webhook_address(context: ContextTypes.DEFAULT_TYPE, address: str) -> None:
    webhook_id = _webhook_id(context)
    helius = context.bot_data.get("helius")
    if webhook_id is None or helius is None:
        raise HeliusError("HELIUS_WEBHOOK_ID is not configured")
    await helius.add_address_to_webhook(webhook_id, address)


async def _remove_webhook_address(context: ContextTypes.DEFAULT_TYPE, address: str) -> None:
    webhook_id = _webhook_id(context)
    helius = context.bot_data.get("helius")
    if webhook_id is None or helius is None:
        raise HeliusError("HELIUS_WEBHOOK_ID is not configured")
    await helius.remove_address_from_webhook(webhook_id, address)


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    message = update.message
    if message is None:
        return
    args = [str(part) for part in (context.args or [])]
    if not args:
        await message.reply_text(watch_usage_text())
        return
    label = " ".join(args[1:]).strip()[:64] or None
    try:
        body = WalletCreate(address=args[0], label=label)
    except ValidationError:
        await message.reply_text(watch_invalid_text())
        return
    if _webhook_id(context) is None or context.bot_data.get("helius") is None:
        await message.reply_text(watch_webhook_missing_text())
        return
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    clock: Clock = context.bot_data["clock"]
    try:
        async with sessions() as session:
            async with session.begin():
                session.add(
                    TrackedWallet(
                        address=body.address,
                        label=body.normalized_label(),
                        first_seen_at=clock(),
                    )
                )
                await session.flush()
                await _add_webhook_address(context, body.address)
    except IntegrityError:
        await message.reply_text(watch_duplicate_text())
        return
    except HeliusError:
        logger.warning("watch_subscribe_failed", address=body.address)
        await message.reply_text(watch_subscribe_failed_text())
        return
    await message.reply_text(watch_saved_text(body.address, body.normalized_label()))


async def cmd_unwatch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    message = update.message
    if message is None:
        return
    args = [str(part) for part in (context.args or [])]
    if not args:
        await message.reply_text("Формат: /unwatch адрес")
        return
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    try:
        async with sessions() as session:
            async with session.begin():
                wallet = await session.scalar(
                    select(TrackedWallet).where(TrackedWallet.address == args[0])
                )
                if wallet is None:
                    await message.reply_text(unwatch_missing_text())
                    return
                trades = await session.scalar(
                    select(func.count())
                    .select_from(WalletTx)
                    .where(WalletTx.wallet_id == wallet.id)
                )
                if int(trades or 0) > 0:
                    await message.reply_text(unwatch_blocked_text())
                    return
                if _webhook_id(context) is None or context.bot_data.get("helius") is None:
                    await message.reply_text(watch_webhook_missing_text())
                    return
                await _remove_webhook_address(context, args[0])
                await session.delete(wallet)
    except IntegrityError:
        await message.reply_text(unwatch_blocked_text())
        return
    except HeliusError:
        logger.warning("unwatch_unsubscribe_failed", address=args[0])
        await message.reply_text(unwatch_unsubscribe_failed_text())
        return
    await message.reply_text(unwatch_done_text(args[0]))


async def _wallets_body(context: ContextTypes.DEFAULT_TYPE) -> str:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        rows = list(
            await session.scalars(
                select(TrackedWallet).order_by(TrackedWallet.first_seen_at.desc()).limit(15)
            )
        )
    lines = [f"{row.address}" + (f" — {row.label}" if row.label else "") for row in rows]
    return wallets_text(lines)


async def _signals_body(context: ContextTypes.DEFAULT_TYPE) -> str:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        rows = (
            await session.execute(
                select(Signal, Token)
                .join(Token, Signal.token_id == Token.id)
                .order_by(Signal.triggered_at.desc(), Signal.id.desc())
                .limit(8)
            )
        ).all()
    lines = [
        f"{token.symbol or token.mint[:4]} composite {signal.composite_score} risk {signal.risk_score}"
        for signal, token in rows
    ]
    return signals_text(lines)


async def _status_body(context: ContextTypes.DEFAULT_TYPE, telegram_id: int) -> str | None:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    clock: Clock = context.bot_data["clock"]
    async with sessions() as session:
        row = await session.scalar(select(User).where(User.telegram_id == telegram_id))
        if row is None:
            return None
        prefs = parse_settings(row.settings)
        since = clock() - timedelta(hours=24)
        alerts_24h = await session.scalar(
            select(func.count())
            .select_from(AlertDelivery)
            .where(
                AlertDelivery.user_id == row.id,
                AlertDelivery.sent_at >= since,
            )
        )
    return status_text(
        min_score=prefs.min_score,
        max_risk=prefs.max_risk,
        active=prefs.active,
        alerts_24h=int(alerts_24h or 0),
    )


async def cmd_wallets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    message = update.message
    if message is None:
        return
    await message.reply_text(await _wallets_body(context))


async def cmd_signals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await message.reply_text(await _signals_body(context))


async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    args = [str(part) for part in (context.args or [])]
    if not args:
        await message.reply_text("Формат: /risk mint")
        return
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        token = await session.scalar(select(Token).where(Token.mint == args[0]))
    if token is None or token.risk_score is None:
        await message.reply_text(risk_missing_text())
        return
    try:
        report = report_from_token(token)
    except ValueError:
        await message.reply_text(risk_missing_text())
        return
    flags = [check.name for check in report.checks if check.passed is False]
    await message.reply_text(risk_text(mint=report.mint, risk_score=report.risk_score, flags=flags))


async def _register(
    session: AsyncSession,
    telegram_id: int,
    now: datetime,
    *,
    admins: frozenset[int],
) -> User:
    row = await session.scalar(select(User).where(User.telegram_id == telegram_id))
    if row is None:
        row = User(
            telegram_id=telegram_id,
            tier="free",
            role="admin" if telegram_id in admins else "user",
            settings=with_updates(
                {},
                min_score=DEFAULT_MIN_SCORE,
                max_risk=DEFAULT_MAX_RISK,
                active=True,
            ),
            created_at=now,
        )
        session.add(row)
        await session.flush()
        return row
    row.settings = with_updates(row.settings, active=True)
    return row


async def _prefs_for(
    context: ContextTypes.DEFAULT_TYPE,
    telegram_id: int,
) -> AlertPrefs | None:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        row = await session.scalar(select(User).where(User.telegram_id == telegram_id))
        if row is None:
            return None
        return parse_settings(row.settings)


def _markup(min_score: Decimal, max_risk: Decimal) -> InlineKeyboardMarkup:
    min_row = [
        InlineKeyboardButton(
            min_score_label(value, selected=value == min_score),
            callback_data=f"pref:min_score:{value}",
        )
        for value in MIN_SCORE_CHOICES
    ]
    max_row = [
        InlineKeyboardButton(
            max_risk_label(value, selected=value == max_risk),
            callback_data=f"pref:max_risk:{value}",
        )
        for value in MAX_RISK_CHOICES
    ]
    return InlineKeyboardMarkup([min_row, max_row])


def _allowed(kind: str, raw: str) -> Decimal | None:
    try:
        value = Decimal(raw)
    except Exception:
        return None
    choices = MIN_SCORE_CHOICES if kind == "min_score" else MAX_RISK_CHOICES
    if kind not in {"min_score", "max_risk"} or value not in choices:
        return None
    return value
