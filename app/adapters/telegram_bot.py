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
from typing import Any, Protocol, cast
from uuid import UUID

import structlog
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Message,
    ReplyParameters,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.adapters.helius import HeliusError
from app.api.schemas import WalletCreate
from app.api.token_risk import report_from_token
from app.config import (
    ALERT_CHAT_INTERVAL_SECONDS,
    DEFAULT_MAX_DEV_CLUSTER_SHARE,
    DEFAULT_MAX_RISK,
    DEFAULT_MIN_SCORE,
    DEV_CLUSTER_SHARE_STEP,
    SETTINGS_STEP,
    SIGNAL_KIND_CONFIRMED,
    SIGNAL_PAGE_SIZE,
)
from app.db.models import AlertDelivery, BaitFlag, Signal, Token, TrackedWallet, User, WalletTx
from app.services.creator_cluster import share_above_limit
from app.services.dialog_state import clear_pending_input, get_pending_input, set_pending_input
from app.services.risk_scanner import unavailable_fields
from app.services.subscriber_settings import ALERT_PRESETS, AlertPrefs, parse_settings, with_updates
from app.services.token_store import AI_REVIEW_COMPLETED
from app.services.track_record import load_track_record
from app.templates import (
    HOW_CHECKS_BUTTON,
    TRACK_RECORD_BUTTON,
    admin_restricted_text,
    checks_explained_text,
    data_incomplete_text,
    format_score,
    format_share,
    help_text,
    menu_text,
    risk_card_text,
    risk_missing_text,
    risk_usage_text,
    settings_text,
    signal_row,
    signals_text,
    stale_menu_text,
    status_text,
    track_record_text,
    unregistered_text,
    unwatch_blocked_text,
    unwatch_confirm_text,
    unwatch_done_text,
    unwatch_missing_text,
    unwatch_unsubscribe_failed_text,
    unwatch_usage_text,
    user_welcome_text,
    wallet_button_label,
    wallets_text,
    watch_duplicate_text,
    watch_invalid_text,
    watch_prompt_text,
    watch_saved_text,
    watch_subscribe_failed_text,
    watch_usage_text,
    watch_webhook_missing_text,
    welcome_text,
)
from app.ui_constants import (
    ICON_ADD,
    ICON_CANCEL,
    ICON_CONFIRM,
    ICON_DELETE,
    ICON_RISK,
    ICON_SETTINGS,
    ICON_SIGNAL,
    ICON_STATUS,
    ICON_WALLET,
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
        reply_markup: InlineKeyboardMarkup | None = None,
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

    async def send_checklist(self, chat_id: int | str, text: str) -> None:
        await self._limiter.wait(chat_id)
        await self._bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            reply_markup=checklist_markup(),
        )


class TelegramChannelSender:
    """Posts to the public channel. Returns the message id for replies."""

    def __init__(self, bot: Any, chat_id: str) -> None:
        self._bot = bot
        self._chat_id = chat_id

    async def post(
        self,
        text: str,
        *,
        reply_to: int | None = None,
        link: tuple[str, str] | None = None,
    ) -> int:
        markup = (
            None
            if link is None
            else InlineKeyboardMarkup([[InlineKeyboardButton(link[0], url=link[1])]])
        )
        message = await self._bot.send_message(
            chat_id=self._chat_id,
            text=text,
            parse_mode="HTML",
            link_preview_options=LinkPreviewOptions(is_disabled=True),
            reply_parameters=(
                None
                if reply_to is None
                else ReplyParameters(message_id=reply_to, allow_sending_without_reply=True)
            ),
            reply_markup=markup,
        )
        return int(message.message_id)


def checklist_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(HOW_CHECKS_BUTTON, callback_data="i:how"),
                InlineKeyboardButton(TRACK_RECORD_BUTTON, callback_data="i:track"),
            ]
        ]
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


AWAITING_WALLET = "awaiting_wallet_address"
_CALLBACK_KINDS = frozenset({"m", "w", "s", "set", "i"})


def parse_callback(data: str) -> tuple[str, tuple[str, ...]] | None:
    """Split a callback into its kind and the rest. Unknown kinds are None."""
    parts = data.split(":")
    if not parts or parts[0] not in _CALLBACK_KINDS:
        return None
    return parts[0], tuple(parts[1:])


BOT_COMMANDS = (
    BotCommand("menu", f"{ICON_SIGNAL} Menu"),
    BotCommand("signals", f"{ICON_SIGNAL} Latest signals"),
    BotCommand("settings", f"{ICON_SETTINGS} Score floor and risk cap"),
    BotCommand("status", f"{ICON_STATUS} Alert status"),
    BotCommand("risk", f"{ICON_RISK} Stored scan for a mint"),
    BotCommand("help", f"{ICON_SIGNAL} How a card is read"),
    BotCommand("checks", "🔍 How the 12 checks work"),
    BotCommand("track", f"{ICON_STATUS} Results after past signals"),
    BotCommand("wallets", f"{ICON_WALLET} Watched wallets"),
    BotCommand("watch", f"{ICON_ADD} Add a wallet"),
    BotCommand("unwatch", f"{ICON_DELETE} Remove a wallet"),
)


async def _publish_commands(application: Application[Any, Any, Any, Any, Any, Any]) -> None:
    await application.bot.set_my_commands(list(BOT_COMMANDS))


def build_application(
    token: str,
    sessions: async_sessionmaker[AsyncSession],
    *,
    clock: Clock | None = None,
    admins: frozenset[int] | None = None,
    redis: object | None = None,
) -> Application[Any, Any, Any, Any, Any, Any]:
    application = Application.builder().token(token).post_init(_publish_commands).build()
    application.bot_data["sessions"] = sessions
    application.bot_data["clock"] = clock or (lambda: datetime.now(timezone.utc))
    application.bot_data["admins"] = admins if admins is not None else frozenset()
    application.bot_data["redis"] = redis
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("checks", cmd_checks))
    application.add_handler(CommandHandler("track", cmd_track))
    application.add_handler(CommandHandler("menu", cmd_menu))
    application.add_handler(CommandHandler("settings", cmd_settings))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("watch", cmd_watch))
    application.add_handler(CommandHandler("unwatch", cmd_unwatch))
    application.add_handler(CommandHandler("wallets", cmd_wallets))
    application.add_handler(CommandHandler("signals", cmd_signals))
    application.add_handler(CommandHandler("risk", cmd_risk))
    application.add_handler(CallbackQueryHandler(on_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_wallet_text))
    return application


def _admins(context: ContextTypes.DEFAULT_TYPE) -> frozenset[int]:
    raw = context.bot_data.get("admins")
    if isinstance(raw, frozenset):
        return raw
    return frozenset()


def _clock(context: ContextTypes.DEFAULT_TYPE) -> Clock:
    clock = context.bot_data.get("clock")
    if callable(clock):
        return cast(Clock, clock)
    return lambda: datetime.now(timezone.utc)


def _redis(context: ContextTypes.DEFAULT_TYPE) -> Any:
    return context.bot_data.get("redis")


async def _send(
    message: Message,
    text: str,
    markup: InlineKeyboardMarkup | None = None,
) -> None:
    await message.reply_text(text, parse_mode="HTML", reply_markup=markup)


async def _edit(
    query: CallbackQuery,
    text: str,
    markup: InlineKeyboardMarkup | None = None,
) -> None:
    await query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)


def menu_markup(*, admin: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(f"{ICON_SIGNAL} Signals", callback_data="m:signals")],
    ]
    if admin:
        rows.append([InlineKeyboardButton(f"{ICON_WALLET} Wallets", callback_data="m:wallets")])
    rows.append([InlineKeyboardButton(f"{ICON_SETTINGS} Settings", callback_data="m:settings")])
    rows.append([InlineKeyboardButton(f"{ICON_STATUS} Status", callback_data="m:status")])
    return InlineKeyboardMarkup(rows)


def settings_markup(
    min_score: Decimal,
    max_risk: Decimal,
    *,
    active: bool,
    max_dev_cluster_share: Decimal = DEFAULT_MAX_DEV_CLUSTER_SHARE,
) -> InlineKeyboardMarkup:
    step = format_score(SETTINGS_STEP)
    alerts = "🔔 Alerts: on" if active else "🔕 Alerts: paused"
    toggle = "off" if active else "on"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🎯 Score floor", callback_data="set:hold")],
            [
                InlineKeyboardButton(f"− {step}", callback_data="set:min:-"),
                InlineKeyboardButton(format_score(min_score), callback_data="set:hold"),
                InlineKeyboardButton(f"+ {step}", callback_data="set:min:+"),
            ],
            [InlineKeyboardButton("🛡 Risk cap", callback_data="set:hold")],
            [
                InlineKeyboardButton(f"− {step}", callback_data="set:max:-"),
                InlineKeyboardButton(format_score(max_risk), callback_data="set:hold"),
                InlineKeyboardButton(f"+ {step}", callback_data="set:max:+"),
            ],
            [InlineKeyboardButton("👥 Creator-funder limit", callback_data="set:hold")],
            [
                InlineKeyboardButton("− 5%", callback_data="set:dev:-"),
                InlineKeyboardButton(format_share(max_dev_cluster_share), callback_data="set:hold"),
                InlineKeyboardButton("+ 5%", callback_data="set:dev:+"),
            ],
            [InlineKeyboardButton(alerts, callback_data=f"set:live:{toggle}")],
            [
                InlineKeyboardButton("🔒 Strict", callback_data="set:go:strict"),
                InlineKeyboardButton("⚖️ Balanced", callback_data="set:go:balanced"),
                InlineKeyboardButton("🌊 Wide", callback_data="set:go:wide"),
            ],
            [_menu_button()],
        ]
    )


def _menu_button() -> InlineKeyboardButton:
    return InlineKeyboardButton("Menu", callback_data="m:home")


def _parse_uuid(raw: str) -> UUID | None:
    try:
        return UUID(raw)
    except ValueError:
        return None


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
    await _send(message, admin_restricted_text())
    return False


async def _clear_dialog(context: ContextTypes.DEFAULT_TYPE, telegram_id: int) -> None:
    redis = _redis(context)
    if redis is None:
        return
    await clear_pending_input(redis, telegram_id)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if user is None or message is None:
        return
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        async with session.begin():
            row = await _register(session, user.id, _clock(context)(), admins=_admins(context))
            role = row.role
    signal_id = parse_signal_link(context.args or [])
    if signal_id is not None:
        card = await _signal_card_text(context, signal_id, user.id)
        if card is not None:
            await _send(message, card, checklist_markup())
            return
    if role == "admin":
        await _send(message, welcome_text())
    else:
        await _send(message, user_welcome_text())
    await _send_menu(message, admin=role == "admin")


SIGNAL_LINK_PREFIX = "sig_"


def signal_link(bot_username: str, signal_id: UUID) -> str:
    """``t.me`` deep link that opens a stored signal card in the bot."""
    return f"https://t.me/{bot_username}?start={SIGNAL_LINK_PREFIX}{signal_id.hex}"


def parse_signal_link(args: list[str]) -> UUID | None:
    if len(args) != 1 or not args[0].startswith(SIGNAL_LINK_PREFIX):
        return None
    return _parse_uuid(args[0][len(SIGNAL_LINK_PREFIX) :])


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await _send(message, help_text())


async def cmd_checks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await _send(message, checks_explained_text())


async def cmd_track(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await _send(message, await _track_record_body(context))


async def _track_record_body(context: ContextTypes.DEFAULT_TYPE) -> str:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        record = await load_track_record(session)
        confirmed = await load_track_record(session, SIGNAL_KIND_CONFIRMED)
    return track_record_text(record, confirmed)


async def _route_info(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    rest: tuple[str, ...],
) -> bool:
    """Buttons under an alert. The answer is a new message so the card stays."""
    message = query.message
    if rest not in {("how",), ("track",)} or not isinstance(message, Message):
        return False
    text = checks_explained_text() if rest == ("how",) else await _track_record_body(context)
    await _send(message, text)
    return True


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if user is None or message is None:
        return
    role = await _role_for(context, user.id)
    if role is None:
        await _send(message, unregistered_text())
        return
    await _send_menu(message, admin=role == "admin")


async def _send_menu(message: Message, *, admin: bool) -> None:
    await _send(message, menu_text(admin=admin), menu_markup(admin=admin))


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    user = update.effective_user
    parsed = parse_callback(query.data or "")
    if user is None or parsed is None:
        await _edit(query, stale_menu_text())
        return
    kind, rest = parsed
    handled = False
    if kind == "m":
        handled = await _route_menu(context, query, user.id, rest)
    elif kind == "w":
        handled = await _route_wallet(context, query, user.id, rest)
    elif kind == "s":
        handled = await _route_signal(context, query, rest)
    elif kind == "set":
        handled = await _route_settings(context, query, user.id, rest)
    elif kind == "i":
        handled = await _route_info(context, query, rest)
    if not handled:
        await _edit(query, stale_menu_text())


async def _route_menu(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    telegram_id: int,
    rest: tuple[str, ...],
) -> bool:
    if len(rest) != 1 or rest[0] not in {"signals", "wallets", "settings", "status", "home"}:
        return False
    role = await _role_for(context, telegram_id)
    if role is None:
        await _edit(query, unregistered_text())
        return True
    name = rest[0]
    if name == "home":
        await _edit(query, menu_text(admin=role == "admin"), menu_markup(admin=role == "admin"))
        return True
    if name == "wallets":
        if role != "admin":
            await _edit(query, admin_restricted_text())
            return True
        await _show_wallets(context, query)
        return True
    if name == "signals":
        return await _show_signals(context, query, 0)
    if name == "settings":
        await _show_settings(context, query, telegram_id)
        return True
    text = await _status_body(context, telegram_id)
    await _edit(
        query,
        text if text is not None else unregistered_text(),
        InlineKeyboardMarkup([[_menu_button()]]),
    )
    return True


async def _route_wallet(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    telegram_id: int,
    rest: tuple[str, ...],
) -> bool:
    if not rest:
        return False
    action = rest[0]
    if action == "hold":
        return True
    if action in {"no", "list"} and len(rest) == 1:
        if await _role_for(context, telegram_id) != "admin":
            await _edit(query, admin_restricted_text())
            return True
        await _clear_dialog(context, telegram_id)
        await _show_wallets(context, query)
        return True
    if action == "add" and len(rest) == 1:
        if await _role_for(context, telegram_id) != "admin":
            await _edit(query, admin_restricted_text())
            return True
        return await _ask_wallet(context, query, telegram_id)
    if action in {"del", "yes"} and len(rest) == 2:
        wallet_id = _parse_uuid(rest[1])
        if wallet_id is None:
            return False
        if await _role_for(context, telegram_id) != "admin":
            await _edit(query, admin_restricted_text())
            return True
        if action == "del":
            return await _confirm_delete(context, query, wallet_id)
        return await _confirm_delete_yes(context, query, wallet_id)
    return False


async def _route_signal(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    rest: tuple[str, ...],
) -> bool:
    if not rest:
        return False
    if rest[0] == "hold":
        return True
    if rest[0] == "page" and len(rest) == 2 and rest[1].isdigit():
        return await _show_signals(context, query, int(rest[1]))
    if rest[0] == "open" and len(rest) == 3 and rest[2].isdigit():
        signal_id = _parse_uuid(rest[1])
        if signal_id is None:
            return False
        return await _show_signal(context, query, signal_id, int(rest[2]))
    return False


async def _route_settings(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    telegram_id: int,
    rest: tuple[str, ...],
) -> bool:
    if rest == ("hold",):
        return True
    if len(rest) == 2 and rest[0] in {"min", "max"} and rest[1] in {"+", "-"}:
        delta = SETTINGS_STEP if rest[1] == "+" else -SETTINGS_STEP
        await _adjust_setting(context, query, telegram_id, rest[0], delta)
        return True
    if len(rest) == 2 and rest[0] == "dev" and rest[1] in {"+", "-"}:
        delta = DEV_CLUSTER_SHARE_STEP if rest[1] == "+" else -DEV_CLUSTER_SHARE_STEP
        await _adjust_setting(context, query, telegram_id, "dev", delta)
        return True
    if rest[0] == "live" and len(rest) == 2 and rest[1] in {"on", "off"}:
        await _set_alerts(context, query, telegram_id, rest[1] == "on")
        return True
    if rest[0] == "go" and len(rest) == 2 and rest[1] in ALERT_PRESETS:
        await _apply_preset(context, query, telegram_id, rest[1])
        return True
    return False


async def _ask_wallet(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    telegram_id: int,
) -> bool:
    redis = _redis(context)
    if redis is None:
        await _edit(query, watch_usage_text(), InlineKeyboardMarkup([[_menu_button()]]))
        return True
    await set_pending_input(redis, telegram_id, AWAITING_WALLET)
    cancel = InlineKeyboardButton(f"{ICON_CANCEL} Cancel", callback_data="w:no")
    await _edit(query, watch_prompt_text(), InlineKeyboardMarkup([[cancel]]))
    return True


async def _confirm_delete(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    wallet_id: UUID,
) -> bool:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        wallet = await session.get(TrackedWallet, wallet_id)
        if wallet is None:
            address = None
            label = None
        else:
            address = wallet.address
            label = wallet.label
    if address is None:
        await _edit(query, unwatch_missing_text(), InlineKeyboardMarkup([[_menu_button()]]))
        return True
    yes = InlineKeyboardButton(f"{ICON_CONFIRM} Yes, delete", callback_data=f"w:yes:{wallet_id}")
    no = InlineKeyboardButton(f"{ICON_CANCEL} Cancel", callback_data="w:no")
    await _edit(query, unwatch_confirm_text(address, label), InlineKeyboardMarkup([[yes, no]]))
    return True


async def _confirm_delete_yes(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    wallet_id: UUID,
) -> bool:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        wallet = await session.get(TrackedWallet, wallet_id)
        address = None if wallet is None else wallet.address
    if address is None:
        await _edit(query, unwatch_missing_text(), InlineKeyboardMarkup([[_menu_button()]]))
        return True
    text = await _unwatch_address(context, address)
    wallets = InlineKeyboardButton(f"{ICON_WALLET} Wallets", callback_data="w:list")
    await _edit(query, text, InlineKeyboardMarkup([[wallets]]))
    return True


async def _show_wallets(context: ContextTypes.DEFAULT_TYPE, query: CallbackQuery) -> None:
    text, markup = await _wallet_view(context)
    await _edit(query, text, markup)


async def _wallet_view(
    context: ContextTypes.DEFAULT_TYPE,
) -> tuple[str, InlineKeyboardMarkup]:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        rows = list(
            await session.scalars(
                select(TrackedWallet).order_by(TrackedWallet.first_seen_at.desc()).limit(15)
            )
        )
    buttons: list[list[InlineKeyboardButton]] = []
    listed: list[tuple[str, str | None]] = []
    for row in rows:
        listed.append((row.address, row.label))
        buttons.append(
            [
                InlineKeyboardButton(
                    wallet_button_label(row.address, row.label), callback_data="w:hold"
                )
            ]
        )
        buttons.append(
            [
                InlineKeyboardButton(
                    f"{ICON_DELETE} Delete",
                    callback_data=f"w:del:{row.id}",
                )
            ]
        )
    buttons.append([InlineKeyboardButton(f"{ICON_ADD} Add wallet", callback_data="w:add")])
    buttons.append([_menu_button()])
    return wallets_text(listed), InlineKeyboardMarkup(buttons)


async def _show_signals(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    page: int,
) -> bool:
    view = await _signal_view(context, page)
    if view is None:
        return False
    text, markup = view
    await _edit(query, text, markup)
    return True


async def _signal_view(
    context: ContextTypes.DEFAULT_TYPE,
    page: int,
) -> tuple[str, InlineKeyboardMarkup] | None:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        total = int(await session.scalar(select(func.count()).select_from(Signal)) or 0)
        if total == 0:
            rows: list[tuple[Signal, Token]] = []
            pages = 0
        else:
            pages = (total + SIGNAL_PAGE_SIZE - 1) // SIGNAL_PAGE_SIZE
            if page < 0 or page >= pages:
                return None
            found = await session.execute(
                select(Signal, Token)
                .join(Token, Signal.token_id == Token.id)
                .order_by(Signal.triggered_at.desc(), Signal.id.desc())
                .offset(page * SIGNAL_PAGE_SIZE)
                .limit(SIGNAL_PAGE_SIZE)
            )
            rows = [(signal, token) for signal, token in found.all()]
    if pages == 0:
        return signals_text(rows=[], page=0, pages=0), InlineKeyboardMarkup([[_menu_button()]])
    lines = [
        signal_row(token.symbol, signal.composite_score, signal.triggered_at)
        for signal, token in rows
    ]
    buttons: list[list[InlineKeyboardButton]] = []
    for signal, token in rows:
        buttons.append(
            [
                InlineKeyboardButton(
                    signal_row(token.symbol, signal.composite_score, signal.triggered_at)[:60],
                    callback_data=f"s:open:{signal.id}:{page}",
                )
            ]
        )
    back_data = f"s:page:{page - 1}" if page > 0 else "s:hold"
    forward_data = f"s:page:{page + 1}" if page + 1 < pages else "s:hold"
    buttons.append(
        [
            InlineKeyboardButton("◀️ Back", callback_data=back_data),
            InlineKeyboardButton(f"Page {page + 1} of {pages}", callback_data="s:hold"),
            InlineKeyboardButton("Forward ▶️", callback_data=forward_data),
        ]
    )
    buttons.append([_menu_button()])
    return signals_text(rows=lines, page=page + 1, pages=pages), InlineKeyboardMarkup(buttons)


async def _show_signal(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    signal_id: UUID,
    page: int,
) -> bool:
    viewer = query.from_user
    text = await _signal_card_text(
        context, signal_id, viewer.id if viewer is not None and isinstance(viewer.id, int) else None
    )
    if text is None:
        return False
    back = InlineKeyboardButton("◀️ Back", callback_data=f"s:page:{page}")
    await _edit(query, text, InlineKeyboardMarkup([[back], [_menu_button()]]))
    return True


async def _signal_card_text(
    context: ContextTypes.DEFAULT_TYPE,
    signal_id: UUID,
    viewer_id: int | None,
) -> str | None:
    """Stored card for one signal, or None when the signal or its token is gone."""
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        signal = await session.get(Signal, signal_id)
        if signal is None:
            return None
        token = await session.get(Token, signal.token_id)
        if token is None:
            return None
        bait_count = int(
            await session.scalar(
                select(func.count()).select_from(BaitFlag).where(BaitFlag.token_id == token.id)
            )
            or 0
        )
        symbol = token.symbol
        mint = token.mint
        composite = signal.composite_score
        triggered_at = signal.triggered_at
        risk_score = signal.risk_score if signal.risk_score is not None else token.risk_score
        raw_flags = token.risk_flags if isinstance(token.risk_flags, list) else []
        flags = [str(flag) for flag in raw_flags]
        stored = signal.context if isinstance(signal.context, dict) else None
        over_limit = False
        if stored is not None and viewer_id is not None:
            prefs = await _prefs_for(context, viewer_id)
            if prefs is not None:
                over_limit = share_above_limit(stored, prefs.max_dev_cluster_share)
        return risk_card_text(
            symbol=symbol,
            mint=mint,
            risk_score=risk_score,
            flags=flags,
            bait_warning=bait_count > 0,
            composite_score=composite,
            triggered_at=triggered_at,
            ai_review=_stored_review(token),
            context=stored,
            creator_over_limit=over_limit,
        )


def _stored_review(token: Token) -> str | None:
    if token.ai_review_status != AI_REVIEW_COMPLETED:
        return None
    text = token.ai_review
    if not isinstance(text, str) or not text.strip():
        return None
    return text


def _failed_flags(token: Token) -> list[str]:
    try:
        report = report_from_token(token)
    except ValueError:
        raw = token.risk_flags if isinstance(token.risk_flags, list) else []
        return [str(flag) for flag in raw]
    return [check.name for check in report.checks if check.passed is False]


async def _show_settings(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    telegram_id: int,
) -> None:
    prefs = await _prefs_for(context, telegram_id)
    if prefs is None:
        await _edit(query, unregistered_text())
        return
    await _edit(query, *_settings_view(prefs))


async def _adjust_setting(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    telegram_id: int,
    field: str,
    delta: Decimal,
) -> None:
    if field == "dev":
        key = "max_dev_cluster_share"
        ceiling = Decimal(1)
    else:
        key = "min_score" if field == "min" else "max_risk"
        ceiling = Decimal(100)
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        async with session.begin():
            row = await session.scalar(select(User).where(User.telegram_id == telegram_id))
            if row is None:
                await _edit(query, unregistered_text())
                return
            prefs = parse_settings(row.settings)
            if field == "dev":
                current = prefs.max_dev_cluster_share
            elif field == "min":
                current = prefs.min_score
            else:
                current = prefs.max_risk
            updated = current + delta
            if updated < 0 or updated > ceiling:
                return
            row.settings = with_updates(row.settings, **{key: updated})
            prefs = parse_settings(row.settings)
            painted = _settings_view(prefs)
    await _edit(query, *painted)


async def _set_alerts(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    telegram_id: int,
    active: bool,
) -> None:
    await _write_prefs(context, query, telegram_id, active=active)


async def _apply_preset(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    telegram_id: int,
    name: str,
) -> None:
    floor, cap = ALERT_PRESETS[name]
    await _write_prefs(context, query, telegram_id, min_score=floor, max_risk=cap)


async def _write_prefs(
    context: ContextTypes.DEFAULT_TYPE,
    query: CallbackQuery,
    telegram_id: int,
    **changes: object,
) -> None:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        async with session.begin():
            row = await session.scalar(select(User).where(User.telegram_id == telegram_id))
            if row is None:
                await _edit(query, unregistered_text())
                return
            row.settings = with_updates(row.settings, **changes)
            prefs = parse_settings(row.settings)
            painted = _settings_view(prefs)
    await _edit(query, *painted)


def _settings_view(prefs: AlertPrefs) -> tuple[str, InlineKeyboardMarkup]:
    return (
        settings_text(
            min_score=prefs.min_score,
            max_risk=prefs.max_risk,
            active=prefs.active,
            max_dev_cluster_share=prefs.max_dev_cluster_share,
        ),
        settings_markup(
            prefs.min_score,
            prefs.max_risk,
            active=prefs.active,
            max_dev_cluster_share=prefs.max_dev_cluster_share,
        ),
    )


async def on_wallet_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if user is None or message is None or not message.text:
        return
    redis = _redis(context)
    if redis is None:
        return
    state = await get_pending_input(redis, user.id)
    if state != AWAITING_WALLET:
        return
    await clear_pending_input(redis, user.id)
    if await _role_for(context, user.id) != "admin":
        await _send(message, admin_restricted_text())
        return
    parts = message.text.split()
    if not parts:
        await _send(message, watch_prompt_text())
        return
    label = " ".join(parts[1:]).strip()[:64] or None
    await _send(message, await _watch_address(context, parts[0], label))


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if user is None or message is None:
        return
    prefs = await _prefs_for(context, user.id)
    if prefs is None:
        await _send(message, unregistered_text())
        return
    text, markup = _settings_view(prefs)
    await _send(message, text, markup)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if user is None or message is None:
        return
    text = await _status_body(context, user.id)
    await _send(
        message,
        text if text is not None else unregistered_text(),
        InlineKeyboardMarkup([[_menu_button()]]),
    )


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    message = update.message
    if message is None:
        return
    args = [str(part) for part in (context.args or [])]
    if not args:
        await _send(message, watch_usage_text())
        return
    label = " ".join(args[1:]).strip()[:64] or None
    await _send(message, await _watch_address(context, args[0], label))


async def cmd_unwatch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    message = update.message
    if message is None:
        return
    args = [str(part) for part in (context.args or [])]
    if not args:
        await _send(message, unwatch_usage_text())
        return
    await _send(message, await _unwatch_address(context, args[0]))


async def _watch_address(
    context: ContextTypes.DEFAULT_TYPE,
    address: str,
    label: str | None,
) -> str:
    try:
        body = WalletCreate(address=address, label=label)
    except ValidationError:
        return watch_invalid_text()
    if _webhook_id(context) is None or context.bot_data.get("helius") is None:
        return watch_webhook_missing_text()
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    try:
        async with sessions() as session:
            async with session.begin():
                session.add(
                    TrackedWallet(
                        address=body.address,
                        label=body.normalized_label(),
                        first_seen_at=_clock(context)(),
                    )
                )
                await session.flush()
                await _add_webhook_address(context, body.address)
    except IntegrityError:
        return watch_duplicate_text()
    except HeliusError:
        logger.warning("watch_subscribe_failed", address=body.address)
        return watch_subscribe_failed_text()
    return watch_saved_text(body.address, body.normalized_label())


async def _unwatch_address(context: ContextTypes.DEFAULT_TYPE, address: str) -> str:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    try:
        async with sessions() as session:
            async with session.begin():
                wallet = await session.scalar(
                    select(TrackedWallet).where(TrackedWallet.address == address)
                )
                if wallet is None:
                    return unwatch_missing_text()
                trades = await session.scalar(
                    select(func.count())
                    .select_from(WalletTx)
                    .where(WalletTx.wallet_id == wallet.id)
                )
                if int(trades or 0) > 0:
                    return unwatch_blocked_text()
                if _webhook_id(context) is None or context.bot_data.get("helius") is None:
                    return watch_webhook_missing_text()
                await _remove_webhook_address(context, address)
                await session.delete(wallet)
    except IntegrityError:
        return unwatch_blocked_text()
    except HeliusError:
        logger.warning("unwatch_unsubscribe_failed", address=address)
        return unwatch_unsubscribe_failed_text()
    return unwatch_done_text(address)


async def _status_body(context: ContextTypes.DEFAULT_TYPE, telegram_id: int) -> str | None:
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        row = await session.scalar(select(User).where(User.telegram_id == telegram_id))
        if row is None:
            return None
        prefs = parse_settings(row.settings)
        since = _clock(context)() - timedelta(hours=24)
        alerts_24h = await session.scalar(
            select(func.count())
            .select_from(AlertDelivery)
            .where(AlertDelivery.user_id == row.id, AlertDelivery.sent_at >= since)
        )
        min_score = prefs.min_score
        max_risk = prefs.max_risk
        active = prefs.active
        dev_share = prefs.max_dev_cluster_share
    return status_text(
        min_score=min_score,
        max_risk=max_risk,
        active=active,
        alerts_24h=int(alerts_24h or 0),
        max_dev_cluster_share=dev_share,
    )


async def cmd_wallets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_admin(update, context):
        return
    message = update.message
    if message is None:
        return
    text, markup = await _wallet_view(context)
    await _send(message, text, markup)


async def cmd_signals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    view = await _signal_view(context, 0)
    if view is None:
        await _send(message, stale_menu_text())
        return
    text, markup = view
    await _send(message, text, markup)


async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    args = [str(part) for part in (context.args or [])]
    if not args:
        await _send(message, risk_usage_text())
        return
    sessions: async_sessionmaker[AsyncSession] = context.bot_data["sessions"]
    async with sessions() as session:
        token = await session.scalar(select(Token).where(Token.mint == args[0]))
        if token is None or token.risk_score is None:
            await _send(message, risk_missing_text())
            return
        bait_count = int(
            await session.scalar(
                select(func.count()).select_from(BaitFlag).where(BaitFlag.token_id == token.id)
            )
            or 0
        )
        missing = unavailable_fields(token.risk_flags)
        if missing:
            text = data_incomplete_text(mint=token.mint, missing=missing)
        else:
            text = risk_card_text(
                symbol=token.symbol,
                mint=token.mint,
                risk_score=token.risk_score,
                flags=_failed_flags(token),
                bait_warning=bait_count > 0,
                ai_review=_stored_review(token),
            )
    await _send(message, text)


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
