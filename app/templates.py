"""Telegram copy. Workers and handlers call these functions; they do not
embed user-facing sentences. Messages use HTML, not Markdown.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from html import escape

from app.config import FOOTER_DISCLAIMER, RISK_ICON_LOW_MAX, RISK_ICON_MED_MAX
from app.ui_constants import (
    ICON_BAIT_WARNING,
    ICON_CANCEL,
    ICON_RISK_HIGH,
    ICON_RISK_LOW,
    ICON_RISK_MED,
    ICON_SETTINGS,
    ICON_SIGNAL,
    ICON_STATUS,
    ICON_WALLET,
)

DISCLAIMER = FOOTER_DISCLAIMER
BAIT_WARNING = f"{ICON_BAIT_WARNING} Copy-trade bait pattern detected"
STORED_SCAN_NOTE = (
    "This is the stored scan, not a new network request. "
    "A high risk_score means the known checks passed, and does not mean the token is safe."
)


def welcome_text() -> str:
    return (
        f"{ICON_SIGNAL} <b>Parse</b>\n"
        "\n"
        "New token alerts land in this chat when they clear your bar.\n"
        "\n"
        f"{ICON_SETTINGS} Score floor, risk cap, presets, and the alert switch are in Settings.\n"
        f"{ICON_WALLET} Wallet list is on the menu.\n"
        f"{ICON_CANCEL} This bot does not buy or sell.\n"
        "\n"
        "Use the buttons, or /menu."
    )


def user_welcome_text() -> str:
    return (
        f"{ICON_SIGNAL} <b>Parse</b>\n"
        "\n"
        "Token alerts from watched wallets land in this chat.\n"
        "\n"
        f"{ICON_SIGNAL} Signals and stored risk cards are on the menu.\n"
        f"{ICON_SETTINGS} Your score floor, risk cap, and alert switch are in Settings.\n"
        f"{ICON_CANCEL} This bot does not buy or sell.\n"
        "\n"
        "Use the buttons, or /menu."
    )


def admin_restricted_text() -> str:
    return "This command is restricted."


def menu_text(*, admin: bool) -> str:
    if admin:
        return (
            f"{ICON_SIGNAL} <b>Menu</b>\n"
            "Signals · wallets · settings · status"
        )
    return (
        f"{ICON_SIGNAL} <b>Menu</b>\n"
        "Signals · settings · status"
    )


def stale_menu_text() -> str:
    return "This menu is out of date. Open /menu again."


def unregistered_text() -> str:
    return "Send /start first to subscribe to signals."


def settings_text(*, min_score: Decimal, max_risk: Decimal, active: bool) -> str:
    alerts = "on" if active else "paused"
    return (
        f"{ICON_SETTINGS} <b>Settings</b>\n"
        "\n"
        f"🎯 <b>Score floor</b>  {format_score(min_score)}\n"
        "Alert when the composite score is at least this.\n"
        "\n"
        f"🛡 <b>Risk cap</b>  {format_score(max_risk)}\n"
        "Highest risk points you still accept (100 − risk score).\n"
        "100 lets every scanned token through. 0 keeps a clean scan only.\n"
        "\n"
        f"🔔 <b>Alerts</b>  {alerts}\n"
        "\n"
        "Presets set both numbers at once.\n"
        "🔒 Strict 70 / 25 · ⚖️ Balanced 50 / 60 · 🌊 Wide 30 / 100"
    )


def status_text(
    *,
    min_score: Decimal,
    max_risk: Decimal,
    active: bool,
    alerts_24h: int,
) -> str:
    state = "on" if active else "paused"
    return (
        f"{ICON_STATUS} <b>Status</b>\n"
        "\n"
        f"🔔 Alerts: {state}\n"
        f"🎯 Score floor: {format_score(min_score)}\n"
        f"🛡 Risk cap: {format_score(max_risk)}\n"
        f"📨 Last 24 hours: {alerts_24h}"
    )


def alert_text(
    *,
    symbol: str | None,
    mint: str,
    composite_score: Decimal,
    risk_score: Decimal | None,
    risk_flags: list[str],
    bait_warning: bool,
    ai_review: str | None = None,
) -> str:
    flags = ", ".join(escape(flag) for flag in risk_flags) if risk_flags else "none"
    risk = "—" if risk_score is None else format_score(risk_score)
    lines = [
        f"{risk_icon(risk_score)} <b>{escape(symbol or 'no ticker')}</b>",
        solscan_token(mint),
        "",
        f"<b>{format_score(composite_score)}</b>",
        f"risk_score: {risk}",
        f"risk_flags: {flags}",
    ]
    if bait_warning:
        lines.extend(["", f"<b>{BAIT_WARNING}</b>"])
    lines.extend(_review_lines(ai_review))
    lines.extend(["", DISCLAIMER])
    return "\n".join(lines)


def risk_card_text(
    *,
    symbol: str | None,
    mint: str,
    risk_score: Decimal | None,
    flags: list[str],
    bait_warning: bool,
    composite_score: Decimal | None = None,
    triggered_at: datetime | None = None,
    ai_review: str | None = None,
) -> str:
    listed = ", ".join(escape(flag) for flag in flags) if flags else "none"
    risk = "—" if risk_score is None else format_score(risk_score)
    lines = [
        f"{risk_icon(risk_score)} <b>{escape(symbol or 'no ticker')}</b>",
        solscan_token(mint),
    ]
    if composite_score is not None:
        lines.append(f"composite_score: <b>{format_score(composite_score)}</b>")
    if triggered_at is not None:
        lines.append(triggered_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    lines.extend(
        [
            f"risk_score: {risk}",
            f"flags: {listed}",
            STORED_SCAN_NOTE,
        ]
    )
    if bait_warning:
        lines.extend(["", f"<b>{BAIT_WARNING}</b>"])
    lines.extend(_review_lines(ai_review))
    lines.extend(["", DISCLAIMER])
    return "\n".join(lines)


_MARKDOWN_BOLD = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)


def _review_lines(ai_review: str | None) -> list[str]:
    if not ai_review or not ai_review.strip():
        return []
    return ["", "<b>AI review</b>", review_html(ai_review)]


def review_html(raw: str) -> str:
    """Escape model text, then turn Markdown bold into Telegram HTML."""
    escaped = escape(raw.strip())
    rendered = _MARKDOWN_BOLD.sub(r"<b>\1</b>", escaped)
    return "\n".join(line.rstrip() for line in rendered.splitlines())


def signal_row(symbol: str | None, composite: Decimal, when: datetime) -> str:
    name = symbol or "no ticker"
    clock = when.astimezone(timezone.utc).strftime("%H:%M")
    return f"{name}  {format_score(composite)}  {clock} UTC"


def signals_text(*, rows: list[str], page: int, pages: int) -> str:
    if pages == 0:
        return f"{ICON_SIGNAL} <b>Signals</b>\nNo signals yet."
    body = "\n".join(escape(row) for row in rows)
    return f"{ICON_SIGNAL} <b>Signals</b>\nPage {page} of {pages}\n{body}"


def watch_usage_text() -> str:
    return "Format: /watch address [label]"


def watch_prompt_text() -> str:
    return "Send the wallet address and an optional label in one message.\nFormat: address label"


def watch_invalid_text() -> str:
    return "That address is not a Solana address: 32–44 base58 characters."


def watch_saved_text(address: str, label: str | None) -> str:
    name = f" ({escape(label)})" if label else ""
    return f"Wallet added and the Helius subscription is active{name}: {solscan_account(address)}"


def watch_subscribe_failed_text() -> str:
    return "Could not subscribe to monitoring. Try again later. The wallet was not saved."


def watch_webhook_missing_text() -> str:
    return (
        "HELIUS_WEBHOOK_ID is not set. Create the webhook once in the Helius "
        "dashboard and put its id in the environment. The wallet was not saved."
    )


def unwatch_unsubscribe_failed_text() -> str:
    return (
        "Could not remove the Helius subscription. The wallet was left on the list. "
        "Try again later."
    )


def watch_duplicate_text() -> str:
    return "That wallet is already on the list."


def unwatch_done_text(address: str) -> str:
    return f"Wallet removed: {solscan_account(address)}"


def unwatch_missing_text() -> str:
    return "That wallet is not on the list."


def unwatch_blocked_text() -> str:
    return "That wallet already has trades, so it cannot be removed."


def unwatch_usage_text() -> str:
    return "Format: /unwatch address"


def unwatch_confirm_text(address: str, label: str | None) -> str:
    short = escape(short_address(address))
    label_bit = f" (label: {escape(label)})" if label else ""
    return (
        f"Delete monitoring {short}{label_bit}? "
        "Removing it from the Helius webhook cannot be undone for this session."
    )


def wallets_text(rows: list[tuple[str, str | None]]) -> str:
    if not rows:
        return f"{ICON_WALLET} <b>Wallets</b>\nNo wallets yet. Add one below or with /watch."
    lines = [f"{ICON_WALLET} <b>Wallets</b>", ""]
    for address, label in rows:
        extra = f" — {escape(label)}" if label else ""
        lines.append(f"{solscan_account(address)}{extra}")
    return "\n".join(lines)


def wallet_button_label(address: str, label: str | None) -> str:
    text = short_address(address)
    if label:
        text = f"{text} — {label}"
    return text[:60]


def risk_usage_text() -> str:
    return "Format: /risk mint"


def risk_missing_text() -> str:
    return "There is no stored scan for that mint."


def risk_icon(risk_score: Decimal | None) -> str:
    if risk_score is None:
        return ICON_RISK_HIGH
    danger = Decimal(100) - risk_score
    if danger <= RISK_ICON_LOW_MAX:
        return ICON_RISK_LOW
    if danger <= RISK_ICON_MED_MAX:
        return ICON_RISK_MED
    return ICON_RISK_HIGH


def format_score(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def short_address(value: str) -> str:
    if len(value) <= 10:
        return value
    return f"{value[:4]}...{value[-4:]}"


def solscan_token(mint: str) -> str:
    return _solscan("token", mint)


def solscan_account(address: str) -> str:
    return _solscan("account", address)


def _solscan(kind: str, value: str) -> str:
    href = escape(value, quote=True)
    return f'<a href="https://solscan.io/{kind}/{href}">{escape(short_address(value))}</a>'
