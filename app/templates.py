"""Telegram copy. Workers and handlers call these functions; they do not
embed user-facing sentences. Messages use HTML, not Markdown.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from html import escape

from app.config import (
    DEFAULT_MAX_DEV_CLUSTER_SHARE,
    FOOTER_DISCLAIMER,
    RISK_ICON_LOW_MAX,
    RISK_ICON_MED_MAX,
)
from app.ui_constants import (
    ICON_BAIT_WARNING,
    ICON_CANCEL,
    ICON_CREATOR,
    ICON_DISTRIBUTION,
    ICON_LP_LOCKED,
    ICON_LP_PENDING,
    ICON_LP_UNLOCKED,
    ICON_RISK,
    ICON_RISK_HIGH,
    ICON_RISK_LOW,
    ICON_RISK_MED,
    ICON_SECTION,
    ICON_SETTINGS,
    ICON_SIGNAL,
    ICON_STATUS,
    ICON_UNAVAILABLE,
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
        f"{ICON_SIGNAL} /signals — latest signals\n"
        f"{ICON_SETTINGS} /settings — score floor, risk cap, presets, alert switch\n"
        f"{ICON_WALLET} /wallets — watched wallets\n"
        f"{ICON_STATUS} /status — current limits\n"
        f"{ICON_SIGNAL} /help — how a card is read\n"
        f"{ICON_CANCEL} This bot does not buy or sell.\n"
        "\n"
        "Use the buttons, or /menu."
    )


def user_welcome_text() -> str:
    return (
        f"{ICON_SIGNAL} <b>Parse</b>\n"
        "\n"
        "New token alerts land in this chat when they clear your bar.\n"
        "\n"
        f"{ICON_SIGNAL} /signals — latest signals and stored risk cards\n"
        f"{ICON_SETTINGS} /settings — score floor, risk cap, alert switch\n"
        f"{ICON_STATUS} /status — current limits\n"
        f"{ICON_SIGNAL} /help — how a card is read\n"
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


def settings_text(
    *,
    min_score: Decimal,
    max_risk: Decimal,
    active: bool,
    max_dev_cluster_share: Decimal = DEFAULT_MAX_DEV_CLUSTER_SHARE,
) -> str:
    alerts = "on" if active else "paused"
    return (
        f"{ICON_SETTINGS} <b>Settings</b>\n"
        "\n"
        f"🎯 <b>Score floor</b>  {format_score(min_score)}\n"
        "Alert when the composite score is at least this.\n"
        "\n"
        f"{ICON_RISK} <b>Risk cap</b>  {format_score(max_risk)}\n"
        "Highest risk points you still accept (100 − risk score).\n"
        "100 lets every scanned token through. 0 keeps a clean scan only.\n"
        "\n"
        f"🔔 <b>Alerts</b>  {alerts}\n"
        "\n"
        f"{ICON_CREATOR} <b>Creator-funder limit</b>  {format_share(max_dev_cluster_share)}\n"
        "Shown on its own line when scanned supply on that funder is above this. "
        "It does not change the score.\n"
        "\n"
        "Presets set the score floor and the risk cap.\n"
        f"{ICON_LP_LOCKED} Strict 70 / 25 · ⚖️ Balanced 50 / 60 · 🌊 Wide 30 / 100"
    )


def status_text(
    *,
    min_score: Decimal,
    max_risk: Decimal,
    active: bool,
    alerts_24h: int,
    max_dev_cluster_share: Decimal = DEFAULT_MAX_DEV_CLUSTER_SHARE,
) -> str:
    state = "on" if active else "paused"
    return (
        f"{ICON_STATUS} <b>Status</b>\n"
        "\n"
        f"🔔 Alerts: {state}\n"
        f"🎯 Score floor: {format_score(min_score)}\n"
        f"{ICON_RISK} Risk cap: {format_score(max_risk)}\n"
        f"{ICON_CREATOR} Creator-funder limit: {format_share(max_dev_cluster_share)}\n"
        f"📨 Last 24 hours: {alerts_24h}"
    )


def unlock_alert_text(*, symbol: str | None, mint: str, unlock_at: datetime) -> str:
    when = unlock_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    name = escape(symbol or "no ticker")
    return (
        f"{ICON_LP_PENDING} <b>{name}</b>\n"
        f"{solscan_token(mint)}\n"
        "\n"
        f"LP unlock is recorded for {_code(when)}. "
        "This is a schedule reading, not a prediction of the price."
    )


def data_incomplete_text(*, mint: str, missing: list[str]) -> str:
    fields = ", ".join(escape(field) for field in missing) if missing else "unknown"
    return (
        f"{ICON_UNAVAILABLE} <b>data incomplete: {fields}</b>\n"
        f"{solscan_token(mint)}\n"
        "\n"
        "The source for these fields did not return them, so this is not a score. "
        "A later scan can still change the picture."
    )


def creator_block_text(context: dict[str, object] | None, *, over_limit: bool = False) -> str:
    """Creator-cluster lines. They are not a substitute for the score."""
    if not context or not context.get("creator"):
        return (
            f"{ICON_CREATOR} <b>Creator cluster</b>\n"
            f"{ICON_UNAVAILABLE} unavailable (creator missing)"
        )
    lines = [
        f"{ICON_CREATOR} <b>Creator cluster</b>",
        f"Creator: {_code(context['creator'])}",
        "Previous launches since tracking started: "
        + _code(context.get("launches_since_tracking", 0)),
        "First-hour drop over 90%, since tracking started: "
        + _reading(context.get("price_drop_shown", "unavailable")),
        "Median time to first sell by the creator or a linked wallet: "
        + _reading(context.get("median_sell_shown", "unavailable")),
        _distribution_line(context),
    ]
    if over_limit:
        lines.append(f"{ICON_DISTRIBUTION} Creator-funder supply is above your limit.")
    return "\n".join(lines)


def help_text() -> str:
    return (
        f"{ICON_SIGNAL} <b>Help</b>\n"
        "\n"
        f"{ICON_UNAVAILABLE} A score uses the checks that returned data.\n"
        f"{ICON_UNAVAILABLE} A line that says unavailable was not filled in with a default.\n"
        "\n"
        f"{ICON_UNAVAILABLE} <b>unavailable (no pool yet)</b>\n"
        "Most pump.fun tokens never open a Raydium pool. "
        "No pool means no price snapshot. That is expected, not a failed lookup.\n"
        "\n"
        f"{ICON_UNAVAILABLE} <b>unavailable (price lookup failed)</b>\n"
        "A pool was expected and the Raydium request failed or omitted the price. "
        "That one is worth a look in the logs.\n"
        "\n"
        f"{ICON_CREATOR} Previous launches count only mints stored since tracking started.\n"
        f"{ICON_UNAVAILABLE} Median time to first sell stays unavailable. "
        "That is the limit of the field: only sells already stored for wallets "
        "you track would count, and those rows are not turned into a median.\n"
        f"{ICON_DISTRIBUTION} The creator-funder share is not added to the score. "
        "Above your limit, the card says so on its own line.\n"
        "\n"
        f"{ICON_SETTINGS} /settings — score floor, risk cap, creator-funder limit\n"
        f"{ICON_SIGNAL} /signals — latest signals\n"
        f"{ICON_STATUS} /status — alerts, floors, and the last 24 hours\n"
        f"{ICON_RISK} /risk mint — stored scan\n"
        f"{ICON_WALLET} /wallets — watched wallets\n"
        f"{ICON_SIGNAL} /menu — buttons for the same screens"
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
    context: dict[str, object] | None = None,
    creator_over_limit: bool = False,
) -> str:
    risk = "—" if risk_score is None else format_score(risk_score)
    other_flags, lp_flags = _split_lp_flags(risk_flags)
    lines = [
        f"{risk_icon(risk_score)} <b>{escape(symbol or 'no ticker')}</b>",
        solscan_token(mint),
        "",
        ICON_SECTION,
        f"<b>{_code(format_score(composite_score))}</b>",
        f"risk_score: {_code(risk)}",
        *_flag_block("risk_flags", other_flags, lp_flags),
    ]
    if bait_warning:
        lines.extend(["", f"<b>{BAIT_WARNING}</b>"])
    if context is not None:
        lines.extend(["", ICON_SECTION, creator_block_text(context, over_limit=creator_over_limit)])
    lines.extend(_lp_section(context, lp_flags))
    lines.extend(_review_lines(ai_review))
    lines.extend(["", DISCLAIMER])
    return "\n".join(lines)


def _distribution_line(context: dict[str, object]) -> str:
    label = "Supply on the creator's funder, scanned holders"
    status = context.get("distribution_status")
    if status == "measured" and context.get("distribution_share") is not None:
        share = Decimal(str(context["distribution_share"]))
        percent = (share * Decimal(100)).quantize(Decimal("1"))
        return f"{label}: {_code(f'{percent}%')}"
    reason = context.get("distribution_reason") or "missing"
    return f"{label}: {ICON_UNAVAILABLE} unavailable ({reason})"


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
    context: dict[str, object] | None = None,
    creator_over_limit: bool = False,
) -> str:
    risk = "—" if risk_score is None else format_score(risk_score)
    other_flags, lp_flags = _split_lp_flags(flags)
    lines = [
        f"{risk_icon(risk_score)} <b>{escape(symbol or 'no ticker')}</b>",
        solscan_token(mint),
        "",
        ICON_SECTION,
    ]
    if composite_score is not None:
        lines.append(f"composite_score: <b>{_code(format_score(composite_score))}</b>")
    if triggered_at is not None:
        when = triggered_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines.append(_code(when))
    lines.extend(
        [
            f"risk_score: {_code(risk)}",
            *_flag_block("flags", other_flags, lp_flags),
            STORED_SCAN_NOTE,
        ]
    )
    if bait_warning:
        lines.extend(["", f"<b>{BAIT_WARNING}</b>"])
    if context is not None:
        lines.extend(["", ICON_SECTION, creator_block_text(context, over_limit=creator_over_limit)])
    lines.extend(_lp_section(context, lp_flags))
    lines.extend(_review_lines(ai_review))
    lines.extend(["", DISCLAIMER])
    return "\n".join(lines)


def _split_lp_flags(flags: list[str]) -> tuple[list[str], list[str]]:
    other: list[str] = []
    lp: list[str] = []
    for flag in flags:
        if flag == "check_lp_lock" or flag.startswith("check_lp_lock:"):
            lp.append(flag)
        else:
            other.append(flag)
    return other, lp


def _flag_block(label: str, other: list[str], lp: list[str]) -> list[str]:
    if not other and not lp:
        return [f"{label}: none"]
    if not other:
        return []
    return [f"{label}:", *(_flag_line(flag) for flag in other)]


def _flag_line(flag: str) -> str:
    safe = escape(flag)
    if flag.endswith(":unavailable"):
        return f"{ICON_UNAVAILABLE} {safe}"
    if flag == "check_lp_lock":
        return f"{ICON_LP_UNLOCKED} {safe}"
    return safe


def _lp_section(context: dict[str, object] | None, lp_flags: list[str]) -> list[str]:
    body: list[str] = []
    if context and context.get("creator"):
        body.append(_lp_history_line(str(context.get("lp_stayed_shown", "unavailable"))))
    body.extend(_flag_line(flag) for flag in lp_flags)
    if not lp_flags:
        body.append(f"{ICON_LP_LOCKED} LP locked")
    if not body:
        return []
    return ["", ICON_SECTION, "<b>LP</b>", *body]


def _lp_history_line(shown: str) -> str:
    label = "LP stayed locked, since tracking started"
    if shown.startswith("unavailable"):
        return f"{ICON_UNAVAILABLE} {label}: {escape(shown)}"
    icon = _lp_measured_icon(shown)
    prefix = f"{icon} " if icon else ""
    return f"{prefix}{label}: {_code(shown)}"


def _lp_measured_icon(shown: str) -> str:
    left, sep, right = shown.partition(" of ")
    if not sep or not left.isdigit() or not right.isdigit():
        return ICON_LP_LOCKED
    stayed = int(left)
    total = int(right)
    if total > 0 and stayed == total:
        return ICON_LP_LOCKED
    if stayed == 0:
        return ICON_LP_UNLOCKED
    return ""


def _reading(value: object) -> str:
    shown = str(value)
    if shown.startswith("unavailable"):
        return f"{ICON_UNAVAILABLE} {escape(shown)}"
    return _code(shown)


def _code(value: object) -> str:
    return f"<code>{escape(str(value))}</code>"


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


def format_share(value: Decimal) -> str:
    percent = (value * Decimal(100)).quantize(Decimal("1"))
    return f"{percent}%"


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
    return (
        f"<code>{escape(value)}</code>\n"
        f'<a href="https://solscan.io/{kind}/{href}">Solscan</a>'
    )
