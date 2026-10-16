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
    HOLDER_CONCENTRATION_MAX_SHARE,
    LP_LOCKED_MIN_SHARE,
    QUALITY_CLONE_WINDOW_HOURS,
    QUALITY_LAUNCH_SLOTS,
    QUALITY_MAX_LAUNCH_TRADES,
    QUALITY_MIN_LIQUIDITY_USD,
    QUALITY_MIN_PASSED,
    RISK_ICON_LOW_MAX,
    RISK_ICON_MED_MAX,
    SIGNAL_MAX_CREATOR_SHARE,
)
from app.services.ascii_chart import format_usd
from app.services.channel_results import HorizonResult, verdict_icon
from app.services.mascots import NEUTRAL, beside, mood_for
from app.services.track_record import TrackRecord
from app.ui_constants import (
    ICON_BAIT_WARNING,
    ICON_CANCEL,
    ICON_CONFIRM,
    ICON_CREATOR,
    ICON_DISTRIBUTION,
    ICON_HOLDERS,
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
    "Checks passed is out of 100: higher means more known checks passed. "
    "It does not mean the token is safe."
)
# risk_score is the sum of weights of passed checks, so higher is better. The
# old "risk_score" label read as "more risk" to users and to the AI review.
CHECKS_PASSED_LABEL = "Checks passed"

QUALITY_KEYS = ("quality_checks", "quality_passed", "quality_total", "market")
QUALITY_GROUPS = (
    ("contract", ICON_LP_LOCKED, "Contract"),
    ("holders", ICON_CREATOR, "Holders"),
    ("market", ICON_HOLDERS, "Market"),
)
QUALITY_STATUS_ICONS = {"pass": ICON_CONFIRM, "fail": ICON_CANCEL, "unavailable": ICON_UNAVAILABLE}
QUALITY_RULE = (
    "Rule: mint, freeze, LP, and creator share must pass, "
    f"and at least {QUALITY_MIN_PASSED} of 12 lines overall. "
    f"{ICON_UNAVAILABLE} means no data and counts as not passed."
)
QUALITY_NOTE = "Safety filter only. It does not predict price."
HOW_CHECKS_BUTTON = "🔍 How checks work"
CHANNEL_CARD_BUTTON = "🔍 Full checklist in the bot"


def channel_signal_text(
    *,
    symbol: str | None,
    mint: str,
    checks: list[dict[str, object]],
    triggered_at: datetime,
    market_cap: Decimal | None,
    market: dict[str, object] | None = None,
) -> str:
    """Public post at the moment of the signal: mascot, start figures, checklist bars."""
    passed = sum(1 for row in checks if row.get("status") == "pass")
    total = len(checks)
    icon = ICON_RISK_LOW if passed == total else ICON_RISK_MED
    when = triggered_at.astimezone(timezone.utc).strftime("%H:%M UTC, %d %b")
    return "\n".join(
        [
            f"{icon} <b>{escape(symbol or 'no ticker')}</b> — {passed}/{total} checks passed",
            f"Signal: {when}",
            f"<pre>{escape(summary_block(mint, checks, market_cap, market))}</pre>",
            _code(mint),
            "Results follow as replies: 1h · 24h · 7d. Every signal, wins and losses.",
            f"<i>{QUALITY_NOTE}</i>",
        ]
    )


def summary_block(
    mint: str,
    checks: list[dict[str, object]],
    market_cap: Decimal | None,
    market: dict[str, object] | None,
) -> str:
    """Mascot beside the start figures, then the safety bar and one bar per group.

    Plain text for a ``<pre>`` block; callers escape it. Same block in the
    channel post and the bot card, so the two read alike.
    """
    passed = sum(1 for row in checks if row.get("status") == "pass")
    total = len(checks)
    snapshot = market or {}
    liquidity = _decimal_or_none(snapshot.get("liquidity_usd"))
    buys, sells = snapshot.get("buys_5m"), snapshot.get("sells_5m")
    figures = [
        f"MCap  {'—' if market_cap is None else format_usd(market_cap)}",
        f"Liq   {'—' if liquidity is None else format_usd(liquidity)}",
        "5m    "
        + (f"{buys} buy/{sells} sell" if isinstance(buys, int) and isinstance(sells, int) else "—"),
    ]
    block = [beside(mint, NEUTRAL, figures), ""]
    block.append(f"{'Safety':<9}{'▰' * passed}{'▱' * (total - passed)} {passed}/{total}")
    for group, _icon, title in QUALITY_GROUPS:
        members = [row for row in checks if row.get("group") == group]
        if not members:
            continue
        ok = sum(1 for row in members if row.get("status") == "pass")
        block.append(f"{title:<9}{'█' * ok}{'░' * (len(members) - ok)} {ok}/{len(members)}")
    return "\n".join(block)


def _decimal_or_none(raw: object) -> Decimal | None:
    if raw is None:
        return None
    try:
        return Decimal(str(raw))
    except ArithmeticError:
        return None


def channel_result_text(
    *,
    symbol: str | None,
    mint: str,
    horizon: str,
    result: HorizonResult,
) -> str:
    change = _signed_percent(result.change)
    if result.peak > result.base:
        peak = f"Peak {_signed_percent(result.peak / result.base - 1)} at {_duration(result.peak_minutes)}"
    else:
        peak = "Never above start"
    figures = [
        f"{horizon:<5}{change}",
        f"{format_usd(result.base)} → {format_usd(result.end)}",
        peak,
    ]
    lines = [
        f"{verdict_icon(result.change)} <b>{escape(symbol or 'no ticker')}</b> · "
        f"{horizon} after signal: <b>{change}</b>",
        f"<pre>{escape(beside(mint, mood_for(result.change), figures))}</pre>",
    ]
    if result.chart:
        lines.append(f"<pre>{escape(result.chart)}</pre>")
    return "\n".join(lines)


def _signed_percent(change: Decimal) -> str:
    return f"{(change * 100).quantize(Decimal('1')):+}%"


def _duration(minutes: float) -> str:
    if minutes < 120:
        return f"{int(round(minutes))}m"
    if minutes < 48 * 60:
        return f"{int(round(minutes / 60))}h"
    return f"{round(minutes / (24 * 60), 1)}d"
TRACK_RECORD_BUTTON = f"{ICON_STATUS} Track record"


def checks_explained_text() -> str:
    holders = _share_text(HOLDER_CONCENTRATION_MAX_SHARE)
    lp = _share_text(LP_LOCKED_MIN_SHARE)
    liquidity = f"${QUALITY_MIN_LIQUIDITY_USD:,.0f}"
    return "\n".join(
        [
            "🔍 <b>How checks work</b>",
            "",
            "Each line is checked on its own. "
            f"{ICON_CONFIRM} passed, {ICON_CANCEL} failed, {ICON_UNAVAILABLE} no data. "
            "No data never counts as passed.",
            "",
            f"{ICON_LP_LOCKED} <b>Contract</b>",
            "• <b>Mint authority</b>: nobody can print more tokens. Required.",
            "• <b>Freeze authority</b>: nobody can freeze your wallet. Required.",
            f"• <b>LP</b>: at least {lp} of pool LP is burned or locked, so the pool "
            "cannot be pulled. Required.",
            "• <b>Token extensions</b>: no transfer fee, transfer hook, permanent "
            "delegate, or pause switch.",
            "",
            f"{ICON_CREATOR} <b>Holders</b>",
            f"• <b>Top-10 holders</b> own at most {holders}, pool excluded.",
            "• <b>Funder cluster</b>: top holders were not all funded by one wallet.",
            "• <b>Creator share</b>: creator and wallets funded by the same source own "
            f"at most {_share_text(SIGNAL_MAX_CREATOR_SHARE)}. Required.",
            f"• <b>Launch trades</b>: at most {QUALITY_MAX_LAUNCH_TRADES} trades in the "
            f"first {QUALITY_LAUNCH_SLOTS} blocks. More usually means bundled snipers.",
            "",
            f"{ICON_HOLDERS} <b>Market</b> (DexScreener at the moment of the signal)",
            f"• <b>Liquidity</b> of at least {liquidity}.",
            "• <b>Buys over sells</b> in the last 5 minutes.",
            f"• <b>Unique ticker</b>: no other token with the same ticker in "
            f"{QUALITY_CLONE_WINDOW_HOURS}h.",
            "• <b>Socials listed</b> in the token metadata. Not verified: anyone can "
            "paste a famous account.",
            "",
            f"<b>Signal rule</b>: every required line passes and at least "
            f"{QUALITY_MIN_PASSED} of 12 lines pass.",
            "Thresholds are hand-picked starting values and may change once the "
            "track record shows which lines matter.",
            "",
            f"<i>{QUALITY_NOTE} Most memecoins lose most of their value.</i>",
        ]
    )


def track_record_text(record: TrackRecord) -> str:
    lines = [f"{ICON_STATUS} <b>Track record</b>", ""]
    if record.signals == 0:
        lines.append("No signals yet.")
        return "\n".join(lines)
    since = (
        record.first_signal_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
        if record.first_signal_at is not None
        else "unknown"
    )
    lines.append(f"Signals sent: {_code(record.signals)} (since {since})")
    lines.append(
        "Change is measured from the price at the signal. Signals without both "
        "prices are left out, not counted."
    )
    lines.append("")
    for stats in record.horizons:
        label = f"After {stats.horizon}"
        if stats.measured == 0:
            lines.append(f"{ICON_UNAVAILABLE} {label}: not measured yet")
            continue
        change = (
            "—"
            if stats.median_change is None
            else f"{(stats.median_change * 100).quantize(Decimal('1')):+}%"
        )
        lines.append(
            f"<b>{label}</b>: {stats.measured} measured · "
            f"{stats.collapsed} fell 90%+ · {stats.doubled} doubled · median {change}"
        )
    lines.extend(["", "<i>Past results do not predict future ones.</i>"])
    return "\n".join(lines)


def _share_text(share: Decimal) -> str:
    return f"{(share * Decimal(100)).quantize(Decimal('1'))}%"


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
        "🔍 /checks — how the 12 checks work\n"
        f"{ICON_STATUS} /track — what happened after past signals\n"
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
    checklist = quality_lines(context, symbol=symbol, mint=mint)
    if checklist is not None:
        lines = [checklist[0], solscan_token(mint), *checklist[1:]]
        return "\n".join(
            _checklist_tail(
                lines,
                context=context,
                bait_warning=bait_warning,
                creator_over_limit=creator_over_limit,
                ai_review=ai_review,
            )
        )
    risk = "—" if risk_score is None else format_score(risk_score)
    other_flags, lp_flags = _split_lp_flags(risk_flags)
    lines = [
        f"{risk_icon(risk_score)} <b>{escape(symbol or 'no ticker')}</b>",
        solscan_token(mint),
        "",
        ICON_SECTION,
        f"{CHECKS_PASSED_LABEL}: {_code(risk)}/100",
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


def quality_lines(
    context: dict[str, object] | None,
    *,
    symbol: str | None,
    mint: str,
) -> list[str] | None:
    """Header, summary block, and the full grouped checklist; None before checklists."""
    raw = context.get("quality_checks") if isinstance(context, dict) else None
    if not isinstance(context, dict) or not isinstance(raw, list) or not raw:
        return None
    rows = [row for row in raw if isinstance(row, dict)]
    passed = sum(1 for row in rows if row.get("status") == "pass")
    total = len(rows)
    icon = ICON_RISK_LOW if passed == total else ICON_RISK_MED
    name = escape(symbol or "no ticker")
    raw_market = context.get("market")
    market = raw_market if isinstance(raw_market, dict) else {}
    cap = _decimal_or_none(market.get("market_cap_usd"))
    lines = [
        f"{icon} <b>{name}</b> — <b>{passed}/{total}</b> checks passed",
        f"<pre>{escape(summary_block(mint, rows, cap, market))}</pre>",
    ]
    for group, group_icon, title in QUALITY_GROUPS:
        members = [row for row in rows if row.get("group") == group]
        if not members:
            continue
        lines.extend(["", f"{group_icon} <b>{title}</b>"])
        for row in members:
            status_icon = QUALITY_STATUS_ICONS.get(str(row.get("status")), ICON_UNAVAILABLE)
            lines.append(f"{status_icon} {escape(str(row.get('text', '')))}")
    lines.extend(["", f"<i>{escape(QUALITY_RULE)}</i>", f"<i>{QUALITY_NOTE}</i>"])
    return lines


def creator_context(context: dict[str, object] | None) -> dict[str, object] | None:
    """The stored context without the checklist keys, or None when nothing is left."""
    if not isinstance(context, dict):
        return None
    rest = {key: value for key, value in context.items() if key not in QUALITY_KEYS}
    return rest or None


def _short_creator_lines(context: dict[str, object], *, over_limit: bool) -> list[str]:
    """Creator wallet, then past-launch lines only when there is a past launch.

    The creator-share line already sits in the checklist, so it is not repeated.
    """
    lines = [f"{ICON_CREATOR} Creator: {_code(context['creator'])}"]
    launches = context.get("launches_since_tracking")
    if isinstance(launches, int) and launches > 0:
        lines.append(f"Previous launches since tracking started: {_code(launches)}")
        drop = str(context.get("price_drop_shown", "unavailable"))
        if not drop.startswith("unavailable"):
            lines.append(f"First-hour drop over 90%: {_code(drop)}")
        stayed = str(context.get("lp_stayed_shown", "unavailable"))
        if not stayed.startswith("unavailable"):
            lines.append(_lp_history_line(stayed))
    if over_limit:
        lines.append(f"{ICON_DISTRIBUTION} Creator-funder supply is above your limit.")
    return lines


def _checklist_tail(
    lines: list[str],
    *,
    context: dict[str, object] | None,
    bait_warning: bool,
    creator_over_limit: bool,
    ai_review: str | None,
) -> list[str]:
    if bait_warning:
        lines.extend(["", f"<b>{BAIT_WARNING}</b>"])
    creator = creator_context(context)
    if creator is not None and creator.get("creator"):
        lines.extend(["", *_short_creator_lines(creator, over_limit=creator_over_limit)])
    lines.extend(_review_lines(ai_review))
    lines.extend(["", DISCLAIMER])
    return lines


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
    checklist = quality_lines(context, symbol=symbol, mint=mint)
    if checklist is not None:
        lines = [checklist[0], solscan_token(mint)]
        if triggered_at is not None:
            when = triggered_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            lines.append(f"Signal sent {_code(when)}. Lines are as of that moment.")
        lines.extend(checklist[1:])
        return "\n".join(
            _checklist_tail(
                lines,
                context=context,
                bait_warning=bait_warning,
                creator_over_limit=creator_over_limit,
                ai_review=ai_review,
            )
        )
    risk = "—" if risk_score is None else format_score(risk_score)
    other_flags, lp_flags = _split_lp_flags(flags)
    lines = [
        f"{risk_icon(risk_score)} <b>{escape(symbol or 'no ticker')}</b>",
        solscan_token(mint),
        "",
        ICON_SECTION,
    ]
    if triggered_at is not None:
        when = triggered_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines.append(_code(when))
    lines.extend(
        [
            f"{CHECKS_PASSED_LABEL}: {_code(risk)}/100",
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
