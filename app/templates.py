"""Telegram copy. Workers and handlers call these functions; they do not
embed user-facing sentences.
"""

from __future__ import annotations

from decimal import Decimal
from html import escape


DISCLAIMER = "Not financial advice. DYOR."
BAIT_WARNING = "⚠️ Обнаружен паттерн copy-trade baiting"


def welcome_text() -> str:
    return (
        "SignalForge следит за покупками отслеживаемых кошельков на Solana "
        "и присылает сигнал, когда композитный балл токена проходит ваш порог.\n"
        "\n"
        "/settings — порог composite score и фильтр риска\n"
        "/status — текущие настройки и сколько алертов пришло за 24 часа\n"
        "\n"
        "Бот ничего не покупает и не продаёт.\n"
        f"{DISCLAIMER}"
    )


def unregistered_text() -> str:
    return "Сначала отправьте /start, чтобы подписаться на сигналы."


def settings_text(*, min_score: Decimal, max_risk: Decimal) -> str:
    return (
        f"min_score: {min_score}\n"
        "Алерт приходит, если composite_score не ниже этого числа.\n"
        "\n"
        f"max_risk: {max_risk}\n"
        "Обратный фильтр: алерт приходит, если 100 − risk_score не выше этого числа. "
        "Большее значение пропускает больше токенов. 100 принимает любой балл, "
        "0 — только токены, у которых все известные проверки прошли.\n"
        "\n"
        "risk_score — балл проверок, не утверждение что токен безопасен."
    )


def min_score_label(value: Decimal, *, selected: bool) -> str:
    prefix = "● " if selected else ""
    return f"{prefix}min {value}"


def max_risk_label(value: Decimal, *, selected: bool) -> str:
    prefix = "● " if selected else ""
    return f"{prefix}risk {value}"


def status_text(
    *,
    min_score: Decimal,
    max_risk: Decimal,
    active: bool,
    alerts_24h: int,
) -> str:
    state = "включён" if active else "выключен: вы заблокировали бота"
    return (
        f"Бот: {state}\n"
        f"min_score: {min_score}\n"
        f"max_risk: {max_risk}\n"
        f"Алертов за 24 часа: {alerts_24h}"
    )


def alert_text(
    *,
    symbol: str | None,
    mint: str,
    composite_score: Decimal,
    risk_score: Decimal | None,
    risk_flags: list[str],
    bait_warning: bool,
) -> str:
    name = escape(symbol or "без тикера")
    short = escape(_short_mint(mint))
    href = escape(mint, quote=True)
    flags = ", ".join(escape(flag) for flag in risk_flags) if risk_flags else "нет"
    risk = "—" if risk_score is None else _format_score(risk_score)
    lines = [
        f"<b>{name}</b>",
        f'<a href="https://solscan.io/token/{href}">{short}</a>',
        "",
        f"<b>{_format_score(composite_score)}</b>",
        f"risk_score: {risk}",
        f"risk_flags: {flags}",
    ]
    if bait_warning:
        lines.extend(["", f"<b>{BAIT_WARNING}</b>"])
    lines.extend(["", DISCLAIMER])
    return "\n".join(lines)


def _short_mint(mint: str) -> str:
    if len(mint) <= 10:
        return mint
    return f"{mint[:4]}…{mint[-4:]}"


def _format_score(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text
