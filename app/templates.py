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
        "SignalForge — приватный бот. Он следит за покупками кошельков, "
        "которые вы сами добавили, и пишет в этот чат, когда балл проходит порог.\n"
        "\n"
        "/watch адрес — добавить кошелёк\n"
        "/wallets — список\n"
        "/unwatch адрес — убрать кошелёк без сделок\n"
        "/signals — последние сигналы\n"
        "/risk mint — сохранённый скан\n"
        "/settings — порог\n"
        "/status — настройки и алерты за 24 часа\n"
        "\n"
        "Бот ничего не покупает и не продаёт. "
        "/watch дописывает адрес в уже созданный Helius webhook.\n"
        f"{DISCLAIMER}"
    )


def private_denied_text() -> str:
    return "Этот бот закрыт. Ваш Telegram id нет в TELEGRAM_ADMIN_IDS."


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


def watch_usage_text() -> str:
    return "Формат: /watch адрес [метка]"


def watch_invalid_text() -> str:
    return "Адрес не похож на Solana-адрес: 32–44 символа base58."


def watch_saved_text(address: str, label: str | None) -> str:
    name = f" ({label})" if label else ""
    return f"Кошелёк добавлен и подписка на Helius активна{name}: {address}"


def watch_subscribe_failed_text() -> str:
    return "Не удалось подписаться на мониторинг, попробуйте позже."


def watch_webhook_missing_text() -> str:
    return (
        "HELIUS_WEBHOOK_ID не задан. Создайте webhook один раз в Helius Dashboard "
        "и запишите его id в окружение. Кошелёк не сохранён."
    )


def unwatch_unsubscribe_failed_text() -> str:
    return "Не удалось снять подписку Helius. Кошелёк оставлен в списке, попробуйте позже."


def watch_duplicate_text() -> str:
    return "Этот кошелёк уже в списке."


def unwatch_done_text(address: str) -> str:
    return f"Кошелёк убран: {address}"


def unwatch_missing_text() -> str:
    return "Такого кошелька в списке нет."


def unwatch_blocked_text() -> str:
    return "У кошелька уже есть сделки, удалить его нельзя."


def wallets_text(lines: list[str]) -> str:
    if not lines:
        return "Список пуст. Добавьте адрес командой /watch."
    return "Кошельки:\n" + "\n".join(lines)


def signals_text(lines: list[str]) -> str:
    if not lines:
        return "Сигналов пока нет."
    return "Последние сигналы:\n" + "\n".join(lines)


def risk_missing_text() -> str:
    return "Сохранённого скана по этому mint нет."


def risk_text(*, mint: str, risk_score: int, flags: list[str]) -> str:
    listed = ", ".join(flags) if flags else "нет"
    return (
        f"{mint}\n"
        f"risk_score: {risk_score}\n"
        f"flags: {listed}\n"
        "Это сохранённый скан, не новый запрос в сеть. "
        "100 значит, что известные проверки прошли, и не значит, что токен безопасен.\n"
        f"{DISCLAIMER}"
    )


