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


def channel_page(*, bot_url: str | None) -> str:
    """Public page linked from the Telegram channel. No live market data."""
    if bot_url:
        personal = (
            "<p>Свой порог, если общая лента слишком широкая:</p>"
            f'<a class="button" href="{escape(bot_url, quote=True)}">Открыть бота</a>'
            "<p>В боте задаются min_score и max_risk. Канал продолжает публиковать общую ленту.</p>"
        )
    else:
        personal = (
            "<p>Свой порог настраивается в боте командой /settings. "
            "Ссылка появится здесь, когда задан TELEGRAM_BOT_USERNAME.</p>"
        )
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SignalForge — лента сигналов</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #1c1915;
      --muted: #5c564c;
      --paper: #f4f0e6;
      --card: #fffdf8;
      --line: #e3d9c8;
      --accent: #9a3412;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Iowan Old Style", "Palatino Linotype", serif;
      background: var(--paper);
      color: var(--ink);
      line-height: 1.45;
    }}
    main {{
      max-width: 40rem;
      margin: 0 auto;
      padding: 2.5rem 1.25rem 4rem;
    }}
    h1 {{
      font-size: 2.4rem;
      line-height: 1;
      margin: 0 0 0.75rem;
      letter-spacing: -0.03em;
    }}
    h2 {{
      font-size: 1.25rem;
      margin: 2.25rem 0 0.6rem;
    }}
    p, li {{ font-size: 1.05rem; }}
    .lede {{ font-size: 1.2rem; margin-top: 0; }}
    .muted {{ color: var(--muted); }}
    .bubble {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 1rem 1rem 1rem 0.25rem;
      padding: 1rem 1.1rem;
      margin: 1rem 0;
    }}
    .bubble strong {{ font-family: ui-sans-serif, system-ui, sans-serif; }}
    .score {{
      font-family: ui-sans-serif, system-ui, sans-serif;
      font-size: 1.8rem;
      margin: 0.4rem 0;
    }}
    .note {{
      border-left: 3px solid var(--accent);
      padding-left: 0.9rem;
      color: var(--accent);
    }}
    a {{ color: var(--accent); }}
    .button {{
      display: inline-block;
      margin: 0.5rem 0 0.25rem;
      padding: 0.7rem 1rem;
      background: var(--ink);
      color: var(--paper);
      text-decoration: none;
      border-radius: 999px;
      font-family: ui-sans-serif, system-ui, sans-serif;
      font-size: 0.95rem;
    }}
  </style>
</head>
<body>
  <main>
    <p class="muted">SignalForge</p>
    <h1>Сигнал в канал, когда покупка проходит порог.</h1>
    <p class="lede">Бот смотрит покупки отслеживаемых кошельков на Solana, считает известные ончейн-проверки и публикует одну запись в канал. Отдельно в личку можно получать ту же ленту со своим порогом.</p>

    <h2>Что увидит подписчик канала</h2>
    <p>Один пост на токен. Ниже пример того, как выглядит сообщение. Это иллюстрация, не текущий рынок.</p>
    <div class="bubble">
      <strong>BONK</strong><br>
      <span class="muted">mint на Solscan</span>
      <p class="score">72.5</p>
      <div>risk_score: 85</div>
      <div>risk_flags: check_funder_cluster_exposure</div>
      <p><strong>{escape(BAIT_WARNING)}</strong></p>
      <div>{escape(DISCLAIMER)}</div>
    </div>
    <p><strong>72.5</strong> — composite score. Пост создаётся, только если он строго выше 50. <strong>risk_score</strong> — сколько известных проверок прошло: 100 значит, что они прошли, и не значит, что токен безопасен. Флаги — какие проверки не прошли. Строка про bait появляется, если к моменту отправки уже есть такой флаг.</p>

    <h2>Как подписаться</h2>
    <p>Подписка на канал уже достаточна. Общая лента приходит туда сама, без команд.</p>
    {personal}

    <h2>Чего здесь нет</h2>
    <p class="note">Это не финансовый совет. SignalForge не хранит ключи и не покупает, не продаёт и не отправляет сделки от имени подписчика.</p>
    <p class="muted">Проверки покрывают mint authority, freeze authority, LP на Raydium, концентрацию холдеров и общий источник финансирования. Отсутствие флага не доказывает, что другой схемы нет.</p>
  </main>
</body>
</html>
"""
