# SignalForge

SignalForge — проект с зафиксированным стеком и жёсткими правилами разработки.
Источник истины по правилам: [`AGENTS.md`](AGENTS.md).

## Как правила попадают в каждый промпт

Один и тот же канон продублирован в трёх местах, которые Cursor читает автоматически:

| Файл | Зачем |
| --- | --- |
| [`AGENTS.md`](AGENTS.md) | Канон. Cursor подмешивает его в контекст агента. |
| [`.cursorrules`](.cursorrules) | Legacy always-on правила (как просили: AGENTS.md «как .cursorrules»). |
| [`.cursor/rules/signalforge.mdc`](.cursor/rules/signalforge.mdc) | Современные Cursor Rules с `alwaysApply: true`. |

Если меняешь запрет, практику или стек — правь **все три файла одинаково**. Не ослабляй формулировки.

## Стек (не менять без согласования)

- Python 3.11+
- FastAPI
- SQLAlchemy 2.0 (async) + Alembic
- PostgreSQL 16 (+ TimescaleDB)
- Redis Streams
- python-telegram-bot v21+
- httpx (async)
- pytest + pytest-asyncio
- Docker Compose

Приватные ключи — только в зашифрованном виде. Секреты — только через `.env` и pydantic Settings. Авто-исполнение сделок на этом этапе вне скоупа.
