# SignalForge

Сервис мониторинга и риск-анализа мемкоинов на Solana.

Источник истины по правилам: [`AGENTS.md`](AGENTS.md). Не ослабляй запреты. Секреты только через `.env` и pydantic Settings. Авто-исполнение сделок вне скоупа.

## Как запустить локально

1. Скопируй переменные: `cp .env.example .env`
2. Подними Postgres, Redis и API: `docker-compose up --build`
3. Проверь health: `curl localhost:8000/health`

Ожидаемый ответ: `{"status":"ok","db":"connected","redis":"connected"}`.

`docker-compose.yml` сам задаёт `DATABASE_URL` и `REDIS_URL` на сервисы `postgres` и `redis`. Файл `.env` для этого шага не редактируется.

## Тесты

```bash
uv sync --group dev
uv run pytest tests/test_health.py
```

`tests/test_health.py` использует явно помеченные mock-фикстуры драйверов. Живые Postgres и Redis проверяет `GET /health` у контейнера.

## Правила в каждом промпте

| Файл | Зачем |
| --- | --- |
| [`AGENTS.md`](AGENTS.md) | Канон. |
| [`.cursorrules`](.cursorrules) | Legacy always-on правила. |
| [`.cursor/rules/signalforge.mdc`](.cursor/rules/signalforge.mdc) | Cursor Rules с `alwaysApply: true`. |

Если меняешь запрет, практику или стек — правь все три файла одинаково.

## Стек

Python 3.11+, FastAPI, SQLAlchemy 2.0 (async) + Alembic, PostgreSQL 16, Redis 7, httpx, structlog, pytest + pytest-asyncio, Docker Compose. Зависимости закреплены в `pyproject.toml` и `uv.lock`.
