"""Settings load from the environment and logs are JSON. No secrets are hardcoded."""

from __future__ import annotations

import json

import pytest
import structlog

from app.config import Settings, configure_logging


def test_settings_read_env_and_hide_secrets(monkeypatch: object) -> None:
    monkeypatch.setenv(  # type: ignore[attr-defined]
        "DATABASE_URL",
        "postgresql+asyncpg://signalforge:signalforge@db:5432/signalforge",
    )
    monkeypatch.setenv("REDIS_URL", "redis://cache:6379/0")  # type: ignore[attr-defined]
    monkeypatch.setenv("ENVIRONMENT", "prod")  # type: ignore[attr-defined]
    monkeypatch.setenv("LOG_LEVEL", "WARNING")  # type: ignore[attr-defined]
    monkeypatch.setenv("HELIUS_API_KEY", "not-a-real-key")  # type: ignore[attr-defined]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "not-a-real-token")  # type: ignore[attr-defined]

    settings = Settings(_env_file=None)

    assert settings.database_url.endswith("@db:5432/signalforge")
    assert settings.redis_url == "redis://cache:6379/0"
    assert settings.environment == "prod"
    assert settings.log_level == "WARNING"
    rendered = repr(settings)
    assert "not-a-real-key" not in rendered
    assert "not-a-real-token" not in rendered


def test_configure_logging_emits_json(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    structlog.get_logger("signalforge.test").info("hello", component="config")

    captured = capsys.readouterr()
    text = captured.out.strip() or captured.err.strip()
    line = text.splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "hello"
    assert payload["component"] == "config"
    assert payload["level"] == "info"
