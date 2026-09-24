"""Runtime settings and JSON logging.

Secrets come only from the environment or a local .env file. Nothing in this
module embeds API keys, RPC URLs, or bot tokens.
"""

from __future__ import annotations

import logging
import sys
from functools import lru_cache
from typing import Literal

import structlog
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def configure_logging(log_level: str) -> None:
    """Send application logs to stdout as one JSON object per line."""
    level = getattr(logging, log_level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )


class Settings(BaseSettings):
    """Process configuration. Field names map to the variables in .env.example."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str
    redis_url: str
    helius_api_key: SecretStr = Field(default=SecretStr(""))
    helius_webhook_secret: SecretStr = Field(default=SecretStr(""))
    helius_api_base_url: str = "https://api.helius.xyz"
    helius_rpc_url: str = "https://mainnet.helius-rpc.com"
    telegram_bot_token: SecretStr = Field(default=SecretStr(""))
    anthropic_api_key: SecretStr = Field(default=SecretStr(""))
    twitter_bearer_token: SecretStr = Field(default=SecretStr(""))
    environment: Literal["dev", "prod"] = "dev"
    log_level: LogLevel = "INFO"

    @field_validator("database_url")
    @classmethod
    def database_url_must_be_async(cls, value: str) -> str:
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError(
                "DATABASE_URL must use the postgresql+asyncpg driver"
            )
        return value

    @field_validator("redis_url")
    @classmethod
    def redis_url_must_be_redis(cls, value: str) -> str:
        if not value.startswith(("redis://", "rediss://")):
            raise ValueError("REDIS_URL must start with redis:// or rediss://")
        return value

    @field_validator("helius_api_base_url", "helius_rpc_url")
    @classmethod
    def helius_url_must_be_https(cls, value: str) -> str:
        if not value.startswith("https://"):
            raise ValueError("Helius URLs must start with https://")
        return value.rstrip("/")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    configure_logging(settings.log_level)
    return settings
