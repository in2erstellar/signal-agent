"""Runtime settings and JSON logging.

Secrets come only from the environment or a local .env file. Nothing in this
module embeds API keys, RPC URLs, or bot tokens.
"""

from __future__ import annotations

import logging
import sys
from decimal import Decimal
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
    raydium_api_base_url: str = "https://api-v3.raydium.io"
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

    @field_validator("helius_api_base_url", "helius_rpc_url", "raydium_api_base_url")
    @classmethod
    def https_base_url(cls, value: str) -> str:
        if not value.startswith("https://"):
            raise ValueError("Service base URLs must start with https://")
        return value.rstrip("/")


# Points added to risk_score when a check passes. 100 means every check passed.
# A perfect score is not a statement that the token is safe; it only means
# none of these known patterns fired. Recalibrate here, not inside RiskScanner.
RISK_CHECK_WEIGHTS: dict[str, int] = {
    "check_mint_authority": 20,
    "check_freeze_authority": 25,
    "check_lp_lock": 25,
    "check_holder_concentration": 15,
    "check_funder_cluster_exposure": 15,
}

# Top-10 share of supply, LP vaults excluded. Above this, the check does not pass.
HOLDER_CONCENTRATION_MAX_SHARE = Decimal("0.50")

# Funder-cluster confidence is a 0-1 heuristic from funder_graph.confidence_for.
# Strictly above this value is a sybil-style risk flag.
FUNDER_CLUSTER_RISK_CONFIDENCE = Decimal("0.5")

# Share of current LP supply that must sit at a burn address or known lock program.
LP_LOCKED_MIN_SHARE = Decimal("0.95")

# Copy-trade bait heuristics. Recalibrate here; BaitDetector only reads them.
# Volume spike is strict: exactly 3x the token's own baseline is not a flag.
BAIT_VOLUME_SPIKE_MULTIPLE = Decimal("3")
# Share of the SOL-notional position sold inside the sell window. Inclusive.
BAIT_MIN_SOLD_SHARE = Decimal("0.50")
# Other wallets' buys in this many minutes after the tracked buy.
BAIT_INFLUX_WINDOW_MINUTES = 5
# Sells count only inside [start, end] minutes after the tracked buy.
BAIT_SELL_WINDOW_START_MINUTES = 15
BAIT_SELL_WINDOW_END_MINUTES = 60
# Phase-1 schedules the recheck at now + this delay. The worker is separate.
BAIT_RECHECK_DELAY_MINUTES = 60
# Average other-wallet buy volume is taken over this lookback, then scaled
# to one influx window. A zero lookback total does not produce a multiple.
BAIT_BASELINE_LOOKBACK_MINUTES = 60
BAIT_PENDING_STREAM = "signalforge:bait:pending"

if BAIT_BASELINE_LOOKBACK_MINUTES <= 0 or BAIT_INFLUX_WINDOW_MINUTES <= 0:
    raise RuntimeError("bait volume windows must be positive")
if BAIT_SELL_WINDOW_START_MINUTES >= BAIT_SELL_WINDOW_END_MINUTES:
    raise RuntimeError("bait sell window must have a positive length")
if BAIT_RECHECK_DELAY_MINUTES < BAIT_SELL_WINDOW_END_MINUTES:
    raise RuntimeError("bait recheck must not be due before the sell window ends")

# Redis names for the pipeline. Bait jobs are delayed, so they live in a
# sorted set scored by scheduled_at. Risk scans are immediate, so they use a stream.
RISK_SCAN_QUEUE = "risk_scan_queue"
BAIT_CHECK_QUEUE = "bait_check_queue"
RISK_SCAN_GROUP = "risk_scan_workers"

BAIT_POLL_INTERVAL_SECONDS = 60
SIGNAL_FUSION_INTERVAL_SECONDS = 30
SIGNAL_RISK_SCAN_MAX_AGE_MINUTES = 5

# composite = risk * 0.5 + wallet * 0.3 - bait_penalty * 0.2
# A score equal to the threshold does not create a signal.
SIGNAL_RISK_WEIGHT = Decimal("0.5")
SIGNAL_WALLET_WEIGHT = Decimal("0.3")
SIGNAL_BAIT_WEIGHT = Decimal("0.2")
SIGNAL_THRESHOLD = Decimal("50")

if sum(RISK_CHECK_WEIGHTS.values()) != 100:
    raise RuntimeError("RISK_CHECK_WEIGHTS must sum to 100")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    configure_logging(settings.log_level)
    return settings
