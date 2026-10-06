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

# Production model on Groq. Override with AI_REVIEW_MODEL when Groq retires it.
AI_REVIEW_MODEL = "openai/gpt-oss-120b"


def configure_logging(log_level: str) -> None:
    """Send application logs to stdout as one JSON object per line."""
    level = getattr(logging, log_level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    # httpx logs the full URL. Telegram requests put the bot token in that URL.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

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
    # Existing webhook created once in the Helius dashboard. Empty disables /watch.
    helius_webhook_id: str = ""
    helius_api_base_url: str = "https://api.helius.xyz"
    helius_rpc_url: str = "https://mainnet.helius-rpc.com"
    raydium_api_base_url: str = "https://api-v3.raydium.io"
    telegram_bot_token: SecretStr = Field(default=SecretStr(""))
    # Comma-separated Telegram user ids. Empty means there are no admins.
    telegram_admin_ids: str = ""
    public_api_key: SecretStr = Field(default=SecretStr(""))
    api_rate_limit_per_minute: int = 60
    anthropic_api_key: SecretStr = Field(default=SecretStr(""))
    twitter_bearer_token: SecretStr = Field(default=SecretStr(""))
    # Free-tier review provider. Empty disables live calls; the worker does not invent text.
    groq_api_key: SecretStr = Field(default=SecretStr(""))
    xai_api_key: SecretStr = Field(default=SecretStr(""))
    # Groq retires model ids on a short notice. Override this without a code change.
    ai_review_model: str = AI_REVIEW_MODEL
    environment: Literal["dev", "prod"] = "dev"
    log_level: LogLevel = "INFO"
    # Optional. subscribeNewToken works without a key. A key is not a license
    # to open a second socket: PumpPortal bans the extra connection for an hour.
    pumpportal_api_key: SecretStr = Field(default=SecretStr(""))
    pumpportal_ws_url: str = "wss://pumpportal.fun/api/data"

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

    @field_validator("api_rate_limit_per_minute")
    @classmethod
    def rate_limit_must_be_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("API_RATE_LIMIT_PER_MINUTE must be at least 1")
        return value

    @field_validator("ai_review_model")
    @classmethod
    def ai_review_model_must_be_set(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("AI_REVIEW_MODEL must not be empty")
        return stripped

    @field_validator("helius_api_base_url", "helius_rpc_url", "raydium_api_base_url")
    @classmethod
    def https_base_url(cls, value: str) -> str:
        if not value.startswith("https://"):
            raise ValueError("Service base URLs must start with https://")
        return value.rstrip("/")

    @field_validator("pumpportal_ws_url")
    @classmethod
    def wss_url(cls, value: str) -> str:
        if not value.startswith("wss://"):
            raise ValueError("PUMPPORTAL_WS_URL must start with wss://")
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
# Holder concentration and the funder walk. Acked separately from RISK_SCAN_QUEUE.
RISK_HEAVY_QUEUE = "risk_heavy_queue"
BAIT_CHECK_QUEUE = "bait_check_queue"
RISK_SCAN_GROUP = "risk_scan_workers"
RISK_HEAVY_GROUP = "risk_heavy_workers"

BAIT_POLL_INTERVAL_SECONDS = 60
SIGNAL_FUSION_INTERVAL_SECONDS = 30
OUTCOME_POLL_INTERVAL_SECONDS = 60
# One sorted set. The member suffix is the horizon. listing is not queued.
OUTCOME_QUEUE = "signalforge:outcome:pending"
OUTCOME_HORIZONS = (
    ("1h", 3600),
    ("24h", 86400),
    ("7d", 604800),
)
# Starting limit for the creator-funder supply flag. It does not change composite_score.
DEFAULT_MAX_DEV_CLUSTER_SHARE = Decimal("0.25")
DEV_CLUSTER_SHARE_STEP = Decimal("0.05")
# Hours before a recorded LP unlock. The signal card does not print this number.
UNLOCK_ALERT_LEAD_HOURS = 6
SIGNAL_RISK_SCAN_MAX_AGE_MINUTES = 5

# composite = risk * 0.5 + wallet * 0.3 - bait_penalty * 0.2
# A score equal to the threshold does not create a signal.
SIGNAL_RISK_WEIGHT = Decimal("0.5")
SIGNAL_WALLET_WEIGHT = Decimal("0.3")
SIGNAL_BAIT_WEIGHT = Decimal("0.2")
SIGNAL_THRESHOLD = Decimal("50")

# Alert fan-out. min_score is a floor on composite_score.
# max_risk is a ceiling on (100 - risk_score): a higher value admits more tokens.
ALERT_DISPATCH_INTERVAL_SECONDS = 15
ALERT_CHAT_INTERVAL_SECONDS = 1
DEFAULT_MIN_SCORE = Decimal("50")
DEFAULT_MAX_RISK = Decimal("100")
SETTINGS_STEP = Decimal("5")
SIGNAL_PAGE_SIZE = 5

# Shown under signal and risk cards. Not used on menus, settings, or confirmations.
FOOTER_DISCLAIMER = "Not financial advice. DYOR."

# Danger is 100 - risk_score. risk_score 100 means every known check passed.
# At or below LOW is green, at or below MED is yellow, above MED is red.
# A missing risk_score is treated as high danger.
RISK_ICON_LOW_MAX = Decimal("33")
RISK_ICON_MED_MAX = Decimal("66")
MIN_SCORE_CHOICES = (
    Decimal("50"),
    Decimal("60"),
    Decimal("70"),
    Decimal("80"),
)
MAX_RISK_CHOICES = (
    Decimal("20"),
    Decimal("40"),
    Decimal("60"),
    Decimal("100"),
)

if sum(RISK_CHECK_WEIGHTS.values()) != 100:
    raise RuntimeError("RISK_CHECK_WEIGHTS must sum to 100")

# How many AI reviews may start. The daily call cap is independent of the hourly
# one: 10/hour for 24 hours is 240, and 150 must stop the day before that.
# Published free-tier row for openai/gpt-oss-120b on
# https://console.groq.com/docs/rate-limits (2026-09-25): 30 RPM, 1_000 RPD,
# 8_000 TPM, 200_000 TPD. The org Limits page can override that summary.
# AI_REVIEW_DAILY_TOKEN_BUDGET is 95% of TPD so a chars/4 estimate can be low
# and still stop before the provider does. A live ELIZABETH review with
# reasoning_effort=low used 350 tokens (216 prompt, 134 completion, 27 of
# them reasoning) and finished with finish_reason stop. 190_000 // 350 = 542,
# which is above this call cap and above the 239 calls a day the hourly cap
# can reach, so 150 stays. A burst of the hourly cap at 350 tokens is 3_500,
# under the 8_000 TPM.
AI_REVIEW_HOURLY_BUDGET = 10
AI_REVIEW_DAILY_BUDGET = 150
AI_REVIEW_DAILY_TOKEN_BUDGET = 190_000
# Starting band for a review candidate. Change these here, not at the call site.
# AI_REVIEW_MIN_LIQUIDITY is Raydium pool TVL in USD, filled only after AMM
# migration. It is the wrong gate for a bonding-curve mint.
AI_REVIEW_MIN_LIQUIDITY = Decimal("5000")
# Starting gate for pre-migration mints, in SOL, from PumpPortal
# vSolInBondingCurve. This is not a USD conversion and not an equivalent of
# the 5000 figure above: there is no SOL price feed in this process, and a
# create frame's virtual reserve sits near 30 SOL for every new mint. A gate
# under that cliff means "the create frame carried a curve snapshot". Replace
# this constant when a different cliff is chosen. Do not copy the value into
# liquidity_usd.
AI_REVIEW_MIN_BONDING_CURVE_LIQUIDITY_SOL = Decimal("1")
AI_REVIEW_SCORE_MIN = Decimal("25")
AI_REVIEW_SCORE_MAX = Decimal("85")

# One process may walk at most this many funder graphs at once.
FUNDER_GRAPH_CONCURRENCY = 3
# Same wallet and the same transient funded-by error. A 403 does not use this
# counter: the free plan forbids that call, so the lookup is permanent at once.
MAX_FUNDER_LOOKUP_RETRIES = 2
# rpc: getSignaturesForAddress + getTransaction on the standard RPC.
# helius_funded_by: GET /v1/wallet/{address}/funded-by (paid plan, 100 credits).
# Switch the constant to go back to the paid call. Do not delete that client method.
FUNDER_LOOKUP_METHOD = "rpc"
# Newest-first pages of 1000. Stop after this many signatures. Hitting the cap
# means the oldest funding transfer was not reached, which is not "no funder".
# Holder wallets from the funded-by 403 sample needed 6k-30k signatures.
# https://www.helius.dev/docs/billing/credits (2026-09-25): each of
# getSignaturesForAddress and getTransaction is 1 credit. A history under
# 1000 signatures is 2 credits. Paging to this cap is 40 signature calls
# + 1 getTransaction = 41 credits. funded-by on that page is 100 credits.
MAX_FUNDING_LOOKUP_SIGNATURES = 40000
# The oldest signature sometimes lists the wallet with no SOL change.
# Walk this many oldest transactions, oldest first, until the balance rises.
MAX_FUNDING_TRANSACTION_INSPECT = 25

# PumpPortal discovery. Holder polls stay bounded; a full pending set is
# revisited over several cycles instead of one RPC burst per minute.
DISCOVERY_DOWN_ALERT_SECONDS = 300
DISCOVERY_PENDING_BATCH = 20
DISCOVERY_REVIEW_INTERVAL_SECONDS = 30
DISCOVERY_PENDING_MAX_AGE_HOURS = 24
# Holder checks prefer mints first seen inside this window. Older pending rows
# take only the batch slots the fresh pass did not fill.
DISCOVERY_FRESH_WINDOW_HOURS = 1

if AI_REVIEW_DAILY_BUDGET >= AI_REVIEW_HOURLY_BUDGET * 24:
    raise RuntimeError(
        "AI_REVIEW_DAILY_BUDGET must bind before 24 consecutive hourly budgets"
    )
if AI_REVIEW_DAILY_TOKEN_BUDGET <= 0 or AI_REVIEW_DAILY_TOKEN_BUDGET >= 200_000:
    raise RuntimeError("AI_REVIEW_DAILY_TOKEN_BUDGET must sit strictly under 200000 TPD")

# groq: Groq free tier, enough daily requests for the cap above.
# grok: xAI. Switching this value changes the provider; call sites stay the same.
AI_REVIEW_PROVIDER = "groq"
AI_REVIEW_STALE_HOURS = 24
AI_REVIEW_ALERT_WAIT_SECONDS = 120
AI_REVIEW_QUEUE = "ai_review_queue"
AI_REVIEW_GROUP = "ai_review_workers"

if AI_REVIEW_PROVIDER not in {"groq", "grok"}:
    raise RuntimeError("AI_REVIEW_PROVIDER must be groq or grok")
if AI_REVIEW_STALE_HOURS <= 0 or AI_REVIEW_ALERT_WAIT_SECONDS < 0:
    raise RuntimeError("AI review staleness and alert wait must be non-negative")
if FUNDER_GRAPH_CONCURRENCY < 1:
    raise RuntimeError("FUNDER_GRAPH_CONCURRENCY must be at least 1")
if MAX_FUNDER_LOOKUP_RETRIES < 1:
    raise RuntimeError("MAX_FUNDER_LOOKUP_RETRIES must be at least 1")
if FUNDER_LOOKUP_METHOD not in {"rpc", "helius_funded_by"}:
    raise RuntimeError("FUNDER_LOOKUP_METHOD must be rpc or helius_funded_by")
if MAX_FUNDING_LOOKUP_SIGNATURES < 1:
    raise RuntimeError("MAX_FUNDING_LOOKUP_SIGNATURES must be at least 1")
if MAX_FUNDING_TRANSACTION_INSPECT < 1:
    raise RuntimeError("MAX_FUNDING_TRANSACTION_INSPECT must be at least 1")
if DISCOVERY_PENDING_BATCH < 1 or DISCOVERY_REVIEW_INTERVAL_SECONDS <= 0:
    raise RuntimeError("discovery review batch and interval must be positive")
if DISCOVERY_FRESH_WINDOW_HOURS <= 0:
    raise RuntimeError("DISCOVERY_FRESH_WINDOW_HOURS must be positive")
if AI_REVIEW_MIN_BONDING_CURVE_LIQUIDITY_SOL <= 0:
    raise RuntimeError("AI_REVIEW_MIN_BONDING_CURVE_LIQUIDITY_SOL must be positive")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    configure_logging(settings.log_level)
    return settings
