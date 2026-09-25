"""Metrics and the trace id that follows one buy through the pipeline.

``trace_id`` is bound into structlog context vars, so every log line emitted
while the binding is active includes it. The id is also stored on the token
and copied onto the signal, because the webhook, the risk worker, fusion,
and the alert sender run as separate tasks and processes.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

import structlog
from prometheus_client import Counter, Gauge, Histogram
from redis.asyncio import Redis

from app.config import (
    AI_REVIEW_DAILY_BUDGET,
    AI_REVIEW_HOURLY_BUDGET,
    AI_REVIEW_QUEUE,
    BAIT_CHECK_QUEUE,
    RISK_SCAN_QUEUE,
)
from app.services.ai_budget import review_counts

logger = structlog.get_logger(__name__)

WEBHOOKS_PROCESSED = Counter(
    "signalforge_webhooks_processed_total",
    "Helius webhook deliveries the watcher accepted and handled.",
)
ALERTS_SENT = Counter(
    "signalforge_alerts_sent_total",
    "Alert messages accepted by the Telegram API.",
)
RISK_SCAN_LATENCY = Histogram(
    "signalforge_risk_scan_latency_seconds",
    "Time to build the funder graph and score one mint.",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
REDIS_QUEUE_SIZE = Gauge(
    "signalforge_redis_queue_size",
    "Current length of a pipeline queue.",
    ["queue"],
)
AI_REVIEW_CALLS_LAST_HOUR = Gauge(
    "signalforge_ai_review_calls_last_hour",
    "AI review reservations in the current UTC hour.",
)
AI_REVIEW_CALLS_LAST_DAY = Gauge(
    "signalforge_ai_review_calls_last_day",
    "AI review reservations in the current UTC day.",
)
AI_REVIEW_BUDGET_REMAINING_HOUR = Gauge(
    "signalforge_ai_review_budget_remaining_hour",
    "AI reviews still allowed in the current UTC hour.",
)
AI_REVIEW_BUDGET_REMAINING_DAY = Gauge(
    "signalforge_ai_review_budget_remaining_day",
    "AI reviews still allowed in the current UTC day.",
)


def new_trace_id() -> str:
    return uuid.uuid4().hex


@contextmanager
def bind_trace(trace_id: str | None) -> Iterator[None]:
    if not trace_id:
        yield
        return
    tokens = structlog.contextvars.bind_contextvars(trace_id=trace_id)
    try:
        yield
    finally:
        structlog.contextvars.reset_contextvars(**tokens)


def record_webhook() -> None:
    WEBHOOKS_PROCESSED.inc()


def record_alert_sent() -> None:
    ALERTS_SENT.inc()


def record_risk_scan_latency(seconds: float) -> None:
    RISK_SCAN_LATENCY.observe(seconds)


async def collect_queue_sizes(redis: Redis) -> None:
    try:
        risk_depth = int(await redis.xlen(RISK_SCAN_QUEUE))
        bait_depth = int(await redis.zcard(BAIT_CHECK_QUEUE))
        review_depth = int(await redis.xlen(AI_REVIEW_QUEUE))
        hour, day = await review_counts(redis, datetime.now(timezone.utc))
    except Exception as exc:
        logger.warning("queue_metrics_unavailable", error=type(exc).__name__)
        return
    REDIS_QUEUE_SIZE.labels(queue=RISK_SCAN_QUEUE).set(risk_depth)
    REDIS_QUEUE_SIZE.labels(queue=BAIT_CHECK_QUEUE).set(bait_depth)
    REDIS_QUEUE_SIZE.labels(queue=AI_REVIEW_QUEUE).set(review_depth)
    AI_REVIEW_CALLS_LAST_HOUR.set(hour)
    AI_REVIEW_CALLS_LAST_DAY.set(day)
    AI_REVIEW_BUDGET_REMAINING_HOUR.set(max(0, AI_REVIEW_HOURLY_BUDGET - hour))
    AI_REVIEW_BUDGET_REMAINING_DAY.set(max(0, AI_REVIEW_DAILY_BUDGET - day))
