"""Prometheus text and the trace id bound into structlog."""

from __future__ import annotations

import os

import structlog
from httpx import ASGITransport, AsyncClient

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://signalforge:signalforge@localhost:5432/signalforge",
)
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("ENVIRONMENT", "dev")
os.environ.setdefault("LOG_LEVEL", "INFO")

from app.observability import (  # noqa: E402
    bind_trace,
    new_trace_id,
    record_alert_sent,
    record_risk_scan_latency,
    record_webhook,
)


async def test_metrics_lists_webhook_alert_scan_and_queues() -> None:
    from app.main import app

    record_webhook()
    record_alert_sent()
    record_risk_scan_latency(0.25)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/metrics")
    assert response.status_code == 200
    body = response.text
    assert "signalforge_webhooks_processed_total" in body
    assert "signalforge_alerts_sent_total" in body
    assert "signalforge_risk_scan_latency_seconds" in body
    assert "signalforge_redis_queue_size" in body


def test_bind_trace_merges_into_the_log_event() -> None:
    trace_id = new_trace_id()
    with bind_trace(trace_id):
        event = structlog.contextvars.merge_contextvars(None, "info", {"event": "pipeline_step"})
    assert event["trace_id"] == trace_id
    assert "trace_id" not in structlog.contextvars.get_contextvars()
