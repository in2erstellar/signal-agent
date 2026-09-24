"""FastAPI entrypoint for SignalForge."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis

from app.api.router import public_router
from app.api.webhooks import router as webhooks_router
from app.config import get_settings
from app.db.base import create_engine, create_sessionmaker
from app.health import probe_database, probe_redis
from app.observability import collect_queue_sizes
from app.workers.wallet_watcher import WalletWatcher

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    engine = create_engine(settings)
    app.state.engine = engine
    app.state.session_factory = create_sessionmaker(engine)
    app.state.redis = Redis.from_url(
        settings.redis_url,
        socket_connect_timeout=5,
        socket_timeout=5,
        decode_responses=True,
    )
    app.state.watcher = WalletWatcher(
        app.state.session_factory,
        app.state.redis,
        settings.helius_webhook_secret.get_secret_value(),
    )
    logger.info("service_started", environment=settings.environment)
    try:
        yield
    finally:
        await app.state.redis.aclose()
        await engine.dispose()


app = FastAPI(
    title="SignalForge",
    description=(
        "Public read and watchlist API over stored signals. "
        "Send the X-API-Key header. A risk_score of 100 is not a claim that a token is safe."
    ),
    lifespan=lifespan,
)
app.include_router(webhooks_router)
app.include_router(public_router)


@app.get("/metrics", include_in_schema=False)
async def metrics(request: Request) -> Response:
    await collect_queue_sizes(request.app.state.redis)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health(request: Request) -> JSONResponse:
    db_state = await probe_database(request.app.state.engine)
    redis_state = await probe_redis(request.app.state.redis)
    healthy = db_state == "connected" and redis_state == "connected"
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "status": "ok" if healthy else "degraded",
            "db": db_state,
            "redis": redis_state,
        },
    )
