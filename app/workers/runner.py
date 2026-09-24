"""Process entry for the risk, bait, and signal workers.

SIGTERM sets a stop event. The current cycle is allowed to commit; the
next cycle does not start. Tasks still running after the grace period
are cancelled.
"""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

import structlog

from app.config import (
    ALERT_DISPATCH_INTERVAL_SECONDS,
    BAIT_POLL_INTERVAL_SECONDS,
    SIGNAL_FUSION_INTERVAL_SECONDS,
    get_settings,
)
from app.db.base import create_engine, create_sessionmaker
from app.workers.alert_dispatcher import AlertDispatcher
from app.workers.bait_check_worker import BaitCheckWorker, detector_for_session
from app.workers.risk_scan_worker import PrebuiltClusters, RiskScanWorker
from app.workers.signal_fusion import SignalFusion

logger = structlog.get_logger(__name__)

Loop = Callable[[], Awaitable[None]]
SHUTDOWN_GRACE_SECONDS = 25


async def poll_loop(
    stop: asyncio.Event,
    cycle: Callable[[], Awaitable[object]],
    interval_seconds: float,
) -> None:
    while not stop.is_set():
        await cycle()
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue


async def run_until_stopped(
    stop: asyncio.Event,
    *loops: Loop,
    grace_seconds: float = SHUTDOWN_GRACE_SECONDS,
) -> None:
    tasks = [asyncio.create_task(loop()) for loop in loops]
    await stop.wait()
    logger.info("workers_stopping")
    done, pending = await asyncio.wait(tasks, timeout=grace_seconds)
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        task.result()
    logger.info("workers_stopped")


async def serve(stop: asyncio.Event | None = None) -> None:
    from redis.asyncio import Redis

    from app.adapters.helius import HeliusClient
    from app.adapters.raydium import RaydiumPools
    from app.adapters.token_security import HeliusTokenSecurity
    from app.services.funder_graph import FunderGraphBuilder
    from app.services.risk_scanner import RiskScanner

    settings = get_settings()
    engine = create_engine(settings)
    sessions = create_sessionmaker(engine)
    redis = Redis.from_url(
        settings.redis_url,
        socket_connect_timeout=5,
        socket_timeout=5,
        decode_responses=True,
    )
    helius = HeliusClient.from_settings(settings)
    raydium = RaydiumPools(settings.raydium_api_base_url)
    facts = HeliusTokenSecurity(helius, raydium)
    stop = stop or asyncio.Event()

    def funder_factory(session: object) -> FunderGraphBuilder:
        return FunderGraphBuilder(session, helius)  # type: ignore[arg-type]

    def scanner_factory(session: object, clusters: list[object]) -> RiskScanner:
        del session
        return RiskScanner(facts, PrebuiltClusters(clusters))  # type: ignore[arg-type]

    def detector_factory(session: object):
        return detector_for_session(
            session,  # type: ignore[arg-type]
            helius,
            lambda: datetime.now(timezone.utc),
        )

    risk = RiskScanWorker(sessions, redis, funder_factory, scanner_factory, stop=stop)
    bait = BaitCheckWorker(sessions, redis, detector_factory, stop=stop)
    fusion = SignalFusion(sessions, stop=stop)

    async def risk_loop() -> None:
        while not stop.is_set():
            await risk.run_once(block_ms=5000)

    async def alerts_loop() -> None:
        from app.adapters.telegram_bot import (
            PerChatRateLimiter,
            TelegramAlertSender,
            build_application,
            telegram_configured,
        )

        token = settings.telegram_bot_token.get_secret_value()
        if not telegram_configured(token):
            logger.info("telegram_disabled")
            await stop.wait()
            return
        application = build_application(token, sessions)
        dispatcher = AlertDispatcher(
            sessions,
            TelegramAlertSender(application.bot, PerChatRateLimiter()),
            stop=stop,
        )
        await application.initialize()
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)
        logger.info("telegram_polling_started")
        try:
            await poll_loop(stop, dispatcher.run_once, ALERT_DISPATCH_INTERVAL_SECONDS)
        finally:
            await application.updater.stop()
            await application.stop()
            await application.shutdown()

    logger.info("workers_started")
    try:
        await run_until_stopped(
            stop,
            risk_loop,
            lambda: poll_loop(stop, bait.run_once, BAIT_POLL_INTERVAL_SECONDS),
            lambda: poll_loop(stop, fusion.run_once, SIGNAL_FUSION_INTERVAL_SECONDS),
            alerts_loop,
        )
    finally:
        await helius.aclose()
        await raydium.aclose()
        await redis.aclose()
        await engine.dispose()


def main() -> None:
    stop = asyncio.Event()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        loop.run_until_complete(serve(stop))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
