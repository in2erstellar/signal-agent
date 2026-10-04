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
    AI_REVIEW_PROVIDER,
    ALERT_DISPATCH_INTERVAL_SECONDS,
    BAIT_POLL_INTERVAL_SECONDS,
    DISCOVERY_REVIEW_INTERVAL_SECONDS,
    FUNDER_GRAPH_CONCURRENCY,
    OUTCOME_POLL_INTERVAL_SECONDS,
    SIGNAL_FUSION_INTERVAL_SECONDS,
    get_settings,
)
from app.db.base import create_engine, create_sessionmaker
from app.services.outcome_jobs import OutcomeWorker
from app.workers.alert_dispatcher import AlertDispatcher
from app.workers.bait_check_worker import BaitCheckWorker, detector_for_session
from app.workers.risk_scan_worker import PrebuiltClusters, RiskScanWorker
from app.workers.signal_fusion import SignalFusion

logger = structlog.get_logger(__name__)

Loop = Callable[[], Awaitable[None]]
SHUTDOWN_GRACE_SECONDS = 25


async def resilient_loop(
    stop: asyncio.Event,
    cycle: Callable[[], Awaitable[object]],
    *,
    name: str,
    pause_seconds: float = 1,
) -> None:
    """Run ``cycle`` until ``stop``. One failure is logged and the loop continues."""
    while not stop.is_set():
        try:
            await cycle()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("worker_loop_failed", loop=name, error_type=type(exc).__name__)
            try:
                await asyncio.wait_for(stop.wait(), timeout=pause_seconds)
            except TimeoutError:
                continue


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

    from app.adapters.ai_review import AiReviewClient, ai_review_configured
    from app.adapters.helius import HeliusClient
    from app.adapters.pumpportal import (
        DISCOVERY_DOWN_MESSAGE,
        MigrationEvent,
        NewTokenEvent,
        PumpPortalClient,
        open_pumpportal_socket,
        pumpportal_ws_url,
    )
    from app.adapters.raydium import RaydiumPools
    from app.adapters.token_security import HeliusTokenSecurity
    from app.services.funder_graph import FunderGraphBuilder
    from app.services.risk_scanner import RiskScanner
    from app.services.startup_checks import assert_worker_ready
    from app.workers.ai_review_worker import AiReviewWorker
    from app.workers.token_discovery import TokenDiscovery

    settings = get_settings()
    engine = create_engine(settings)
    try:
        await assert_worker_ready(engine, settings.helius_api_key.get_secret_value())
    except Exception:
        await engine.dispose()
        raise
    sessions = create_sessionmaker(engine)
    # XREADGROUP blocks for 5s. The socket timeout has to be longer than that
    # block, or the first read raises and the consumer task dies.
    redis = Redis.from_url(
        settings.redis_url,
        socket_connect_timeout=5,
        socket_timeout=20,
        decode_responses=True,
    )
    helius = HeliusClient.from_settings(settings)
    raydium = RaydiumPools(settings.raydium_api_base_url)
    facts = HeliusTokenSecurity(helius, raydium)
    stop = stop or asyncio.Event()
    graph_slots = asyncio.Semaphore(FUNDER_GRAPH_CONCURRENCY)

    async def notify_discovery_down(text: str = DISCOVERY_DOWN_MESSAGE) -> None:
        from app.adapters.telegram_bot import admin_ids, telegram_configured

        token = settings.telegram_bot_token.get_secret_value()
        if not telegram_configured(token):
            logger.critical("pumpportal_down_no_telegram")
            return
        admins = admin_ids(settings.telegram_admin_ids)
        if not admins:
            logger.critical("pumpportal_down_no_admins")
            return
        from telegram import Bot

        bot = Bot(token)
        await bot.initialize()
        try:
            for chat_id in sorted(admins):
                try:
                    await bot.send_message(chat_id=chat_id, text=text)
                except Exception as exc:
                    logger.critical(
                        "pumpportal_down_alert_failed",
                        chat_id=chat_id,
                        error_type=type(exc).__name__,
                    )
        finally:
            await bot.shutdown()

    portal = PumpPortalClient(
        pumpportal_ws_url(settings),
        opener=open_pumpportal_socket,
        notifier=notify_discovery_down,
    )
    discovery = TokenDiscovery(sessions, redis, helius)

    def funder_factory(session: object) -> FunderGraphBuilder:
        return FunderGraphBuilder(session, helius, semaphore=graph_slots)  # type: ignore[arg-type]

    def scanner_factory(session: object, clusters: list[object]) -> RiskScanner:
        del session
        return RiskScanner(facts, PrebuiltClusters(clusters))  # type: ignore[arg-type]

    def detector_factory(session: object):
        return detector_for_session(
            session,  # type: ignore[arg-type]
            helius,
            lambda: datetime.now(timezone.utc),
        )

    helius_key = settings.helius_api_key.get_secret_value().strip()
    logger.info(
        "helius_configured",
        configured=bool(helius_key) and helius_key != "replace-me",
    )
    logger.info(
        "ai_review_configured",
        configured=ai_review_configured(settings),
        provider=AI_REVIEW_PROVIDER,
        model=settings.ai_review_model,
    )
    reviews = AiReviewClient.from_settings(settings)
    risk = RiskScanWorker(sessions, redis, funder_factory, scanner_factory, stop=stop)
    bait = BaitCheckWorker(sessions, redis, detector_factory, stop=stop)
    outcomes = OutcomeWorker(sessions, redis, raydium, lp_custody=facts, stop=stop)
    fusion = SignalFusion(
        sessions,
        redis,
        reviews_enabled=ai_review_configured(settings),
        stop=stop,
    )
    ai_reviews = AiReviewWorker(sessions, redis, reviews, stop=stop)

    async def risk_loop() -> None:
        await resilient_loop(stop, lambda: risk.run_once(block_ms=5000), name="risk_scan")

    async def discovery_loop() -> None:
        async for event in portal.iter_events(stop):
            try:
                if isinstance(event, NewTokenEvent):
                    await discovery.ingest_create(event)
                elif isinstance(event, MigrationEvent):
                    await discovery.handle_migration(event)
            except Exception as exc:
                logger.warning(
                    "token_discovery_ingest_failed",
                    error_type=type(exc).__name__,
                )

    async def ai_review_loop() -> None:
        await resilient_loop(stop, lambda: ai_reviews.run_once(block_ms=5000), name="ai_review")

    async def review_once() -> None:
        try:
            await discovery.review_pending()
        except Exception as exc:
            logger.warning(
                "token_discovery_review_failed",
                error_type=type(exc).__name__,
            )

    async def alerts_loop() -> None:
        from app.adapters.telegram_bot import (
            PerChatRateLimiter,
            TelegramAlertSender,
            admin_ids,
            build_application,
            telegram_configured,
        )

        token = settings.telegram_bot_token.get_secret_value()
        if not telegram_configured(token):
            logger.info("telegram_disabled")
            await stop.wait()
            return
        admins = admin_ids(settings.telegram_admin_ids)
        application = build_application(token, sessions, admins=admins, redis=redis)
        application.bot_data["helius"] = helius
        application.bot_data["webhook_id"] = settings.helius_webhook_id.strip()
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
            discovery_loop,
            lambda: poll_loop(stop, review_once, DISCOVERY_REVIEW_INTERVAL_SECONDS),
            lambda: poll_loop(stop, bait.run_once, BAIT_POLL_INTERVAL_SECONDS),
            lambda: poll_loop(stop, outcomes.run_once, OUTCOME_POLL_INTERVAL_SECONDS),
            lambda: poll_loop(stop, fusion.run_once, SIGNAL_FUSION_INTERVAL_SECONDS),
            ai_review_loop,
            alerts_loop,
        )
    finally:
        await reviews.aclose()
        await portal.close()
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
