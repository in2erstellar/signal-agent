"""Webhook to signal on real Postgres and Redis containers.

Helius is a scripted double. The databases are not.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.helius import FundingInfo
from app.adapters.token_security import HolderBook, HolderRow, LpBook, LpHolderRow, MintView
from app.config import configure_logging
from app.db.models import BaitFlag, Signal, Token, WalletTx
from app.services.funder_graph import FunderGraphBuilder
from app.services.risk_scanner import RiskScanner
from app.workers.bait_check_worker import BaitCheckWorker, detector_for_session
from app.workers.risk_scan_worker import PrebuiltClusters, RiskScanWorker
from app.workers.signal_fusion import SignalFusion
from app.workers.wallet_watcher import WalletWatcher
from tests.factories import make_tracked_wallet
from tests.pipeline_containers import truncate_pipeline
from tests.test_wallet_watcher import MINT, SECRET, T0, WALLET, helius_buy

pytest_plugins = ["tests.pipeline_containers"]

LOG_PATH = "/tmp/signalforge_pipeline_cycle.log"
ROOT_FUNDER = "RootFunder11111111111111111111111111111111"
HOLDERS = [WALLET, "HolderA", "HolderB", "HolderC", "HolderD", "HolderE"]


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class PassingFacts:
    async def mint_view(self, mint: str) -> MintView:
        return MintView(None, None, 1_000_000)

    async def holder_book(self, mint: str) -> HolderBook:
        return HolderBook(1_000_000, [HolderRow("retail", 100, is_lp_pool=False)])

    async def lp_book(self, mint: str) -> LpBook:
        return LpBook(
            "LpMint",
            1_000,
            [LpHolderRow("burn", 1_000, is_burn_address=True, is_lock_program=False)],
        )


class ScriptedHelius:
    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]:
        return HOLDERS[:top_n]

    async def find_first_funding_tx(self, wallet_address: str) -> FundingInfo | None:
        if wallet_address not in HOLDERS:
            return None
        return FundingInfo(
            wallet_address=wallet_address,
            funder=ROOT_FUNDER,
            amount_sol=Decimal("1"),
            signature=f"fund-{wallet_address}",
            funded_at=T0,
            slot=1,
        )


def _capture_logs() -> None:
    configure_logging("INFO")
    root = logging.getLogger()
    handler = logging.FileHandler(LOG_PATH, mode="w")
    handler.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(handler)


async def test_webhook_risk_scan_signal_and_bait_check(pipeline_infra: object) -> None:
    infra = pipeline_infra
    engine = create_async_engine(infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    redis = Redis.from_url(infra.redis_url, decode_responses=True)  # type: ignore[attr-defined]
    await truncate_pipeline(engine)
    await redis.flushdb()
    _capture_logs()
    clock = Clock(T0)
    helius = ScriptedHelius()
    try:
        async with sessions() as session:
            async with session.begin():
                session.add(make_tracked_wallet(address=WALLET, win_rate=Decimal("1")))

        watcher = WalletWatcher(sessions, redis, SECRET, clock=clock)
        result = await watcher.handle(helius_buy(), SECRET)
        assert result.stored == 1
        assert result.risk_enqueued == 1
        assert result.bait_enqueued == 1

        def funder_factory(session: object) -> FunderGraphBuilder:
            return FunderGraphBuilder(session, helius)  # type: ignore[arg-type]

        def scanner_factory(session: object, clusters: list[object]) -> RiskScanner:
            del session
            return RiskScanner(
                PassingFacts(),
                PrebuiltClusters(clusters),  # type: ignore[arg-type]
                clock=clock,
            )

        risk = RiskScanWorker(sessions, redis, funder_factory, scanner_factory)
        processed, failed = await risk.run_once()
        assert (processed, failed) == (1, 0)

        fusion = SignalFusion(sessions, clock=clock)
        seen, fusion_failed, created = await fusion.run_once()
        assert (seen, fusion_failed, created) == (1, 0, 1)

        clock.now = T0 + timedelta(minutes=60)
        bait = BaitCheckWorker(
            sessions,
            redis,
            lambda session: detector_for_session(session, helius, clock),
            clock=clock,
        )
        bait_processed, bait_failed = await bait.run_once()
        assert (bait_processed, bait_failed) == (1, 0)

        async with sessions() as session:
            token = await session.scalar(select(Token).where(Token.mint == MINT))
            signal = await session.scalar(select(Signal))
            flags = list(await session.scalars(select(BaitFlag)))
            txs = list(await session.scalars(select(WalletTx)))
        assert token is not None
        assert token.risk_score == Decimal("85")
        assert "check_funder_cluster_exposure" in (token.risk_flags or [])
        assert len(txs) == 1
        assert signal is not None
        assert signal.composite_score == Decimal("72.5")
        assert signal.wallet_score == Decimal("100")
        assert signal.risk_score == Decimal("85")
        assert [flag.flag_type for flag in flags] == ["funder_cluster_member"]
        assert flags[0].confidence == Decimal("0.6")

        for handler in logging.getLogger().handlers:
            handler.flush()
        text = open(LOG_PATH, encoding="utf-8").read()
        for event in (
            "webhook_handled",
            "risk_scan_stored",
            "signal_created",
            "bait_check_stored",
        ):
            assert event in text
    finally:
        for handler in list(logging.getLogger().handlers):
            if isinstance(handler, logging.FileHandler) and handler.baseFilename == LOG_PATH:
                handler.close()
                logging.getLogger().removeHandler(handler)
        await truncate_pipeline(engine)
        await redis.flushdb()
        await redis.aclose()
        await engine.dispose()
