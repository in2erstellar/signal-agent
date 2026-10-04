"""Creator-cluster card fields stay out of the score and do not invent data."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select

from app.adapters.raydium import PoolQuote, RaydiumError
from app.config import OUTCOME_HORIZONS, OUTCOME_QUEUE
from app.db.models import OutcomeCheck, Token
from app.services.creator_cluster import (
    HolderStake,
    distribution_of,
    share_above_limit,
    summarize_lp_stayed,
    summarize_price_drops,
)
from app.services.lp_unlock import LpCustody, classify_lp_custody, lp_is_locked, unlock_alert_due
from app.services.outcome_jobs import (
    OutcomeWorker,
    outcome_member,
    quote_observation,
    record_listing,
)
from app.templates import creator_block_text, help_text
from app.workers.signal_fusion import composite_without_wallet

pytest_plugins = ["tests.pipeline_containers"]

NOW = datetime(2026, 10, 4, 12, 0, tzinfo=timezone.utc)


def test_missing_holder_funder_is_not_a_zero_share() -> None:
    stakes = [
        HolderStake("dev", 100, excluded=False, root_funder="root"),
        HolderStake("other", 50, excluded=False, root_funder=None),
    ]
    result = distribution_of(stakes, supply_raw=1_000, dev_root="root")
    assert result.status == "unavailable"
    assert result.share is None
    assert result.reason == "holder_funder_missing"
    assert share_above_limit(
        {"distribution_status": "unavailable", "distribution_share": None},
        Decimal("0.25"),
    ) is False


def test_measured_share_does_not_change_the_composite() -> None:
    stakes = [
        HolderStake("a", 400, excluded=False, root_funder="root"),
        HolderStake("pool", 500, excluded=True, root_funder=None),
        HolderStake("b", 100, excluded=False, root_funder="else"),
    ]
    result = distribution_of(stakes, supply_raw=1_000, dev_root="root")
    assert result.status == "measured"
    assert result.share == Decimal("0.4")
    score = composite_without_wallet(Decimal("75"), Decimal("0"))
    assert score == Decimal("53.57")


def test_no_pool_and_failed_lookup_are_different_lines() -> None:
    no_pool = summarize_price_drops([("no_pool", None, None, None)])
    failed = summarize_price_drops([("price_lookup_failed", None, None, None)])
    assert no_pool.shown == "unavailable (no pool yet)"
    assert failed.shown == "unavailable (price lookup failed)"
    assert no_pool.shown != failed.shown


def test_card_counts_launches_since_tracking_and_keeps_the_long_note_in_help() -> None:
    text = creator_block_text(
        {
            "creator": "CreatorWallet",
            "launches_since_tracking": 3,
            "lp_stayed_shown": "2 of 3",
            "price_drop_shown": "unavailable (no pool yet)",
            "median_sell_shown": "unavailable (not in the tracked-wallet ledger)",
            "distribution_status": "unavailable",
            "distribution_reason": "holder_funder_missing",
            "distribution_share": None,
        }
    )
    assert "Previous launches since tracking started: 3" in text
    assert "unavailable (no pool yet)" in text
    assert "Most pump.fun tokens" not in text
    assert "scanned holders: 0%" not in text
    assert "Most pump.fun tokens never open a Raydium pool" in help_text()
    assert "unavailable (price lookup failed)" in help_text()


def test_price_drop_uses_only_pairs_that_both_exist() -> None:
    line = summarize_price_drops(
        [
            (None, Decimal("1"), None, Decimal("0.05")),
            ("no_pool", None, None, None),
        ]
    )
    assert line.status == "measured"
    assert line.shown == "1 of 1"


def test_lp_follow_up_missing_is_not_zero_stayed() -> None:
    line = summarize_lp_stayed([None, None])
    assert line.shown == "unavailable (no follow-up yet)"
    known = summarize_lp_stayed([True, False, None])
    assert known.shown == "1 of 2"


def test_follow_up_lock_needs_a_measurable_supply() -> None:
    assert lp_is_locked(None, [LpCustody(True, False, 10)]) is None
    assert lp_is_locked(100, [LpCustody(True, False, 95)]) is True
    assert lp_is_locked(100, [LpCustody(False, False, 100)]) is False


def test_burn_has_no_schedule_and_lock_program_is_unreadable() -> None:
    burn = classify_lp_custody([LpCustody(True, False, 10)])
    assert burn.status == "no_schedule"
    assert burn.reason == "burn_address"
    locked = classify_lp_custody([LpCustody(False, True, 10)])
    assert locked.status == "unavailable"
    assert locked.reason == "lock_schedule_unreadable"


def test_unlock_alert_is_due_only_inside_the_lead_window() -> None:
    unlock_at = NOW + timedelta(hours=2)
    assert unlock_alert_due(
        status="scheduled",
        unlock_at=unlock_at,
        alerted_at=None,
        now=NOW,
        lead=timedelta(hours=6),
    )
    assert not unlock_alert_due(
        status="unavailable",
        unlock_at=unlock_at,
        alerted_at=None,
        now=NOW,
        lead=timedelta(hours=6),
    )
    assert not unlock_alert_due(
        status="scheduled",
        unlock_at=unlock_at,
        alerted_at=NOW,
        now=NOW,
        lead=timedelta(hours=6),
    )


def test_quote_does_not_turn_a_missing_price_into_zero() -> None:
    status, price, reason = quote_observation(PoolQuote("lp", Decimal("10"), None))
    assert (status, price, reason) == ("unavailable", None, "price_lookup_failed")
    status, price, reason = quote_observation(PoolQuote(None, None, None))
    assert reason == "no_pool"
    assert price is None


class _Redis:
    def __init__(self) -> None:
        self.scores: dict[str, dict[str, float]] = {}

    async def zadd(self, key: str, mapping: dict[str, float]) -> None:
        self.scores.setdefault(key, {}).update(mapping)

    async def zrangebyscore(self, key: str, _min: str, _max: float) -> list[str]:
        return [member for member, score in self.scores.get(key, {}).items() if score <= _max]

    async def zrem(self, key: str, member: str) -> None:
        self.scores.get(key, {}).pop(member, None)


class _Pools:
    def __init__(self, quote: PoolQuote | None = None, error: Exception | None = None) -> None:
        self.quote = quote or PoolQuote("lp", Decimal("10"), Decimal("0.5"))
        self.error = error

    async def pool_quote(self, mint: str) -> PoolQuote:
        del mint
        if self.error is not None:
            raise self.error
        return self.quote


async def test_listing_without_a_pool_does_not_schedule_followups(pipeline_infra: object) -> None:
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from tests.pipeline_containers import truncate_pipeline

    infra = pipeline_infra
    engine = create_async_engine(infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    redis = _Redis()
    try:
        async with sessions() as session:
            async with session.begin():
                token = Token(mint="mint-no-pool", first_seen_at=NOW)
                session.add(token)
                await session.flush()
                await record_listing(
                    session,
                    redis,  # type: ignore[arg-type]
                    token,
                    pool_status="no_pool",
                    price_usd=None,
                    seen_at=NOW,
                )
                token_id = token.id
        assert OUTCOME_QUEUE not in redis.scores or redis.scores[OUTCOME_QUEUE] == {}
        async with sessions() as session:
            row = await session.scalar(select(OutcomeCheck).where(OutcomeCheck.token_id == token_id))
        assert row is not None
        assert row.horizon == "listing"
        assert row.price_reason == "no_pool"
        assert row.price_usd is None
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


async def test_followups_share_one_queue_and_a_failed_lookup_is_not_a_zero(
    pipeline_infra: object,
) -> None:
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from tests.pipeline_containers import truncate_pipeline

    infra = pipeline_infra
    engine = create_async_engine(infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    redis = _Redis()
    try:
        async with sessions() as session:
            async with session.begin():
                token = Token(mint="mint-pool", first_seen_at=NOW, creator_address="creator")
                session.add(token)
                await session.flush()
                await record_listing(
                    session,
                    redis,  # type: ignore[arg-type]
                    token,
                    pool_status="observed",
                    price_usd=Decimal("1.25"),
                    seen_at=NOW,
                )
                token_id = token.id
        members = set(redis.scores[OUTCOME_QUEUE])
        assert members == {outcome_member(token_id, name) for name, _seconds in OUTCOME_HORIZONS}
        worker = OutcomeWorker(
            sessions,
            redis,  # type: ignore[arg-type]
            _Pools(error=RaydiumError("down")),
            clock=lambda: NOW + timedelta(hours=2),
        )
        processed, failed = await worker.run_once()
        assert failed == 0
        assert processed == 1
        async with sessions() as session:
            row = await session.scalar(
                select(OutcomeCheck).where(
                    OutcomeCheck.token_id == token_id,
                    OutcomeCheck.horizon == "1h",
                )
            )
        assert row is not None
        assert row.status == "unavailable"
        assert row.price_reason == "price_lookup_failed"
        assert row.price_usd is None
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


def test_over_limit_needs_a_measured_share() -> None:
    assert share_above_limit(
        {"distribution_status": "measured", "distribution_share": "0.40"},
        Decimal("0.25"),
    )
    assert creator_block_text(
        {
            "creator": "CreatorWallet",
            "launches_since_tracking": 0,
            "lp_stayed_shown": "unavailable (no earlier launches)",
            "price_drop_shown": "unavailable (no earlier launches)",
            "median_sell_shown": "unavailable (not in the tracked-wallet ledger)",
            "distribution_status": "measured",
            "distribution_share": "0",
            "distribution_reason": None,
        },
        over_limit=False,
    ).endswith("0%")
