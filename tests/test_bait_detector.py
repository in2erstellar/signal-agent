"""Bait-detector tests. Ledgers, clusters, and Redis are in-memory. No sockets."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.adapters.bait_queue import RedisPendingBaitQueue
from app.config import (
    BAIT_BASELINE_LOOKBACK_MINUTES,
    BAIT_INFLUX_WINDOW_MINUTES,
    BAIT_MIN_SOLD_SHARE,
    BAIT_PENDING_STREAM,
    BAIT_RECHECK_DELAY_MINUTES,
    BAIT_SELL_WINDOW_END_MINUTES,
    BAIT_SELL_WINDOW_START_MINUTES,
    BAIT_VOLUME_SPIKE_MULTIPLE,
    FUNDER_CLUSTER_RISK_CONFIDENCE,
)
from app.db.models import WalletTx
from app.services import bait_detector as bait_module
from app.services.bait_detector import (
    HEURISTIC_NOTE,
    BaitDetector,
    BaitFlag,
    BaitRecheckTooEarly,
    BuyView,
    ClusterCohort,
    FlowTx,
    PendingBaitCheck,
)

T0 = datetime(2026, 9, 24, 15, 0, tzinfo=timezone.utc)
MINT = "Mint111111111111111111111111111111111111111"
WALLET = "WalletTracked111111111111111111111111111"
OTHER = "WalletOther11111111111111111111111111111"
HIDDEN = "WalletHidden1111111111111111111111111111"
PUBLIC = "WalletPublic1111111111111111111111111111"
ROOT = "RootFunder111111111111111111111111111111"


class ListLedger:
    def __init__(self, flows: list[FlowTx]) -> None:
        self.flows = flows

    async def token_flows(
        self,
        token_mint: str,
        start: datetime,
        end: datetime,
    ) -> list[FlowTx]:
        return [
            flow
            for flow in self.flows
            if start <= flow.block_time <= end
        ]


class MapBook:
    def __init__(self, cohorts: list[ClusterCohort]) -> None:
        self.cohorts = cohorts

    async def cohorts_for(self, wallet_address: str, token_mint: str) -> list[ClusterCohort]:
        return list(self.cohorts)


class MapCensus:
    def __init__(self, holders: set[str], top: set[str]) -> None:
        self._holders = holders
        self._top = top

    async def holders(self, token_mint: str) -> set[str]:
        return set(self._holders)

    async def public_top_holders(self, token_mint: str) -> set[str]:
        return set(self._top)


class MemoryQueue:
    def __init__(self) -> None:
        self.checks: list[PendingBaitCheck] = []

    async def enqueue(self, check: PendingBaitCheck) -> None:
        self.checks.append(check)


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def test_module_docstring_states_the_heuristic_limit() -> None:
    text = " ".join((bait_module.__doc__ or "").split())
    assert "не доказательство умысла" in text
    assert "Ложные срабатывания возможны и ожидаемы" in text
    assert "confidence" in text


def test_thresholds_live_in_config() -> None:
    assert BAIT_VOLUME_SPIKE_MULTIPLE == Decimal("3")
    assert BAIT_MIN_SOLD_SHARE == Decimal("0.50")
    assert BAIT_INFLUX_WINDOW_MINUTES == 5
    assert BAIT_SELL_WINDOW_START_MINUTES == 15
    assert BAIT_SELL_WINDOW_END_MINUTES == 60
    assert BAIT_RECHECK_DELAY_MINUTES == 60
    assert BAIT_BASELINE_LOOKBACK_MINUTES == 60
    assert BAIT_PENDING_STREAM == "signalforge:bait:pending"
    assert FUNDER_CLUSTER_RISK_CONFIDENCE == Decimal("0.5")


def _flow(
    wallet: str,
    action: str,
    amount: str,
    minutes: float,
    signature: str | None = None,
) -> FlowTx:
    kind = "buy" if action == "buy" else "sell"
    return FlowTx(
        wallet_address=wallet,
        action=kind,
        amount_sol=Decimal(amount),
        block_time=T0 + timedelta(minutes=minutes),
        signature=signature or f"{wallet}-{action}-{minutes}-{amount}",
    )


def _buy_tx(amount: str = "10", signature: str = "buy-sig") -> WalletTx:
    return WalletTx(
        wallet_id=uuid.uuid4(),
        token_id=uuid.uuid4(),
        action="buy",
        amount_sol=Decimal(amount),
        tx_signature=signature,
        block_time=T0,
    )


def _detector(
    flows: list[FlowTx] | None = None,
    cohorts: list[ClusterCohort] | None = None,
    holders: set[str] | None = None,
    top: set[str] | None = None,
    clock: Clock | None = None,
) -> tuple[BaitDetector, MemoryQueue, Clock]:
    clock = clock or Clock(T0)
    queue = MemoryQueue()
    detector = BaitDetector(
        ListLedger(flows or []),
        MapBook(cohorts or []),
        MapCensus(holders or set(), top or set()),
        queue,
        clock=clock,
    )
    return detector, queue, clock


def _standard_flows(influx: str, sell: str, sell_minute: float = 15) -> list[FlowTx]:
    return [
        _flow(OTHER, "buy", "120", -30),
        _flow(WALLET, "buy", "10", 0, signature="buy-sig"),
        _flow(OTHER, "buy", influx, 5),
        _flow(WALLET, "sell", sell, sell_minute),
    ]


def _buy_view() -> BuyView:
    return BuyView(WALLET, MINT, Decimal("10"), T0, "buy-sig")


async def test_dump_flags_at_3_1x_and_exact_half_sold_on_the_window_edge() -> None:
    detector, _, _ = _detector(_standard_flows("31", "5", sell_minute=15))
    flag = await detector._detect_dump_after_influx(_buy_view())
    assert flag is not None
    assert flag.flag_type == "dump_after_copytrade_influx"
    assert flag.confidence == Decimal("0.5")
    assert flag.evidence["volume_multiple"] == "3.1"
    assert flag.evidence["sold_share"] == "0.5"
    assert flag.evidence["note"] == HEURISTIC_NOTE
    assert flag.wallet_address == WALLET
    assert flag.token_mint == MINT
    assert isinstance(flag, BaitFlag)


async def test_dump_does_not_flag_at_2_9x_even_if_the_position_is_fully_sold() -> None:
    detector, _, _ = _detector(_standard_flows("29", "10", sell_minute=30))
    assert await detector._detect_dump_after_influx(_buy_view()) is None


async def test_dump_does_not_flag_at_exact_3x() -> None:
    detector, _, _ = _detector(_standard_flows("30", "10", sell_minute=30))
    assert await detector._detect_dump_after_influx(_buy_view()) is None


async def test_dump_does_not_flag_a_market_move_without_a_token_specific_influx() -> None:
    # The wallet exits after a broad rally, but this mint only saw 1.2x its
    # own baseline. That is not a copy-trade influx on this token.
    detector, _, _ = _detector(_standard_flows("12", "9", sell_minute=40))
    assert await detector._detect_dump_after_influx(_buy_view()) is None


async def test_dump_does_not_flag_when_sold_share_is_just_under_half() -> None:
    detector, _, _ = _detector(_standard_flows("31", "4.99", sell_minute=30))
    assert await detector._detect_dump_after_influx(_buy_view()) is None


async def test_dump_ignores_sells_outside_the_15_to_60_minute_window() -> None:
    too_early, _, _ = _detector(_standard_flows("31", "10", sell_minute=14.999))
    assert await too_early._detect_dump_after_influx(_buy_view()) is None
    too_late, _, _ = _detector(_standard_flows("31", "10", sell_minute=60.001))
    assert await too_late._detect_dump_after_influx(_buy_view()) is None
    on_end, _, _ = _detector(_standard_flows("31", "8", sell_minute=60))
    flag = await on_end._detect_dump_after_influx(_buy_view())
    assert flag is not None
    assert flag.confidence == Decimal("0.8")


async def test_dump_ignores_the_tracked_wallets_own_buys_and_does_not_double_count() -> None:
    flows = [
        _flow(WALLET, "buy", "10000", -10, signature="old-self"),
        _flow(OTHER, "buy", "120", -30),
        _flow(WALLET, "buy", "10", 0, signature="buy-sig"),
        _flow(WALLET, "buy", "10000", 2, signature="self-during-influx"),
        _flow(OTHER, "buy", "31", 5),
        _flow(WALLET, "sell", "20010", 30),
    ]
    detector, _, _ = _detector(flows)
    flag = await detector._detect_dump_after_influx(_buy_view())
    assert flag is not None
    assert flag.evidence["volume_multiple"] == "3.1"
    assert flag.evidence["position_sol"] == "20010"
    assert flag.confidence == Decimal("1")


async def test_dump_confidence_caps_at_one_when_sol_sold_exceeds_the_position() -> None:
    detector, _, _ = _detector(_standard_flows("31", "15", sell_minute=30))
    flag = await detector._detect_dump_after_influx(_buy_view())
    assert flag is not None
    assert flag.confidence == Decimal("1")
    assert flag.evidence["sold_share"] == "1.5"


async def test_hidden_dual_position_flags_a_cluster_mate_missing_from_the_top() -> None:
    cohort = ClusterCohort(
        ROOT,
        Decimal("0.8"),
        (WALLET, PUBLIC),
        (HIDDEN,),
    )
    detector, _, clock = _detector(
        cohorts=[cohort],
        holders={WALLET, PUBLIC, HIDDEN},
        top={WALLET, PUBLIC},
    )
    flag = await detector._detect_hidden_dual_position(WALLET, MINT)
    assert flag is not None
    assert flag.flag_type == "hidden_dual_position"
    assert flag.confidence == Decimal("0.5")
    assert flag.evidence["hidden_wallets"] == [HIDDEN]
    assert flag.evidence["public_holder_wallets"] == [PUBLIC]
    assert flag.evidence["note"] == HEURISTIC_NOTE
    assert flag.detected_at == clock.now


async def test_hidden_dual_position_ignores_a_visible_second_wallet() -> None:
    cohort = ClusterCohort(ROOT, Decimal("0.8"), (WALLET, PUBLIC), (PUBLIC,))
    detector, _, _ = _detector(
        cohorts=[cohort],
        holders={WALLET, PUBLIC},
        top={WALLET, PUBLIC},
    )
    assert await detector._detect_hidden_dual_position(WALLET, MINT) is None


async def test_hidden_dual_position_ignores_a_cluster_mate_that_does_not_hold() -> None:
    cohort = ClusterCohort(ROOT, Decimal("0.8"), (WALLET,), (HIDDEN,))
    detector, _, _ = _detector(
        cohorts=[cohort],
        holders={WALLET},
        top={WALLET},
    )
    assert await detector._detect_hidden_dual_position(WALLET, MINT) is None


async def test_funder_cluster_member_flags_above_the_confidence_cut() -> None:
    cohort = ClusterCohort(ROOT, Decimal("0.51"), (WALLET, PUBLIC, HIDDEN), ())
    detector, _, _ = _detector(cohorts=[cohort])
    flag = await detector._detect_funder_cluster_member(WALLET, MINT)
    assert flag is not None
    assert flag.flag_type == "funder_cluster_member"
    assert flag.confidence == Decimal("0.51")
    assert flag.evidence["root_funder"] == ROOT
    assert flag.evidence["cluster_size"] == 3
    assert flag.evidence["note"] == HEURISTIC_NOTE


async def test_funder_cluster_member_does_not_flag_at_exact_threshold() -> None:
    cohort = ClusterCohort(ROOT, Decimal("0.5"), (WALLET, PUBLIC, HIDDEN), ())
    detector, _, _ = _detector(cohorts=[cohort])
    assert await detector._detect_funder_cluster_member(WALLET, MINT) is None


async def test_funder_cluster_member_ignores_a_cluster_the_wallet_is_not_in() -> None:
    cohort = ClusterCohort(ROOT, Decimal("0.9"), (PUBLIC, HIDDEN, OTHER), ())
    detector, _, _ = _detector(cohorts=[cohort])
    assert await detector._detect_funder_cluster_member(WALLET, MINT) is None


async def test_analyze_buy_schedules_recheck_and_does_not_emit_the_dump_flag() -> None:
    flows = _standard_flows("31", "10", sell_minute=30)
    cohort = ClusterCohort(ROOT, Decimal("0.7"), (WALLET, PUBLIC), (HIDDEN,))
    detector, queue, clock = _detector(
        flows,
        cohorts=[cohort],
        holders={WALLET, HIDDEN},
        top={WALLET},
        clock=Clock(T0),
    )
    flags = await detector.analyze_wallet_action(WALLET, MINT, _buy_tx())
    assert [flag.flag_type for flag in flags] == [
        "hidden_dual_position",
        "funder_cluster_member",
    ]
    assert len(queue.checks) == 1
    pending = queue.checks[0]
    assert pending.scheduled_at == clock.now + timedelta(minutes=BAIT_RECHECK_DELAY_MINUTES)
    assert pending.buy_signature == "buy-sig"
    assert pending.buy_amount_sol == Decimal("10")
    assert pending.wallet_address == WALLET
    assert pending.token_mint == MINT
    assert "dump_after_copytrade_influx" not in {flag.flag_type for flag in flags}


async def test_complete_pending_check_waits_until_scheduled_at_then_flags() -> None:
    detector, _, clock = _detector(_standard_flows("31", "8", sell_minute=30))
    pending = PendingBaitCheck(
        wallet_address=WALLET,
        token_mint=MINT,
        buy_signature="buy-sig",
        buy_amount_sol=Decimal("10"),
        buy_block_time=T0,
        scheduled_at=T0 + timedelta(minutes=60),
        created_at=T0,
    )
    clock.now = T0 + timedelta(minutes=59)
    with pytest.raises(BaitRecheckTooEarly):
        await detector.complete_pending_check(pending)
    clock.now = T0 + timedelta(minutes=60)
    flags = await detector.complete_pending_check(pending)
    assert len(flags) == 1
    assert flags[0].flag_type == "dump_after_copytrade_influx"
    assert flags[0].confidence == Decimal("0.8")
    assert flags[0].detected_at == clock.now


async def test_sell_does_not_schedule_a_recheck() -> None:
    detector, queue, _ = _detector()
    sell = WalletTx(
        wallet_id=uuid.uuid4(),
        token_id=uuid.uuid4(),
        action="sell",
        amount_sol=Decimal("1"),
        tx_signature="sell-sig",
        block_time=T0,
    )
    assert await detector.analyze_wallet_action(WALLET, MINT, sell) == []
    assert queue.checks == []


async def test_pending_record_round_trips_through_redis_stream_fields() -> None:
    detector, queue, _ = _detector()
    await detector.analyze_wallet_action(WALLET, MINT, _buy_tx())
    fields = queue.checks[0].to_stream_fields()
    assert all(isinstance(value, str) for value in fields.values())
    restored = PendingBaitCheck.from_stream_fields(fields)
    assert restored == queue.checks[0]

    class FakeRedis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, str]]] = []

        async def xadd(self, name: str, fields: dict[str, str]) -> str:
            self.calls.append((name, fields))
            return "1-0"

    redis = FakeRedis()
    await RedisPendingBaitQueue(redis).enqueue(queue.checks[0])
    assert redis.calls == [(BAIT_PENDING_STREAM, fields)]
