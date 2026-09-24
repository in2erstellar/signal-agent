"""Эвристики copy-trade baiting.

Ограничение метода: это эвристика на основе паттернов, не доказательство
умысла. Ложные срабатывания возможны и ожидаемы, поэтому результат —
confidence от 0 до 1, а не бинарный вердикт. Отсутствие флага не означает,
что схемы нет: видны только сделки, попавшие в журнал, и только заданные окна.

Сброс после притока — двухфазный процесс. Покупка пишет запись
``pending_bait_check`` в Redis Stream с ``scheduled_at = now + 60 мин``
и не выносит вердикт в тот же вызов. Вердикт считает ``complete_pending_check``,
когда отдельный воркер забирает созревшую запись. Как именно воркер ждёт
``scheduled_at`` (Redis Streams consumer group, Celery beat или APScheduler),
этот модуль не выбирает.

Доля позиции считается в SOL notional по ``amount_sol``. Количества токенов
в журнале нет, поэтому движение цены неотличимо от продажи части позиции.
База объёма — средний 5-минутный объём покупок других кошельков этого же
mint за lookback, не рынок целиком. Рост рынка без всплеска на этом mint
порог не проходит. Нулевая база не трактуется как кратный всплеск.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Literal, Protocol

import structlog
from pydantic import BaseModel, ConfigDict, Field

from app.config import (
    BAIT_BASELINE_LOOKBACK_MINUTES,
    BAIT_INFLUX_WINDOW_MINUTES,
    BAIT_MIN_SOLD_SHARE,
    BAIT_RECHECK_DELAY_MINUTES,
    BAIT_SELL_WINDOW_END_MINUTES,
    BAIT_SELL_WINDOW_START_MINUTES,
    BAIT_VOLUME_SPIKE_MULTIPLE,
    FUNDER_CLUSTER_RISK_CONFIDENCE,
)
from app.db.models import WalletTx

logger = structlog.get_logger(__name__)

FlagType = Literal[
    "dump_after_copytrade_influx",
    "hidden_dual_position",
    "funder_cluster_member",
]
Clock = Callable[[], datetime]
HEURISTIC_NOTE = "Pattern heuristic only. This is not proof of intent."


class BaitFlag(BaseModel):
    """One pattern hit. ``confidence`` grades the hint; it does not prove intent."""

    model_config = ConfigDict(extra="forbid")

    wallet_address: str
    token_mint: str
    flag_type: FlagType
    confidence: Decimal = Field(ge=0, le=1)
    evidence: dict[str, Any]
    detected_at: datetime


class PendingBaitCheck(BaseModel):
    """Phase-1 record. The worker calls phase 2 only after ``scheduled_at``."""

    model_config = ConfigDict(extra="forbid")

    wallet_address: str
    token_mint: str
    buy_signature: str
    buy_amount_sol: Decimal
    buy_block_time: datetime
    scheduled_at: datetime
    created_at: datetime

    def to_stream_fields(self) -> dict[str, str]:
        return {
            "wallet_address": self.wallet_address,
            "token_mint": self.token_mint,
            "buy_signature": self.buy_signature,
            "buy_amount_sol": str(self.buy_amount_sol),
            "buy_block_time": self.buy_block_time.isoformat(),
            "scheduled_at": self.scheduled_at.isoformat(),
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_stream_fields(cls, fields: dict[Any, Any]) -> PendingBaitCheck:
        decoded = {
            _text(key): _text(value)
            for key, value in fields.items()
        }
        return cls(
            wallet_address=decoded["wallet_address"],
            token_mint=decoded["token_mint"],
            buy_signature=decoded["buy_signature"],
            buy_amount_sol=Decimal(decoded["buy_amount_sol"]),
            buy_block_time=datetime.fromisoformat(decoded["buy_block_time"]),
            scheduled_at=datetime.fromisoformat(decoded["scheduled_at"]),
            created_at=datetime.fromisoformat(decoded["created_at"]),
        )


class BaitRecheckTooEarly(Exception):
    """Phase 2 was called before the sell window can have closed."""

    def __init__(self, scheduled_at: datetime) -> None:
        super().__init__(
            f"pending bait check is not due until {scheduled_at.isoformat()}"
        )
        self.scheduled_at = scheduled_at


@dataclass(frozen=True)
class BuyView:
    wallet_address: str
    token_mint: str
    amount_sol: Decimal
    block_time: datetime
    signature: str


@dataclass(frozen=True)
class FlowTx:
    wallet_address: str
    action: Literal["buy", "sell"]
    amount_sol: Decimal
    block_time: datetime
    signature: str


@dataclass(frozen=True)
class ClusterCohort:
    """One stored funder cluster plus wallets funded by the same root.

    ``member_wallets`` is the published cluster. ``funded_by_root`` may
    include addresses that never appeared in the public top-holder list.
    """

    root_funder: str
    confidence: Decimal | None
    member_wallets: tuple[str, ...]
    funded_by_root: tuple[str, ...]


class SwapLedger(Protocol):
    async def token_flows(
        self,
        token_mint: str,
        start: datetime,
        end: datetime,
    ) -> list[FlowTx]: ...


class ClusterBook(Protocol):
    async def cohorts_for(
        self,
        wallet_address: str,
        token_mint: str,
    ) -> list[ClusterCohort]: ...


class HolderCensus(Protocol):
    async def holders(self, token_mint: str) -> set[str]: ...

    async def public_top_holders(self, token_mint: str) -> set[str]: ...


class PendingBaitQueue(Protocol):
    async def enqueue(self, check: PendingBaitCheck) -> None: ...


class BaitDetector:
    """Schedules the delayed dump check and evaluates the other two patterns now."""

    def __init__(
        self,
        ledger: SwapLedger,
        clusters: ClusterBook,
        holders: HolderCensus,
        queue: PendingBaitQueue,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._ledger = ledger
        self._clusters = clusters
        self._holders = holders
        self._queue = queue
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    async def analyze_wallet_action(
        self,
        wallet_address: str,
        token_mint: str,
        action_tx: WalletTx,
    ) -> list[BaitFlag]:
        """Phase 1 for a buy, plus the two checks that do not need a delay.

        A buy never returns ``dump_after_copytrade_influx``. That pattern
        needs the 15–60 minute sell window, so this method only enqueues
        ``pending_bait_check``.
        """
        flags: list[BaitFlag] = []
        hidden = await self._detect_hidden_dual_position(wallet_address, token_mint)
        if hidden is not None:
            flags.append(hidden)
        member = await self._detect_funder_cluster_member(wallet_address, token_mint)
        if member is not None:
            flags.append(member)
        # Enqueue after the synchronous checks. A failed XADD then retries
        # without an extra stream record from a half-finished call.
        if action_tx.action == "buy":
            await self._schedule_dump_recheck(wallet_address, token_mint, action_tx)
        return flags

    async def complete_pending_check(self, pending: PendingBaitCheck) -> list[BaitFlag]:
        """Phase 2. The worker must call this only once ``scheduled_at`` has passed."""
        now = self._clock()
        if now < pending.scheduled_at:
            raise BaitRecheckTooEarly(pending.scheduled_at)
        flag = await self._detect_dump_after_influx(
            BuyView(
                wallet_address=pending.wallet_address,
                token_mint=pending.token_mint,
                amount_sol=pending.buy_amount_sol,
                block_time=pending.buy_block_time,
                signature=pending.buy_signature,
            )
        )
        return [flag] if flag is not None else []

    async def evaluate_mature_buy(self, pending: PendingBaitCheck) -> list[BaitFlag]:
        """Run all three patterns after the delay. Does not schedule another check."""
        flags = list(await self.complete_pending_check(pending))
        hidden = await self._detect_hidden_dual_position(
            pending.wallet_address,
            pending.token_mint,
        )
        if hidden is not None:
            flags.append(hidden)
        member = await self._detect_funder_cluster_member(
            pending.wallet_address,
            pending.token_mint,
        )
        if member is not None:
            flags.append(member)
        return flags

    async def _schedule_dump_recheck(
        self,
        wallet_address: str,
        token_mint: str,
        action_tx: WalletTx,
    ) -> None:
        created_at = self._clock()
        pending = PendingBaitCheck(
            wallet_address=wallet_address,
            token_mint=token_mint,
            buy_signature=action_tx.tx_signature,
            buy_amount_sol=action_tx.amount_sol,
            buy_block_time=action_tx.block_time,
            scheduled_at=created_at + timedelta(minutes=BAIT_RECHECK_DELAY_MINUTES),
            created_at=created_at,
        )
        await self._queue.enqueue(pending)
        logger.info(
            "bait_recheck_scheduled",
            wallet_address=wallet_address,
            token_mint=token_mint,
            buy_signature=action_tx.tx_signature,
            scheduled_at=pending.scheduled_at.isoformat(),
        )

    async def _detect_dump_after_influx(self, buy: BuyView) -> BaitFlag | None:
        """Other wallets' buy volume spiked, then this wallet sold inside the window.

        The triggering buy is counted from ``buy`` itself. A ledger row with
        the same signature is ignored so the position is not added twice.
        """
        lookback = timedelta(minutes=BAIT_BASELINE_LOOKBACK_MINUTES)
        influx_window = timedelta(minutes=BAIT_INFLUX_WINDOW_MINUTES)
        sell_start_at = buy.block_time + timedelta(minutes=BAIT_SELL_WINDOW_START_MINUTES)
        sell_end_at = buy.block_time + timedelta(minutes=BAIT_SELL_WINDOW_END_MINUTES)
        flows = await self._ledger.token_flows(
            buy.token_mint,
            buy.block_time - lookback,
            sell_end_at,
        )
        others = [
            flow
            for flow in flows
            if flow.wallet_address != buy.wallet_address
            and flow.action == "buy"
            and flow.signature != buy.signature
        ]
        baseline_start = buy.block_time - lookback
        baseline_sum = sum(
            (
                flow.amount_sol
                for flow in others
                if baseline_start <= flow.block_time < buy.block_time
            ),
            Decimal(0),
        )
        baseline_rate = (
            baseline_sum
            * Decimal(BAIT_INFLUX_WINDOW_MINUTES)
            / Decimal(BAIT_BASELINE_LOOKBACK_MINUTES)
        )
        influx = sum(
            (
                flow.amount_sol
                for flow in others
                if buy.block_time < flow.block_time <= buy.block_time + influx_window
            ),
            Decimal(0),
        )
        # No established base level: a multiple is undefined, so this is not a spike.
        if baseline_rate <= 0 or influx / baseline_rate <= BAIT_VOLUME_SPIKE_MULTIPLE:
            return None

        position = buy.amount_sol
        sold = Decimal(0)
        for flow in flows:
            if flow.signature == buy.signature or flow.wallet_address != buy.wallet_address:
                continue
            if flow.block_time < sell_start_at:
                if flow.action == "buy":
                    position += flow.amount_sol
                elif flow.action == "sell":
                    position -= flow.amount_sol
            elif flow.action == "sell" and flow.block_time <= sell_end_at:
                sold += flow.amount_sol
        if position <= 0:
            return None
        sold_share = sold / position
        if sold_share < BAIT_MIN_SOLD_SHARE:
            return None
        confidence = min(Decimal(1), sold_share)
        return self._flag(
            wallet_address=buy.wallet_address,
            token_mint=buy.token_mint,
            flag_type="dump_after_copytrade_influx",
            confidence=confidence,
            evidence={
                "baseline_sol_per_window": _decimal(baseline_rate),
                "influx_sol": _decimal(influx),
                "volume_multiple": _decimal(influx / baseline_rate),
                "position_sol": _decimal(position),
                "sold_sol": _decimal(sold),
                "sold_share": _decimal(sold_share),
                "influx_window_minutes": BAIT_INFLUX_WINDOW_MINUTES,
                "sell_window_start": sell_start_at.isoformat(),
                "sell_window_end": sell_end_at.isoformat(),
                "buy_signature": buy.signature,
                "note": HEURISTIC_NOTE,
            },
        )

    async def _detect_hidden_dual_position(
        self,
        wallet_address: str,
        token_mint: str,
    ) -> BaitFlag | None:
        """Cluster-mates hold the mint and are absent from the public top list."""
        holders = await self._holders.holders(token_mint)
        public_top = await self._holders.public_top_holders(token_mint)
        best: tuple[Decimal, ClusterCohort, list[str], list[str]] | None = None
        for cohort in await self._cohorts(wallet_address, token_mint):
            cohort_wallets = set(cohort.member_wallets) | set(cohort.funded_by_root)
            others_holding = (cohort_wallets & holders) - {wallet_address}
            hidden = sorted(others_holding - public_top)
            public_others = sorted(others_holding & public_top)
            if not hidden:
                continue
            confidence = min(
                Decimal(1),
                Decimal(len(hidden)) / Decimal(len(hidden) + len(public_others)),
            )
            if best is None or confidence > best[0]:
                best = (confidence, cohort, hidden, public_others)
        if best is None:
            return None
        confidence, cohort, hidden, public_others = best
        return self._flag(
            wallet_address=wallet_address,
            token_mint=token_mint,
            flag_type="hidden_dual_position",
            confidence=confidence,
            evidence={
                "root_funder": cohort.root_funder,
                "cluster_confidence": _optional_decimal(cohort.confidence),
                "hidden_wallets": hidden,
                "public_holder_wallets": public_others,
                "note": HEURISTIC_NOTE,
            },
        )

    async def _detect_funder_cluster_member(
        self,
        wallet_address: str,
        token_mint: str,
    ) -> BaitFlag | None:
        """The tracked wallet itself sits in a cluster above the confidence cut."""
        qualified = [
            cohort
            for cohort in await self._cohorts(wallet_address, token_mint)
            if cohort.confidence is not None
            and cohort.confidence > FUNDER_CLUSTER_RISK_CONFIDENCE
        ]
        if not qualified:
            return None
        cohort = max(
            qualified,
            key=lambda item: (item.confidence or Decimal(0), item.root_funder),
        )
        assert cohort.confidence is not None
        return self._flag(
            wallet_address=wallet_address,
            token_mint=token_mint,
            flag_type="funder_cluster_member",
            confidence=cohort.confidence,
            evidence={
                "root_funder": cohort.root_funder,
                "cluster_confidence": _decimal(cohort.confidence),
                "cluster_size": len(cohort.member_wallets),
                "note": HEURISTIC_NOTE,
            },
        )

    async def _cohorts(self, wallet_address: str, token_mint: str) -> list[ClusterCohort]:
        return [
            cohort
            for cohort in await self._clusters.cohorts_for(wallet_address, token_mint)
            if wallet_address in cohort.member_wallets
        ]

    def _flag(
        self,
        *,
        wallet_address: str,
        token_mint: str,
        flag_type: FlagType,
        confidence: Decimal,
        evidence: dict[str, Any],
    ) -> BaitFlag:
        flag = BaitFlag(
            wallet_address=wallet_address,
            token_mint=token_mint,
            flag_type=flag_type,
            confidence=confidence,
            evidence=evidence,
            detected_at=self._clock(),
        )
        logger.info(
            "bait_pattern_matched",
            flag_type=flag_type,
            wallet_address=wallet_address,
            token_mint=token_mint,
            confidence=str(confidence),
        )
        return flag


def _decimal(value: Decimal) -> str:
    return str(value)


def _optional_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
