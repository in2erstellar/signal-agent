"""Технические сигналы риска SPL-токена.

Disclaimer: этот скан не является финансовой рекомендацией и не гарантирует
безопасность токена — только явные технические сигналы риска. Отсутствие
сработавшего паттерна не доказывает, что rug pull невозможен. SPL-токен
не содержит произвольного байткода; проверяются известные поля mint,
custody LP и уже посчитанный funder graph.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol

import structlog
from pydantic import BaseModel, ConfigDict, Field

from app.adapters.token_security import HolderBook, LpBook, MintView
from app.config import (
    FUNDER_CLUSTER_RISK_CONFIDENCE,
    HOLDER_CONCENTRATION_MAX_SHARE,
    LP_LOCKED_MIN_SHARE,
    RISK_CHECK_WEIGHTS,
)
from app.db.models import FunderCluster

logger = structlog.get_logger(__name__)

Clock = Callable[[], datetime]


class CheckResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    passed: bool | None
    detail: str


class RiskReport(BaseModel):
    """Aggregated known-pattern signals. ``risk_score`` of 100 is not a safety claim."""

    model_config = ConfigDict(extra="forbid")

    mint: str
    risk_score: int = Field(ge=0, le=100)
    checks: list[CheckResult]
    scanned_at: datetime
    # Raydium pool TVL in USD from the same lookup as the LP mint. None when
    # that response has no tvl, including pre-migration Pump.fun mints.
    liquidity_usd: Decimal | None = None
    # no_pool: Raydium listed nothing. lookup_failed: the request failed.
    # observed: a pool mint was returned. price_usd may still be missing.
    pool_status: str = "lookup_failed"
    price_usd: Decimal | None = None


class TokenFacts(Protocol):
    async def mint_view(self, mint: str) -> MintView: ...

    async def holder_book(self, mint: str) -> HolderBook: ...

    async def lp_book(self, mint: str) -> LpBook: ...


class FunderClusters(Protocol):
    async def build_graph_for_token(
        self,
        token_mint: str,
        top_n_holders: int = 20,
    ) -> list[FunderCluster]: ...


class RiskScanner:
    """Runs each check on its own. A network failure is recorded, not swallowed."""

    def __init__(
        self,
        facts: TokenFacts,
        funder_graph: FunderClusters,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._facts = facts
        self._funder_graph = funder_graph
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._liquidity_usd: Decimal | None = None
        self._pool_status = "lookup_failed"
        self._price_usd: Decimal | None = None
        self.holder_book: HolderBook | None = None
        self.lp_book: LpBook | None = None

    async def scan_token(self, mint: str) -> RiskReport:
        """All five checks. Callers that split the queue use the two halves below."""
        fast = await self.scan_fast(mint)
        heavy = await self.scan_heavy(mint)
        checks = [*fast.checks, *heavy.checks]
        report = self._report(mint, checks)
        logger.info(
            "risk_scan_finished",
            mint=mint,
            risk_score=report.risk_score,
            unavailable=[check.name for check in checks if check.passed is None],
        )
        return report

    async def scan_fast(self, mint: str) -> RiskReport:
        """Mint, freeze, and LP lock. The LP mint's largest-accounts read stays here."""
        self._liquidity_usd = None
        self._pool_status = "lookup_failed"
        self._price_usd = None
        self.holder_book = None
        self.lp_book = None
        checks = [
            await self._run("check_mint_authority", self._check_mint_authority, mint),
            await self._run("check_freeze_authority", self._check_freeze_authority, mint),
            await self._run("check_lp_lock", self._check_lp_lock, mint),
        ]
        return self._report(mint, checks)

    async def scan_heavy(self, mint: str) -> RiskReport:
        """Holder concentration and funder exposure. Does not clear the LP reading."""
        checks = [
            await self._run(
                "check_holder_concentration",
                self._check_holder_concentration,
                mint,
            ),
            await self._run(
                "check_funder_cluster_exposure",
                self._check_funder_cluster_exposure,
                mint,
            ),
        ]
        return self._report(mint, checks)

    def _report(self, mint: str, checks: list[CheckResult]) -> RiskReport:
        return RiskReport(
            mint=mint,
            risk_score=composite_risk_score(checks),
            checks=checks,
            scanned_at=self._clock(),
            liquidity_usd=self._liquidity_usd,
            pool_status=self._pool_status,
            price_usd=self._price_usd,
        )

    async def _run(
        self,
        name: str,
        check: Callable[[str], Awaitable[tuple[bool | None, str]]],
        mint: str,
    ) -> CheckResult:
        try:
            passed, detail = await check(mint)
        except Exception as exc:
            if name == "check_lp_lock":
                self._pool_status = "lookup_failed"
            logger.warning(
                "risk_check_unavailable",
                check=name,
                mint=mint,
                reason=type(exc).__name__,
                error_type=type(exc).__name__,
            )
            return CheckResult(
                name=name,
                passed=None,
                detail=f"{name}: unavailable ({type(exc).__name__})",
            )
        return CheckResult(name=name, passed=passed, detail=detail)

    async def _check_mint_authority(self, mint: str) -> tuple[bool | None, str]:
        view = await self._facts.mint_view(mint)
        if view.mint_authority is None:
            return (
                True,
                "Mint authority is null. Extra supply cannot be minted through this "
                "authority. This is only the absence of that pattern.",
            )
        return (
            False,
            f"Mint authority is still {view.mint_authority}. "
            "That authority can increase supply. High-risk pattern.",
        )

    async def _check_freeze_authority(self, mint: str) -> tuple[bool | None, str]:
        view = await self._facts.mint_view(mint)
        if view.freeze_authority is None:
            return (
                True,
                "Freeze authority is null. This mint field cannot freeze holder "
                "accounts. This is only the absence of that pattern.",
            )
        return (
            False,
            f"Freeze authority is still {view.freeze_authority}. "
            "That authority can freeze holder wallets. Critical-risk pattern.",
        )

    async def _check_lp_lock(self, mint: str) -> tuple[bool | None, str]:
        book = await self._facts.lp_book(mint)
        self.lp_book = book
        self._liquidity_usd = book.liquidity_usd
        self._price_usd = book.price_usd
        if book.lp_mint is None or book.supply_raw is None:
            self._pool_status = "no_pool"
            missing = ["lp_mint"]
            if book.liquidity_usd is None:
                missing.append("liquidity_usd")
            logger.warning(
                "liquidity_lookup_incomplete",
                mint=mint,
                reason="raydium_returned_no_pool",
                missing=missing,
            )
            return (
                None,
                "data incomplete: " + ", ".join(missing),
            )
        self._pool_status = "observed"
        if book.supply_raw <= 0:
            return (
                False,
                f"LP mint {book.lp_mint} has zero supply. Custody at a burn address "
                "or lock program cannot be measured. High-risk pattern: no locked "
                "LP balance was observed.",
            )
        locked_raw = sum(
            row.amount_raw for row in book.rows if row.is_burn_address or row.is_lock_program
        )
        share = Decimal(locked_raw) / Decimal(book.supply_raw)
        if share >= LP_LOCKED_MIN_SHARE:
            return (
                True,
                f"At least {LP_LOCKED_MIN_SHARE} of LP supply for {book.lp_mint} "
                "sits at a burn address or the Raydium LP lock program. "
                "This does not prove the liquidity can never be removed.",
            )
        return (
            False,
            f"Locked or burned LP share for {book.lp_mint} is {share}. "
            f"Below {LP_LOCKED_MIN_SHARE} at the incinerator or Raydium lock program. "
            "High-risk rug-pull pattern.",
        )

    async def _check_holder_concentration(self, mint: str) -> tuple[bool | None, str]:
        book = await self._facts.holder_book(mint)
        self.holder_book = book
        if book.supply_raw <= 0:
            raise ValueError("mint supply is zero")
        holders = [row for row in book.rows if not row.is_lp_pool]
        top = holders[:10]
        share = Decimal(sum(row.amount_raw for row in top)) / Decimal(book.supply_raw)
        excluded = sum(1 for row in book.rows if row.is_lp_pool)
        if share > HOLDER_CONCENTRATION_MAX_SHARE:
            return (
                False,
                f"Top {len(top)} holders excluding {excluded} LP-pool account(s) "
                f"hold {share} of supply. Above {HOLDER_CONCENTRATION_MAX_SHARE}. "
                "High-risk concentration pattern.",
            )
        return (
            True,
            f"Top {len(top)} holders excluding {excluded} LP-pool account(s) "
            f"hold {share} of supply, within {HOLDER_CONCENTRATION_MAX_SHARE}. "
            "This is not evidence the remaining supply is widely held.",
        )

    async def _check_funder_cluster_exposure(self, mint: str) -> tuple[bool | None, str]:
        # A skipped pre-migration walk leaves an empty list. That is not the
        # same result as a built graph with no cluster above the threshold.
        if getattr(self._funder_graph, "built", True) is False:
            return (
                None,
                "Funder graph was not built. An empty cluster list is not "
                "evidence that holders were funded independently.",
            )
        clusters = await self._funder_graph.build_graph_for_token(mint)
        flagged = [
            cluster
            for cluster in clusters
            if cluster.confidence is not None
            and cluster.confidence > FUNDER_CLUSTER_RISK_CONFIDENCE
        ]
        if not flagged:
            return (
                True,
                "No top-holder funder cluster has confidence above "
                f"{FUNDER_CLUSTER_RISK_CONFIDENCE}. This is not evidence the "
                "holders were funded independently.",
            )
        worst = max(flagged, key=lambda cluster: cluster.confidence or Decimal(0))
        return (
            False,
            "Top holders sit in a funder cluster "
            f"(root {worst.root_funder}, size {worst.cluster_size}, "
            f"confidence {worst.confidence}). Sybil-style funding pattern.",
        )


def unavailable_fields(flags: object) -> list[str]:
    """Check names whose source did not return data. A failed check is not in this list."""
    if not isinstance(flags, list):
        return []
    missing: list[str] = []
    for flag in flags:
        text = str(flag)
        if text.endswith(":unavailable"):
            missing.append(text.split(":", 1)[0])
    return missing


def composite_risk_score(checks: list[CheckResult]) -> int:
    """Sum weights of checks that passed. Failed and unavailable add nothing.

    Unavailable is not treated as a pass. The missing points keep the score
    from reading as if that pattern had been cleared.
    """
    score = 0
    for check in checks:
        if check.passed is True:
            score += RISK_CHECK_WEIGHTS[check.name]
    if score > 100:
        raise ValueError("risk score exceeded 100; check RISK_CHECK_WEIGHTS")
    return score
