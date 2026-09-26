"""Risk-scanner tests. On-chain facts and the funder graph are fakes. No sockets."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.adapters.helius import HeliusError
from app.adapters.raydium import RaydiumError
from app.adapters.token_security import HolderBook, HolderRow, LpBook, LpHolderRow, MintView
from app.config import (
    FUNDER_CLUSTER_RISK_CONFIDENCE,
    HOLDER_CONCENTRATION_MAX_SHARE,
    LP_LOCKED_MIN_SHARE,
    RISK_CHECK_WEIGHTS,
)
from app.db.models import FunderCluster
from app.services.risk_scanner import CheckResult, RiskReport, RiskScanner, composite_risk_score

MINT = "TokenMint111111111111111111111111111111111"
AUTHORITY = "Auth1111111111111111111111111111111111111"
LP_MINT = "LpMint111111111111111111111111111111111111"
BURN = "1nc1nerator11111111111111111111111111111111"
LOCK_OWNER = "LockEscrow11111111111111111111111111111111"
POOL = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
WHEN = datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)


class FakeFacts:
    def __init__(
        self,
        *,
        mint_view: MintView | None = None,
        holder_book: HolderBook | None = None,
        lp_book: LpBook | None = None,
        errors: dict[str, Exception] | None = None,
    ) -> None:
        self.mint_view_value = mint_view or MintView(None, None, 1_000)
        self.holder_book_value = holder_book or HolderBook(1_000, [])
        self.lp_book_value = lp_book or LpBook(LP_MINT, 1_000, [])
        self.errors = errors or {}
        self.calls: list[str] = []

    async def mint_view(self, mint: str) -> MintView:
        self.calls.append(f"mint_view:{mint}")
        error = self.errors.get("mint_view")
        if error is not None:
            raise error
        return self.mint_view_value

    async def holder_book(self, mint: str) -> HolderBook:
        self.calls.append(f"holder_book:{mint}")
        error = self.errors.get("holder_book")
        if error is not None:
            raise error
        return self.holder_book_value

    async def lp_book(self, mint: str) -> LpBook:
        self.calls.append(f"lp_book:{mint}")
        error = self.errors.get("lp_book")
        if error is not None:
            raise error
        return self.lp_book_value


class FakeGraph:
    def __init__(
        self,
        clusters: list[FunderCluster] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.clusters = clusters or []
        self.error = error
        self.calls: list[tuple[str, int]] = []

    async def build_graph_for_token(
        self,
        token_mint: str,
        top_n_holders: int = 20,
    ) -> list[FunderCluster]:
        self.calls.append((token_mint, top_n_holders))
        if self.error is not None:
            raise self.error
        return self.clusters


def _cluster(confidence: Decimal | None, root: str = "RootFunder") -> FunderCluster:
    return FunderCluster(
        root_funder=root,
        member_wallets=["a", "b", "c"],
        cluster_size=6,
        confidence=confidence,
    )


def _scanner(
    facts: FakeFacts | None = None,
    graph: FakeGraph | None = None,
) -> tuple[RiskScanner, FakeFacts, FakeGraph]:
    facts = facts or FakeFacts()
    graph = graph or FakeGraph()
    return (
        RiskScanner(facts, graph, clock=lambda: WHEN),
        facts,
        graph,
    )


def _assert_risk_language(detail: str) -> None:
    lowered = detail.lower()
    assert "safe" not in lowered
    assert "безопас" not in lowered


def test_weights_are_config_constants_and_sum_to_100() -> None:
    assert RISK_CHECK_WEIGHTS == {
        "check_mint_authority": 20,
        "check_freeze_authority": 25,
        "check_lp_lock": 25,
        "check_holder_concentration": 15,
        "check_funder_cluster_exposure": 15,
    }
    assert sum(RISK_CHECK_WEIGHTS.values()) == 100
    assert HOLDER_CONCENTRATION_MAX_SHARE == Decimal("0.50")
    assert FUNDER_CLUSTER_RISK_CONFIDENCE == Decimal("0.5")
    assert LP_LOCKED_MIN_SHARE == Decimal("0.95")


async def test_mint_authority_renounced_and_still_set() -> None:
    renounced, _, _ = _scanner(FakeFacts(mint_view=MintView(None, "freeze", 1)))
    passed, detail = await renounced._check_mint_authority(MINT)
    assert passed is True
    _assert_risk_language(detail)

    active, _, _ = _scanner(FakeFacts(mint_view=MintView(AUTHORITY, None, 1)))
    passed, detail = await active._check_mint_authority(MINT)
    assert passed is False
    assert AUTHORITY in detail
    assert "High-risk" in detail
    _assert_risk_language(detail)


async def test_freeze_authority_renounced_and_still_set() -> None:
    renounced, _, _ = _scanner(FakeFacts(mint_view=MintView("mint", None, 1)))
    passed, detail = await renounced._check_freeze_authority(MINT)
    assert passed is True
    _assert_risk_language(detail)

    active, _, _ = _scanner(FakeFacts(mint_view=MintView(None, AUTHORITY, 1)))
    passed, detail = await active._check_freeze_authority(MINT)
    assert passed is False
    assert AUTHORITY in detail
    assert "Critical-risk" in detail
    _assert_risk_language(detail)


async def test_lp_lock_burn_and_lock_program_pass_at_threshold() -> None:
    book = LpBook(
        LP_MINT,
        1_000,
        [
            LpHolderRow(BURN, 700, is_burn_address=True, is_lock_program=False),
            LpHolderRow(LOCK_OWNER, 250, is_burn_address=False, is_lock_program=True),
            LpHolderRow("wallet", 50, is_burn_address=False, is_lock_program=False),
        ],
    )
    scanner, _, _ = _scanner(FakeFacts(lp_book=book))
    passed, detail = await scanner._check_lp_lock(MINT)
    assert passed is True
    assert LP_MINT in detail
    _assert_risk_language(detail)


async def test_lp_lock_below_threshold_is_high_risk() -> None:
    book = LpBook(
        LP_MINT,
        1_000,
        [LpHolderRow(BURN, 949, is_burn_address=True, is_lock_program=False)],
    )
    scanner, _, _ = _scanner(FakeFacts(lp_book=book))
    passed, detail = await scanner._check_lp_lock(MINT)
    assert passed is False
    assert "High-risk" in detail
    _assert_risk_language(detail)


async def test_lp_lock_missing_pool_is_incomplete_not_a_score() -> None:
    scanner, _, _ = _scanner(FakeFacts(lp_book=LpBook(None, None, [])))
    passed, detail = await scanner._check_lp_lock(MINT)
    assert passed is None
    assert detail == "data incomplete: lp_mint, liquidity_usd"
    _assert_risk_language(detail)


async def test_lp_lock_zero_supply_fails() -> None:
    scanner, _, _ = _scanner(FakeFacts(lp_book=LpBook(LP_MINT, 0, [])))
    passed, detail = await scanner._check_lp_lock(MINT)
    assert passed is False
    assert "zero supply" in detail


async def test_holder_concentration_excludes_lp_and_allows_exact_threshold() -> None:
    book = HolderBook(
        supply_raw=1_000,
        rows=[
            HolderRow(POOL, 400, is_lp_pool=True),
            HolderRow("h1", 500, is_lp_pool=False),
        ],
    )
    scanner, _, _ = _scanner(FakeFacts(holder_book=book))
    passed, detail = await scanner._check_holder_concentration(MINT)
    assert passed is True
    assert "0.5" in detail
    _assert_risk_language(detail)


async def test_holder_concentration_above_threshold_is_high_risk() -> None:
    rows = [HolderRow(f"h{index}", 51, is_lp_pool=False) for index in range(10)]
    rows.append(HolderRow(POOL, 200, is_lp_pool=True))
    book = HolderBook(supply_raw=1_000, rows=rows)
    scanner, _, _ = _scanner(FakeFacts(holder_book=book))
    passed, detail = await scanner._check_holder_concentration(MINT)
    assert passed is False
    assert Decimal("0.51") == Decimal(510) / Decimal(1000)
    assert "0.51" in detail
    assert "High-risk" in detail
    _assert_risk_language(detail)


async def test_holder_concentration_zero_supply_raises() -> None:
    scanner, _, _ = _scanner(FakeFacts(holder_book=HolderBook(0, [])))
    with pytest.raises(ValueError, match="mint supply is zero"):
        await scanner._check_holder_concentration(MINT)


async def test_funder_cluster_at_threshold_is_not_a_flag() -> None:
    graph = FakeGraph([_cluster(Decimal("0.5"))])
    scanner, _, _ = _scanner(graph=graph)
    passed, detail = await scanner._check_funder_cluster_exposure(MINT)
    assert passed is True
    assert graph.calls == [(MINT, 20)]
    _assert_risk_language(detail)


async def test_funder_cluster_above_threshold_is_a_flag() -> None:
    graph = FakeGraph(
        [
            _cluster(Decimal("0.4"), root="QuietRoot"),
            _cluster(Decimal("0.51"), root="HotRoot"),
        ]
    )
    scanner, _, _ = _scanner(graph=graph)
    passed, detail = await scanner._check_funder_cluster_exposure(MINT)
    assert passed is False
    assert "HotRoot" in detail
    assert "QuietRoot" not in detail
    assert "Sybil-style" in detail
    _assert_risk_language(detail)


async def test_scan_token_aggregates_all_five_checks() -> None:
    facts = FakeFacts(
        mint_view=MintView(None, AUTHORITY, 1_000),
        holder_book=HolderBook(
            1_000,
            [HolderRow("whale", 600, is_lp_pool=False)],
        ),
        lp_book=LpBook(
            LP_MINT,
            1_000,
            [LpHolderRow(BURN, 980, is_burn_address=True, is_lock_program=False)],
        ),
    )
    graph = FakeGraph([])
    scanner, _, _ = _scanner(facts, graph)
    report = await scanner.scan_token(MINT)

    by_name = {check.name: check for check in report.checks}
    assert list(by_name) == list(RISK_CHECK_WEIGHTS)
    assert by_name["check_mint_authority"].passed is True
    assert by_name["check_freeze_authority"].passed is False
    assert by_name["check_lp_lock"].passed is True
    assert by_name["check_holder_concentration"].passed is False
    assert by_name["check_funder_cluster_exposure"].passed is True

    expected = (
        RISK_CHECK_WEIGHTS["check_mint_authority"]
        + RISK_CHECK_WEIGHTS["check_lp_lock"]
        + RISK_CHECK_WEIGHTS["check_funder_cluster_exposure"]
    )
    assert report.risk_score == expected == 60
    assert report.mint == MINT
    assert report.scanned_at == WHEN
    assert isinstance(report, RiskReport)
    for check in report.checks:
        _assert_risk_language(check.detail)
        assert "unavailable" not in check.detail


async def test_scan_token_marks_a_failed_check_unavailable_and_keeps_the_rest() -> None:
    facts = FakeFacts(
        mint_view=MintView(None, None, 1_000),
        holder_book=HolderBook(1_000, [HolderRow("h", 100, is_lp_pool=False)]),
        lp_book=LpBook(LP_MINT, 1_000, []),
        errors={"lp_book": RaydiumError("pool api down")},
    )
    graph = FakeGraph([_cluster(Decimal("0.2"))])
    scanner, _, _ = _scanner(facts, graph)
    report = await scanner.scan_token(MINT)

    by_name = {check.name: check for check in report.checks}
    assert len(report.checks) == 5
    unavailable = by_name["check_lp_lock"]
    assert unavailable.passed is None
    assert unavailable.detail == "check_lp_lock: unavailable (RaydiumError)"
    assert all(
        check.passed is True
        for name, check in by_name.items()
        if name != "check_lp_lock"
    )
    assert "holder_book" in "".join(facts.calls)
    assert "mint_view" in "".join(facts.calls)
    assert graph.calls == [(MINT, 20)]
    expected = 100 - RISK_CHECK_WEIGHTS["check_lp_lock"]
    assert report.risk_score == expected == 75


async def test_zero_mint_supply_makes_only_concentration_unavailable() -> None:
    facts = FakeFacts(
        mint_view=MintView(None, None, 0),
        holder_book=HolderBook(0, []),
        lp_book=LpBook(
            LP_MINT,
            100,
            [LpHolderRow(BURN, 100, is_burn_address=True, is_lock_program=False)],
        ),
    )
    scanner, _, _ = _scanner(facts, FakeGraph())
    report = await scanner.scan_token(MINT)
    by_name = {check.name: check for check in report.checks}
    concentration = by_name["check_holder_concentration"]
    assert concentration.passed is None
    assert concentration.detail == "check_holder_concentration: unavailable (ValueError)"
    assert by_name["check_mint_authority"].passed is True
    assert by_name["check_freeze_authority"].passed is True
    assert by_name["check_lp_lock"].passed is True
    assert by_name["check_funder_cluster_exposure"].passed is True
    assert report.risk_score == 100 - RISK_CHECK_WEIGHTS["check_holder_concentration"]


async def test_helius_error_on_funder_graph_does_not_abort_the_scan() -> None:
    facts = FakeFacts(
        mint_view=MintView(AUTHORITY, AUTHORITY, 1_000),
        holder_book=HolderBook(1_000, [HolderRow("h", 600, is_lp_pool=False)]),
        lp_book=LpBook(None, None, []),
    )
    scanner, _, _ = _scanner(facts, FakeGraph(error=HeliusError("funded-by 403")))
    report = await scanner.scan_token(MINT)
    funder = next(check for check in report.checks if check.name == "check_funder_cluster_exposure")
    assert funder.passed is None
    assert funder.detail == "check_funder_cluster_exposure: unavailable (HeliusError)"
    assert report.risk_score == 0
    assert composite_risk_score(report.checks) == 0


def test_composite_score_ignores_failed_and_unavailable() -> None:
    checks = [
        CheckResult(name="check_mint_authority", passed=True, detail="ok"),
        CheckResult(name="check_freeze_authority", passed=False, detail="flag"),
        CheckResult(name="check_lp_lock", passed=None, detail="check_lp_lock: unavailable (RaydiumError)"),
        CheckResult(name="check_holder_concentration", passed=True, detail="ok"),
        CheckResult(name="check_funder_cluster_exposure", passed=False, detail="flag"),
    ]
    assert composite_risk_score(checks) == 20 + 15
