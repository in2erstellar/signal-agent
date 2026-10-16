"""The 12-line checklist on a signal card.

Every line is pass, fail, or unavailable, and unavailable is never a pass.
A signal needs every line in ``QUALITY_REQUIRED_CHECKS`` to pass and at least
``QUALITY_MIN_PASSED`` of the 12 to pass. These lines filter out known scam
patterns. None of them predicts price.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from app.adapters.dexscreener import DexQuote
from app.adapters.token_security import MintView
from app.config import (
    HOLDER_CONCENTRATION_MAX_SHARE,
    LP_LOCKED_MIN_SHARE,
    QUALITY_CLONE_WINDOW_HOURS,
    QUALITY_LAUNCH_SLOTS,
    QUALITY_MAX_LAUNCH_TRADES,
    QUALITY_MIN_LIQUIDITY_USD,
    QUALITY_MIN_PASSED,
    QUALITY_REQUIRED_CHECKS,
    SIGNAL_MAX_CREATOR_SHARE,
)

PASS = "pass"
FAIL = "fail"
UNAVAILABLE = "unavailable"

GROUP_TITLES = {"contract": "Contract", "holders": "Holders", "market": "Market"}
# Lines that need a call outside the database. The rest come from the stored scan.
EXTERNAL_CHECKS = ("token_extensions", "launch_trades", "liquidity", "buy_pressure", "socials")
CHECK_COUNT = 12

# Token-2022 extensions that let someone take, freeze, or tax holder balances.
_RISKY_EXTENSIONS = {
    "transferFeeConfig": "transfer fee",
    "transferHook": "transfer hook",
    "permanentDelegate": "permanent delegate",
    "defaultAccountState": "frozen by default",
    "nonTransferable": "non-transferable",
    "pausableConfig": "pausable",
}


@dataclass(frozen=True)
class QualityCheck:
    key: str
    group: str
    status: str
    text: str

    def as_dict(self) -> dict[str, str]:
        return {"key": self.key, "group": self.group, "status": self.status, "text": self.text}


@dataclass(frozen=True)
class ExternalReadings:
    """What came back from outside the database. ``None`` means unavailable."""

    dex: DexQuote | None = None
    mint_view: MintView | None = None
    launch_trades: int | None = None
    # Creator-written links from the metadata JSON. ``{}`` means none listed.
    metadata_links: dict[str, str] | None = None

    @property
    def complete(self) -> bool:
        return (
            self.dex is not None
            and self.mint_view is not None
            and self.launch_trades is not None
            and (self.metadata_links is not None or bool(self.dex.social_links))
        )


@dataclass(frozen=True)
class QualityVerdict:
    passed: int
    total: int
    required_failed: tuple[str, ...]

    @property
    def emits(self) -> bool:
        return not self.required_failed and self.passed >= QUALITY_MIN_PASSED

    @property
    def score(self) -> Decimal:
        """Share of passed lines on a 0-100 scale, stored as ``composite_score``."""
        value = Decimal(self.passed) * Decimal(100) / Decimal(self.total)
        return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def build_checklist(
    *,
    risk_flags: object,
    creator_context: dict[str, Any] | None,
    clone_count: int | None,
    symbol: str | None,
    readings: ExternalReadings,
) -> list[QualityCheck]:
    flags = [str(flag) for flag in risk_flags] if isinstance(risk_flags, list) else []
    lp_share = _percent(LP_LOCKED_MIN_SHARE)
    holder_max = _percent(HOLDER_CONCENTRATION_MAX_SHARE)
    return [
        _from_flag(
            flags,
            "check_mint_authority",
            "mint_authority",
            "contract",
            label="Mint authority",
            passed="Mint authority revoked",
            failed="Mint authority still set: more supply can be minted",
        ),
        _from_flag(
            flags,
            "check_freeze_authority",
            "freeze_authority",
            "contract",
            label="Freeze authority",
            passed="Freeze authority revoked",
            failed="Freeze authority still set: wallets can be frozen",
        ),
        _from_flag(
            flags,
            "check_lp_lock",
            "lp_locked",
            "contract",
            label="LP lock",
            passed=f"LP burned or locked (at least {lp_share})",
            failed=f"LP not burned or locked (under {lp_share})",
        ),
        _extensions_check(readings.mint_view),
        _from_flag(
            flags,
            "check_holder_concentration",
            "holder_concentration",
            "holders",
            label="Top-10 holder share",
            passed=f"Top-10 holders own at most {holder_max}",
            failed=f"Top-10 holders own over {holder_max}",
        ),
        _from_flag(
            flags,
            "check_funder_cluster_exposure",
            "funder_cluster",
            "holders",
            label="Funder cluster",
            passed="No top holders funded by one wallet",
            failed="Top holders funded by one wallet (sybil pattern)",
        ),
        _creator_share_check(creator_context),
        _launch_check(readings.launch_trades),
        _liquidity_check(readings.dex),
        _buy_pressure_check(readings.dex),
        _clone_check(clone_count, symbol),
        _socials_check(readings),
    ]


def verdict(checks: list[QualityCheck]) -> QualityVerdict:
    by_key = {check.key: check for check in checks}
    required_failed = tuple(
        key for key in QUALITY_REQUIRED_CHECKS if key not in by_key or by_key[key].status != PASS
    )
    return QualityVerdict(
        passed=sum(1 for check in checks if check.status == PASS),
        total=len(checks),
        required_failed=required_failed,
    )


_REVIEW_STATUS = {PASS: "pass", FAIL: "fail", UNAVAILABLE: "no data"}


def checklist_for_review(context: object) -> tuple[str, ...]:
    """Stored checklist rows as ``status: text`` lines for the AI note."""
    rows = context.get("quality_checks") if isinstance(context, dict) else None
    if not isinstance(rows, list):
        return ()
    return tuple(
        f"{_REVIEW_STATUS.get(str(row.get('status')), 'no data')}: {row.get('text', '')}"
        for row in rows
        if isinstance(row, dict)
    )


# Short forms for the monospace box: (no-data label, passed, failed). A
# ``{n}`` is filled from the numbers in the stored long text.
def _short_forms() -> dict[str, tuple[str, str, str]]:
    return {
        "mint_authority": ("Mint", "Mint revoked", "Mint still active"),
        "freeze_authority": ("Freeze", "Freeze revoked", "Freeze still active"),
        "lp_locked": (
            "LP",
            f"LP burned {chr(8805)}{_percent(LP_LOCKED_MIN_SHARE)}",
            "LP not locked",
        ),
        "token_extensions": ("Extensions", "No fee/hook/delegate", "Risky extensions"),
        "holder_concentration": (
            "Top-10",
            f"Top-10 {chr(8804)}{_percent(HOLDER_CONCENTRATION_MAX_SHARE)}",
            f"Top-10 >{_percent(HOLDER_CONCENTRATION_MAX_SHARE)}",
        ),
        "funder_cluster": ("Funders", "No funder cluster", "Funder cluster found"),
        "socials": ("Socials", "Socials (unverified)", "No socials"),
    }


SHORT_WIDTH = 25


def short_line(row: dict[str, Any]) -> str:
    """At most ``SHORT_WIDTH`` characters for one stored checklist row."""
    key = str(row.get("key", ""))
    status = str(row.get("status", UNAVAILABLE))
    text = str(row.get("text", ""))
    numbers = re.findall(r"\$?\d+(?:\.\d+)?[k%M]?", text)
    if status == UNAVAILABLE:
        label = _short_forms().get(key, ("",))[0] or _NO_DATA_LABELS.get(key) or text.split(":")[0]
        return _fit(f"{label}: no data")
    forms = _short_forms()
    if key in forms:
        return _fit(forms[key][1 if status == PASS else 2])
    if key == "creator_share" and numbers:
        return _fit(f"Creator {numbers[0]} ({chr(8804)}{_percent(SIGNAL_MAX_CREATOR_SHARE)})")
    if key == "launch_trades" and numbers:
        return _fit(
            f"Launch trades {numbers[-2] if len(numbers) > 2 else numbers[0]} ({chr(8804)}{QUALITY_MAX_LAUNCH_TRADES})"
        )
    if key == "liquidity" and numbers:
        return _fit(
            f"Liq {numbers[0]} ({chr(8805)}{_usd(QUALITY_MIN_LIQUIDITY_USD).replace('.0k', 'k')})"
        )
    if key == "buy_pressure" and len(numbers) >= 3:
        return _fit(f"5m {numbers[1]} buy/{numbers[2]} sell")
    if key == "not_a_clone":
        if status == PASS:
            return "Unique ticker"
        return _fit(f"Ticker clones: {numbers[0]}" if numbers else "Ticker clones")
    return _fit(text)


_NO_DATA_LABELS = {
    "creator_share": "Creator",
    "launch_trades": "Launch trades",
    "liquidity": "Liquidity",
    "buy_pressure": "5m trades",
    "not_a_clone": "Clones",
}


def _fit(text: str) -> str:
    return text if len(text) <= SHORT_WIDTH else text[: SHORT_WIDTH - 1] + "…"


def best_possible(checks: list[QualityCheck]) -> int:
    """Passed lines if every external line that is not in yet came back as a pass."""
    return sum(
        1
        for check in checks
        if check.status == PASS or (check.key in EXTERNAL_CHECKS and check.status == UNAVAILABLE)
    )


def _from_flag(
    flags: list[str],
    flag: str,
    key: str,
    group: str,
    *,
    label: str,
    passed: str,
    failed: str,
) -> QualityCheck:
    if f"{flag}:unavailable" in flags:
        return QualityCheck(key, group, UNAVAILABLE, f"{label}: no data")
    if flag in flags or any(text.startswith(f"{flag}:") for text in flags):
        return QualityCheck(key, group, FAIL, failed)
    return QualityCheck(key, group, PASS, passed)


def _extensions_check(view: MintView | None) -> QualityCheck:
    key, group = "token_extensions", "contract"
    if view is None:
        return QualityCheck(key, group, UNAVAILABLE, "Token extensions: no data")
    risky = [label for name, state in view.extensions if (label := _risky(name, state))]
    if risky:
        return QualityCheck(key, group, FAIL, "Risky token extensions: " + ", ".join(risky))
    return QualityCheck(key, group, PASS, "No transfer fee, hook, or permanent delegate")


def _risky(name: str, state: dict[str, Any]) -> str | None:
    label = _RISKY_EXTENSIONS.get(name)
    if label is None:
        return None
    if name == "transferFeeConfig":
        fees = [state.get("newerTransferFee"), state.get("olderTransferFee")]
        charged = any(
            isinstance(fee, dict) and int(fee.get("transferFeeBasisPoints") or 0) > 0
            for fee in fees
        )
        if not charged and not state.get("transferFeeConfigAuthority"):
            return None
    elif name == "transferHook":
        if not state.get("programId") and not state.get("authority"):
            return None
    elif name == "permanentDelegate":
        if not state.get("delegate"):
            return None
    elif name == "defaultAccountState":
        if state.get("accountState") != "frozen":
            return None
    elif name == "pausableConfig":
        if not state.get("authority"):
            return None
    return label


def _creator_share_check(context: dict[str, Any] | None) -> QualityCheck:
    key, group = "creator_share", "holders"
    limit = _percent(SIGNAL_MAX_CREATOR_SHARE)
    if (
        context
        and context.get("distribution_status") == "measured"
        and context.get("distribution_share") is not None
    ):
        share = Decimal(str(context["distribution_share"]))
        shown = _percent(share)
        if share <= SIGNAL_MAX_CREATOR_SHARE:
            return QualityCheck(
                key, group, PASS, f"Creator and linked wallets: {shown} (max {limit})"
            )
        return QualityCheck(key, group, FAIL, f"Creator and linked wallets: {shown} (max {limit})")
    return QualityCheck(key, group, UNAVAILABLE, "Creator and linked wallets: no data")


def _launch_check(trades: int | None) -> QualityCheck:
    key, group = "launch_trades", "holders"
    window = f"first {QUALITY_LAUNCH_SLOTS} blocks"
    if trades is None:
        return QualityCheck(key, group, UNAVAILABLE, f"Trades in the {window}: no data")
    text = f"Trades in the {window}: {trades} (max {QUALITY_MAX_LAUNCH_TRADES})"
    status = PASS if trades <= QUALITY_MAX_LAUNCH_TRADES else FAIL
    return QualityCheck(key, group, status, text)


def _liquidity_check(dex: DexQuote | None) -> QualityCheck:
    key, group = "liquidity", "market"
    floor = _usd(QUALITY_MIN_LIQUIDITY_USD)
    if dex is None or dex.liquidity_usd is None:
        return QualityCheck(key, group, UNAVAILABLE, "Liquidity: no data")
    text = f"Liquidity {_usd(dex.liquidity_usd)} (min {floor})"
    status = PASS if dex.liquidity_usd >= QUALITY_MIN_LIQUIDITY_USD else FAIL
    return QualityCheck(key, group, status, text)


def _buy_pressure_check(dex: DexQuote | None) -> QualityCheck:
    key, group = "buy_pressure", "market"
    if dex is None or dex.buys_5m is None or dex.sells_5m is None:
        return QualityCheck(key, group, UNAVAILABLE, "Last 5 min buys vs sells: no data")
    text = f"Last 5 min: {_plural(dex.buys_5m, 'buy')} / {_plural(dex.sells_5m, 'sell')}"
    status = PASS if dex.buys_5m > dex.sells_5m else FAIL
    return QualityCheck(key, group, status, text)


def _clone_check(clone_count: int | None, symbol: str | None) -> QualityCheck:
    key, group = "not_a_clone", "market"
    window = f"{QUALITY_CLONE_WINDOW_HOURS}h"
    if clone_count is None or not symbol:
        return QualityCheck(key, group, UNAVAILABLE, "Ticker clones: no data")
    if clone_count == 0:
        return QualityCheck(key, group, PASS, f"Unique ticker (no copies in {window})")
    return QualityCheck(
        key,
        group,
        FAIL,
        f"{_plural(clone_count, 'other token')} named {symbol} in {window}",
    )


def _socials_check(readings: ExternalReadings) -> QualityCheck:
    key, group = "socials", "market"
    names = sorted((readings.metadata_links or {}).keys())
    if readings.dex is not None and readings.dex.social_links and "dexscreener" not in names:
        names.append("dexscreener profile")
    if names:
        return QualityCheck(key, group, PASS, f"Socials listed: {', '.join(names)} (not verified)")
    if readings.metadata_links is None:
        return QualityCheck(key, group, UNAVAILABLE, "Socials: no data")
    return QualityCheck(key, group, FAIL, "No socials listed")


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _percent(share: Decimal) -> str:
    return f"{(share * Decimal(100)).quantize(Decimal('1'), rounding=ROUND_HALF_UP)}%"


def _usd(value: Decimal) -> str:
    if value >= 1000:
        return f"${(value / Decimal(1000)).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)}k"
    return f"${value.quantize(Decimal('1'), rounding=ROUND_HALF_UP)}"
