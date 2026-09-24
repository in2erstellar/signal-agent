"""Build a RiskReport from the scan the worker already stored.

Passed checks are the known checks absent from ``risk_flags``. The worker
persists failures and unavailable checks only, so a passed detail says
that and does not invent an on-chain observation.
"""

from __future__ import annotations

from datetime import datetime
from decimal import ROUND_HALF_UP

from app.config import RISK_CHECK_WEIGHTS
from app.db.models import FunderCluster, Token
from app.services.risk_scanner import CheckResult, RiskReport

STORED_SCAN_DISCLAIMER = (
    "Stored scan of known on-chain patterns. "
    "A high risk_score is not a claim that the token is safe."
)


def report_from_token(token: Token) -> RiskReport:
    if token.risk_score is None:
        raise ValueError("token has no stored risk score")
    flags = [str(flag) for flag in token.risk_flags] if isinstance(token.risk_flags, list) else []
    scanned_at = token.updated_at or token.first_seen_at
    score = int(token.risk_score.to_integral_value(rounding=ROUND_HALF_UP))
    return RiskReport(
        mint=token.mint,
        risk_score=score,
        checks=_checks(flags),
        scanned_at=scanned_at if isinstance(scanned_at, datetime) else token.first_seen_at,
    )


def clusters_from_rows(rows: list[FunderCluster]) -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for row in rows:
        members = row.member_wallets if isinstance(row.member_wallets, list) else []
        payload.append(
            {
                "id": row.id,
                "root_funder": row.root_funder,
                "member_wallets": [str(member) for member in members],
                "cluster_size": row.cluster_size,
                "confidence": row.confidence,
                "first_detected_at": row.first_detected_at,
            }
        )
    return payload


def _checks(flags: list[str]) -> list[CheckResult]:
    unavailable = {flag.split(":", 1)[0] for flag in flags if flag.endswith(":unavailable")}
    failed = {flag for flag in flags if not flag.endswith(":unavailable")}
    checks: list[CheckResult] = []
    for name in RISK_CHECK_WEIGHTS:
        if name in unavailable:
            detail = next(flag for flag in flags if flag.startswith(f"{name}:"))
            checks.append(CheckResult(name=name, passed=None, detail=detail))
        elif name in failed:
            checks.append(CheckResult(name=name, passed=False, detail=name))
        else:
            checks.append(
                CheckResult(
                    name=name,
                    passed=True,
                    detail="not flagged in the stored scan",
                )
            )
    known = set(RISK_CHECK_WEIGHTS)
    for flag in flags:
        base = flag.split(":", 1)[0]
        if base not in known:
            checks.append(CheckResult(name=base, passed=False, detail=flag))
    return checks
