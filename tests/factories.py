"""Builders for phase-1 rows.

These return unsaved ORM instances. Tests persist them through a session.
They are not stand-ins for Helius, Telegram, or any other external API.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from app.db.models import (
    BaitFlag,
    FunderCluster,
    Signal,
    Token,
    TrackedWallet,
    User,
    WalletFunder,
    WalletTx,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def make_token(**overrides: Any) -> Token:
    values: dict[str, Any] = {
        "mint": f"mint_{uuid.uuid4().hex}",
        "symbol": "SF",
        "name": "SignalForge Test",
        "first_seen_at": _now(),
        "liquidity_usd": Decimal("12000.50"),
        "holder_count": 42,
        "mint_authority_renounced": True,
        "freeze_authority_renounced": False,
        "risk_score": Decimal("37.5"),
        "risk_flags": ["low_liquidity"],
        "updated_at": _now(),
    }
    values.update(overrides)
    return Token(**values)


def make_tracked_wallet(**overrides: Any) -> TrackedWallet:
    values: dict[str, Any] = {
        "address": f"wallet_{uuid.uuid4().hex}",
        "label": "test-wallet",
        "first_seen_at": _now(),
        "win_rate": Decimal("0.55"),
        "avg_pnl_sol": Decimal("1.25"),
        "is_flagged_baiter": False,
    }
    values.update(overrides)
    return TrackedWallet(**values)


def make_wallet_tx(
    *,
    wallet_id: uuid.UUID,
    token_id: uuid.UUID,
    **overrides: Any,
) -> WalletTx:
    values: dict[str, Any] = {
        "wallet_id": wallet_id,
        "token_id": token_id,
        "action": "buy",
        "amount_sol": Decimal("2.5"),
        "tx_signature": f"sig_{uuid.uuid4().hex}",
        "block_time": _now(),
    }
    values.update(overrides)
    return WalletTx(**values)


def make_wallet_funder(**overrides: Any) -> WalletFunder:
    values: dict[str, Any] = {
        "wallet_address": f"wallet_{uuid.uuid4().hex}",
        "funder_address": f"funder_{uuid.uuid4().hex}",
        "amount_sol": Decimal("10"),
        "hop_depth": 1,
        "funded_at": _now(),
    }
    values.update(overrides)
    return WalletFunder(**values)


def make_funder_cluster(
    *,
    token_id: uuid.UUID | None = None,
    **overrides: Any,
) -> FunderCluster:
    member = f"wallet_{uuid.uuid4().hex}"
    values: dict[str, Any] = {
        "root_funder": f"funder_{uuid.uuid4().hex}",
        "member_wallets": [member],
        "cluster_size": 1,
        "token_id": token_id,
        "first_detected_at": _now(),
        "confidence": Decimal("0.75"),
    }
    values.update(overrides)
    return FunderCluster(**values)


def make_bait_flag(
    *,
    wallet_id: uuid.UUID,
    token_id: uuid.UUID,
    **overrides: Any,
) -> BaitFlag:
    values: dict[str, Any] = {
        "wallet_id": wallet_id,
        "token_id": token_id,
        "flag_type": "funder_cluster_member",
        "confidence": Decimal("0.9"),
        "detected_at": _now(),
        "evidence": {"hop_depth": 1},
    }
    values.update(overrides)
    return BaitFlag(**values)


def make_signal(*, token_id: uuid.UUID, **overrides: Any) -> Signal:
    values: dict[str, Any] = {
        "token_id": token_id,
        "wallet_score": Decimal("60"),
        "risk_score": Decimal("37.5"),
        "composite_score": Decimal("48"),
        "triggered_at": _now(),
        "notified": False,
    }
    values.update(overrides)
    return Signal(**values)


def make_user(**overrides: Any) -> User:
    values: dict[str, Any] = {
        "telegram_id": uuid.uuid4().int % 1_000_000_000_000,
        "tier": "free",
        "settings": {"min_score": 40, "max_risk": 70},
        "created_at": _now(),
    }
    values.update(overrides)
    return User(**values)
