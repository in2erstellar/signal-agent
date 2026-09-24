"""Request and response models for the public API."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.services.risk_scanner import RiskReport


class ErrorBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    detail: str


class PageParams(BaseModel):
    limit: int = Field(default=50, ge=1, le=200)
    offset: int = Field(default=0, ge=0)


class WalletCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: str = Field(min_length=32, max_length=44, pattern=r"^[1-9A-HJ-NP-Za-km-z]+$")
    label: str | None = Field(default=None, max_length=64)

    def normalized_label(self) -> str | None:
        if self.label is None:
            return None
        stripped = self.label.strip()
        return stripped or None


class WalletOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    address: str
    label: str | None
    first_seen_at: datetime | None
    win_rate: Decimal | None
    avg_pnl_sol: Decimal | None
    is_flagged_baiter: bool


class WalletList(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[WalletOut]
    limit: int
    offset: int
    total: int


class SignalOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    token_id: UUID
    mint: str
    symbol: str | None
    composite_score: Decimal
    risk_score: Decimal | None
    wallet_score: Decimal | None
    triggered_at: datetime
    notified: bool


class BaitFlagOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    wallet_address: str
    flag_type: str
    confidence: Decimal
    detected_at: datetime
    evidence: dict[str, Any] | None


class SignalDetail(SignalOut):
    risk_flags: list[str]
    bait_flags: list[BaitFlagOut]


class SignalList(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[SignalOut]
    limit: int
    offset: int
    total: int


class FunderClusterOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    root_funder: str
    member_wallets: list[str]
    cluster_size: int
    confidence: Decimal | None
    first_detected_at: datetime | None


class TokenRiskResponse(BaseModel):
    """Last stored scan. ``report.risk_score`` of 100 is not a safety claim."""

    model_config = ConfigDict(extra="forbid")

    report: RiskReport
    funder_clusters: list[FunderClusterOut]
    disclaimer: str


class UserSettingsOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    telegram_id: int
    min_score: Decimal
    max_risk: Decimal
    active: bool


class SettingsPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_score: Decimal | None = Field(default=None, ge=0, le=100)
    max_risk: Decimal | None = Field(default=None, ge=0, le=100)
    active: bool | None = None
