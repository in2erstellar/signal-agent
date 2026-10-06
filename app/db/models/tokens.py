"""Token risk snapshots. One row per Solana mint."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import CheckConstraint, DateTime, Index, Numeric, String, Uuid, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.db.models.bait_detection import BaitFlag
    from app.db.models.funder_graph import FunderCluster
    from app.db.models.signals import Signal
    from app.db.models.wallets import WalletTx


class Token(Base):
    __tablename__ = "tokens"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    mint: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    symbol: Mapped[str | None] = mapped_column(String)
    name: Mapped[str | None] = mapped_column(String)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    liquidity_usd: Mapped[Decimal | None] = mapped_column(Numeric)
    holder_count: Mapped[int | None] = mapped_column()
    mint_authority_renounced: Mapped[bool | None] = mapped_column()
    freeze_authority_renounced: Mapped[bool | None] = mapped_column()
    risk_score: Mapped[Decimal | None] = mapped_column(Numeric)
    risk_flags: Mapped[list[Any] | None] = mapped_column(JSONB)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    trace_id: Mapped[str | None] = mapped_column(String)
    # Pump.fun creates only. NULL means the row did not come from discovery.
    # pending_first_buy waits for a non-creator holder. qualified is enqueued.
    # dead is still stored after 24 hours with no such holder.
    discovery_status: Mapped[str | None] = mapped_column(String)
    creator_address: Mapped[str | None] = mapped_column(String)
    bonding_curve_key: Mapped[str | None] = mapped_column(String)
    # PumpPortal vSolInBondingCurve at create, in SOL. Not Raydium TVL and
    # never copied into liquidity_usd.
    bonding_curve_liquidity_sol: Mapped[Decimal | None] = mapped_column(Numeric)
    # skipped_pre_migration: bonding curve, graph not built.
    # pending_post_migration: migration event seen, graph not built yet.
    # completed_post_migration: graph built from post-migration holders.
    # built: wallet-discovered row whose graph finished.
    # failed_permanently: funded-by failed for good (plan 403, or the retry cap).
    # NULL: not decided.
    funder_graph_status: Mapped[str | None] = mapped_column(String)
    ai_review: Mapped[str | None] = mapped_column(String)
    ai_review_status: Mapped[str | None] = mapped_column(String)
    ai_review_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Creator-funder supply among scanned holders. Not an input to composite_score.
    # unavailable is stored as status + reason. A missing row is not a zero share.
    dev_distribution_status: Mapped[str | None] = mapped_column(String)
    dev_distribution_share: Mapped[Decimal | None] = mapped_column(Numeric)
    dev_distribution_reason: Mapped[str | None] = mapped_column(String)
    # pending: risk_scan_queue is acked and risk_heavy_queue has not finished.
    # complete: heavy checks were written. failed: the funder walk gave up.
    # NULL: the row never entered the split scan.
    heavy_scan_status: Mapped[str | None] = mapped_column(String)

    transactions: Mapped[list[WalletTx]] = relationship(
        back_populates="token",
        passive_deletes=True,
    )
    bait_flags: Mapped[list[BaitFlag]] = relationship(
        back_populates="token",
        passive_deletes=True,
    )
    signals: Mapped[list[Signal]] = relationship(
        back_populates="token",
        passive_deletes=True,
    )
    funder_clusters: Mapped[list[FunderCluster]] = relationship(
        back_populates="token",
        passive_deletes=True,
    )

    __table_args__ = (
        CheckConstraint(
            "risk_score IS NULL OR (risk_score >= 0 AND risk_score <= 100)",
            name="risk_score_range",
        ),
        CheckConstraint(
            "discovery_status IS NULL OR discovery_status IN "
            "('pending_first_buy', 'qualified', 'dead')",
            name="discovery_status_values",
        ),
        CheckConstraint(
            "funder_graph_status IS NULL OR funder_graph_status IN "
            "('skipped_pre_migration', 'pending_post_migration', "
            "'completed_post_migration', 'built', 'failed_permanently')",
            name="funder_graph_status_values",
        ),
        CheckConstraint(
            "ai_review_status IS NULL OR ai_review_status IN "
            "('pending', 'completed', 'failed', 'skipped_budget_exceeded', 'truncated')",
            name="ai_review_status_values",
        ),
        CheckConstraint(
            "dev_distribution_status IS NULL OR dev_distribution_status IN "
            "('measured', 'unavailable')",
            name="dev_distribution_status_values",
        ),
        CheckConstraint(
            "heavy_scan_status IS NULL OR heavy_scan_status IN "
            "('pending', 'complete', 'failed')",
            name="heavy_scan_status_values",
        ),
        Index("ix_tokens_creator_address", "creator_address"),
    )
