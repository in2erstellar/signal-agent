"""Token risk snapshots. One row per Solana mint."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import CheckConstraint, DateTime, Numeric, String, Uuid, text
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
    # skipped_pre_migration: bonding curve, graph not built.
    # pending_post_migration: migration event seen, graph not built yet.
    # completed_post_migration: graph built from post-migration holders.
    # built: wallet-discovered row whose graph finished. NULL: not decided.
    funder_graph_status: Mapped[str | None] = mapped_column(String)
    ai_review: Mapped[str | None] = mapped_column(String)
    ai_review_status: Mapped[str | None] = mapped_column(String)
    ai_review_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

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
            "'completed_post_migration', 'built')",
            name="funder_graph_status_values",
        ),
        CheckConstraint(
            "ai_review_status IS NULL OR ai_review_status IN "
            "('pending', 'completed', 'failed', 'skipped_budget_exceeded', 'truncated')",
            name="ai_review_status_values",
        ),
    )
