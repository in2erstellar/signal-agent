"""Funder edges and sybil clusters. Addresses are not foreign keys.

A funder may never have been inserted into ``tracked_wallets``.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.db.models.tokens import Token


class WalletFunder(Base):
    __tablename__ = "wallet_funders"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    wallet_address: Mapped[str] = mapped_column(String, nullable=False)
    funder_address: Mapped[str] = mapped_column(String, nullable=False)
    amount_sol: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    hop_depth: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    funded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "wallet_address",
            "funder_address",
            "hop_depth",
            name="uq_wallet_funders_wallet_funder_hop",
        ),
        Index("ix_wallet_funders_wallet_address", "wallet_address"),
    )


class FunderCluster(Base):
    __tablename__ = "funder_clusters"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    root_funder: Mapped[str] = mapped_column(String, nullable=False, index=True)
    member_wallets: Mapped[list[Any]] = mapped_column(JSONB, nullable=False)
    cluster_size: Mapped[int] = mapped_column(Integer, nullable=False)
    token_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tokens.id", ondelete="SET NULL"),
    )
    first_detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    confidence: Mapped[Decimal | None] = mapped_column(Numeric)

    token: Mapped[Token | None] = relationship(back_populates="funder_clusters")

    __table_args__ = (
        CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="confidence_range",
        ),
    )
