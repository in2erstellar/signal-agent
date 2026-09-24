"""Bait flags raised against a tracked wallet on a specific token."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Numeric, String, Uuid, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.db.models.tokens import Token
    from app.db.models.wallets import TrackedWallet

BAIT_FLAG_TYPES = (
    "dump_after_copytrade_influx",
    "hidden_dual_position",
    "funder_cluster_member",
)


class BaitFlag(Base):
    __tablename__ = "bait_flags"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    wallet_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tracked_wallets.id", ondelete="CASCADE"),
        nullable=False,
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tokens.id", ondelete="CASCADE"),
        nullable=False,
    )
    flag_type: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSONB)

    wallet: Mapped[TrackedWallet] = relationship(back_populates="bait_flags")
    token: Mapped[Token] = relationship(back_populates="bait_flags")

    __table_args__ = (
        CheckConstraint(
            "flag_type IN ("
            "'dump_after_copytrade_influx', "
            "'hidden_dual_position', "
            "'funder_cluster_member'"
            ")",
            name="flag_type_values",
        ),
        CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            name="confidence_range",
        ),
    )
