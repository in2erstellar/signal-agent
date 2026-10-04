"""Price snapshots and LP-unlock readings. Not part of the composite score."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class OutcomeCheck(Base):
    __tablename__ = "outcome_checks"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tokens.id", ondelete="CASCADE"),
        nullable=False,
    )
    # listing is the scan-time snapshot. 1h, 24h, and 7d share this table.
    horizon: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    price_usd: Mapped[Decimal | None] = mapped_column(Numeric)
    price_reason: Mapped[str | None] = mapped_column(String)
    lp_locked: Mapped[bool | None] = mapped_column(Boolean)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("token_id", "horizon", name="uq_outcome_checks_token_horizon"),
        CheckConstraint(
            "horizon IN ('listing', '1h', '24h', '7d')",
            name="outcome_horizon_values",
        ),
        CheckConstraint(
            "status IN ('pending', 'observed', 'unavailable')",
            name="outcome_status_values",
        ),
    )


class LpUnlock(Base):
    __tablename__ = "lp_unlocks"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tokens.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    status: Mapped[str] = mapped_column(String, nullable=False)
    reason: Mapped[str | None] = mapped_column(String)
    unlock_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    alerted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "status IN ('no_schedule', 'unavailable', 'scheduled', 'alerted')",
            name="lp_unlock_status_values",
        ),
    )
