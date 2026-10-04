"""Composite scores emitted for a token. Derived from token risk, not a source ledger."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    UniqueConstraint,
    Uuid,
    false,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.db.models.tokens import Token


class Signal(Base):
    __tablename__ = "signals"

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
    wallet_score: Mapped[Decimal | None] = mapped_column(Numeric)
    risk_score: Mapped[Decimal | None] = mapped_column(Numeric)
    composite_score: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    notified: Mapped[bool] = mapped_column(
        nullable=False,
        server_default=false(),
        default=False,
    )
    trace_id: Mapped[str | None] = mapped_column(String)
    # Creator-cluster block. Separate from composite_score and risk_score.
    context: Mapped[dict[str, Any] | None] = mapped_column(JSONB)

    token: Mapped[Token] = relationship(back_populates="signals")

    __table_args__ = (
        CheckConstraint(
            "risk_score IS NULL OR (risk_score >= 0 AND risk_score <= 100)",
            name="risk_score_range",
        ),
        UniqueConstraint("token_id", name="uq_signals_token_id"),
    )
