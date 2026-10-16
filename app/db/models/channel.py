"""Market-cap points behind signal charts, and posts in the public channel."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class MarketPoint(Base):
    __tablename__ = "market_points"

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
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    market_cap_usd: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    price_usd: Mapped[Decimal | None] = mapped_column(Numeric)
    liquidity_usd: Mapped[Decimal | None] = mapped_column(Numeric)

    __table_args__ = (
        CheckConstraint("market_cap_usd > 0", name="market_point_cap_positive"),
        Index("ix_market_points_token_observed", "token_id", "observed_at"),
    )


class ChannelPost(Base):
    __tablename__ = "channel_posts"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    signal_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("signals.id", ondelete="CASCADE"),
        nullable=False,
    )
    # signal is the first post; 1h, 24h, and 7d are replies to it.
    kind: Mapped[str] = mapped_column(String, nullable=False)
    message_id: Mapped[int | None] = mapped_column(BigInteger)
    # skipped: the reply was not posted (no first post, or no reading in time).
    status: Mapped[str] = mapped_column(String, nullable=False)
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("signal_id", "kind", name="uq_channel_posts_signal_kind"),
        CheckConstraint("kind IN ('signal', '1h', '24h', '7d')", name="channel_post_kind_values"),
        CheckConstraint("status IN ('posted', 'skipped')", name="channel_post_status_values"),
    )
