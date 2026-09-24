"""Tracked wallets and their swap ledger.

``wallet_txs`` is a TimescaleDB hypertable partitioned by ``block_time``.
Timescale requires every unique index to include the partition column, so the
primary key is ``(id, block_time)`` and ``tx_signature`` is unique together
with ``block_time``. No extra columns are added.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    UniqueConstraint,
    Uuid,
    false,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.db.models.bait_detection import BaitFlag
    from app.db.models.tokens import Token

WALLET_TX_ACTIONS = ("buy", "sell")


class TrackedWallet(Base):
    __tablename__ = "tracked_wallets"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    address: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    label: Mapped[str | None] = mapped_column(String)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    win_rate: Mapped[Decimal | None] = mapped_column(Numeric)
    avg_pnl_sol: Mapped[Decimal | None] = mapped_column(Numeric)
    is_flagged_baiter: Mapped[bool] = mapped_column(
        nullable=False,
        server_default=false(),
        default=False,
    )

    transactions: Mapped[list[WalletTx]] = relationship(
        back_populates="wallet",
        passive_deletes=True,
    )
    bait_flags: Mapped[list[BaitFlag]] = relationship(
        back_populates="wallet",
        passive_deletes=True,
    )


class WalletTx(Base):
    __tablename__ = "wallet_txs"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    wallet_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tracked_wallets.id", ondelete="RESTRICT"),
        nullable=False,
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tokens.id", ondelete="RESTRICT"),
        nullable=False,
    )
    action: Mapped[str] = mapped_column(String, nullable=False)
    amount_sol: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    tx_signature: Mapped[str] = mapped_column(String, nullable=False)
    block_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )

    wallet: Mapped[TrackedWallet] = relationship(back_populates="transactions")
    token: Mapped[Token] = relationship(back_populates="transactions")

    __table_args__ = (
        UniqueConstraint(
            "tx_signature",
            "block_time",
            name="uq_wallet_txs_tx_signature_block_time",
        ),
        CheckConstraint("action IN ('buy', 'sell')", name="action_values"),
        Index("ix_wallet_txs_block_time", "block_time"),
    )
