"""Delivery log so a signal is sent once per subscriber.

Revision ID: 0003_alert_deliveries
Revises: 0002_pipeline_idempotency
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_alert_deliveries"
down_revision: str | Sequence[str] | None = "0002_pipeline_idempotency"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "alert_deliveries",
        sa.Column(
            "id",
            sa.Uuid(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("signal_id", sa.Uuid(), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_alert_deliveries"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="fk_alert_deliveries_user_id_users",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["signal_id"],
            ["signals.id"],
            name="fk_alert_deliveries_signal_id_signals",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "user_id",
            "signal_id",
            name="uq_alert_deliveries_user_id_signal_id",
        ),
    )
    op.create_index(
        "ix_alert_deliveries_user_id_sent_at",
        "alert_deliveries",
        ["user_id", "sent_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_alert_deliveries_user_id_sent_at", table_name="alert_deliveries")
    op.drop_table("alert_deliveries")
