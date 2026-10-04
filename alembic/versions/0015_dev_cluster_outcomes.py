"""Creator-cluster fields, price snapshots, and LP unlock readings.

Revision ID: 0015_dev_cluster_outcomes
Revises: 0014_funder_cache_signature
Create Date: 2026-10-04
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0015_dev_cluster_outcomes"
down_revision: str | Sequence[str] | None = "0014_funder_cache_signature"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("tokens", sa.Column("dev_distribution_status", sa.String(), nullable=True))
    op.add_column("tokens", sa.Column("dev_distribution_share", sa.Numeric(), nullable=True))
    op.add_column("tokens", sa.Column("dev_distribution_reason", sa.String(), nullable=True))
    op.create_index("ix_tokens_creator_address", "tokens", ["creator_address"])
    op.create_check_constraint(
        "dev_distribution_status_values",
        "tokens",
        "dev_distribution_status IS NULL OR dev_distribution_status IN ('measured', 'unavailable')",
    )
    op.add_column("signals", sa.Column("context", postgresql.JSONB(), nullable=True))
    op.create_table(
        "outcome_checks",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("horizon", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("price_usd", sa.Numeric(), nullable=True),
        sa.Column("price_reason", sa.String(), nullable=True),
        sa.Column("lp_locked", sa.Boolean(), nullable=True),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token_id", "horizon", name="uq_outcome_checks_token_horizon"),
        sa.CheckConstraint(
            "horizon IN ('listing', '1h', '24h', '7d')",
            name="outcome_horizon_values",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'observed', 'unavailable')",
            name="outcome_status_values",
        ),
    )
    op.create_table(
        "lp_unlocks",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("reason", sa.String(), nullable=True),
        sa.Column("unlock_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("alerted_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token_id"),
        sa.CheckConstraint(
            "status IN ('no_schedule', 'unavailable', 'scheduled', 'alerted')",
            name="lp_unlock_status_values",
        ),
    )


def downgrade() -> None:
    op.drop_table("lp_unlocks")
    op.drop_table("outcome_checks")
    op.drop_column("signals", "context")
    op.drop_constraint("dev_distribution_status_values", "tokens", type_="check")
    op.drop_index("ix_tokens_creator_address", table_name="tokens")
    op.drop_column("tokens", "dev_distribution_reason")
    op.drop_column("tokens", "dev_distribution_share")
    op.drop_column("tokens", "dev_distribution_status")
