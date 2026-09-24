"""Unique keys so pipeline retries do not duplicate flags or signals.

Revision ID: 0002_pipeline_idempotency
Revises: 0001_initial_schema
Create Date: 2026-09-24
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002_pipeline_idempotency"
down_revision: str | Sequence[str] | None = "0001_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_bait_flags_wallet_token_type",
        "bait_flags",
        ["wallet_id", "token_id", "flag_type"],
    )
    op.create_unique_constraint(
        "uq_signals_token_id",
        "signals",
        ["token_id"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_signals_token_id", "signals", type_="unique")
    op.drop_constraint("uq_bait_flags_wallet_token_type", "bait_flags", type_="unique")
