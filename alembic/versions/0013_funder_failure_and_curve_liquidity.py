"""Permanent funder-graph failures and bonding-curve SOL snapshot.

Revision ID: 0013_funder_fail_curve_sol
Revises: 0012_ai_review_truncated_status
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0013_funder_fail_curve_sol"
down_revision: str | Sequence[str] | None = "0012_ai_review_truncated_status"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FUNDER_STATUSES = (
    "funder_graph_status IS NULL OR funder_graph_status IN "
    "('skipped_pre_migration', 'pending_post_migration', "
    "'completed_post_migration', 'built', 'failed_permanently')"
)
_FUNDER_STATUSES_PREVIOUS = (
    "funder_graph_status IS NULL OR funder_graph_status IN "
    "('skipped_pre_migration', 'pending_post_migration', "
    "'completed_post_migration', 'built')"
)


def upgrade() -> None:
    op.drop_constraint("ck_tokens_funder_graph_status_values", "tokens", type_="check")
    op.create_check_constraint(
        "ck_tokens_funder_graph_status_values",
        "tokens",
        _FUNDER_STATUSES,
    )
    op.add_column(
        "tokens",
        sa.Column("bonding_curve_liquidity_sol", sa.Numeric(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("tokens", "bonding_curve_liquidity_sol")
    op.drop_constraint("ck_tokens_funder_graph_status_values", "tokens", type_="check")
    op.create_check_constraint(
        "ck_tokens_funder_graph_status_values",
        "tokens",
        _FUNDER_STATUSES_PREVIOUS,
    )
