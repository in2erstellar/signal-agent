"""Post-migration funder-graph statuses and stored AI reviews.

Revision ID: 0010_migration_and_ai_review
Revises: 0009_token_discovery
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010_migration_and_ai_review"
down_revision: str | Sequence[str] | None = "0009_token_discovery"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FUNDER_STATUSES = (
    "funder_graph_status IS NULL OR funder_graph_status IN "
    "('skipped_pre_migration', 'pending_post_migration', "
    "'completed_post_migration', 'built')"
)
_FUNDER_STATUSES_PREVIOUS = (
    "funder_graph_status IS NULL OR funder_graph_status IN "
    "('skipped_pre_migration', 'built')"
)


def upgrade() -> None:
    op.drop_constraint("ck_tokens_funder_graph_status_values", "tokens", type_="check")
    op.create_check_constraint(
        "ck_tokens_funder_graph_status_values",
        "tokens",
        _FUNDER_STATUSES,
    )
    op.add_column("tokens", sa.Column("ai_review", sa.String(), nullable=True))
    op.add_column("tokens", sa.Column("ai_review_status", sa.String(), nullable=True))
    op.add_column(
        "tokens",
        sa.Column("ai_review_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_tokens_ai_review_status_values",
        "tokens",
        "ai_review_status IS NULL OR ai_review_status IN ('pending', 'completed', 'failed')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_tokens_ai_review_status_values", "tokens", type_="check")
    op.drop_column("tokens", "ai_review_at")
    op.drop_column("tokens", "ai_review_status")
    op.drop_column("tokens", "ai_review")
    op.drop_constraint("ck_tokens_funder_graph_status_values", "tokens", type_="check")
    op.create_check_constraint(
        "ck_tokens_funder_graph_status_values",
        "tokens",
        _FUNDER_STATUSES_PREVIOUS,
    )
