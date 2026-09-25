"""Allow ai_review_status truncated.

Revision ID: 0012_ai_review_truncated_status
Revises: 0011_ai_review_budget_status
Create Date: 2026-09-25
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0012_ai_review_truncated_status"
down_revision: str | Sequence[str] | None = "0011_ai_review_budget_status"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_STATUSES = (
    "ai_review_status IS NULL OR ai_review_status IN "
    "('pending', 'completed', 'failed', 'skipped_budget_exceeded', 'truncated')"
)
_STATUSES_PREVIOUS = (
    "ai_review_status IS NULL OR ai_review_status IN "
    "('pending', 'completed', 'failed', 'skipped_budget_exceeded')"
)


def upgrade() -> None:
    op.drop_constraint("ck_tokens_ai_review_status_values", "tokens", type_="check")
    op.create_check_constraint(
        "ck_tokens_ai_review_status_values",
        "tokens",
        _STATUSES,
    )


def downgrade() -> None:
    op.drop_constraint("ck_tokens_ai_review_status_values", "tokens", type_="check")
    op.create_check_constraint(
        "ck_tokens_ai_review_status_values",
        "tokens",
        _STATUSES_PREVIOUS,
    )
