"""Track a risk scan whose heavy half is still on risk_heavy_queue.

``pending`` means ``risk_scan_queue`` was acked and ``risk_score`` is still
null. ``complete`` and ``failed`` are written with the score. There is no
deadline column: a queued mint stays pending until its heavy message is acked.

Revision ID: 0016_heavy_scan_status
Revises: 0015_dev_cluster_outcomes
Create Date: 2026-10-06
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0016_heavy_scan_status"
down_revision: str | Sequence[str] | None = "0015_dev_cluster_outcomes"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("tokens", sa.Column("heavy_scan_status", sa.String(), nullable=True))
    op.create_check_constraint(
        "heavy_scan_status_values",
        "tokens",
        "heavy_scan_status IS NULL OR heavy_scan_status IN ('pending', 'complete', 'failed')",
    )


def downgrade() -> None:
    op.drop_constraint("heavy_scan_status_values", "tokens", type_="check")
    op.drop_column("tokens", "heavy_scan_status")
