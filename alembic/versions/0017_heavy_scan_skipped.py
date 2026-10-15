"""Allow heavy_scan_status = skipped for a mint whose fast checks rule out a signal.

The fast scan acks without enqueueing the heavy half when an unavailable fast
check or the fast-check weight already lost makes the signal threshold
unreachable. ``risk_score`` stays null. A later fast scan of the same mint
decides again.

Revision ID: 0017_heavy_scan_skipped
Revises: 0016_heavy_scan_status
Create Date: 2026-10-15
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0017_heavy_scan_skipped"
down_revision: str | Sequence[str] | None = "0016_heavy_scan_status"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("heavy_scan_status_values", "tokens", type_="check")
    op.create_check_constraint(
        "heavy_scan_status_values",
        "tokens",
        "heavy_scan_status IS NULL OR heavy_scan_status IN "
        "('pending', 'complete', 'failed', 'skipped')",
    )


def downgrade() -> None:
    # 0016 has no skipped state. Those mints never got a score, which 0016
    # reads as a heavy half that is still owed: pending.
    op.execute("UPDATE tokens SET heavy_scan_status = 'pending' WHERE heavy_scan_status = 'skipped'")
    op.drop_constraint("heavy_scan_status_values", "tokens", type_="check")
    op.create_check_constraint(
        "heavy_scan_status_values",
        "tokens",
        "heavy_scan_status IS NULL OR heavy_scan_status IN ('pending', 'complete', 'failed')",
    )
