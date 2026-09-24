"""Carry a trace id from a webhook buy through the signal and the alert.

Revision ID: 0004_trace_id
Revises: 0003_alert_deliveries
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_trace_id"
down_revision: str | Sequence[str] | None = "0003_alert_deliveries"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("tokens", sa.Column("trace_id", sa.String(), nullable=True))
    op.add_column("signals", sa.Column("trace_id", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("signals", "trace_id")
    op.drop_column("tokens", "trace_id")
