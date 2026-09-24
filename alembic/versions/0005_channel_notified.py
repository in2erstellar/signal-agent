"""Remember whether a signal was posted to the public Telegram channel.

Revision ID: 0005_channel_notified
Revises: 0004_trace_id
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_channel_notified"
down_revision: str | Sequence[str] | None = "0004_trace_id"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "signals",
        sa.Column(
            "channel_notified",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("signals", "channel_notified")
