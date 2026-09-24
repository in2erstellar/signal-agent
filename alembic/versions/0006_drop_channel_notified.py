"""Drop the public-channel delivery flag. Alerts stay in private chats.

Revision ID: 0006_drop_channel_notified
Revises: 0005_channel_notified
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_drop_channel_notified"
down_revision: str | Sequence[str] | None = "0005_channel_notified"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("signals", "channel_notified")


def downgrade() -> None:
    op.add_column(
        "signals",
        sa.Column(
            "channel_notified",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
