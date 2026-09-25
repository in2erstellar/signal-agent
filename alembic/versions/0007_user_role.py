"""Add users.role. Access role is separate from the subscription tier.

Revision ID: 0007_user_role
Revises: 0006_drop_channel_notified
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007_user_role"
down_revision: str | Sequence[str] | None = "0006_drop_channel_notified"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("role", sa.String(), server_default=sa.text("'user'"), nullable=False),
    )
    op.create_check_constraint(
        "ck_users_role_values",
        "users",
        "role IN ('user', 'admin')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_users_role_values", "users", type_="check")
    op.drop_column("users", "role")
