"""Promote the Telegram ids that were TELEGRAM_OWNER_IDS to role=admin.

Schema stays in 0007. This revision only changes existing rows.

    UPDATE users
    SET role = 'admin'
    WHERE telegram_id IN (8700246141);

Revision ID: 0008_promote_admins
Revises: 0007_user_role
Create Date: 2026-09-24
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0008_promote_admins"
down_revision: str | Sequence[str] | None = "0007_user_role"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The owner list in use when registration opened: 8700246141.
UPGRADE_SQL = "UPDATE users SET role = 'admin' WHERE telegram_id IN (8700246141)"
DOWNGRADE_SQL = "UPDATE users SET role = 'user' WHERE telegram_id IN (8700246141)"


def upgrade() -> None:
    op.execute(UPGRADE_SQL)


def downgrade() -> None:
    op.execute(DOWNGRADE_SQL)
