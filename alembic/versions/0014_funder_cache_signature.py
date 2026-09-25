"""Store the first-funding signature so a later graph build skips RPC.

Revision ID: 0014_funder_cache_signature
Revises: 0013_funder_fail_curve_sol
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0014_funder_cache_signature"
down_revision: str | Sequence[str] | None = "0013_funder_fail_curve_sol"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("wallet_funders", sa.Column("funding_signature", sa.String(), nullable=True))
    op.add_column("wallet_funders", sa.Column("funding_slot", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("wallet_funders", "funding_slot")
    op.drop_column("wallet_funders", "funding_signature")
