"""Discovery status for Pump.fun creates that are not risk-scanned yet.

Revision ID: 0009_token_discovery
Revises: 0008_promote_admins
Create Date: 2026-09-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009_token_discovery"
down_revision: str | Sequence[str] | None = "0008_promote_admins"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("tokens", sa.Column("discovery_status", sa.String(), nullable=True))
    op.add_column("tokens", sa.Column("creator_address", sa.String(), nullable=True))
    op.add_column("tokens", sa.Column("bonding_curve_key", sa.String(), nullable=True))
    op.add_column("tokens", sa.Column("funder_graph_status", sa.String(), nullable=True))
    op.create_check_constraint(
        "ck_tokens_discovery_status_values",
        "tokens",
        "discovery_status IS NULL OR discovery_status IN "
        "('pending_first_buy', 'qualified', 'dead')",
    )
    op.create_check_constraint(
        "ck_tokens_funder_graph_status_values",
        "tokens",
        "funder_graph_status IS NULL OR funder_graph_status IN "
        "('skipped_pre_migration', 'built')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_tokens_funder_graph_status_values", "tokens", type_="check")
    op.drop_constraint("ck_tokens_discovery_status_values", "tokens", type_="check")
    op.drop_column("tokens", "funder_graph_status")
    op.drop_column("tokens", "bonding_curve_key")
    op.drop_column("tokens", "creator_address")
    op.drop_column("tokens", "discovery_status")
