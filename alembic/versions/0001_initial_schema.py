"""Phase-1 schema: tokens, wallets, funder graph, bait flags, signals, users.

wallet_txs is converted to a TimescaleDB hypertable on block_time. Unique
indexes on a hypertable must include the partition column, so the primary key
is (id, block_time) and tx_signature is unique with block_time.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_schema"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    op.create_table(
        "tokens",
        sa.Column(
            "id",
            sa.Uuid(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("mint", sa.String(), nullable=False),
        sa.Column("symbol", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("liquidity_usd", sa.Numeric(), nullable=True),
        sa.Column("holder_count", sa.Integer(), nullable=True),
        sa.Column("mint_authority_renounced", sa.Boolean(), nullable=True),
        sa.Column("freeze_authority_renounced", sa.Boolean(), nullable=True),
        sa.Column("risk_score", sa.Numeric(), nullable=True),
        sa.Column("risk_flags", postgresql.JSONB(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_tokens"),
        sa.UniqueConstraint("mint", name="uq_tokens_mint"),
        sa.CheckConstraint(
            "risk_score IS NULL OR (risk_score >= 0 AND risk_score <= 100)",
            name="ck_tokens_risk_score_range",
        ),
    )
    op.create_table(
        "tracked_wallets",
        sa.Column(
            "id",
            sa.Uuid(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("address", sa.String(), nullable=False),
        sa.Column("label", sa.String(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("win_rate", sa.Numeric(), nullable=True),
        sa.Column("avg_pnl_sol", sa.Numeric(), nullable=True),
        sa.Column(
            "is_flagged_baiter",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_tracked_wallets"),
        sa.UniqueConstraint("address", name="uq_tracked_wallets_address"),
    )
    op.create_table(
        "users",
        sa.Column(
            "id",
            sa.Uuid(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("telegram_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "tier",
            sa.String(),
            server_default=sa.text("'free'"),
            nullable=False,
        ),
        sa.Column(
            "settings",
            postgresql.JSONB(),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_users"),
        sa.UniqueConstraint("telegram_id", name="uq_users_telegram_id"),
        sa.CheckConstraint(
            "tier IN ('free', 'premium')",
            name="ck_users_tier_values",
        ),
    )
    op.create_table(
        "wallet_funders",
        sa.Column(
            "id",
            sa.Uuid(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("wallet_address", sa.String(), nullable=False),
        sa.Column("funder_address", sa.String(), nullable=False),
        sa.Column("amount_sol", sa.Numeric(), nullable=False),
        sa.Column("hop_depth", sa.SmallInteger(), nullable=False),
        sa.Column("funded_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_wallet_funders"),
        sa.UniqueConstraint(
            "wallet_address",
            "funder_address",
            "hop_depth",
            name="uq_wallet_funders_wallet_funder_hop",
        ),
    )
    op.create_index(
        "ix_wallet_funders_wallet_address",
        "wallet_funders",
        ["wallet_address"],
    )
    op.create_table(
        "wallet_txs",
        sa.Column(
            "id",
            sa.Uuid(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("wallet_id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("amount_sol", sa.Numeric(), nullable=False),
        sa.Column("tx_signature", sa.String(), nullable=False),
        sa.Column("block_time", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", "block_time", name="pk_wallet_txs"),
        sa.UniqueConstraint(
            "tx_signature",
            "block_time",
            name="uq_wallet_txs_tx_signature_block_time",
        ),
        sa.CheckConstraint(
            "action IN ('buy', 'sell')",
            name="ck_wallet_txs_action_values",
        ),
        sa.ForeignKeyConstraint(
            ["wallet_id"],
            ["tracked_wallets.id"],
            name="fk_wallet_txs_wallet_id_tracked_wallets",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["token_id"],
            ["tokens.id"],
            name="fk_wallet_txs_token_id_tokens",
            ondelete="RESTRICT",
        ),
    )
    op.create_index("ix_wallet_txs_block_time", "wallet_txs", ["block_time"])
    op.execute("SELECT create_hypertable('wallet_txs', 'block_time')")

    op.create_table(
        "funder_clusters",
        sa.Column(
            "id",
            sa.Uuid(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("root_funder", sa.String(), nullable=False),
        sa.Column("member_wallets", postgresql.JSONB(), nullable=False),
        sa.Column("cluster_size", sa.Integer(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=True),
        sa.Column("first_detected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("confidence", sa.Numeric(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_funder_clusters"),
        sa.CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="ck_funder_clusters_confidence_range",
        ),
        sa.ForeignKeyConstraint(
            ["token_id"],
            ["tokens.id"],
            name="fk_funder_clusters_token_id_tokens",
            ondelete="SET NULL",
        ),
    )
    op.create_index(
        "ix_funder_clusters_root_funder",
        "funder_clusters",
        ["root_funder"],
    )
    op.create_table(
        "bait_flags",
        sa.Column(
            "id",
            sa.Uuid(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("wallet_id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("flag_type", sa.String(), nullable=False),
        sa.Column("confidence", sa.Numeric(), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence", postgresql.JSONB(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_bait_flags"),
        sa.CheckConstraint(
            "flag_type IN ("
            "'dump_after_copytrade_influx', "
            "'hidden_dual_position', "
            "'funder_cluster_member'"
            ")",
            name="ck_bait_flags_flag_type_values",
        ),
        sa.CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            name="ck_bait_flags_confidence_range",
        ),
        sa.ForeignKeyConstraint(
            ["wallet_id"],
            ["tracked_wallets.id"],
            name="fk_bait_flags_wallet_id_tracked_wallets",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["token_id"],
            ["tokens.id"],
            name="fk_bait_flags_token_id_tokens",
            ondelete="CASCADE",
        ),
    )
    op.create_table(
        "signals",
        sa.Column(
            "id",
            sa.Uuid(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("wallet_score", sa.Numeric(), nullable=True),
        sa.Column("risk_score", sa.Numeric(), nullable=True),
        sa.Column("composite_score", sa.Numeric(), nullable=False),
        sa.Column("triggered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "notified",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name="pk_signals"),
        sa.CheckConstraint(
            "risk_score IS NULL OR (risk_score >= 0 AND risk_score <= 100)",
            name="ck_signals_risk_score_range",
        ),
        sa.ForeignKeyConstraint(
            ["token_id"],
            ["tokens.id"],
            name="fk_signals_token_id_tokens",
            ondelete="CASCADE",
        ),
    )
    op.create_index("ix_signals_triggered_at", "signals", ["triggered_at"])


def downgrade() -> None:
    op.drop_index("ix_signals_triggered_at", table_name="signals")
    op.drop_table("signals")
    op.drop_table("bait_flags")
    op.drop_index("ix_funder_clusters_root_funder", table_name="funder_clusters")
    op.drop_table("funder_clusters")
    op.drop_index("ix_wallet_txs_block_time", table_name="wallet_txs")
    op.drop_table("wallet_txs")
    op.drop_index(
        "ix_wallet_funders_wallet_address",
        table_name="wallet_funders",
    )
    op.drop_table("wallet_funders")
    op.drop_table("users")
    op.drop_table("tracked_wallets")
    op.drop_table("tokens")
    op.execute("DROP EXTENSION IF EXISTS timescaledb")
