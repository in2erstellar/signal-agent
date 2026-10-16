"""Market-cap points for signal charts and posts in the public channel.

``market_points`` holds DexScreener readings taken after a signal: every few
minutes in the first hour, then hourly, then every few hours to 7 days. A
missing reading is no row, never a zero. ``channel_posts`` maps a signal to
its channel message and each result reply, so nothing is posted twice.

Revision ID: 0018_channel_track_record
Revises: 0017_heavy_scan_skipped
Create Date: 2026-10-16
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0018_channel_track_record"
down_revision: str | Sequence[str] | None = "0017_heavy_scan_skipped"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "market_points",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("market_cap_usd", sa.Numeric(), nullable=False),
        sa.Column("price_usd", sa.Numeric(), nullable=True),
        sa.Column("liquidity_usd", sa.Numeric(), nullable=True),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint("market_cap_usd > 0", name="market_point_cap_positive"),
    )
    op.create_index("ix_market_points_token_observed", "market_points", ["token_id", "observed_at"])
    op.create_table(
        "channel_posts",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("signal_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(), nullable=False),
        sa.Column("message_id", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("posted_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["signal_id"], ["signals.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("signal_id", "kind", name="uq_channel_posts_signal_kind"),
        sa.CheckConstraint(
            "kind IN ('signal', '1h', '24h', '7d')", name="channel_post_kind_values"
        ),
        sa.CheckConstraint("status IN ('posted', 'skipped')", name="channel_post_status_values"),
    )


def downgrade() -> None:
    op.drop_table("channel_posts")
    op.drop_index("ix_market_points_token_observed", table_name="market_points")
    op.drop_table("market_points")
