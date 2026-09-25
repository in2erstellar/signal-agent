"""Insert a token row once. Callers share this instead of a second upsert.

On ``uq_tokens_mint`` the insert does nothing and the existing row is
returned unchanged. Discovery must not overwrite a wallet-discovered mint,
and the wallet watcher must not clear discovery columns it did not set.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Token

DISCOVERY_PENDING = "pending_first_buy"
DISCOVERY_QUALIFIED = "qualified"
DISCOVERY_DEAD = "dead"
FUNDER_GRAPH_SKIPPED = "skipped_pre_migration"
FUNDER_GRAPH_PENDING_POST = "pending_post_migration"
FUNDER_GRAPH_COMPLETED_POST = "completed_post_migration"
FUNDER_GRAPH_BUILT = "built"
AI_REVIEW_PENDING = "pending"
AI_REVIEW_COMPLETED = "completed"
AI_REVIEW_FAILED = "failed"
AI_REVIEW_SKIPPED_BUDGET = "skipped_budget_exceeded"
AI_REVIEW_TRUNCATED = "truncated"


async def insert_token(
    session: AsyncSession,
    *,
    mint: str,
    first_seen_at: datetime,
    symbol: str | None = None,
    name: str | None = None,
    creator_address: str | None = None,
    bonding_curve_key: str | None = None,
    discovery_status: str | None = None,
    funder_graph_status: str | None = None,
) -> tuple[Token, bool]:
    inserted = await session.scalar(
        pg_insert(Token)
        .values(
            mint=mint,
            first_seen_at=first_seen_at,
            symbol=symbol,
            name=name,
            creator_address=creator_address,
            bonding_curve_key=bonding_curve_key,
            discovery_status=discovery_status,
            funder_graph_status=funder_graph_status,
        )
        .on_conflict_do_nothing(constraint="uq_tokens_mint")
        .returning(Token.id)
    )
    token = await session.scalar(select(Token).where(Token.mint == mint))
    if token is None:
        raise RuntimeError(f"token {mint} was not stored")
    return token, inserted is not None
