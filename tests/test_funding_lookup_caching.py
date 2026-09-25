"""A resolved first funder is read from wallet_funders, not from RPC again."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.helius import FundingInfo
from app.db.models import WalletFunder
from app.services.funder_graph import FunderGraphBuilder
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)


class _Helius:
    def __init__(self) -> None:
        self.funding_calls: list[str] = []

    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]:
        del mint, top_n
        return ["H1"]

    async def find_first_funding_tx(self, wallet_address: str) -> FundingInfo | None:
        self.funding_calls.append(wallet_address)
        parent = {"H1": "F", "F": "ROOT"}[wallet_address]
        return FundingInfo(
            wallet_address=wallet_address,
            funder=parent,
            amount_sol=Decimal("1.5"),
            signature=f"sig-{wallet_address}",
            funded_at=T0,
            slot=11,
        )


@pytest.fixture
async def sessions(pipeline_infra: object) -> AsyncIterator[async_sessionmaker]:
    database_url = pipeline_infra.database_url  # type: ignore[attr-defined]
    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        yield factory
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


async def test_second_build_does_not_call_rpc_for_a_resolved_wallet(sessions: async_sessionmaker) -> None:
    helius = _Helius()
    async with sessions() as session:
        async with session.begin():
            await FunderGraphBuilder(session, helius).build_graph_for_token("mint-a")
    assert helius.funding_calls == ["H1", "F"]

    async with sessions() as session:
        async with session.begin():
            await FunderGraphBuilder(session, helius).build_graph_for_token("mint-b")
    assert helius.funding_calls == ["H1", "F"]

    async with sessions() as session:
        rows = list(await session.scalars(select(WalletFunder).order_by(WalletFunder.wallet_address)))
    stored = {(row.wallet_address, row.funder_address, row.funding_signature) for row in rows}
    assert stored == {("H1", "F", "sig-H1"), ("F", "ROOT", "sig-F")}
