"""A wallet past the signature cap is stored as its own root, not a failed token."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.adapters.helius import FundingDepthExceeded, FundingInfo
from app.db.models import FunderCluster, WalletFunder
from app.services.creator_cluster import stored_root
from app.services.funder_graph import FunderGraphBuilder

pytest_plugins = ["tests.test_funder_graph"]


class _DeepHistoryHelius:
    """Explicit double. ``deep`` wallets raise the depth error the real client raises."""

    def __init__(self, funding: dict[str, FundingInfo | None], deep: set[str]) -> None:
        self._funding = funding
        self._deep = deep
        self.funding_calls: list[str] = []

    async def get_top_holder_owners(self, mint: str, top_n: int) -> list[str]:
        del mint, top_n
        return ["H1", "H2", "H3", "OLD"]

    async def find_first_funding_tx(self, wallet_address: str) -> FundingInfo | None:
        self.funding_calls.append(wallet_address)
        if wallet_address in self._deep:
            raise FundingDepthExceeded(wallet_address, 1000)
        return self._funding.get(wallet_address)


def _funding(wallet: str, funder: str) -> FundingInfo:
    return FundingInfo(
        wallet_address=wallet,
        funder=funder,
        amount_sol=Decimal("1.5"),
        signature=f"sig-{wallet}",
        funded_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        slot=10,
    )


async def test_deep_holder_and_deep_funder_do_not_fail_the_graph(session: AsyncSession) -> None:
    # H1-H3 share F_SHARED. F_SHARED is an exchange-style wallet with a long
    # history, and OLD is an established holder. Before, either one failed the token.
    helius = _DeepHistoryHelius(
        {name: _funding(name, "F_SHARED") for name in ("H1", "H2", "H3")},
        deep={"OLD", "F_SHARED"},
    )
    clusters = await FunderGraphBuilder(session, helius).build_graph_for_token("MintDeep")

    assert [(c.root_funder, c.member_wallets) for c in clusters] == [
        ("F_SHARED", ["H1", "H2", "H3"])
    ]
    self_edges = {
        row.wallet_address
        for row in await session.scalars(
            select(WalletFunder).where(WalletFunder.wallet_address == WalletFunder.funder_address)
        )
    }
    assert self_edges == {"OLD", "F_SHARED"}
    assert await stored_root(session, "OLD") == "OLD"
    assert await stored_root(session, "H1") == "F_SHARED"
    stored = await session.scalars(select(FunderCluster))
    assert len(list(stored)) == 1


async def test_a_stored_established_wallet_is_not_looked_up_again(session: AsyncSession) -> None:
    helius = _DeepHistoryHelius({}, deep={"OLD"})
    builder = FunderGraphBuilder(session, helius)
    await builder.build_graph_for_token("MintOne")
    calls_after_first = helius.funding_calls.count("OLD")
    await builder.build_graph_for_token("MintTwo")
    assert calls_after_first == 1
    assert helius.funding_calls.count("OLD") == 1


async def test_an_established_creator_is_its_own_root(session: AsyncSession) -> None:
    helius = _DeepHistoryHelius({}, deep={"CREATOR"})
    builder = FunderGraphBuilder(session, helius)
    assert await builder.root_for_wallet("CREATOR") == "CREATOR"
    assert await builder.root_for_wallet("CREATOR") == "CREATOR"
    assert helius.funding_calls == ["CREATOR"]


async def test_a_deep_hop2_funder_stops_the_walk_at_hop1(session: AsyncSession) -> None:
    helius = _DeepHistoryHelius({"CREATOR": _funding("CREATOR", "EXCHANGE")}, deep={"EXCHANGE"})
    builder = FunderGraphBuilder(session, helius)
    assert await builder.root_for_wallet("CREATOR") == "EXCHANGE"
    assert await stored_root(session, "CREATOR") == "EXCHANGE"
