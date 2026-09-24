"""Read models for the bait detector.

``SessionSwapLedger`` only sees ``wallet_txs``. Buys by wallets that are
not tracked never enter the baseline or the influx. ``SessionClusterBook``
reads clusters written by ``funder_graph`` and hop-1 / hop-2 edges that
share the cluster root. It does not decide who holds the mint or who is
in the public top list — that census is a separate port, because
``getTokenLargestAccounts`` cannot see a hidden holder.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import FunderCluster, Token, TrackedWallet, WalletFunder, WalletTx
from app.services.bait_detector import ClusterCohort, FlowTx


class SessionSwapLedger:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def token_flows(
        self,
        token_mint: str,
        start: datetime,
        end: datetime,
    ) -> list[FlowTx]:
        statement = (
            select(
                TrackedWallet.address,
                WalletTx.action,
                WalletTx.amount_sol,
                WalletTx.block_time,
                WalletTx.tx_signature,
            )
            .join(TrackedWallet, WalletTx.wallet_id == TrackedWallet.id)
            .join(Token, WalletTx.token_id == Token.id)
            .where(
                Token.mint == token_mint,
                WalletTx.block_time >= start,
                WalletTx.block_time <= end,
            )
        )
        rows = (await self._session.execute(statement)).all()
        flows: list[FlowTx] = []
        for address, action, amount, block_time, signature in rows:
            kind: Literal["buy", "sell"]
            if action == "buy":
                kind = "buy"
            elif action == "sell":
                kind = "sell"
            else:
                continue
            flows.append(
                FlowTx(
                    wallet_address=address,
                    action=kind,
                    amount_sol=amount,
                    block_time=block_time,
                    signature=signature,
                )
            )
        return flows


class SessionClusterBook:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def cohorts_for(
        self,
        wallet_address: str,
        token_mint: str,
    ) -> list[ClusterCohort]:
        # A mint has a handful of clusters. Filtering members in process
        # avoids depending on a JSONB containment operator for a string list.
        rows = (
            await self._session.execute(
                select(FunderCluster)
                .join(Token, FunderCluster.token_id == Token.id)
                .where(Token.mint == token_mint)
            )
        ).scalars().all()
        cohorts: list[ClusterCohort] = []
        for row in rows:
            members = tuple(str(member) for member in row.member_wallets or [])
            if wallet_address not in members:
                continue
            funded = await self._funded_by_root(row.root_funder)
            cohorts.append(
                ClusterCohort(
                    root_funder=row.root_funder,
                    confidence=row.confidence,
                    member_wallets=members,
                    funded_by_root=tuple(sorted(funded)),
                )
            )
        return cohorts

    async def _funded_by_root(self, root: str) -> set[str]:
        direct = select(WalletFunder.wallet_address).where(
            WalletFunder.funder_address == root,
            WalletFunder.hop_depth == 1,
        )
        intermediates = select(WalletFunder.wallet_address).where(
            WalletFunder.funder_address == root,
            WalletFunder.hop_depth == 2,
        )
        indirect = select(WalletFunder.wallet_address).where(
            WalletFunder.hop_depth == 1,
            WalletFunder.funder_address.in_(intermediates),
        )
        rows = (await self._session.execute(direct.union(indirect))).all()
        return {address for (address,) in rows}
