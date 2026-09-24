"""Tracked-wallet watchlist."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session, page_params
from app.api.schemas import PageParams, WalletCreate, WalletList, WalletOut
from app.db.models import TrackedWallet

router = APIRouter(prefix="/watchlist", tags=["watchlist"])


def _wallet_out(row: TrackedWallet) -> WalletOut:
    return WalletOut(
        id=row.id,
        address=row.address,
        label=row.label,
        first_seen_at=row.first_seen_at,
        win_rate=row.win_rate,
        avg_pnl_sol=row.avg_pnl_sol,
        is_flagged_baiter=row.is_flagged_baiter,
    )


@router.post("/wallet", response_model=WalletOut, status_code=201)
async def add_wallet(
    body: WalletCreate,
    session: AsyncSession = Depends(get_session),
) -> WalletOut:
    wallet = TrackedWallet(
        address=body.address,
        label=body.normalized_label(),
        first_seen_at=datetime.now(timezone.utc),
    )
    try:
        async with session.begin():
            session.add(wallet)
    except IntegrityError:
        raise HTTPException(status_code=409, detail="wallet already tracked") from None
    return _wallet_out(wallet)


@router.get("/wallet", response_model=WalletList)
async def list_wallets(
    page: PageParams = Depends(page_params),
    session: AsyncSession = Depends(get_session),
) -> WalletList:
    total = int(await session.scalar(select(func.count()).select_from(TrackedWallet)) or 0)
    rows = await session.scalars(
        select(TrackedWallet)
        .order_by(TrackedWallet.first_seen_at.desc().nullslast(), TrackedWallet.address)
        .limit(page.limit)
        .offset(page.offset)
    )
    return WalletList(
        items=[_wallet_out(row) for row in rows],
        limit=page.limit,
        offset=page.offset,
        total=total,
    )


@router.delete("/wallet/{address}", response_model=WalletOut)
async def delete_wallet(
    address: str,
    session: AsyncSession = Depends(get_session),
) -> WalletOut:
    try:
        async with session.begin():
            wallet = await session.scalar(
                select(TrackedWallet).where(TrackedWallet.address == address)
            )
            if wallet is None:
                raise HTTPException(status_code=404, detail="wallet not found")
            payload = _wallet_out(wallet)
            await session.delete(wallet)
    except IntegrityError:
        raise HTTPException(
            status_code=409,
            detail="wallet has transactions and cannot be deleted",
        ) from None
    return payload
