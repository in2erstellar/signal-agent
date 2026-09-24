"""Signal feed and one signal with its stored risk and bait flags."""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session, page_params
from app.api.schemas import BaitFlagOut, PageParams, SignalDetail, SignalList, SignalOut
from app.db.models import BaitFlag, Signal, Token, TrackedWallet

router = APIRouter(prefix="/signals", tags=["signals"])


def _signal_out(signal: Signal, token: Token) -> SignalOut:
    return SignalOut(
        id=signal.id,
        token_id=signal.token_id,
        mint=token.mint,
        symbol=token.symbol,
        composite_score=signal.composite_score,
        risk_score=signal.risk_score,
        wallet_score=signal.wallet_score,
        triggered_at=signal.triggered_at,
        notified=signal.notified,
    )


@router.get("", response_model=SignalList)
async def list_signals(
    page: PageParams = Depends(page_params),
    min_score: Decimal | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> SignalList:
    filtered = select(Signal, Token).join(Token, Signal.token_id == Token.id)
    counted = select(func.count()).select_from(Signal)
    if min_score is not None:
        filtered = filtered.where(Signal.composite_score >= min_score)
        counted = counted.where(Signal.composite_score >= min_score)
    total = int(await session.scalar(counted) or 0)
    rows = await session.execute(
        filtered.order_by(Signal.triggered_at.desc(), Signal.id.desc())
        .limit(page.limit)
        .offset(page.offset)
    )
    return SignalList(
        items=[_signal_out(signal, token) for signal, token in rows],
        limit=page.limit,
        offset=page.offset,
        total=total,
    )


@router.get("/{signal_id}", response_model=SignalDetail)
async def get_signal(
    signal_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> SignalDetail:
    row = (
        await session.execute(
            select(Signal, Token)
            .join(Token, Signal.token_id == Token.id)
            .where(Signal.id == signal_id)
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="signal not found")
    signal, token = row
    flags = await session.execute(
        select(BaitFlag, TrackedWallet.address)
        .join(TrackedWallet, BaitFlag.wallet_id == TrackedWallet.id)
        .where(BaitFlag.token_id == token.id)
        .order_by(BaitFlag.detected_at.desc())
    )
    risk_flags = [str(flag) for flag in token.risk_flags] if isinstance(token.risk_flags, list) else []
    base = _signal_out(signal, token)
    return SignalDetail(
        **base.model_dump(),
        risk_flags=risk_flags,
        bait_flags=[
            BaitFlagOut(
                id=flag.id,
                wallet_address=address,
                flag_type=flag.flag_type,
                confidence=flag.confidence,
                detected_at=flag.detected_at,
                evidence=flag.evidence if isinstance(flag.evidence, dict) else None,
            )
            for flag, address in flags
        ],
    )
