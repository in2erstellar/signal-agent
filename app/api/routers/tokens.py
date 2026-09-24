"""Stored token risk scan and the funder clusters tied to that mint."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session
from app.api.schemas import FunderClusterOut, TokenRiskResponse
from app.api.token_risk import STORED_SCAN_DISCLAIMER, clusters_from_rows, report_from_token
from app.db.models import FunderCluster, Token

router = APIRouter(tags=["tokens"])


@router.get("/token/{mint}/risk", response_model=TokenRiskResponse)
async def get_token_risk(
    mint: str,
    session: AsyncSession = Depends(get_session),
) -> TokenRiskResponse:
    token = await session.scalar(select(Token).where(Token.mint == mint))
    if token is None or token.risk_score is None:
        raise HTTPException(status_code=404, detail="stored risk scan not found")
    clusters = list(
        await session.scalars(
            select(FunderCluster)
            .where(FunderCluster.token_id == token.id)
            .order_by(FunderCluster.confidence.desc().nullslast(), FunderCluster.root_funder)
        )
    )
    return TokenRiskResponse(
        report=report_from_token(token),
        funder_clusters=[FunderClusterOut.model_validate(row) for row in clusters_from_rows(clusters)],
        disclaimer=STORED_SCAN_DISCLAIMER,
    )
