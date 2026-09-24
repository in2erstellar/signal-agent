"""Authenticated public API. Health and the Helius webhook stay outside it."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.rate_limit import limit_principal
from app.api.routers import signals, tokens, users, watchlist
from app.api.schemas import ErrorBody

public_router = APIRouter(
    dependencies=[Depends(limit_principal)],
    responses={
        401: {"model": ErrorBody, "description": "Missing or invalid API key"},
        429: {"model": ErrorBody, "description": "Per-key rate limit exceeded"},
        503: {"model": ErrorBody, "description": "API key or rate limiter is not available"},
    },
)
public_router.include_router(watchlist.router)
public_router.include_router(signals.router)
public_router.include_router(tokens.router)
public_router.include_router(users.router)
