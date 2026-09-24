"""Request dependencies for the public API.

Routers depend on ``ApiPrincipal``, not on the header. Replacing the MVP
API key with OAuth or JWT means changing this module, not the routers.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated

from fastapi import HTTPException, Query, Request, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas import PageParams
from app.config import get_settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

_UNCONFIGURED = frozenset({"", "replace-me"})


@dataclass(frozen=True)
class ApiPrincipal:
    """Caller identity. ``key_id`` is a hash, never the secret itself."""

    key_id: str


async def get_principal(
    x_api_key: Annotated[str | None, Security(api_key_header)],
) -> ApiPrincipal:
    expected = get_settings().public_api_key.get_secret_value().strip()
    if expected in _UNCONFIGURED:
        raise HTTPException(status_code=503, detail="API key is not configured")
    presented = (x_api_key or "").strip()
    if not presented or not hmac.compare_digest(presented, expected):
        raise HTTPException(status_code=401, detail="missing or invalid API key")
    digest = hashlib.sha256(presented.encode()).hexdigest()
    return ApiPrincipal(key_id=digest)


def page_params(
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> PageParams:
    return PageParams(limit=limit, offset=offset)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    factory = request.app.state.session_factory
    async with factory() as session:
        yield session
