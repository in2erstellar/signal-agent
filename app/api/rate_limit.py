"""Fixed-window rate limit, one Redis counter per API key per minute.

This is the stand-in for a library limiter such as slowapi: the bucket is
shared by every API process, and the key is the principal from ``deps``,
not the client IP.
"""

from __future__ import annotations

import time

import structlog
from fastapi import HTTPException, Request, Security

from app.api.deps import ApiPrincipal, get_principal
from app.config import get_settings

logger = structlog.get_logger(__name__)


async def limit_principal(
    request: Request,
    principal: ApiPrincipal = Security(get_principal),
) -> ApiPrincipal:
    limit = get_settings().api_rate_limit_per_minute
    window = int(time.time()) // 60
    key = f"signalforge:api_rate:{principal.key_id}:{window}"
    try:
        count = int(await request.app.state.redis.incr(key))
        if count == 1:
            await request.app.state.redis.expire(key, 120)
    except Exception as exc:
        logger.warning("api_rate_limit_unavailable", error=type(exc).__name__)
        raise HTTPException(status_code=503, detail="rate limiter unavailable") from None
    if count > limit:
        retry_after = 60 - (int(time.time()) % 60)
        logger.info("api_rate_limited", key_id=principal.key_id[:12], count=count)
        raise HTTPException(
            status_code=429,
            detail="rate limit exceeded",
            headers={"Retry-After": str(retry_after)},
        )
    return principal
