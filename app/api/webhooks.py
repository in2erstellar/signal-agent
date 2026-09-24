"""HTTP entry for Helius. The handler lives in ``app.workers.wallet_watcher``."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.workers.wallet_watcher import WalletWatcher, WebhookRejected

router = APIRouter()


@router.post("/webhooks/helius")
async def helius_webhook(request: Request) -> JSONResponse:
    watcher: WalletWatcher = request.app.state.watcher
    try:
        payload: Any = json.loads(await request.body() or b"null")
        result = await watcher.handle(payload, request.headers.get("authorization"))
    except WebhookRejected as exc:
        return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})
    except json.JSONDecodeError:
        return JSONResponse(status_code=400, content={"error": "invalid json"})
    return JSONResponse(
        status_code=200,
        content={
            "stored": result.stored,
            "duplicated": result.duplicated,
            "skipped": result.skipped,
            "risk_enqueued": result.risk_enqueued,
            "bait_enqueued": result.bait_enqueued,
        },
    )
