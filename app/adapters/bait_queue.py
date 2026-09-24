"""Redis Stream sink for phase-1 bait rechecks.

The stream is the pending record. This adapter does not wait until
``scheduled_at`` and does not choose a consumer group, Celery, or APScheduler.
"""

from __future__ import annotations

from typing import Any, Protocol

from app.config import BAIT_PENDING_STREAM
from app.services.bait_detector import PendingBaitCheck


class StreamWriter(Protocol):
    async def xadd(self, name: str, fields: dict[str, str]) -> Any: ...


class RedisPendingBaitQueue:
    def __init__(self, client: StreamWriter) -> None:
        self._client = client

    async def enqueue(self, check: PendingBaitCheck) -> None:
        await self._client.xadd(BAIT_PENDING_STREAM, check.to_stream_fields())
