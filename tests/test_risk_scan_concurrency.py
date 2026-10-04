"""A risk-scan batch runs at most FUNDER_GRAPH_CONCURRENCY scans at once."""

from __future__ import annotations

import asyncio

from redis.exceptions import ResponseError

from app.config import FUNDER_GRAPH_CONCURRENCY, RISK_SCAN_GROUP, RISK_SCAN_QUEUE
from app.workers.risk_scan_worker import RiskScanWorker


class _Redis:
    def __init__(self, count: int) -> None:
        self._messages = [
            (f"{index}-0", {"mint": f"mint-{index}", "trace_id": f"trace-{index}"})
            for index in range(count)
        ]
        self._served_new = False

    async def xgroup_create(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        raise ResponseError("BUSYGROUP Consumer Group name already exists")

    async def xreadgroup(
        self,
        *,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int,
        block: int | None,
    ) -> list[object]:
        del groupname, consumername, count, block
        stream_id = next(iter(streams.values()))
        if stream_id == "0" or self._served_new:
            return []
        self._served_new = True
        return [[RISK_SCAN_QUEUE, self._messages]]

    async def xack(self, queue: str, group: str, message_id: str) -> int:
        assert queue == RISK_SCAN_QUEUE
        assert group == RISK_SCAN_GROUP
        del message_id
        return 1

    async def get(self, key: str) -> None:
        del key
        return None


async def test_batch_runs_at_most_three_scans_together() -> None:
    redis = _Redis(20)
    current = 0
    peak = 0
    entered = 0
    release = asyncio.Event()
    full = asyncio.Event()

    async def scan(mint: str, trace_id: str) -> None:
        nonlocal current, peak, entered
        del mint, trace_id
        current += 1
        entered += 1
        peak = max(peak, current)
        if entered >= FUNDER_GRAPH_CONCURRENCY:
            full.set()
        await release.wait()
        current -= 1

    worker = RiskScanWorker(
        sessions=None,  # type: ignore[arg-type]
        redis=redis,  # type: ignore[arg-type]
        funder_factory=lambda _session: None,
        scanner_factory=lambda _session, _clusters: None,  # type: ignore[return-value, misc]
        scan_slots=asyncio.Semaphore(FUNDER_GRAPH_CONCURRENCY),
    )
    worker._scan = scan  # type: ignore[method-assign]

    cycle = asyncio.create_task(worker.run_once())
    await asyncio.wait_for(full.wait(), timeout=2)
    await asyncio.sleep(0)
    assert entered == FUNDER_GRAPH_CONCURRENCY
    assert peak == FUNDER_GRAPH_CONCURRENCY
    release.set()
    processed, failed = await cycle
    assert (processed, failed) == (20, 0)
    assert peak == FUNDER_GRAPH_CONCURRENCY
