"""Fast and heavy scans do not share a semaphore."""

from __future__ import annotations

import asyncio

from redis.exceptions import ResponseError

from app.config import (
    FAST_SCAN_CONCURRENCY,
    FUNDER_GRAPH_CONCURRENCY,
    RISK_HEAVY_GROUP,
    RISK_HEAVY_QUEUE,
    RISK_SCAN_GROUP,
    RISK_SCAN_PRIORITY_GROUP,
    RISK_SCAN_PRIORITY_QUEUE,
    RISK_SCAN_QUEUE,
)
from app.workers.risk_scan_worker import RiskScanWorker


class _Redis:
    def __init__(self, queues: dict[str, list[tuple[str, dict[str, str]]]]) -> None:
        self._queues = queues
        self._served: set[str] = set()
        self.acked: list[tuple[str, str]] = []

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
        name = next(iter(streams))
        stream_id = streams[name]
        if stream_id == "0" or name in self._served:
            return []
        self._served.add(name)
        messages = self._queues.get(name) or []
        if not messages:
            return []
        return [[name, messages]]

    async def xack(self, queue: str, group: str, message_id: str) -> int:
        if queue == RISK_SCAN_QUEUE:
            assert group == RISK_SCAN_GROUP
        elif queue == RISK_SCAN_PRIORITY_QUEUE:
            assert group == RISK_SCAN_PRIORITY_GROUP
        elif queue == RISK_HEAVY_QUEUE:
            assert group == RISK_HEAVY_GROUP
        else:
            raise AssertionError(queue)
        self.acked.append((queue, message_id))
        return 1

    async def get(self, key: str) -> None:
        del key
        return None


def _messages(prefix: str, count: int) -> list[tuple[str, dict[str, str]]]:
    return [
        (f"{index}-0", {"mint": f"{prefix}-{index}", "trace_id": f"trace-{prefix}-{index}"})
        for index in range(count)
    ]


def _worker(
    redis: _Redis,
    *,
    fast_slots: asyncio.Semaphore,
    heavy_slots: asyncio.Semaphore,
) -> RiskScanWorker:
    return RiskScanWorker(
        sessions=None,  # type: ignore[arg-type]
        redis=redis,  # type: ignore[arg-type]
        funder_factory=lambda _session: None,
        scanner_factory=lambda _session, _clusters: None,  # type: ignore[return-value, misc]
        fast_slots=fast_slots,
        heavy_slots=heavy_slots,
    )


async def test_a_full_heavy_semaphore_does_not_block_a_fast_scan() -> None:
    heavy_slots = asyncio.Semaphore(FUNDER_GRAPH_CONCURRENCY)
    for _ in range(FUNDER_GRAPH_CONCURRENCY):
        await heavy_slots.acquire()
    fast_slots = asyncio.Semaphore(FAST_SCAN_CONCURRENCY)
    redis = _Redis({RISK_SCAN_QUEUE: _messages("fast", 1)})
    worker = _worker(redis, fast_slots=fast_slots, heavy_slots=heavy_slots)
    entered = asyncio.Event()

    async def scan(mint: str, trace_id: str) -> None:
        del mint, trace_id
        entered.set()

    worker._scan = scan  # type: ignore[method-assign]
    cycle = asyncio.create_task(worker.run_once())
    await asyncio.wait_for(entered.wait(), timeout=1)
    processed, failed = await cycle
    assert (processed, failed) == (1, 0)
    assert redis.acked == [(RISK_SCAN_QUEUE, "0-0")]


async def test_fast_peak_stays_at_eight_and_heavy_peak_stays_at_three() -> None:
    fast_slots = asyncio.Semaphore(FAST_SCAN_CONCURRENCY)
    heavy_slots = asyncio.Semaphore(FUNDER_GRAPH_CONCURRENCY)
    redis = _Redis(
        {
            RISK_SCAN_QUEUE: _messages("fast", 20),
            RISK_HEAVY_QUEUE: _messages("heavy", 20),
        }
    )
    worker = _worker(redis, fast_slots=fast_slots, heavy_slots=heavy_slots)
    fast_release = asyncio.Event()
    heavy_release = asyncio.Event()
    fast_state = {"current": 0, "peak": 0, "entered": 0}
    heavy_state = {"current": 0, "peak": 0, "entered": 0}
    fast_full = asyncio.Event()
    heavy_full = asyncio.Event()

    async def scan(mint: str, trace_id: str) -> None:
        del mint, trace_id
        fast_state["current"] += 1
        fast_state["entered"] += 1
        fast_state["peak"] = max(fast_state["peak"], fast_state["current"])
        if fast_state["entered"] >= FAST_SCAN_CONCURRENCY:
            fast_full.set()
        await fast_release.wait()
        fast_state["current"] -= 1

    async def scan_heavy(mint: str, trace_id: str) -> None:
        del mint, trace_id
        heavy_state["current"] += 1
        heavy_state["entered"] += 1
        heavy_state["peak"] = max(heavy_state["peak"], heavy_state["current"])
        if heavy_state["entered"] >= FUNDER_GRAPH_CONCURRENCY:
            heavy_full.set()
        await heavy_release.wait()
        heavy_state["current"] -= 1

    worker._scan = scan  # type: ignore[method-assign]
    worker._scan_heavy = scan_heavy  # type: ignore[method-assign]

    fast_cycle = asyncio.create_task(worker.run_once())
    heavy_cycle = asyncio.create_task(worker.run_heavy_once())
    await asyncio.wait_for(fast_full.wait(), timeout=2)
    await asyncio.wait_for(heavy_full.wait(), timeout=2)
    await asyncio.sleep(0)
    assert fast_state["entered"] == FAST_SCAN_CONCURRENCY
    assert fast_state["peak"] == FAST_SCAN_CONCURRENCY
    assert heavy_state["entered"] == FUNDER_GRAPH_CONCURRENCY
    assert heavy_state["peak"] == FUNDER_GRAPH_CONCURRENCY
    fast_release.set()
    heavy_release.set()
    assert await fast_cycle == (20, 0)
    assert await heavy_cycle == (20, 0)
    assert fast_state["peak"] == FAST_SCAN_CONCURRENCY
    assert heavy_state["peak"] == FUNDER_GRAPH_CONCURRENCY
