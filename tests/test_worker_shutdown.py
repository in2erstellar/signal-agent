"""SIGTERM finishes the cycle that is already writing, then stops."""

from __future__ import annotations

import asyncio

from app.workers.runner import poll_loop, resilient_loop


async def test_stop_waits_for_the_open_cycle_to_commit() -> None:
    stop = asyncio.Event()
    phases: list[str] = []

    async def cycle() -> None:
        phases.append("start")
        await asyncio.sleep(0.05)
        phases.append("committed")

    task = asyncio.create_task(poll_loop(stop, cycle, interval_seconds=30))
    await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=1)
    assert phases == ["start", "committed"]


async def test_resilient_loop_continues_after_a_redis_timeout() -> None:
    stop = asyncio.Event()
    calls = 0

    async def cycle() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("Timeout reading from redis")
        stop.set()

    await resilient_loop(stop, cycle, name="risk_scan", pause_seconds=0)
    assert calls == 2
