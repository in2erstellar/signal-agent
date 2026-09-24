"""SIGTERM finishes the cycle that is already writing, then stops."""

from __future__ import annotations

import asyncio

from app.workers.runner import poll_loop


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
