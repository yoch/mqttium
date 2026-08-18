"""Experiment-only async-publish microbatch patch for ARM64 measurements.

This file is intentionally outside the ``mqttium`` package.  It is loaded only
when an experimental candidate ``src`` directory is placed on ``PYTHONPATH`` by
the existing paired benchmark workers.  The release baseline is untouched.

The experiment keeps ``WritePump.try_enqueue`` and ``publish_nowait`` exactly as
shipped in rc7.  Only ``AsyncClient.publish`` gets a post-admission opportunistic
flush, testing whether bounded queue residence can recover async network latency
without giving back the synchronous capacity fix from #254.

Do not ship this file.  A surviving hypothesis must be implemented normally in
``WritePump``/``AsyncClient`` with focused tests.
"""

from __future__ import annotations

import time
from functools import wraps
from typing import Any

from mqttium.api.async_client import AsyncClient
from mqttium.api._writer import WritePump

_INLINE_BATCH_ITEMS = 8
_ORIGINAL_PUBLISH = AsyncClient.publish


def _restore_batch(pump: WritePump, items: list[bytes]) -> None:
    queue = pump.queue
    for _ in items:
        queue.task_done()
    for item in items:
        queue.put_nowait(item)


def _try_flush_async_publish_batch(pump: WritePump) -> bool:
    """Flush one exact contiguous group without changing the base writer policy."""
    write_nowait = pump._write_nowait  # noqa: SLF001 - intentional experiment
    queue = pump.queue
    if (
        write_nowait is None
        or pump._writing  # noqa: SLF001 - intentional experiment
        or pump.waiters
        or queue.qsize() != _INLINE_BATCH_ITEMS
    ):
        return False

    pump._sample_high_water()  # noqa: SLF001 - intentional experiment
    raw: list[Any] = [queue.get_nowait() for _ in range(_INLINE_BATCH_ITEMS)]
    if any(not isinstance(item, bytes) for item in raw):
        for _ in raw:
            queue.task_done()
        for item in raw:
            queue.put_nowait(item)
        return False

    items = raw
    combined = b"".join(items)
    if not write_nowait(combined):
        _restore_batch(pump, items)
        return False

    released = len(combined)
    for _ in items:
        queue.task_done()
    pump.queued_bytes -= released
    if pump.queued_bytes < 0:
        raise AssertionError("writer queued-byte accounting underflow")
    pump.batches += 1
    pump.batched_items += len(items)
    pump.batched_bytes += released
    pump.last_outbound = time.monotonic()
    return True


@wraps(_ORIGINAL_PUBLISH)
async def _publish_with_async_microbatch(
    client: AsyncClient,
    *args: Any,
    **kwargs: Any,
) -> Any:
    receipt = await _ORIGINAL_PUBLISH(client, *args, **kwargs)
    _try_flush_async_publish_batch(client._write_pump)  # noqa: SLF001
    return receipt


AsyncClient.publish = _publish_with_async_microbatch  # type: ignore[method-assign]
