"""Experiment-only writer microbatch patch for ARM64 paired measurements.

This file is intentionally outside the ``mqttium`` package.  Python imports
``sitecustomize`` at interpreter startup when the candidate ``src`` directory
is on ``PYTHONPATH``, which lets the existing paired harness measure an exact
runtime hypothesis without changing the baseline tree or release-gate policy.

Do not ship this file.  If the hypothesis survives the release-grade gates, the
mechanism must be implemented normally inside ``WritePump`` with focused tests.
"""

from __future__ import annotations

import time
from typing import Any

from mqttium.api._writer import WritePump

_INLINE_BATCH_ITEMS = 4
_ORIGINAL_TRY_ENQUEUE = WritePump.try_enqueue


def _restore_batch(pump: WritePump, items: list[bytes]) -> None:
    """Restore a speculative whole-queue batch without changing join accounting."""
    queue = pump.queue
    for _ in items:
        queue.task_done()
    for item in items:
        queue.put_nowait(item)


def _try_inline_microbatch(pump: WritePump) -> bool:
    """Flush one exact small burst group without waiting for the writer task.

    The producer-side path is deliberately narrow:

    * exactly four queued frames, so a failed speculative flush can be restored
      without moving them behind older work;
    * only contiguous ``bytes`` frames, never segmented writes;
    * no writer batch in flight and no producer waiting for queue space;
    * one existing ``write_nowait`` call for the *combined* bytes, not four
      individual eager writes.

    A tight burst therefore becomes ``1 eager + groups of 4 + writer tail``.
    A paced producer remains unchanged: its first frame still takes the normal
    zero-hop eager path.
    """
    write_nowait = pump._write_nowait  # noqa: SLF001 - intentional experiment
    queue = pump.queue
    if (
        write_nowait is None
        or pump._writing  # noqa: SLF001 - intentional experiment
        or pump.waiters
        or queue.qsize() != _INLINE_BATCH_ITEMS
    ):
        return False

    # The writer may not get a chance to observe this short-lived depth before
    # the producer drains it, so preserve the queue telemetry first.
    pump._sample_high_water()  # noqa: SLF001 - intentional experiment

    raw: list[Any] = [queue.get_nowait() for _ in range(_INLINE_BATCH_ITEMS)]
    if any(not isinstance(item, bytes) for item in raw):
        # qsize was exactly the batch size, so reinsertion preserves FIFO order.
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


def _try_enqueue_with_inline_microbatch(
    pump: WritePump,
    item: Any,
    *,
    epoch: int | None = None,
) -> bool:
    eager_before = pump.eager_writes
    accepted = _ORIGINAL_TRY_ENQUEUE(pump, item, epoch=epoch)
    if not accepted or pump.eager_writes != eager_before:
        return accepted
    _try_inline_microbatch(pump)
    return accepted


WritePump.try_enqueue = _try_enqueue_with_inline_microbatch  # type: ignore[method-assign]
