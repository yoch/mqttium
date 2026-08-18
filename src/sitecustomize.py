"""Experiment-only producer-side vector microbatch for ARM64 measurement.

Loaded through PYTHONPATH/sitecustomize by paired benchmark workers only.

This sibling uses two-frame vector batches to explore the latency end of the
new producer-side coalescing design. It keeps rc7's first eager frame exactly
as-is and applies the same mechanism to async publish and publish_nowait.

Do not ship this monkeypatch. A surviving design must be implemented normally
with lifecycle/order/backpressure tests.
"""

from __future__ import annotations

import time

from mqttium.api._effects import StaleConnectionEffect
from mqttium.api._writer import WritePump
from mqttium.transport._stream import StreamTransport, _WRITE_BUFFER_HIGH_WATER
from mqttium.transport.writes import WriteItem, item_size

_INLINE_BATCH_ITEMS = 2


def _write_many_nowait(self: StreamTransport, parts: list[bytes]) -> bool:
    """Attempt one non-awaiting vector write below the local high-water mark."""
    if not parts:
        return True
    if len(parts) == 1:
        return self.write_nowait(parts[0])

    transport = self._writer.transport  # noqa: SLF001 - experiment
    total = sum(map(len, parts))
    if (
        transport is not None
        and transport.get_write_buffer_size() + total > _WRITE_BUFFER_HIGH_WATER
    ):
        return False
    self._writer.writelines(parts)  # noqa: SLF001 - experiment
    return True


def _restore_batch(pump: WritePump, items: list[WriteItem]) -> None:
    queue = pump.queue
    for _ in items:
        queue.task_done()
    for item in items:
        queue.put_nowait(item)


def _try_flush_inline_batch(pump: WritePump) -> bool:
    """Flush one exact contiguous byte batch without yielding to the writer."""
    transport = pump.transport
    write_many_nowait = (
        None if transport is None else getattr(transport, "write_many_nowait", None)
    )
    queue = pump.queue
    if (
        write_many_nowait is None
        or pump._writing  # noqa: SLF001 - experiment
        or pump.waiters
        or queue.qsize() != _INLINE_BATCH_ITEMS
    ):
        return False

    pump._sample_high_water()  # noqa: SLF001 - experiment
    raw: list[WriteItem] = [queue.get_nowait() for _ in range(_INLINE_BATCH_ITEMS)]
    if any(not isinstance(item, bytes) for item in raw):
        _restore_batch(pump, raw)
        return False

    parts = [item for item in raw if isinstance(item, bytes)]
    if not write_many_nowait(parts):
        _restore_batch(pump, raw)
        return False

    released = sum(map(len, parts))
    for _ in raw:
        queue.task_done()
    pump.queued_bytes -= released
    if pump.queued_bytes < 0:
        raise AssertionError("writer queued-byte accounting underflow")
    pump.batches += 1
    pump.batched_items += len(parts)
    pump.batched_bytes += released
    pump.last_outbound = time.monotonic()
    return True


def _try_enqueue_vector_microbatch(
    self: WritePump,
    item: WriteItem,
    *,
    epoch: int | None = None,
) -> bool:
    if epoch is None:
        epoch = self.epoch
    if epoch != self.epoch:
        raise StaleConnectionEffect

    size = item_size(item)
    if not self.can_enqueue_size(size):
        return False
    if self._try_write_eager(item):  # noqa: SLF001 - experiment
        return True

    self.queue.put_nowait(item)
    self.queued_bytes += size
    _try_flush_inline_batch(self)
    return True


StreamTransport.write_many_nowait = _write_many_nowait  # type: ignore[attr-defined]
WritePump.try_enqueue = _try_enqueue_vector_microbatch  # type: ignore[method-assign]
