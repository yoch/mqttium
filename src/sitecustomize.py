"""Experiment-only integrated async microbatch for ARM64 measurement.

Loaded through PYTHONPATH/sitecustomize by paired benchmark workers only.

This experiment keeps rc7's WritePump and publish_nowait path untouched.  It
replaces AsyncClient.publish with the rc7 implementation plus one post-admission
check: ordinary awaited publish() may synchronously flush exactly four queued
contiguous byte frames as one joined buffer.  publish(..., nowait=True) does not
take this latency-oriented shortcut.

Compared with the earlier proof-of-mechanism, this removes the wrapper coroutine
and avoids probing the queue through a second call on every publish.  Compared
with producer-side vector-microbatch, it never changes publish_nowait capacity
and uses one small memcpy + write() rather than sendmsg/writelines for four tiny
frames.

Do not ship this monkeypatch.  A surviving design must be implemented normally
with lifecycle/order/backpressure tests.
"""

from __future__ import annotations

import time

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import QoS
from mqttium.errors import FlowControlError
from mqttium.types import Properties

_INLINE_BATCH_ITEMS = 4


def _try_flush_async_publish_batch(client: AsyncClient) -> bool:
    pump = client._write_pump  # noqa: SLF001 - experiment
    queue = pump.queue

    # The exact-size check is deliberate.  It means failure restoration cannot
    # reorder these frames behind later queue entries.
    if queue.qsize() != _INLINE_BATCH_ITEMS:
        return False

    write_nowait = pump._write_nowait  # noqa: SLF001 - experiment
    if (
        write_nowait is None
        or pump._writing  # noqa: SLF001 - experiment
        or pump.waiters
    ):
        return False

    pump._sample_high_water()  # noqa: SLF001 - experiment
    raw = [queue.get_nowait() for _ in range(_INLINE_BATCH_ITEMS)]
    if any(not isinstance(item, bytes) for item in raw):
        for _ in raw:
            queue.task_done()
        for item in raw:
            queue.put_nowait(item)
        return False

    combined = b"".join(raw)
    if not write_nowait(combined):
        for _ in raw:
            queue.task_done()
        for item in raw:
            queue.put_nowait(item)
        return False

    released = len(combined)
    for _ in raw:
        queue.task_done()
    pump.queued_bytes -= released
    if pump.queued_bytes < 0:
        raise AssertionError("writer queued-byte accounting underflow")
    pump.batches += 1
    pump.batched_items += len(raw)
    pump.batched_bytes += released
    pump.last_outbound = time.monotonic()
    return True


async def _publish_async_microbatch(
    self: AsyncClient,
    topic: str,
    payload: bytes | str = b"",
    *,
    qos: int | QoS = 0,
    retain: bool = False,
    properties: Properties | None = None,
    nowait: bool = False,
) -> PublishReceipt:
    # This body is rc7 AsyncClient.publish verbatim apart from the marked
    # opportunistic flush immediately before the successful return.
    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    while True:
        wait_for_space = False
        async with self._engine_lock:  # noqa: SLF001 - copied rc7 method body
            try:
                direct = self._try_direct_qos0_publish(  # noqa: SLF001
                    topic,
                    data,
                    qos=qos,
                    retain=retain,
                    properties=properties,
                    nowait=nowait,
                )
                if direct is not None:
                    return direct
                if nowait:
                    self._check_nowait_publish_capacity(  # noqa: SLF001
                        topic, data, qos, retain, properties
                    )
                handle = self._engine.outbound.queue_publish(  # noqa: SLF001
                    topic,
                    data,
                    qos=qos,
                    retain=retain,
                    properties=properties,
                )
                if handle.qos == QoS.AT_MOST_ONCE:
                    receipt = PublishReceipt(mid=None, qos=handle.qos)
                else:
                    assert handle.mid is not None
                    receipt = PublishReceipt(mid=handle.mid, qos=handle.qos)
                    self._register_publish_receipt(handle.mid, receipt)  # noqa: SLF001
            except FlowControlError as flow_exc:
                if (
                    nowait
                    or self._publish_backpressure == "error"  # noqa: SLF001
                    or not self._engine.can_ever_admit_publish(  # noqa: SLF001
                        topic, data, qos, properties
                    )
                ):
                    raise
                terminal = self._publish_wait_failure()  # noqa: SLF001
                if terminal is not None:
                    raise terminal from flow_exc
                self._publish_space.clear()  # noqa: SLF001
                self._publish_waiters += 1  # noqa: SLF001
                wait_for_space = True
            else:
                self._collect_effects_locked()  # noqa: SLF001
                self._drain_effects_inline()  # noqa: SLF001
        if not wait_for_space:
            if self._effect_pump.pending:  # noqa: SLF001
                if nowait:
                    self._schedule_effect_flush()  # noqa: SLF001
                else:
                    await self._drain_effects()  # noqa: SLF001

            # Experiment-only delta from rc7.  The public nowait contract keeps
            # the exact rc7 admission/capacity strategy.
            if not nowait:
                pump = self._write_pump  # noqa: SLF001
                if pump.queue.qsize() == _INLINE_BATCH_ITEMS:
                    _try_flush_async_publish_batch(self)
            return receipt
        try:
            await self._publish_space.wait()  # noqa: SLF001
        finally:
            self._publish_waiters -= 1  # noqa: SLF001


AsyncClient.publish = _publish_async_microbatch  # type: ignore[method-assign]
