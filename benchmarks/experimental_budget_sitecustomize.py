"""Temporary awaited QoS microbatch using a byte budget.

Lab only. QoS0, publish_nowait(), publish(..., nowait=True), and WritePump stay rc7.
Ordinary awaited QoS1/2 publish() may flush an exact short queue as one joined
write. The final screening candidate uses a 24 KiB target capped at 32 frames.
"""
from __future__ import annotations

import time

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import QoS
from mqttium.errors import FlowControlError
from mqttium.types import Properties

_TARGET_BYTES = 24 * 1024
_MAX_ITEMS = 32
_MIN_ITEMS = 4
_ESTIMATED_OVERHEAD = 16


def _batch_items(data: bytes) -> int:
    frame_size = max(1, len(data) + _ESTIMATED_OVERHEAD)
    return max(_MIN_ITEMS, min(_MAX_ITEMS, _TARGET_BYTES // frame_size))


def _try_flush(client: AsyncClient, count: int) -> bool:
    pump = client._write_pump
    queue = pump.queue
    if queue.qsize() != count:
        return False
    write_nowait = pump._write_nowait
    if write_nowait is None or pump._writing or pump.waiters:
        return False
    raw = [queue.get_nowait() for _ in range(count)]
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
    for _ in raw:
        queue.task_done()
    pump.queued_bytes -= len(combined)
    if pump.queued_bytes < 0:
        raise AssertionError("writer queued-byte accounting underflow")
    pump.batches += 1
    pump.batched_items += count
    pump.batched_bytes += len(combined)
    pump.last_outbound = time.monotonic()
    return True


async def _publish(
    self: AsyncClient,
    topic: str,
    payload: bytes | str = b"",
    *,
    qos: int | QoS = 0,
    retain: bool = False,
    properties: Properties | None = None,
    nowait: bool = False,
) -> PublishReceipt:
    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    while True:
        wait_for_space = False
        async with self._engine_lock:
            try:
                direct = self._try_direct_qos0_publish(
                    topic, data, qos=qos, retain=retain, properties=properties, nowait=nowait
                )
                if direct is not None:
                    return direct
                if nowait:
                    self._check_nowait_publish_capacity(topic, data, qos, retain, properties)
                handle = self._engine.outbound.queue_publish(
                    topic, data, qos=qos, retain=retain, properties=properties
                )
                if handle.qos == QoS.AT_MOST_ONCE:
                    receipt = PublishReceipt(mid=None, qos=handle.qos)
                else:
                    assert handle.mid is not None
                    receipt = PublishReceipt(mid=handle.mid, qos=handle.qos)
                    self._register_publish_receipt(handle.mid, receipt)
            except FlowControlError as flow_exc:
                if (
                    nowait
                    or self._publish_backpressure == "error"
                    or not self._engine.can_ever_admit_publish(topic, data, qos, properties)
                ):
                    raise
                terminal = self._publish_wait_failure()
                if terminal is not None:
                    raise terminal from flow_exc
                self._publish_space.clear()
                self._publish_waiters += 1
                wait_for_space = True
            else:
                self._collect_effects_locked()
                self._drain_effects_inline()
        if not wait_for_space:
            if self._effect_pump.pending:
                if nowait:
                    self._schedule_effect_flush()
                else:
                    await self._drain_effects()
            if not nowait and receipt.qos != QoS.AT_MOST_ONCE:
                count = _batch_items(data)
                if self._write_pump.queue.qsize() == count:
                    _try_flush(self, count)
            return receipt
        try:
            await self._publish_space.wait()
        finally:
            self._publish_waiters -= 1


AsyncClient.publish = _publish
