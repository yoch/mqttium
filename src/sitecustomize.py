"""Experiment-only awaited QoS microbatch lab.

Loaded through PYTHONPATH/sitecustomize by benchmark workers only.

The experiment preserves rc7's WritePump, publish_nowait(), QoS 0 path and
publish(..., nowait=True) path. Only ordinary awaited QoS 1/2 publish() may
flush an exact short queue as one joined write. The batch size is selected by
MQTTIUM_EXPERIMENT_BATCH_ITEMS so the temporary labs can sweep nearby points.

Do not ship this monkeypatch or environment knob.
"""

from __future__ import annotations

import os
import time

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import QoS
from mqttium.errors import FlowControlError
from mqttium.types import Properties


def _batch_items() -> int:
    value = int(os.environ.get("MQTTIUM_EXPERIMENT_BATCH_ITEMS", "4"))
    if value < 2:
        raise RuntimeError("MQTTIUM_EXPERIMENT_BATCH_ITEMS must be >= 2")
    return value


_BATCH_ITEMS = _batch_items()


def _try_flush_awaited_qos_batch(client: AsyncClient) -> bool:
    pump = client._write_pump  # noqa: SLF001 - experiment
    queue = pump.queue
    if queue.qsize() != _BATCH_ITEMS:
        return False

    write_nowait = pump._write_nowait  # noqa: SLF001 - experiment
    if write_nowait is None or pump._writing or pump.waiters:  # noqa: SLF001
        return False

    pump._sample_high_water()  # noqa: SLF001 - experiment
    raw = [queue.get_nowait() for _ in range(_BATCH_ITEMS)]
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
    pump.batched_items += len(raw)
    pump.batched_bytes += len(combined)
    pump.last_outbound = time.monotonic()
    return True


async def _publish_awaited_microbatch(
    self: AsyncClient,
    topic: str,
    payload: bytes | str = b"",
    *,
    qos: int | QoS = 0,
    retain: bool = False,
    properties: Properties | None = None,
    nowait: bool = False,
) -> PublishReceipt:
    # rc7 AsyncClient.publish(), with one experiment-only QoS1/2 flush.
    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    while True:
        wait_for_space = False
        async with self._engine_lock:  # noqa: SLF001
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

            if not nowait and receipt.qos != QoS.AT_MOST_ONCE:
                pump = self._write_pump  # noqa: SLF001
                if pump.queue.qsize() == _BATCH_ITEMS:
                    _try_flush_awaited_qos_batch(self)
            return receipt
        try:
            await self._publish_space.wait()  # noqa: SLF001
        finally:
            self._publish_waiters -= 1  # noqa: SLF001


AsyncClient.publish = _publish_awaited_microbatch  # type: ignore[method-assign]
