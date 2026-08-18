"""Experiment-only QoS-aware writer microbatch for ARM64 measurement.

Loaded through PYTHONPATH/sitecustomize by benchmark workers only.

Policy under test:
- preserve rc7's QoS 0 path exactly: no extra producer-side transport work;
- for QoS 1/2, after normal admission/effect application, opportunistically
  flush exactly four contiguous queued byte frames as one joined write;
- keep the underlying WritePump policy untouched, so tails and all other
  traffic retain rc7's writer batching/backpressure semantics.

This targets the measured split directly: rc7 wins QoS 0 admission capacity,
while short producer-side batches improved QoS 1 capacity and sharply reduced
ACK/delivery latency.  b"".join()+write_nowait() is intentional for four small
frames; the preceding writelines/sendmsg experiment added excessive producer
cost on the ARM64 target.

Do not ship this monkeypatch. A surviving policy must be implemented normally
with ordering, accounting, lifecycle and backpressure tests.
"""

from __future__ import annotations

import asyncio
import time

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import QoS
from mqttium.errors import FlowControlError
from mqttium.types import Properties

_BATCH_ITEMS = 4


def _try_flush_qos_batch(client: AsyncClient) -> bool:
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


def _publish_nowait_qos_aware(
    self: AsyncClient,
    topic: str,
    payload: bytes | str = b"",
    *,
    qos: int | QoS = 0,
    retain: bool = False,
    properties: Properties | None = None,
) -> PublishReceipt:
    # rc7 AsyncClient.publish_nowait(), with only the QoS>0 flush at the end.
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as exc:
        raise RuntimeError(
            "publish_nowait() must be called from the client's event-loop thread"
        ) from exc
    owner_loop = self._owner_loop  # noqa: SLF001
    if owner_loop is None:
        self._owner_loop = loop  # noqa: SLF001
    elif owner_loop is not loop:
        raise RuntimeError("AsyncClient is bound to a different event loop")

    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    direct = self._try_direct_qos0_publish(  # noqa: SLF001
        topic,
        data,
        qos=qos,
        retain=retain,
        properties=properties,
        nowait=True,
    )
    if direct is not None:
        return direct

    self._check_nowait_publish_capacity(  # noqa: SLF001
        topic, data, qos, retain, properties
    )
    receipt = self._queue_publish_on_loop(  # noqa: SLF001
        topic,
        data,
        qos=qos,
        retain=retain,
        properties=properties,
    )
    self._finalize_loop_commands()  # noqa: SLF001

    if receipt.qos != QoS.AT_MOST_ONCE:
        pump = self._write_pump  # noqa: SLF001
        if pump.queue.qsize() == _BATCH_ITEMS:
            _try_flush_qos_batch(self)
    return receipt


async def _publish_qos_aware(
    self: AsyncClient,
    topic: str,
    payload: bytes | str = b"",
    *,
    qos: int | QoS = 0,
    retain: bool = False,
    properties: Properties | None = None,
    nowait: bool = False,
) -> PublishReceipt:
    # rc7 AsyncClient.publish(), with only the QoS>0 flush before return.
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

            if receipt.qos != QoS.AT_MOST_ONCE:
                pump = self._write_pump  # noqa: SLF001
                if pump.queue.qsize() == _BATCH_ITEMS:
                    _try_flush_qos_batch(self)
            return receipt
        try:
            await self._publish_space.wait()  # noqa: SLF001
        finally:
            self._publish_waiters -= 1  # noqa: SLF001


AsyncClient.publish_nowait = _publish_nowait_qos_aware  # type: ignore[method-assign]
AsyncClient.publish = _publish_qos_aware  # type: ignore[method-assign]
