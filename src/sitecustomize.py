"""Experiment-only cooperative QoS writer handoff for ARM64 measurement.

Loaded through PYTHONPATH/sitecustomize by benchmark workers only.

Policy under test:
- preserve rc7's synchronous publish_nowait path exactly;
- preserve rc7's WritePump/eager/batching policy exactly;
- for ordinary awaited QoS 1/2 publish(), yield one event-loop turn after a
  small bounded queue has accumulated behind the first eager write;
- the existing writer then drains that short group through write_many() ->
  StreamWriter.writelines(), keeping scatter/gather in the writer task where
  it can be amortized and preserving producer-side admission capacity.

The default is seven queued frames (roughly eight publications in the common
case because rc7 sends the first frame eagerly).  The temporary ARM64 lab may
override it with MQTTIUM_EXPERIMENT_HANDOFF_QUEUED to compare nearby fixed
handoff points without creating one branch per threshold.

Do not ship this monkeypatch or the environment knob. A surviving policy must
be implemented normally with ordering, lifecycle, fairness and backpressure
tests.
"""

from __future__ import annotations

import asyncio
import os

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import QoS
from mqttium.errors import FlowControlError
from mqttium.types import Properties


def _handoff_queued_items() -> int:
    raw = os.environ.get("MQTTIUM_EXPERIMENT_HANDOFF_QUEUED", "7")
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            "MQTTIUM_EXPERIMENT_HANDOFF_QUEUED must be an integer"
        ) from exc
    if value <= 0:
        raise RuntimeError("MQTTIUM_EXPERIMENT_HANDOFF_QUEUED must be positive")
    return value


_HANDOFF_QUEUED_ITEMS = _handoff_queued_items()


async def _publish_cooperative_handoff(
    self: AsyncClient,
    topic: str,
    payload: bytes | str = b"",
    *,
    qos: int | QoS = 0,
    retain: bool = False,
    properties: Properties | None = None,
    nowait: bool = False,
) -> PublishReceipt:
    # rc7 AsyncClient.publish(), with one cooperative handoff before the
    # successful return for ordinary awaited QoS 1/2 publications.
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

            if (
                not nowait
                and receipt.qos != QoS.AT_MOST_ONCE
                and self._write_pump.queue.qsize() >= _HANDOFF_QUEUED_ITEMS  # noqa: SLF001
            ):
                # Let the already-awake rc7 writer consume the queued group.
                # Unlike producer-side micro-flushes this performs no transport
                # syscall/copy here; StreamTransport.write_many() retains the
                # existing writelines/scatter-gather path.
                await asyncio.sleep(0)
            return receipt
        try:
            await self._publish_space.wait()  # noqa: SLF001
        finally:
            self._publish_waiters -= 1  # noqa: SLF001


AsyncClient.publish = _publish_cooperative_handoff  # type: ignore[method-assign]
