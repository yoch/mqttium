"""A publication whose admission failed is never sent later by the same client.

When admission fails after the durable row was written and the cleanup
delete also fails, the row stays in the store. Releasing its packet
identifier then let this client replay a publication whose caller already
saw the failure, and let a later publication overwrite the row. The row is
sealed instead (#521): kept for recovery, never sent by this client.
"""

from __future__ import annotations

import pytest

from mqttium.enums import PacketType, QoS
from mqttium.packets import encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from tests.support import feed_engine


class _DeleteFails(MemoryInflightStore):
    def delete_out(self, mid: int) -> None:
        raise OSError("store unavailable")


def _connect(engine: ProtocolEngine, *, session_present: bool) -> list:
    engine.begin_connect()
    engine.take_effects()
    flags = b"\x01" if session_present else b"\x00"
    feed_engine(engine, encode_frame(PacketType.CONNACK, 0, flags + b"\x00"))
    return engine.take_effects()


def test_failed_admission_with_failed_cleanup_is_sealed() -> None:
    store = _DeleteFails()
    engine = ProtocolEngine(EngineConfig(client_id="c", clean_start=False), store)
    _connect(engine, session_present=False)

    original_send = engine._send

    def fail_once(item: object) -> None:
        engine._send = original_send  # type: ignore[method-assign]
        raise RuntimeError("fault after the row was written")

    engine._send = fail_once  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="fault after the row"):
        engine.queue_publish("t", b"x", qos=QoS.AT_LEAST_ONCE)

    orphan = [s.mid for page in store.out_summary_pages() for s in page]
    assert orphan == [1]
    assert engine.packet_ids.in_use(1)
    assert engine.outbound.unacknowledged_messages == 0

    fresh = engine.queue_publish("u", b"y", qos=QoS.AT_LEAST_ONCE).mid
    assert fresh != 1
    engine.take_effects()
    engine.notify_transport_closed()
    engine.take_effects()
    sends = [e for e in _connect(engine, session_present=True) if e.kind.name == "SEND"]
    assert len(sends) == 1  # only the fresh publication is replayed
    record = store.get_out(1)
    assert record is not None and record.topic == "t"  # kept for recovery
