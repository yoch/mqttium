"""Releasing SUBSCRIBE/UNSUBSCRIBE identifiers never frees a sealed one.

A sealed publication (#521) no longer counts as unacknowledged, but its
durable row and packet identifier stay reserved. When the transport closed
with a SUBSCRIBE in flight and only sealed publications left, the engine
reset the whole identifier pool, so a later publication reused the sealed
identifier and overwrote its row.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.enums import OutboundQoSState, PacketType, QoS
from mqttium.packets import encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from tests.support import feed_engine


def _connect(engine: ProtocolEngine, *, session_present: bool) -> None:
    engine.begin_connect()
    engine.take_effects()
    flags = b"\x01" if session_present else b"\x00"
    feed_engine(engine, encode_frame(PacketType.CONNACK, 0, flags + b"\x00"))
    engine.take_effects()


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_subscription_release_keeps_sealed_identifiers(kind: str, tmp_path: Path) -> None:
    store = MemoryInflightStore() if kind == "memory" else SqliteInflightStore(tmp_path / "s.db")
    try:
        engine = ProtocolEngine(EngineConfig(client_id="c", clean_start=False), store)
        _connect(engine, session_present=False)
        sealed = engine.queue_publish("t", b"x", qos=QoS.AT_LEAST_ONCE).mid
        assert sealed is not None
        engine.queue_subscribe([("f/#", 0)])
        engine.take_effects()
        engine.seal_publications([sealed])
        assert engine.outbound.unacknowledged_messages == 0

        engine.notify_transport_closed()
        engine.take_effects()
        assert engine.packet_ids.in_use(sealed)

        _connect(engine, session_present=True)
        fresh = engine.queue_publish("u", b"y", qos=QoS.AT_LEAST_ONCE).mid
        assert fresh != sealed
        record = store.get_out(sealed)
        assert record is not None and record.topic == "t"
        assert record.state is OutboundQoSState.WAIT_PUBACK
    finally:
        if isinstance(store, SqliteInflightStore):
            store.close()
