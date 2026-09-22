"""Property-heavy replay respects the same logical-byte bound in both stores."""

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.enums import InboundQoSState, MQTTProtocolVersion, PacketType, QoS
from mqttium.persistence import MemoryInflightStore, SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import REPLAY_BATCH_BYTES
from mqttium.types import InboundMessage, Properties
from tests.support import stored_record


class _LoosePageStore(MemoryInflightStore):
    """Exercise the independent engine guard, not the store's page guard."""

    def in_replay_pages(self, max_messages=64, max_bytes=1 << 20):
        yield tuple(self.get_in(mid) for mid in (1, 2, 3))


@pytest.mark.parametrize("kind", ["memory", "sqlite", "loose"])
@pytest.mark.parametrize("property_count", [12, 20])
def test_property_bytes_bound_pages_and_engine_replay(tmp_path, kind, property_count):
    store = (
        SqliteInflightStore(tmp_path / "replay.db")
        if kind == "sqlite"
        else _LoosePageStore()
        if kind == "loose"
        else MemoryInflightStore()
    )
    properties = Properties(
        {"user_property": tuple((f"key{i}", "x" * 60000) for i in range(property_count))}
    )
    try:
        for mid in (1, 2, 3):
            store.put_in(
                stored_record(
                    InboundMessage(
                        mid=mid,
                        topic="t",
                        payload=b"x",
                        qos=QoS.EXACTLY_ONCE,
                        retain=False,
                        state=InboundQoSState.WAIT_PUBREL,
                        properties=properties,
                    )
                )
            )
        if kind != "loose":
            pages = list(store.in_replay_pages(64, REPLAY_BATCH_BYTES))
            assert [len(page) for page in pages] == [1, 1, 1]
            assert (pages[0][0].logical_size > REPLAY_BATCH_BYTES) == (property_count == 20)
        engine = ProtocolEngine(
            EngineConfig(
                client_id="property-replay",
                protocol=MQTTProtocolVersion.MQTTv5,
                clean_start=False,
            ),
            store=store,
        )
        engine.begin_connect()
        engine.take_effects()
        engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x01\x00\x00"))
        seen = []
        for _ in range(4):
            messages = [
                effect.data for effect in engine.take_effects() if effect.kind is EffectKind.MESSAGE
            ]
            assert len(messages) <= 1
            seen.extend(message.mid for message in messages)
            if not engine.inbound.replay_pending:
                break
            engine.continue_inbound_replay()
        assert seen == [1, 2, 3]
        assert not engine.inbound.replay_pending
    finally:
        if isinstance(store, SqliteInflightStore):
            store.close()
