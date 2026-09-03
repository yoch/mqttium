from __future__ import annotations

from mqttium.api._writer import WritePump
from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine


async def _ignore_failure(_exc: BaseException) -> None:
    pass


def _pump(writes: list[bytes]) -> WritePump:
    pump = WritePump(max_bytes=1024, max_messages=16, on_failure=_ignore_failure)

    def write_nowait(data: bytes) -> bool:
        writes.append(data)
        return True

    pump._write_nowait = write_nowait
    pump._eager_armed = True
    pump._ack_eager_armed = True
    return pump


async def test_ack_shaped_data_uses_data_permit_not_wire_classification() -> None:
    writes: list[bytes] = []
    pump = _pump(writes)
    ack_shaped_data = b"\x40\x02\x00\x01"

    assert pump.try_enqueue(ack_shaped_data)
    assert writes == [ack_shaped_data]
    assert pump._eager_armed is False
    assert pump._ack_eager_armed is True


async def test_explicit_ack_uses_independent_ack_permit() -> None:
    writes: list[bytes] = []
    pump = _pump(writes)
    pump._eager_armed = False
    ack = b"\x40\x02\x00\x01"

    assert pump.try_enqueue_ack(ack)
    assert writes == [ack]
    assert pump._eager_armed is False
    assert pump._ack_eager_armed is False


async def test_explicit_ack_does_not_consume_data_permit() -> None:
    writes: list[bytes] = []
    pump = _pump(writes)
    ack = b"\x40\x02\x00\x01"

    assert pump.try_enqueue_ack(ack)
    assert pump._eager_armed is True
    assert pump._ack_eager_armed is False


def test_inbound_qos1_emits_explicit_success_ack_effect() -> None:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv311))
    engine.state = ConnectionState.CONNECTED
    remaining = pack_utf8("ack/provenance") + pack_u16(7) + b"payload"

    engine.handle_raw(RawPacket(PacketType.PUBLISH, 0x02, remaining))

    effects = engine.take_effects()
    assert effects[0].kind is EffectKind.SEND_ACK
    assert effects[1].kind is EffectKind.MESSAGE
