"""Representative codec workload for a profile-guided native build (build-time only).

Run by the build hook against the instrumented extension. It drives the
protocol engine and decoder the way a connected client does, without a broker:
QoS 0/1/2 publication and reception for MQTT 3.1.1 and 5, framing of single,
batched and fragmented reads, properties and subscription packets.
"""

from __future__ import annotations

import os
import sys

from mqttium.codec._backend import BACKEND
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import PUBLISH, decode_properties, encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    SubAckPacket,
    SubscribeOptions,
    SubscribePacket,
    Subscription,
    encode_frame,
)
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import Properties

TOPIC = "sensors/site-4/temperature"
SMALL = b'{"t":21.5,"h":40}'
LARGE = b"z" * 4096
PROPERTIES = Properties(
    {
        "content_type": "application/json",
        "user_property": (("site", "north"), ("sensor", "temperature")),
        "message_expiry_interval": 60,
    }
)


def _connected(protocol: MQTTProtocolVersion) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="train", protocol=protocol), store=MemoryInflightStore()
    )
    engine.begin_connect()
    body = b"\x00\x00" if protocol == MQTTProtocolVersion.MQTTv311 else b"\x00\x00\x00"
    _feed(engine, IncrementalDecoder(), encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()
    return engine


def _feed(engine: ProtocolEngine, decoder: IncrementalDecoder, wire: bytes) -> int:
    decoder.feed(wire)
    count = 0
    while (raw := decoder.next_packet()) is not None:
        engine.handle_raw(raw)
        count += 1
    return count


def _publish(
    engine: ProtocolEngine, protocol: MQTTProtocolVersion, decoder: IncrementalDecoder
) -> None:
    properties = PROPERTIES if protocol == MQTTProtocolVersion.MQTTv5 else None
    for payload in (SMALL, SMALL, SMALL, LARGE):
        engine.queue_publish(TOPIC, payload, qos=0, properties=properties)
        engine.take_effects()
        handle = engine.queue_publish(TOPIC, payload, qos=1)
        engine.take_effects()
        _feed(engine, decoder, PubAckPacket(mid=handle.mid or 0).encode(protocol))
        engine.take_effects()
    handle = engine.queue_publish(TOPIC, SMALL, qos=2, properties=properties)
    engine.take_effects()
    mid = handle.mid or 0
    _feed(engine, decoder, PubRecPacket(mid=mid).encode(protocol))
    engine.take_effects()
    _feed(engine, decoder, PubCompPacket(mid=mid).encode(protocol))
    engine.take_effects()


def _receive(
    engine: ProtocolEngine, protocol: MQTTProtocolVersion, decoder: IncrementalDecoder, mid: int
) -> int:
    def publish(qos: int, payload: bytes, packet_id: int | None) -> bytes:
        return PublishPacket(
            topic=TOPIC,
            payload=payload,
            qos=QoS(qos),
            retain=False,
            dup=False,
            mid=packet_id,
        ).encode(protocol)

    # One frame per read, the common case at moderate rates.
    for _ in range(4):
        _feed(engine, decoder, publish(0, SMALL, None))
        engine.take_effects()
        mid = mid % 60000 + 1
        _feed(engine, decoder, publish(1, SMALL, mid))
        engine.take_effects()
    # Batched and fragmented reads.
    batch = b"".join(publish(0, SMALL, None) for _ in range(16))
    _feed(engine, decoder, batch)
    engine.take_effects()
    wire = publish(0, LARGE, None) + publish(0, SMALL, None)
    for start in range(0, len(wire), 1400):
        _feed(engine, decoder, wire[start : start + 1400])
    engine.take_effects()
    mid = mid % 60000 + 1
    _feed(engine, decoder, publish(2, SMALL, mid))
    engine.take_effects()
    _feed(engine, decoder, PubRelPacket(mid=mid).encode(protocol))
    engine.take_effects()
    return mid


def _control(protocol: MQTTProtocolVersion) -> None:
    wire = encode_properties(PROPERTIES, PUBLISH)
    decode_properties(wire, 0, PUBLISH)
    options = SubscribeOptions(qos=QoS.AT_LEAST_ONCE)
    SubscribePacket(mid=7, subscriptions=(Subscription("a/+/c", options),)).encode(protocol)
    properties = b"\x00" if protocol == MQTTProtocolVersion.MQTTv5 else b""
    SubAckPacket.decode(b"\x00\x07" + properties + b"\x01", protocol)


def main(rounds: int = 3000) -> None:
    if os.environ.get("MQTTIUM_CODEC_BACKEND") == "native" and BACKEND != "native":
        raise SystemExit("training requires the native codec")
    engines = {
        protocol: (_connected(protocol), IncrementalDecoder())
        for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5)
    }
    mid = 0
    for index in range(rounds):
        for protocol, (engine, decoder) in engines.items():
            _publish(engine, protocol, decoder)
            mid = _receive(engine, protocol, decoder, mid)
            if index % 10 == 0:
                _control(protocol)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 3000)
