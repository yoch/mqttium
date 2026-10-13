"""Representative codec workload for a profile-guided native build (build-time only).

Run by the build hook against the instrumented extension. A real client
publishes to itself through a minimal in-process loopback broker, one message
at a time and in bursts, so the receive transport, writer and codec run as in
an application. The protocol engine is also driven directly for QoS 2,
properties, and batched and fragmented reads.
"""

from __future__ import annotations

import asyncio
import os
import sys

from mqttium.api import AsyncClient
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


def _vbi(value: int) -> bytes:
    out = bytearray()
    while True:
        value, digit = value >> 7, value & 0x7F
        out.append(digit | (0x80 if value else 0))
        if not value:
            return bytes(out)


async def _loopback_broker(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Acknowledge and deliver back every QoS 0/1 PUBLISH; nothing more."""
    level = 4
    delivery = 0
    try:
        while True:
            first = (await reader.readexactly(1))[0]
            remaining = shift = 0
            while True:
                byte = (await reader.readexactly(1))[0]
                remaining |= (byte & 0x7F) << shift
                shift += 7
                if byte < 0x80:
                    break
            body = await reader.readexactly(remaining)
            kind = first >> 4
            if kind == 1:
                level = body[6]
                writer.write(b"\x20\x03\x00\x00\x00" if level == 5 else b"\x20\x02\x00\x00")
            elif kind == 8:
                start = 2 + (1 + body[2] if level == 5 else 0)
                size = int.from_bytes(body[start : start + 2], "big")
                ack = (
                    body[:2]
                    + (b"\x00" if level == 5 else b"")
                    + bytes([body[start + 2 + size] & 3])
                )
                writer.write(bytes([0x90, len(ack)]) + ack)
            elif kind == 3:
                qos = (first >> 1) & 3
                end = 2 + int.from_bytes(body[:2], "big")
                rest = body[end + 2 :] if qos else body[end:]
                if qos:
                    writer.write(b"\x40\x02" + body[end : end + 2])
                    delivery = delivery % 65535 + 1
                    rest = delivery.to_bytes(2, "big") + rest
                out = body[:end] + rest
                writer.write(bytes([first & 0xF6]) + _vbi(len(out)) + out)
            elif kind == 12:
                writer.write(b"\xd0\x00")
            elif kind == 14:
                break
    except asyncio.IncompleteReadError:
        pass
    finally:
        writer.close()


async def _client(
    port: int, protocol: MQTTProtocolVersion, qos: int, count: int, burst: int
) -> None:
    received = 0
    target = 0
    arrived = asyncio.Event()

    def on_message(message: object) -> None:
        nonlocal received
        received += 1
        if received >= target:
            arrived.set()

    client = AsyncClient("train", protocol=protocol, message_delivery="callback")
    client.on_message = on_message
    await client.connect("127.0.0.1", port, timeout=10)
    await client.subscribe(TOPIC, qos=qos)
    for _ in range(count // burst):
        target += burst
        arrived.clear()
        if qos:
            receipts = [await client.publish(TOPIC, SMALL, qos=qos) for _ in range(burst)]
            for receipt in receipts:
                await receipt.wait()
        else:
            for _ in range(burst):
                client.publish_nowait(TOPIC, SMALL, qos=0)
        if received < target:
            await asyncio.wait_for(arrived.wait(), 10)
    await client.disconnect()


async def _loopback(count: int) -> None:
    server = await asyncio.start_server(_loopback_broker, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
            for qos in (0, 1):
                for burst in (1, 32):
                    await _client(port, protocol, qos, count, burst)


def main(rounds: int = 3000) -> None:
    if os.environ.get("MQTTIUM_CODEC_BACKEND") == "native" and BACKEND != "native":
        raise SystemExit("training requires the native codec")
    asyncio.run(_loopback(rounds))
    engines = {
        protocol: (_connected(protocol), IncrementalDecoder())
        for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5)
    }
    mid = 0
    for index in range(rounds // 3):
        for protocol, (engine, decoder) in engines.items():
            _publish(engine, protocol, decoder)
            mid = _receive(engine, protocol, decoder, mid)
            if index % 10 == 0:
                _control(protocol)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 3000)
