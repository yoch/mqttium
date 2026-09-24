"""A resumed session whose mandatory replay the new CONNACK forbids (#539).

With Session Present, every unacknowledged QoS 1/2 PUBLISH must be resent with
its original packet identifier [MQTT-4.4.0-1]. When the replacement CONNACK
narrows Maximum QoS, Retain Available or Maximum Packet Size so that this
packet is forbidden [MQTT-3.2.2-11], the session cannot be resumed: the
exchange and its packet id must survive, and the connection must end rather
than continue a session it cannot honour.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.errors import ProtocolError, SessionReplayError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import OutboundMessage, Properties
from tests.support import QueueTransport, feed_engine, wait_until

V5 = MQTTProtocolVersion.MQTTv5

# (publication, narrowed CONNACK properties, expected live state)
CONFLICTS = {
    "qos2-maximum-qos": (
        {"qos": QoS.EXACTLY_ONCE, "retain": False, "payload": b"old"},
        {"maximum_qos": 1},
        OutboundQoSState.WAIT_PUBREC,
    ),
    "qos1-retain-available": (
        {"qos": QoS.AT_LEAST_ONCE, "retain": True, "payload": b"old"},
        {"retain_available": 0},
        OutboundQoSState.WAIT_PUBACK,
    ),
    "qos1-maximum-packet-size": (
        {"qos": QoS.AT_LEAST_ONCE, "retain": False, "payload": b"x" * 200},
        {"maximum_packet_size": 64},
        OutboundQoSState.WAIT_PUBACK,
    ),
}


def _connack(session_present: bool, properties: dict[str, int]) -> bytes:
    body = bytes((1 if session_present else 0, 0))
    body += encode_properties(Properties(properties) if properties else None, "CONNACK")
    return encode_frame(PacketType.CONNACK, 0, body)


class _SilentBroker(QueueTransport):
    """Never acknowledges a publication; answers CONNECT with a fixed CONNACK."""

    def __init__(self, connack: bytes) -> None:
        super().__init__()
        self.connack = connack
        self.decoder = IncrementalDecoder()
        self.publishes: list[PublishPacket] = []

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        self.decoder.feed(data if isinstance(data, bytes) else data[0] + data[1])
        for raw in self.decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self.push_rx(self.connack)
            elif raw.packet_type is PacketType.PUBLISH:
                self.publishes.append(PublishPacket.decode(raw.flags, raw.remaining, V5))

    async def write_many(self, parts: list[bytes]) -> None:
        await self.write(b"".join(parts))


def _store(kind: str, path: Path) -> MemoryInflightStore | SqliteInflightStore:
    return MemoryInflightStore() if kind == "memory" else SqliteInflightStore(str(path))


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
@pytest.mark.parametrize("conflict", sorted(CONFLICTS))
async def test_forbidden_replay_ends_the_resumed_session_and_keeps_the_exchange(
    kind: str, conflict: str, tmp_path: Path
) -> None:
    publication, narrowed, live_state = CONFLICTS[conflict]
    path = tmp_path / "session.db"
    store = _store(kind, path)
    brokers: list[_SilentBroker] = []
    disconnects: list[BaseException | None] = []
    client = AsyncClient(
        "resume-conflict",
        protocol=V5,
        clean_start=False,
        connect_properties=Properties({"session_expiry_interval": 600}),
        store=store,
        keepalive=0,
        reconnect=ReconnectPolicy(initial_delay=0.01, max_delay=0.01, stable_after=0.01),
    )
    client.on_disconnect = disconnects.append

    async def factory(host: str, port: int, *, ssl: object = None) -> _SilentBroker:
        del host, port, ssl
        broker = _SilentBroker(_connack(bool(brokers), narrowed if brokers else {}))
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    await client.connect("fake")
    receipt = await client.publish("old/topic", **publication)
    mid = receipt.mid
    assert mid is not None
    await wait_until(lambda: len(brokers[0].publishes) == 1)

    await brokers[0].close()  # Lost before the broker acknowledged it.
    await wait_until(lambda: disconnects[1:] != [])

    assert isinstance(disconnects[-1], SessionReplayError)
    assert len(brokers) == 2
    assert brokers[1].publishes == []
    assert not client.is_connected
    # The exchange and its packet id still belong to the broker session.
    record = store.get_out(mid)
    assert record is not None
    assert record.state is live_state
    assert client._engine.packet_ids.in_use(mid)
    # A local terminal failure: the policy does not retry it.
    await asyncio.sleep(0.1)
    assert len(brokers) == 2
    assert isinstance(receipt._error, SessionReplayError)

    await client.disconnect()
    if isinstance(store, SqliteInflightStore):
        store.close()
        reopened = SqliteInflightStore(str(path))
        survived = reopened.get_out(mid)
        assert survived is not None and survived.state is live_state
        reopened.close()


def _resumable_engine(record: OutboundMessage) -> ProtocolEngine:
    store = MemoryInflightStore()
    store.put_out(record)
    return ProtocolEngine(
        EngineConfig(client_id="c", protocol=V5, clean_start=False),
        store=store,
    )


def _record(mid: int, state: OutboundQoSState, qos: QoS) -> OutboundMessage:
    return OutboundMessage(
        mid=mid,
        topic="old",
        payload=b"x",
        qos=qos,
        retain=False,
        state=state,
        logical_size=4,
    )


@pytest.mark.parametrize(
    ("state", "qos"),
    [
        (OutboundQoSState.WAIT_PUBREC, QoS.EXACTLY_ONCE),
        (OutboundQoSState.WAIT_PUBACK, QoS.AT_LEAST_ONCE),
    ],
)
def test_engine_refuses_the_session_before_any_state_change(
    state: OutboundQoSState, qos: QoS
) -> None:
    engine = _resumable_engine(_record(1, state, qos))
    engine.begin_connect()
    engine.take_effects()

    maximum_qos = 0 if qos is QoS.AT_LEAST_ONCE else 1
    with pytest.raises(SessionReplayError, match="mid 1") as raised:
        feed_engine(engine, _connack(True, {"maximum_qos": maximum_qos}))

    assert isinstance(raised.value.__cause__, ProtocolError)
    assert engine.state is ConnectionState.DISCONNECTED
    assert engine.take_effects() == []
    stored = engine.store.get_out(1)
    assert stored is not None and stored.state is state
    assert engine.packet_ids.in_use(1)


def test_queued_work_behind_a_replayable_session_still_fails_alone() -> None:
    # Never-sent QUEUED work is not part of the broker session: a narrowed
    # CONNACK fails it individually and the resumed session continues.
    engine = _resumable_engine(_record(1, OutboundQoSState.WAIT_PUBACK, QoS.AT_LEAST_ONCE))
    queued = engine.queue_publish("later", b"y", qos=QoS.EXACTLY_ONCE)
    assert queued.mid is not None
    engine.begin_connect()
    engine.take_effects()

    feed_engine(engine, _connack(True, {"maximum_qos": 1}))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert any(e.kind is EffectKind.PUBLISH_FAILED and e.data.mid == queued.mid for e in effects)
    assert engine.store.get_out(queued.mid) is None
    assert any(e.kind is EffectKind.SEND for e in effects)  # mid 1 resent
    assert engine.store.get_out(1) is not None
