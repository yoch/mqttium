"""Ingress failure semantics: local store failures keep their attribution.

A store/persistence failure inside packet handling must surface with its
original exception object — never converted into a peer-attributed
``PROTOCOL_ERROR`` — while an already-observed terminal broker outcome still
settles its receipt. A failed lot keeps only terminal publish outcomes;
anything else from that lot is never exposed. A local-terminal failure
fail-stops the client: no automatic reconnect or replay, and the instance
refuses silent reuse.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from mqttium import MQTTError
from mqttium.api import AsyncClient, ReconnectPolicy
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
)
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage


STORE_FAILURE = "durable settle failed"


class _FailSettleStore(MemoryInflightStore):
    """Refuse durable settlement while admitting everything else."""

    fail_mids: set[int] | None = None

    def _must_fail(self, mid: int) -> bool:
        return self.fail_mids is None or mid in self.fail_mids

    def complete_out(self, mid: int, expected_state):  # noqa: ANN001, ANN202
        if self._must_fail(mid):
            raise OSError(STORE_FAILURE)
        return super().complete_out(mid, expected_state)


class _FailPutInStore(MemoryInflightStore):
    def put_in(self, msg: InboundMessage) -> None:
        raise OSError(STORE_FAILURE)


class _FailSqliteSettleStore(SqliteInflightStore):
    """Transactional store whose settlement can fail on demand (rollback test)."""

    fail_mids: set[int] | None = None

    def complete_out(self, mid: int, expected_state):  # noqa: ANN001, ANN202
        if self.fail_mids is None or mid in self.fail_mids:
            raise OSError(STORE_FAILURE)
        return super().complete_out(mid, expected_state)


class _ExitFailingStore(SqliteInflightStore):
    """Runs the lot body normally, then fails like a commit error on exit."""

    fail_exit = False

    @contextmanager
    def batch(self) -> Iterator[None]:
        with super().batch():
            yield
            if self.fail_exit:
                raise OSError("batch commit failed")


def _raw(wire: bytes) -> RawPacket:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    packet = decoder.next_packet()
    assert packet is not None
    return packet


def _connected_engine(
    *,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    store=None,
) -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(protocol=protocol), store=store)
    engine.state = ConnectionState.CONNECTED
    return engine


def test_qos2_put_in_failure_raises_without_effects() -> None:
    """A commit-dependent inbound path fails before emitting anything."""
    engine = _connected_engine(store=_FailPutInStore())
    packet = PublishPacket(
        topic="failure/commit-dependent",
        payload=b"body",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=31,
    )
    with pytest.raises(OSError, match=STORE_FAILURE):
        engine.handle_raw(_raw(packet.encode()))
    assert engine.take_effects() == []
    assert engine.inbound.stats().inflight == 0


def test_puback_settle_failure_emits_complete_and_raises() -> None:
    """Observed PUBACK success settles even when durable cleanup fails."""
    store = _FailSettleStore()
    engine = _connected_engine(store=store)
    handle = engine.queue_publish("failure/terminal", b"x", qos=QoS.AT_LEAST_ONCE)
    assert handle.mid is not None
    engine.take_effects()
    store.fail_mids = {handle.mid}

    with pytest.raises(OSError, match=STORE_FAILURE):
        engine.handle_raw(_raw(PubAckPacket(mid=handle.mid).encode()))
    effects = engine.take_effects()
    assert [(effect.kind, effect.data) for effect in effects] == [
        (EffectKind.PUBLISH_COMPLETE, handle.mid)
    ]
    # The failed exchange keeps its ownership: no silent MID/flow reuse.
    assert engine.packet_ids.in_use(handle.mid)
    assert engine.flow.inflight == 1


def test_puback_broker_failure_survives_cleanup_failure() -> None:
    """A broker 0x87 keeps its exact reason when durable cleanup fails."""
    store = _FailSettleStore()
    engine = _connected_engine(protocol=MQTTProtocolVersion.MQTTv5, store=store)
    handle = engine.queue_publish("failure/broker-reason", b"x", qos=QoS.AT_LEAST_ONCE)
    assert handle.mid is not None
    engine.take_effects()
    store.fail_mids = {handle.mid}

    wire = PubAckPacket(mid=handle.mid, reason_code=0x87).encode(MQTTProtocolVersion.MQTTv5)
    with pytest.raises(OSError, match=STORE_FAILURE):
        engine.handle_raw(_raw(wire))
    effects = engine.take_effects()
    assert len(effects) == 1
    assert effects[0].kind is EffectKind.PUBLISH_FAILED
    assert effects[0].data.mid == handle.mid
    assert "135" in str(effects[0].data.reason)
    assert STORE_FAILURE not in str(effects[0].data.reason)


def test_pubcomp_broker_failure_survives_cleanup_failure() -> None:
    """A broker 0x92 PUBCOMP keeps its exact reason when cleanup fails."""
    store = _FailSettleStore()
    engine = _connected_engine(protocol=MQTTProtocolVersion.MQTTv5, store=store)
    handle = engine.queue_publish("failure/comp-reason", b"x", qos=QoS.EXACTLY_ONCE)
    assert handle.mid is not None
    engine.handle_raw(
        _raw(PubRecPacket(mid=handle.mid, reason_code=0).encode(MQTTProtocolVersion.MQTTv5))
    )
    engine.take_effects()
    store.fail_mids = {handle.mid}

    wire = PubCompPacket(mid=handle.mid, reason_code=0x92).encode(MQTTProtocolVersion.MQTTv5)
    with pytest.raises(OSError, match=STORE_FAILURE):
        engine.handle_raw(_raw(wire))
    effects = engine.take_effects()
    assert len(effects) == 1
    assert effects[0].kind is EffectKind.PUBLISH_FAILED
    assert effects[0].data.mid == handle.mid
    assert "146" in str(effects[0].data.reason)


def test_negative_pubrec_preserves_broker_failure_on_cleanup_failure() -> None:
    """A negative MQTT 5 PUBREC settles the broker failure despite cleanup failure."""
    store = _FailSettleStore()
    engine = _connected_engine(protocol=MQTTProtocolVersion.MQTTv5, store=store)
    handle = engine.queue_publish("failure/pubrec-reason", b"x", qos=QoS.EXACTLY_ONCE)
    assert handle.mid is not None
    engine.take_effects()
    store.fail_mids = {handle.mid}

    wire = PubRecPacket(mid=handle.mid, reason_code=0x87).encode(MQTTProtocolVersion.MQTTv5)
    with pytest.raises(OSError, match=STORE_FAILURE):
        engine.handle_raw(_raw(wire))
    effects = engine.take_effects()
    assert len(effects) == 1
    assert effects[0].kind is EffectKind.PUBLISH_FAILED
    assert effects[0].data.mid == handle.mid
    assert "135" in str(effects[0].data.reason)


class _ManualBrokerTransport:
    """Broker double that answers CONNECT but never auto-acknowledges PUBLISH."""

    def __init__(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False
        self.protocol = protocol
        self.decoder = IncrementalDecoder()
        self.written: list[bytes] = []
        self.publishes: list[PublishPacket] = []

    def push_rx(self, data: bytes) -> None:
        self._rx.put_nowait(data)

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def write(self, data: bytes) -> None:
        if isinstance(data, tuple):
            data = data[0] + data[1]
        self.written.append(data)
        self.decoder.feed(data)
        for raw in self.decoder.drain_packets():
            self.handle_packet(raw)

    async def write_many(self, parts: list[bytes]) -> None:
        await self.write(b"".join(parts))

    def handle_packet(self, raw: RawPacket) -> None:
        if raw.packet_type is PacketType.CONNECT:
            body = b"\x00\x00"
            self.push_rx(bytes((0x20, len(body))) + body)
        elif raw.packet_type is PacketType.PUBLISH:
            publish = PublishPacket.decode(raw.flags, raw.remaining, self.protocol)
            self.publishes.append(publish)

    async def close(self) -> None:
        self._closing = True
        self.push_rx(b"")

    def is_closing(self) -> bool:
        return self._closing


def _written_types(transport: _ManualBrokerTransport) -> list[int]:
    types: list[int] = []
    for frame in transport.written:
        types.append(frame[0] >> 4)
    return types


async def _wait_for(predicate, timeout: float = 5.0) -> None:  # noqa: ANN001, ANN202
    async def _ready() -> bool:
        return bool(predicate())

    await asyncio.wait_for(_poll(_ready), timeout=timeout)


async def _poll(ready) -> None:  # noqa: ANN001, ANN202
    while not await ready():
        await asyncio.sleep(0.01)


def _failing_client(
    transport: _ManualBrokerTransport,
    store,
    on_disconnect: list,
    disconnected: asyncio.Event,
) -> AsyncClient:
    policy = ReconnectPolicy(enabled=True, initial_delay=0.05, max_delay=0.05)
    client = AsyncClient(client_id="ingress-failure", store=store, reconnect=policy)
    calls = 0
    inner = transport

    async def factory(host: str, port: int, *, ssl: object = None):  # noqa: ANN202
        nonlocal calls
        del host, port, ssl
        calls += 1
        return inner

    client._transport_factory = factory  # type: ignore[assignment]

    def _record(error: BaseException | None) -> None:
        on_disconnect.append(error)
        disconnected.set()

    client.on_disconnect = _record  # type: ignore[assignment]
    client._factory_calls = lambda: calls  # type: ignore[attr-defined]
    return client


async def test_puback_store_failure_keeps_receipt_and_fail_stops() -> None:
    """PUBACK success + settle failure: receipt success, original error, no
    peer-blaming DISCONNECT, no reconnect, instance refuses silent reuse."""
    store = _FailSettleStore()
    transport = _ManualBrokerTransport()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    client = _failing_client(transport, store, errors, disconnected)

    await client.connect("fake", timeout=2.0)
    receipt = await client.publish("failure/terminal", b"x", qos=1)
    await _wait_for(lambda: len(transport.publishes) == 1)
    mid = transport.publishes[0].mid
    assert mid is not None
    store.fail_mids = {mid}
    transport.push_rx(PubAckPacket(mid=mid).encode())

    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    await asyncio.wait_for(receipt.wait(), timeout=5.0)

    assert len(errors) == 1
    assert isinstance(errors[0], OSError)
    assert STORE_FAILURE in str(errors[0])
    assert 14 not in _written_types(transport)
    await asyncio.sleep(0.4)
    assert client._factory_calls() == 1
    assert client.state is ConnectionState.DISCONNECTED
    with pytest.raises(MQTTError, match="local terminal failure"):
        await client.connect("fake", timeout=2.0)
    await client.disconnect()


async def test_batch_exit_failure_rolls_back_and_fail_stops(
    tmp_path: Path,
) -> None:
    """A commit failure at batch exit latched, filtered, and fail-stopped.

    The lot body runs normally (terminal settle + commit-dependent put committed
    in-transaction); the OSError at exit rolls everything back. The receipt of
    the settled message stays terminal, the other lot effects never surface,
    the original commit error reaches on_disconnect, and nothing reconnects.
    """
    store = _ExitFailingStore(tmp_path / "exit.db")
    transport = _ManualBrokerTransport()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    client = _failing_client(transport, store, errors, disconnected)

    await client.connect("fake", timeout=2.0)
    receipt = await client.publish("failure/exit-a1", b"a1", qos=1)
    await _wait_for(lambda: len(transport.publishes) == 1)
    mid = transport.publishes[0].mid
    assert mid is not None
    store.fail_exit = True
    lot = (
        PubAckPacket(mid=mid).encode()
        + PublishPacket(
            topic="failure/exit-a2",
            payload=b"a2",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=False,
            mid=42,
        ).encode()
    )
    transport.push_rx(lot)

    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    await asyncio.wait_for(receipt.wait(), timeout=5.0)

    assert len(errors) == 1
    assert isinstance(errors[0], OSError)
    assert "commit failed" in str(errors[0])
    # Rolled back: the settled row is back, the QoS 2 row never landed.
    assert store.get_out(mid) is not None
    assert store.get_in(42) is None
    # Filtered: no PUBREC for the rolled-back QoS 2 publish.
    assert 5 not in _written_types(transport)
    assert 14 not in _written_types(transport)
    await asyncio.sleep(0.4)
    assert client._factory_calls() == 1
    with pytest.raises(MQTTError, match="local terminal failure"):
        await client.connect("fake", timeout=2.0)
    await client.disconnect()


async def test_no_admission_after_fail_stop() -> None:
    """Once fail-stopped, the instance admits nothing new.

    The latch and the transport-closed retire happen synchronously under the
    engine lock with no await between them, so no admission can slip into the
    window: QoS 0 publish, subscribe, and manual ack are centrally refused via
    ConnectionState, and no MID, row, or effect is created.
    """
    store = _FailSettleStore()
    transport = _ManualBrokerTransport()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    client = _failing_client(transport, store, errors, disconnected)

    await client.connect("fake", timeout=2.0)
    receipt = await client.publish("failure/admission", b"x", qos=1)
    await _wait_for(lambda: len(transport.publishes) == 1)
    mid = transport.publishes[0].mid
    assert mid is not None
    store.fail_mids = {mid}
    mids_before = len(client._engine.packet_ids)
    rows_before = sorted(m.mid for m in store.out_items())
    transport.push_rx(PubAckPacket(mid=mid).encode())

    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    await asyncio.wait_for(receipt.wait(), timeout=5.0)
    assert client._engine.state is ConnectionState.DISCONNECTED

    from mqttium.errors import NotConnectedError

    with pytest.raises(NotConnectedError):
        client.publish_nowait("failure/admission", b"y", qos=0)
    with pytest.raises(NotConnectedError):
        await client.subscribe("failure/admission")
    with pytest.raises(NotConnectedError):
        client._engine.ack(mid)
    assert len(client._engine.packet_ids) == mids_before
    assert sorted(m.mid for m in store.out_items()) == rows_before
    assert not client._engine.take_effects()
    await client.disconnect()


async def test_failed_lot_keeps_terminal_and_drops_commit_dependent(
    tmp_path: Path,
) -> None:
    """A/B lot: terminal ACK A + cleanup, commit-dependent A2, failing B.

    The outer rollback resurrects A's row (known-stale, never silently
    replayed); A's receipt stays terminal; A2's effects never surface; the
    client fail-stops with the original exception and refuses reuse.
    """
    store = _FailSqliteSettleStore(tmp_path / "ab.db")
    transport = _ManualBrokerTransport()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    client = _failing_client(transport, store, errors, disconnected)

    await client.connect("fake", timeout=2.0)
    first = await client.publish("failure/ab-a1", b"a1", qos=1)
    second = await client.publish("failure/ab-b1", b"b1", qos=1)
    await _wait_for(lambda: len(transport.publishes) == 2)
    mid_a1 = transport.publishes[0].mid
    mid_b1 = transport.publishes[1].mid
    assert mid_a1 is not None and mid_b1 is not None
    store.fail_mids = {mid_b1}

    inbound_qos2 = PublishPacket(
        topic="failure/ab-a2",
        payload=b"a2",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=41,
    ).encode()
    lot = PubAckPacket(mid=mid_a1).encode() + inbound_qos2 + PubAckPacket(mid=mid_b1).encode()
    transport.push_rx(lot)

    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    await asyncio.wait_for(first.wait(), timeout=5.0)
    # B1's PUBACK was genuinely observed as success: rule 2 keeps the broker
    # outcome even though its durable cleanup failed. The local failure
    # surfaces only through on_disconnect and fail-stop, never by replacing
    # either receipt.
    await asyncio.wait_for(second.wait(), timeout=5.0)

    assert len(errors) == 1
    assert isinstance(errors[0], OSError)
    assert STORE_FAILURE in str(errors[0])
    # Terminal receipt of A1 preserved despite the resurrected row.
    assert store.get_out(mid_a1) is not None
    # A2's commit-dependent effects never surfaced: no PUBREC, no delivery.
    assert 5 not in _written_types(transport)
    assert store.get_in(41) is None
    # Known-stale configuration, exactly: A1 completed its normal path
    # (settle, emit, MID release) before B failed, so its MID is free while
    # its row is back; B1 never got past settle, so its MID stays held.
    # Neither may ever be silently reused: fail-stop owns both.
    assert not client._engine.packet_ids.in_use(mid_a1)
    assert client._engine.packet_ids.in_use(mid_b1)
    assert 14 not in _written_types(transport)
    await asyncio.sleep(0.4)
    assert client._factory_calls() == 1
    with pytest.raises(MQTTError, match="local terminal failure"):
        await client.connect("fake", timeout=2.0)
    await client.disconnect()
