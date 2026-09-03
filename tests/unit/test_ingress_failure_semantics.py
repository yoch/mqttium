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
from mqttium.api import AsyncClient, PublishMessage, ReconnectPolicy
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import (
    ConnectionState,
    MQTTProtocolVersion,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    encode_frame,
)
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage, InboundQoSState, OutboundMessage


STORE_FAILURE = "durable settle failed"


class _FailSettleStore(MemoryInflightStore):
    """Refuse durable settlement while admitting everything else."""

    fail_mids: set[int] | None = None
    error: BaseException | None = None

    def _must_fail(self, mid: int) -> bool:
        return self.fail_mids is None or mid in self.fail_mids

    def complete_out(self, mid: int, expected_state):  # noqa: ANN001, ANN202
        if self._must_fail(mid):
            if self.error is not None:
                raise self.error
            raise OSError(STORE_FAILURE)
        return super().complete_out(mid, expected_state)


class _FailMarkStore(MemoryInflightStore):
    """Accept delivery rows but refuse the delivered mark."""

    fail_mark = False
    mark_error: BaseException | None = None

    def mark_in_delivered(self, mid: int) -> bool:
        if self.fail_mark:
            if self.mark_error is not None:
                raise self.mark_error
            raise OSError("delivery mark failed")
        return super().mark_in_delivered(mid)


class _FailReplayPagesStore(MemoryInflightStore):
    """Serve the first replay page, then fail like a cursor read error."""

    fail_pages = False
    page_error: BaseException | None = None

    def in_replay_pages(
        self,
        max_messages: int = 64,
        max_bytes: int = 1 << 20,
    ):
        pages = super().in_replay_pages(max_messages=max_messages, max_bytes=max_bytes)
        first = True
        for page in pages:
            if not first:
                if self.page_error is not None:
                    raise self.page_error
                raise OSError("replay page failed")
            first = False
            yield page


class _FailReplayStore(MemoryInflightStore):
    """Refuse the outbound replay index read used at session restore."""

    fail_replay = False
    restore_error: BaseException | None = None

    def out_summary_pages(self, page_size: int = 256):
        if self.fail_replay:
            if self.restore_error is not None:
                raise self.restore_error
            raise OSError("restore failed")
        yield from super().out_summary_pages(page_size)


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


class _BatchExitFailingStore(SqliteInflightStore):
    """Runs the lot body normally, then raises a batch-exit failure.

    The exception traverses the SQLite batch, which rolls the lot back —
    like a commit error at exit, without mocking sqlite internals.
    """

    fail_exit = False

    @contextmanager
    def batch(self) -> Iterator[None]:
        with super().batch():
            yield
            if self.fail_exit:
                raise OSError("batch exit failed")


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
    sentinel = OSError(STORE_FAILURE)
    store.error = sentinel
    store.fail_mids = {handle.mid}

    with pytest.raises(OSError) as raised:
        engine.handle_raw(_raw(PubAckPacket(mid=handle.mid).encode()))
    assert raised.value is sentinel
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
        self.connack_override: bytes | None = None
        # Optional write gating for fail-stop race tests. When a frame's type
        # nibble is in block_kinds, write() signals entered_write and waits
        # for release_write; with fail_writes set it raises write_error
        # instead (simulating a write failing while teardown is closing).
        self.block_kinds: set[int] = set()
        self.entered_write = asyncio.Event()
        self.release_write = asyncio.Event()
        self.close_entered: asyncio.Event | None = None
        self.allow_close: asyncio.Event | None = None

    def push_rx(self, data: bytes) -> None:
        self._rx.put_nowait(data)

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def write(self, data: bytes) -> None:
        if isinstance(data, tuple):
            data = data[0] + data[1]
        if (data[0] >> 4) in self.block_kinds:
            self.entered_write.set()
            await self.release_write.wait()
        self.written.append(data)
        self.decoder.feed(data)
        for raw in self.decoder.drain_packets():
            self.handle_packet(raw)

    async def write_many(self, parts: list[bytes]) -> None:
        await self.write(b"".join(parts))

    def handle_packet(self, raw: RawPacket) -> None:
        if raw.packet_type is PacketType.CONNECT:
            if self.connack_override is not None:
                self.push_rx(self.connack_override)
                return
            body = b"\x00\x00"
            self.push_rx(bytes((0x20, len(body))) + body)
        elif raw.packet_type is PacketType.PUBLISH:
            publish = PublishPacket.decode(raw.flags, raw.remaining, self.protocol)
            self.publishes.append(publish)

    async def close(self) -> None:
        self._closing = True
        if self.close_entered is not None:
            self.close_entered.set()
        if self.allow_close is not None:
            await self.allow_close.wait()
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
    *,
    clean_start: bool = True,
) -> AsyncClient:
    policy = ReconnectPolicy(enabled=True, initial_delay=0.05, max_delay=0.05)
    client = AsyncClient(
        client_id="ingress-failure",
        store=store,
        reconnect=policy,
        clean_start=clean_start,
    )
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
    sentinel = OSError(STORE_FAILURE)
    store.error = sentinel
    store.fail_mids = {mid}
    transport.push_rx(PubAckPacket(mid=mid).encode())

    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    await asyncio.wait_for(receipt.wait(), timeout=5.0)

    assert len(errors) == 1
    assert errors[0] is sentinel
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
    store = _BatchExitFailingStore(tmp_path / "exit.db")
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
    assert "batch exit failed" in str(errors[0])
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


class _DivergentInboundStore(MemoryInflightStore):
    """Reports a WAIT_PUBREL record, then denies its conditional delete."""

    diverge = False

    def complete_in(self, mid: int, expected_state: InboundQoSState) -> object:
        if self.diverge:
            return None
        return super().complete_in(mid, expected_state)


def _qos2_exchange(engine: ProtocolEngine, mid: int = 31) -> None:
    engine.handle_raw(
        _raw(
            PublishPacket(
                topic="failure/divergent",
                payload=b"body",
                qos=QoS.EXACTLY_ONCE,
                retain=False,
                dup=False,
                mid=mid,
            ).encode()
        )
    )
    engine.take_effects()


def test_pubrel_store_divergence_traverses_unchanged() -> None:
    """A local store divergence is not a peer protocol error.

    The store reports a WAIT_PUBREL record, then its conditional delete
    observes a move: RuntimeError with identity, nothing emitted, no PUBCOMP.
    """
    store = _DivergentInboundStore()
    engine = _connected_engine(store=store)
    _qos2_exchange(engine)
    assert store.get_in(31) is not None
    store.diverge = True

    failure = None
    try:
        engine.handle_raw(_raw(PubRelPacket(mid=31).encode()))
    except RuntimeError as exc:
        failure = exc
    assert failure is not None
    assert "changed while completing PUBREL" in str(failure)
    assert engine.take_effects() == []


async def test_pubrel_store_divergence_fail_stops_client() -> None:
    """Client-level traversal: identity, no peer DISCONNECT, no reconnect."""
    store = _DivergentInboundStore()
    transport = _ManualBrokerTransport()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    client = _failing_client(transport, store, errors, disconnected)

    await client.connect("fake", timeout=2.0)
    transport.push_rx(
        PublishPacket(
            topic="failure/divergent",
            payload=b"body",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=False,
            mid=31,
        ).encode()
    )
    # Wait for the handshake PUBREC on the wire instead of draining the
    # engine effect stream owned by the live client and its EffectPump.
    await _wait_for(lambda: any((frame[0] >> 4) == 5 for frame in transport.written))
    assert store.get_in(31) is not None
    store.diverge = True
    transport.push_rx(PubRelPacket(mid=31).encode())

    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "changed while completing PUBREL" in str(errors[0])
    # The QoS 2 handshake PUBREC went out normally; no PUBCOMP for the
    # unfinished exchange and no peer-blaming DISCONNECT followed it.
    assert _written_types(transport) == [1, 5]
    assert 7 not in _written_types(transport)
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
    reader = client._reader_task
    assert reader is not None
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
    # QoS 1/2 offline queueing is a feature while merely disconnected, so the
    # fail-stop latch — not ConnectionState — must refuse it here.
    from mqttium.api import PublishMessage

    with pytest.raises(MQTTError, match="local terminal failure"):
        await client.publish("failure/admission", b"z", qos=1)
    with pytest.raises(MQTTError, match="local terminal failure"):
        client.publish_nowait("failure/admission", b"z", qos=1)
    with pytest.raises(MQTTError, match="local terminal failure"):
        await client.publish_many([PublishMessage("failure/admission", b"z", qos=1)])
    with pytest.raises(MQTTError, match="local terminal failure"):
        client._queue_qosn_on_loop("failure/admission", b"z", qos=QoS.AT_LEAST_ONCE, retain=False)
    assert len(client._engine.packet_ids) == mids_before
    assert sorted(m.mid for m in store.out_items()) == rows_before
    # Only assert on the engine effect stream once its owning reader task is
    # fully terminated; anything earlier races teardown.
    await _wait_for(lambda: reader.done())
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


def _parked_setup(store, max_pending: int = 1):  # noqa: ANN001, ANN202
    """Connected client with one held QoS 1 publish filling pending capacity."""
    transport = _ManualBrokerTransport()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    policy = ReconnectPolicy(enabled=True, initial_delay=0.05, max_delay=0.05)
    client = AsyncClient(
        client_id="parked-crossing",
        store=store,
        reconnect=policy,
        max_pending_outbound_messages=max_pending,
    )
    calls = 0

    async def factory(host: str, port: int, *, ssl: object = None):  # noqa: ANN202
        nonlocal calls
        del host, port, ssl
        calls += 1
        return transport

    client._transport_factory = factory  # type: ignore[assignment]

    def _record(error: BaseException | None) -> None:
        errors.append(error)
        disconnected.set()

    client.on_disconnect = _record  # type: ignore[assignment]
    return client, transport, errors, disconnected, lambda: calls


async def test_parked_publish_fails_after_fail_stop() -> None:
    """A publish parked on capacity that wakes after fail-stop is refused.

    B passes admission while healthy and parks; the lot settles A (freeing
    capacity) then fails on the QoS 2 put_in; teardown wakes B, whose retry
    rechecks under the lock and fails without admitting anything.
    """
    store = _FailPutInStore()
    client, transport, errors, disconnected, calls = _parked_setup(store)

    await client.connect("fake", timeout=2.0)
    first = await client.publish("failure/parked-a", b"a", qos=1)
    await _wait_for(lambda: len(transport.publishes) == 1)
    parked = asyncio.create_task(client.publish("failure/parked-b", b"b", qos=1))
    await _wait_for(lambda: client._publish_waiters >= 1)
    lot = (
        PubAckPacket(mid=transport.publishes[0].mid).encode()
        + PublishPacket(
            topic="failure/parked-a2",
            payload=b"a2",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=False,
            mid=41,
        ).encode()
    )
    transport.push_rx(lot)

    with pytest.raises(MQTTError, match="local terminal failure"):
        await asyncio.wait_for(parked, timeout=5.0)
    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    await asyncio.wait_for(first.wait(), timeout=5.0)

    assert len(errors) == 1
    assert isinstance(errors[0], OSError)
    assert list(store.out_items()) == []
    assert len(client._engine.packet_ids) == 0
    await asyncio.sleep(0.4)
    assert calls() == 1
    await client.disconnect()


async def test_parked_publish_many_fails_after_fail_stop() -> None:
    """Same crossing for a batch parked mid-submission: PublishBatchError."""
    from mqttium.errors import PublishBatchError

    store = _FailPutInStore()
    client, transport, errors, disconnected, calls = _parked_setup(store, max_pending=2)

    await client.connect("fake", timeout=2.0)
    first = await client.publish("failure/many-a", b"a", qos=1)
    await _wait_for(lambda: len(transport.publishes) == 1)
    batch = asyncio.create_task(
        client.publish_many(
            [
                PublishMessage("failure/many-b1", b"b1", qos=1),
                PublishMessage("failure/many-b2", b"b2", qos=1),
            ]
        )
    )
    await _wait_for(lambda: client._publish_waiters >= 1)
    lot = (
        PubAckPacket(mid=transport.publishes[0].mid).encode()
        + PublishPacket(
            topic="failure/many-a2",
            payload=b"a2",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=False,
            mid=41,
        ).encode()
    )
    transport.push_rx(lot)

    with pytest.raises(PublishBatchError) as failed:
        await asyncio.wait_for(batch, timeout=5.0)
    assert isinstance(failed.value.__cause__, MQTTError)
    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    await asyncio.wait_for(first.wait(), timeout=5.0)
    assert list(store.out_items()) == []
    await asyncio.sleep(0.4)
    assert calls() == 1
    await client.disconnect()


async def test_connack_restore_failure_fails_fast_with_original_cause() -> None:
    """Store failure during session restore fails connect() with the cause.

    A second concurrent connect task that passed its entry guard while healthy
    is refused by the recheck after the lock; no transport is opened for it,
    on_connect never succeeds, and no CONNACK is exposed.
    """
    store = _FailReplayStore()
    sentinel = OSError("restore failed")
    store.restore_error = sentinel
    record = OutboundMessage(
        mid=9,
        topic="held/restore",
        payload=b"x",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        state=OutboundQoSState.WAIT_PUBACK,
    )
    store.put_out(record)
    transport = _ManualBrokerTransport()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    policy = ReconnectPolicy(enabled=True, initial_delay=0.05, max_delay=0.05)
    client = AsyncClient(
        client_id="connack-restore", store=store, reconnect=policy, clean_start=False
    )
    calls = 0
    connected_calls: list[object] = []

    async def factory(host: str, port: int, *, ssl: object = None):  # noqa: ANN202
        nonlocal calls
        del host, port, ssl
        calls += 1
        return transport

    client._transport_factory = factory  # type: ignore[assignment]

    def _record(error: BaseException | None) -> None:
        errors.append(error)
        disconnected.set()

    client.on_disconnect = _record  # type: ignore[assignment]
    client.on_connect = connected_calls.append  # type: ignore[assignment]
    transport.connack_override = encode_frame(PacketType.CONNACK, 0, b"\x01\x00")
    # Armed after construction (hydrate already ran) and before any CONNACK:
    # nothing reads the replay index in between.
    store.fail_replay = True

    first = asyncio.create_task(client.connect("fake", timeout=5.0))
    await _wait_for(lambda: len(transport.written) == 1)
    second = asyncio.create_task(client.connect("fake", timeout=5.0))
    await asyncio.sleep(0.05)

    with pytest.raises(OSError) as failed:
        await asyncio.wait_for(first, timeout=5.0)
    assert failed.value is sentinel
    with pytest.raises(MQTTError, match="local terminal failure"):
        await asyncio.wait_for(second, timeout=5.0)
    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    assert connected_calls == []
    assert len(errors) == 1
    assert errors[0] is sentinel
    assert calls == 1
    with pytest.raises(MQTTError, match="local terminal failure"):
        await client.connect("fake", timeout=2.0)
    await client.disconnect()


async def test_reconnect_in_progress_stops_on_restore_failure() -> None:
    """No second reconnect attempt after a restore failure during the first."""
    store = _FailReplayStore()
    sentinel = OSError("restore failed")
    store.restore_error = sentinel
    first_transport = _ManualBrokerTransport()
    second_transport = _ManualBrokerTransport()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    policy = ReconnectPolicy(enabled=True, initial_delay=0.05, max_delay=0.05)
    client = AsyncClient(
        client_id="reconnect-stop", store=store, reconnect=policy, clean_start=False
    )
    calls = 0

    async def factory(host: str, port: int, *, ssl: object = None):  # noqa: ANN202
        nonlocal calls
        del host, port, ssl
        calls += 1
        return first_transport if calls == 1 else second_transport

    client._transport_factory = factory  # type: ignore[assignment]

    def _record(error: BaseException | None) -> None:
        errors.append(error)
        if len(errors) == 2:
            disconnected.set()

    client.on_disconnect = _record  # type: ignore[assignment]

    await client.connect("fake", timeout=2.0)
    await client.publish("failure/reconnect-held", b"x", qos=1)
    await _wait_for(lambda: len(first_transport.publishes) == 1)
    # Arm before the loss: nothing reads the replay index between here and the
    # second CONNACK, while the auto-answer below would race a later arming.
    store.fail_replay = True
    second_transport.connack_override = encode_frame(PacketType.CONNACK, 0, b"\x01\x00")
    await first_transport.close()
    await _wait_for(lambda: calls == 2)
    await _wait_for(
        lambda: any(
            (frame[0] & 0xF0) == int(PacketType.CONNECT) for frame in second_transport.written
        )
    )

    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    assert errors[-1] is sentinel
    await asyncio.sleep(0.4)
    assert calls == 2
    with pytest.raises(MQTTError, match="local terminal failure"):
        await client.connect("fake", timeout=2.0)
    await client.disconnect()


async def test_delivery_mark_failure_latches_without_disturbing_delivery() -> None:
    """A delivery-mark store failure fail-stops but keeps the delivered message."""
    store = _FailMarkStore()
    sentinel = OSError("delivery mark failed")
    store.mark_error = sentinel
    transport = _ManualBrokerTransport()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    received: list[bytes] = []
    client = _failing_client(transport, store, errors, disconnected)
    client.on_message = lambda message: received.append(message.payload)

    await client.connect("fake", timeout=2.0)
    store.fail_mark = True
    transport.push_rx(
        PublishPacket(
            topic="failure/mark",
            payload=b"delivered",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=False,
            mid=31,
        ).encode()
    )

    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    await _wait_for(lambda: len(received) == 1)
    assert received == [b"delivered"]
    assert len(errors) == 1
    assert errors[0] is sentinel
    await asyncio.sleep(0.4)
    assert client._factory_calls() == 1
    with pytest.raises(MQTTError, match="local terminal failure"):
        await client.connect("fake", timeout=2.0)
    await client.disconnect()


async def test_replay_continuation_failure_latches() -> None:
    """A redelivery-cursor store failure fail-stops after the first batch."""
    store = _FailReplayPagesStore()
    sentinel = OSError("replay page failed")
    store.page_error = sentinel
    for mid in range(1, 71):
        store.put_in(
            InboundMessage(
                mid=mid,
                topic=f"failure/replay-{mid}",
                payload=b"x",
                qos=QoS.EXACTLY_ONCE,
                retain=False,
                state=InboundQoSState.WAIT_PUBREL,
            )
        )
    transport = _ManualBrokerTransport()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    received: list[bytes] = []
    client = _failing_client(transport, store, errors, disconnected, clean_start=False)
    client.on_message = lambda message: received.append(message.payload)
    transport.connack_override = encode_frame(PacketType.CONNACK, 0, b"\x01\x00")

    await client.connect("fake", timeout=2.0)

    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    assert len(received) == 64
    assert len(errors) == 1
    assert errors[0] is sentinel
    await asyncio.sleep(0.4)
    assert client._factory_calls() == 1
    with pytest.raises(MQTTError, match="local terminal failure"):
        await client.connect("fake", timeout=2.0)
    await client.disconnect()


async def test_secondary_writer_failure_preserves_terminal_cause() -> None:
    """A writer failure during close must not replace the latched cause.

    An outbound PUBLISH write stays genuinely in flight (blocked) while an
    inbound delivery-mark failure latches and starts the close. A writer
    failure landing as ``_disconnect_exc`` at that point — ``_writer_failed``
    assigns it unconditionally, verified by code reading — must not leak
    into settlement or callbacks: the teardown uses the latched cause.
    """
    store = _FailMarkStore()
    sentinel = OSError("delivery mark failed")
    store.mark_error = sentinel
    store.fail_mark = True
    writer_failure = OSError("writer write failed")
    transport = _ManualBrokerTransport()
    transport.block_kinds = {3}
    transport.close_entered = asyncio.Event()
    transport.allow_close = asyncio.Event()
    disconnected = asyncio.Event()
    errors: list[BaseException | None] = []
    received: list[bytes] = []
    client = _failing_client(transport, store, errors, disconnected)
    client.on_message = lambda message: received.append(message.payload)

    await client.connect("fake", timeout=2.0)
    outbound = await client.publish("failure/writer-race", b"out", qos=1)
    await asyncio.wait_for(transport.entered_write.wait(), timeout=5.0)
    transport.push_rx(
        PublishPacket(
            topic="failure/writer-mark",
            payload=b"in",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=False,
            mid=31,
        ).encode()
    )
    await asyncio.wait_for(transport.close_entered.wait(), timeout=5.0)
    assert client._local_terminal_failure is sentinel
    # Simulate the writer failure landing first: _writer_failed assigns
    # _disconnect_exc unconditionally.
    client._disconnect_exc = writer_failure
    transport.release_write.set()
    transport.allow_close.set()

    await asyncio.wait_for(disconnected.wait(), timeout=5.0)
    assert received == [b"in"]
    assert len(errors) == 1
    assert errors[0] is sentinel
    with pytest.raises(OSError) as failed:
        await asyncio.wait_for(outbound.wait(), timeout=5.0)
    assert failed.value is sentinel
    await asyncio.sleep(0.4)
    assert client._factory_calls() == 1
    with pytest.raises(MQTTError, match="local terminal failure"):
        await client.connect("fake", timeout=2.0)
    await client.disconnect()
