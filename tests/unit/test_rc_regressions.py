"""Regressions for defects found while hardening the 1.0.0rc1 candidate.

The harnesses that found them live in ``tests/fuzz/test_stateful_invariants.py``
(engine invariants, store differential) and ``tests/unit/test_hostile_broker.py``
(liveness under a misbehaving peer).
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import (
    ConnectionState,
    InboundQoSState,
    MQTTProtocolVersion,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.protocol.packet_ids import PacketIdPool
from mqttium.types import InboundMessage, OutboundMessage


# --------------------------------------------------------------------------
# 1. PacketIdPool: a duplicate reserve() exactly at the allocation frontier
#    used to leave a phantom entry in _reserved that was counted forever.
# --------------------------------------------------------------------------


def _pool_at_frontier() -> PacketIdPool:
    """A pool whose frontier has just been walked up to a reserved identifier."""
    pool = PacketIdPool()
    for _ in range(4):
        pool.allocate()  # 1..4, frontier is now 5
    pool.reserve(7)  # above the frontier -> parked in _reserved
    assert pool.allocate() == 5
    assert pool.allocate() == 6  # frontier is now exactly 7
    return pool


def test_duplicate_reserve_at_the_frontier_is_idempotent() -> None:
    """``reserve()`` is idempotent — the contract already asserted for ids above
    the frontier must also hold for an id the frontier has just reached."""
    pool = _pool_at_frontier()

    pool.reserve(7)  # duplicate reserve, this time *at* the frontier

    live = {1, 2, 3, 4, 5, 6, 7}
    assert pool._used == live
    assert len(pool) == len(live)
    assert pool.available == PacketIdPool._MAX_ID - len(live)


def test_pool_returns_to_empty_after_duplicate_frontier_reserve() -> None:
    """The phantom entry was permanent: the pool never reported itself empty
    again, which also blocked the release() reset fast path."""
    pool = _pool_at_frontier()
    pool.reserve(7)

    for mid in range(1, 8):
        pool.release(mid)

    assert len(pool) == 0
    assert pool._used == set()
    assert not pool.in_use(7)


def test_frontier_reserve_still_allocates_every_identifier_once() -> None:
    """Discarding the redundant reservation must not hand the id out twice."""
    pool = _pool_at_frontier()
    pool.reserve(7)

    handed_out = {pool.allocate() for _ in range(5)}
    assert handed_out.isdisjoint({1, 2, 3, 4, 5, 6, 7})
    assert len(handed_out) == 5


# --------------------------------------------------------------------------
# 2. InboundSession: the inbound store is keyed by mid alone. A PUBLISH whose
#    identifier is still owned by an unfinished exchange of the *other* QoS
#    used to overwrite the record, stranding its Receive Maximum slot and byte
#    reservation. It is now refused with 0x82 before anything is stored.
# --------------------------------------------------------------------------


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connect(engine: ProtocolEngine) -> None:
    engine.begin_connect()
    remaining = b"\x00\x00"
    if engine.config.protocol is MQTTProtocolVersion.MQTTv5:
        remaining += b"\x00"
    _feed(engine, encode_frame(PacketType.CONNACK, 0, remaining))
    engine.take_effects()


def _inbound_publish(engine: ProtocolEngine, mid: int, qos: QoS, payload: bytes) -> None:
    _feed(
        engine,
        PublishPacket(
            topic="collide/topic",
            payload=payload,
            qos=qos,
            retain=False,
            dup=False,
            mid=mid,
        ).encode(engine.config.protocol),
    )


def _manual_ack_engine(local_receive_maximum: int = 8) -> ProtocolEngine:
    config = EngineConfig(
        client_id="rc",
        protocol=MQTTProtocolVersion.MQTTv5,
        manual_ack=True,
        local_receive_maximum=local_receive_maximum,
    )
    engine = ProtocolEngine(config, MemoryInflightStore())
    _connect(engine)
    return engine


def _assert_accounting_exact(engine: ProtocolEngine) -> None:
    inbound = engine.inbound
    records = list(engine.store.in_items())
    assert inbound._pending_bytes == sum(inbound.stored_logical_size(r) for r in records)
    assert inbound._inflight == len(records)


@pytest.mark.parametrize(
    ("held_qos", "arriving_qos"),
    [
        (QoS.EXACTLY_ONCE, QoS.AT_LEAST_ONCE),
        (QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE),
    ],
    ids=["qos1-over-qos2", "qos2-over-qos1"],
)
def test_inbound_packet_id_collision_is_refused_without_touching_the_record(
    held_qos: QoS, arriving_qos: QoS
) -> None:
    engine = _manual_ack_engine()

    _inbound_publish(engine, 5, held_qos, b"A" * 100)
    engine.take_effects()
    before = {r.mid: (r.qos, r.state, r.payload) for r in engine.store.in_items()}
    assert before, "the first PUBLISH must have been stored"
    _assert_accounting_exact(engine)

    _inbound_publish(engine, 5, arriving_qos, b"B" * 10)
    effects = engine.take_effects()

    after = {r.mid: (r.qos, r.state, r.payload) for r in engine.store.in_items()}
    assert after == before, "the live record must survive the colliding PUBLISH"
    _assert_accounting_exact(engine)

    disconnects = [e.data for e in effects if e.kind is EffectKind.DISCONNECTED]
    assert disconnects, "a packet id collision must not be accepted silently"
    assert disconnects[0].reason_code == 0x82
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    assert engine.state is ConnectionState.DISCONNECTED


def test_inbound_collision_never_reports_a_full_window_while_holding_nothing() -> None:
    """The leak used to be cumulative: with Receive Maximum 4 the client tore a
    healthy connection down reporting 0x93 while the store was empty."""
    engine = _manual_ack_engine(local_receive_maximum=4)

    _inbound_publish(engine, 9, QoS.EXACTLY_ONCE, b"A" * 10)
    _inbound_publish(engine, 9, QoS.AT_LEAST_ONCE, b"B" * 2)
    codes = [e.data.reason_code for e in engine.take_effects() if e.kind is EffectKind.DISCONNECTED]

    assert 0x93 not in codes, "Receive Maximum must not be reported as exhausted here"
    assert codes == [0x82]
    _assert_accounting_exact(engine)


def test_inbound_qos2_duplicate_is_still_answered_with_pubrec() -> None:
    """The collision check must not break genuine QoS 2 duplicate handling."""
    engine = _manual_ack_engine()

    _inbound_publish(engine, 6, QoS.EXACTLY_ONCE, b"payload")
    engine.take_effects()
    _inbound_publish(engine, 6, QoS.EXACTLY_ONCE, b"payload")
    effects = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert not [e for e in effects if e.kind is EffectKind.DISCONNECTED]
    assert [e for e in effects if e.kind is EffectKind.SEND], "duplicate must re-send PUBREC"
    _assert_accounting_exact(engine)


def test_inbound_qos1_duplicate_is_still_redelivered_under_manual_ack() -> None:
    engine = _manual_ack_engine()

    _inbound_publish(engine, 4, QoS.AT_LEAST_ONCE, b"payload")
    engine.take_effects()
    _inbound_publish(engine, 4, QoS.AT_LEAST_ONCE, b"payload")
    effects = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    messages = [e.data for e in effects if e.kind is EffectKind.MESSAGE]
    assert len(messages) == 1 and messages[0].dup
    _assert_accounting_exact(engine)


# --------------------------------------------------------------------------
# 3. EffectPump used to retain the exception raised by the background flush
#    task and hand it to whichever call next suspended inside drain() — an
#    operation with nothing to do with the incident.
# --------------------------------------------------------------------------


class _RudeBrokerTransport:
    """Acks CONNECT and QoS 1 PUBLISH, and can inject a second CONNACK."""

    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
            elif raw.packet_type is PacketType.PUBLISH:
                pub = PublishPacket.decode(raw.flags, raw.remaining)
                if pub.qos is QoS.AT_LEAST_ONCE:
                    assert pub.mid is not None
                    self._rx.put_nowait(
                        encode_frame(PacketType.PUBACK, 0, pub.mid.to_bytes(2, "big"))
                    )

    def inject_second_connack(self) -> None:
        """Illegal on an established session: the engine reports a protocol error."""
        self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x01\x00"))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


async def test_protocol_error_is_not_reported_against_a_later_unrelated_publish() -> None:
    transport = _RudeBrokerTransport()
    client = AsyncClient(client_id="rc-stale")
    # on_publish pushes PUBLISH_COMPLETE onto the async slow path, so publish()
    # genuinely suspends inside drain() instead of finishing inline.
    client.on_publish = lambda mid, reason: None

    async def factory(*args: object, **kwargs: object) -> _RudeBrokerTransport:
        return transport

    client._transport_factory = factory  # type: ignore[assignment]
    await client.connect("broker", 1883, timeout=5)

    receipt = await client.publish("rc/first", b"one", qos=1)
    await asyncio.wait_for(receipt.wait(), timeout=5)

    # A protocol error arrives while no caller is waiting on the effect pump.
    transport.inject_second_connack()
    await asyncio.sleep(0.1)
    assert client.is_connected, "the rude CONNACK must not take the session down"
    assert client._effect_pump.error is None, "an unowned failure must not be retained"

    try:
        for index in range(3):
            receipt = await client.publish(f"rc/later-{index}", b"two", qos=1)
            await asyncio.wait_for(receipt.wait(), timeout=5)
    finally:
        await client.disconnect()


@pytest.mark.parametrize("has_pending_work", [True, False])
async def test_effect_pump_keeps_only_a_failure_that_blocks_pending_work(
    has_pending_work: bool,
) -> None:
    """Clearing the unowned error must not silence a genuine stuck-work failure.

    An empty deque means ``applied`` has caught up with ``enqueued``, so nobody
    can be blocked and the exception has no owner. A non-empty one means work
    really is stuck and whoever waits on it must still be told.
    """
    from mqttium.api._effects import EffectPump
    from mqttium.protocol.effects import EngineEffect

    client = AsyncClient(client_id="rc-blocked")
    pump: EffectPump = client._effect_pump

    async def failing() -> None:
        raise RuntimeError("apply failed")

    task = asyncio.create_task(failing())
    with pytest.raises(RuntimeError):
        await task

    boom = RuntimeError("apply failed")
    pump.error = boom
    if has_pending_work:
        pump.pending.append(EngineEffect(kind=EffectKind.PROTOCOL_ERROR, data="stuck"))

    asyncio.get_running_loop().set_exception_handler(lambda loop, context: None)
    pump._done(task)

    if has_pending_work:
        assert pump.error is boom, "a failure blocking pending effects must be reported"
        pump.pending.clear()
    else:
        assert pump.error is None, "an unowned failure must not ambush a later caller"


# --------------------------------------------------------------------------
# 4. InflightStore.update_out/update_in only guarantee the mutable state
#    fields. SqliteInflightStore deliberately narrows the write to them so a
#    retransmission does not rewrite the payload BLOB; MemoryInflightStore
#    replaces the record. The engine must only ever depend on the guaranteed
#    part, and both stores must agree on it.
# --------------------------------------------------------------------------


def test_update_out_agrees_on_the_guaranteed_state_fields(tmp_path) -> None:  # noqa: ANN001
    from mqttium.persistence.sqlite import SqliteInflightStore

    memory = MemoryInflightStore()
    sqlite = SqliteInflightStore(tmp_path / "store.db")

    original = OutboundMessage(
        mid=7,
        topic="orig",
        payload=b"X" * 32,
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=OutboundQoSState.WAIT_PUBREC,
    )
    # What _retransmit writes back: the record is re-sent with DUP set.
    resent = OutboundMessage(
        mid=7,
        topic="orig",
        payload=b"X" * 32,
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=OutboundQoSState.WAIT_PUBCOMP,
        dup=True,
    )
    try:
        for store in (memory, sqlite):
            store.put_out(original)
            store.update_out(resent)

        from_memory = memory.get_out(7)
        from_sqlite = sqlite.get_out(7)
        assert from_memory is not None and from_sqlite is not None
        assert (from_memory.state, from_memory.dup) == (from_sqlite.state, from_sqlite.dup)
        assert (from_sqlite.state, from_sqlite.dup) == (OutboundQoSState.WAIT_PUBCOMP, True)
    finally:
        sqlite.close()


def test_update_in_agrees_on_the_guaranteed_state_fields(tmp_path) -> None:  # noqa: ANN001
    from mqttium.persistence.sqlite import SqliteInflightStore

    memory = MemoryInflightStore()
    sqlite = SqliteInflightStore(tmp_path / "store.db")

    original = InboundMessage(
        mid=3,
        topic="orig",
        payload=b"X" * 16,
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=InboundQoSState.WAIT_PUBREL,
    )
    acknowledged = InboundMessage(
        mid=3,
        topic="orig",
        payload=b"X" * 16,
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=InboundQoSState.WAIT_USER_ACK,
        delivered=True,
        user_acked=True,
    )
    try:
        for store in (memory, sqlite):
            store.put_in(original)
            store.update_in(acknowledged)

        from_memory = memory.get_in(3)
        from_sqlite = sqlite.get_in(3)
        assert from_memory is not None and from_sqlite is not None
        assert (from_memory.state, from_memory.delivered, from_memory.user_acked) == (
            from_sqlite.state,
            from_sqlite.delivered,
            from_sqlite.user_acked,
        )
    finally:
        sqlite.close()


def test_update_out_preserves_retransmission_order(tmp_path) -> None:  # noqa: ANN001
    """Order is part of the contract: an update must not move the record."""
    from mqttium.persistence.sqlite import SqliteInflightStore

    sqlite = SqliteInflightStore(tmp_path / "order.db")
    try:
        for mid in (1, 2, 3):
            sqlite.put_out(
                OutboundMessage(
                    mid=mid,
                    topic=f"t/{mid}",
                    payload=b"p",
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    state=OutboundQoSState.WAIT_PUBACK,
                )
            )
        first = sqlite.get_out(1)
        assert first is not None
        first.dup = True
        sqlite.update_out(first)

        assert [m.mid for m in sqlite.out_items()] == [1, 2, 3]
    finally:
        sqlite.close()


def test_both_built_in_stores_compact_through_the_transition_extension() -> None:
    """The engine never depends on update_out for compaction: both built-in
    stores implement TransitionInflightStore, so on_pubrec takes that path."""
    from mqttium.persistence.memory import TransitionInflightStore
    from mqttium.persistence.sqlite import SqliteInflightStore

    assert isinstance(MemoryInflightStore(), TransitionInflightStore)
    assert issubclass(SqliteInflightStore, TransitionInflightStore)
