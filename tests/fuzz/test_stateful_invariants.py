"""Stateful invariant fuzzing for the protocol core and the persistence layer.

Two harnesses, both seed-reproducible and both cheap enough to run in CI:

* ``test_engine_invariants_hold`` drives a ``ProtocolEngine`` through random
  publish / acknowledge / reconnect / drop sequences and re-checks the
  invariants documented in ``AGENTS.md`` after *every* step. It is a regression
  net for the accounting that ``OutboundSession`` and ``InboundSession`` own:
  budgets and packet identifiers must agree with the durable store, while the
  connection-scoped flow window must always remain within its negotiated bound.

* ``test_store_implementations_agree`` treats ``MemoryInflightStore`` as the
  reference model for ``SqliteInflightStore``. The sessions are written against
  the ``InflightStore`` protocol, not against either implementation, so an
  observable divergence is a bug in one of them.

Scale with ``MQTTIUM_FUZZ_SEEDS`` / ``MQTTIUM_FUZZ_STEPS`` for longer campaigns.

The deliberately narrow ``update_out``/``update_in`` contract is excluded here
and pinned by focused persistence transition tests.
"""

from __future__ import annotations

import os
import random
from collections.abc import Callable
from typing import Any

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import (
    ConnectionState,
    InboundQoSState,
    MQTTProtocolVersion,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.errors import MQTTError
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
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import InboundMessage, OutboundMessage, Properties

SEEDS = int(os.environ.get("MQTTIUM_FUZZ_SEEDS", "12"))
STEPS = int(os.environ.get("MQTTIUM_FUZZ_STEPS", "200"))

_LAUNCHED = (
    OutboundQoSState.WAIT_PUBACK,
    OutboundQoSState.WAIT_PUBREC,
    OutboundQoSState.WAIT_PUBCOMP,
)


# --------------------------------------------------------------------- engine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack(engine: ProtocolEngine, *, session_present: bool) -> None:
    remaining = bytes((1 if session_present else 0, 0))
    if engine.config.protocol is MQTTProtocolVersion.MQTTv5:
        remaining += b"\x00"
    _feed(engine, encode_frame(PacketType.CONNACK, 0, remaining))


def _check_invariants(engine: ProtocolEngine, step: int, history: list[str]) -> None:
    def fail(message: str) -> None:
        trail = "\n    ".join(history[-25:])
        pytest.fail(f"step {step}: {message}\n  history:\n    {trail}")

    outbound = engine.outbound
    records = list(
        engine.store.get_out(summary.mid)
        for page in engine.store.out_summary_pages()
        for summary in page
    )
    pool = engine.packet_ids

    expected_mids = {record.mid for record in records} | set(engine._pending_sub_mids)
    if len(pool) != len(expected_mids):
        fail(f"packet id pool: len()={len(pool)} but owners={len(expected_mids)}")

    for record in records:
        if not pool.in_use(record.mid):
            fail(f"durable record mid={record.mid} ({record.state.name}) is not held in the pool")

    # A sealed row (#521) keeps its record and identifier but no reservation.
    sealed = outbound._sealed
    unsealed = [r for r in records if r.mid not in sealed]
    if outbound.unacknowledged_messages != len(unsealed):
        fail(
            f"pending_messages={outbound.unacknowledged_messages} but the store holds "
            f"{len(unsealed)} unsealed records"
        )
    stray_sealed = set(sealed) - {r.mid for r in records}
    if stray_sealed:
        fail(f"sealed mids with no durable record: {sorted(stray_sealed)}")

    expected_bytes = sum(outbound.stored_logical_size(r) for r in unsealed)
    if outbound.unacknowledged_bytes != expected_bytes:
        fail(
            f"pending_bytes={outbound.unacknowledged_bytes} but the records sum to {expected_bytes}"
        )

    # Send Quota is connection-scoped credit, not durable-record occupancy.
    # A resumed WAIT_PUBCOMP retransmits PUBREL without consuming quota, while
    # its later PUBCOMP can replenish quota consumed by another PUBLISH. After
    # reconnect there is therefore no one-to-one mapping from durable WAIT_*
    # states to flow.inflight; the negotiated bounds remain invariant.
    if not 0 <= outbound.flow.inflight <= outbound.flow.limit:
        fail(f"flow.inflight={outbound.flow.inflight} outside [0, {outbound.flow.limit}]")

    queued = [m.mid for m in outbound._queued]
    if len(queued) != len(set(queued)):
        fail(f"_queued holds duplicate mids: {queued}")
    stray = set(queued) - {r.mid for r in records}
    if stray:
        fail(f"_queued indexes mids with no durable record: {sorted(stray)}")

    inbound_records = list(
        engine.store.get_in(meta.mid) for page in engine.store.in_index_pages() for meta in page
    )
    inbound = engine.inbound
    expected_inbound = sum(inbound.stored_logical_size(r) for r in inbound_records)
    if inbound._pending_bytes != expected_inbound:
        fail(
            f"inbound pending_bytes={inbound._pending_bytes} but the records sum to "
            f"{expected_inbound}"
        )


def _emitted_deliveries(engine: ProtocolEngine) -> list[tuple[int, object]]:
    """Take this step's effects; return the deliveries the runtime must commit."""
    return [
        (effect.data.mid, effect.exchange_token)
        for effect in engine.take_effects()
        if effect.kind is EffectKind.MESSAGE and effect.requires_delivery_mark
    ]


def _deliver_one(
    engine: ProtocolEngine,
    rng: random.Random,
    undelivered: list[tuple[int, object]],
    history: list[str],
    step: int,
) -> set[int]:
    """Commit one pending delivery; return the identifiers it really owned."""
    if not undelivered:
        return set()
    mid, token = undelivered.pop(rng.randrange(len(undelivered)))
    history.append(f"{step}: application owns mid={mid}")
    current = engine.inbound._exchange_tokens.get(mid) is token
    engine.mark_inbound_delivered(mid, token)
    return {mid} if current else set()


def _check_completed_only_after_delivery(
    engine: ProtocolEngine,
    before: dict[int, bool],
    committed: set[int],
    step: int,
    history: list[str],
) -> None:
    # MESSAGE emitted is not MESSAGE committed: a persisted exchange is never
    # completed before the application owns it (#519, #520).
    after = _inbound_delivery_states(engine)
    for mid, delivered in before.items():
        if mid not in after and not delivered and mid not in committed:
            raise AssertionError(
                f"step {step}: undelivered inbound mid={mid} was completed\n"
                + "\n".join(history[-25:])
            )


def _subscription_or_seal(
    engine: ProtocolEngine, rng: random.Random, operation: str, history: list[str], step: int
) -> None:
    """Share the packet identifier pool with SUBSCRIBE and sealed rows (#521)."""
    if operation == "subscribe":
        if engine.state is ConnectionState.CONNECTED:
            history.append(f"{step}: SUBSCRIBE")
            engine.queue_subscribe([(f"s/{rng.randint(0, 3)}", 1)])
        return
    # A terminal receipt failure seals what is still stored.
    mids = [summary.mid for page in engine.store.out_summary_pages() for summary in page]
    if mids:
        chosen = rng.sample(mids, rng.randint(1, len(mids)))
        history.append(f"{step}: seal {sorted(chosen)}")
        engine.seal_publications(chosen)


def _inbound_delivery_states(engine: ProtocolEngine) -> dict[int, bool]:
    return {meta.mid: meta.delivered for page in engine.store.in_index_pages() for meta in page}


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("seed", range(SEEDS))
def test_engine_invariants_hold(protocol: MQTTProtocolVersion, seed: int) -> None:
    rng = random.Random(seed)
    engine = ProtocolEngine(
        EngineConfig(
            client_id="fuzz",
            protocol=protocol,
            clean_start=False,
            max_outbound_inflight=rng.choice([1, 2, 4]),
            max_inbound_inflight=rng.choice([2, 5, 20]),
            max_unacknowledged_messages=rng.choice([None, 6, 12]),
            max_unacknowledged_bytes=rng.choice([None, 4096]),
        ),
        MemoryInflightStore(),
    )
    history: list[str] = []

    engine.begin_connect()
    _connack(engine, session_present=False)
    engine.take_effects()
    _check_invariants(engine, -1, history)

    operations = [
        "publish",
        "puback",
        "pubrec",
        "pubcomp",
        "inbound_publish",
        "inbound_pubrel",
        "deliver",
        "drop",
        "reconnect",
        "subscribe",
        "seal",
    ]
    weights = [40, 14, 12, 12, 10, 6, 8, 4, 6, 4, 3]
    # Deliveries the runtime has not committed yet, in emission order. A
    # "deliver" step commits one of them, possibly after its exchange ended
    # and the identifier was reused (a late mark must then be ignored).
    undelivered: list[tuple[int, object]] = []

    for step in range(STEPS):
        operation = rng.choices(operations, weights=weights)[0]
        before = _inbound_delivery_states(engine)
        committed: set[int] = set()
        discarded_session = False
        try:
            if operation == "publish":
                qos = rng.choice(list(QoS))
                history.append(f"{step}: publish qos={int(qos)}")
                engine.queue_publish(f"t/{rng.randint(0, 5)}", bytes(rng.randint(0, 900)), qos=qos)
            elif operation in ("puback", "pubrec", "pubcomp"):
                candidates = [
                    r
                    for r in (
                        engine.store.get_out(summary.mid)
                        for page in engine.store.out_summary_pages()
                        for summary in page
                    )
                    if r.state in _LAUNCHED
                ]
                if not candidates:
                    continue
                mid = rng.choice(candidates).mid
                history.append(f"{step}: {operation.upper()} mid={mid}")
                packet = {
                    "puback": PubAckPacket,
                    "pubrec": PubRecPacket,
                    "pubcomp": PubCompPacket,
                }[operation]
                _feed(engine, packet(mid=mid).encode(protocol))
            elif operation == "inbound_publish":
                if engine.state is not ConnectionState.CONNECTED:
                    continue
                qos = rng.choice([QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
                mid = rng.randint(1, 12)
                history.append(f"{step}: inbound PUBLISH qos={int(qos)} mid={mid}")
                _feed(
                    engine,
                    PublishPacket(
                        topic=f"in/{rng.randint(0, 3)}",
                        payload=bytes(rng.randint(0, 300)),
                        qos=qos,
                        retain=False,
                        dup=False,
                        mid=mid,
                    ).encode(protocol),
                )
            elif operation == "inbound_pubrel":
                if engine.state is not ConnectionState.CONNECTED:
                    continue
                mid = rng.randint(1, 12)
                history.append(f"{step}: inbound PUBREL mid={mid}")
                _feed(engine, PubRelPacket(mid=mid).encode(protocol))
            elif operation == "deliver":
                committed = _deliver_one(engine, rng, undelivered, history, step)
            elif operation in ("subscribe", "seal"):
                _subscription_or_seal(engine, rng, operation, history, step)
            elif operation == "drop":
                history.append(f"{step}: transport closed")
                engine.notify_transport_closed()
            else:
                if engine.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING):
                    engine.notify_transport_closed()
                session_present = rng.random() < 0.75
                discarded_session = not session_present
                history.append(f"{step}: reconnect session_present={session_present}")
                engine.begin_connect()
                _connack(engine, session_present=session_present)
        except MQTTError as exc:
            history.append(f"    -> refused: {type(exc).__name__}: {exc}")
        undelivered.extend(_emitted_deliveries(engine))
        _check_invariants(engine, step, history)
        if not discarded_session:
            _check_completed_only_after_delivery(engine, before, committed, step, history)


# ---------------------------------------------------------------------- stores

_OUT_STATES = list(OutboundQoSState)
_IN_STATES = [
    InboundQoSState.WAIT_PUBREL,
    InboundQoSState.WAIT_PUBACK,
    InboundQoSState.WAIT_USER_ACK,
]


def _norm_out(message: OutboundMessage | None) -> tuple | None:
    if message is None:
        return None
    return (
        message.mid,
        message.topic,
        bytes(message.payload),
        int(message.qos),
        message.retain,
        message.state,
        message.dup,
        message.logical_size,
        None if message.properties is None else sorted(message.properties.values.items(), key=str),
    )


def _norm_in(message: InboundMessage | None) -> tuple | None:
    if message is None:
        return None
    return (
        message.mid,
        message.topic,
        bytes(message.payload),
        int(message.qos),
        message.retain,
        message.state,
        message.delivered,
        message.user_acked,
        message.logical_size,
    )


def _norm_meta(meta: object | None) -> tuple | None:
    if meta is None:
        return None
    base = (meta.mid, meta.state, meta.logical_size)  # type: ignore[attr-defined]
    user_acked = getattr(meta, "user_acked", None)
    delivered = getattr(meta, "delivered", None)
    if user_acked is None:
        return base
    return base + (user_acked, delivered)


def _make_out(rng: random.Random, mid: int) -> OutboundMessage:
    properties = None
    if rng.random() < 0.3:
        properties = Properties()
        properties = Properties(
            {**properties.values, "message_expiry_interval": rng.randint(0, 1000)}
        )
    return OutboundMessage(
        mid=mid,
        topic=f"t/{rng.randint(0, 4)}",
        payload=bytes(rng.randint(0, 200)),
        qos=rng.choice([QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE]),
        retain=rng.random() < 0.3,
        state=rng.choice(_OUT_STATES),
        dup=rng.random() < 0.3,
        properties=properties,
        logical_size=rng.randint(1, 500),
    )


def _make_in(rng: random.Random, mid: int) -> InboundMessage:
    return InboundMessage(
        mid=mid,
        topic=f"in/{rng.randint(0, 4)}",
        payload=bytes(rng.randint(0, 200)),
        qos=rng.choice([QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE]),
        retain=rng.random() < 0.3,
        state=rng.choice(_IN_STATES),
        delivered=rng.random() < 0.5,
        user_acked=rng.random() < 0.3,
        logical_size=rng.randint(1, 500),
    )


def _store_operations(rng: random.Random) -> tuple[str, Callable[[Any], object]]:
    """Draw one operation, binding every random parameter up front.

    Both stores then receive exactly the same call, which is what makes the
    comparison a one-liner instead of a branch per method. Every parameter is
    drawn before the closures are built, so the two invocations cannot diverge
    by consuming the generator twice.

    ``update_out``/``update_in`` are excluded: they only guarantee the mutable
    state fields, and ``SqliteInflightStore`` deliberately narrows the write to
    them, so the two stores legitimately differ on everything else. That
    guaranteed part is compared in
    ``tests/unit/test_packet_id_and_store_consistency.py``.
    """
    mid = rng.randint(1, 12)
    page_size = rng.choice([1, 2, 5])
    outbound_message = _make_out(rng, mid)
    inbound_message = _make_in(rng, mid)
    out_expected = rng.choice(_OUT_STATES)
    out_old, out_new = rng.choice(_OUT_STATES), rng.choice(_OUT_STATES)
    compact = rng.random() < 0.5
    in_expected = rng.choice(_IN_STATES)
    in_old, in_new = rng.choice(_IN_STATES), rng.choice(_IN_STATES)
    user_acked = rng.choice([None, True, False])
    replay_messages, replay_budget = rng.choice([1, 3]), rng.choice([16, 256])

    choices: list[tuple[str, Callable[[Any], object]]] = [
        (f"put_out(mid={mid})", lambda s: s.put_out(outbound_message)),
        (f"get_out({mid})", lambda s: _norm_out(s.get_out(mid))),
        (f"delete_out({mid})", lambda s: s.delete_out(mid)),
        (
            f"out_summary_pages({page_size})",
            lambda s: [
                (x.mid, x.payload_size, x.state, x.logical_size)
                for page in s.out_summary_pages(page_size)
                for x in page
            ],
        ),
        (f"out_meta({mid})", lambda s: _norm_meta(s.out_meta(mid))),
        (
            f"complete_out({mid}, {out_expected.name})",
            lambda s: _norm_meta(s.complete_out(mid, out_expected)),
        ),
        (
            f"transition_out({mid}, {out_old.name}->{out_new.name}, compact={compact})",
            lambda s: _norm_meta(s.transition_out(mid, out_old, out_new, compact=compact)),
        ),
        (f"put_in(mid={mid})", lambda s: s.put_in(inbound_message)),
        (f"get_in({mid})", lambda s: _norm_in(s.get_in(mid))),
        (f"in_meta({mid})", lambda s: _norm_meta(s.in_meta(mid))),
        (f"mark_in_delivered({mid})", lambda s: s.mark_in_delivered(mid)),
        (
            f"transition_in({mid}, {in_old.name}->{in_new.name}, user_acked={user_acked})",
            lambda s: _norm_meta(s.transition_in(mid, in_old, in_new, user_acked=user_acked)),
        ),
        (
            f"complete_in({mid}, {in_expected.name})",
            lambda s: _norm_meta(s.complete_in(mid, in_expected)),
        ),
        (
            f"in_index_pages({page_size})",
            lambda s: [_norm_meta(m) for page in s.in_index_pages(page_size) for m in page],
        ),
        ("in_count()", lambda s: s.in_count()),
        (
            f"in_replay_pages({replay_messages}, {replay_budget})",
            lambda s: [
                _norm_in(m)
                for page in s.in_replay_pages(replay_messages, replay_budget)
                for m in page
            ],
        ),
        ("clear_out()", lambda s: s.clear_out()),
        ("clear_in()", lambda s: s.clear_in()),
    ]
    return rng.choice(choices)


@pytest.mark.parametrize("seed", range(SEEDS))
def test_store_implementations_agree(seed: int, tmp_path) -> None:  # noqa: ANN001
    """SqliteInflightStore must be observationally equal to the reference store."""
    rng = random.Random(seed)
    memory = MemoryInflightStore()
    sqlite = SqliteInflightStore(tmp_path / f"seed{seed}.db")
    history: list[str] = []

    def agree(left: object, right: object, label: str, step: int) -> None:
        if left != right:
            trail = "\n    ".join(history[-20:])
            pytest.fail(
                f"step {step}: {label} diverged\n  memory: {left!r}\n  sqlite: {right!r}"
                f"\n  history:\n    {trail}"
            )

    try:
        for step in range(STEPS):
            label, operation = _store_operations(rng)
            history.append(f"{step}: {label}")
            agree(operation(memory), operation(sqlite), label, step)

            agree(
                [
                    _norm_out(m)
                    for m in (
                        memory.get_out(summary.mid)
                        for page in memory.out_summary_pages()
                        for summary in page
                    )
                ],
                [
                    _norm_out(m)
                    for m in (
                        sqlite.get_out(summary.mid)
                        for page in sqlite.out_summary_pages()
                        for summary in page
                    )
                ],
                "outbound contents after the step",
                step,
            )
            agree(
                [
                    _norm_in(m)
                    for m in (
                        memory.get_in(meta.mid) for page in memory.in_index_pages() for meta in page
                    )
                ],
                [
                    _norm_in(m)
                    for m in (
                        sqlite.get_in(meta.mid) for page in sqlite.in_index_pages() for meta in page
                    )
                ],
                "inbound contents after the step",
                step,
            )
    finally:
        sqlite.close()


@pytest.mark.parametrize("seed", range(SEEDS))
def test_replay_interleavings_agree_and_never_emit_stale_rows(  # noqa: ANN001, C901
    seed: int, tmp_path
) -> None:
    """Exercise the yield boundary between replay batches against both stores."""
    rng = random.Random(seed)
    memory = MemoryInflightStore()
    sqlite = SqliteInflightStore(tmp_path / f"replay-seed{seed}.db")
    for store in (memory, sqlite):
        with store.batch():
            for mid in range(1, 301):
                topic = f"replay/{mid}"
                store.put_in(
                    InboundMessage(
                        mid=mid,
                        topic=topic,
                        payload=b"body",
                        qos=QoS.EXACTLY_ONCE,
                        retain=False,
                        state=InboundQoSState.WAIT_PUBREL,
                        logical_size=len(topic) + 4,
                    )
                )
    engines = [
        ProtocolEngine(EngineConfig(client_id="replay-fuzz", clean_start=False), store=memory),
        ProtocolEngine(EngineConfig(client_id="replay-fuzz", clean_start=False), store=sqlite),
    ]

    def resume_pair() -> tuple[list[int], list[int]]:
        outputs: list[list[int]] = []
        for engine in engines:
            if engine.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING):
                engine.notify_transport_closed()
                engine.take_effects()
            engine.begin_connect()
            _connack(engine, session_present=True)
            outputs.append(
                [e.data.mid for e in engine.take_effects() if e.kind is EffectKind.MESSAGE]
            )
        return outputs[0], outputs[1]

    suppressed: set[int] = set()
    try:
        assert resume_pair() == (list(range(1, 65)), list(range(1, 65)))
        for step in range(STEPS):
            operation = rng.choices(
                ["complete", "delivered", "continue", "close", "reconnect"],
                weights=[24, 20, 38, 6, 12],
            )[0]
            mid = rng.randint(65, 300)
            outputs: list[list[int]] = []
            for engine in engines:
                if operation == "complete" and engine.state is ConnectionState.CONNECTED:
                    _feed(engine, PubRelPacket(mid=mid).encode(engine.config.protocol))
                    engine.take_effects()
                elif operation == "delivered":
                    engine.mark_inbound_delivered(mid)
                    engine.take_effects()
                elif operation == "continue":
                    engine.continue_inbound_replay()
                    outputs.append(
                        [e.data.mid for e in engine.take_effects() if e.kind is EffectKind.MESSAGE]
                    )
                elif operation == "close":
                    engine.notify_transport_closed()
                    engine.take_effects()
                    engine.continue_inbound_replay()
                    outputs.append(
                        [e.data.mid for e in engine.take_effects() if e.kind is EffectKind.MESSAGE]
                    )
                elif operation == "reconnect":
                    # Applied below once for the pair so the output comparison
                    # stays explicit.
                    pass

            if operation in {"complete", "delivered"}:
                current = (memory.get_in(mid), sqlite.get_in(mid))
                if all(record is None or record.delivered for record in current):
                    suppressed.add(mid)
            elif operation == "reconnect":
                left, right = resume_pair()
                outputs = [left, right]
            if outputs:
                assert outputs[0] == outputs[1], (step, operation, outputs)
                assert suppressed.isdisjoint(outputs[0]), (step, operation, suppressed, outputs[0])
            _check_invariants(engines[0], step, [operation])
            _check_invariants(engines[1], step, [operation])
            assert [
                _norm_in(m)
                for m in (
                    memory.get_in(meta.mid) for page in memory.in_index_pages() for meta in page
                )
            ] == [
                _norm_in(m)
                for m in (
                    sqlite.get_in(meta.mid) for page in sqlite.in_index_pages() for meta in page
                )
            ]
    finally:
        sqlite.close()
