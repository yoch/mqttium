"""Write the hand-made corpus of traces the oracle is tested on.

Every trace here is written by hand, event by event: none comes from a run of
the broker. A good trace is a history the specification allows. A faulty one
is one edit of a good trace, and is refused for one assertion and no other.

Run from the ``mqttiumd`` directory::

    python tests/oracle/build_traces.py           # rewrite the corpus
    python tests/oracle/build_traces.py --check   # fail if it is not up to date

Standard library only.
"""

from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import shutil
import sys

ORACLE = pathlib.Path(__file__).resolve().parent
ROOT = ORACLE.parent.parent
OUT = ORACLE / "traces"
BASE = "tests/oracle/traces"

OUTPUTS: dict[str, bytes] = {}
INDEX: list[dict] = []

ASSERTIONS = [
    "one_admission_per_observed_inbound_exchange",
    "same_required_obligations_at_both_checkpoints",
    "same_recipe_for_op_A",
    "no_positive_ack_before_confirmed_commit",
    "no_pubrec_observed_before_crash",
    "broker_inputs_have_provenance",
    "all_required_faults_hit",
    "trace_complete",
]


def dump(value) -> bytes:
    return (json.dumps(value, indent=2) + "\n").encode()


# ---------------------------------------------------------------- scenarios
# P1-02 is the text of the specification and does not list the provenance
# assertion; the corpus judges it on a copy that does.
P1_02 = json.loads((ROOT / "tests/scenarios/p1/P1-02.json").read_text())
P1_02["assertions"] = ASSERTIONS
P1_02B = json.loads(
    (ROOT / "contracts/examples/v1/valid/scenario-held-command-and-result.json").read_text()
)
P1_02B["assertions"] = ASSERTIONS
OUTPUTS["scenarios/p1-02.json"] = dump(P1_02)
OUTPUTS["scenarios/p1-02b.json"] = dump(P1_02B)


# ------------------------------------------------------------------ identities
def be(value, width=8):
    return value.to_bytes(width, "big").hex()


def scoped(epoch, sequence):
    return {"incarnation": "1", "epoch": str(epoch), "owner": "0", "sequence": str(sequence)}


def scoped_hex(identity):
    return "".join(
        be(int(identity[name])) for name in ("incarnation", "epoch", "owner", "sequence")
    )


def effect(epoch, sequence):
    return {"epoch": str(epoch), "owner": "0", "sequence": str(sequence)}


PRODUCER, B, C = 1, 2, 3
CLIENT_IDS = {PRODUCER: "p1-producer", B: "p1-b", C: "p1-c"}
TOPIC = b"p1/example"
PAYLOAD = b"payload"
OP_ADMIT, PUB, PAYLOAD_ID = scoped(1, 1), scoped(1, 1), scoped(1, 1)
OP_RELEASE = scoped(2, 1)
OP_SECOND, PUB_SECOND, PAYLOAD_SECOND = scoped(2, 2), scoped(2, 2), scoped(2, 2)
OP_RELEASE_B = scoped(1, 2)


def admit_recipe(operation, publication, payload, generation, targets=((B, 1), (C, 1))):
    """The canonical bytes of an admission of the producer's Packet Identifier 7."""
    return "".join(
        [
            b"MQDR".hex() + be(1, 2) + be(1, 1),
            scoped_hex(operation) + scoped_hex(operation) + scoped_hex(publication),
            be(PRODUCER) + be(1) + be(7, 2) + be(generation),
            be(1) + be(2, 1) + be(0, 1),
            be(len(TOPIC), 2) + TOPIC.hex(),
            scoped_hex(payload) + be(len(PAYLOAD)),
            be(len(PAYLOAD)) + be(len(targets)) + be(1) + be(1),
            be(len(targets), 2),
            *(
                be(session) + be(1) + be(index, 4) + be(qos, 1) + be(1, 1)
                for index, (session, qos) in enumerate(targets)
            ),
        ]
    )


def release_recipe(operation, generation):
    return (
        b"MQDR".hex()
        + be(1, 2)
        + be(2, 1)
        + scoped_hex(operation)
        + be(PRODUCER)
        + be(7, 2)
        + be(generation)
    )


# ------------------------------------------------------------------- snapshots
def snapshot(run, checkpoint, admissions, releases=(), open_exchange=True, epoch=1):
    """A snapshot holding the given admissions: (operation, publication, payload, generation)."""
    return {
        "format": "mqttiumd.store-snapshot",
        "schema_version": 1,
        "run_id": run,
        "checkpoint": checkpoint,
        "backend": "sim-store",
        "meta": {"incarnation": "1", "epoch": str(epoch)},
        "sessions": [
            {"session": str(session), "client_id_hex": name.encode().hex(), "revision": "1"}
            for session, name in CLIENT_IDS.items()
        ],
        "routes": [
            {
                "topic_hex": TOPIC.hex(),
                "revision": "1",
                "targets": [{"session": str(B), "qos": 1}, {"session": str(C), "qos": 1}],
            }
        ],
        "operations": [
            {
                "operation_id": operation,
                "command_kind": "AdmitPublication",
                "recipe_hex": admit_recipe(operation, publication, payload, generation),
                "decision": "Committed",
            }
            for operation, publication, payload, generation in admissions
        ]
        + [
            {
                "operation_id": operation,
                "command_kind": "ReleaseInbound",
                "recipe_hex": release_recipe(operation, generation),
                "decision": "Committed",
            }
            for operation, generation in releases
        ],
        "fences": [],
        "publications": [
            {
                "publication_id": publication,
                "topic_hex": TOPIC.hex(),
                "qos": 2,
                "retain": False,
                "payload": {
                    "payload_id": payload,
                    "length": str(len(PAYLOAD)),
                    "sha256": hashlib.sha256(PAYLOAD).hexdigest(),
                },
            }
            for _, publication, payload, _ in admissions
        ],
        "obligations": [
            {
                "publication_id": publication,
                "index": index,
                "session": str(target),
                "cause": "LiveExact",
                "effective_qos": 1,
                "phase": "Queued",
            }
            for _, publication, _, _ in admissions
            for index, target in enumerate((B, C))
        ],
        "inbound_exchanges": [
            {
                "exchange": {"session": str(PRODUCER), "generation": str(admissions[-1][3])},
                "packet_id": 7,
                "publication_id": admissions[-1][1],
                "phase": "AwaitPubrel",
            }
        ]
        if open_exchange
        else [],
        "accounts": [],
    }


# ---------------------------------------------------------------------- events
CONNECT_HEX = "101700044d5154540500000a00000a70312d70726f6475636572"
CONNACK_HEX = "20030100" + "00"
PUBLISH_HEX = "3416000a" + TOPIC.hex() + "000700" + PAYLOAD.hex()
PUBLISH_DUP_HEX = "3c" + PUBLISH_HEX[2:]
PUBREC_HEX, PUBREL_HEX, PUBCOMP_HEX = "50020007", "62020007", "70020007"


def ev(label, origin, kind, payload, parents=(), inc=1):
    return {
        "label": label,
        "origin": origin,
        "inc": inc,
        "kind": kind,
        "payload": payload,
        "parents": list(parents),
    }


def page(epoch, facts):
    return {"run_epoch": str(epoch), "page": "0", "last": True, "facts": facts}


SESSION_FACTS = [
    {
        "kind": "Session",
        "session": str(session),
        "client_id_hex": name.encode().hex(),
        "revision": "1",
    }
    for session, name in CLIENT_IDS.items()
]


def connect(tag, inc, generation, first_effect):
    """A client resumes its session on a new connection of broker incarnation ``inc``."""
    connection = {"session": str(PRODUCER), "generation": str(generation)}
    return [
        ev(f"{tag}.open", "peer", "PeerConnectionOpened", {"client": "producer"}),
        ev(
            f"{tag}.connect",
            "peer",
            "PeerPacketSent",
            {"client": "producer", "packet_kind": "CONNECT", "bytes_hex": CONNECT_HEX},
        ),
        ev(
            f"{tag}.connect.bytes",
            "transport",
            "ClientBytesReceived",
            {"connection_generation": str(generation), "bytes_hex": CONNECT_HEX},
            [f"{tag}.connect"],
            inc,
        ),
        ev(
            f"{tag}.connect.in",
            "broker-core",
            "PacketDelivered",
            {
                "connection_generation": str(generation),
                "ingress_order": "0",
                "packet_kind": "CONNECT",
            },
            [f"{tag}.connect.bytes"],
            inc,
        ),
        ev(
            f"{tag}.connack",
            "broker-core",
            "FrameEligible",
            {
                "effect_id": effect(inc, first_effect),
                "connection": connection,
                "packet_kind": "CONNACK",
                "session_present": True,
            },
            [f"{tag}.connect.in"],
            inc,
        ),
        ev(
            f"{tag}.connack.written",
            "transport",
            "FrameWriteProgress",
            {
                "effect_id": effect(inc, first_effect),
                "connection": connection,
                "bytes_confirmed": "5",
                "bytes_total": "5",
            },
            [f"{tag}.connack"],
            inc,
        ),
        ev(
            f"{tag}.connack.seen",
            "peer",
            "PeerPacketObserved",
            {
                "client": "producer",
                "packet_kind": "CONNACK",
                "reason_code": 0,
                "session_present": True,
                "bytes_hex": CONNACK_HEX,
            },
            [f"{tag}.connack.written"],
        ),
    ]


def packet_in(tag, inc, generation, order, kind, hex_bytes, dup=None):
    """The client sends a packet and the core consumes it."""
    sent = {"client": "producer", "packet_kind": kind, "packet_id": 7, "bytes_hex": hex_bytes}
    if dup is not None:
        sent["dup"] = dup
    return [
        ev(f"{tag}", "peer", "PeerPacketSent", sent),
        ev(
            f"{tag}.bytes",
            "transport",
            "ClientBytesReceived",
            {"connection_generation": str(generation), "bytes_hex": hex_bytes},
            [tag],
            inc,
        ),
        ev(
            f"{tag}.in",
            "broker-core",
            "PacketDelivered",
            {
                "connection_generation": str(generation),
                "ingress_order": str(order),
                "packet_kind": kind,
                "packet_id": 7,
            },
            [f"{tag}.bytes"],
            inc,
        ),
    ]


def ack_out(tag, inc, generation, effect_sequence, kind, hex_bytes, cause):
    """The core makes a positive acknowledgement eligible and the client observes it."""
    connection = {"session": str(PRODUCER), "generation": str(generation)}
    return [
        ev(
            tag,
            "broker-core",
            "FrameEligible",
            {
                "effect_id": effect(inc, effect_sequence),
                "connection": connection,
                "packet_kind": kind,
                "packet_id": 7,
                "reason": "Success",
            },
            [cause],
            inc,
        ),
        ev(
            f"{tag}.written",
            "transport",
            "FrameWriteProgress",
            {
                "effect_id": effect(inc, effect_sequence),
                "connection": connection,
                "bytes_confirmed": "4",
                "bytes_total": "4",
            },
            [tag],
            inc,
        ),
        ev(
            f"{tag}.seen",
            "peer",
            "PeerPacketObserved",
            {
                "client": "producer",
                "packet_kind": kind,
                "packet_id": 7,
                "reason_code": 0,
                "bytes_hex": hex_bytes,
            },
            [f"{tag}.written"],
        ),
    ]


def persist(tag, inc, effect_sequence, operation, kind, cause, token):
    """The store adapter accepts a command, then the timer accepts its deadline."""
    return [
        ev(
            f"{tag}.submitted",
            "store-adapter",
            "CommandSubmitted",
            {
                "effect_id": effect(inc, effect_sequence),
                "operation_id": operation,
                "command_kind": kind,
            },
            [cause],
            inc,
        ),
        ev(
            f"{tag}.armed",
            "timer",
            "DeadlineArmed",
            {"effect_id": effect(inc, effect_sequence + 1), "token": str(token), "after": "30"},
            [cause],
            inc,
        ),
    ]


def committed(tag, inc, effect_sequence, operation, kind, cause=None):
    """Hidden truth: the store commits the command and has its result ready."""
    return [
        ev(
            f"{tag}.stable",
            "store-evidence",
            "StoreCommitStable",
            {"operation_id": operation, "command_kind": kind},
            [cause or f"{tag}.submitted"],
        ),
        ev(
            f"{tag}.ready",
            "store-evidence",
            "StoreResultReady",
            {
                "effect_id": effect(inc, effect_sequence),
                "operation_id": operation,
                "outcome": "Committed",
            },
            [f"{tag}.stable"],
        ),
    ]


def delivered(tag, inc, effect_sequence, operation, outcome="Committed", cause=None, label=None):
    return ev(
        label or f"{tag}.delivered",
        "broker-core",
        "StoreResultDelivered",
        {"effect_id": effect(inc, effect_sequence), "operation_id": operation, "outcome": outcome},
        [cause or f"{tag}.ready"],
        inc,
    )


def inspected(checkpoint):
    return ev(
        f"inspect.{checkpoint}",
        "bench-supervisor",
        "DurableStateInspected",
        {"checkpoint": checkpoint, "snapshot": None},
    )


def header(scenario):
    return ev(
        "header",
        "bench-supervisor",
        "TraceHeader",
        {"scenario_id": scenario, "backend": "sim-store"},
    )


def recovery(inc, facts, tag="recovery"):
    return [
        ev(f"{tag}.read", "store-adapter", "RecoveryPageRead", page(inc, facts), [], inc),
        ev(
            f"{tag}.in",
            "broker-core",
            "RecoveryPageDelivered",
            page(inc, facts),
            [f"{tag}.read"],
            inc,
        ),
    ]


BOOT_FACTS = [{"kind": "Epoch", "incarnation": "1", "epoch": "1"}, *SESSION_FACTS]


def fault_hit(fault, point, cause, operation=OP_ADMIT, effect_id=None, occurrence=1):
    return ev(
        f"hit.{fault}" if occurrence == 1 else f"hit.{fault}.{occurrence}",
        "fault-injector",
        "FaultHit",
        {
            "fault_id": fault,
            "point": point,
            "occurrence": str(occurrence),
            "operation_id": operation,
            "effect_id": effect_id or effect(1, 1),
        },
        [cause],
    )


def p1_02():
    """P1-02: the commit result is lost, the broker crashes, the client replays."""
    return (
        [header("P1-02")]
        + recovery(1, BOOT_FACTS, "boot")
        + connect("c1", 1, 1, 0)
        + packet_in("publish", 1, 1, 1, "PUBLISH", PUBLISH_HEX, dup=False)
        + persist("admit", 1, 1, OP_ADMIT, "AdmitPublication", "publish.in", 1)
        + committed("admit", 1, 1, OP_ADMIT, "AdmitPublication")
        + [
            fault_hit("lost_admission_result", "store.result.ready", "admit.ready"),
            inspected("after_commit_before_crash"),
            ev("crash", "bench-supervisor", "BrokerCrashed", {"failure_model": "process"}),
            ev("c1.close", "peer", "PeerConnectionClosed", {"client": "producer"}),
            ev("restart", "bench-supervisor", "BrokerRecovered", {"run_epoch": "2"}),
        ]
        + recovery(
            2,
            [{"kind": "Epoch", "incarnation": "1", "epoch": "2"}]
            + SESSION_FACTS
            + [
                {
                    "kind": "InboundExchange",
                    "exchange": {"session": str(PRODUCER), "generation": "1"},
                    "packet_id": 7,
                    "publication": PUB,
                    "phase": "AwaitPubrel",
                },
                {"kind": "Operation", "operation_id": OP_ADMIT, "knowledge": "Committed"},
            ],
        )
        + connect("c2", 2, 2, 0)
        + packet_in("replay", 2, 2, 1, "PUBLISH", PUBLISH_DUP_HEX, dup=True)
        + ack_out("pubrec", 2, 2, 1, "PUBREC", PUBREC_HEX, "replay.in")
        + packet_in("pubrel", 2, 2, 2, "PUBREL", PUBREL_HEX)
        + persist("release", 2, 2, OP_RELEASE, "ReleaseInbound", "pubrel.in", 1)
        + committed("release", 2, 2, OP_RELEASE, "ReleaseInbound")
        + [delivered("release", 2, 2, OP_RELEASE)]
        + ack_out("pubcomp", 2, 2, 4, "PUBCOMP", PUBCOMP_HEX, "release.delivered")
        + [inspected("final")]
    )


FIRST = (OP_ADMIT, PUB, PAYLOAD_ID, 1)


def p1_02_snapshots(run):
    return {
        "after_commit_before_crash": snapshot(run, "after_commit_before_crash", [FIRST]),
        "final": snapshot(run, "final", [FIRST], [(OP_RELEASE, 1)], open_exchange=False, epoch=2),
    }


def p1_02b():
    """P1-02b: no crash; a held command, a held unknown answer and two deadlines."""
    resolve_1, resolve_2 = 3, 5
    return (
        [header("P1-02b")]
        + recovery(1, BOOT_FACTS, "boot")
        + connect("c1", 1, 1, 0)
        + packet_in("publish", 1, 1, 1, "PUBLISH", PUBLISH_HEX, dup=False)
        + persist("admit", 1, 1, OP_ADMIT, "AdmitPublication", "publish.in", 1)
        + [
            fault_hit("held_admission", "store.command.transit", "admit.submitted"),
            # The first deadline fires: the core resolves and arms the next one.
            ev(
                "deadline.1",
                "broker-core",
                "DeadlineDelivered",
                {"token": "1", "now": "30"},
                ["admit.armed"],
            ),
            ev(
                "resolve.1",
                "store-adapter",
                "ResolveSubmitted",
                {"effect_id": effect(1, resolve_1), "operation_id": OP_ADMIT},
                ["deadline.1"],
            ),
            ev(
                "armed.2",
                "timer",
                "DeadlineArmed",
                {"effect_id": effect(1, resolve_1 + 1), "token": "2", "after": "30"},
                ["deadline.1"],
            ),
            # The command is still held: no fence can be established.
            ev(
                "unknown.ready",
                "store-evidence",
                "StoreResultReady",
                {
                    "effect_id": effect(1, resolve_1),
                    "operation_id": OP_ADMIT,
                    "outcome": "PendingOrUnknown",
                },
                ["resolve.1"],
            ),
            fault_hit(
                "held_unknown",
                "store.result.ready",
                "unknown.ready",
                effect_id=effect(1, resolve_1),
            ),
            ev(
                "released",
                "fault-injector",
                "HeldItemReleased",
                {"fault_id": "held_admission"},
                ["hit.held_admission"],
            ),
        ]
        + committed("admit", 1, 1, OP_ADMIT, "AdmitPublication", cause="released")
        + [
            fault_hit("lost_admission_result", "store.result.ready", "admit.ready"),
            inspected("after_commit"),
            ev(
                "deadline.2",
                "broker-core",
                "DeadlineDelivered",
                {"token": "2", "now": "60"},
                ["armed.2"],
            ),
            ev(
                "resolve.2",
                "store-adapter",
                "ResolveSubmitted",
                {"effect_id": effect(1, resolve_2), "operation_id": OP_ADMIT},
                ["deadline.2"],
            ),
            ev(
                "armed.3",
                "timer",
                "DeadlineArmed",
                {"effect_id": effect(1, resolve_2 + 1), "token": "3", "after": "30"},
                ["deadline.2"],
            ),
            ev(
                "resolved.ready",
                "store-evidence",
                "StoreResultReady",
                {
                    "effect_id": effect(1, resolve_2),
                    "operation_id": OP_ADMIT,
                    "outcome": "Committed",
                },
                ["resolve.2"],
            ),
            delivered("resolved", 1, resolve_2, OP_ADMIT),
        ]
        + ack_out("pubrec", 1, 1, 7, "PUBREC", PUBREC_HEX, "resolved.delivered")
        + [
            # The old unknown answer arrives after the commit is known.
            ev(
                "unknown.released",
                "fault-injector",
                "HeldItemReleased",
                {"fault_id": "held_unknown"},
                ["hit.held_unknown"],
            ),
            delivered(
                "unknown",
                1,
                resolve_1,
                OP_ADMIT,
                outcome="PendingOrUnknown",
                cause="unknown.released",
            ),
        ]
        + packet_in("pubrel", 1, 1, 2, "PUBREL", PUBREL_HEX)
        + persist("release", 1, 8, OP_RELEASE_B, "ReleaseInbound", "pubrel.in", 4)
        + committed("release", 1, 8, OP_RELEASE_B, "ReleaseInbound")
        + [delivered("release", 1, 8, OP_RELEASE_B)]
        + ack_out("pubcomp", 1, 1, 10, "PUBCOMP", PUBCOMP_HEX, "release.delivered")
        + [inspected("final")]
    )


def p1_02b_snapshots(run):
    return {
        "after_commit": snapshot(run, "after_commit", [FIRST]),
        "final": snapshot(run, "final", [FIRST], [(OP_RELEASE_B, 1)], open_exchange=False),
    }


# ------------------------------------------------------------------- rendering
def render(run, events, snapshots):
    """Number the events: collector sequence, sequence per origin, causes."""
    keys, counters, records = {}, {}, []
    for position, event in enumerate(
        events + [ev("footer", "bench-supervisor", "TraceFooter", {})]
    ):
        origin = (event["origin"], event["inc"])
        sequence = counters.get(origin, 0)
        counters[origin] = sequence + 1
        keys[event["label"]] = (event["origin"], event["inc"], sequence)
        payload = copy.deepcopy(event["payload"])
        if event["kind"] == "DurableStateInspected":
            name = payload["checkpoint"]
            data = dump(snapshots[name])
            payload["snapshot"] = {
                "path": f"{BASE}/{run}/{name}.snapshot.json",
                "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": str(len(data)),
            }
        if event["kind"] == "TraceFooter":
            payload = {"record_count": str(len(events) + 1), "complete": True}
        records.append(
            {
                "format": "mqttiumd.trace-record",
                "schema_version": 1,
                "run_id": run,
                "record_sequence": str(position),
                "origin_component": event["origin"],
                "origin_incarnation": str(event["inc"]),
                "origin_sequence": str(sequence),
                "causal_parents": [
                    {
                        "origin_component": keys[label][0],
                        "origin_incarnation": str(keys[label][1]),
                        "origin_sequence": str(keys[label][2]),
                    }
                    for label in event["parents"]
                ],
                "time_domain": "logical-step",
                "time_value": str(position),
                "kind": event["kind"],
                "payload": payload,
            }
        )
    return records


def lines(records) -> bytes:
    return b"".join(json.dumps(record).encode() + b"\n" for record in records)


def case(
    name,
    why,
    expected=None,
    *,
    base=p1_02,
    scenario="p1-02",
    events=lambda events: None,
    snapshots=lambda snapshots: None,
    raw=lambda records: None,
    data=None,
    files=lambda files: None,
):
    """One trace of the corpus.

    ``events`` edits the history before it is numbered, ``snapshots`` the
    store states, ``raw`` the numbered records, ``data`` the bytes of the
    file and ``files`` the snapshot files written beside it.
    """
    run = name
    history = base()
    events(history)
    states = (p1_02_snapshots if base is p1_02 else p1_02b_snapshots)(run)
    snapshots(states)
    records = render(run, history, states)
    raw(records)
    content = lines(records)
    OUTPUTS[f"{run}/trace.jsonl"] = data(content) if data else content
    written = {
        f"{run}/{checkpoint}.snapshot.json": dump(state) for checkpoint, state in states.items()
    }
    files(written)
    OUTPUTS.update(written)
    statuses = dict.fromkeys(ASSERTIONS, "PASS")
    if base is p1_02b:
        statuses["no_pubrec_observed_before_crash"] = "OUT_OF_SCOPE"
    statuses.update(expected or {})
    INDEX.append(
        {
            "name": name,
            "scenario": f"{BASE}/scenarios/{scenario}.json",
            "trace": f"{BASE}/{run}/trace.jsonl",
            "expected": statuses,
            "why": why,
        }
    )


def at(events, label):
    return next(index for index, event in enumerate(events) if event["label"] == label)


def move(events, label, before):
    """Move one event just before another, without touching its causes."""
    event = events.pop(at(events, label))
    events.insert(at(events, before), event)


def insert_after(events, label, new):
    index = at(events, label) + 1
    events[index:index] = new


ALL_INCONCLUSIVE = dict.fromkeys(ASSERTIONS, "INCONCLUSIVE")

# ----------------------------------------------------------------- good traces
case("p1-02-good", "The history of P1-02 the specification describes.")


def other_interleaving(events):
    # Records of unrelated origins arrive in another order; every cause
    # still precedes its consequence and each origin keeps its own order.
    move(events, "admit.armed", "admit.ready")
    move(events, "restart", "c1.close")
    move(events, "release.armed", "release.ready")


case(
    "p1-02-good-other-interleaving",
    "The same history with independent records interleaved differently: also accepted.",
    events=other_interleaving,
)
case(
    "p1-02b-good",
    "P1-02b: an old unknown answer arrives after the commit is known, and nothing follows it.",
    base=p1_02b,
    scenario="p1-02b",
)

# ------------------------------------------------------------ incomplete traces
case(
    "trace-cut-before-footer",
    "The trace was cut: no footer. Nothing is concluded from it.",
    ALL_INCONCLUSIVE,
    raw=lambda records: records.pop(),
)
case(
    "trace-cut-inside-a-record",
    "The last line was cut in the middle of a record.",
    ALL_INCONCLUSIVE,
    data=lambda content: content[:-40],
)


def drop_record(label_kind):
    def edit(records):
        index = next(i for i, record in enumerate(records) if record["kind"] == label_kind)
        del records[index]

    return edit


case(
    "trace-record-missing",
    "A record is missing: the collector sequence and one origin sequence have a gap.",
    ALL_INCONCLUSIVE,
    raw=drop_record("FrameWriteProgress"),
)


def renumber(records):
    for position, record in enumerate(records):
        record["record_sequence"] = str(position)
    records[-1]["payload"]["record_count"] = str(len(records))


def drop_and_renumber(kind):
    def edit(records):
        drop_record(kind)(records)
        renumber(records)

    return edit


case(
    "trace-origin-gap",
    "The collector sequence is gapless and the footer agrees, yet one origin skips a number.",
    ALL_INCONCLUSIVE,
    raw=drop_and_renumber("StoreResultReady"),
)


def orphan(records):
    record = next(record for record in records if record["kind"] == "FaultHit")
    record["causal_parents"][0]["origin_sequence"] = "99"


case(
    "trace-cause-missing",
    "A record names a cause that is not in the trace.",
    ALL_INCONCLUSIVE,
    raw=orphan,
)
case(
    "trace-footer-count-wrong",
    "The footer counts more records than the trace holds.",
    ALL_INCONCLUSIVE,
    raw=lambda records: records[-1]["payload"].update(record_count=str(len(records) + 1)),
)
case(
    "trace-footer-not-complete",
    "The collector itself says the trace is not complete.",
    ALL_INCONCLUSIVE,
    raw=lambda records: records[-1]["payload"].update(complete=False),
)


def consequence_first(records):
    first = next(i for i, record in enumerate(records) if record["kind"] == "ClientBytesReceived")
    records[first], records[first + 1] = records[first + 1], records[first]
    renumber(records)


case(
    "trace-consequence-before-cause",
    "In a simulated run the core consumes a packet before the transport received its bytes.",
    ALL_INCONCLUSIVE,
    raw=consequence_first,
)


def claim_origin(records):
    record = next(record for record in records if record["kind"] == "RecoveryPageRead")
    record["origin_component"] = "bench-supervisor"


case(
    "trace-record-claims-another-origin",
    "A recovery page is stated by the bench: the record breaks the origin rule and the trace is unreadable.",
    ALL_INCONCLUSIVE,
    raw=claim_origin,
)
case(
    "trace-of-another-scenario",
    "A complete trace of P1-02b is offered for P1-02: a run is judged only against its own scenario.",
    ALL_INCONCLUSIVE,
    base=p1_02b,
    scenario="p1-02",
)

# --------------------------------------------------------------- one admission
SECOND = (OP_SECOND, PUB_SECOND, PAYLOAD_SECOND, 2)


def second_admission(events):
    # DEM-MU-04: at the replay a new admission hides behind a fresh
    # ExchangeId. The client lived one exchange.
    insert_after(
        events,
        "replay.in",
        persist("again", 2, 5, OP_SECOND, "AdmitPublication", "replay.in", 2)
        + committed("again", 2, 5, OP_SECOND, "AdmitPublication")
        + [delivered("again", 2, 5, OP_SECOND)],
    )


def second_admission_state(states):
    states["final"] = snapshot(
        "x", "final", [FIRST, SECOND], [(OP_RELEASE, 2)], open_exchange=False, epoch=2
    )


def with_run(edit):
    """Snapshots carry the run identifier of their trace."""

    def apply(states):
        run = next(iter(states.values()))["run_id"]
        edit(states)
        for state in states.values():
            state["run_id"] = run

    return apply


case(
    "admission-hidden-behind-a-fresh-exchange",
    "DEM-MU-04: the replay is admitted again under a new ExchangeId; one client exchange has two admissions.",
    {"one_admission_per_observed_inbound_exchange": "FAIL"},
    events=second_admission,
    snapshots=with_run(second_admission_state),
)


def admission_before_publish(events):
    for label in ("admit.stable", "admit.ready"):
        events.append(events.pop(at(events, label)))
    move(events, "admit.stable", "publish")
    move(events, "admit.ready", "publish")
    events[at(events, "admit.stable")]["parents"] = []
    events[at(events, "hit.lost_admission_result")]["parents"] = ["admit.submitted"]


case(
    "admission-outside-any-exchange",
    "The store commits an admission of Packet Identifier 7 before the client sent any PUBLISH.",
    {"one_admission_per_observed_inbound_exchange": "FAIL"},
    events=admission_before_publish,
)

# ----------------------------------------------------------------- obligations


def retarget(states):
    states["final"]["obligations"][1]["session"] = str(PRODUCER)


case(
    "obligation-destination-changed",
    "DEM-MU-06: two obligations at both checkpoints, but one now targets another session.",
    {"same_required_obligations_at_both_checkpoints": "FAIL"},
    snapshots=retarget,
)
case(
    "obligation-gone",
    "An obligation held at the first checkpoint is absent at the second.",
    {"same_required_obligations_at_both_checkpoints": "FAIL"},
    snapshots=lambda states: states["final"]["obligations"].pop(),
)


def misrouted(states):
    for state in states.values():
        state["obligations"][1]["session"] = str(PRODUCER)


case(
    "obligation-not-the-required-one",
    "The same two obligations at both checkpoints, and one is not towards a target of the route.",
    {"same_required_obligations_at_both_checkpoints": "FAIL"},
    snapshots=misrouted,
)


def other_payload(states):
    states["final"]["publications"][0]["payload"]["sha256"] = hashlib.sha256(b"other").hexdigest()


case(
    "obligation-payload-changed",
    "The obligations keep their identities and destinations; the payload they refer to changed.",
    {"same_required_obligations_at_both_checkpoints": "FAIL"},
    snapshots=other_payload,
)

# ---------------------------------------------------------------------- recipe


def other_recipe(states):
    states["final"]["operations"][0]["recipe_hex"] = admit_recipe(
        OP_ADMIT, PUB, PAYLOAD_ID, 1, targets=((B, 1),)
    )


case(
    "recipe-changed-under-its-identity",
    "The recipe stored for op_A has other targets at the second checkpoint.",
    {"same_recipe_for_op_A": "FAIL"},
    snapshots=other_recipe,
)
case(
    "recipe-lost",
    "op_A was committed before the second inspection, which holds no recipe for it.",
    {"same_recipe_for_op_A": "FAIL"},
    snapshots=lambda states: states["final"]["operations"].pop(0),
)

# ------------------------------------------------------- positive acknowledgement


def early_pubrec(events):
    # The store had committed, but the core never received that result.
    insert_after(
        events,
        "hit.lost_admission_result",
        [
            ev(
                "early",
                "broker-core",
                "FrameEligible",
                {
                    "effect_id": effect(1, 3),
                    "connection": {"session": str(PRODUCER), "generation": "1"},
                    "packet_kind": "PUBREC",
                    "packet_id": 7,
                    "reason": "Success",
                },
                ["publish.in"],
            )
        ],
    )


case(
    "pubrec-eligible-on-a-hidden-commit",
    "The store had committed and the core had not been told: the PUBREC is premature.",
    {"no_positive_ack_before_confirmed_commit": "FAIL"},
    events=early_pubrec,
)


def pubcomp_first(events):
    move(events, "pubcomp", "release.delivered")
    events[at(events, "pubcomp")]["parents"] = ["pubrel.in"]


case(
    "pubcomp-eligible-before-its-result",
    "PUBCOMP is made eligible before the core received the result of the release.",
    {"no_positive_ack_before_confirmed_commit": "FAIL"},
    events=pubcomp_first,
)

# ---------------------------------------------------------- PUBREC before crash


def acknowledged_before_crash(events):
    insert_after(
        events,
        "hit.lost_admission_result",
        [delivered("admit", 1, 1, OP_ADMIT)]
        + ack_out("first", 1, 1, 3, "PUBREC", PUBREC_HEX, "admit.delivered"),
    )


case(
    "pubrec-observed-before-the-crash",
    "The result was not lost: the client observed PUBREC before the crash.",
    {"no_pubrec_observed_before_crash": "FAIL"},
    events=acknowledged_before_crash,
)

# ----------------------------------------------------------------------- faults
case(
    "fault-hit-twice",
    "The injection was reached twice; once was announced.",
    {"all_required_faults_hit": "INCONCLUSIVE"},
    events=lambda events: insert_after(
        events,
        "hit.lost_admission_result",
        [fault_hit("lost_admission_result", "store.result.ready", "admit.ready", occurrence=2)],
    ),
)

# ------------------------------------------------------------------- provenance
case(
    "provenance-result-answers-no-effect",
    "DEM-MU-03: after the restart the core receives a result for op_A that answers no effect of this incarnation.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=lambda events: insert_after(
        events,
        "recovery.in",
        [delivered("injected", 2, 1, OP_ADMIT, cause="recovery.in", label="injected")],
    ),
)


def remembered_fact(events):
    events[at(events, "recovery.in")]["payload"]["facts"].append(
        {"kind": "Operation", "operation_id": OP_RELEASE, "knowledge": "Unknown"}
    )


def deepcopy_payloads(events):
    for event in events:
        event["payload"] = copy.deepcopy(event["payload"])


def remembered(events):
    deepcopy_payloads(events)
    remembered_fact(events)


case(
    "provenance-page-holds-a-fact-nobody-read",
    "DEM-MU-03: the page the core consumed holds a fact the store adapter did not read.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=remembered,
)
case(
    "provenance-input-refused",
    "The dispatcher refused a result that answered no effect in flight.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=lambda events: insert_after(
        events,
        "recovery.in",
        [
            ev(
                "refused",
                "dispatcher",
                "InputRefused",
                {
                    "input": "StoreResult",
                    "effect_id": effect(1, 1),
                    "operation_id": OP_ADMIT,
                    "reason": "no-effect-in-flight",
                },
                [],
                2,
            )
        ],
    ),
)
case(
    "provenance-deadline-never-armed",
    "The core consumes a deadline whose token no timer armed.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=lambda events: insert_after(
        events,
        "recovery.in",
        [ev("fired", "broker-core", "DeadlineDelivered", {"token": "9", "now": "30"}, [], 2)],
    ),
)
case(
    "provenance-deadline-delivered-twice",
    "The first deadline of P1-02b is delivered a second time.",
    {"broker_inputs_have_provenance": "FAIL"},
    base=p1_02b,
    scenario="p1-02b",
    events=lambda events: insert_after(
        events,
        "armed.2",
        [
            ev(
                "again",
                "broker-core",
                "DeadlineDelivered",
                {"token": "1", "now": "31"},
                ["admit.armed"],
            )
        ],
    ),
)


def unsourced_packet(events):
    events[at(events, "replay.in")]["parents"] = []


case(
    "provenance-packet-without-bytes",
    "The core consumes a PUBLISH that follows no bytes of the transport.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=unsourced_packet,
)
case(
    "provenance-result-delivered-twice",
    "One effect is answered twice.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=lambda events: insert_after(
        events, "release.delivered", [delivered("release", 2, 2, OP_RELEASE, label="twice")]
    ),
)

# -------------------------------------------------------------------- snapshots


def corrupt(files):
    name = next(name for name in files if name.endswith("final.snapshot.json"))
    files[name] = files[name].replace(b'"Queued"', b'"Queued" ')


case(
    "snapshot-not-the-cited-file",
    "The final snapshot is not the file the trace cites: what depends on it is not concluded.",
    {
        "same_required_obligations_at_both_checkpoints": "INCONCLUSIVE",
        "same_recipe_for_op_A": "INCONCLUSIVE",
    },
    files=corrupt,
)

OUTPUTS["index.json"] = dump({"traces": INDEX})


def main() -> int:
    check = sys.argv[1:] == ["--check"]
    if sys.argv[1:] and not check:
        print(__doc__, file=sys.stderr)
        return 2
    present = (
        {path.relative_to(OUT).as_posix() for path in OUT.rglob("*") if path.is_file()}
        if OUT.is_dir()
        else set()
    )
    stale = sorted(present - set(OUTPUTS))
    changed = sorted(
        name
        for name, data in OUTPUTS.items()
        if not (OUT / name).is_file() or (OUT / name).read_bytes() != data
    )
    if check:
        for name in changed:
            print(f"not up to date: {name}", file=sys.stderr)
        for name in stale:
            print(f"not written by this tool: {name}", file=sys.stderr)
        print(f"{len(OUTPUTS)} files, {len(changed)} not up to date, {len(stale)} stale")
        return 1 if changed or stale else 0
    for name in stale:
        (OUT / name).unlink()
    for name in changed:
        target = OUT / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(OUTPUTS[name])
    for directory in sorted(OUT.rglob("*"), reverse=True):
        if directory.is_dir() and not any(directory.iterdir()):
            shutil.rmtree(directory)
    print(f"{len(OUTPUTS)} files, {len(changed)} rewritten, {len(stale)} removed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
