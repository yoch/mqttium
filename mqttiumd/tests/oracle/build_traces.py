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
import itertools
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
QABS_ASSERTIONS = [
    "one_admission_per_observed_inbound_exchange",
    "no_positive_ack_before_confirmed_commit",
    "fenced_operation_never_executed",
    "certain_rejection_has_a_closure",
    "all_required_faults_hit",
    "trace_complete",
    "broker_inputs_have_provenance",
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
# The same scenario with a second injection: the result of the release is
# duplicated, and the dispatcher refuses the copy.
P1_02_DUPLICATE = copy.deepcopy(P1_02)
P1_02_DUPLICATE["faults"].append(
    {
        "id": "duplicated_release_result",
        "point": "store.result.ready",
        "match": {"command_kind": "ReleaseInbound"},
        "occurrence": "1",
        "action": "duplicate_result",
        "expected_hits": "1",
    }
)
OUTPUTS["scenarios/p1-02-duplicate.json"] = dump(P1_02_DUPLICATE)
# P1-02 for a run that drives the core with validated packets: it requires
# that perimeter and lists what it can decide without peers.
PEER_ASSERTIONS = (
    "one_admission_per_observed_inbound_exchange",
    "no_pubrec_observed_before_crash",
)
CORE_ASSERTIONS = [name for name in ASSERTIONS if name not in PEER_ASSERTIONS]
P1_02_CORE = copy.deepcopy(P1_02)
P1_02_CORE.update(perimeter="core-without-bytes", assertions=CORE_ASSERTIONS)
P1_02_CORE_WITH_PEER = copy.deepcopy(P1_02_CORE)
P1_02_CORE_WITH_PEER["assertions"] = [*CORE_ASSERTIONS, "no_pubrec_observed_before_crash"]
OUTPUTS["scenarios/p1-02-core.json"] = dump(P1_02_CORE)
OUTPUTS["scenarios/p1-02-core-with-a-peer-assertion.json"] = dump(P1_02_CORE_WITH_PEER)
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
            # The fixture's table is at revision 2: one per AddRoute.
            be(2) + be(2, 1) + be(0, 1),
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
    exchange = lambda generation: {"session": str(PRODUCER), "generation": str(generation)}  # noqa: E731
    return {
        "format": "mqttiumd.store-snapshot",
        "schema_version": 1,
        "run_id": run,
        "checkpoint": checkpoint,
        "backend": "sim-store",
        "meta": {
            "incarnation": "1",
            "last_opened_epoch": str(epoch),
            "opened_by": {"startup_hex": f"{epoch:02x}" * 16, "ticket": "1"},
            "collected_through": "0",
            "state_revision": str(len(admissions) + len(releases)),
            "next_session": "4",
        },
        "sessions": [
            {
                "session": str(session),
                "client_id_hex": name.encode().hex(),
                "revision": "1",
                "state": "Active",
            }
            for session, name in CLIENT_IDS.items()
        ],
        "route_table": {
            "revision": "2",
            "routes": [
                {
                    "topic_hex": TOPIC.hex(),
                    "targets": [{"session": str(B), "qos": 1}, {"session": str(C), "qos": 1}],
                }
            ],
        },
        "operations": sorted(
            [
                {
                    "operation_id": operation,
                    "recipe_hex": admit_recipe(operation, publication, payload, generation),
                    "decision": {
                        "kind": "Committed",
                        "facts": {
                            "kind": "PublicationAdmitted",
                            "publication_id": publication,
                            "exchange": exchange(generation),
                        },
                    },
                }
                for operation, publication, payload, generation in admissions
            ]
            + [
                {
                    "operation_id": operation,
                    "recipe_hex": release_recipe(operation, generation),
                    "decision": {
                        "kind": "Committed",
                        "facts": {"kind": "InboundReleased", "exchange": exchange(generation)},
                    },
                }
                for operation, generation in releases
            ],
            key=lambda row: [int(part) for part in row["operation_id"].values()],
        ),
        "fences": [],
        "payloads": [
            {
                "payload_id": payload,
                "length": str(len(PAYLOAD)),
                "sha256": hashlib.sha256(PAYLOAD).hexdigest(),
                "references": "2",
            }
            for _, _, payload, _ in admissions
        ],
        "publications": [
            {
                "publication_id": publication,
                "operation_id": operation,
                "source_session": str(PRODUCER),
                "topic_hex": TOPIC.hex(),
                "qos": 2,
                "retain": False,
                "payload_id": payload,
            }
            for operation, publication, payload, _ in admissions
        ],
        "obligations": [
            {
                "publication_id": publication,
                "index": index,
                "session": str(target),
                "target_revision": "1",
                "cause": "LiveExact",
                "effective_qos": 1,
                "phase": "Queued",
            }
            for _, publication, _, _ in admissions
            for index, target in enumerate((B, C))
        ],
        "inbound_exchanges": [
            {
                "exchange": exchange(admissions[-1][3]),
                "packet_id": 7,
                "publication_id": admissions[-1][1],
                "phase": "AwaitPubrel",
            }
        ]
        if open_exchange
        else [],
        "account": {
            "used": {
                "payload_bytes": str(len(PAYLOAD) * len(admissions)),
                "references": str(2 * len(admissions)),
                "exchange_slots": "1" if open_exchange else "0",
                "receipt_slots": str(len(admissions) + len(releases)),
            },
            "limits": {
                "payload_bytes": "4096",
                "references": "16",
                "exchange_slots": "4",
                "receipt_slots": "8",
                "max_targets_per_admission": "4",
                "max_topic_bytes": "256",
                "max_payload_bytes": "1024",
                "max_sessions": "8",
                "max_route_targets": "8",
            },
        },
    }


# ---------------------------------------------------------------------- events
_CONNECT_BODY = bytes.fromhex("00044d5154540500000a00") + b"\x00\x0bp1-producer"
CONNECT_HEX = (bytes([0x10, len(_CONNECT_BODY)]) + _CONNECT_BODY).hex()
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


def result(effect_id, operation, outcome):
    """The payload of a result; a certain rejection of this corpus is a fence."""
    payload = {"effect_id": effect_id, "operation_id": operation, "outcome": outcome}
    if outcome == "RejectedCertain":
        payload["reject_reason"] = "Fenced"
    return payload


def delivered(tag, inc, effect_sequence, operation, outcome="Committed", cause=None, label=None):
    return ev(
        label or f"{tag}.delivered",
        "broker-core",
        "StoreResultDelivered",
        result(effect(inc, effect_sequence), operation, outcome),
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
        {"scenario_id": scenario, "backend": "sim-store", "perimeter": "full"},
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


def route_fact(topic, target, qos=1):
    return {
        "kind": "Route",
        "topic_hex": topic.hex(),
        "target": str(target),
        "target_revision": "1",
        "max_qos": qos,
    }


# What a core needs to prepare an admission (record 0014): the bounds, the
# revision of the route table, and its routes in the order of their topic.
ROUTE_FACTS = [
    {
        "kind": "Limits",
        "max_targets_per_admission": "4",
        "max_topic_bytes": "256",
        "max_payload_bytes": "1024",
    },
    {"kind": "RouteTable", "revision": "2"},
    route_fact(TOPIC, B),
    route_fact(TOPIC, C),
]
BOOT_FACTS = [{"kind": "Epoch", "incarnation": "1", "epoch": "1"}, *ROUTE_FACTS, *SESSION_FACTS]


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
            + ROUTE_FACTS
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
            # The resolution read the absence; an injection refuses its fence.
            fault_hit(
                "refused_fence",
                "store.fence.transit",
                "resolve.1",
                effect_id=effect(1, resolve_1),
            ),
            ev(
                "fence.refused",
                "store-evidence",
                "StoreFenceRefused",
                {"effect_id": effect(1, resolve_1), "operation_id": OP_ADMIT},
                ["hit.refused_fence"],
            ),
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
    assertions=ASSERTIONS,
    states_of=None,
    scenario_path=None,
):
    """One trace of the corpus.

    ``events`` edits the history before it is numbered, ``snapshots`` the
    store states, ``raw`` the numbered records, ``data`` the bytes of the
    file and ``files`` the snapshot files written beside it.
    """
    run = name
    history = base()
    events(history)
    states = (states_of or (p1_02_snapshots if base is p1_02 else p1_02b_snapshots))(run)
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
    statuses = dict.fromkeys(assertions, "PASS")
    if base is p1_02b:
        statuses["no_pubrec_observed_before_crash"] = "OUT_OF_SCOPE"
    statuses.update(expected or {})
    INDEX.append(
        {
            "name": name,
            "scenario": scenario_path or f"{BASE}/scenarios/{scenario}.json",
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


def drop_admission_commit(records):
    index = next(
        i
        for i, record in enumerate(records)
        if record["kind"] == "StoreCommitStable"
        and record["payload"]["command_kind"] == "AdmitPublication"
    )
    del records[index]


case(
    "trace-missing-the-commit-of-the-admission",
    "The missing record is the commit: an acknowledgement without admission is not concluded.",
    ALL_INCONCLUSIVE,
    raw=drop_admission_commit,
)


def drop_first_publish(records):
    index = next(
        i
        for i, record in enumerate(records)
        if record["kind"] == "PeerPacketSent" and record["payload"]["packet_kind"] == "PUBLISH"
    )
    del records[index]


case(
    "trace-missing-the-publish-of-the-client",
    "The missing record is the PUBLISH: an admission outside any exchange is not concluded.",
    ALL_INCONCLUSIVE,
    raw=drop_first_publish,
)
case(
    "trace-origin-gap",
    "The collector sequence is gapless and the footer agrees, yet one origin skips a number.",
    ALL_INCONCLUSIVE,
    raw=drop_and_renumber("PeerConnectionClosed"),
)


def collector_gap(records):
    # Every origin is gapless and the footer counts what the file holds; only
    # the collector skipped a number.
    for record in records[10:]:
        record["record_sequence"] = str(int(record["record_sequence"]) + 1)


case(
    "trace-collector-gap",
    "The collector sequence skips a number: a record did not reach the file.",
    ALL_INCONCLUSIVE,
    raw=collector_gap,
)


def not_a_record(records):
    record = next(record for record in records if record["kind"] == "FaultHit")
    record["payload"]["note"] = "extra"


case(
    "trace-record-not-valid",
    "One line is not a valid record: its sequences are in order, its payload is not of its kind.",
    ALL_INCONCLUSIVE,
    raw=not_a_record,
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

case(
    "admission-twice-on-a-cut-trace",
    "The trace is cut, and what it holds already shows two admissions: no missing record undoes that.",
    {**ALL_INCONCLUSIVE, "one_admission_per_observed_inbound_exchange": "FAIL"},
    events=second_admission,
    snapshots=with_run(second_admission_state),
    raw=lambda records: records.pop(),
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


def never_admitted(events):
    for label in ("admit.stable", "admit.ready"):
        del events[at(events, label)]
    events[at(events, "hit.lost_admission_result")]["parents"] = ["admit.submitted"]


case(
    "acknowledged-without-admission",
    "The client observes a positive PUBREC for an exchange the store never admitted.",
    {"one_admission_per_observed_inbound_exchange": "FAIL"},
    events=never_admitted,
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
    states["final"]["payloads"][0]["sha256"] = hashlib.sha256(b"other").hexdigest()


case(
    "obligation-payload-changed",
    "The obligations keep their identities and destinations; the payload they refer to changed.",
    {"same_required_obligations_at_both_checkpoints": "FAIL"},
    snapshots=other_payload,
)


def no_obligation(states):
    for state in states.values():
        state["obligations"] = []
        state["publications"] = []


case(
    "obligation-none-held",
    "Both checkpoints agree, on nothing: the store holds no publication and no obligation.",
    {"same_required_obligations_at_both_checkpoints": "FAIL"},
    snapshots=no_obligation,
)


def one_more(states):
    extra = dict(states["final"]["obligations"][1], index=6)
    states["final"]["obligations"].append(extra)


case(
    "obligation-appeared",
    "The second checkpoint holds one obligation more for the same publication: not the same obligations.",
    {"same_required_obligations_at_both_checkpoints": "FAIL"},
    snapshots=one_more,
)


def no_session_row(states):
    for state in states.values():
        del state["sessions"][2]


case(
    "obligation-towards-a-session-the-store-lost",
    "The store holds no session for a target of the route: a finding, not a crash of the judge.",
    {"same_required_obligations_at_both_checkpoints": "FAIL"},
    snapshots=no_session_row,
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


def on_sqlite(edit=None):
    def apply(states):
        for state in states.values():
            state["backend"] = "sqlite"
        if edit is not None:
            edit(states)

    return apply


def sqlite_header(events):
    events[0]["payload"] = dict(events[0]["payload"], backend="sqlite")


case(
    "recipe-lost-on-a-store-without-total-order",
    "On a backend with no total order, the trace does not say whether op_A was committed before the inspection that lost its recipe.",
    {
        "same_recipe_for_op_A": "INCONCLUSIVE",
        # Nor whether the admission falls inside its exchange: an exchange
        # whose admission cannot be placed does not lack one.
        "one_admission_per_observed_inbound_exchange": "INCONCLUSIVE",
        "no_pubrec_observed_before_crash": "INCONCLUSIVE",
    },
    events=sqlite_header,
    snapshots=on_sqlite(lambda states: states["final"]["operations"].pop(0)),
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


def in_pages(*edit):
    """The recovery of the second incarnation, read and delivered as several pages.

    ``edit`` changes the facts before they are cut; the cuts are the indexes
    where a new page starts. What the core is delivered is what the adapter
    read: only the inventory is at fault, when it is.
    """
    *changes, cuts = edit

    def apply(events):
        deepcopy_payloads(events)
        first = at(events, "recovery.read")
        facts = events[first]["payload"]["facts"]
        for change in changes:
            change(facts)
        bounds = [0, *cuts, len(facts)]
        pages = []
        for number, (start, end) in enumerate(itertools.pairwise(bounds)):
            content = {
                "run_epoch": "2",
                "page": str(number),
                "last": end == len(facts),
                "facts": facts[start:end],
            }
            read = f"recovery.read.{number}"
            pages += [
                ev(read, "store-adapter", "RecoveryPageRead", content, [], 2),
                ev(
                    f"recovery.in.{number}",
                    "broker-core",
                    "RecoveryPageDelivered",
                    content,
                    [read],
                    2,
                ),
            ]
        events[first : first + 2] = pages

    return apply


# Facts of the second recovery: 0 Epoch, 1 Limits, 2 RouteTable, 3 and 4
# Route, 5 to 7 Session, 8 InboundExchange, 9 Operation.
case(
    "recovery-inventory-over-three-pages",
    "The inventory of a recovery is cut inside its routes and inside its sessions: still one ordered inventory.",
    events=in_pages([4, 6]),
)
case(
    "recovery-routes-unsorted-across-pages",
    "Each page is in order, and the first route of the second page sorts before the last of the first.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=in_pages(lambda facts: facts.__setitem__(4, route_fact(b"p1/a", C)), [4]),
)
case(
    "recovery-limits-after-the-routes-of-an-earlier-page",
    "Each page is in order, and the limits come in a page after the routes they bound.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=in_pages(lambda facts: facts.insert(4, facts.pop(1)), [4]),
)
case(
    "recovery-route-table-again-in-a-later-page",
    "Each page is in order, and the second page states the route table a second time.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=in_pages(lambda facts: facts.insert(3, dict(facts[2])), [3]),
)
case(
    "recovery-route-without-its-table",
    "Routes belong to the table that precedes them: an inventory with routes and no table is not one.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=in_pages(lambda facts: facts.pop(2), []),
)


# Rules the core applies to a whole inventory (lot L1): a route is listed
# once, and every route targets a session of the inventory.
case(
    "recovery-route-listed-twice-across-pages",
    "The same route, topic and target, is in two pages of one inventory.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=in_pages(lambda facts: facts.insert(4, dict(facts[3])), [4]),
)
case(
    "recovery-route-to-a-session-the-inventory-lacks",
    "A route targets a session that no fact of the inventory recovers.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=in_pages(lambda facts: facts.insert(5, route_fact(TOPIC, 9)), []),
)


def read_out_of_order(events):
    # Page 0 holds the sessions and page 1 the epoch and the routes. The
    # adapter reads page 1 first, which is an inventory in order; the core
    # is given page 0 first, which is not.
    deepcopy_payloads(events)
    first = at(events, "recovery.read")
    facts = events[first]["payload"]["facts"]
    head, tail = facts[:5], facts[5:]
    pages = [
        {"run_epoch": "2", "page": "0", "last": False, "facts": tail},
        {"run_epoch": "2", "page": "1", "last": True, "facts": head},
    ]
    events[first : first + 2] = [
        ev("recovery.read.1", "store-adapter", "RecoveryPageRead", pages[1], [], 2),
        ev("recovery.read.0", "store-adapter", "RecoveryPageRead", pages[0], [], 2),
        ev(
            "recovery.in.0",
            "broker-core",
            "RecoveryPageDelivered",
            pages[0],
            ["recovery.read.0"],
            2,
        ),
        ev(
            "recovery.in.1",
            "broker-core",
            "RecoveryPageDelivered",
            pages[1],
            ["recovery.read.1"],
            2,
        ),
    ]


case(
    "recovery-delivered-out-of-inventory-order",
    "Read in an order that is an inventory, delivered in one that is not: the core got the sessions before the epoch.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=read_out_of_order,
)


def page_read_twice(events):
    # The adapter reads page 0 again after a transient error; the core is
    # given each page once, in order.
    in_pages([5])(events)
    first = at(events, "recovery.read.0")
    again = dict(events[first], label="recovery.read.0.again")
    events.insert(first + 1, again)


case(
    "recovery-page-read-again",
    "A page read twice by the adapter and delivered once is one inventory, not a second epoch.",
    events=page_read_twice,
)


def lying_observer(events):
    # The peer says it decoded a refusing PUBREC; the bytes it observed are
    # the positive one.
    deepcopy_payloads(events)
    for event in events:
        if event["kind"] == "PeerPacketObserved" and event["payload"]["packet_kind"] == "PUBREC":
            event["payload"]["reason_code"] = 0x83


case(
    "observed-reason-is-not-the-bytes",
    "The reason a peer says it decoded is not the reason in the bytes it observed.",
    {"one_admission_per_observed_inbound_exchange": "INCONCLUSIVE"},
    events=lying_observer,
)


def trailing_byte(events):
    # The bytes the peer observed are more than the one PUBREC it states.
    deepcopy_payloads(events)
    for event in events:
        if event["kind"] == "PeerPacketObserved" and event["payload"]["packet_kind"] == "PUBREC":
            event["payload"]["bytes_hex"] += "00"


case(
    "observed-bytes-are-more-than-the-packet",
    "The bytes a peer observed hold more than the one packet it says it decoded.",
    {"one_admission_per_observed_inbound_exchange": "INCONCLUSIVE"},
    events=trailing_byte,
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


def duplicate_refused(events):
    insert_after(
        events,
        "release.delivered",
        [
            fault_hit(
                "duplicated_release_result",
                "store.result.ready",
                "release.ready",
                operation=OP_RELEASE,
                effect_id=effect(2, 2),
            ),
            ev(
                "copy.refused",
                "dispatcher",
                "InputRefused",
                {
                    "input": "StoreResult",
                    "effect_id": effect(2, 2),
                    "operation_id": OP_RELEASE,
                    "reason": "no-effect-in-flight",
                },
                ["hit.duplicated_release_result"],
                2,
            ),
        ],
    )


case(
    "p1-02-good-duplicate-refused",
    "A declared injection duplicates a result; the dispatcher refuses the copy, and that is the expected outcome.",
    scenario="p1-02-duplicate",
    events=duplicate_refused,
)


def duplicate_delivered(events):
    duplicate_refused(events)
    events[at(events, "copy.refused")] = delivered("release", 2, 2, OP_RELEASE, label="copy.in")


case(
    "provenance-duplicate-reaches-the-core",
    "The duplicated result is not refused: the core consumes a second answer to one effect.",
    {"broker_inputs_have_provenance": "FAIL"},
    scenario="p1-02-duplicate",
    events=duplicate_delivered,
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
        # The recipe of the release is in that snapshot only: without it the
        # result the core received cannot be attributed to the exchange.
        "no_positive_ack_before_confirmed_commit": "INCONCLUSIVE",
    },
    files=corrupt,
)


def reordered_identities(states):
    # The same identities with their members written in another order.
    def turn(value):
        if isinstance(value, dict):
            return {key: turn(value[key]) for key in sorted(value, reverse=True)}
        if isinstance(value, list):
            return [turn(item) for item in value]
        return value

    for name in states:
        states[name] = turn(states[name])


case(
    "p1-02-good-identities-in-another-member-order",
    "An identity is its named components, in whatever order a writer puts the members.",
    snapshots=reordered_identities,
)
case(
    "snapshot-of-another-run",
    "The final snapshot is a valid snapshot of another run: it is not evidence for this one.",
    {
        "same_required_obligations_at_both_checkpoints": "INCONCLUSIVE",
        "same_recipe_for_op_A": "INCONCLUSIVE",
        "no_positive_ack_before_confirmed_commit": "INCONCLUSIVE",
    },
    snapshots=lambda states: states["final"].update(run_id="another-run"),
)
case(
    "snapshot-of-another-store",
    "The final snapshot was read from another backend than the run declares.",
    {
        "same_required_obligations_at_both_checkpoints": "INCONCLUSIVE",
        "same_recipe_for_op_A": "INCONCLUSIVE",
        "no_positive_ack_before_confirmed_commit": "INCONCLUSIVE",
    },
    snapshots=lambda states: states["final"].update(backend="sqlite"),
)


def invented_packets(events):
    # Every packet the core consumed follows bytes the transport says it
    # received, but those bytes are a PINGREQ that no peer sent.
    for event in events:
        if event["kind"] == "ClientBytesReceived":
            event["payload"] = dict(event["payload"], bytes_hex="c000")
            event["parents"] = []


case(
    "provenance-packets-nobody-sent",
    "The transport records bytes no peer sent, and they are not the packets the core consumed.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=invented_packets,
)


def other_bytes(events):
    # Still the PUBLISH the core consumed, without the DUP flag the peer set.
    received = events[at(events, "replay.bytes")]
    received["payload"] = dict(received["payload"], bytes_hex=PUBLISH_HEX)


case(
    "provenance-bytes-are-not-what-the-peer-sent",
    "The transport records other bytes than the peer sent for the packet the core consumed.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=other_bytes,
)


def bytes_of_the_transport_itself(events):
    # The same bytes as the peer sent, with the wrong cause: an earlier
    # record of the transport that carries them too.
    events[at(events, "c2.connect.bytes")]["parents"] = ["c1.connect.bytes"]


case(
    "provenance-bytes-caused-by-no-peer",
    "The bytes the transport received name as their cause a record that is not a packet a peer sent.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=bytes_of_the_transport_itself,
)


def another_kind(events):
    delivered = events[at(events, "replay.in")]
    delivered["payload"] = dict(delivered["payload"], packet_kind="PUBREL")


case(
    "provenance-packet-of-another-kind-than-the-bytes",
    "The peer sent a PUBLISH, the transport received it, and the core consumed a PUBREL.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=another_kind,
)


def another_identifier(events):
    # Peer and transport agree on a PUBREL of Packet Identifier 8; the core
    # consumed one of Packet Identifier 7.
    for label in ("pubrel", "pubrel.bytes"):
        event = events[at(events, label)]
        event["payload"] = dict(event["payload"], bytes_hex="62020008")


case(
    "provenance-packet-of-another-identifier-than-the-bytes",
    "The bytes received carry another Packet Identifier than the packet the core consumed.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=another_identifier,
)


def two_packets(events):
    for label in ("pubrel", "pubrel.bytes"):
        event = events[at(events, label)]
        event["payload"] = dict(event["payload"], bytes_hex=PUBREL_HEX + PUBREL_HEX)


case(
    "provenance-bytes-are-more-than-the-packet",
    "The bytes received are more than the one packet the core consumed.",
    {"broker_inputs_have_provenance": "FAIL"},
    events=two_packets,
)

UNUSABLE_FINAL = {
    "same_required_obligations_at_both_checkpoints": "INCONCLUSIVE",
    "same_recipe_for_op_A": "INCONCLUSIVE",
    "no_positive_ack_before_confirmed_commit": "INCONCLUSIVE",
}
case(
    "snapshot-of-another-checkpoint",
    "The file cited for the final checkpoint is the snapshot of another checkpoint.",
    UNUSABLE_FINAL,
    snapshots=lambda states: states["final"].update(checkpoint="elsewhere"),
)
case(
    "snapshot-checkpoint-inspected-twice",
    "The first checkpoint is inspected twice: neither inspection is the one the scenario asks for.",
    {
        "same_required_obligations_at_both_checkpoints": "INCONCLUSIVE",
        "same_recipe_for_op_A": "INCONCLUSIVE",
    },
    events=lambda events: insert_after(
        events,
        "inspect.after_commit_before_crash",
        [dict(inspected("after_commit_before_crash"), label="inspect.again")],
    ),
)
case(
    "provenance-refusal-not-caused-by-a-duplication",
    "An input is refused after an injection that duplicates nothing: the refusal is not excused.",
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
                ["hit.lost_admission_result"],
                2,
            )
        ],
    ),
)

# -------------------------------------------------------------------- perimeter


def without_bytes(events):
    """The same history when the core is driven with validated packets."""
    gone = {event["label"] for event in events if event["origin"] in ("peer", "transport")}
    events[:] = [event for event in events if event["label"] not in gone]
    for event in events:
        event["parents"] = [label for label in event["parents"] if label not in gone]
        if event["kind"] == "TraceHeader":
            event["payload"] = dict(event["payload"], perimeter="core-without-bytes")


case(
    "core-without-bytes-good",
    "A run without bytes, judged against the scenario of that perimeter.",
    scenario="p1-02-core",
    events=without_bytes,
    assertions=CORE_ASSERTIONS,
)
case(
    "core-without-bytes-against-the-full-scenario",
    "A run without bytes is offered for the full scenario: it never passes it.",
    dict.fromkeys(ASSERTIONS, "OUT_OF_SCOPE"),
    events=without_bytes,
)
case(
    "full-run-against-a-scenario-without-bytes",
    "A full run is offered for the scenario of another perimeter.",
    dict.fromkeys(CORE_ASSERTIONS, "OUT_OF_SCOPE"),
    scenario="p1-02-core",
    assertions=CORE_ASSERTIONS,
)
case(
    "core-without-bytes-lists-a-peer-assertion",
    "A scenario without bytes lists an assertion only peers can decide: it stays out of scope.",
    {"no_pubrec_observed_before_crash": "OUT_OF_SCOPE"},
    scenario="p1-02-core-with-a-peer-assertion",
    events=without_bytes,
    assertions=P1_02_CORE_WITH_PEER["assertions"],
)


def provenance_without_bytes(events):
    without_bytes(events)
    corpus_insert = [delivered("injected", 2, 1, OP_ADMIT, cause="recovery.in", label="injected")]
    insert_after(events, "recovery.in", corpus_insert)


case(
    "core-without-bytes-result-answers-no-effect",
    "Without bytes the provenance of results is still checked.",
    {"broker_inputs_have_provenance": "FAIL"},
    scenario="p1-02-core",
    events=provenance_without_bytes,
    assertions=CORE_ASSERTIONS,
)

# ------------------------------------------------------------------------ QABS
# DEM-018: a command held before the writer, a resolution that reads its
# absence, and the fence of that resolution ordered against the command.
RESOLVE_1 = 3


def refusal_out(tag, cause):
    """The core makes a refusing PUBREC eligible and the client observes it."""
    # No quota is the cause of a fenced admission: the reason is 0x83, the
    # PUBLISH is valid and the receiver does not accept it (record 0013).
    events = ack_out(tag, 1, 1, 6, "PUBREC", "5003000783", cause)
    events[0]["payload"]["reason"] = "ImplementationSpecificError"
    events[2]["payload"]["reason_code"] = 0x83
    return events


def qabs_start(scenario):
    """Up to the hold of the admission and the first deadline."""
    return (
        [header(scenario)]
        + recovery(1, BOOT_FACTS, "boot")
        + connect("c1", 1, 1, 0)
        + packet_in("publish", 1, 1, 1, "PUBLISH", PUBLISH_HEX, dup=False)
        + persist("admit", 1, 1, OP_ADMIT, "AdmitPublication", "publish.in", 1)
        + [
            fault_hit("held_admission", "store.command.transit", "admit.submitted"),
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
                {"effect_id": effect(1, RESOLVE_1), "operation_id": OP_ADMIT},
                ["deadline.1"],
            ),
            ev(
                "armed.2",
                "timer",
                "DeadlineArmed",
                {"effect_id": effect(1, RESOLVE_1 + 1), "token": "2", "after": "30"},
                ["deadline.1"],
            ),
        ]
    )


def released(fault):
    return ev(
        f"released.{fault}",
        "fault-injector",
        "HeldItemReleased",
        {"fault_id": fault},
        [f"hit.{fault}"],
    )


def ready(label, effect_sequence, outcome, cause):
    return ev(
        label,
        "store-evidence",
        "StoreResultReady",
        result(effect(1, effect_sequence), OP_ADMIT, outcome),
        [cause],
    )


def held_fence_hit():
    return fault_hit(
        "held_fence", "store.fence.transit", "resolve.1", effect_id=effect(1, RESOLVE_1)
    )


def qabs_a():
    """The command commits before the fence: the resolution finds it committed."""
    return (
        qabs_start("QABS-A")
        + [
            held_fence_hit(),
            released("held_admission"),
            ev(
                "admit.stable",
                "store-evidence",
                "StoreCommitStable",
                {"operation_id": OP_ADMIT, "command_kind": "AdmitPublication"},
                ["released.held_admission"],
            ),
            fault_hit("admission_applied", "store.commit.applied", "admit.stable"),
            ready("admit.ready", 1, "Committed", "admit.stable"),
            inspected("after_commit"),
            delivered("admit", 1, 1, OP_ADMIT),
            # The held fence enters the writer, reads again and writes nothing.
            released("held_fence"),
            ready("resolved.ready", RESOLVE_1, "Committed", "released.held_fence"),
            delivered("resolved", 1, RESOLVE_1, OP_ADMIT),
        ]
        + ack_out("pubrec", 1, 1, 6, "PUBREC", PUBREC_HEX, "admit.delivered")
        + packet_in("pubrel", 1, 1, 2, "PUBREL", PUBREL_HEX)
        + persist("release", 1, 8, OP_RELEASE_B, "ReleaseInbound", "pubrel.in", 3)
        + committed("release", 1, 8, OP_RELEASE_B, "ReleaseInbound")
        + [delivered("release", 1, 8, OP_RELEASE_B)]
        + ack_out("pubcomp", 1, 1, 10, "PUBCOMP", PUBCOMP_HEX, "release.delivered")
        + [inspected("final")]
    )


def qabs_a_snapshots(run):
    return {
        "after_commit": snapshot(run, "after_commit", [FIRST]),
        "final": snapshot(run, "final", [FIRST], [(OP_RELEASE_B, 1)], open_exchange=False),
    }


def qabs_b():
    """The fence commits before the command: a certain refusal, and no later effect."""
    return (
        qabs_start("QABS-B")
        + [
            held_fence_hit(),
            released("held_fence"),
            ev(
                "fence.stable",
                "store-evidence",
                "StoreFenceStable",
                {"operation_id": OP_ADMIT},
                ["released.held_fence"],
            ),
            ready("fenced.ready", RESOLVE_1, "RejectedCertain", "fence.stable"),
            delivered("fenced", 1, RESOLVE_1, OP_ADMIT, outcome="RejectedCertain"),
        ]
        + refusal_out("refusal", "fenced.delivered")
        + [
            # The old command is put back in circulation: refused, no effect.
            released("held_admission"),
            ready("late.ready", 1, "RejectedCertain", "released.held_admission"),
            fault_hit("late_rejection", "store.result.consume", "late.ready"),
            inspected("final"),
        ]
    )


def empty_snapshot(run, checkpoint, fences=()):
    state = snapshot(run, checkpoint, [FIRST], open_exchange=False)
    for table in ("operations", "payloads", "publications", "obligations"):
        state[table] = []
    state["fences"] = [{"operation_id": operation} for operation in fences]
    state["account"]["used"] = dict.fromkeys(state["account"]["used"], "0")
    state["meta"]["state_revision"] = str(len(fences))
    return state


def qabs_b_snapshots(run):
    return {"final": empty_snapshot(run, "final", [OP_ADMIT])}


def qabs_c():
    """The command stays held and the fence cannot be written: the outcome is unknown."""
    return qabs_start("QABS-C") + [
        fault_hit(
            "refused_fence", "store.fence.transit", "resolve.1", effect_id=effect(1, RESOLVE_1)
        ),
        ev(
            "fence.refused",
            "store-evidence",
            "StoreFenceRefused",
            {"effect_id": effect(1, RESOLVE_1), "operation_id": OP_ADMIT},
            ["hit.refused_fence"],
        ),
        ready("unknown.ready", RESOLVE_1, "PendingOrUnknown", "fence.refused"),
        fault_hit(
            "unknown_answer",
            "store.result.consume",
            "unknown.ready",
            effect_id=effect(1, RESOLVE_1),
        ),
        delivered(
            "unknown", 1, RESOLVE_1, OP_ADMIT, outcome="PendingOrUnknown", cause="unknown.ready"
        ),
        inspected("unknown"),
    ]


def qabs_c_snapshots(run):
    return {"unknown": empty_snapshot(run, "unknown")}


def qabs(name, why, expected=None, *, letter, **options):
    base, states = {
        "A": (qabs_a, qabs_a_snapshots),
        "B": (qabs_b, qabs_b_snapshots),
        "C": (qabs_c, qabs_c_snapshots),
    }[letter]
    listed = json.loads((ROOT / f"tests/scenarios/qabs/QABS-{letter}.json").read_text())
    if callable(expected):
        expected = expected(listed["assertions"])
    case(
        name,
        why,
        expected,
        base=base,
        states_of=states,
        scenario_path=f"tests/scenarios/qabs/QABS-{letter}.json",
        assertions=listed["assertions"],
        **options,
    )


qabs("qabs-a-good", "QABS-A: the command commits before the fence; one effect.", letter="A")
qabs("qabs-b-good", "QABS-B: the fence commits first; the old command is refused.", letter="B")
qabs("qabs-c-good", "QABS-C: no fence can be written; the outcome stays unknown.", letter="C")


def executed_after_fence(events):
    # The fenced command commits all the same, before the client learns of
    # the refusal.
    for label in ("released.held_admission", "late.ready", "hit.late_rejection"):
        events.insert(at(events, "refusal"), events.pop(at(events, label)))
    stable = ev(
        "late.stable",
        "store-evidence",
        "StoreCommitStable",
        {"operation_id": OP_ADMIT, "command_kind": "AdmitPublication"},
        ["released.held_admission"],
    )
    events.insert(at(events, "late.ready"), stable)
    late = events[at(events, "late.ready")]
    late["payload"] = {
        **{key: value for key, value in late["payload"].items() if key != "reject_reason"},
        "outcome": "Committed",
    }
    late["parents"] = ["late.stable"]


def executed_state(states):
    run = states["final"]["run_id"]
    # The fence is known from the trace alone: this snapshot shows only the
    # commit, so that the admission can be attributed.
    states["final"] = snapshot(run, "final", [FIRST])


qabs(
    "fenced-command-executed",
    "A command is committed after the fence that made it definitively not executable.",
    {"fenced_operation_never_executed": "FAIL"},
    letter="B",
    events=executed_after_fence,
    snapshots=executed_state,
)


def fenced_and_committed(states):
    run = states["final"]["run_id"]
    states["final"] = snapshot(run, "final", [FIRST])
    states["final"]["fences"] = [{"operation_id": OP_ADMIT}]


qabs(
    "snapshot-holds-a-fenced-commit",
    "No commit is in the trace, yet the store holds the operation both fenced and committed.",
    {"fenced_operation_never_executed": "FAIL"},
    letter="B",
    snapshots=fenced_and_committed,
)


def invented_rejection(events):
    # DEM-MU-01: an absence that was read is reported as a certain refusal,
    # with no fence to make it one.
    for label in ("unknown.ready", "unknown.delivered"):
        event = events[at(events, label)]
        event["payload"] = dict(event["payload"], outcome="RejectedCertain", reject_reason="Fenced")


qabs(
    "certain-rejection-without-a-fence",
    "DEM-MU-01: the store answers a certain refusal though its fence was refused.",
    {"certain_rejection_has_a_closure": "FAIL"},
    letter="C",
    events=invented_rejection,
)


def cut(**standing):
    """What a cut trace concludes: nothing, but the failures that stand."""

    def expected(assertions):
        owed = [*assertions, "trace_complete", "broker_inputs_have_provenance"]
        return {**dict.fromkeys(owed, "INCONCLUSIVE"), **standing}

    return expected


REASON = "refusal_carries_the_reason_of_its_rejection"


def refused_with(reason, code):
    """The refusing PUBREC carries ``reason``, and the peer decodes ``code``."""

    def edit(events):
        deepcopy_payloads(events)
        events[at(events, "refusal")]["payload"]["reason"] = reason
        observed = next(
            event
            for event in events[at(events, "refusal") :]
            if event["kind"] == "PeerPacketObserved"
        )
        observed["payload"]["reason_code"] = code
        observed["payload"]["bytes_hex"] = f"50030007{code:02x}"

    return edit


def reason_of(rejection):
    def edit(events):
        deepcopy_payloads(events)
        for event in events:
            if event["payload"].get("reject_reason"):
                event["payload"]["reject_reason"] = rejection

    return edit


qabs(
    "refusal-with-a-quota-for-a-fence",
    "The admission is refused because it was fenced, and the PUBREC says a quota is the cause.",
    {REASON: "FAIL"},
    letter="B",
    events=refused_with("QuotaExceeded", 0x97),
)
qabs(
    "refusal-for-lack-of-capacity",
    "The admission is refused for lack of capacity: the PUBREC says a quota is the cause.",
    letter="B",
    events=lambda events: (
        reason_of("CapacityExceeded")(events),
        refused_with("QuotaExceeded", 0x97)(events),
    ),
)
qabs(
    "refusal-for-lack-of-capacity-without-the-quota",
    "The admission is refused for lack of capacity, and the PUBREC hides that a quota is the cause.",
    {REASON: "FAIL"},
    letter="B",
    events=reason_of("CapacityExceeded"),
)
qabs(
    "refusal-answering-no-rejection",
    "The PUBREC refuses, and no certain rejection is among its causes.",
    {REASON: "FAIL"},
    letter="B",
    events=lambda events: events[at(events, "refusal")].update(parents=["publish.in"]),
)


def answered_by_an_unknown(events):
    # The core is told the outcome is unknown, and refuses on that.
    deepcopy_payloads(events)
    payload = events[at(events, "fenced.delivered")]["payload"]
    payload["outcome"] = "PendingOrUnknown"
    del payload["reject_reason"]


qabs(
    "refusal-answering-an-unknown-outcome",
    "The PUBREC refuses, and the result among its causes is an unknown outcome, not a rejection.",
    {REASON: "FAIL"},
    letter="B",
    events=answered_by_an_unknown,
)
# Review of the stack, findings 1 to 4 and 6.


def fence_listed_afterwards(states):
    # The fence the final snapshot lists was not established when the store
    # answered: the snapshot is inspected after the refusal.
    states["unknown"] = empty_snapshot(states["unknown"]["run_id"], "unknown", [OP_ADMIT])


qabs(
    "certain-rejection-closed-only-afterwards",
    "DEM-MU-01: the store refuses on an absence it read, and the fence a later snapshot lists came after.",
    {"certain_rejection_has_a_closure": "FAIL"},
    letter="C",
    events=invented_rejection,
    snapshots=fence_listed_afterwards,
)


def fenced_after_its_commit(events):
    events.insert(
        at(events, "admit.ready") + 1,
        ev(
            "late.fence",
            "store-evidence",
            "StoreFenceStable",
            {"operation_id": OP_ADMIT},
            ["admit.stable"],
        ),
    )


qabs(
    "fence-stable-after-the-commit",
    "The operation is committed, and the store then writes a fence for the same identity.",
    {"fenced_operation_never_executed": "FAIL"},
    letter="A",
    events=fenced_after_its_commit,
)
qabs(
    "certain-rejection-without-a-fence-on-a-cut-trace",
    "The trace is cut: the fence that closes the refusal may be the record that is missing.",
    cut(),
    letter="C",
    events=invented_rejection,
    raw=lambda records: records.pop(),
)
qabs(
    "fenced-command-executed-on-a-cut-trace",
    "The trace is cut, and it already holds a commit after its fence: no missing record undoes that.",
    cut(fenced_operation_never_executed="FAIL"),
    letter="B",
    events=executed_after_fence,
    snapshots=executed_state,
    raw=lambda records: records.pop(),
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
