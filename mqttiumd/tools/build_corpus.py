"""Rebuild what is derived in the v1 contracts: the requirement register and the examples.

Two sets of files are derived from other files, and are written by this tool rather than by
hand:

* ``contracts/v1/requirements.json``: every identifier the archived chapters define, with
  the digest of each chapter, and every identifier an accepted decision record introduces,
  with the digest of each record.
* ``contracts/examples/v1/``: the valid and invalid examples and their index. Each invalid
  example is one edit of a valid document, so that it is refused for one reason.

The schemas, the catalogues, the transition registry, the fixture and scenario P1-02 are
sources: they are edited directly and this tool only reads them.

Run from the ``mqttiumd`` directory::

    python tools/build_corpus.py           # rewrite the derived files
    python tools/build_corpus.py --check   # fail if a derived file is not up to date

After editing a chapter digest, a decision record, the base scenario or the fixture, run it
without ``--check`` and commit the result. Standard library only.
"""

from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SPEC = ROOT / "docs/spec/fr"
V1 = ROOT / "contracts/v1"
EX = ROOT / "contracts/examples/v1"

# Path relative to the mqttiumd root -> exact text of the derived file.
OUTPUTS: dict[str, str] = {}


def emit(path: pathlib.Path, text: str) -> None:
    OUTPUTS[str(path.relative_to(ROOT))] = text


def dump(path, value):
    emit(path, json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def env(fmt, **members):
    return {"format": fmt, "schema_version": 1, **members}


# ------------------------------------------------------------- requirements
entries = []
sources = []
for chapter in range(1, 5):
    path = SPEC / f"chapitre-{chapter}.md"
    data = path.read_bytes()
    sources.append(
        {
            "chapter": chapter,
            "path": f"docs/spec/fr/chapitre-{chapter}.md",
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    )
    for match in re.finditer(r"\*\*([A-Z0-9]+-[0-9]+) — ", data.decode()):
        entries.append(
            {"id": match.group(1), "kind": "requirement", "source": {"chapter": chapter}}
        )


def add(kind, chapter, ids):
    entries.extend({"id": i, "kind": kind, "source": {"chapter": chapter}} for i in ids)


add("scenario", 1, [f"T{n:02d}" for n in range(1, 19)])
add("scenario", 3, [f"P1-{n:02d}" for n in range(1, 20)])
add("scenario", 3, [f"P2-{n:02d}" for n in range(1, 24)])
add("scenario", 3, [f"P3-{n:02d}" for n in range(1, 16)])
add("scenario", 4, ["QABS-A", "QABS-B", "QABS-C"])
add("mutation", 3, [f"MU-{n:02d}" for n in range(1, 26)])
add("mutation", 4, [f"DEM-MU-{n:02d}" for n in range(1, 7)])
add("model", 3, [f"M{n}" for n in range(1, 7)])
add("model", 4, ["M1a", "M1b"])
add("gate", 3, [f"G{n}" for n in range(0, 6)])
add("open-decision", 2, ["D1"])
add("open-decision", 3, ["T1"])
add("lot", 4, [f"L{n}" for n in range(0, 11)])
add("tranche", 4, ["S0", "S1"])
# Identifiers introduced by accepted decision records. The archived chapters
# are never edited to hold them.
decisions = []
for path in sorted((ROOT / "docs/decisions").glob("0*.md")):
    text = path.read_text()
    if "Status: **Accepted" not in text:
        continue
    decisions.append(
        {
            "decision": path.name[:4],
            "path": f"docs/decisions/{path.name}",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    )
entries.append({"id": "P1-02b", "kind": "scenario", "source": {"decision": "0001"}})
dump(
    V1 / "requirements.json",
    env("mqttiumd.requirement-register", chapters=sources, decisions=decisions, entries=entries),
)


def rule(
    rid,
    machine,
    state,
    event,
    guard,
    disposition,
    nxt,
    required,
    forbidden,
    delta,
    reqs,
    witnesses,
    mutations,
    **extra,
):
    return {
        "id": rid,
        "machine": machine,
        "state_class": state,
        "event_class": event,
        "guard_partition": guard,
        "disposition": disposition,
        "next_state_class": nxt,
        "required_effects": required,
        "forbidden_effects": forbidden,
        "resource_delta_class": delta,
        "requirements": reqs,
        "witnesses": witnesses,
        "mutations": mutations,
        **extra,
    }


# ------------------------------------------------------------------ examples
BASE = json.loads((ROOT / "tests/scenarios/p1/P1-02.json").read_text())
valid = [
    "tests/scenarios/p1/P1-02.json",
    "tests/fixtures/p1/three-persistent-sessions.json",
]
invalid = []


def ok(name, value):
    dump(EX / "valid" / name, value)
    valid.append(f"contracts/examples/v1/valid/{name}")


def bad(name, code, pointer, why, value=None, raw=None):
    path = EX / "invalid" / name
    if raw is None:
        dump(path, value)
    else:
        emit(path, raw)
    invalid.append(
        {
            "file": f"contracts/examples/v1/invalid/{name}",
            "code": code,
            "pointer": pointer,
            "why": why,
        }
    )


def scenario(edit):
    value = copy.deepcopy(BASE)
    edit(value)
    return value


def setter(path, new):
    def edit(value):
        *parents, last = path
        for key in parents:
            value = value[key]
        value[last] = new

    return edit


def schema_case(name, path, new, why):
    pointer = "".join(f"/{key}" for key in path)
    bad(f"scenario-{name}.json", "schema-violation", pointer, why, scenario(setter(path, new)))


BUDGET = ["budgets", "max_steps"]
schema_case("u64-leading-zero", BUDGET, "04096", "A canonical decimal string has no leading zero.")
schema_case("u64-signed", BUDGET, "+4096", "A canonical decimal string has no sign.")
schema_case("u64-fraction", BUDGET, "4096.0", "A canonical decimal string has no fraction.")
schema_case("u64-leading-space", BUDGET, " 4096", "A canonical decimal string has no whitespace.")
schema_case(
    "u64-trailing-line-break",
    BUDGET,
    "4096\n",
    "The whole string is checked: `$` must not accept a trailing line break.",
)
schema_case("u64-as-number", BUDGET, 4096, "64-bit quantities are strings, never JSON numbers.")
schema_case("u64-empty", BUDGET, "", "The empty string is not a number.")
schema_case("u64-twenty-one-digits", BUDGET, "100000000000000000000", "At most twenty digits.")
bad(
    "scenario-u64-overflow.json",
    "u64-out-of-range",
    "/budgets/max_trace_bytes",
    "2^64 matches the pattern and exceeds the range; the semantic pass refuses it.",
    scenario(setter(["budgets", "max_trace_bytes"], "18446744073709551616")),
)
PID = ["steps", 1, "packet_id"]
schema_case("packet-id-zero", PID, 0, "A Packet Identifier is never zero.")
schema_case("packet-id-65536", PID, 65536, "A Packet Identifier fits sixteen bits.")
schema_case("packet-id-negative", PID, -1, "A Packet Identifier is positive.")
schema_case("packet-id-as-string", PID, "7", "Packet Identifiers stay JSON integers.")
bad(
    "scenario-packet-id-float.json",
    "number-not-integer",
    "",
    "7.0 is an integer for JSON Schema; the strict reader refuses the spelling.",
    raw=json.dumps(BASE, indent=2).replace(
        '"packet_id": 7,\n      "payload_hex"', '"packet_id": 7.0,\n      "payload_hex"'
    )
    + "\n",
)
bad(
    "scenario-duplicate-key.json",
    "duplicate-key",
    "",
    "A duplicate key is refused while parsing, before a map overwrites a value.",
    raw=json.dumps(BASE, indent=2).replace('"id": "P1-02",', '"id": "P1-02",\n  "id": "P1-03",', 1)
    + "\n",
)
bad(
    "scenario-truncated.json",
    "json-syntax",
    "",
    "The document ends early.",
    raw=json.dumps(BASE, indent=2)[:400],
)
bad(
    "scenario-nan.json",
    "json-syntax",
    "",
    "NaN is not JSON.",
    raw=json.dumps(BASE, indent=2).replace(
        '"packet_id": 7,\n      "payload_hex"', '"packet_id": NaN,\n      "payload_hex"'
    )
    + "\n",
)
bad(
    "scenario-lone-surrogate.json",
    "json-syntax",
    "",
    "A lone surrogate escape is not a Unicode scalar value.",
    raw=json.dumps(BASE, indent=2).replace('"topic": "p1/example"', '"topic": "p1/\\ud800"') + "\n",
)
bad(
    "scenario-integer-beyond-64-bits.json",
    "number-not-integer",
    "",
    "An integer literal beyond 64 bits is refused while parsing.",
    raw=json.dumps(BASE, indent=2).replace(
        '"packet_id": 7,\n      "payload_hex"',
        '"packet_id": 18446744073709551616,\n      "payload_hex"',
    )
    + "\n",
)
bad(
    "scenario-negative-zero.json",
    "number-not-integer",
    "",
    "The lexeme -0 is not an integer literal of version 1; zero is written 0.",
    raw=json.dumps(scenario(lambda v: v["steps"][1].pop("packet_id")), indent=2).replace(
        '"qos": 2', '"qos": -0'
    )
    + "\n",
)
bad(
    "scenario-two-documents.json",
    "json-syntax",
    "",
    "One file holds exactly one JSON value.",
    raw=json.dumps(BASE) + "\n" + json.dumps(BASE) + "\n",
)
bad(
    "scenario-unknown-top-level-member.json",
    "schema-violation",
    "",
    "Objects are closed.",
    scenario(lambda v: v.update(note="extra")),
)
bad(
    "scenario-unknown-schema-version.json",
    "schema-version-unknown",
    "/schema_version",
    "An unknown version is an error, not a best effort.",
    scenario(setter(["schema_version"], 2)),
)
bad(
    "scenario-schema-version-as-string.json",
    "schema-version-unknown",
    "/schema_version",
    "The version is the integer 1.",
    scenario(setter(["schema_version"], "1")),
)
bad(
    "scenario-unknown-format.json",
    "format-unknown",
    "/format",
    "The format name is closed.",
    scenario(setter(["format"], "mqttiumd.scenari0")),
)
bad(
    "scenario-missing-format.json",
    "format-unknown",
    "/format",
    "Every document declares its format.",
    scenario(lambda v: v.pop("format")),
)
schema_case(
    "unknown-step-kind", ["steps", 4, "kind"], "ClientTeleport", "Step kinds are a closed union."
)
bad(
    "scenario-unknown-step-member.json",
    "schema-violation",
    "/steps/4",
    "Each step variant is a closed object.",
    scenario(lambda v: v["steps"][4].update(signal="KILL")),
)
bad(
    "scenario-missing-step-member.json",
    "schema-violation",
    "/steps/0",
    "Every member of a step is required.",
    scenario(lambda v: v["steps"][0].pop("max_steps")),
)
bad(
    "scenario-qos2-without-packet-id.json",
    "schema-violation",
    "/steps/1",
    "A QoS 2 PUBLISH carries a Packet Identifier.",
    scenario(lambda v: v["steps"][1].pop("packet_id")),
)
bad(
    "scenario-qos0-with-packet-id.json",
    "schema-violation",
    "/steps/1",
    "A QoS 0 PUBLISH carries no Packet Identifier.",
    scenario(setter(["steps", 1, "qos"], 0)),
)
schema_case("qos-3", ["steps", 1, "qos"], 3, "QoS is 0, 1 or 2.")
schema_case(
    "payload-hex-odd", ["steps", 1, "payload_hex"], "706", "Two hexadecimal digits per byte."
)
schema_case(
    "payload-hex-upper-case", ["steps", 1, "payload_hex"], "7061796C", "Lower-case digits only."
)
schema_case(
    "unknown-failure-model",
    ["steps", 4, "failure_model"],
    "power",
    "S0 qualifies the process crash only; a power cut is another campaign.",
)
schema_case("zero-workers", ["steps", 5, "workers"], 0, "A broker has at least one worker.")
schema_case("empty-steps", ["steps"], [], "A scenario has at least one step.")
schema_case(
    "duplicate-assertion",
    ["assertions"],
    ["trace_complete", "trace_complete"],
    "Assertions are listed once.",
)

bad(
    "scenario-binding-undefined-exchange.json",
    "binding-undefined",
    "/steps/7/exchange",
    "ClientResumeExchange refers to an exchange no earlier step bound.",
    scenario(setter(["steps", 7, "exchange"], "pub_B")),
)
bad(
    "scenario-binding-undefined-fault.json",
    "binding-undefined",
    "/steps/2/binding",
    "DriveUntil waits for a fault that is not declared.",
    scenario(setter(["steps", 2, "binding"], "lost_release_result")),
)
bad(
    "scenario-binding-undefined-publication.json",
    "binding-undefined",
    "/faults/0/match/publication_binding",
    "The fault matches a publication no step binds.",
    scenario(setter(["faults", 0, "match", "publication_binding"], "pub_Z")),
)
bad(
    "scenario-binding-duplicate.json",
    "binding-duplicate",
    "/steps/1/bind_exchange_as",
    "An operation and an exchange share one binding name.",
    scenario(setter(["faults", 0, "bind_operation_as"], "pub_A")),
)
bad(
    "scenario-checkpoint-duplicate.json",
    "binding-duplicate",
    "/steps/11/checkpoint",
    "Two inspections share one checkpoint name.",
    scenario(setter(["steps", 11, "checkpoint"], "after_commit_before_crash")),
)
bad(
    "scenario-fault-point-unknown.json",
    "fault-point-unknown",
    "/faults/0/point",
    "Injection points are contractual and catalogued.",
    scenario(setter(["faults", 0, "point"], "store.result.lost")),
)
bad(
    "scenario-fault-action-unknown.json",
    "fault-action-unknown",
    "/faults/0/action",
    "The point exists and does not offer this action.",
    scenario(setter(["faults", 0, "action"], "hold_command")),
)
bad(
    "scenario-command-kind-unknown.json",
    "command-kind-unknown",
    "/faults/0/match/command_kind",
    "Command kinds are a closed set.",
    scenario(setter(["faults", 0, "match", "command_kind"], "AdmitEverything")),
)
bad(
    "scenario-profile-unknown.json",
    "profile-unknown",
    "/profile",
    "Profiles are catalogued.",
    scenario(setter(["profile"], "p1-volatile")),
)
bad(
    "scenario-assertion-unknown.json",
    "assertion-unknown",
    "/assertions/6",
    "An assertion nobody defined cannot be satisfied.",
    scenario(setter(["assertions", 6], "everything_is_fine")),
)
bad(
    "scenario-step-budget-exceeded.json",
    "budget-exceeded",
    "/steps/0/max_steps",
    "A step may not run longer than the scenario budget.",
    scenario(setter(["steps", 0, "max_steps"], "8192")),
)
bad(
    "scenario-client-unknown.json",
    "client-unknown",
    "/steps/9/client",
    "The client is not a session of the fixture.",
    scenario(setter(["steps", 9, "client"], "ghost")),
)
bad(
    "scenario-fixture-missing.json",
    "file-unreadable",
    "/initial_fixture",
    "The fixture file does not exist.",
    scenario(setter(["initial_fixture"], "tests/fixtures/p1/four-persistent-sessions.json")),
)
bad(
    "scenario-fixture-path-escape.json",
    "path-escape",
    "/initial_fixture",
    "A path may not leave the mqttiumd root.",
    scenario(setter(["initial_fixture"], "tests/../../outside.json")),
)
bad(
    "scenario-fixture-wrong-format.json",
    "reference-invalid",
    "/initial_fixture",
    "The referenced file is valid and is not a fixture.",
    scenario(setter(["initial_fixture"], "contracts/v1/profiles.json")),
)


def swap_inspect_and_crash(value):
    value["steps"][3], value["steps"][4] = value["steps"][4], value["steps"][3]


bad(
    "scenario-inspect-while-crashed.json",
    "step-order-invalid",
    "/steps/4",
    "Nothing reads the store between the kill and the restart (ADR 0002).",
    scenario(swap_inspect_and_crash),
)
bad(
    "scenario-restart-while-running.json",
    "step-order-invalid",
    "/steps/4",
    "RestartBroker needs a crashed broker.",
    scenario(setter(["steps", 4], {"kind": "RestartBroker", "workers": 1})),
)

ok(
    "scenario-u64-max.json",
    scenario(setter(["budgets", "max_trace_bytes"], "18446744073709551615")),
)
ok(
    "scenario-core-without-bytes.json",
    scenario(
        lambda v: v.update(
            perimeter="core-without-bytes",
            assertions=[
                name
                for name in BASE["assertions"]
                if name
                not in (
                    "no_pubrec_observed_before_crash",
                    "one_admission_per_observed_inbound_exchange",
                )
            ],
        )
    ),
)
schema_case(
    "unknown-perimeter", ["perimeter"], "everything", "A scenario requires a known perimeter."
)
ok(
    "scenario-qos0-publish.json",
    scenario(lambda v: (v["steps"][1].pop("packet_id"), v["steps"][1].update(qos=0))),
)

# The steps of P1-02b (ADR 0001): no crash, a held command, a held result and
# two deadlines. `tests/scenarios/p1/P1-02b.json` itself comes with lot L4;
# this document only fixes the shape of the steps.
MATCH_A = {"command_kind": "AdmitPublication", "publication_binding": "pub_A"}


def fault(fid, point, action, **extra):
    match = {**MATCH_A, **extra.pop("match", {})}
    return {
        "id": fid,
        "point": point,
        "match": match,
        "occurrence": "1",
        "action": action,
        **extra,
        "expected_hits": "1",
    }


def drive_fault(binding):
    return {"kind": "DriveUntil", "event": "FaultHit", "binding": binding, "max_steps": "512"}


HELD = scenario(
    lambda v: v.update(
        id="P1-02b",
        faults=[
            fault(
                "held_admission",
                "store.command.transit",
                "hold_command",
                bind_operation_as="op_A",
            ),
            fault(
                "held_unknown",
                "store.result.ready",
                "delay_result",
                match={"result_of": "ResolveAndFence"},
            ),
            fault(
                "lost_admission_result",
                "store.result.ready",
                "drop_result",
                match={"result_of": "Persist"},
            ),
        ],
        steps=[
            BASE["steps"][0],
            BASE["steps"][1],
            drive_fault("held_admission"),
            {"kind": "FireDeadline", "max_steps": "512"},
            drive_fault("held_unknown"),
            {"kind": "ReleaseHeldCommand", "fault": "held_admission"},
            drive_fault("lost_admission_result"),
            {"kind": "InspectDurableState", "checkpoint": "after_commit"},
            {"kind": "FireDeadline", "max_steps": "512"},
            BASE["steps"][8],
            {"kind": "DeliverHeldResult", "fault": "held_unknown", "max_steps": "512"},
            BASE["steps"][9],
            BASE["steps"][10],
            BASE["steps"][11],
        ],
        assertions=[
            name for name in BASE["assertions"] if name != "no_pubrec_observed_before_crash"
        ],
    )
)
RELEASE, DELIVER = 5, 10


def held(edit):
    value = copy.deepcopy(HELD)
    edit(value)
    return value


def swap(first, second):
    def edit(value):
        steps = value["steps"]
        steps[first], steps[second] = steps[second], steps[first]

    return edit


ok("scenario-held-command-and-result.json", HELD)
bad(
    "scenario-release-before-hold.json",
    "step-order-invalid",
    "/steps/2",
    "A command is released only after the step that reached its hold.",
    held(swap(2, RELEASE)),
)
bad(
    "scenario-release-twice.json",
    "step-order-invalid",
    f"/steps/{RELEASE + 1}",
    "A held command is released once.",
    held(lambda v: v["steps"].insert(RELEASE + 1, copy.deepcopy(v["steps"][RELEASE]))),
)
bad(
    "scenario-deliver-after-crash.json",
    "step-order-invalid",
    f"/steps/{DELIVER + 2}",
    "A crash destroys what the broker process held; nothing is left to deliver.",
    held(
        lambda v: v["steps"].__setitem__(
            slice(DELIVER, DELIVER), [BASE["steps"][4], BASE["steps"][5]]
        )
    ),
)
bad(
    "scenario-release-undeclared-fault.json",
    "binding-undefined",
    f"/steps/{RELEASE}/fault",
    "ReleaseHeldCommand names a fault that is not declared.",
    held(setter(["steps", RELEASE, "fault"], "held_release")),
)
bad(
    "scenario-release-of-a-held-result.json",
    "held-item-mismatch",
    f"/steps/{RELEASE}/fault",
    "The fault delays a result; it holds no command to release.",
    held(setter(["steps", RELEASE, "fault"], "held_unknown")),
)
bad(
    "scenario-deliver-of-a-dropped-result.json",
    "held-item-mismatch",
    f"/steps/{DELIVER}/fault",
    "A dropped result is gone; only a delayed result can be delivered.",
    held(setter(["steps", DELIVER, "fault"], "lost_admission_result")),
)
bad(
    "scenario-result-of-on-a-command-point.json",
    "fault-match-invalid",
    "/faults/0/match/result_of",
    "The point carries a command; `result_of` selects among results.",
    held(setter(["faults", 0, "match", "result_of"], "Persist")),
)
bad(
    "scenario-result-of-unknown-effect.json",
    "schema-violation",
    "/faults/1/match/result_of",
    "Only Persist and ResolveAndFence are answered by a store result.",
    held(setter(["faults", 1, "match", "result_of"], "LoadWork")),
)
bad(
    "scenario-fire-deadline-without-budget.json",
    "schema-violation",
    "/steps/3",
    "Firing a deadline is bounded like every step that drives the broker.",
    held(lambda v: v["steps"][3].pop("max_steps")),
)
bad(
    "scenario-fire-deadline-budget-exceeded.json",
    "budget-exceeded",
    "/steps/3/max_steps",
    "The new steps obey the scenario budget.",
    held(setter(["steps", 3, "max_steps"], "8192")),
)

# fault catalogue
FAULTS = json.loads((V1 / "fault-points.json").read_text())
ok("fault-catalog.json", FAULTS)
bad(
    "fault-catalog-point-without-carries.json",
    "schema-violation",
    "/points/0",
    "Every injection point says whether a command or a result passes through it.",
    (lambda v: (v["points"][0].pop("carries"), v)[1])(copy.deepcopy(FAULTS)),
)

# fixture
fixture = json.loads((ROOT / "tests/fixtures/p1/three-persistent-sessions.json").read_text())


def fix(edit):
    value = copy.deepcopy(fixture)
    edit(value)
    return value


bad(
    "fixture-route-target-unknown.json",
    "client-unknown",
    "/routes/0/targets/1/client",
    "A route targets a session the fixture does not create.",
    fix(setter(["routes", 0, "targets", 1, "client"], "d")),
)
bad(
    "fixture-duplicate-client-id.json",
    "duplicate-name",
    "/sessions/2/client_id",
    "Two sessions share one ClientID.",
    fix(setter(["sessions", 2, "client_id"], "p1-b")),
)
bad(
    "fixture-capacity-overflow.json",
    "u64-out-of-range",
    "/capacities/payload_bytes",
    "Capacities are 64-bit.",
    fix(setter(["capacities", "payload_bytes"], "99999999999999999999")),
)
bad(
    "fixture-session-not-persistent.json",
    "schema-violation",
    "/sessions/0/persistent",
    "S0 fixtures hold persistent sessions only.",
    fix(setter(["sessions", 0, "persistent"], False)),
)

# trace records
OP = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "1"}
EFFECT = {"epoch": "1", "owner": "0", "sequence": "1"}
RESOLVE = {"epoch": "1", "owner": "0", "sequence": "3"}
PUBLICATION = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "1"}
PUBLISH_HEX = "3416000a70312f6578616d706c650007007061796c6f6164"


def record(sequence, origin, origin_sequence, kind, payload, parents=()):
    return env(
        "mqttiumd.trace-record",
        run_id="p1-02-example",
        record_sequence=str(sequence),
        origin_component=origin,
        origin_incarnation="1",
        origin_sequence=str(origin_sequence),
        causal_parents=[
            {"origin_component": o, "origin_incarnation": "1", "origin_sequence": str(s)}
            for o, s in parents
        ],
        time_domain="logical-step",
        time_value=str(sequence),
        kind=kind,
        payload=payload,
    )


HEADER = record(
    0,
    "bench-supervisor",
    0,
    "TraceHeader",
    {"scenario_id": "P1-02", "backend": "sim-store", "perimeter": "full"},
)
ok("trace-record-header.json", HEADER)
commit = record(
    5,
    "store-evidence",
    0,
    "StoreCommitStable",
    {"operation_id": OP, "command_kind": "AdmitPublication"},
    [("store-adapter", 0)],
)
ok("trace-record-store-commit-stable.json", commit)
ok(
    "trace-record-fault-hit.json",
    record(
        6,
        "fault-injector",
        0,
        "FaultHit",
        {
            "fault_id": "lost_admission_result",
            "point": "store.result.ready",
            "occurrence": "1",
            "operation_id": OP,
            "effect_id": EFFECT,
        },
        [("store-evidence", 1)],
    ),
)
ok(
    "trace-record-peer-packet-observed.json",
    record(
        20,
        "peer",
        3,
        "PeerPacketObserved",
        {
            "client": "producer",
            "packet_kind": "PUBREC",
            "packet_id": 7,
            "reason_code": 0,
            "bytes_hex": "50020007",
        },
    ),
)
ok(
    "trace-record-footer.json",
    record(31, "bench-supervisor", 4, "TraceFooter", {"record_count": "32", "complete": True}),
)

# What a client sent, what the transport received and what the core consumed:
# the oracle builds its own identity of an exchange from the first, and the
# provenance of broker inputs (ADR 0003) from the chain.
PEER_PUBLISH = record(
    3,
    "peer",
    2,
    "PeerPacketSent",
    {
        "client": "producer",
        "packet_kind": "PUBLISH",
        "packet_id": 7,
        "dup": False,
        "bytes_hex": PUBLISH_HEX,
    },
)
ok(
    "trace-record-peer-connection-opened.json",
    record(1, "peer", 0, "PeerConnectionOpened", {"client": "producer"}),
)
ok("trace-record-peer-packet-sent.json", PEER_PUBLISH)
ok(
    "trace-record-peer-connection-closed.json",
    record(30, "peer", 5, "PeerConnectionClosed", {"client": "producer"}),
)
ok(
    "trace-record-client-bytes-received.json",
    record(
        4,
        "transport",
        1,
        "ClientBytesReceived",
        {"connection_generation": "1", "bytes_hex": PUBLISH_HEX},
        [("peer", 2)],
    ),
)
DELIVERED_PACKET = record(
    5,
    "broker-core",
    1,
    "PacketDelivered",
    {"connection_generation": "1", "ingress_order": "1", "packet_kind": "PUBLISH", "packet_id": 7},
    [("transport", 1)],
)
ok("trace-record-packet-delivered.json", DELIVERED_PACKET)
ok(
    "trace-record-resolve-submitted.json",
    record(
        9,
        "store-adapter",
        1,
        "ResolveSubmitted",
        {"effect_id": RESOLVE, "operation_id": OP},
    ),
)
ok(
    "trace-record-store-result-ready.json",
    record(
        7,
        "store-evidence",
        1,
        "StoreResultReady",
        {
            "effect_id": EFFECT,
            "operation_id": OP,
            "outcome": "Committed",
        },
        [("store-evidence", 0)],
    ),
)
ok(
    "trace-record-store-fence-stable.json",
    record(
        10, "store-evidence", 2, "StoreFenceStable", {"operation_id": OP}, [("store-adapter", 1)]
    ),
)
ARMED = record(
    8,
    "timer",
    0,
    "DeadlineArmed",
    {"effect_id": {"epoch": "1", "owner": "0", "sequence": "2"}, "token": "1", "after": "30"},
)
ok("trace-record-deadline-armed.json", ARMED)
FIRED = record(
    11, "broker-core", 2, "DeadlineDelivered", {"token": "1", "now": "30"}, [("timer", 0)]
)
ok("trace-record-deadline-delivered.json", FIRED)
PAGE = {
    "run_epoch": "2",
    "page": "0",
    "last": True,
    "facts": [
        {"kind": "Epoch", "incarnation": "1", "epoch": "2"},
        {
            "kind": "Session",
            "session": "1",
            "client_id_hex": "70312d70726f6475636572",
            "revision": "1",
        },
        {
            "kind": "InboundExchange",
            "exchange": {"session": "1", "generation": "1"},
            "packet_id": 7,
            "publication": PUBLICATION,
            "phase": "AwaitPubrel",
        },
        {"kind": "Operation", "operation_id": OP, "knowledge": "Committed"},
    ],
}
PAGE_READ = record(14, "store-adapter", 2, "RecoveryPageRead", PAGE)
ok("trace-record-recovery-page-read.json", PAGE_READ)
ok(
    "trace-record-recovery-page-delivered.json",
    record(15, "broker-core", 3, "RecoveryPageDelivered", PAGE, [("store-adapter", 2)]),
)
REFUSED = record(
    16,
    "dispatcher",
    0,
    "InputRefused",
    {
        "input": "StoreResult",
        "effect_id": EFFECT,
        "operation_id": OP,
        "reason": "no-effect-in-flight",
    },
)
ok("trace-record-input-refused.json", REFUSED)
ok(
    "trace-record-held-item-released.json",
    record(
        12,
        "fault-injector",
        1,
        "HeldItemReleased",
        {"fault_id": "held_admission"},
        [("fault-injector", 0)],
    ),
)
ELIGIBLE = record(
    18,
    "broker-core",
    4,
    "FrameEligible",
    {
        "effect_id": {"epoch": "1", "owner": "0", "sequence": "4"},
        "connection": {"session": "1", "generation": "1"},
        "packet_kind": "PUBREC",
        "packet_id": 7,
        "reason": "Success",
    },
    [("broker-core", 3)],
)
ok("trace-record-frame-eligible.json", ELIGIBLE)
ok(
    "trace-record-frame-eligible-connack.json",
    record(
        2,
        "broker-core",
        0,
        "FrameEligible",
        {
            "effect_id": {"epoch": "1", "owner": "0", "sequence": "0"},
            "connection": {"session": "1", "generation": "1"},
            "packet_kind": "CONNACK",
            "session_present": True,
        },
    ),
)


def edited(base, edit):
    value = copy.deepcopy(base)
    edit(value)
    return value


def dropped(*path):
    def edit(value):
        *parents, last = path
        for key in parents:
            value = value[key]
        del value[last]

    return edit


def trace_case(name, base, pointer, why, edit, code="schema-violation"):
    bad(f"trace-record-{name}.json", code, pointer, why, edited(base, edit))


trace_case(
    "recovery-page-from-the-bench",
    PAGE_READ,
    "/origin_component",
    "A recovery page is read by the store adapter; no other origin may state one (ADR 0003).",
    setter(["origin_component"], "bench-supervisor"),
)
trace_case(
    "input-refused-by-the-core",
    REFUSED,
    "/origin_component",
    "The dispatcher refuses an input before the core sees it; the core cannot record it.",
    setter(["origin_component"], "broker-core"),
)
trace_case(
    "recovered-fact-unknown-kind",
    PAGE_READ,
    "/payload/facts/3/kind",
    "Recovered facts are a closed union: a remembered binding is not one of them.",
    setter(["payload", "facts", 3], {"kind": "RememberedOperation", "operation_id": OP}),
)
trace_case(
    "recovered-operation-without-knowledge",
    PAGE_READ,
    "/payload/facts/3",
    "A recovered operation states what the store knows of it.",
    dropped("payload", "facts", 3, "knowledge"),
)
trace_case(
    "peer-publish-without-dup",
    PEER_PUBLISH,
    "/payload",
    "A PUBLISH that a client sent says whether it is a retransmission.",
    dropped("payload", "dup"),
)
trace_case(
    "peer-packet-sent-of-a-broker-kind",
    PEER_PUBLISH,
    "/payload/packet_kind",
    "A client of the subset sends CONNECT, PUBLISH and PUBREL; PUBREC comes from the broker.",
    setter(["payload", "packet_kind"], "PUBREC"),
)
trace_case(
    "frame-eligible-pubrec-without-reason",
    ELIGIBLE,
    "/payload",
    "A PUBREC is positive or not: the record carries its reason.",
    dropped("payload", "reason"),
)
trace_case(
    "deadline-token-overflow",
    ARMED,
    "/payload/token",
    "A deadline token is 64-bit.",
    setter(["payload", "token"], "18446744073709551616"),
    code="u64-out-of-range",
)
trace_case(
    "packet-delivered-ingress-order-overflow",
    DELIVERED_PACKET,
    "/payload/ingress_order",
    "The ingress order is 64-bit.",
    setter(["payload", "ingress_order"], "18446744073709551616"),
    code="u64-out-of-range",
)
trace_case(
    "recovery-page-fact-overflow",
    PAGE_READ,
    "/payload/facts/1/revision",
    "Identity components inside a recovered fact are 64-bit as well.",
    setter(["payload", "facts", 1, "revision"], "18446744073709551616"),
    code="u64-out-of-range",
)
OVER = "18446744073709551616"
for case_name, base, path in [
    ("deadline-time-overflow", FIRED, ["payload", "now"]),
    ("deadline-delay-overflow", ARMED, ["payload", "after"]),
    ("recovery-page-number-overflow", PAGE_READ, ["payload", "page"]),
    (
        "packet-delivered-connection-overflow",
        DELIVERED_PACKET,
        ["payload", "connection_generation"],
    ),
]:
    trace_case(
        case_name,
        base,
        "".join(f"/{key}" for key in path),
        "Every 64-bit member of a payload is range-checked, whatever its name.",
        setter(path, OVER),
        code="u64-out-of-range",
    )

INSPECTED = record(
    13,
    "bench-supervisor",
    1,
    "DurableStateInspected",
    {
        "checkpoint": "after_commit_before_crash",
        "snapshot": {
            "path": "runs/p1-02-example/after_commit_before_crash.snapshot.json",
            "sha256": "7" * 64,
            "bytes": "2048",
        },
    },
)
ok("trace-record-durable-state-inspected.json", INSPECTED)
trace_case(
    "durable-state-inspected-by-the-core",
    INSPECTED,
    "/origin_component",
    "The core never reads the store for the oracle: an inspection belongs to the bench.",
    setter(["origin_component"], "broker-core"),
)
trace_case(
    "durable-state-inspected-size-overflow",
    INSPECTED,
    "/payload/snapshot/bytes",
    "The size of a cited snapshot is 64-bit.",
    setter(["payload", "snapshot", "bytes"], "18446744073709551616"),
    code="u64-out-of-range",
)
trace_case(
    "durable-state-inspected-without-digest",
    INSPECTED,
    "/payload/snapshot",
    "A snapshot is cited by path, digest and size, like any evidence.",
    dropped("payload", "snapshot", "sha256"),
)

ok(
    "trace-record-header-core-without-bytes.json",
    edited(HEADER, setter(["payload", "perimeter"], "core-without-bytes")),
)
trace_case(
    "header-without-perimeter",
    HEADER,
    "/payload",
    "A run says what it exercised: a run without bytes must not be read as a full one.",
    dropped("payload", "perimeter"),
)
trace_case(
    "header-unknown-perimeter",
    HEADER,
    "/payload/perimeter",
    "Perimeters are a closed set.",
    setter(["payload", "perimeter"], "mostly-full"),
)


def rec(edit):
    value = copy.deepcopy(commit)
    edit(value)
    return value


bad(
    "trace-record-store-truth-from-core.json",
    "schema-violation",
    "/origin_component",
    "Hidden store truth belongs to the evidence origin; the core never states it.",
    rec(setter(["origin_component"], "broker-core")),
)
bad(
    "trace-record-unknown-kind.json",
    "schema-violation",
    "/kind",
    "Record kinds are a closed union.",
    rec(setter(["kind"], "StoreProbablyCommitted")),
)
bad(
    "trace-record-payload-of-another-kind.json",
    "schema-violation",
    "/payload",
    "The payload shape follows the kind.",
    rec(setter(["payload"], {"record_count": "1", "complete": True})),
)
bad(
    "trace-record-store-commit-without-command-kind.json",
    "schema-violation",
    "/payload",
    "Hidden truth says what was committed, not only that something was.",
    rec(dropped("payload", "command_kind")),
)
bad(
    "trace-record-operation-overflow.json",
    "u64-out-of-range",
    "/payload/operation_id/sequence",
    "Identity components are 64-bit.",
    rec(setter(["payload", "operation_id", "sequence"], "18446744073709551616")),
)


# store snapshots: what InspectDurableState writes and the oracle reads
SESSIONS = [("1", "p1-producer"), ("2", "p1-b"), ("3", "p1-c")]
PAYLOAD = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "1"}
USAGE = {"payload_bytes": "7", "references": "2", "exchange_slots": "1", "receipt_slots": "1"}
LIMITS = {
    name: fixture["capacities"][name]
    for name in ("payload_bytes", "references", "exchange_slots", "receipt_slots")
}


def be(value, width=8):
    """`value` as `width` big-endian bytes, in hexadecimal."""
    return value.to_bytes(width, "big").hex()


def scoped(sequence):
    """A scoped identity of incarnation 1, epoch 1, owner 0."""
    return be(1) + be(1) + be(0) + be(sequence)


# The canonical recipe of the admission, byte for byte (`MQDR`, format 1).
RECIPE_HEX = "".join(
    [
        b"MQDR".hex() + be(1, 2) + be(1, 1),
        scoped(1) + scoped(1) + scoped(1),  # operation, request, publication
        be(1) + be(1) + be(7, 2) + be(1),  # source: session, revision, packet, generation
        be(1) + be(2, 1) + be(0, 1),  # route revision, QoS, retain
        be(10, 2) + b"p1/example".hex(),
        scoped(1) + be(7),  # payload identity and length
        be(7) + be(2) + be(1) + be(1),  # costs
        be(2, 2),
        be(2) + be(1) + be(0, 4) + be(1, 1) + be(1, 1),  # target b
        be(3) + be(1) + be(1, 4) + be(1, 1) + be(1, 1),  # target c
    ]
)
snapshot = env(
    "mqttiumd.store-snapshot",
    run_id="p1-02-example",
    checkpoint="after_commit_before_crash",
    backend="sim-store",
    meta={"incarnation": "1", "epoch": "1"},
    sessions=[
        {"session": number, "client_id_hex": client_id.encode().hex(), "revision": "1"}
        for number, client_id in SESSIONS
    ],
    routes=[
        {
            "topic_hex": b"p1/example".hex(),
            "revision": "1",
            "targets": [{"session": "2", "qos": 1}, {"session": "3", "qos": 1}],
        }
    ],
    operations=[
        {
            "operation_id": OP,
            "command_kind": "AdmitPublication",
            "recipe_hex": RECIPE_HEX,
            "decision": "Committed",
        }
    ],
    fences=[],
    publications=[
        {
            "publication_id": PUBLICATION,
            "topic_hex": b"p1/example".hex(),
            "qos": 2,
            "retain": False,
            "payload": {
                "payload_id": PAYLOAD,
                "length": "7",
                "sha256": hashlib.sha256(b"payload").hexdigest(),
            },
        }
    ],
    obligations=[
        {
            "publication_id": PUBLICATION,
            "index": index,
            "session": target,
            "cause": "LiveExact",
            "effective_qos": 1,
            "phase": "Queued",
        }
        for index, target in enumerate(["2", "3"])
    ],
    inbound_exchanges=[
        {
            "exchange": {"session": "1", "generation": "1"},
            "packet_id": 7,
            "publication_id": PUBLICATION,
            "phase": "AwaitPubrel",
        }
    ],
    accounts=[{"account": "broker", "used": USAGE, "limits": LIMITS}],
)
ok("store-snapshot-after-commit.json", snapshot)
ok(
    "store-snapshot-empty.json",
    env(
        "mqttiumd.store-snapshot",
        run_id="p1-02-example",
        checkpoint="initial",
        backend="sqlite",
        meta={"incarnation": "1", "epoch": "1"},
        sessions=[],
        routes=[],
        operations=[],
        fences=[],
        publications=[],
        obligations=[],
        inbound_exchanges=[],
        accounts=[],
    ),
)
# A snapshot shows what the store holds, sound or not: an obligation without
# its publication is a finding for the oracle, not a malformed document.
ok(
    "store-snapshot-orphan-obligation.json",
    edited(snapshot, lambda v: v.update(publications=[])),
)


def snap_case(name, code, pointer, why, edit):
    bad(f"store-snapshot-{name}.json", code, pointer, why, edited(snapshot, edit))


snap_case(
    "unknown-table",
    "schema-violation",
    "",
    "The tables of a snapshot are a closed set.",
    lambda v: v.update(wills=[]),
)
snap_case(
    "without-fences",
    "schema-violation",
    "",
    "Every table is present, even empty: an absent table is not an empty one.",
    dropped("fences"),
)
snap_case(
    "decision-unknown-outcome",
    "schema-violation",
    "/operations/0/decision",
    "A receipt records a terminal decision; an unknown outcome is not a durable fact.",
    setter(["operations", 0, "decision"], "PendingOrUnknown"),
)
snap_case(
    "recipe-not-hex",
    "schema-violation",
    "/operations/0/recipe_hex",
    "A recipe is its exact canonical bytes.",
    setter(["operations", 0, "recipe_hex"], "MQDR"),
)
snap_case(
    "obligation-without-cause",
    "schema-violation",
    "/obligations/0",
    "The cause of an obligation stays inspectable (DEM-011).",
    dropped("obligations", 0, "cause"),
)
snap_case(
    "operation-listed-twice",
    "duplicate-name",
    "/operations/1/operation_id",
    "An OperationId designates one recipe: one row.",
    lambda v: v["operations"].append(copy.deepcopy(v["operations"][0])),
)
snap_case(
    "obligation-listed-twice",
    "duplicate-name",
    "/obligations/2",
    "A DeliveryId designates one obligation, whatever destination a second row claims.",
    lambda v: v["obligations"].append({**v["obligations"][0], "session": "1"}),
)
snap_case(
    "inbound-slot-listed-twice",
    "duplicate-name",
    "/inbound_exchanges/1",
    "A session has one inbound exchange per Packet Identifier.",
    lambda v: v["inbound_exchanges"].append(
        {**v["inbound_exchanges"][0], "exchange": {"session": "1", "generation": "2"}}
    ),
)
snap_case(
    "session-listed-twice",
    "duplicate-name",
    "/sessions/3/session",
    "A SessionId designates one session.",
    lambda v: v["sessions"].append({**v["sessions"][0], "client_id_hex": "7a"}),
)
snap_case(
    "route-listed-twice",
    "duplicate-name",
    "/routes/1/topic_hex",
    "An exact topic has one route.",
    lambda v: v["routes"].append(copy.deepcopy(v["routes"][0])),
)
snap_case(
    "fence-listed-twice",
    "duplicate-name",
    "/fences/1/operation_id",
    "An identity is fenced once.",
    lambda v: v.update(fences=[{"operation_id": OP}, {"operation_id": OP}]),
)
snap_case(
    "publication-listed-twice",
    "duplicate-name",
    "/publications/1/publication_id",
    "A PublicationId designates one publication.",
    lambda v: v["publications"].append(copy.deepcopy(v["publications"][0])),
)
snap_case(
    "account-listed-twice",
    "duplicate-name",
    "/accounts/1/account",
    "An account is listed once.",
    lambda v: v["accounts"].append(copy.deepcopy(v["accounts"][0])),
)
for member in ("payload_bytes", "exchange_slots", "receipt_slots"):
    snap_case(
        f"account-limit-{member.replace('_', '-')}-overflow",
        "u64-out-of-range",
        f"/accounts/0/limits/{member}",
        "Every limit of an account is 64-bit.",
        setter(["accounts", 0, "limits", member], OVER),
    )
snap_case(
    "payload-length-overflow",
    "u64-out-of-range",
    "/publications/0/payload/length",
    "Sizes are 64-bit.",
    setter(["publications", 0, "payload", "length"], OVER),
)
snap_case(
    "account-usage-overflow",
    "u64-out-of-range",
    "/accounts/0/used/references",
    "Accounts are 64-bit.",
    setter(["accounts", 0, "used", "references"], OVER),
)
snap_case(
    "obligation-session-overflow",
    "u64-out-of-range",
    "/obligations/1/session",
    "Identities inside a list are range-checked as well.",
    setter(["obligations", 1, "session"], OVER),
)


# run results. Qualifying results name files by digest, so their examples
# are built by the test suite against the contracts in use; the static
# corpus holds provisional documents and schema-level cases.
def result(verdict, exit_code, statuses, hits="1", qualification="provisional", faults=True):
    return env(
        "mqttiumd.run-result",
        qualification=qualification,
        run_id="p1-02-example",
        verdict=verdict,
        exit_code=exit_code,
        scope="P1-02 on sim-store, core perimeter without bytes",
        reasons=[],
        assertions=[{"name": name, "status": status} for name, status in statuses],
        faults=[{"id": "lost_admission_result", "expected_hits": "1", "hits": hits}]
        if faults
        else [],
        artifacts=[],
    )


PASSING = [("trace_complete", "PASS"), ("all_required_faults_hit", "PASS")]
ok("run-result-provisional-all-passing.json", result("INCONCLUSIVE", 2, PASSING))
ok(
    "run-result-provisional-fail-with-inconclusive.json",
    result(
        "FAIL",
        1,
        [
            ("one_admission_per_observed_inbound_exchange", "FAIL"),
            ("trace_complete", "INCONCLUSIVE"),
        ],
    ),
)
ok(
    "run-result-provisional-fault-not-hit.json",
    result("INCONCLUSIVE", 2, [("all_required_faults_hit", "INCONCLUSIVE")], hits="0"),
)
ok(
    "run-result-provisional-out-of-scope.json",
    result(
        "OUT_OF_SCOPE",
        3,
        [("trace_complete", "PASS"), ("no_pubrec_observed_before_crash", "OUT_OF_SCOPE")],
    ),
)
ok("run-result-provisional-nothing-evaluated.json", result("INCONCLUSIVE", 2, [], faults=False))

bad(
    "run-result-provisional-pass.json",
    "schema-violation",
    "/verdict",
    "A provisional document lacks the evidence a PASS states.",
    result("PASS", 0, PASSING),
)
bad(
    "run-result-pass-with-only-out-of-scope.json",
    "schema-violation",
    "/verdict",
    "Out-of-scope assertions never add up to a PASS.",
    result("PASS", 0, [("trace_complete", "OUT_OF_SCOPE")]),
)
bad(
    "run-result-qualifying-with-empty-lists.json",
    "schema-violation",
    "/assertions",
    "A qualifying result names its scenario, manifest, trace and contracts, and holds assertions.",
    result("PASS", 0, [], qualification="qualifying", faults=False),
)
bad(
    "run-result-without-qualification.json",
    "schema-violation",
    "",
    "Every result says whether it is provisional or qualifying.",
    (lambda v: (v.pop("qualification"), v)[1])(result("INCONCLUSIVE", 2, PASSING)),
)
bad(
    "run-result-exit-code-mismatch.json",
    "schema-violation",
    "",
    "INCONCLUSIVE exits 2.",
    result("INCONCLUSIVE", 1, PASSING),
)
bad(
    "run-result-inconclusive-hides-fail.json",
    "verdict-inconsistent",
    "/verdict",
    "An established FAIL stands; it is not softened into INCONCLUSIVE.",
    result(
        "INCONCLUSIVE", 2, [("trace_complete", "FAIL"), ("all_required_faults_hit", "INCONCLUSIVE")]
    ),
)
bad(
    "run-result-fail-without-failed-assertion.json",
    "verdict-inconsistent",
    "/verdict",
    "The verdict is the one the assertions determine, neither better nor worse.",
    result("FAIL", 1, PASSING),
)
bad(
    "run-result-out-of-scope-with-nothing-evaluated.json",
    "verdict-inconsistent",
    "/verdict",
    "An empty list concludes nothing; it is inconclusive, not out of scope.",
    result("OUT_OF_SCOPE", 3, [], faults=False),
)
bad(
    "run-result-out-of-scope-with-fault-not-hit.json",
    "verdict-inconsistent",
    "/verdict",
    "An injection that was not reached proves nothing.",
    result("OUT_OF_SCOPE", 3, [("trace_complete", "OUT_OF_SCOPE")], hits="0"),
)
bad(
    "run-result-inconclusive-with-out-of-scope.json",
    "verdict-inconsistent",
    "/verdict",
    "Without failure or doubt, an out-of-scope assertion makes the run out of scope.",
    result(
        "INCONCLUSIVE",
        2,
        [("trace_complete", "PASS"), ("no_pubrec_observed_before_crash", "OUT_OF_SCOPE")],
    ),
)
bad(
    "run-result-duplicate-assertion.json",
    "duplicate-name",
    "/assertions/1/name",
    "An assertion is reported once.",
    result("INCONCLUSIVE", 2, [("trace_complete", "PASS"), ("trace_complete", "PASS")]),
)

# manifest
LISTED = [
    {"path": "contracts/schema/v1/primitives.schema.json", "sha256": "3" * 64, "bytes": "5382"},
    {"path": "contracts/v1/profiles.json", "sha256": "4" * 64, "bytes": "921"},
]
LISTING = "".join(f"{f['sha256']}  {f['path']}\n" for f in LISTED)
manifest = env(
    "mqttiumd.run-manifest",
    artifact={
        "sha256": "5" * 64,
        "bytes": "4194304",
        "built_with": {
            "rustc": "rustc 1.97.0 (2d8144b78 2026-07-07)",
            "target": "x86_64-unknown-linux-gnu",
            "profile": "release",
            "opt_level": "3",
            "debug_assertions": False,
            "rustflags": "",
        },
    },
    checkout={
        "observed": "at-execution",
        "revision": "0" * 40,
        "tree_state": "clean",
        "modified_paths": [],
        "modifications_sha256": hashlib.sha256(b"").hexdigest(),
        "lockfile_sha256": "1" * 64,
        "workspace_manifest_sha256": "2" * 64,
    },
    tools_at_execution={
        "rustc": "rustc 1.97.0 (2d8144b78 2026-07-07)",
        "cargo": "cargo 1.97.0 (c980f4866 2026-06-30)",
    },
    contracts=LISTED,
    contracts_digest=hashlib.sha256(LISTING.encode()).hexdigest(),
    platform={"os": "linux", "arch": "x86_64"},
)


def man(edit):
    value = copy.deepcopy(manifest)
    edit(value)
    return value


ok("run-manifest-content-only.json", manifest)
bad(
    "run-manifest-branch-name-as-revision.json",
    "schema-violation",
    "/checkout/revision",
    "A branch name does not identify a content.",
    man(setter(["checkout", "revision"], "main")),
)
bad(
    "run-manifest-without-artifact.json",
    "schema-violation",
    "",
    "A manifest identifies the binary that ran.",
    man(lambda v: v.pop("artifact")),
)
REVERSED = list(reversed(LISTED))
bad(
    "run-manifest-contracts-out-of-order.json",
    "evidence-mismatch",
    "/contracts/1",
    "Contract files are listed once each, in path order; the digest is defined over that order.",
    man(
        lambda v: v.update(
            contracts=REVERSED,
            contracts_digest=hashlib.sha256(
                "".join(f"{f['sha256']}  {f['path']}\n" for f in REVERSED).encode()
            ).hexdigest(),
        )
    ),
)
bad(
    "run-manifest-digest-of-other-contracts.json",
    "evidence-mismatch",
    "/contracts_digest",
    "The contract digest is the digest of the files the manifest lists.",
    man(setter(["contracts_digest"], "6" * 64)),
)

# tiny transition catalogue and registries
dump(
    EX / "valid/toy.catalog.json",
    env(
        "mqttiumd.transition-catalog",
        id="toy",
        guard_dimensions=[
            {
                "name": "capacity",
                "values": [
                    {"value": "sufficient", "meaning": "Room is available."},
                    {"value": "insufficient", "meaning": "Room is missing."},
                ],
            }
        ],
        event_classes=[
            {"name": "Tick", "meaning": "Work arrives.", "guard_dimensions": ["capacity"]},
            {"name": "Stop", "meaning": "Work ends.", "guard_dimensions": []},
        ],
        effects=["Persist"],
        machines=[
            {"name": "toy", "state_classes": ["Idle", "Busy"], "event_classes": ["Tick", "Stop"]}
        ],
    ),
)
valid.append("contracts/examples/v1/valid/toy.catalog.json")


def toy(rid, state, event, guard, disposition, nxt, required=()):
    return rule(
        rid,
        "toy",
        state,
        event,
        guard,
        disposition,
        nxt,
        list(required),
        [],
        "none",
        ["DEM-015"],
        [],
        [],
    )


TOY = [
    toy(
        "toy.idle.tick",
        "Idle",
        "Tick",
        {"capacity": ["sufficient"]},
        "TRANSITION",
        "Busy",
        ["Persist"],
    ),
    toy("toy.idle.tick.defer", "Idle", "Tick", {"capacity": ["insufficient"]}, "DEFER", "Idle"),
    toy("toy.idle.stop", "Idle", "Stop", {}, "IGNORE_STALE", "Idle"),
    toy("toy.busy.tick", "Busy", "Tick", {}, "REJECT", "Busy"),
    toy("toy.busy.stop", "Busy", "Stop", {}, "TRANSITION", "Idle"),
]


def toy_registry(status="complete", edit=lambda rules: None):
    rules = copy.deepcopy(TOY)
    edit(rules)
    return env(
        "mqttiumd.transition-registry",
        id="toy",
        catalog="contracts/examples/v1/valid/toy.catalog.json",
        status=status,
        rules=rules,
    )


ok("toy.transitions.complete.json", toy_registry())
ok("toy.transitions.seed-with-hole.json", toy_registry("seed", lambda rules: rules.pop()))
bad(
    "toy.transitions.hole.json",
    "registry-hole",
    "/rules",
    "A registry declared complete leaves Busy x Stop without a rule.",
    toy_registry(edit=lambda rules: rules.pop()),
)
bad(
    "toy.transitions.overlap.json",
    "registry-overlap",
    "/rules/5",
    "Two rules cover Idle x Tick; there is no implicit priority.",
    toy_registry(
        edit=lambda rules: rules.append(
            toy("toy.idle.tick.any", "Idle", "Tick", {}, "REJECT", "Idle")
        )
    ),
)
bad(
    "toy.transitions.unknown-state.json",
    "catalog-reference-unknown",
    "/rules/0/next_state_class",
    "The next state class is not in the catalogue.",
    toy_registry("seed", lambda rules: rules[0].update(next_state_class="Flying")),
)
bad(
    "toy.transitions.unknown-guard-value.json",
    "catalog-reference-unknown",
    "/rules/0/guard_partition/capacity/0",
    "A guard value must be defined by the catalogue, not free text.",
    toy_registry("seed", lambda rules: rules[0].update(guard_partition={"capacity": ["plenty"]})),
)
bad(
    "toy.transitions.unknown-requirement.json",
    "requirement-unknown",
    "/rules/0/requirements/0",
    "A rule cites a requirement that does not exist.",
    toy_registry("seed", lambda rules: rules[0].update(requirements=["DEM-999"])),
)
bad(
    "toy.transitions.effect-conflict.json",
    "registry-effect-conflict",
    "/rules/0/forbidden_effects/0",
    "One effect is both required and forbidden.",
    toy_registry("seed", lambda rules: rules[0].update(forbidden_effects=["Persist"])),
)
bad(
    "toy.transitions.unreachable-without-justification.json",
    "schema-violation",
    "/rules/3",
    "UNREACHABLE needs a justification and a witness of the excluding precondition.",
    toy_registry(edit=lambda rules: rules[3].update(disposition="UNREACHABLE")),
)
bad(
    "toy.transitions.catalog-missing.json",
    "file-unreadable",
    "/catalog",
    "The catalogue file does not exist.",
    (lambda v: (v.update(catalog="contracts/examples/v1/valid/none.catalog.json"), v)[1])(
        toy_registry()
    ),
)

# a catalogue too wide for its coverage to be checked: 4^8 points per cell
WIDE = [f"d{n}" for n in range(8)]
ok(
    "wide.catalog.json",
    env(
        "mqttiumd.transition-catalog",
        id="wide",
        guard_dimensions=[
            {
                "name": name,
                "values": [
                    {"value": f"v{n}", "meaning": "One value of a wide guard dimension."}
                    for n in range(4)
                ],
            }
            for name in WIDE
        ],
        event_classes=[
            {
                "name": "Wide",
                "meaning": "An event with eight guard dimensions.",
                "guard_dimensions": WIDE,
            }
        ],
        effects=["Persist"],
        machines=[{"name": "wide", "state_classes": ["Only"], "event_classes": ["Wide"]}],
    ),
)
bad(
    "wide.transitions.json",
    "coverage-budget-exceeded",
    "/catalog",
    "65 536 guard points in one cell exceed the coverage budget; they are counted, never built.",
    env(
        "mqttiumd.transition-registry",
        id="wide",
        catalog="contracts/examples/v1/valid/wide.catalog.json",
        status="seed",
        rules=[],
    ),
)

dump(EX / "index.json", env("mqttiumd.example-index", valid=valid, invalid=invalid))


def derived_on_disk() -> set[str]:
    """Derived files that exist now, so that a stale one is noticed."""
    found = {str(path.relative_to(ROOT)) for path in EX.rglob("*.json")}
    found.add("contracts/v1/requirements.json")
    return found


def main() -> int:
    check = sys.argv[1:] == ["--check"]
    if sys.argv[1:] and not check:
        print(__doc__, file=sys.stderr)
        return 2
    stale = sorted(derived_on_disk() - set(OUTPUTS))
    changed = sorted(
        name
        for name, text in OUTPUTS.items()
        if not (ROOT / name).is_file() or (ROOT / name).read_text() != text
    )
    if check:
        for name in changed:
            print(f"not up to date: {name}", file=sys.stderr)
        for name in stale:
            print(f"not derived by this tool: {name}", file=sys.stderr)
        print(f"{len(OUTPUTS)} derived files, {len(changed)} not up to date, {len(stale)} stale")
        return 1 if changed or stale else 0
    for name in stale:
        (ROOT / name).unlink()
    for name in changed:
        target = ROOT / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(OUTPUTS[name])
    print(f"{len(OUTPUTS)} derived files, {len(changed)} rewritten, {len(stale)} removed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
