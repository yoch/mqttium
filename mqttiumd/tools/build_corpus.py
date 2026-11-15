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
    "scenario-qos0-publish.json",
    scenario(lambda v: (v["steps"][1].pop("packet_id"), v["steps"][1].update(qos=0))),
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


ok(
    "trace-record-header.json",
    record(
        0, "bench-supervisor", 0, "TraceHeader", {"scenario_id": "P1-02", "backend": "sim-store"}
    ),
)
commit = record(
    5, "store-evidence", 0, "StoreCommitStable", {"operation_id": OP}, [("store-adapter", 0)]
)
ok("trace-record-store-commit-stable.json", commit)
ok(
    "trace-record-fault-hit.json",
    record(
        6,
        "fault-injector",
        0,
        "FaultHit",
        {"fault_id": "lost_admission_result", "point": "store.result.ready", "occurrence": "1"},
        [("store-evidence", 0)],
    ),
)
ok(
    "trace-record-peer-packet-observed.json",
    record(
        20,
        "peer",
        3,
        "PeerPacketObserved",
        {"client": "producer", "packet_kind": "PUBREC", "packet_id": 7, "bytes_hex": "5004000700"},
    ),
)
ok(
    "trace-record-footer.json",
    record(31, "bench-supervisor", 4, "TraceFooter", {"record_count": "32", "complete": True}),
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
    "trace-record-operation-overflow.json",
    "u64-out-of-range",
    "/payload/operation_id/sequence",
    "Identity components are 64-bit.",
    rec(setter(["payload", "operation_id", "sequence"], "18446744073709551616")),
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
