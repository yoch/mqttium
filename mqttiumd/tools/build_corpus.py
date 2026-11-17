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

After editing a chapter digest, a decision record, the base scenario, the fixture or the
fault or assertion catalogue (invalid examples are edits of them), run it without ``--check`` and commit the
result. Standard library only.
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
    "tests/scenarios/crash/K1.json",
    "tests/scenarios/crash/K2b.json",
    "tests/scenarios/crash/K3.json",
    "tests/scenarios/p1/P1-02.json",
    "tests/scenarios/qabs/QABS-A.json",
    "tests/scenarios/qabs/QABS-B.json",
    "tests/scenarios/qabs/QABS-C.json",
    "tests/fixtures/p1/three-persistent-sessions.json",
]
invalid = []


VALID = {}


def ok(name, value):
    dump(EX / "valid" / name, value)
    valid.append(f"contracts/examples/v1/valid/{name}")
    VALID[name] = value


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


def scenario(edit, base=None):
    value = copy.deepcopy(BASE if base is None else base)
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
    "scenario-exchange-used-before-binding.json",
    "binding-undefined",
    "/steps/1/exchange",
    "An exchange is resumed only after the step that binds it, not merely somewhere in the file.",
    scenario(lambda v: v["steps"].insert(1, copy.deepcopy(v["steps"][7]))),
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
            # The first resolution reads the absence and cannot write its
            # fence: that, not the held command, is why it answers unknown.
            fault("refused_fence", "store.fence.transit", "refuse_fence"),
        ],
        steps=[
            BASE["steps"][0],
            BASE["steps"][1],
            drive_fault("held_admission"),
            {"kind": "FireDeadline", "max_steps": "512"},
            drive_fault("refused_fence"),
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
# LeaveBarrier: QABS-A holds the adapter at the barrier of the applied
# commit, inspects there, then leaves the barrier.
QABS_A = json.loads((ROOT / "tests/scenarios/qabs/QABS-A.json").read_text())
LEAVE = next(i for i, step in enumerate(QABS_A["steps"]) if step["kind"] == "LeaveBarrier")


def qabs_a(edit):
    value = copy.deepcopy(QABS_A)
    edit(value)
    return value


bad(
    "scenario-fence-fault-of-a-command-nobody-selected.json",
    "fault-match-invalid",
    "/faults/1/match",
    "A fence fault names the resolved operation by its command, which an earlier fault must select.",
    qabs_a(setter(["faults", 1, "match", "command_kind"], "ReleaseInbound")),
)
# A kill inside a transaction (record 0020): the barrier of QABS-A moved to
# the two points of the SQLite store's transaction.
for point, name in (("store.commit.prepared", "prepared"), ("store.commit.logged", "logged")):
    ok(f"scenario-barrier-at-commit-{name}.json", qabs_a(setter(["faults", 2, "point"], point)))
    bad(
        f"scenario-hold-at-commit-{name}.json",
        "fault-action-unknown",
        "/faults/2/action",
        f"{point} only stops the writer at a barrier: no command is held there.",
        qabs_a(
            lambda v, point=point: (
                v["faults"][2].update(point=point, action="hold_command"),
                v["steps"].pop(LEAVE),
            )
        ),
    )
    bad(
        f"scenario-result-at-commit-{name}.json",
        "fault-match-invalid",
        "/faults/2/match/result_of",
        f"{point} carries a command; no result exists there yet.",
        qabs_a(
            lambda v, point=point: (
                v["faults"][2].update(point=point),
                v["faults"][2]["match"].update(result_of="Persist"),
            )
        ),
    )
bad(
    "scenario-leave-of-a-held-command.json",
    "held-item-mismatch",
    f"/steps/{LEAVE}/fault",
    "LeaveBarrier names a fault that holds a command, not the adapter at a barrier.",
    qabs_a(setter(["steps", LEAVE, "fault"], "held_fence")),
)
bad(
    "scenario-leave-before-the-barrier.json",
    "step-order-invalid",
    f"/steps/{LEAVE - 2}",
    "LeaveBarrier comes before any step drove the broker to the barrier.",
    qabs_a(lambda v: v["steps"].insert(LEAVE - 2, v["steps"].pop(LEAVE))),
)
bad(
    "scenario-leave-twice.json",
    "step-order-invalid",
    f"/steps/{LEAVE + 1}",
    "The adapter leaves a barrier once.",
    qabs_a(lambda v: v["steps"].insert(LEAVE + 1, copy.deepcopy(v["steps"][LEAVE]))),
)
bad(
    "scenario-leave-of-an-undeclared-fault.json",
    "binding-undefined",
    f"/steps/{LEAVE}/fault",
    "LeaveBarrier names a fault that is not declared.",
    qabs_a(setter(["steps", LEAVE, "fault"], "applied")),
)

# A permanent delay (record 0018): every answer of a resolution of op_A is
# delayed, each delivered once six newer ones are held, and the bench fires
# the deadlines while it waits. The scenario does not fix at which
# delivery the core progresses.
F3 = json.loads((ROOT / "tests/scenarios/audit/F3-steady-lag.json").read_text())
LATE = {
    "id": "late_answers",
    "point": "store.result.ready",
    "match": {
        "command_kind": "AdmitPublication",
        "publication_binding": "pub_A",
        "result_of": "ResolveAndFence",
    },
    "from_occurrence": "1",
    "action": "delay_result",
    "lag": 6,
    "expected_hits_at_least": "1",
}
PERMANENT = copy.deepcopy(F3)
PERMANENT.update(
    id="F3-PERMANENT",
    faults=[F3["faults"][0], LATE],
    steps=[
        *F3["steps"][:4],
        {
            "kind": "DriveUntil",
            "event": "PeerPacketObserved",
            "client": "producer",
            "packet_kind": "PUBREC",
            "packet_id": 7,
            "fire_deadlines": True,
            "max_steps": "4096",
        },
        *F3["steps"][-3:],
    ],
)
LATE_AT = "/faults/1"
FIRE_AT = 4


def permanent(edit):
    return scenario(edit, PERMANENT)


def late(**changes):
    def edit(value):
        for key, new in changes.items():
            if new is None:
                value["faults"][1].pop(key, None)
            else:
                value["faults"][1][key] = new

    return edit


ok("scenario-permanent-delay.json", PERMANENT)
for name, changes, pointer, why in [
    (
        "repeated-fault-with-one-occurrence",
        {"occurrence": "1"},
        LATE_AT,
        "A fault names one occurrence or every occurrence from one on, not both.",
    ),
    (
        "fault-without-occurrence",
        {"from_occurrence": None},
        LATE_AT,
        "A fault names one occurrence or every occurrence from one on.",
    ),
    (
        "repeated-fault-with-exact-hits",
        {"expected_hits": "3"},
        LATE_AT,
        "How often a repeated fault is reached depends on the product: only a minimum is expected.",
    ),
    (
        "repeated-fault-without-expected-hits",
        {"expected_hits_at_least": None},
        LATE_AT,
        "A repeated fault says how often it must at least be reached.",
    ),
    (
        "repeated-fault-expected-zero-times",
        {"expected_hits_at_least": "0"},
        f"{LATE_AT}/expected_hits_at_least",
        "A repeated fault is expected at least once.",
    ),
    (
        "repeated-fault-binding-an-operation",
        {"bind_operation_as": "op_B"},
        LATE_AT,
        "A repeated fault selects many items: it binds no operation.",
    ),
    (
        "lag-of-zero",
        {"lag": 0},
        f"{LATE_AT}/lag",
        "A delayed answer is delivered once at least one newer one is held.",
    ),
    ("lag-beyond-its-bound", {"lag": 65}, f"{LATE_AT}/lag", "The lag is bounded."),
    (
        "repeated-delay-without-lag",
        {"lag": None},
        LATE_AT,
        "Answers delayed without end and never delivered: a repeated delay has a lag.",
    ),
    (
        "lag-on-another-action",
        {"action": "drop_result"},
        LATE_AT,
        "Only a delayed answer has a lag.",
    ),
]:
    bad(f"scenario-{name}.json", "schema-violation", pointer, why, permanent(late(**changes)))
bad(
    "scenario-lag-on-one-occurrence.json",
    "schema-violation",
    "/faults/0",
    "A lag counts newer answers of the same selection: a fault of one occurrence has none.",
    permanent(lambda v: v["faults"][0].update(action="delay_result", lag=2)),
)
bad(
    "scenario-fire-deadlines-not-a-boolean.json",
    "schema-violation",
    f"/steps/{FIRE_AT}/fire_deadlines",
    "Whether the bench fires the deadlines while it waits is true or false.",
    permanent(setter(["steps", FIRE_AT, "fire_deadlines"], "yes")),
)
bad(
    "scenario-repeated-fault-from-beyond-64-bits.json",
    "u64-out-of-range",
    f"{LATE_AT}/from_occurrence",
    "The first occurrence of a repeated fault is 64-bit.",
    permanent(late(from_occurrence="18446744073709551616")),
)
bad(
    "scenario-repeated-fault-expected-beyond-64-bits.json",
    "u64-out-of-range",
    f"{LATE_AT}/expected_hits_at_least",
    "The minimum of a repeated fault is 64-bit.",
    permanent(late(expected_hits_at_least="18446744073709551616")),
)
bad(
    "scenario-held-result-of-a-repeated-fault.json",
    "held-item-mismatch",
    f"/steps/{FIRE_AT + 1}/fault",
    "A repeated delay holds many answers and delivers each by its lag: no step delivers one.",
    permanent(
        lambda v: v["steps"].insert(
            FIRE_AT + 1, {"kind": "DeliverHeldResult", "fault": "late_answers", "max_steps": "512"}
        )
    ),
)


def single_before(occurrence, start):
    """A fault of one occurrence, declared before the repeated one from ``start``."""

    def edit(value):
        late = value["faults"][1]
        late["from_occurrence"] = start
        value["faults"].insert(
            1,
            {
                **{key: late[key] for key in ("point", "match")},
                "id": "single",
                "occurrence": occurrence,
                "action": "drop_result",
                "expected_hits": "1",
            },
        )

    return edit


ok(
    "scenario-repeated-fault-after-the-occurrence-of-another.json",
    permanent(single_before("3", "5")),
)
bad(
    "scenario-one-occurrence-overlapped-by-a-later-repeated-fault.json",
    "fault-match-invalid",
    "/faults/2/match",
    "A repeated fault from the third occurrence on selects the seventh, which another fault selects.",
    permanent(single_before("7", "3")),
)
bad(
    "scenario-two-repeated-faults-of-one-selection.json",
    "fault-match-invalid",
    "/faults/2/match",
    "Two repeated faults of one selection both select every occurrence beyond the later start.",
    permanent(
        lambda v: v["faults"].append(
            {
                **{key: v["faults"][1][key] for key in ("point", "match")},
                "id": "later",
                "from_occurrence": "40",
                "action": "drop_result",
                "expected_hits_at_least": "1",
            }
        )
    ),
)
bad(
    "scenario-repeated-fault-overlapping-one-occurrence.json",
    "fault-match-invalid",
    "/faults/2/match",
    "A repeated fault from the first occurrence on selects the third too.",
    permanent(
        lambda v: v["faults"].append(
            {
                **{k: v["faults"][1][k] for k in ("point", "match")},
                "id": "third",
                "occurrence": "3",
                "action": "drop_result",
                "expected_hits": "1",
            }
        )
    ),
)

RELEASE, DELIVER = 6, 11


def held(edit):
    return scenario(edit, HELD)


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
    "scenario-second-wait-on-a-holding-fault.json",
    "step-order-invalid",
    f"/steps/{RELEASE + 1}",
    "A fault that holds something holds one item in the life of a scenario: it is waited for once.",
    held(lambda v: v["steps"].insert(RELEASE + 1, drive_fault("held_admission"))),
)
bad(
    "scenario-holding-fault-expected-twice.json",
    "held-item-mismatch",
    "/faults/0/expected_hits",
    "A fault that holds something is reached once: a second item would have no step to release it.",
    held(setter(["faults", 0, "expected_hits"], "2")),
)
ok(
    "scenario-hold-reached-after-a-restart.json",
    held(
        lambda v: v["steps"].__setitem__(
            slice(2, 2), [BASE["steps"][4], BASE["steps"][5], BASE["steps"][0]]
        )
    ),
)
ok(
    "scenario-command-held-to-the-end.json",
    held(
        lambda v: v.update(
            faults=v["faults"][:1],
            steps=[*v["steps"][:3], {"kind": "InspectDurableState", "checkpoint": "held"}],
        )
    ),
)
bad(
    "scenario-wait-for-a-fault-never-expected.json",
    "step-order-invalid",
    "/steps/7",
    "A scenario does not wait for an injection it announces will never be reached.",
    held(setter(["faults", 2, "expected_hits"], "0")),
)
bad(
    "scenario-two-faults-select-one-result.json",
    "fault-match-invalid",
    "/faults/2/match",
    "Without result_of the delay counts over every result of the operation: its second may be the "
    "first of the Persist, which another fault drops.",
    held(
        lambda v: (
            v["faults"][1]["match"].pop("result_of"),
            v["faults"][1].update(occurrence="2"),
        )
    ),
)
bad(
    "scenario-two-faults-select-one-occurrence.json",
    "fault-match-invalid",
    "/faults/4/match",
    "Two faults name the same occurrence of the same result: only one could act.",
    held(
        lambda v: v["faults"].append(
            {**copy.deepcopy(v["faults"][2]), "id": "duplicated", "action": "duplicate_result"}
        )
    ),
)
bad(
    "scenario-release-of-a-refused-fence.json",
    "held-item-mismatch",
    f"/steps/{RELEASE}/fault",
    "A refused fence is not written and not kept: nothing is left to release.",
    held(setter(["steps", RELEASE, "fault"], "refused_fence")),
)
bad(
    "scenario-refuse-fence-where-no-fence-waits.json",
    "fault-action-unknown",
    "/faults/3/action",
    "Only the point where a fence waits offers to refuse it.",
    held(setter(["faults", 3, "point"], "store.commit.applied")),
)
bad(
    "scenario-two-faults-one-without-binding.json",
    "fault-match-invalid",
    "/faults/4/match",
    "A fault without a publication binding counts over every publication: its second command "
    "may be the first of the publication another fault names.",
    held(
        lambda v: v["faults"].append(
            {
                "id": "any_admission",
                "point": "store.command.transit",
                "match": {"command_kind": "AdmitPublication"},
                "occurrence": "2",
                "action": "hold_command",
                "expected_hits": "1",
            }
        )
    ),
)
bad(
    "scenario-deliver-while-crashed.json",
    "step-order-invalid",
    f"/steps/{DELIVER + 1}",
    "One misplaced step is one diagnostic: the hand-over is not judged again while the broker is down.",
    held(
        lambda v: (
            v["steps"].insert(DELIVER, BASE["steps"][4]),
            v["steps"].insert(DELIVER + 2, BASE["steps"][5]),
            v["steps"].insert(DELIVER + 3, BASE["steps"][0]),
        )
    ),
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
# The catalogue is a source: it is listed by its own path, like the fixture.
valid.append("contracts/v1/fault-points.json")
bad(
    "fault-catalog-result-action-on-a-command-point.json",
    "fault-action-unknown",
    "/points/0/actions/1",
    "A point that carries a command cannot delay a result.",
    (lambda v: (v["points"][0]["actions"].append("delay_result"), v)[1])(copy.deepcopy(FAULTS)),
)
READY_AT = next(i for i, p in enumerate(FAULTS["points"]) if p["point"] == "store.result.ready")
bad(
    "fault-catalog-refuse-fence-on-a-result-point.json",
    "fault-action-unknown",
    f"/points/{READY_AT}/actions/{len(FAULTS['points'][READY_AT]['actions'])}",
    "A fence is a command of the writer: a point that carries a result cannot refuse one.",
    (lambda v: (v["points"][READY_AT]["actions"].append("refuse_fence"), v)[1])(
        copy.deepcopy(FAULTS)
    ),
)
bad(
    "fault-catalog-action-nobody-defines.json",
    "fault-action-unknown",
    "/points/0/actions/0",
    "The actions of an injection point are a closed vocabulary.",
    (lambda v: (v["points"][0].update(actions=["teleport"]), v)[1])(copy.deepcopy(FAULTS)),
)
bad(
    "fault-catalog-point-without-carries.json",
    "schema-violation",
    "/points/0",
    "Every injection point says whether a command or a result passes through it.",
    (lambda v: (v["points"][0].pop("carries"), v)[1])(copy.deepcopy(FAULTS)),
)

# assertion catalogue
ASSERTION_CATALOG = json.loads((V1 / "assertions.json").read_text())
bad(
    "assertion-catalog-entry-without-every-run.json",
    "schema-violation",
    "/assertions/0",
    "Every assertion says whether every run owes it or only the scenarios that list it.",
    (lambda v: (v["assertions"][0].pop("every_run", None), v)[1])(copy.deepcopy(ASSERTION_CATALOG)),
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
    "fixture-topic-limit-over-32-bits.json",
    "u32-out-of-range",
    "/capacities/max_topic_bytes",
    "The bound on a topic is a 32-bit quantity, in a fixture as in a snapshot or a trace.",
    fix(setter(["capacities", "max_topic_bytes"], "4294967296")),
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
# K1: the result the store produced, handed over by the adapter, consumed
# by the core. And the opening of an epoch (records agreed with lot L2).
HANDED = record(
    8,
    "store-adapter",
    3,
    "StoreResultHandedOver",
    {"effect_id": EFFECT, "operation_id": OP, "outcome": "Committed"},
    [("store-evidence", 1)],
)
ok("trace-record-store-result-handed-over.json", HANDED)
ATTEMPT = {"startup_hex": "02" * 16, "ticket": "1"}
ok(
    "trace-record-opening-submitted.json",
    record(
        1,
        "store-adapter",
        0,
        "OpeningSubmitted",
        {"attempt": ATTEMPT, "incarnation": "1", "after": "1"},
    ),
)
ok(
    "trace-record-store-epoch-opened.json",
    record(
        2,
        "store-evidence",
        0,
        "StoreEpochOpened",
        {"incarnation": "1", "epoch": "2", "attempt": ATTEMPT},
        [("store-adapter", 0)],
    ),
)
REPLY = record(
    3,
    "broker-core",
    0,
    "OpeningReplyDelivered",
    {"attempt": ATTEMPT, "incarnation": "1", "after": "1", "outcome": "Committed", "epoch": "2"},
    [("store-adapter", 0)],
)
ok("trace-record-opening-reply-delivered.json", REPLY)
ok(
    "trace-record-opening-reply-rejected.json",
    record(
        3,
        "broker-core",
        0,
        "OpeningReplyDelivered",
        {
            "attempt": ATTEMPT,
            "incarnation": "1",
            "after": "1",
            "outcome": "RejectedCertain",
            "reject_reason": "PredecessorMoved",
        },
        [("store-adapter", 0)],
    ),
)
ok(
    "trace-record-opening-abandoned.json",
    record(4, "broker-core", 1, "OpeningAbandoned", {"attempt": ATTEMPT}, [("broker-core", 0)]),
)
ok(
    "trace-record-epoch-grant-handed-over.json",
    record(
        5,
        "broker-core",
        2,
        "EpochGrantHandedOver",
        {"incarnation": "1", "epoch": "2"},
        [("broker-core", 0)],
    ),
)
# A run that stopped before its last step says why (the bench): no action
# was eligible and the awaited event had not happened, or the budget of
# steps ran out.
BLOCKED = record(
    9,
    "bench-supervisor",
    3,
    "StepBlocked",
    {
        "step": "19",
        "step_kind": "DriveUntil",
        "cause": "quiescent",
        "armed_deadlines": "1",
        "holding_faults": ["hold_result"],
    },
)
ok("trace-record-step-blocked.json", BLOCKED)
# A real kill (record 0019): the broker runs as a child process, the
# supervisor sends SIGKILL and records the status wait() returned, before
# BrokerCrashed. The trace header says the run claims it.
KILLED = record(
    10,
    "bench-supervisor",
    4,
    "BrokerKilled",
    {"signal_sent": "SIGKILL", "wait_status": {"signal": 9}},
    [("broker-core", 7)],
)
ok("trace-record-broker-killed.json", KILLED)
CHILD_HEADER = copy.deepcopy(HEADER)
CHILD_HEADER["payload"]["broker"] = "child-process"
ok("trace-record-header-of-a-child-process.json", CHILD_HEADER)
REJECTED = record(
    7,
    "store-evidence",
    1,
    "StoreResultReady",
    {
        "effect_id": EFFECT,
        "operation_id": OP,
        "outcome": "RejectedCertain",
        "reject_reason": "Fenced",
    },
    [("store-evidence", 0)],
)
ok("trace-record-store-result-rejected.json", REJECTED)
# The store refuses a recipe that differs from the one an OperationId
# already designates: no outcome, a violation (StoreViolation).
VIOLATION = record(
    7,
    "store-evidence",
    1,
    "StoreResultReady",
    {
        "effect_id": EFFECT,
        "operation_id": OP,
        "outcome": "Violation",
        "violation": "RecipeMismatch",
    },
    [("store-evidence", 0)],
)
ok("trace-record-store-result-violation.json", VIOLATION)

ok(
    "trace-record-store-result-delivered-rejected.json",
    record(
        8,
        "broker-core",
        2,
        "StoreResultDelivered",
        {
            "effect_id": EFFECT,
            "operation_id": OP,
            "outcome": "RejectedCertain",
            "reject_reason": "CapacityExceeded",
        },
        [("store-evidence", 1)],
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
policy_page = copy.deepcopy(PAGE)
policy_page["facts"][1].update(expiry="OnDisconnect", opened_by=OP)
ok("trace-record-recovery-page-session-policy.json", record(14, "store-adapter", 2, "RecoveryPageRead", policy_page))
bad_policy_page = copy.deepcopy(policy_page)
bad_policy_page["facts"][1].update(expiry="Sometimes")
bad("trace-record-recovery-session-policy-invalid.json", "schema-violation", "/payload/facts/1/expiry",
    "A runtime recovery inventory validates policy before the core consumes it.",
    record(14, "store-adapter", 2, "RecoveryPageRead", bad_policy_page))
bad_attachment_page = copy.deepcopy(policy_page)
bad_attachment_page["facts"][1]["opened_by"].update(sequence="18446744073709551616")
bad("trace-record-recovery-session-attachment-overflow.json", "u64-out-of-range", "/payload/facts/1/opened_by/sequence",
    "The opening attachment is a complete checked OperationId in recovery too.",
    record(14, "store-adapter", 2, "RecoveryPageRead", bad_attachment_page))

RECEIPT_FRONTIER = {
    "origin": {"incarnation": "1", "epoch": "1", "owner": "0"},
    "through": "1", "publication_through": "1", "payload_through": "1",
}
frontier_page = copy.deepcopy(PAGE)
frontier_page["facts"].insert(-1, {"kind": "ReceiptFrontier", **RECEIPT_FRONTIER})
ok("trace-record-recovery-page-receipt-frontier.json",
   record(14, "store-adapter", 2, "RecoveryPageRead", frontier_page))
late_frontier_page = copy.deepcopy(frontier_page)
late_frontier_page["facts"][-2:] = reversed(late_frontier_page["facts"][-2:])
bad("trace-record-recovery-page-late-frontier.json", "rows-out-of-order", "/payload/facts/4/kind",
    "Compact retirement frontiers precede operation facts; they are not rejection facts.",
    record(14, "store-adapter", 2, "RecoveryPageRead", late_frontier_page))
OUTBOUND_QUEUED = {
    "kind": "OutboundDelivery",
    "delivery_id": {"publication_id": PUBLICATION, "index": 0},
    "target_session": "2",
    "target_revision": "1",
    "qos": 1,
    "claim": None,
}
OUTBOUND_CLAIMED = {
    **OUTBOUND_QUEUED,
    "claim": {"operation_id": OP, "packet_id": 1},
}
for name, fact in [("queued", OUTBOUND_QUEUED), ("claimed", OUTBOUND_CLAIMED)]:
    page = copy.deepcopy(PAGE)
    page["facts"].insert(-1, fact)
    ok(f"trace-record-recovery-page-outbound-{name}.json",
       record(14, "store-adapter", 2, "RecoveryPageRead", page))
qos0_page = copy.deepcopy(PAGE)
qos0_page["facts"].insert(-1, {**OUTBOUND_QUEUED, "qos": 0})
ok("trace-record-recovery-page-outbound-qos0.json",
   record(14, "store-adapter", 2, "RecoveryPageRead", qos0_page))
qos0_claim_record = record(14, "store-adapter", 2, "RecoveryPageRead", copy.deepcopy(qos0_page))
qos0_claim_record["payload"]["facts"][3]["claim"] = OUTBOUND_CLAIMED["claim"]
bad("trace-record-recovery-page-qos0-claim.json", "schema-violation", "/payload/facts/3/claim",
    "QoS 0 has no outgoing Packet Identifier or QoS 1 claim.",
    qos0_claim_record)
ok(
    "trace-record-recovery-page-delivered.json",
    record(15, "broker-core", 3, "RecoveryPageDelivered", PAGE, [("store-adapter", 2)]),
)


# An inventory with the limits, the route table and its routes (record
# 0014). Routes are in the order of their topic bytes: a prefix first, a
# byte 0xff after every ASCII byte, and two targets of one topic together.
def route(topic, target, qos=1):
    return {
        "kind": "Route",
        "topic_hex": topic.hex(),
        "target": str(target),
        "target_revision": "1",
        "max_qos": qos,
    }


PAGE_FULL = {
    "run_epoch": "2",
    "page": "0",
    "last": True,
    "facts": [
        {"kind": "Epoch", "incarnation": "1", "epoch": "2"},
        {
            "kind": "Limits",
            "max_targets_per_admission": "4",
            "max_topic_bytes": "256",
            "max_payload_bytes": "1024",
        },
        {"kind": "RouteTable", "revision": "2"},
        route(b"p1", 2),
        route(b"p1/example", 2),
        route(b"p1/example", 3, qos=2),
        route(b"\xff", 3, qos=0),
        *PAGE["facts"][1:],
    ],
}
FULL_READ = record(14, "store-adapter", 2, "RecoveryPageRead", PAGE_FULL)
ok("trace-record-recovery-page-with-routes-and-limits.json", FULL_READ)
ok(
    "trace-record-recovery-page-of-routes-only.json",
    record(
        15,
        "store-adapter",
        3,
        "RecoveryPageRead",
        {"run_epoch": "2", "page": "1", "last": False, "facts": [route(b"p1/z", 2)]},
    ),
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
    "trace-record-frame-eligible-refusal-without-quota.json",
    {**ELIGIBLE, "payload": {**ELIGIBLE["payload"], "reason": "ImplementationSpecificError"}},
)
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
trace_case(
    "opening-committed-without-its-epoch",
    REPLY,
    "/payload",
    "A committed opening names the epoch it opened.",
    dropped("payload", "epoch"),
)
trace_case(
    "opening-rejected-with-an-epoch",
    REPLY,
    "/payload",
    "A rejected opening opened nothing: it names a reason, not an epoch.",
    lambda value: value["payload"].update(
        outcome="RejectedCertain", reject_reason="PredecessorMoved"
    ),
)
trace_case(
    "opening-rejected-for-an-unknown-reason",
    REPLY,
    "/payload/reject_reason",
    "The reasons of a rejected opening are those of OpeningRejection.",
    lambda value: (
        value["payload"].pop("epoch"),
        value["payload"].update(outcome="RejectedCertain", reject_reason="Busy"),
    ),
)
trace_case(
    "handed-over-rejection-without-its-reason",
    HANDED,
    "/payload",
    "What the adapter hands over is the result the store produced, reason included.",
    setter(["payload", "outcome"], "RejectedCertain"),
)
trace_case(
    "step-blocked-for-an-unknown-cause",
    BLOCKED,
    "/payload/cause",
    "A run stops because nothing was eligible or because its budget ran out.",
    setter(["payload", "cause"], "timeout"),
)
trace_case(
    "step-blocked-overflow",
    BLOCKED,
    "/payload/step",
    "The index of a step is 64-bit.",
    setter(["payload", "step"], "18446744073709551616"),
    code="u64-out-of-range",
)
trace_case(
    "step-blocked-deadlines-overflow",
    BLOCKED,
    "/payload/armed_deadlines",
    "The count of armed deadlines is 64-bit.",
    setter(["payload", "armed_deadlines"], "18446744073709551616"),
    code="u64-out-of-range",
)
trace_case(
    "step-blocked-without-what-waits",
    BLOCKED,
    "/payload",
    "A stopped run says what still waits: the deadlines armed and not fired.",
    dropped("payload", "armed_deadlines"),
)
trace_case(
    "step-blocked-without-what-is-held",
    BLOCKED,
    "/payload",
    "A stopped run says which faults still hold an item.",
    dropped("payload", "holding_faults"),
)
trace_case(
    "step-blocked-naming-a-fault-twice",
    BLOCKED,
    "/payload/holding_faults",
    "Each fault that holds an item is named once.",
    setter(["payload", "holding_faults"], ["hold_result", "hold_result"]),
)
trace_case(
    "broker-killed-by-another-signal",
    KILLED,
    "/payload/signal_sent",
    "The supervisor kills the broker with SIGKILL, the one signal it cannot catch.",
    setter(["payload", "signal_sent"], "SIGTERM"),
)
trace_case(
    "broker-killed-that-exited",
    KILLED,
    "/payload/wait_status",
    "A status of a process that exited is no kill: it is not written by this record.",
    setter(["payload", "wait_status"], {"exited": 0}),
)
trace_case(
    "broker-killed-by-signal-zero",
    KILLED,
    "/payload/wait_status/signal",
    "A signal number is from 1 to 64.",
    setter(["payload", "wait_status", "signal"], 0),
)
trace_case(
    "broker-killed-by-signal-sixty-five",
    KILLED,
    "/payload/wait_status/signal",
    "A signal number is from 1 to 64.",
    setter(["payload", "wait_status", "signal"], 65),
)
trace_case(
    "broker-killed-with-its-pid",
    KILLED,
    "/payload",
    "The trace names no pid: the same seed gives the same bytes; the pid goes to campaign.json.",
    lambda v: v["payload"].update(pid=4242),
)
trace_case(
    "broker-killed-without-its-status",
    KILLED,
    "/payload",
    "The record states the status wait() returned.",
    dropped("payload", "wait_status"),
)
trace_case(
    "broker-killed-stated-by-the-core",
    KILLED,
    "/origin_component",
    "Only the supervisor kills the broker.",
    setter(["origin_component"], "broker-core"),
)
trace_case(
    "header-of-an-unknown-topology",
    HEADER,
    "/payload/broker",
    "A broker runs in the process of the bench or as a child process.",
    lambda v: v["payload"].update(broker="container"),
)
trace_case(
    "violation-without-its-kind",
    VIOLATION,
    "/payload",
    "A violation says which one: the store reports no outcome, a violation of the contract.",
    dropped("payload", "violation"),
)
trace_case(
    "commit-with-a-violation",
    VIOLATION,
    "/payload",
    "Only a violation names a violation.",
    setter(["payload", "outcome"], "Committed"),
)
trace_case(
    "rejection-without-its-reason",
    REJECTED,
    "/payload",
    "A certain rejection says why: the reason of the refusing PUBREC follows from it.",
    dropped("payload", "reject_reason"),
)
trace_case(
    "commit-with-a-rejection-reason",
    REJECTED,
    "/payload",
    "Only a certain rejection has a reason.",
    setter(["payload", "outcome"], "Committed"),
)
trace_case(
    "rejection-of-an-unknown-reason",
    REJECTED,
    "/payload/reject_reason",
    "The reasons of a rejection are those of the contract.",
    setter(["payload", "reject_reason"], "Busy"),
)
trace_case(
    "recovered-facts-rank-decreases",
    FULL_READ,
    "/payload/facts/8/kind",
    "The facts of a page are in the order of the inventory: a route never follows a session.",
    setter(["payload", "facts", 8], route(b"p1/z", 2)),
    code="rows-out-of-order",
)
trace_case(
    "recovered-epoch-twice",
    FULL_READ,
    "/payload/facts/1/kind",
    "An inventory holds its epoch once.",
    setter(["payload", "facts", 1], PAGE_FULL["facts"][0]),
    code="duplicate-name",
)
trace_case(
    "recovered-limits-twice",
    FULL_READ,
    "/payload/facts/2/kind",
    "An inventory holds its limits once.",
    setter(["payload", "facts", 2], PAGE_FULL["facts"][1]),
    code="duplicate-name",
)
trace_case(
    "recovered-routes-unsorted",
    FULL_READ,
    "/payload/facts/5/topic_hex",
    "Routes are sorted by the bytes of their topic: a prefix comes before what it prefixes.",
    setter(["payload", "facts", 5, "topic_hex"], b"p1".hex()),
    code="rows-out-of-order",
)
trace_case(
    "recovered-route-qos-3",
    FULL_READ,
    "/payload/facts/3/max_qos",
    "The QoS of a route is 0, 1 or 2.",
    setter(["payload", "facts", 3, "max_qos"], 3),
)
trace_case(
    "recovered-limit-of-targets-as-a-number",
    FULL_READ,
    "/payload/facts/1/max_targets_per_admission",
    "A bound is a decimal string, in a trace as in a snapshot: one quantity, one encoding.",
    setter(["payload", "facts", 1, "max_targets_per_admission"], 4),
)
trace_case(
    "recovered-topic-limit-over-32-bits",
    FULL_READ,
    "/payload/facts/1/max_topic_bytes",
    "The bound on a topic is a 32-bit quantity.",
    setter(["payload", "facts", 1, "max_topic_bytes"], "4294967296"),
    code="u32-out-of-range",
)
trace_case(
    "recovered-payload-limit-overflow",
    FULL_READ,
    "/payload/facts/1/max_payload_bytes",
    "The bound on a payload is 64-bit.",
    setter(["payload", "facts", 1, "max_payload_bytes"], "18446744073709551616"),
    code="u64-out-of-range",
)
trace_case(
    "recovered-route-target-overflow",
    FULL_READ,
    "/payload/facts/4/target",
    "The session a route targets is a 64-bit identity.",
    setter(["payload", "facts", 4, "target"], "18446744073709551616"),
    code="u64-out-of-range",
)
trace_case(
    "recovered-route-target-revision-overflow",
    FULL_READ,
    "/payload/facts/4/target_revision",
    "The revision of a route target is 64-bit.",
    setter(["payload", "facts", 4, "target_revision"], "18446744073709551616"),
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

ok(
    "trace-record-store-fence-refused.json",
    record(
        10,
        "store-evidence",
        2,
        "StoreFenceRefused",
        {"effect_id": RESOLVE, "operation_id": OP},
        [("fault-injector", 1)],
    ),
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

# Typed failures are supervisor observations, even when startup has not
# reached Started. The process incarnation is distinct from the observer's.
READ_FAILED = record(
    14, "bench-supervisor", 2, "DurableReadFailed",
    {"code": "InvalidData", "phase": "Recovery", "process_incarnation": "2", "page": "1"},
)
CONTROL_FAILED = record(
    15, "bench-supervisor", 3, "ProcessControlFailed",
    {"code": "TruncatedFrame", "phase": "KillDrain", "process_incarnation": "2"},
)
ok("trace-record-durable-read-failed.json", READ_FAILED)
ok("trace-record-process-control-failed.json", CONTROL_FAILED)
ok("trace-record-durable-lookup-failed.json", edited(
    READ_FAILED, lambda v: v["payload"].update(code="Io", phase="Lookup")
))
for case, base, pointer, edit in [
    ("read-failure-unknown-code", READ_FAILED, "/payload/code", setter(["payload", "code"], "rusqlite error text")),
    ("read-failure-wrong-phase", READ_FAILED, "/payload/phase", setter(["payload", "phase"], "KillDrain")),
    ("read-failure-numeric-incarnation", READ_FAILED, "/payload/process_incarnation", setter(["payload", "process_incarnation"], 2)),
    ("read-failure-numeric-page", READ_FAILED, "/payload/page", setter(["payload", "page"], 1)),
    ("read-failure-unbounded-message", READ_FAILED, "/payload/message", setter(["payload", "message"], "x" * 1025)),
    ("read-failure-by-the-core", READ_FAILED, "/origin_component", setter(["origin_component"], "broker-core")),
    ("control-failure-read-code", CONTROL_FAILED, "/payload/code", setter(["payload", "code"], "InvalidData")),
    ("control-failure-with-page", CONTROL_FAILED, "/payload", setter(["payload", "page"], "1")),
]:
    trace_case(case, base, pointer, "Failures have bounded, stable codes and phase-specific typed context (record 0021).", edit)
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
        be(2) + be(2, 1) + be(0, 1),  # route revision, QoS, retain
        be(10, 2) + b"p1/example".hex(),
        scoped(1) + be(7),  # payload identity and length
        be(7) + be(2) + be(1) + be(1),  # costs
        be(2, 2),
        be(2) + be(1) + be(0, 4) + be(1, 1) + be(1, 1),  # target b
        be(3) + be(1) + be(1, 4) + be(1, 1) + be(1, 1),  # target c
    ]
)
SIZES = ("payload_bytes", "references", "exchange_slots", "receipt_slots")
LIMITS = {name: fixture["capacities"][name] for name in SIZES} | {
    "max_targets_per_admission": fixture["capacities"]["max_targets_per_admission"],
    "max_topic_bytes": fixture["capacities"]["max_topic_bytes"],
    "max_payload_bytes": fixture["capacities"]["max_payload_bytes"],
    "max_sessions": "8",
    "max_route_targets": "8",
}
EXCHANGE = {"session": "1", "generation": "1"}
snapshot = env(
    "mqttiumd.store-snapshot",
    receipt_frontiers=[],
    run_id="p1-02-example",
    checkpoint="after_commit_before_crash",
    backend="sim-store",
    meta={
        "incarnation": "1",
        "last_opened_epoch": "1",
        "opened_by": {"startup_hex": "a7" * 16, "ticket": "1"},
        "collected_through": "0",
        "state_revision": "1",
        "next_session": "4",
    },
    sessions=[
        {
            "session": number,
            "client_id_hex": client_id.encode().hex(),
            "revision": "1",
            "state": "Active",
            "expiry": "Never",
            "opened_by": None,
        }
        for number, client_id in SESSIONS
    ],
    route_table={
        "revision": "2",
        "routes": [
            {
                "topic_hex": b"p1/example".hex(),
                "targets": [{"session": "2", "qos": 1}, {"session": "3", "qos": 1}],
            }
        ],
    },
    operations=[
        {
            "operation_id": OP,
            "recipe_hex": RECIPE_HEX,
            "decision": {
                "kind": "Committed",
                "facts": {
                    "kind": "PublicationAdmitted",
                    "publication_id": PUBLICATION,
                    "exchange": EXCHANGE,
                },
            },
        }
    ],
    fences=[],
    payloads=[
        {
            "payload_id": PAYLOAD,
            "length": "7",
            "sha256": hashlib.sha256(b"payload").hexdigest(),
            "references": "2",
        }
    ],
    publications=[
        {
            "publication_id": PUBLICATION,
            "operation_id": OP,
            "source_session": "1",
            "topic_hex": b"p1/example".hex(),
            "qos": 2,
            "retain": False,
            "payload_id": PAYLOAD,
        }
    ],
    obligations=[
        {
            "publication_id": PUBLICATION,
            "index": index,
            "session": target,
            "target_revision": "1",
            "cause": "LiveExact",
            "effective_qos": 1,
            "phase": "Queued",
        }
        for index, target in enumerate(["2", "3"])
    ],
    inbound_exchanges=[
        {
            "exchange": EXCHANGE,
            "packet_id": 7,
            "publication_id": PUBLICATION,
            "phase": "AwaitPubrel",
        }
    ],
    outbound=[],
    account={
        "used": {
            "payload_bytes": "7",
            "references": "2",
            "exchange_slots": "1",
            "receipt_slots": "1",
        },
        "limits": LIMITS,
    },
)
ok("store-snapshot-after-commit.json", snapshot)
legacy_session_snapshot = copy.deepcopy(snapshot)
for row in legacy_session_snapshot["sessions"]:
    row.pop("expiry")
    row.pop("opened_by")
ok("store-snapshot-legacy-session-defaults.json", legacy_session_snapshot)
OUTBOUND_SLOT = {
    "delivery_id": {"publication_id": PUBLICATION, "index": 0},
    "target_session": "2",
    "target_revision": "1",
    "packet_id": 1,
    "claim_operation_id": OP,
    "phase": "AwaitPuback",
    "emission": "MayHaveStarted",
}
ok("store-snapshot-outbound-claim.json",
   edited(snapshot, lambda v: v.update(outbound=[OUTBOUND_SLOT])))
for suffix, code, pointer, rows in [
    ("duplicate", "duplicate-name", "/outbound/1/delivery_id", [OUTBOUND_SLOT, OUTBOUND_SLOT]),
    ("packet-id-zero", "schema-violation", "/outbound/0/packet_id",
     [{**OUTBOUND_SLOT, "packet_id": 0}]),
    ("target-overflow", "u64-out-of-range", "/outbound/0/target_session",
     [{**OUTBOUND_SLOT, "target_session": "18446744073709551616"}]),
    ("claim-overflow", "u64-out-of-range", "/outbound/0/claim_operation_id/sequence",
     [{**OUTBOUND_SLOT, "claim_operation_id": {**OP, "sequence": "18446744073709551616"}}]),
]:
    bad(f"store-snapshot-outbound-{suffix}.json", code, pointer,
        "Outgoing claim rows retain bounded identities, unique DeliveryIds and valid packet identifiers.",
        edited(snapshot, lambda v, rows=rows: v.update(outbound=rows)))
# Descriptive canonical examples for the product extension. These snapshots
# are format examples, not fixture-based qualifying campaign results.
def product_operation(sequence, kind, body, facts):
    return {
        "operation_id": {**OP, "sequence": str(sequence)},
        "recipe_hex": b"MQDR".hex() + be(1, 2) + be(kind, 1) + scoped(sequence) + body,
        "decision": {"kind": "Committed", "facts": facts},
    }


subscription_operation = product_operation(
    20, 3, be(2) + be(1) + be(2) + be(1, 1) + be(1, 1) + be(10, 2) + b"p1/example".hex(),
    {"kind": "SubscriptionsChanged", "session": "2", "route_revision": "3"},
)
subscription_snapshot = copy.deepcopy(snapshot)
subscription_snapshot["operations"].append(subscription_operation)
subscription_snapshot["route_table"]["revision"] = "3"
subscription_snapshot["account"]["used"]["receipt_slots"] = "2"
ok("store-snapshot-subscription-receipt.json", subscription_snapshot)
delivery_body = scoped(1) + be(0, 4) + be(2) + be(1) + be(5, 2)
claim_operation = product_operation(21, 4, delivery_body, {
    "kind": "DeliveryClaimed", "delivery_id": OUTBOUND_SLOT["delivery_id"],
    "target_session": "2", "packet_id": 5, "claim_operation_id": {**OP, "sequence": "21"},
})
claimed_snapshot = copy.deepcopy(snapshot)
claimed_snapshot["operations"].append(claim_operation)
claimed_snapshot["outbound"] = [{**OUTBOUND_SLOT, "packet_id": 5,
                                 "claim_operation_id": claim_operation["operation_id"]}]
claimed_snapshot["account"]["used"].update(exchange_slots="2", receipt_slots="2")
ok("store-snapshot-delivery-claim-receipt.json", claimed_snapshot)
finished_snapshot = copy.deepcopy(claimed_snapshot)
finished_snapshot["operations"].append(product_operation(22, 5, delivery_body + scoped(21), {
    **claim_operation["decision"]["facts"], "kind": "DeliveryFinished",
}))
finished_snapshot["outbound"] = []
finished_snapshot["obligations"].pop(0)
finished_snapshot["payloads"][0]["references"] = "1"
finished_snapshot["account"]["used"].update(references="1", exchange_slots="1", receipt_slots="3")
ok("store-snapshot-delivery-finish-receipt.json", finished_snapshot)
granted_snapshot = copy.deepcopy(snapshot)
granted_snapshot["operations"].append(product_operation(
    24, 7, be(2) + be(1) + be(2) + be(1, 1) + be(1, 1) + be(10, 2) + b"p1/example".hex(),
    {"kind": "SubscriptionsChanged", "session": "2", "route_revision": "3"},
))
granted_snapshot["route_table"]["revision"] = "3"
granted_snapshot["route_table"]["routes"][0]["targets"][0]["qos"] = 0
granted_snapshot["account"]["used"]["receipt_slots"] = "2"
ok("store-snapshot-subscription-qos0-receipt.json", granted_snapshot)
consumed_snapshot = copy.deepcopy(snapshot)
consumed_snapshot["route_table"]["routes"][0]["targets"][0]["qos"] = 0
qos0_recipe = bytearray.fromhex(RECIPE_HEX)
qos0_recipe[-24] = 0  # First target QoS; the canonical kind-1 layout is unchanged.
consumed_snapshot["operations"][0]["recipe_hex"] = qos0_recipe.hex()
consumed_snapshot["operations"].append(product_operation(
    25, 6, scoped(1) + be(0, 4) + be(2) + be(1),
    {"kind": "DeliveryConsumed", "delivery_id": OUTBOUND_SLOT["delivery_id"],
     "target_session": "2"},
))
consumed_snapshot["obligations"].pop(0)
consumed_snapshot["payloads"][0]["references"] = "1"
consumed_snapshot["account"]["used"].update(references="1", receipt_slots="2")
ok("store-snapshot-delivery-consume-receipt.json", consumed_snapshot)
# Lifecycle kind8 preserves kinds1--7 and does not enlarge fixture qualification.
# JSON-only historical fixture fields may be omitted: Never/None is explicit.
lifecycle_open = copy.deepcopy(snapshot)
open_operation = {**OP, "sequence": "26"}
new_session = {"session": "4", "client_id_hex": b"new-client".hex(), "revision": "1",
               "state": "Active", "expiry": "OnDisconnect", "opened_by": open_operation}
lifecycle_open["sessions"].append(new_session)
lifecycle_open["meta"]["next_session"] = "5"
lifecycle_open["operations"].append(product_operation(
    26, 8, be(0, 1) + be(1, 1) + be(0, 1) + be(0, 1) + be(10, 2) + b"new-client".hex(),
    {"kind": "SessionLifecycle", "change": {
        "kind": "Opened", "session": "4", "revision": "1", "session_present": False,
        "expiry": "OnDisconnect", "retired": None, "route_revision": "2",
        "cancelled": {"inbound": 0, "deliveries": 0, "routes": 0},
    }},
))
lifecycle_open["account"]["used"]["receipt_slots"] = "2"
ok("store-snapshot-session-open-receipt.json", lifecycle_open)
# An old producer's identity remains historical after target-only cleanup.
# Its other destinations and immutable shared payload remain owed.
lifecycle_close = copy.deepcopy(snapshot)
lifecycle_close["sessions"].pop(0)
lifecycle_close["inbound_exchanges"] = []
lifecycle_close["operations"].append(product_operation(
    27, 8, be(1, 1) + be(1) + be(0, 1) + be(0, 1),
    {"kind": "SessionLifecycle", "change": {
        "kind": "Closed", "session": "1", "route_revision": "2",
        "cancelled": {"inbound": 1, "deliveries": 0, "routes": 0},
    }},
))
lifecycle_close["account"]["used"].update(exchange_slots="0", receipt_slots="2")
ok("store-snapshot-session-close-receipt.json", lifecycle_close)
bad("store-snapshot-session-policy-invalid.json", "schema-violation", "/sessions/3/expiry",
    "Persisted policy is a closed discriminant, including an explicit bounded finite After variant.",
    edited(lifecycle_open, lambda v: v["sessions"][3].update(expiry="Sometimes")))
bad("store-snapshot-session-attachment-overflow.json", "u64-out-of-range", "/sessions/3/opened_by/sequence",
    "An attachment is the original complete OperationId, not a reusable ClientID.",
    edited(lifecycle_open, lambda v: v["sessions"][3]["opened_by"].update(sequence=OVER)))
bad("store-snapshot-session-cancel-count-overflow.json", "schema-violation", "/operations/1/decision/facts/change",
    "Cancellation counts remain bounded u32 numbers, not success delivery counts.",
    edited(lifecycle_close, lambda v: v["operations"][1]["decision"]["facts"]["change"]["cancelled"].update(inbound=4294967296)))
bad("store-snapshot-session-retired-overflow.json", "u64-out-of-range", "/operations/1/decision/facts/change/retired",
    "Even a no-longer-retained SessionId remains an unsigned checked causal identity.",
    edited(lifecycle_open, lambda v: v["operations"][1]["decision"]["facts"]["change"].update(retired=OVER)))
ok("store-snapshot-explicit-legacy-session-policy.json", edited(snapshot, lambda v:
    [row.update(expiry="Never", opened_by=None) for row in v["sessions"]]))
retained_frontier_snapshot = copy.deepcopy(snapshot)
retained_frontier_snapshot["receipt_frontiers"] = [copy.deepcopy(RECEIPT_FRONTIER)]
ok("store-snapshot-receipt-frontier-retained-rows.json", retained_frontier_snapshot)
collected_snapshot = copy.deepcopy(retained_frontier_snapshot)
collected_snapshot["operations"] = []
collected_snapshot["account"]["used"]["receipt_slots"] = "0"
ok("store-snapshot-receipt-frontier-collected.json", collected_snapshot)
bad("store-snapshot-receipt-frontier-missing-bound.json", "schema-violation",
    "/receipt_frontiers/0", "All three durable identity boundaries are explicit.",
    edited(collected_snapshot, lambda v: v["receipt_frontiers"][0].pop("payload_through")))
bad("store-snapshot-receipt-frontier-duplicate.json", "duplicate-name", "/receipt_frontiers/1/origin",
    "There is one compact frontier per origin, not per operation.",
    edited(collected_snapshot, lambda v: v["receipt_frontiers"].append(copy.deepcopy(RECEIPT_FRONTIER))))
bad("store-snapshot-receipt-frontier-out-of-order.json", "rows-out-of-order", "/receipt_frontiers/1/origin",
    "Origins sort numerically by incarnation, epoch and owner.",
    edited(collected_snapshot, lambda v: v["receipt_frontiers"].insert(0, {
        **RECEIPT_FRONTIER, "origin": {**RECEIPT_FRONTIER["origin"], "owner": "2"}})))
bad("store-snapshot-receipt-frontier-origin-limit.json", "schema-violation", "/receipt_frontiers",
    "Compact origin guards are bounded independently of retained receipt rows.",
    edited(collected_snapshot, lambda v: v.update(receipt_frontiers=[{
        **RECEIPT_FRONTIER, "origin": {**RECEIPT_FRONTIER["origin"], "owner": str(owner)}}
        for owner in range(65)])))
for component in ("through", "publication_through", "payload_through"):
    bad(f"store-snapshot-receipt-frontier-{component}-overflow.json", "u64-out-of-range",
        f"/receipt_frontiers/0/{component}", "Prefix boundaries are checked u64 decimal strings.",
        edited(collected_snapshot, lambda v, component=component:
               v["receipt_frontiers"][0].update({component: OVER})))
bad("store-snapshot-consumed-with-packet-id.json", "schema-violation", "/operations/1/decision/facts",
    "A consumed QoS 0 obligation never has an outgoing MQTT Packet Identifier.",
    edited(consumed_snapshot, lambda v: v["operations"][1]["decision"]["facts"].update(packet_id=1)))
ok(
    "store-snapshot-empty.json",
    env(
        "mqttiumd.store-snapshot",
        receipt_frontiers=[],
        run_id="p1-02-example",
        checkpoint="initial",
        backend="sqlite",
        meta={
            "incarnation": "1",
            "last_opened_epoch": "0",
            "opened_by": None,
            "collected_through": "0",
            "state_revision": "0",
            "next_session": "1",
        },
        sessions=[],
        route_table={"revision": "0", "routes": []},
        operations=[],
        fences=[],
        payloads=[],
        publications=[],
        obligations=[],
        inbound_exchanges=[],
        outbound=[],
        account={"used": dict.fromkeys(SIZES, "0"), "limits": LIMITS},
    ),
)
# A snapshot shows what the store holds, sound or not: an obligation without
# its publication is a finding for the oracle, not a malformed document.
ok(
    "store-snapshot-orphan-obligation.json",
    edited(snapshot, lambda v: v.update(publications=[])),
)
REFUSED_OP = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "2"}
ok(
    "store-snapshot-refused-and-fenced.json",
    edited(
        snapshot,
        lambda v: (
            v["operations"].append(
                {
                    "operation_id": REFUSED_OP,
                    "recipe_hex": RECIPE_HEX,
                    "decision": {
                        "kind": "RejectedCertain",
                        "reason": "CapacityExceeded",
                        "resource": "references",
                    },
                }
            ),
            v["fences"].append(
                {"operation_id": {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "3"}}
            ),
            v["sessions"][2].update(state="Terminal"),
            # A publication outlives its payload once its obligations ended.
            v.update(payloads=[], obligations=[]),
        ),
    ),
)


def snap_case(name, code, pointer, why, edit):
    bad(f"store-snapshot-{name}.json", code, pointer, why, edited(snapshot, edit))


def twice(table, index=0, **changes):
    """Repeat one row of a table, with ``changes``, right after itself."""

    def edit(value):
        rows = value[table]
        rows.insert(index + 1, {**copy.deepcopy(rows[index]), **changes})

    return edit


snap_case(
    "target-limit-over-32-bits",
    "u32-out-of-range",
    "/account/limits/max_targets_per_admission",
    "The bound on the targets of an admission is a 32-bit quantity, in a snapshot as in a trace.",
    setter(["account", "limits", "max_targets_per_admission"], "4294967296"),
)
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
    "/operations/0/decision/kind",
    "A receipt records a terminal decision; an unknown outcome is not a durable fact.",
    setter(["operations", 0, "decision"], {"kind": "PendingOrUnknown"}),
)
snap_case(
    "commit-without-facts",
    "schema-violation",
    "/operations/0/decision",
    "A committed receipt says what was committed.",
    dropped("operations", 0, "decision", "facts"),
)
snap_case(
    "rejection-without-reason",
    "schema-violation",
    "/operations/0/decision",
    "A certain rejection is recorded with its reason: that is what keeps it rejected.",
    setter(["operations", 0, "decision"], {"kind": "RejectedCertain"}),
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
    "opening-attempt-without-ticket",
    "schema-violation",
    "/meta/opened_by",
    "The attempt that opened the last epoch is its start-up nonce and its ticket.",
    dropped("meta", "opened_by", "ticket"),
)
snap_case(
    "startup-nonce-too-short",
    "schema-violation",
    "/meta/opened_by",
    "A start-up nonce is sixteen bytes.",
    setter(["meta", "opened_by", "startup_hex"], "a7" * 15),
)
snap_case(
    "session-state-unknown",
    "schema-violation",
    "/sessions/0/state",
    "A session is active or terminal; a deleted one stays, terminal.",
    setter(["sessions", 0, "state"], "Deleted"),
)
snap_case(
    "operation-listed-twice",
    "duplicate-name",
    "/operations/1/operation_id",
    "An OperationId designates one recipe: one row.",
    twice("operations"),
)
snap_case(
    "obligation-listed-twice",
    "duplicate-name",
    "/obligations/1",
    "A DeliveryId designates one obligation, whatever destination a second row claims.",
    twice("obligations", session="1"),
)
snap_case(
    "inbound-slot-listed-twice",
    "duplicate-name",
    "/inbound_exchanges/1",
    "A session has one inbound exchange per Packet Identifier.",
    twice("inbound_exchanges", exchange={"session": "1", "generation": "2"}),
)
snap_case(
    "session-listed-twice",
    "duplicate-name",
    "/sessions/1/session",
    "A SessionId designates one session.",
    twice("sessions", client_id_hex="7a"),
)
snap_case(
    "route-listed-twice",
    "duplicate-name",
    "/route_table/routes/1/topic_hex",
    "An exact topic has one route.",
    lambda v: v["route_table"]["routes"].append(copy.deepcopy(v["route_table"]["routes"][0])),
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
    twice("publications"),
)
snap_case(
    "payload-listed-twice",
    "duplicate-name",
    "/payloads/1/payload_id",
    "A PayloadId designates one payload.",
    twice("payloads"),
)


# The type of a key comes from the schema, never from how its value looks:
# topic bytes made of decimal digits only are still bytes.
def routes(*topics):
    def edit(value):
        value["route_table"]["routes"] = [
            {"topic_hex": topic.encode().hex(), "targets": []} for topic in topics
        ]

    return edit


ok("store-snapshot-routes-in-byte-order.json", edited(snapshot, routes("a/b", "aa", "b", "p")))
ok(
    "store-snapshot-long-digit-topics.json",
    edited(snapshot, routes("tttttttttttt", "tttttttttttu")),
)
snap_case(
    "digit-topics-out-of-byte-order",
    "rows-out-of-order",
    "/route_table/routes/1/topic_hex",
    "6161 comes before 62 as bytes, whatever the two read as decimal numbers.",
    routes("b", "aa"),
)
snap_case(
    "long-digit-topic-listed-twice",
    "duplicate-name",
    "/route_table/routes/1/topic_hex",
    "Twenty-four hexadecimal digits are bytes, not a number too large to compare.",
    routes("tttttttttttt", "tttttttttttt"),
)
snap_case(
    "empty-topic-listed-twice",
    "duplicate-name",
    "/route_table/routes/1/topic_hex",
    "The empty byte string is a key like another.",
    routes("", ""),
)
# Two stores in the same state write the same bytes: rows are in key order.
snap_case(
    "sessions-out-of-order",
    "rows-out-of-order",
    "/sessions/1/session",
    "Sessions are listed in the numeric order of their SessionId.",
    lambda v: v["sessions"].reverse(),
)
snap_case(
    "sessions-in-text-order",
    "rows-out-of-order",
    "/sessions/3/session",
    "The order is numeric: 10 comes after 3, not between 1 and 2.",
    lambda v: (
        v["sessions"].append({**v["sessions"][0], "session": "10", "client_id_hex": "7a"}),
        v["sessions"].sort(key=lambda row: row["session"]),
    ),
)
snap_case(
    "obligations-out-of-order",
    "rows-out-of-order",
    "/obligations/1",
    "Obligations are listed by publication, then index.",
    lambda v: v["obligations"].reverse(),
)
snap_case(
    "operations-out-of-order",
    "rows-out-of-order",
    "/operations/1/operation_id",
    "Operations are listed by incarnation, epoch, owner, sequence.",
    lambda v: v["operations"].insert(
        0, {**copy.deepcopy(v["operations"][0]), "operation_id": REFUSED_OP}
    ),
)
snap_case(
    "payload-length-overflow",
    "u64-out-of-range",
    "/payloads/0/length",
    "Sizes are 64-bit.",
    setter(["payloads", 0, "length"], OVER),
)
snap_case(
    "obligation-session-overflow",
    "u64-out-of-range",
    "/obligations/1/session",
    "Identities inside a list are range-checked as well.",
    setter(["obligations", 1, "session"], OVER),
)
for path in (
    ["meta", "last_opened_epoch"],
    ["meta", "opened_by", "ticket"],
    ["meta", "collected_through"],
    ["meta", "state_revision"],
    ["meta", "next_session"],
    ["route_table", "revision"],
    ["payloads", 0, "references"],
    ["publications", 0, "source_session"],
    ["obligations", 0, "target_revision"],
    ["account", "used", "references"],
    ["account", "limits", "payload_bytes"],
    ["account", "limits", "exchange_slots"],
    ["account", "limits", "receipt_slots"],
    ["account", "limits", "max_payload_bytes"],
    ["account", "limits", "max_sessions"],
    ["account", "limits", "max_route_targets"],
):
    snap_case(
        "overflow-" + "-".join(str(part).replace("_", "-") for part in path),
        "u64-out-of-range",
        "".join(f"/{part}" for part in path),
        "Every 64-bit member of a snapshot is range-checked.",
        setter(path, OVER),
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

FAILED_READ_RESULT = result("INCONCLUSIVE", 2, [("durable_reads_complete", "INCONCLUSIVE")])
FAILED_READ_RESULT.update(failure_count="1", failures=[{
    "kind": READ_FAILED["kind"], "record_sequence": READ_FAILED["record_sequence"],
    **READ_FAILED["payload"],
}])
ok("run-result-provisional-typed-read-failure.json", FAILED_READ_RESULT)
bad(
    "run-result-failure-without-count.json", "schema-violation", "",
    "A bounded typed failure list declares its untruncated total (record 0021).",
    edited(FAILED_READ_RESULT, lambda v: v.pop("failure_count")),
)
bad(
    "run-result-unbounded-failure-list.json", "schema-violation", "/failures",
    "At most the first 64 failure contexts are kept in a result (record 0021).",
    edited(FAILED_READ_RESULT, lambda v: v.update(failure_count="65", failures=v["failures"] * 65)),
)
for count in ("0", "2", "65"):
    bad(
        f"run-result-failure-count-{count}-with-one-context.json", "result-incomplete", "/failures",
        "The retained inventory contains exactly min(failure_count, 64) contexts, without concealing discarded ones.",
        edited(FAILED_READ_RESULT, lambda v, count=count: v.update(failure_count=count)),
    )
for path in (["failure_count"], ["failures", 0, "process_incarnation"], ["failures", 0, "record_sequence"]):
    bad(
        "run-result-failure-overflow-" + "-".join(map(str, path)) + ".json", "u64-out-of-range",
        "".join(f"/{part}" for part in path), "Failure totals and identities remain unsigned 64-bit decimal strings.",
        edited(FAILED_READ_RESULT, setter(path, OVER)),
    )
trace_case(
    "read-failure-process-incarnation-overflow", READ_FAILED, "/payload/process_incarnation",
    "The failed process is a 64-bit identity even before Started.",
    setter(["payload", "process_incarnation"], OVER), code="u64-out-of-range",
)


def at_least(minimum, hits):
    """A result of a repeated fault: reached ``hits`` times, ``minimum`` expected."""

    def edit(value):
        value["faults"] = [{"id": "late_answers", "expected_hits_at_least": minimum, "hits": hits}]

    return edit


SCOPED = [("trace_complete", "PASS"), ("no_pubrec_observed_before_crash", "OUT_OF_SCOPE")]
ok(
    "run-result-provisional-repeated-fault-reached-more-than-its-minimum.json",
    edited(result("OUT_OF_SCOPE", 3, SCOPED), at_least("1", "3")),
)
bad(
    "run-result-repeated-fault-below-its-minimum.json",
    "verdict-inconsistent",
    "/verdict",
    "A repeated fault reached fewer times than its minimum leaves the run inconclusive.",
    edited(result("OUT_OF_SCOPE", 3, SCOPED), at_least("2", "1")),
)
bad(
    "run-result-fault-with-both-expectations.json",
    "schema-violation",
    "/faults/0",
    "A fault is expected an exact number of times or at least a number of times.",
    edited(
        result("INCONCLUSIVE", 2, PASSING),
        lambda v: v["faults"][0].update(expected_hits_at_least="1"),
    ),
)
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


# The oracle and the generators that judged and built a campaign are
# identified apart from the lab binary (V3-002): each a digest over the
# files it lists, in path order, as the contract digest is.
def identified(*files):
    listed = [{"path": path, "sha256": digest * 64, "bytes": size} for path, digest, size in files]
    listing = "".join(f"{f['sha256']}  {f['path']}\n" for f in listed)
    return {"sha256": hashlib.sha256(listing.encode()).hexdigest(), "files": listed}


ORACLE = identified(
    ("tests/oracle/mqttiumd_oracle/judge.py", "7", "52011"),
    ("tests/oracle/mqttiumd_oracle/trace.py", "8", "9004"),
)
GENERATORS = identified(("tools/run_campaign.sh", "9", "1210"))
ok(
    "run-manifest-with-oracle-and-generators.json",
    man(lambda v: v.update(oracle=ORACLE, generators=GENERATORS)),
)
bad(
    "run-manifest-oracle-of-other-files.json",
    "evidence-mismatch",
    "/oracle/sha256",
    "The digest of the oracle is the digest of the files it lists.",
    man(lambda v: v.update(oracle={**ORACLE, "sha256": "6" * 64})),
)
bad(
    "run-manifest-generators-out-of-order.json",
    "evidence-mismatch",
    "/generators/files/1",
    "The files of a generator are listed once each, in path order.",
    man(
        lambda v: v.update(
            generators=identified(
                ("tools/run_campaign.sh", "9", "1210"), ("tools/build_corpus.py", "a", "77")
            )
        )
    ),
)
bad(
    "run-manifest-oracle-without-files.json",
    "schema-violation",
    "/oracle",
    "An identity names the files it is the digest of.",
    man(lambda v: v.update(oracle={"sha256": ORACLE["sha256"]})),
)


# Where the broker ran (record 0019): in the process of the bench, or as a
# child process the supervisor can kill.
ok("run-manifest-of-a-child-process.json", man(lambda v: v.update(broker="child-process")))
bad(
    "run-manifest-of-an-unknown-topology.json",
    "schema-violation",
    "/broker",
    "A broker runs in the process of the bench or as a child process.",
    man(lambda v: v.update(broker="container")),
)

# The binary tied to the source it was rebuilt from (run --rebuild).
BUILT_FROM = {
    "source": {"path": "source.tar", "sha256": "5" * 64, "bytes": "20480"},
    "build": "cargo build --locked --offline -p mqttiumd-lab --bin mqttiumd-lab",
}
ok(
    "run-manifest-built-from-its-source.json",
    man(lambda v: v["artifact"].update(built_from=copy.deepcopy(BUILT_FROM))),
)
bad(
    "run-manifest-built-from-nothing.json",
    "schema-violation",
    "/artifact/built_from",
    "A binary built from a source names that source.",
    man(lambda v: v["artifact"].update(built_from={"build": BUILT_FROM["build"]})),
)
bad(
    "run-manifest-built-from-without-its-command.json",
    "schema-violation",
    "/artifact/built_from",
    "A binary built from a source says how it was built.",
    man(lambda v: v["artifact"].update(built_from={"source": BUILT_FROM["source"]})),
)
bad(
    "run-manifest-built-from-a-source-too-large.json",
    "u64-out-of-range",
    "/artifact/built_from/source/bytes",
    "The size of the source is 64-bit.",
    man(
        lambda v: v["artifact"].update(
            built_from={
                **BUILT_FROM,
                "source": {**BUILT_FROM["source"], "bytes": "18446744073709551616"},
            }
        )
    ),
)
bad(
    "run-manifest-built-from-a-source-outside.json",
    "schema-violation",
    "/artifact/built_from/source/path",
    "The source is a file of the campaign, named by a relative path.",
    man(
        lambda v: v["artifact"].update(
            built_from={**BUILT_FROM, "source": {**BUILT_FROM["source"], "path": "/tmp/source.tar"}}
        )
    ),
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

# Every kind of record is stated by one origin (ADR 0003): for each kind
# the audit added, a valid record stated by another origin is refused.
for kind_name, origin in [
    ("peer-connection-opened", "transport"),
    ("peer-connection-closed", "broker-core"),
    ("client-bytes-received", "peer"),
    ("resolve-submitted", "broker-core"),
    ("store-fence-stable", "store-adapter"),
    ("store-fence-refused", "broker-core"),
    ("recovery-page-delivered", "store-adapter"),
    ("held-item-released", "bench-supervisor"),
    ("fault-hit", "broker-core"),
    ("peer-packet-observed", "transport"),
    ("store-result-handed-over", "broker-core"),
    ("opening-submitted", "broker-core"),
    ("store-epoch-opened", "store-adapter"),
    ("opening-reply-delivered", "store-adapter"),
    ("opening-abandoned", "bench-supervisor"),
    ("epoch-grant-handed-over", "store-evidence"),
    ("step-blocked", "broker-core"),
]:
    trace_case(
        f"{kind_name}-stated-by-{origin}",
        VALID[f"trace-record-{kind_name}.json"],
        "/origin_component",
        "A kind of record is stated by its one origin; another may not state it.",
        setter(["origin_component"], origin),
    )
# Product system evidence is separate from fixture-based qualifying campaigns.
ok("product-profile.json", json.loads((V1 / "product-profile.json").read_text()))
product_profile = json.loads((V1 / "product-profile.json").read_text())
for field, expected in (("closing_input_slots", 32), ("terminal_input_slots", 544),
                        ("frame_registration_slots", 208), ("pre_connack_packet_slots", 1),
                        ("session_shutdown_timeout_ms", 2000)):
    bad(f"product-profile-wrong-{field}.json", "schema-violation", f"/{field}",
        "The configured bound matches the declared runtime queue rather than historical traffic.",
        {**product_profile, field: expected + 1})
ok("product-retained-transitions.json", json.loads((V1 / "retained.transitions.json").read_text()))
hierarchical_transitions = json.loads((V1 / "hierarchical.transitions.json").read_text())
ok("product-hierarchical-transitions.json", hierarchical_transitions)
bad("product-hierarchical-transitions-wrong-decision.json", "schema-violation", "/decision",
    "Hierarchical routing is a descriptive Proposed0031 subset, not a retroactive qualification.",
    {**hierarchical_transitions, "decision": "0030"})
product_transitions = json.loads((V1 / "pubsub.transitions.json").read_text())
ok("product-pubsub-transitions.json", product_transitions)
receipt_transitions = json.loads((V1 / "receipts.transitions.json").read_text())
ok("product-receipt-transitions.json", receipt_transitions)
pi_transitions = json.loads((V1 / "pi.transitions.json").read_text())
ok("product-pi-transitions.json", pi_transitions)
session_transitions = json.loads((V1 / "sessions.transitions.json").read_text())
ok("product-session-transitions.json", session_transitions)
bad("product-session-transitions-wrong-decision.json", "schema-violation", "/decision",
    "Lifecycle transitions name Proposed0029 and retain system-test scope.",
    {**session_transitions, "decision": "0028"})
bad("product-pi-transitions-wrong-decision.json", "schema-violation", "/decision",
    "The recyclable identifier subset refers to Proposed 0028 without relabeling earlier subsets.",
    {**pi_transitions, "decision": "0027"})
bad("product-receipt-transitions-wrong-decision.json", "schema-violation", "/decision",
    "The receipt subset refers to its own proposed decision, without relabeling the pub/sub subset.",
    {**receipt_transitions, "decision": "0026"})
bad("product-transitions-claiming-complete.json", "schema-violation", "/partition_complete",
    "The product transition subset is not a complete guard partition.",
    {**product_transitions, "partition_complete": True})
bad("product-transitions-claiming-qualification.json", "schema-violation", "/scope",
    "Product system tests do not extend fixture-based qualification.",
    {**product_transitions, "scope": "qualifying"})
bad("product-transitions-duplicate-rule.json", "duplicate-name", "/rules/1/id",
    "Each documented product transition has one stable identifier.",
    edited(product_transitions, lambda v: v["rules"].insert(1, copy.deepcopy(v["rules"][0]))))
product_event = env("mqttiumd.product-event", seq="1", kind="startup_refused", pid=1,
                    cause="InvalidData", phase="Snapshot", details={"code": "durable_read"})
ok("product-event.json", product_event)
qos0_drop_event = env("mqttiumd.product-event", seq="2", kind="qos0_dropped", pid=1,
    details={"origin": "volatile/1/1/3", "reason": "no_subscribers", "target": ""})
ok("product-qos0-drop-event.json", qos0_drop_event)
bad("product-event-leading-digit.json", "schema-violation", "/kind",
    "An event name starts with a letter; QoS digits are allowed only after it.",
    {**qos0_drop_event, "kind": "0qos_dropped"})
puback_target = {
    "delivery_id": {"publication_id": PUBLICATION, "index": 0},
    "claim_operation_id": {"incarnation": "1", "epoch": "2", "owner": "0", "sequence": "3"},
    "frame_effect_id": {"epoch": "3", "owner": "0", "sequence": "4"},
}
puback_event = env("mqttiumd.product-event", seq="5", kind="puback_observed", pid=1, connection="7",
    details={"authority": "reader_dequeue"}, puback={
        "connection": {"session": "2", "generation": "7"}, "packet_id": 1,
        "reason_code": 0, "target": puback_target})
ok("product-puback-observed.json", puback_event)
ok("product-puback-no-target.json", edited(puback_event, lambda v: v["puback"].update(target=None)))
ok("product-puback-ignored-historical-target.json", edited(puback_event, lambda v: v.update(kind="puback_ignored")))
ok("product-puback-retained-copy.json", edited(puback_event, lambda v: v["puback"].update(source_observation_sequence="99")))
ok("product-puback-negative.json", edited(puback_event, lambda v: v["puback"].update(reason_code=0x97)))
bad("product-puback-missing-observation.json", "schema-violation", "",
    "A claimed PUBACK observation includes its immutable connection/PI/reason/target tuple.",
    edited(puback_event, lambda v: v.pop("puback")))
bad("product-puback-implicit-target.json", "schema-violation", "/puback",
    "No target is represented by null; omitting the association cannot permit a later lookup.",
    edited(puback_event, lambda v: v["puback"].pop("target")))
bad("product-puback-opaque-target.json", "schema-violation", "/puback/target",
    "The association has individually bounded durable and frame identities, not an opaque token.",
    edited(puback_event, lambda v: v["puback"].update(target="1/2/3")))
bad("product-puback-partial-target.json", "schema-violation", "/puback/target",
    "All delivery, claim and frame guards belong to the same immutable observation.",
    edited(puback_event, lambda v: v["puback"]["target"].pop("frame_effect_id")))
bad("product-puback-rebinding-field.json", "schema-violation", "/puback",
    "A no-target observation has no field that can attach a newer replacement target.",
    edited(puback_event, lambda v: v["puback"].update(target=None, replacement_target=puback_target)))
bad("product-puback-wrong-authority.json", "schema-violation", "/details/authority",
    "Unobserved reader-channel and socket bytes are upstream of the documented runtime-dequeue authority.",
    edited(puback_event, lambda v: v["details"].update(authority="socket_bytes")))
bad("product-puback-generation-mismatch.json", "evidence-mismatch", "/connection",
    "An optional envelope generation cannot contradict its immutable nested connection key.",
    edited(puback_event, lambda v: v.update(connection="8")))
for name, member, value in (("zero", "packet_id", 0), ("too-large", "packet_id", 65536),
                            ("wrong-role-reason", "reason_code", 0x92)):
    bad(f"product-puback-{name}.json", "schema-violation", f"/puback/{member}",
        "A network PI is nonzero u16; only reason codes permitted for PUBACK describe a validated observation.",
        edited(puback_event, lambda v, member=member, value=value: v["puback"].update({member: value})))
for pointer in (("connection", "session"), ("connection", "generation"),
                ("target", "delivery_id", "publication_id", "sequence"),
                ("target", "claim_operation_id", "sequence"),
                ("target", "frame_effect_id", "sequence"), ("source_observation_sequence",)):
    value = copy.deepcopy(puback_event)
    at = value["puback"]
    for member in pointer[:-1]:
        at = at[member]
    at[pointer[-1]] = "18446744073709551616"
    bad("product-puback-overflow-" + "-".join(pointer) + ".json", "u64-out-of-range",
        "/puback/" + "/".join(pointer), "Causal and observation identities never wrap when a resource PI is reused.", value)
maintenance_event = env("mqttiumd.product-event", seq="8", kind="receipt_collection_applied", pid=1,
    command_kind="ReceiptCollection", details={"effect": "1/0/9", "incarnation": "1", "epoch": "1",
    "owner": "0", "through": "64", "publication_through": "32", "payload_through": "32",
    "limit": 128, "attempt": 1, "outcome": "Applied", "removed": 64, "retained": "8", "more": False})
ok("product-receipt-collection.json", maintenance_event)
absolute_collection = edited(maintenance_event, lambda v: v["details"].update(
    through="32", publication_through="16", payload_through="16",
    returned_incarnation="1", returned_epoch="1", returned_owner="0", returned_through="64",
    returned_publication_through="32", returned_payload_through="32"))
ok("product-receipt-collection-returned-closure.json", absolute_collection)
for member in ("incarnation", "epoch", "owner", "through", "publication_through", "payload_through"):
    bad(f"product-receipt-returned-{member}-overflow.json", "u64-out-of-range",
        f"/details/returned_{member}", "The actual returned absolute guard is a bounded u64 decimal identity.",
        edited(absolute_collection, lambda v, member=member:
               v["details"].update({"returned_" + member: "18446744073709551616"})))
ok("product-receipt-prepared.json", env("mqttiumd.product-event", seq="9", kind="barrier_hit", pid=1,
    point="receipt.close.prepared", command_kind="ReceiptCollection", details={
    "incarnation": "1", "epoch": "1", "owner": "0", "through": "64",
    "publication_through": "32", "payload_through": "32"}))
ok("product-maintenance-read-refused.json", env("mqttiumd.product-event", seq="10", kind="runtime_refused",
    pid=1, cause="Io", phase="Maintenance", details={"code": "durable_read"}))
bad("product-receipt-collection-overflow.json", "u64-out-of-range", "/details/through",
    "Maintenance uses stable scalar decimal identity guards, not diagnostic text.",
    edited(maintenance_event, lambda v: v["details"].update(through="18446744073709551616")))
bad("product-receipt-collection-fake-operation.json", "schema-violation", "",
    "Collection is a dedicated control and does not invent an OperationId or recursive receipt.",
    {**maintenance_event, "operation": OP})
ok("product-ack-receipt.json", env("mqttiumd.product-event", seq="2", kind="barrier_hit", pid=1,
   connection="1", point="ack.receipt", details={"effect":"2/0/7", "packet_id":1,
   "publish_order":"3", "bytes":4, "socket_write_complete":True}))
ok("product-puback-credit.json", env("mqttiumd.product-event", seq="3", kind="receive_credit", pid=1,
   connection="1", details={"packet_kind":"PUBACK", "effect":"2/0/7", "packet_id":1,
   "publish_order":"3", "released":False}))
bad("product-event-untyped-cause.json", "schema-violation", "/cause",
    "A durable cause is a stable code, never an SQL message.",
    {**product_event, "cause": "qos 99"})
bad("product-event-unbounded-detail.json", "schema-violation", "/details/code",
    "Product diagnostics never copy unbounded values.",
    {**product_event, "details": {"code": "x" * 257}})
for pointer, edit in [
    ("/seq", {**product_event, "seq": "18446744073709551616"}),
    ("/connection", {**product_event, "connection": "18446744073709551616"}),
    ("/operation/sequence", {**product_event, "operation": {
        "incarnation": "1", "epoch": "1", "owner": "0", "sequence": "18446744073709551616"}}),
]:
    bad("product-event-overflow-" + pointer.rsplit("/", 1)[1] + ".json", "u64-out-of-range", pointer,
        "Product event identities retain the shared 64-bit range.", edit)
# Shutdown preserves uncertainty without inventing an operation or success.
shutdown_completed = env("mqttiumd.product-event", seq="14", kind="session_shutdown_completed", pid=1,
    details={"persistent_sessions":128, "owner_bindings":0})
ok("product-session-shutdown-completed.json", shutdown_completed)
for member, value in (("persistent_sessions",129),("owner_bindings",1)):
    bad("product-session-shutdown-invalid-" + member.replace("_","-") + ".json", "schema-violation", "/details/"+member,
        "Completed session shutdown retains a bounded persistent inventory and no transport binding.",
        edited(shutdown_completed, lambda v, key=member, value=value: v["details"].update({key:value})))
bad("product-session-shutdown-missing-binding-count.json", "schema-violation", "/details",
    "A completion must state the absence of owner transport bindings.",
    edited(shutdown_completed, lambda v: v["details"].pop("owner_bindings")))
shutdown_unknown = env("mqttiumd.product-event", seq="15", kind="shutdown_incomplete", pid=1,
    operation=OP, details={"code":"pending_or_unknown", "phase":"Shutdown", "lifecycle_pending":True,
    "retired_flights":256, "in_flight":17, "closing_inputs":32})
ok("product-session-shutdown-unknown.json", shutdown_unknown)
ok("product-session-shutdown-business-unknown.json", edited(shutdown_unknown,
    lambda v: (v.pop("operation"), v["details"].update(lifecycle_pending=False))))
ok("product-writer-shutdown-deadline.json", env("mqttiumd.product-event", seq="16", kind="shutdown_incomplete", pid=1,
    details={"code":"writer_deadline", "accepted_outcome":"unknown"}))
for member, value in (("phase","Recovery"),("retired_flights",257),("in_flight",18),("closing_inputs",33)):
    bad("product-session-shutdown-invalid-"+member.replace("_","-")+".json", "schema-violation", "/details/"+member,
        "Uncertain session shutdown retains its phase and configured resource bounds.",
        edited(shutdown_unknown, lambda v, key=member, value=value: v["details"].update({key:value})))
bad("product-session-shutdown-missing-original-operation.json", "schema-violation", "",
    "A pending lifecycle retains its original operation rather than losing correlation at shutdown.",
    edited(shutdown_unknown, lambda v: v.pop("operation")))
# Lifecycle observations keep exact identities and bounded resource inventories.
for observed_kind in ("session_change_copied", "session_change_replayed",
                      "session_close_copied", "session_close_replayed"):
    is_change = observed_kind.startswith("session_change_")
    observed = env("mqttiumd.product-event", seq="10", kind=observed_kind, pid=1,
        connection="4", operation={**OP, "sequence": "26"},
        details={"session": "9", **({"action": "Opened"} if is_change else {})})
    ok("product-" + observed_kind.replace("_", "-") + ".json", observed)
    required_identity = "operation" if is_change else "connection"
    bad("product-" + observed_kind.replace("_", "-") + "-missing-identity.json", "schema-violation", "",
        "An internal copied observation retains its authoritative operation or original transport generation.",
        edited(observed, lambda v, key=required_identity: v.pop(key)))
    bad("product-" + observed_kind.replace("_", "-") + "-missing-session.json", "schema-violation", "/details",
        "The copied fact names its historical durable incarnation without a new ClientID lookup.",
        edited(observed, lambda v: v["details"].pop("session")))
    bad("product-" + observed_kind.replace("_", "-") + "-retarget-client.json", "schema-violation", "/details",
        "Capture/replay evidence does not replace an immutable SessionId with a reusable ClientID.",
        edited(observed, lambda v: v["details"].update(client_id="reusable-name")))
    if not is_change:
        ok("product-" + observed_kind.replace("_", "-") + "-legacy-attachment.json",
           edited(observed, lambda v: v.pop("operation")))
session_open_submitted = env("mqttiumd.product-event", seq="10", kind="command_submitted", pid=1,
    operation={**OP, "sequence": "26"}, details={"recipe_kind": "SessionOpen", "action": "Open",
    "clean_start": True, "expiry": "OnDisconnect", "expected_session": None})
ok("product-session-open-submitted.json", session_open_submitted)
bad("product-session-expected-overflow.json", "u64-out-of-range", "/details/expected_session",
    "A nonnull expected session retains the same checked identity domain.",
    edited(session_open_submitted, lambda v: v["details"].update(expected_session=OVER)))
bad("product-session-null-expected-wrong-kind.json", "schema-violation", "/kind",
    "Null is accepted only for the named optional identity in its event context.",
    {**session_open_submitted, "kind": "runtime_refused"})
session_changed = env("mqttiumd.product-event", seq="11", kind="session_changed", pid=1,
    operation=open_operation, connection="1", details={
    "action": "Opened", "session": "4", "route_revision": "2", "revision": "1",
    "session_present": False, "expiry": "OnDisconnect", "retired_session": None,
    "cancelled_inbound": 0, "cancelled_deliveries": 0, "cancelled_routes": 0})
ok("product-session-opened.json", session_changed)
session_closed = env("mqttiumd.product-event", seq="12", kind="session_changed", pid=1,
    operation={**OP, "sequence": "27"}, details={"action": "Closed", "session": "1",
    "route_revision": "2", "cancelled_inbound": 1, "cancelled_deliveries": 0, "cancelled_routes": 0})
ok("product-session-closed.json", session_closed)
ok("product-session-refused.json", env("mqttiumd.product-event", seq="13", kind="session_refused", pid=1,
    connection="2", details={"reason_code": 151}))
cardinalities = {name: 0 for name in ("session_cache", "owner_sessions", "owner_bindings", "work_requests",
    "work_ready", "emit_registrations", "lifecycle_requests", "closed_generations", "retired_flights",
    "owner_inbound", "owner_outgoing", "frames", "timers")}
ok("product-session-cardinality.json", env("mqttiumd.product-event", seq="14", kind="session_cardinality", pid=1,
    details={**cardinalities, "closed_generations": 8}))
for member in ("session", "route_revision", "revision", "retired_session"):
    bad(f"product-session-{member}-overflow.json", "u64-out-of-range", f"/details/{member}",
        "Lifecycle observations use checked identities even for retired historical incarnations.",
        edited(session_changed, lambda v, member=member: v["details"].update({member: OVER})))
bad("product-session-missing-operation.json", "schema-violation", "",
    "An applied lifecycle result is correlated by its original OperationId.",
    edited(session_changed, lambda v: v.pop("operation")))
bad("product-session-missing-policy.json", "schema-violation", "/details",
    "An open observation must identify the established durable retention policy.",
    edited(session_changed, lambda v: v["details"].pop("expiry")))
bad("product-session-refused-success.json", "schema-violation", "/details/reason_code",
    "A refusal must not describe a success CONNACK or fabricate a session identity.",
    env("mqttiumd.product-event", seq="13", kind="session_refused", pid=1, connection="2", details={"reason_code": 0}))
# Finite expiry is a descriptive extension, never a fixture-based qualification.
expiry_transitions = json.loads((V1 / "expiry.transitions.json").read_text())
ok("product-expiry-transitions.json", expiry_transitions)
bad("product-expiry-transitions-wrong-decision.json", "schema-violation", "/decision",
    "The finite time subset retains Proposed0030 and system-test scope.",
    {**expiry_transitions, "decision": "0029"})
finite_open = copy.deepcopy(lifecycle_open)
finite_operation = {**OP, "sequence": "28"}
finite_clock = {"reading": {"domain": "ab" * 32, "tick": "100"}, "logical": "100"}
finite_open["meta"]["session_clock"] = copy.deepcopy(finite_clock)
finite_open["sessions"][3].update(expiry={"After": 7}, opened_by=finite_operation)
finite_open["operations"][-1] = product_operation(
    28, 8, be(2, 1) + be(1, 1) + be(2, 1) + be(7, 4) + be(0, 1)
    + be(10, 2) + b"new-client".hex() + be(100),
    {"kind": "SessionLifecycle", "change": {
        "kind": "Opened", "session": "4", "revision": "1", "session_present": False,
        "expiry": {"After": 7}, "retired": None, "route_revision": "2",
        "cancelled": {"inbound": 0, "deliveries": 0, "routes": 0},
    }},
)
ok("store-snapshot-session-finite-open.json", finite_open)
finite_detached = copy.deepcopy(finite_open)
detach_operation = {**OP, "sequence": "29"}
detached = {"operation": detach_operation, "deadline": "7000000100"}
finite_detached["sessions"][3]["detached"] = detached
finite_detached["operations"].append(product_operation(
    29, 8, be(3, 1) + be(4) + be(1, 1) + scoped(28) + be(2, 1) + be(7, 4) + be(100),
    {"kind": "SessionLifecycle", "change": {
        "kind": "Detached", "session": "4", "expiry": {"After": 7}, "detached": detached,
    }},
))
finite_detached["account"]["used"]["receipt_slots"] = "3"
ok("store-snapshot-session-finite-detached.json", finite_detached)
finite_expired = copy.deepcopy(finite_detached)
finite_expired["sessions"].pop(3)
finite_expired["operations"].append(product_operation(
    30, 8, be(4, 1) + be(4) + be(1, 1) + scoped(28) + scoped(29) + be(1, 1)
    + be(7_000_000_100) + be(7_000_000_100),
    {"kind": "SessionLifecycle", "change": {
        "kind": "Closed", "session": "4", "route_revision": "2",
        "cancelled": {"inbound": 0, "deliveries": 0, "routes": 0},
    }},
))
finite_expired["account"]["used"]["receipt_slots"] = "4"
ok("store-snapshot-session-finite-expired.json", finite_expired)
for seconds in (0, 4294967295):
    bad(f"store-snapshot-session-after-{seconds}.json", "schema-violation", "/sessions/3/expiry",
        "Zero and infinity are not aliases for the finite After discriminant.",
        edited(finite_detached, lambda v, n=seconds: v["sessions"][3].update(expiry={"After": n})))
bad("store-snapshot-session-deadline-overflow.json", "u64-out-of-range", "/sessions/3/detached/deadline",
    "An offline deadline is checked logical nanoseconds, never a saturating value.",
    edited(finite_detached, lambda v: v["sessions"][3]["detached"].update(deadline=OVER)))
for field in ("logical", "tick"):
    target = finite_detached["meta"]["session_clock"]
    pointer = "/meta/session_clock/" + ("reading/" if field == "tick" else "") + field
    def bad_anchor(v, name=field):
        anchor = v["meta"]["session_clock"]
        (anchor["reading"] if name == "tick" else anchor)[name] = OVER
    bad(f"store-snapshot-session-clock-{field}-overflow.json", "u64-out-of-range", pointer,
        "The one anchor preserves checked durable logical progress.", edited(finite_detached, bad_anchor))
bad("store-snapshot-session-clock-invalid-domain.json", "schema-violation", "/meta/session_clock/reading/domain",
    "A domain is a fixed bounded lower-case 32-byte identity, not UTC.",
    edited(finite_detached, lambda v: v["meta"]["session_clock"]["reading"].update(domain="AB" * 32)))
finite_recovery = copy.deepcopy(PAGE_READ)
finite_recovery["payload"]["facts"][1].update(expiry={"After": 7}, opened_by=finite_operation, detached=detached)
ok("trace-record-session-finite-recovery.json", finite_recovery)
ok("product-session-clock-anchor.json", env("mqttiumd.product-event", seq="1", kind="session_clock_anchor", pid=1,
    details={"domain":"ab"*32,"tick":"100","logical":"100","origin_status":"boottime_origin_recognized"}))
finite_changed = env("mqttiumd.product-event", seq="20", kind="session_changed", pid=1,
    operation=detach_operation, connection="1", details={
        "action": "Detached", "session": "4", "expiry": {"After": 7},
        "detached_by": detach_operation, "deadline": "7000000100"})
ok("product-lifecycle-submitted-finite.json", env("mqttiumd.product-event", seq="15", pid=1,
    kind="command_submitted", operation=finite_operation, command_kind="SessionLifecycle",
    details={"action":"Open", "clean_start":False, "expected_session":None, "expiry":{"After":1}}))
ok("product-session-detached.json", finite_changed)
ok("product-session-opened-finite.json", edited(session_changed, lambda v: v["details"].update(expiry={"After": 7})))
for field in ("deadline", "detached_by"):
    bad(f"product-session-detached-missing-{field}.json", "schema-violation", "/details",
        "The detached occurrence retains both its original operation and deadline.",
        edited(finite_changed, lambda v, key=field: v["details"].pop(key)))
for kind, details in (
    ("session_clock_checkpoint", {"logical": "100", "tick": "100"}),
    ("session_clock_checkpoint", {"logical": "100", "tick": "100", "same_domain": True, "source": "maintenance_commit"}),
    ("session_clock_degraded", {"code": "checkpoint_late", "age_ms": "31001"}),
    ("expiry_inventory", {"finite_timers": 128, "checkpoint_inflight": True, "queued": 4, "period_ms": 30000}),
):
    suffix = "-writer" if "source" in details else ""
    ok("product-" + kind.replace("_", "-") + suffix + ".json",
       env("mqttiumd.product-event", seq="21", kind=kind, pid=1, details=details))
bad("product-expiry-timer-budget-exceeded.json", "schema-violation", "/details/finite_timers",
    "A session has at most one keyed wakeup within the unchanged128-session budget.",
    env("mqttiumd.product-event", seq="22", kind="expiry_inventory", pid=1,
        details={"finite_timers": 129, "checkpoint_inflight": False, "queued": 0, "period_ms": 30000}))

# Hierarchical/options records are descriptive formats, not routing qualification.
route_options_page = record(15, "store-adapter", 3, "RecoveryPageRead", {
    "run_epoch": "2", "page": "1", "last": False,
    "facts": [{**route(b"p1/+/#", 2), "options": 44}],
})
ok("trace-record-recovery-page-hierarchical-options.json", route_options_page)
options_snapshot = copy.deepcopy(snapshot)
options_snapshot["operations"].append(product_operation(
    31, 9, be(2) + be(1) + be(2) + be(1, 1) + be(2, 1) + be(44, 1)
    + be(6, 2) + b"p1/+/#".hex(),
    {"kind": "SubscriptionsChanged", "session": "2", "route_revision": "3"},
))
options_snapshot["route_table"]["revision"] = "3"
options_snapshot["route_table"]["routes"].insert(0, {
    "topic_hex": b"p1/+/#".hex(), "targets": [{"session": "2", "qos": 1, "options": 44}],
})
options_snapshot["account"]["used"]["receipt_slots"] = "2"
ok("store-snapshot-subscription-options-receipt.json", options_snapshot)
for name, options in (("qos-bits", 1), ("rh3", 48), ("boolean", True)):
    bad("trace-record-route-options-" + name + ".json", "schema-violation", "/payload/facts/0/options",
        "Subscription options exclude QoS/reserved bits and RH3; booleans are not byte integers.",
        edited(route_options_page, lambda v, bits=options: v["payload"]["facts"][0].update(options=bits)))
    bad("store-snapshot-route-options-" + name + ".json", "schema-violation", "/route_table/routes/0/targets/0/options",
        "Stored nondefault option bytes retain their validated discriminant.",
        edited(options_snapshot, lambda v, bits=options: v["route_table"]["routes"][0]["targets"][0].update(options=bits)))
capabilities = env("mqttiumd.product-event", seq="1", kind="ready", pid=1, details={
    "wildcard_subscriptions": True, "no_local": True, "shared_subscriptions": False,
    "subscription_identifiers": False, "sqlite_schema": 9, "profile": "tcp-message-expiry-pubsub-v1",
    "retained_available": True, "retained_topics": 32, "retained_bytes": 1048576, "max_replay_candidates": 256,
})
ok("product-hierarchical-capabilities.json", capabilities)
bad("product-details-over-budget.json", "schema-violation", "/details",
    "Product diagnostics remain bounded even with the six new startup capabilities.",
    env("mqttiumd.product-event", seq="1", kind="starting", pid=1,
        details={"field" + str(index): 0 for index in range(41)}))

# Descriptive Proposed0033 examples. These are generated format fixtures, never
# a fabricated product run, clock-continuity proof or qualifying expiry result.
message_transitions = json.loads((V1 / "message-expiry.transitions.json").read_text())
ok("product-message-expiry-transitions.json", message_transitions)
bad("product-message-expiry-transitions-wrong-decision.json", "schema-violation", "/decision",
    "Message lifetime is the explicitly Proposed0033 product subset.",
    {**message_transitions, "decision": "0032"})
message_time = {"domain_hex": "ab" * 32, "tick": "100"}
message_expiry = {"observed": message_time, "interval": 2}
message_time_hex = "ab" * 32 + be(100)
message_expiry_hex = message_time_hex + be(2, 4)
message_snapshot = copy.deepcopy(snapshot)
message_snapshot["publications"][0]["expiry"] = copy.deepcopy(message_expiry)
message_snapshot["operations"][0]["recipe_hex"] = (
    b"MQDR".hex() + be(1, 2) + be(13, 1) + be(len(RECIPE_HEX) // 2, 4)
    + RECIPE_HEX + message_expiry_hex)
message_snapshot["meta"]["session_clock"] = {
    "reading": {"domain": "ab" * 32, "tick": "100"}, "logical": "100"}
ok("store-snapshot-message-expiry.json", message_snapshot)
for suffix, interval in (("zero", 0), ("maximum", 4294967295)):
    ok(f"store-snapshot-message-expiry-{suffix}.json",
       edited(message_snapshot, lambda v, interval=interval:
           (v["publications"][0]["expiry"].update(interval=interval),
            v["operations"][0].update(recipe_hex=v["operations"][0]["recipe_hex"][:-8] + be(interval, 4)))))
for suffix, pointer, edit in (
    ("interval-overflow", "/publications/0/expiry/interval", lambda v: v["publications"][0]["expiry"].update(interval=4294967296)),
    ("interval-boolean", "/publications/0/expiry/interval", lambda v: v["publications"][0]["expiry"].update(interval=True)),
    ("invalid-domain", "/publications/0/expiry/observed/domain_hex", lambda v: v["publications"][0]["expiry"]["observed"].update(domain_hex="AB" * 32)),
    ("utc-authority", "/publications/0/expiry", lambda v: v["publications"][0]["expiry"].update(utc="2026-01-01")),
):
    bad(f"store-snapshot-message-expiry-{suffix}.json", "schema-violation", pointer,
        "Message expiry preserves a typed, bounded original domain/tick and finite u32 seconds.",
        edited(message_snapshot, edit))
bad("store-snapshot-message-expiry-tick-overflow.json", "u64-out-of-range", "/publications/0/expiry/observed/tick",
    "Reception ticks are canonical checked nanoseconds.",
    edited(message_snapshot, lambda v: v["publications"][0]["expiry"]["observed"].update(tick=OVER)))
bad("store-snapshot-message-expiry-deadline-overflow.json", "u64-out-of-range", "/publications/0/expiry",
    "A valid u64 reception tick cannot make a finite lifetime overflow its deadline.",
    edited(message_snapshot, lambda v: v["publications"][0]["expiry"]["observed"].update(tick="18446744073709551615")))
started_snapshot = copy.deepcopy(claimed_snapshot)
started_snapshot["publications"][0]["expiry"] = copy.deepcopy(message_expiry)
started_snapshot["operations"][0]["recipe_hex"] = message_snapshot["operations"][0]["recipe_hex"]
started_snapshot["meta"]["session_clock"] = copy.deepcopy(message_snapshot["meta"]["session_clock"])
started_snapshot["outbound"][0]["started"] = True
started_snapshot["operations"].append(product_operation(26, 15, delivery_body + scoped(21), {
    **claim_operation["decision"]["facts"], "kind": "DeliveryStarted"}))
started_snapshot["account"]["used"]["receipt_slots"] = "3"
ok("store-snapshot-delivery-started.json", started_snapshot)
expired_snapshot = copy.deepcopy(finished_snapshot)
expired_snapshot["publications"][0]["expiry"] = copy.deepcopy(message_expiry)
expired_snapshot["operations"][0]["recipe_hex"] = message_snapshot["operations"][0]["recipe_hex"]
expired_snapshot["meta"]["session_clock"] = copy.deepcopy(message_snapshot["meta"]["session_clock"])
expired_snapshot["operations"][-1] = product_operation(26, 16,
    delivery_body + scoped(21) + message_expiry_hex + "ab" * 32 + be(2_000_000_100), {
        **claim_operation["decision"]["facts"], "kind": "DeliveryExpired"})
ok("store-snapshot-delivery-expired.json", expired_snapshot)
message_recovery = copy.deepcopy(PAGE_READ)
message_recovery["payload"]["facts"] = [{
    "kind": "OutboundDelivery", "delivery_id": OUTBOUND_SLOT["delivery_id"],
    "target_session": "2", "target_revision": "1", "qos": 1,
    "expiry": copy.deepcopy(message_expiry), "started": True,
    "claim": {"operation_id": OP, "packet_id": 1, "started": True},
}]
ok("trace-record-message-expiry-recovered-started.json", message_recovery)
bad("trace-record-message-expiry-start-contradiction.json", "schema-violation", "/payload/facts/0/claim/started",
    "Recovered start facts and their exact claim cannot disagree.",
    edited(message_recovery, lambda v: v["payload"]["facts"][0]["claim"].update(started=False)))
for index, kind in enumerate(("message_expiry_received", "message_expiry_sweep", "message_expiry_clock",
        "message_transmission_started", "message_transmission_not_started", "message_expiry_quarantined", "message_expiry_applied",
        "message_expiry_result_dropped")):
    ok(f"product-{kind.replace('_', '-')}.json", env("mqttiumd.product-event", seq=str(index + 1), pid=1, kind=kind,
        details={"tick": "100", "code": "format_example"}))
for index, command in enumerate(("DeliveryStarted", "MessageExpire")):
    ok(f"product-command-{command}.json", env("mqttiumd.product-event", seq=str(index + 1), pid=1,
        kind="command_submitted", operation=OP, command_kind=command, details={}))
for field in ("message_expiry", "message_expiry_interval", "message_expiry_scan_ms", "message_expiry_batch",
        "message_expiry_policy", "message_expiry_time_unit", "sqlite_schema_version"):
    invalid_profile = copy.deepcopy(product_profile)
    invalid_profile[field] = False if field == "message_expiry" else "unsupported"
    bad(f"product-profile-wrong-{field}.json", "schema-violation", f"/{field}",
        "The profile declares the actual bounded Message Expiry implementation and quarantine policy.", invalid_profile)

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
