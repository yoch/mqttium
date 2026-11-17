"""Semantic rules of the v1 formats: what a JSON Schema cannot state.

Written from ``contracts/README.md`` and the chapters it cites (DEM-014,
DEM-015, DEM-017, DEM-023). Each rule yields diagnostics whose codes are the
closed vocabulary of the example corpus.
"""

from __future__ import annotations

import hashlib
import itertools

from .strict import Diagnostic, Refused, u64

MAX_POINTS_PER_CELL = 4096
MAX_POINTS_PER_REGISTRY = 262_144


def check(contracts, document):
    """The semantic diagnostics of a document that satisfies its schema."""
    rule = _RULES.get(document["format"])
    out = []
    if rule is not None:
        rule(contracts, document, out)
    return out


# ------------------------------------------------------------------ helpers


def _get(document, pointer):
    value = document
    for part in pointer.split("/")[1:]:
        if isinstance(value, list):
            value = value[int(part)] if part.isdigit() and int(part) < len(value) else None
        elif isinstance(value, dict):
            value = value.get(part)
        else:
            return None
        if value is None:
            return None
    return value


# The capacities of a fixture that are 32-bit quantities, as the fixture
# schema declares them through the ``u32`` primitive.
U32_CAPACITIES = ("max_targets_per_admission", "max_topic_bytes")


def _u64(document, pointer, out):
    """The value of the decimal string at ``pointer``; ``None`` if absent or bad."""
    raw = _get(document, pointer)
    if not isinstance(raw, str):
        return None
    try:
        return u64(raw)
    except ValueError as error:
        out.append(Diagnostic(str(error), pointer, f"{raw!r} is not a 64-bit decimal string"))
        return None


def _unique(entries, what, code, out):
    """``entries`` are ``(pointer, key)``; every repeated key is reported."""
    seen = set()
    for pointer, key in entries:
        if key in seen:
            out.append(Diagnostic(code, pointer, f"{what} {key!r} is defined twice"))
        seen.add(key)


def _named(document, table, key):
    return [(f"/{table}/{index}/{key}", row[key]) for index, row in enumerate(document[table])]


def _product_registry(contracts, document, out):
    _unique(_named(document, "rules", "id"), "product rule", "duplicate-name", out)


def _requirements(contracts, holder, base, keys, out):
    for key in keys:
        for index, identifier in enumerate(holder.get(key, [])):
            if identifier not in contracts.catalogs["requirements"]:
                out.append(
                    Diagnostic(
                        "requirement-unknown",
                        f"{base}/{key}/{index}",
                        f"{identifier!r} is not in the requirement register",
                    )
                )


# ----------------------------------------------------------------- scenario

# What each action of an injection point acts on; None fits any point.
_ACTS_ON = {
    "barrier": None,
    "hold_command": "command",
    "refuse_fence": "command",
    "drop_result": "result",
    "delay_result": "result",
    "duplicate_result": "result",
}
# What a fault keeps until a step hands it over.
# A barrier keeps the adapter where it stopped until LeaveBarrier.
_HOLDS = {"hold_command": "command", "delay_result": "result", "barrier": "barrier"}

_NOT_YET, _HELD, _HANDED, _DESTROYED = range(4)
_WHY_NOT = {
    _NOT_YET: "no earlier step drives until this fault is hit",
    _HANDED: "an earlier step already handed it over",
    _DESTROYED: "a crash destroyed what the fault held",
}


def _overlap(fault, earlier):
    """Whether two faults can select the same command or result.

    Selections of one point, one command kind and compatible bindings
    overlap when they name the same occurrence, or when only one of them
    names the effect or the publication: the two then count occurrences
    over different sets.
    """
    mine, theirs = fault["match"], earlier["match"]

    def compatible(key):
        return key not in mine or key not in theirs or mine[key] == theirs[key]

    return (
        fault["point"] == earlier["point"]
        and mine["command_kind"] == theirs["command_kind"]
        and compatible("publication_binding")
        and compatible("result_of")
        and (
            _same_occurrence(fault, earlier)
            or ("result_of" in mine) != ("result_of" in theirs)
            or ("publication_binding" in mine) != ("publication_binding" in theirs)
        )
    )


def _occurrences(fault):
    """The first occurrence a fault selects, and whether it selects every later one."""
    if "from_occurrence" in fault:
        return int(fault["from_occurrence"]), True
    return int(fault["occurrence"]), False


def _same_occurrence(fault, earlier):
    """Whether two faults can select the same occurrence (record 0018)."""
    (first, every), (other, every_other) = _occurrences(fault), _occurrences(earlier)
    if every and every_other:
        return True
    if every:
        return other >= first
    if every_other:
        return first >= other
    return first == other


def _scenario(contracts, document, out):
    catalogs = contracts.catalogs
    budget = _u64(document, "/budgets/max_steps", out)
    _u64(document, "/budgets/max_trace_bytes", out)
    if document["profile"] not in catalogs["profiles"]:
        out.append(Diagnostic("profile-unknown", "/profile", "the profile is not catalogued"))

    clients = None
    try:
        fixture = contracts.load_reference(
            "/initial_fixture", document["initial_fixture"], "mqttiumd.fixture"
        )
        clients = {session["client"] for session in fixture["sessions"]}
    except Refused as refusal:
        out.append(refusal.diagnostic)

    definitions = []
    # fault -> [what it keeps, whether it still keeps it, expected at all]
    holds = {}
    for index, fault in enumerate(document["faults"]):
        base = f"/faults/{index}"
        repeated = "from_occurrence" in fault
        if repeated:
            # Record 0018: a repeated fault is reached at least so often; a
            # repeated delay delivers each answer by its lag, so it holds
            # no one item a step could hand over.
            _u64(document, f"{base}/from_occurrence", out)
            expected = _u64(document, f"{base}/expected_hits_at_least", out)
        else:
            _u64(document, f"{base}/occurrence", out)
            expected = _u64(document, f"{base}/expected_hits", out)
        definitions.append((f"{base}/id", fault["id"]))
        item = None if repeated else _HOLDS.get(fault["action"])
        holds.setdefault(fault["id"], [item, _NOT_YET, expected != 0])
        if item is not None and expected is not None and expected > 1:
            out.append(
                Diagnostic(
                    "held-item-mismatch",
                    f"{base}/expected_hits",
                    "a fault that holds something holds one item: it is reached at most once",
                )
            )
        if "bind_operation_as" in fault:
            definitions.append((f"{base}/bind_operation_as", fault["bind_operation_as"]))
        point = catalogs["fault_points"].get(fault["point"])
        if point is None:
            out.append(
                Diagnostic(
                    "fault-point-unknown",
                    f"{base}/point",
                    f"{fault['point']!r} is not a contractual injection point",
                )
            )
        else:
            carries, actions = point
            if fault["action"] not in actions:
                out.append(
                    Diagnostic(
                        "fault-action-unknown",
                        f"{base}/action",
                        f"{fault['point']!r} does not offer {fault['action']!r}",
                    )
                )
            if "result_of" in fault["match"] and carries != "result":
                out.append(
                    Diagnostic(
                        "fault-match-invalid",
                        f"{base}/match/result_of",
                        f"{fault['point']!r} carries a command, not a result",
                    )
                )
        if fault["match"]["command_kind"] not in catalogs["command_kinds"]:
            out.append(
                Diagnostic(
                    "command-kind-unknown",
                    f"{base}/match/command_kind",
                    f"{fault['match']['command_kind']!r} is not a command kind",
                )
            )
        # Two faults must not select the same item: which of them acts
        # would depend on the store.
        for earlier in document["faults"][:index]:
            if _overlap(fault, earlier):
                out.append(
                    Diagnostic(
                        "fault-match-invalid",
                        f"{base}/match",
                        f"{earlier['id']!r} and {fault['id']!r} can select the same item",
                    )
                )

    # A fence fault names the resolved operation by its command as the
    # bench knows it: some fault at the command point selects that command
    # kind and that publication (record 0015, the fault catalogue).
    for index, fault in enumerate(document["faults"]):
        if fault["point"] != "store.fence.transit":
            continue
        if not any(
            other["point"] == "store.command.transit"
            and other["match"]["command_kind"] == fault["match"]["command_kind"]
            and other["match"].get("publication_binding")
            == fault["match"].get("publication_binding")
            for other in document["faults"]
        ):
            out.append(
                Diagnostic(
                    "fault-match-invalid",
                    f"/faults/{index}/match",
                    f"{fault['id']!r} names a command no fault at store.command.transit selects",
                )
            )

    exchanges = {
        step["bind_exchange_as"] for step in document["steps"] if "bind_exchange_as" in step
    }
    for index, fault in enumerate(document["faults"]):
        binding = fault["match"].get("publication_binding")
        if binding is not None and binding not in exchanges:
            out.append(
                Diagnostic(
                    "binding-undefined",
                    f"/faults/{index}/match/publication_binding",
                    f"no step binds an exchange as {binding!r}",
                )
            )

    bound = set()
    checkpoints = []
    broker_up = True
    for index, step in enumerate(document["steps"]):
        base = f"/steps/{index}"
        kind = step["kind"]
        # A step refused where it stands is not judged further: one
        # misplaced step is one diagnostic.
        if kind == "RestartBroker":
            if broker_up:
                out.append(Diagnostic("step-order-invalid", base, "the broker is already running"))
            broker_up = True
        elif not broker_up:
            out.append(
                Diagnostic(
                    "step-order-invalid", base, "the broker is crashed; only a restart may follow"
                )
            )
        elif kind == "CrashBroker":
            broker_up = False
            for hold in holds.values():
                if hold[1] == _HELD:
                    hold[1] = _DESTROYED
        elif kind == "DriveUntil" and step["event"] == "FaultHit":
            hold = holds.get(step["binding"])
            if hold is None:
                pass
            elif not hold[2]:
                out.append(
                    Diagnostic(
                        "step-order-invalid",
                        base,
                        "the scenario waits for a fault it expects never to reach",
                    )
                )
            elif hold[0] is not None and hold[1] != _NOT_YET:
                out.append(
                    Diagnostic(
                        "step-order-invalid",
                        base,
                        "a fault that holds something is waited for once",
                    )
                )
            elif hold[0] is not None:
                hold[1] = _HELD
        elif kind in ("ReleaseHeldCommand", "DeliverHeldResult", "LeaveBarrier"):
            wanted = {
                "ReleaseHeldCommand": "command",
                "DeliverHeldResult": "result",
                "LeaveBarrier": "barrier",
            }[kind]
            hold = holds.get(step["fault"])
            if hold is None:
                out.append(
                    Diagnostic(
                        "binding-undefined",
                        f"{base}/fault",
                        f"no fault is declared as {step['fault']!r}",
                    )
                )
            elif hold[0] != wanted:
                out.append(
                    Diagnostic(
                        "held-item-mismatch",
                        f"{base}/fault",
                        f"{step['fault']!r} holds no {wanted}",
                    )
                )
            elif hold[1] != _HELD:
                out.append(Diagnostic("step-order-invalid", base, _WHY_NOT[hold[1]]))
            else:
                hold[1] = _HANDED

        limit = _u64(document, f"{base}/max_steps", out)
        if limit is not None and budget is not None and limit > budget:
            out.append(
                Diagnostic(
                    "budget-exceeded", f"{base}/max_steps", f"{limit} exceeds the scenario budget"
                )
            )
        if clients is not None and "client" in step and step["client"] not in clients:
            out.append(
                Diagnostic(
                    "client-unknown",
                    f"{base}/client",
                    f"{step['client']!r} is not a session of the fixture",
                )
            )
        if "bind_exchange_as" in step:
            definitions.append((f"{base}/bind_exchange_as", step["bind_exchange_as"]))
            bound.add(step["bind_exchange_as"])
        if "exchange" in step and step["exchange"] not in bound:
            out.append(
                Diagnostic(
                    "binding-undefined",
                    f"{base}/exchange",
                    f"no earlier step binds an exchange as {step['exchange']!r}",
                )
            )
        if kind == "DriveUntil" and "binding" in step and step["binding"] not in holds:
            out.append(
                Diagnostic(
                    "binding-undefined",
                    f"{base}/binding",
                    f"no fault is declared as {step['binding']!r}",
                )
            )
        if "checkpoint" in step:
            checkpoints.append((f"{base}/checkpoint", step["checkpoint"]))
    _unique(definitions, "binding", "binding-duplicate", out)
    _unique(checkpoints, "checkpoint", "binding-duplicate", out)

    for index, assertion in enumerate(document["assertions"]):
        if assertion not in catalogs["assertions"]:
            out.append(
                Diagnostic(
                    "assertion-unknown",
                    f"/assertions/{index}",
                    f"{assertion!r} is not in the assertion catalogue",
                )
            )


# ------------------------------------------------------------------ fixture


def _fixture(contracts, document, out):
    _u64(document, "/store/incarnation", out)
    for name in document["capacities"]:
        value = _u64(document, f"/capacities/{name}", out)
        if name in U32_CAPACITIES and value is not None and value > 0xFFFF_FFFF:
            out.append(
                Diagnostic("u32-out-of-range", f"/capacities/{name}", f"{value} exceeds 4294967295")
            )
    if document["profile"] not in contracts.catalogs["profiles"]:
        out.append(Diagnostic("profile-unknown", "/profile", "the profile is not catalogued"))
    _unique(_named(document, "sessions", "client"), "client", "duplicate-name", out)
    _unique(_named(document, "sessions", "client_id"), "ClientID", "duplicate-name", out)
    _unique(_named(document, "routes", "topic"), "route", "duplicate-name", out)
    clients = {session["client"] for session in document["sessions"]}
    for route_index, route in enumerate(document["routes"]):
        for target_index, target in enumerate(route["targets"]):
            if target["client"] not in clients:
                out.append(
                    Diagnostic(
                        "client-unknown",
                        f"/routes/{route_index}/targets/{target_index}/client",
                        f"{target['client']!r} is not a session of this fixture",
                    )
                )


# --------------------------------------------------------------- catalogues


def _transition_catalog(contracts, document, out):
    for table in ("guard_dimensions", "event_classes", "machines"):
        _unique(_named(document, table, "name"), table, "duplicate-name", out)
    dimensions = {dimension["name"] for dimension in document["guard_dimensions"]}
    for index, dimension in enumerate(document["guard_dimensions"]):
        _unique(
            [
                (f"/guard_dimensions/{index}/values/{at}/value", value["value"])
                for at, value in enumerate(dimension["values"])
            ],
            "guard value",
            "duplicate-name",
            out,
        )
    events = {event["name"] for event in document["event_classes"]}
    for index, event in enumerate(document["event_classes"]):
        for at, dimension in enumerate(event["guard_dimensions"]):
            if dimension not in dimensions:
                out.append(
                    Diagnostic(
                        "catalog-reference-unknown",
                        f"/event_classes/{index}/guard_dimensions/{at}",
                        f"{dimension!r} is not a guard dimension",
                    )
                )
    for index, machine in enumerate(document["machines"]):
        for at, event in enumerate(machine["event_classes"]):
            if event not in events:
                out.append(
                    Diagnostic(
                        "catalog-reference-unknown",
                        f"/machines/{index}/event_classes/{at}",
                        f"{event!r} is not an event class",
                    )
                )


def _names_once(table, key):
    def rule(contracts, document, out):
        _unique(_named(document, table, key), table, "duplicate-name", out)

    return rule


def _fault_catalog(contracts, document, out):
    _unique(_named(document, "points", "point"), "injection point", "duplicate-name", out)
    # A point offers only actions that act on what it carries.
    for index, point in enumerate(document["points"]):
        for at, action in enumerate(point["actions"]):
            if action not in _ACTS_ON or _ACTS_ON[action] not in (None, point["carries"]):
                out.append(
                    Diagnostic(
                        "fault-action-unknown",
                        f"/points/{index}/actions/{at}",
                        f"{action!r} is not an action on a {point['carries']}",
                    )
                )


def _assertion_catalog(contracts, document, out):
    _unique(_named(document, "assertions", "name"), "assertion", "duplicate-name", out)
    for index, entry in enumerate(document["assertions"]):
        _requirements(contracts, entry, f"/assertions/{index}", ["requirements"], out)


# ----------------------------------------------------------------- registry


def _registry(contracts, document, out):
    try:
        catalog = contracts.load_reference(
            "/catalog", document["catalog"], "mqttiumd.transition-catalog"
        )
    except Refused as refusal:
        out.append(refusal.diagnostic)
        return
    dimensions = {
        dimension["name"]: [value["value"] for value in dimension["values"]]
        for dimension in catalog["guard_dimensions"]
    }
    events = {event["name"]: event["guard_dimensions"] for event in catalog["event_classes"]}
    machines = {machine["name"]: machine for machine in catalog["machines"]}
    effects = set(catalog["effects"])
    _unique(_named(document, "rules", "id"), "rule", "duplicate-name", out)

    def unknown(pointer, what, name):
        out.append(
            Diagnostic("catalog-reference-unknown", pointer, f"{name!r} is not a known {what}")
        )

    well_formed = []
    for index, rule in enumerate(document["rules"]):
        base = f"/rules/{index}"
        before = len(out)
        _requirements(contracts, rule, base, ["requirements", "witnesses", "mutations"], out)
        machine = machines.get(rule["machine"])
        if machine is None:
            unknown(f"{base}/machine", "machine", rule["machine"])
            continue
        for key in ("state_class", "next_state_class"):
            if rule[key] not in machine["state_classes"]:
                unknown(f"{base}/{key}", "state class", rule[key])
        if rule["event_class"] not in machine["event_classes"]:
            unknown(f"{base}/event_class", "event class", rule["event_class"])
        allowed = events.get(rule["event_class"], [])
        for dimension, values in rule["guard_partition"].items():
            if dimension not in allowed:
                unknown(
                    f"{base}/guard_partition/{dimension}",
                    "guard dimension of this event class",
                    dimension,
                )
                continue
            for at, value in enumerate(values):
                if value not in dimensions.get(dimension, []):
                    unknown(f"{base}/guard_partition/{dimension}/{at}", "guard value", value)
        for key in ("required_effects", "forbidden_effects"):
            for at, effect in enumerate(rule[key]):
                if effect not in effects:
                    unknown(f"{base}/{key}/{at}", "effect", effect)
                elif key == "forbidden_effects" and effect in rule["required_effects"]:
                    out.append(
                        Diagnostic(
                            "registry-effect-conflict",
                            f"{base}/{key}/{at}",
                            f"{effect!r} is both required and forbidden",
                        )
                    )
        if len(out) == before:
            well_formed.append((index, rule))

    # The budget first: sizes are multiplied, never enumerated, before any
    # point exists.
    def size(event):
        points = 1
        for dimension in events.get(event, []):
            points *= len(dimensions.get(dimension, []))
        return points

    total = 0
    for machine in machines.values():
        for _state in machine["state_classes"]:
            for event in machine["event_classes"]:
                points = size(event)
                total += points
                if points > MAX_POINTS_PER_CELL or total > MAX_POINTS_PER_REGISTRY:
                    out.append(
                        Diagnostic(
                            "coverage-budget-exceeded",
                            "/catalog",
                            "the guard domain is larger than the coverage budget",
                        )
                    )
                    return

    complete = document["status"] == "complete"
    for name, machine in machines.items():
        for state in machine["state_classes"]:
            for event in machine["event_classes"]:
                domain = [(dimension, dimensions[dimension]) for dimension in events[event]]
                cell = [
                    (index, rule)
                    for index, rule in well_formed
                    if (rule["machine"], rule["state_class"], rule["event_class"])
                    == (name, state, event)
                ]
                holes = 0
                for point in itertools.product(*(values for _, values in domain)):
                    covering = [
                        index
                        for index, rule in cell
                        if all(
                            value in rule["guard_partition"].get(dimension, [value])
                            for (dimension, _), value in zip(domain, point, strict=True)
                        )
                    ]
                    if not covering:
                        holes += 1
                    elif len(covering) > 1:
                        out.append(
                            Diagnostic(
                                "registry-overlap",
                                f"/rules/{covering[1]}",
                                f"two rules cover {name} {state} x {event} {point}",
                            )
                        )
                if holes and complete:
                    out.append(
                        Diagnostic(
                            "registry-hole",
                            "/rules",
                            f"{name} {state} x {event}: {holes} guard points uncovered",
                        )
                    )


# ---------------------------------------------------------------- run result


def expectation(fault):
    """How often a fault of a scenario or a result is expected: exactly, or at least."""
    if "expected_hits_at_least" in fault:
        return ("at_least", fault["expected_hits_at_least"])
    return ("exactly", fault["expected_hits"])


def reached(fault, hits):
    """Whether ``hits`` reaches what ``fault`` expects."""
    kind, count = expectation(fault)
    return int(hits) >= int(count) if kind == "at_least" else int(hits) == int(count)


def verdict_of(statuses, injections_reached):
    """The verdict that assertion statuses and injections determine (DEM-023)."""
    if "FAIL" in statuses:
        return "FAIL"
    if "INCONCLUSIVE" in statuses or not injections_reached or not statuses:
        return "INCONCLUSIVE"
    if all(status == "PASS" for status in statuses):
        return "PASS"
    return "OUT_OF_SCOPE"


def _run_result(contracts, document, out):
    _unique(_named(document, "assertions", "name"), "assertion", "duplicate-name", out)
    _unique(_named(document, "faults", "id"), "fault", "duplicate-name", out)
    statuses = [assertion["status"] for assertion in document["assertions"]]
    reached = True
    for index, fault in enumerate(document["faults"]):
        hits = _u64(document, f"/faults/{index}/hits", out)
        if "expected_hits_at_least" in fault:
            least = _u64(document, f"/faults/{index}/expected_hits_at_least", out)
            reached = reached and None not in (least, hits) and hits >= least
        else:
            expected = _u64(document, f"/faults/{index}/expected_hits", out)
            reached = reached and expected is not None and expected == hits
    for index in range(len(document["artifacts"])):
        _u64(document, f"/artifacts/{index}/bytes", out)
    if "failure_count" in document:
        count = _u64(document, "/failure_count", out)
        failures = document["failures"]
        for index, failure in enumerate(failures):
            for member in ("process_incarnation", "record_sequence", "page"):
                if member in failure:
                    _u64(document, f"/failures/{index}/{member}", out)
        if count is not None and len(failures) != min(count, 64):
            out.append(Diagnostic(
                "result-incomplete", "/failures",
                "the failure list must contain exactly min(failure_count, 64) contexts",
            ))

    qualifying = document["qualification"] == "qualifying"
    expected = verdict_of(statuses, reached)
    if not qualifying and expected == "PASS":
        # A provisional document lacks the evidence a PASS states.
        expected = "INCONCLUSIVE"
    if document["verdict"] != expected:
        out.append(
            Diagnostic(
                "verdict-inconsistent",
                "/verdict",
                f"the assertions and injections determine {expected}, not {document['verdict']}",
            )
        )
    if not qualifying:
        return

    if document["contracts_digest"] != contracts.digest:
        out.append(
            Diagnostic(
                "evidence-mismatch",
                "/contracts_digest",
                f"the result names other contracts than the ones in use ({contracts.digest})",
            )
        )
    trace = document["trace"]
    size = _u64(document, "/trace/bytes", out)
    try:
        digest, length = contracts.hash_evidence(trace["path"])
        if digest != trace["sha256"] or length != size:
            out.append(
                Diagnostic(
                    "evidence-mismatch",
                    "/trace",
                    f"{trace['path']} has {length} bytes and the digest {digest}",
                )
            )
    except Refused as refusal:
        found = refusal.diagnostic
        out.append(Diagnostic(found.code, "/trace", found.message))

    def cited(member, form):
        entry = document[member]
        size = _u64(document, f"/{member}/bytes", out)
        return contracts.load_reference(
            f"/{member}", entry["path"], form, cited=(entry["sha256"], size)
        )

    try:
        manifest = cited("manifest", "mqttiumd.run-manifest")
        if manifest["contracts_digest"] != document["contracts_digest"]:
            out.append(
                Diagnostic(
                    "evidence-mismatch",
                    "/manifest",
                    "the manifest and the result name different contracts",
                )
            )
    except Refused as refusal:
        out.append(refusal.diagnostic)
    try:
        scenario = cited("scenario", "mqttiumd.scenario")
    except Refused as refusal:
        out.append(refusal.diagnostic)
        return

    reported = {assertion["name"] for assertion in document["assertions"]}
    if set(scenario["assertions"]) | set(contracts.catalogs["every_run"]) != reported:
        out.append(
            Diagnostic(
                "result-incomplete",
                "/assertions",
                "the result does not report exactly the assertions of its scenario "
                "and those every run owes",
            )
        )
    for index, status in enumerate(statuses):
        if status == "OUT_OF_SCOPE":
            out.append(
                Diagnostic(
                    "result-incomplete",
                    f"/assertions/{index}/status",
                    "an assertion of the scenario cannot be out of scope for a run of it",
                )
            )
    declared = {fault["id"]: expectation(fault) for fault in scenario["faults"]}
    observed = {fault["id"]: expectation(fault) for fault in document["faults"]}
    if declared != observed:
        out.append(
            Diagnostic(
                "result-incomplete", "/faults", "the faults are not exactly those of the scenario"
            )
        )


# ------------------------------------------------- manifest, trace, snapshot


def contracts_digest(files):
    """The digest of ``(sha256, path)`` pairs, in the order given."""
    listing = "".join(f"{sha256}  {path}\n" for sha256, path in files)
    return hashlib.sha256(listing.encode()).hexdigest()


def _run_manifest(contracts, document, out):
    _u64(document, "/artifact/bytes", out)
    for index in range(len(document["contracts"])):
        _u64(document, f"/contracts/{index}/bytes", out)
    if "built_from" in document["artifact"]:
        _u64(document, "/artifact/built_from/source/bytes", out)
    for pointer in ("/scenario/bytes", "/fixture/bytes", "/scheduler/version", "/scheduler/seed"):
        _u64(document, pointer, out)
    paths = [entry["path"] for entry in document["contracts"]]
    for index in range(1, len(paths)):
        if paths[index - 1] >= paths[index]:
            out.append(
                Diagnostic(
                    "evidence-mismatch",
                    f"/contracts/{index}",
                    "contract files are listed once each, in path order",
                )
            )
            break
    # The oracle and the generators are identified the same way.
    for member in ("oracle", "generators"):
        identity = document.get(member)
        if identity is None:
            continue
        for index in range(len(identity["files"])):
            _u64(document, f"/{member}/files/{index}/bytes", out)
        files = [entry["path"] for entry in identity["files"]]
        for index in range(1, len(files)):
            if files[index - 1] >= files[index]:
                out.append(
                    Diagnostic(
                        "evidence-mismatch",
                        f"/{member}/files/{index}",
                        "the files of an identity are listed once each, in path order",
                    )
                )
                break
        digest = contracts_digest((entry["sha256"], entry["path"]) for entry in identity["files"])
        if digest != identity["sha256"]:
            out.append(
                Diagnostic(
                    "evidence-mismatch",
                    f"/{member}/sha256",
                    f"the listed files have the digest {digest}",
                )
            )
    listed = contracts_digest((entry["sha256"], entry["path"]) for entry in document["contracts"])
    if listed != document["contracts_digest"]:
        out.append(
            Diagnostic(
                "evidence-mismatch",
                "/contracts_digest",
                f"the listed contract files have the digest {listed}",
            )
        )


def _decimal_members(schemas, file, schema, value, pointer, out, depth=0):
    """Range-check every member of ``value`` that its schema declares 64-bit.

    The walk follows the schema, not member names: a member is a 64-bit
    quantity when the schema that applies to it refers to the ``u64``
    primitive. ``file`` is the schema file ``schema`` belongs to, against
    which its relative references resolve.
    """
    if depth > 32 or not isinstance(schema, dict):
        return
    reference = schema.get("$ref")
    if reference is not None:
        if reference.endswith("#/$defs/u32"):
            if isinstance(value, str):
                try:
                    if u64(value) > 0xFFFF_FFFF:
                        out.append(
                            Diagnostic("u32-out-of-range", pointer, f"{value!r} exceeds 4294967295")
                        )
                except ValueError as error:
                    out.append(
                        Diagnostic(str(error), pointer, f"{value!r} is not a decimal string")
                    )
            return
        if reference.endswith("#/$defs/u64"):
            if isinstance(value, str):
                try:
                    u64(value)
                except ValueError as error:
                    out.append(
                        Diagnostic(str(error), pointer, f"{value!r} is not a 64-bit decimal string")
                    )
            return
        target_file, _, fragment = reference.partition("#")
        target_file = target_file or file
        target = schemas[target_file]
        for part in fragment.split("/")[1:]:
            target = target[part]
        _decimal_members(schemas, target_file, target, value, pointer, out, depth + 1)
        return

    def descend(child, item, where):
        _decimal_members(schemas, file, child, item, where, out, depth + 1)

    for branch in schema.get("allOf", []):
        condition = branch.get("if")
        if condition is None:
            descend(branch, value, pointer)
        elif _matches(condition, value):
            descend(branch.get("then"), value, pointer)
        else:
            descend(branch.get("else"), value, pointer)
    # An alternative applies to the values of its type: a nullable member
    # that holds an object is that object.
    for branch in schema.get("oneOf", []) + schema.get("anyOf", []):
        if isinstance(branch, dict) and branch.get("type") in (None, "object", "array"):
            descend(branch, value, pointer)
    if isinstance(value, dict):
        for key, child in schema.get("properties", {}).items():
            if key in value:
                descend(child, value[key], f"{pointer}/{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            descend(schema.get("items"), item, f"{pointer}/{index}")


def _matches(condition, value):
    """Whether ``value`` satisfies an ``if`` made of ``required`` and constants."""
    if not isinstance(value, dict):
        return False
    if any(key not in value for key in condition.get("required", [])):
        return False
    for key, expected in condition.get("properties", {}).items():
        if key in value and "const" in expected and value[key] != expected["const"]:
            return False
    return True


def _by_schema(contracts, document, out):
    from .documents import FORMATS

    file = FORMATS[document["format"]]
    _decimal_members(contracts.schemas, file, contracts.schemas[file], document, "", out)


# The place of a kind of fact in the inventory of a recovery, first to last
# (record 0014), and the kinds an inventory holds at most once.
FACT_RANKS = (
    "Epoch", "Limits", "RouteTable", "Route", "Session", "InboundExchange",
    "OutboundDelivery", "ReceiptFrontier", "Operation",
)
FACTS_ONCE = ("Epoch", "Limits", "RouteTable")


class InventoryOrder:
    """The order of the facts of a recovery, as ``RecoveryOrder`` states it.

    Ranks never decrease; the epoch, the limits and the route table appear
    at most once; routes are sorted by the bytes of their topic, equal
    topics accepted. A fresh value checks one page; a value carried from
    page to page checks a whole inventory. A refusal changes nothing.
    """

    def __init__(self):
        self.last = None
        self.last_topic = None

    def admit(self, fact):
        """``None`` when ``fact`` may come next, else ``(code, member, why)``."""
        rank = FACT_RANKS.index(fact["kind"])
        if self.last is not None:
            if rank < self.last:
                return (
                    "rows-out-of-order",
                    "kind",
                    f"a {fact['kind']} fact follows a {FACT_RANKS[self.last]} fact",
                )
            if rank == self.last and fact["kind"] in FACTS_ONCE:
                return ("duplicate-name", "kind", f"a second {fact['kind']} fact")
        if fact["kind"] == "Route":
            # Lower-case hexadecimal sorts like the bytes it spells.
            if self.last_topic is not None and fact["topic_hex"] < self.last_topic:
                return (
                    "rows-out-of-order",
                    "topic_hex",
                    "the topic of the route sorts before the topic of the route before it",
                )
            self.last_topic = fact["topic_hex"]
        self.last = rank
        return None


def _message_expiries(value, pointer, out):
    """Read checked deadline shape without asserting age or clock continuity."""
    if isinstance(value, list):
        for index, member in enumerate(value):
            _message_expiries(member, f"{pointer}/{index}", out)
    elif isinstance(value, dict):
        if "observed" in value and "interval" in value:
            try:
                tick = u64(value["observed"]["tick"])
            except (KeyError, ValueError, TypeError):
                pass  # The schema/decimal reader already reports the bad field.
            else:
                if tick + value["interval"] * 1_000_000_000 > 2**64 - 1:
                    out.append(Diagnostic("u64-out-of-range", pointer,
                        "the message expiry deadline exceeds 18446744073709551615 nanoseconds"))
        for name, member in value.items():
            _message_expiries(member, f"{pointer}/{name}", out)


def _trace_record(contracts, document, out):
    _by_schema(contracts, document, out)
    _message_expiries(document.get("payload"), "/payload", out)
    if document["kind"] in ("RecoveryPageRead", "RecoveryPageDelivered"):
        order = InventoryOrder()
        for index, fact in enumerate(document["payload"]["facts"]):
            refusal = order.admit(fact)
            if refusal is not None:
                code, member, why = refusal
                out.append(Diagnostic(code, f"/payload/facts/{index}/{member}", why))


def _key(value):
    """A hashable form of a JSON value."""
    if isinstance(value, dict):
        return tuple(sorted((key, _key(item)) for key, item in value.items()))
    if isinstance(value, list):
        return tuple(_key(item) for item in value)
    return value


# Table, the members that form the key of a row, and where a diagnostic
# points inside the row.
# A table, the members of its row key with how each is read, and where a
# diagnostic points. The schema says what a member is; its value never
# does: topic bytes made of decimal digits are bytes.
_SNAPSHOT_KEYS = [
    ("/receipt_frontiers", [("/origin/incarnation", "decimal"), ("/origin/epoch", "decimal"),
                            ("/origin/owner", "decimal")], "/origin"),
    ("/sessions", [("/session", "decimal")], "/session"),
    ("/route_table/routes", [("/topic_hex", "bytes")], "/topic_hex"),
    ("/operations", [("/operation_id", "scoped")], "/operation_id"),
    ("/fences", [("/operation_id", "scoped")], "/operation_id"),
    ("/payloads", [("/payload_id", "scoped")], "/payload_id"),
    ("/publications", [("/publication_id", "scoped")], "/publication_id"),
    ("/retained", [("/topic_hex", "bytes")], "/topic_hex"),
    ("/obligations", [("/publication_id", "scoped"), ("/index", "integer")], ""),
    (
        "/inbound_exchanges",
        [("/exchange/session", "decimal"), ("/packet_id", "integer")],
        "",
    ),
    (
        "/outbound",
        [("/delivery_id/publication_id", "scoped"), ("/delivery_id/index", "integer")],
        "/delivery_id",
    ),
]
_SCOPED = ("incarnation", "epoch", "owner", "sequence")


def _sort_key(value, kind):
    """The components of one key member, read as ``kind``.

    A 64-bit quantity and a small integer sort numerically; bytes in
    lower-case hexadecimal sort like the bytes they spell; a scoped identity
    is its four components in their declared order. ``ValueError`` when a
    number is not one.
    """
    if kind == "decimal":
        return (u64(value),)
    if kind == "scoped":
        return tuple(u64(value[name]) for name in _SCOPED)
    return (value,)


def _store_snapshot(contracts, document, out):
    _by_schema(contracts, document, out)
    _message_expiries(document, "", out)
    for table, members, suffix in _SNAPSHOT_KEYS:
        previous = None
        for index, row in enumerate(_get(document, table) or ()):
            try:
                key = sum((_sort_key(_get(row, member), kind) for member, kind in members), ())
            except ValueError:
                # The range check already reported it.
                continue
            if previous is not None and key == previous:
                out.append(
                    Diagnostic(
                        "duplicate-name", f"{table}/{index}{suffix}", "the row is listed twice"
                    )
                )
            elif previous is not None and key < previous:
                out.append(
                    Diagnostic(
                        "rows-out-of-order",
                        f"{table}/{index}{suffix}",
                        "the row comes after a row of a greater key",
                    )
                )
            else:
                # A row out of order does not move the mark: one misplaced
                # row is one diagnostic.
                previous = key


def _product_event(contracts, document, out):
    """The product format shares the range of the durable identities."""
    _u64(document, "/seq", out)
    if "connection" in document:
        _u64(document, "/connection", out)
    if "operation" in document:
        for member in ("incarnation", "epoch", "owner", "sequence"):
            _u64(document, f"/operation/{member}", out)
    if "puback" in document:
        _decimal_members(contracts.schemas, "product-event.schema.json",
                         contracts.schemas["product-event.schema.json"]["$defs"]["puback_observation"],
                         document["puback"], "/puback", out)
        if "connection" in document:
            try:
                outer = u64(document["connection"])
                inner = u64(document["puback"]["connection"]["generation"])
            except ValueError:
                pass
            else:
                if outer != inner:
                    out.append(Diagnostic("evidence-mismatch", "/connection",
                                          "the envelope generation contradicts its immutable PUBACK observation"))
    for member in ("incarnation", "epoch", "owner", "through", "publication_through", "payload_through"):
        if "returned_" + member in document.get("details", {}):
            _u64(document, f"/details/returned_{member}", out)
    if document["kind"] == "command_submitted" and "expected_session" in document.get("details", {}):
        _u64(document, "/details/expected_session", out)
    if document["kind"] in {"session_changed", "session_clock_checkpoint", "session_clock_degraded",
            "message_expiry_received", "message_expiry_sweep", "message_expiry_clock",
            "message_transmission_started", "message_transmission_not_started",
            "message_expiry_quarantined", "message_expiry_applied", "message_expiry_result_dropped"}:
        for member in ("deadline", "logical", "tick", "age_ms"):
            if member in document.get("details", {}):
                _u64(document, f"/details/{member}", out)
        if "detached_by" in document.get("details", {}):
            for member in ("incarnation", "epoch", "owner", "sequence"):
                _u64(document, f"/details/detached_by/{member}", out)
    if document["kind"] in {"session_changed", "session_change_copied", "session_change_replayed",
                             "session_close_copied", "session_close_replayed"}:
        for member in ("session", "route_revision", "revision", "retired_session"):
            if member in document.get("details", {}):
                _u64(document, f"/details/{member}", out)
    if document["kind"] in {
        "receipt_collection_submitted", "receipt_collection_result", "receipt_collection_applied",
        "log_rotated",
    } or document.get("command_kind") == "ReceiptCollection":
        for member in ("incarnation", "epoch", "owner", "through", "publication_through",
                       "payload_through", "retained", "retained_from", "previous_last_seq",
                       "discarded_through"):
            if member in document.get("details", {}):
                _u64(document, f"/details/{member}", out)


_RULES = {
    "mqttiumd.product-event": _product_event,
    "mqttiumd.scenario": _scenario,
    "mqttiumd.fixture": _fixture,
    "mqttiumd.transition-registry": _registry,
    "mqttiumd.product-transition-registry": _product_registry,
    "mqttiumd.transition-catalog": _transition_catalog,
    "mqttiumd.run-result": _run_result,
    "mqttiumd.run-manifest": _run_manifest,
    "mqttiumd.trace-record": _trace_record,
    "mqttiumd.store-snapshot": _store_snapshot,
    "mqttiumd.requirement-register": _names_once("entries", "id"),
    "mqttiumd.assertion-catalog": _assertion_catalog,
    "mqttiumd.fault-catalog": _fault_catalog,
    "mqttiumd.profile-catalog": _names_once("profiles", "id"),
}
