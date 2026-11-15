"""Deciding the assertions of a run from its observations (DEM-019).

The observations are the trace, the store snapshots the trace cites, and the
scenario with its fixture. Every expectation is computed here, from the
specification: nothing is asked of the broker, and what the broker says
about itself (an ExchangeId, a generation) is never taken for the identity
the oracle reasons on.

Three rules hold for every assertion:

* Hidden truth of the store judges a run and never justifies an answer of
  the core.
* Order is what the trace establishes (``Trace.before``). Where it does not
  say, the conclusion is inconclusive: an event is never moved before its
  prerequisite to make a trace acceptable.
* Missing data is inconclusive, never accepted.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from . import recipe as recipes
from .strict import Refused, parse
from .trace import Trace
from .trace import read as read_trace

PASS, FAIL, INCONCLUSIVE, OUT_OF_SCOPE = "PASS", "FAIL", "INCONCLUSIVE", "OUT_OF_SCOPE"

# A violation of these stays a violation whatever records are added to the
# trace, so it stands even when the trace is incomplete. For the others a
# missing record could be the very one that justifies what was observed.
MONOTONE = {
    "one_admission_per_observed_inbound_exchange",
    "same_required_obligations_at_both_checkpoints",
    "same_recipe_for_op_A",
}


@dataclass
class Status:
    status: str
    reasons: list = field(default_factory=list)


def _key(identity: dict) -> tuple:
    """An identity object of decimal strings, as a tuple of integers."""
    return tuple(int(value) for value in identity.values())


@dataclass
class Exchange:
    """One QoS 2 exchange as the client lived it: the oracle's own identity."""

    client: str
    packet_id: int
    ordinal: int
    opened: object
    closed: object = None
    positive_pubrec: object = None


@dataclass
class Run:
    contracts: object
    scenario: dict
    fixture: dict
    trace: Trace
    # checkpoint -> snapshot document
    snapshots: dict = field(default_factory=dict)
    # checkpoint -> why its snapshot cannot be used
    missing: dict = field(default_factory=dict)

    # ------------------------------------------------------------- loading

    @classmethod
    def load(cls, contracts, scenario_path, trace_path):
        problems = contracts.check_file(scenario_path)
        if problems:
            raise Refused("reference-invalid", "", f"{scenario_path}: {problems[0]}")
        scenario = parse(contracts.read(scenario_path))
        fixture = parse(contracts.read(scenario["initial_fixture"]))
        budget = int(scenario["budgets"]["max_trace_bytes"])
        with (contracts.root / trace_path).open("rb") as stream:
            trace = read_trace(contracts, stream, max_bytes=budget)
        run = cls(contracts, scenario, fixture, trace)
        run._load_snapshots()
        return run

    def _load_snapshots(self):
        for record in self.trace.of_kind("DurableStateInspected"):
            checkpoint = record.payload["checkpoint"]
            cited = record.payload["snapshot"]
            if checkpoint in self.snapshots or checkpoint in self.missing:
                self.missing[checkpoint] = "the checkpoint was inspected twice"
                self.snapshots.pop(checkpoint, None)
                continue
            try:
                data = self.contracts.read(cited["path"])
            except Refused as refusal:
                self.missing[checkpoint] = str(refusal.diagnostic)
                continue
            if hashlib.sha256(data).hexdigest() != cited["sha256"] or len(data) != int(
                cited["bytes"]
            ):
                self.missing[checkpoint] = f"{cited['path']} is not the file the trace cites"
                continue
            problems = self.contracts.check_bytes(data)
            if problems:
                self.missing[checkpoint] = f"{cited['path']}: {problems[0]}"
                continue
            snapshot = parse(data)
            if snapshot["format"] != "mqttiumd.store-snapshot":
                self.missing[checkpoint] = f"{cited['path']} is not a store snapshot"
            elif snapshot["checkpoint"] != checkpoint:
                self.missing[checkpoint] = f"{cited['path']} is the snapshot of another checkpoint"
            else:
                self.snapshots[checkpoint] = (record, snapshot)

    # -------------------------------------------------------- derived facts

    def checkpoints(self):
        return [
            step["checkpoint"]
            for step in self.scenario["steps"]
            if step["kind"] == "InspectDurableState"
        ]

    def client_of_session(self):
        """SessionId -> client name, through the ClientID the fixture gives."""
        names = {
            session["client_id"].encode().hex(): session["client"]
            for session in self.fixture["sessions"]
        }
        found = {}
        for _, snapshot in self.snapshots.values():
            for row in snapshot["sessions"]:
                client = names.get(row["client_id_hex"])
                if client is not None:
                    found[int(row["session"])] = client
        return found

    def recipes(self):
        """OperationId -> set of recipe bytes seen for it in the snapshots."""
        found = {}
        for _, snapshot in self.snapshots.values():
            for row in snapshot["operations"]:
                found.setdefault(_key(row["operation_id"]), set()).add(row["recipe_hex"])
        return found

    def slots(self):
        """OperationId -> (kind, session, Packet Identifier) of the exchange it serves.

        Every content seen for the operation must be readable and name the
        same exchange; a recipe whose other fields changed still tells which
        exchange it belongs to.
        """
        found = {}
        for operation, seen in self.recipes().items():
            slots = set()
            for content in seen:
                try:
                    recipe = recipes.decode(bytes.fromhex(content))
                except recipes.RecipeError:
                    slots.add(None)
                    continue
                slots.add((recipe.kind, recipe.session, recipe.packet_id))
            if len(slots) == 1 and None not in slots:
                found[operation] = next(iter(slots))
        return found

    def exchanges(self):
        """The QoS 2 exchanges of every client, from what the peers did.

        An exchange opens with the first PUBLISH of a Packet Identifier that
        is not in use, and ends when the client observes PUBCOMP or a PUBREC
        that carries a failure. A PUBLISH sent meanwhile with the same
        identifier is a replay of the same exchange, whatever the broker
        calls it.
        """
        found, open_now, notes = [], {}, []
        for record in self.trace.records:
            if record.origin[0] != "peer":
                continue
            payload = record.payload
            slot = (payload.get("client"), payload.get("packet_id"))
            if record.kind == "PeerPacketSent" and payload["packet_kind"] == "PUBLISH":
                header = bytes.fromhex(payload["bytes_hex"][:2] or "00")[0]
                if header >> 4 != 3 or bool(header & 0x08) != payload["dup"]:
                    notes.append(f"record {record.sequence}: the bytes are not the PUBLISH stated")
                    continue
                if (header >> 1) & 3 != 2:
                    continue
                if slot not in open_now:
                    ordinal = sum(
                        1 for exchange in found if (exchange.client, exchange.packet_id) == slot
                    )
                    open_now[slot] = Exchange(slot[0], slot[1], ordinal, record)
                    found.append(open_now[slot])
            elif record.kind == "PeerPacketObserved" and slot in open_now:
                if payload["packet_kind"] == "PUBREC":
                    if payload["reason_code"] >= 0x80:
                        open_now.pop(slot).closed = record
                    elif open_now[slot].positive_pubrec is None:
                        open_now[slot].positive_pubrec = record
                elif payload["packet_kind"] == "PUBCOMP":
                    open_now.pop(slot).closed = record
        return found, notes


# ---------------------------------------------------------------- assertions


def _worst(statuses):
    for status in (FAIL, INCONCLUSIVE):
        if status in statuses:
            return status
    return PASS


def trace_complete(run):
    trace = run.trace
    if trace.unreadable:
        return Status(INCONCLUSIVE, list(trace.unreadable))
    if trace.incomplete:
        return Status(INCONCLUSIVE, list(trace.incomplete))
    if trace.scenario_id != run.scenario["id"]:
        return Status(INCONCLUSIVE, [f"the trace is a run of {trace.scenario_id}"])
    return Status(PASS)


def all_required_faults_hit(run):
    reasons = []
    hits = fault_hits(run)
    declared = {fault["id"]: int(fault["expected_hits"]) for fault in run.scenario["faults"]}
    for fault, expected in declared.items():
        if hits.get(fault, 0) != expected:
            reasons.append(f"{fault} was reached {hits.get(fault, 0)} times, {expected} expected")
    for fault in hits.keys() - declared.keys():
        reasons.append(f"{fault} was reached and is not declared")
    return Status(INCONCLUSIVE if reasons else PASS, reasons)


def fault_hits(run):
    hits = {}
    for record in run.trace.of_kind("FaultHit"):
        hits[record.payload["fault_id"]] = hits.get(record.payload["fault_id"], 0) + 1
    return hits


def one_admission_per_observed_inbound_exchange(run):
    exchanges, notes = run.exchanges()
    if notes:
        return Status(INCONCLUSIVE, notes)
    bound = [
        step
        for step in run.scenario["steps"]
        if step["kind"] == "ClientPublish" and step["qos"] == 2 and "bind_exchange_as" in step
    ]
    if len(exchanges) < len(bound):
        return Status(INCONCLUSIVE, ["the peers did not record every exchange the scenario binds"])
    clients = run.client_of_session()
    slots = run.slots()
    counts = {id(exchange): [] for exchange in exchanges}
    statuses, reasons = [], []
    for commit in run.trace.of_kind("StoreCommitStable"):
        if commit.payload["command_kind"] != "AdmitPublication":
            continue
        operation = _key(commit.payload["operation_id"])
        kind, session, packet_id = slots.get(operation, (None, None, None))
        if kind != "AdmitPublication" or session not in clients:
            statuses.append(INCONCLUSIVE)
            reasons.append(f"the admission {operation} cannot be attributed: no readable recipe")
            continue
        slot = (clients[session], packet_id)
        owner, unknown = None, False
        for exchange in exchanges:
            if (exchange.client, exchange.packet_id) != slot:
                continue
            opened = run.trace.before(exchange.opened, commit)
            ended = exchange.closed is not None and run.trace.before(exchange.closed, commit)
            if opened is None or ended is None:
                unknown = True
            elif opened and not ended:
                owner = exchange
        if owner is not None:
            counts[id(owner)].append(operation)
        elif unknown:
            statuses.append(INCONCLUSIVE)
            reasons.append(f"the trace does not order the admission {operation} with its exchange")
        else:
            statuses.append(FAIL)
            reasons.append(f"the admission {operation} belongs to no exchange {slot} had open")
    for exchange in exchanges:
        admissions = counts[id(exchange)]
        name = f"exchange {exchange.ordinal} of {exchange.client}, Packet Identifier {exchange.packet_id}"
        if len(admissions) > 1:
            statuses.append(FAIL)
            reasons.append(f"{name} has {len(admissions)} admissions: {admissions}")
        elif not admissions and exchange.positive_pubrec is not None:
            statuses.append(FAIL)
            reasons.append(f"{name} was acknowledged and has no admission")
    return Status(_worst(statuses), reasons)


def _obligations(snapshot):
    """DeliveryId -> what identifies the obligation, payload reference included."""
    payloads = {
        _key(row["publication_id"]): (
            _key(row["payload"]["payload_id"]),
            row["payload"]["length"],
            row["payload"]["sha256"],
        )
        for row in snapshot["publications"]
    }
    return {
        (_key(row["publication_id"]), row["index"]): (
            int(row["session"]),
            row["cause"],
            row["effective_qos"],
            payloads.get(_key(row["publication_id"])),
        )
        for row in snapshot["obligations"]
    }


def same_required_obligations_at_both_checkpoints(run):
    names = run.checkpoints()
    if len(names) < 2:
        return Status(OUT_OF_SCOPE, ["the scenario inspects fewer than two checkpoints"])
    unusable = [
        f"{name}: {run.missing.get(name, 'no snapshot')}"
        for name in names
        if name not in run.snapshots
    ]
    if unusable:
        return Status(INCONCLUSIVE, unusable)
    first = run.snapshots[names[0]][1]
    lot = _obligations(first)
    if not lot:
        return Status(FAIL, [f"{names[0]} holds no obligation"])
    reasons = []
    # What the fixture requires of each publication, computed here: the
    # exact-topic route and the lower of the two QoS.
    sessions = {client: session for session, client in run.client_of_session().items()}
    routes = {route["topic"].encode().hex(): route["targets"] for route in run.fixture["routes"]}
    for row in first["publications"]:
        publication = _key(row["publication_id"])
        required = sorted(
            (sessions.get(target["client"]), "LiveExact", min(row["qos"], target["qos"]))
            for target in routes.get(row["topic_hex"], [])
        )
        held = sorted(
            obligation[:3] for (owner, _), obligation in lot.items() if owner == publication
        )
        if held != required:
            reasons.append(
                f"{names[0]}: publication {publication} holds {held}, the routes require {required}"
            )
    for name in names[1:]:
        later = _obligations(run.snapshots[name][1])
        for delivery, obligation in lot.items():
            if delivery not in later:
                reasons.append(f"{name}: the obligation {delivery} is gone")
            elif later[delivery] != obligation:
                reasons.append(
                    f"{name}: the obligation {delivery} changed from {obligation} to {later[delivery]}"
                )
    return Status(FAIL if reasons else PASS, reasons)


def same_recipe_for_op_A(run):
    binding = next(
        (fault for fault in run.scenario["faults"] if fault.get("bind_operation_as") == "op_A"),
        None,
    )
    if binding is None:
        return Status(OUT_OF_SCOPE, ["the scenario binds no operation as op_A"])
    hits = [
        record
        for record in run.trace.of_kind("FaultHit")
        if record.payload["fault_id"] == binding["id"]
    ]
    if not hits or "operation_id" not in hits[0].payload:
        return Status(INCONCLUSIVE, [f"no hit of {binding['id']} names the operation it stopped"])
    operation = _key(hits[0].payload["operation_id"])
    unusable = [
        f"{name}: {run.missing.get(name, 'no snapshot')}"
        for name in run.checkpoints()
        if name not in run.snapshots
    ]
    seen = run.recipes().get(operation, set())
    if len(seen) > 1:
        return Status(FAIL, [f"the recipe of {operation} has {len(seen)} different contents"])
    if unusable:
        return Status(INCONCLUSIVE, unusable)
    commits = [
        record
        for record in run.trace.of_kind("StoreCommitStable")
        if _key(record.payload["operation_id"]) == operation
    ]
    reasons, observed = [], 0
    for name, (inspection, snapshot) in run.snapshots.items():
        present = any(_key(row["operation_id"]) == operation for row in snapshot["operations"])
        observed += present
        if not present and any(run.trace.before(commit, inspection) for commit in commits):
            reasons.append(f"{name}: the committed operation {operation} has no recipe")
    if reasons:
        return Status(FAIL, reasons)
    if observed == 0:
        return Status(INCONCLUSIVE, [f"no snapshot holds the recipe of {operation}"])
    return Status(PASS)


def no_positive_ack_before_confirmed_commit(run):
    """A positive PUBREC or PUBCOMP follows what the core itself learned.

    The justification is looked for in the records of the same incarnation
    of the core, before the frame: a result delivered to the core, or a
    recovered fact that is sufficient. That the store had committed is not
    one.
    """
    slots = run.slots()
    statuses, reasons = [], []
    for origin in sorted(
        {record.origin for record in run.trace.records if record.origin[0] == "broker-core"}
    ):
        learned = {"PUBREC": {}, "PUBCOMP": {}}
        unattributed = False
        for record in run.trace.records:
            if record.origin != origin:
                continue
            payload = record.payload
            if record.kind == "StoreResultDelivered" and payload["outcome"] == "Committed":
                known = slots.get(_key(payload["operation_id"]))
                if known is None:
                    unattributed = True
                elif known[0] == "AdmitPublication":
                    learned["PUBREC"][known[1:]] = record
                else:
                    learned["PUBCOMP"][known[1:]] = record
            elif record.kind == "RecoveryPageDelivered":
                for fact in payload["facts"]:
                    if fact["kind"] == "InboundExchange" and fact["phase"] == "AwaitPubrel":
                        slot = (int(fact["exchange"]["session"]), fact["packet_id"])
                        learned["PUBREC"][slot] = record
            elif record.kind == "FrameEligible" and payload["packet_kind"] in learned:
                if payload["reason"] != "Success":
                    continue
                kind = payload["packet_kind"]
                slot = (int(payload["connection"]["session"]), payload["packet_id"])
                if slot in learned[kind]:
                    if kind == "PUBCOMP":
                        # The exchange is over: its knowledge justifies no
                        # later acknowledgement of the same slot.
                        learned["PUBCOMP"].pop(slot)
                        learned["PUBREC"].pop(slot, None)
                elif unattributed:
                    statuses.append(INCONCLUSIVE)
                    reasons.append(
                        f"record {record.sequence}: a result the core received cannot be attributed"
                    )
                else:
                    statuses.append(FAIL)
                    reasons.append(
                        f"record {record.sequence}: {kind} of Packet Identifier {slot[1]} is "
                        "eligible before the core knew of a commit"
                    )
    return Status(_worst(statuses), reasons)


def no_pubrec_observed_before_crash(run):
    crashes = run.trace.of_kind("BrokerCrashed")
    if not any(step["kind"] == "CrashBroker" for step in run.scenario["steps"]):
        return Status(OUT_OF_SCOPE, ["the scenario has no crash"])
    if not crashes:
        return Status(INCONCLUSIVE, ["the trace records no crash"])
    bound = {
        (step["client"], step["packet_id"])
        for step in run.scenario["steps"]
        if step["kind"] == "ClientPublish" and "bind_exchange_as" in step and "packet_id" in step
    }
    statuses, reasons = [], []
    for record in run.trace.of_kind("PeerPacketObserved"):
        payload = record.payload
        if (
            payload["packet_kind"] != "PUBREC"
            or (payload["client"], payload["packet_id"]) not in bound
        ):
            continue
        order = run.trace.before(record, crashes[0])
        if order is None:
            statuses.append(INCONCLUSIVE)
            reasons.append(f"record {record.sequence}: not ordered with the crash")
        elif order:
            statuses.append(FAIL)
            reasons.append(f"record {record.sequence}: PUBREC observed before the crash")
    return Status(_worst(statuses), reasons)


def broker_inputs_have_provenance(run):
    """Every input the core consumed has its counterpart (ADR 0003)."""
    trace = run.trace
    statuses, reasons = [], []

    def blame(record, why):
        statuses.append(FAIL)
        reasons.append(f"record {record.sequence}: {why}")

    def ordered(cause, record, what):
        order = trace.before(cause, record)
        if order is None:
            statuses.append(INCONCLUSIVE)
            reasons.append(f"record {record.sequence}: not ordered with {what}")
        elif not order:
            blame(record, f"it precedes {what}")
        return bool(order)

    def side(component, incarnation, *kinds):
        return [
            record for record in trace.of_kind(*kinds) if record.origin == (component, incarnation)
        ]

    cores = sorted(
        {record.origin[1] for record in trace.records if record.origin[0] == "broker-core"}
    )
    for incarnation in cores:
        submitted = {
            _key(record.payload["effect_id"]): record
            for record in side("store-adapter", incarnation, "CommandSubmitted", "ResolveSubmitted")
        }
        answered = set()
        for record in side("broker-core", incarnation, "StoreResultDelivered"):
            effect = _key(record.payload["effect_id"])
            submission = submitted.get(effect)
            if submission is None:
                blame(record, f"the result answers the effect {effect}, which is not in flight")
            elif submission.payload["operation_id"] != record.payload["operation_id"]:
                blame(record, "the result names another operation than its submission")
            elif effect in answered:
                blame(record, f"the effect {effect} was already answered")
            else:
                ordered(submission, record, "its submission")
            answered.add(effect)

        armed = {
            record.payload["token"]: record
            for record in side("timer", incarnation, "DeadlineArmed")
        }
        fired = set()
        for record in side("broker-core", incarnation, "DeadlineDelivered"):
            token = record.payload["token"]
            if token not in armed:
                blame(record, f"the deadline token {token} was never armed")
            elif token in fired:
                blame(record, f"the deadline token {token} was already delivered")
            else:
                ordered(armed[token], record, "its arming")
            fired.add(token)

        pages = side("store-adapter", incarnation, "RecoveryPageRead")
        used = set()
        expected = 0
        for record in side("broker-core", incarnation, "RecoveryPageDelivered"):
            source = next(
                (page for page in pages if page.payload == record.payload and page.key not in used),
                None,
            )
            if source is None:
                blame(record, "the recovery page is not a page the store adapter read")
                continue
            used.add(source.key)
            ordered(source, record, "the read of its page")
            if int(record.payload["page"]) != expected:
                blame(record, f"page {record.payload['page']} delivered, page {expected} expected")
            expected += 1

        received = {
            record.key: record for record in side("transport", incarnation, "ClientBytesReceived")
        }
        for record in side("broker-core", incarnation, "PacketDelivered"):
            sources = [received[parent] for parent in record.parents if parent in received]
            if not any(
                source.payload["connection_generation"] == record.payload["connection_generation"]
                for source in sources
            ):
                blame(record, "the packet comes from no bytes the transport received")

        eligible = {
            _key(record.payload["effect_id"]): record
            for record in side("broker-core", incarnation, "FrameEligible")
        }
        for record in side("transport", incarnation, "FrameWriteProgress"):
            frame = eligible.get(_key(record.payload["effect_id"]))
            if frame is None:
                blame(record, "write progress of a frame the core never made eligible")
            else:
                ordered(frame, record, "the frame it writes")

    duplicating = {
        fault["id"] for fault in run.scenario["faults"] if fault["action"] == "duplicate_result"
    }
    by_key = {record.key: record for record in trace.records}
    for record in trace.of_kind("InputRefused"):
        causes = [by_key[key] for key in trace.ancestors(record) if key in by_key]
        if not any(
            cause.kind == "FaultHit" and cause.payload["fault_id"] in duplicating
            for cause in causes
        ):
            blame(record, f"an input was refused: {record.payload['reason']}")
    return Status(_worst(statuses), reasons)


ASSERTIONS = {
    "one_admission_per_observed_inbound_exchange": one_admission_per_observed_inbound_exchange,
    "same_required_obligations_at_both_checkpoints": same_required_obligations_at_both_checkpoints,
    "same_recipe_for_op_A": same_recipe_for_op_A,
    "no_positive_ack_before_confirmed_commit": no_positive_ack_before_confirmed_commit,
    "no_pubrec_observed_before_crash": no_pubrec_observed_before_crash,
    "broker_inputs_have_provenance": broker_inputs_have_provenance,
    "all_required_faults_hit": all_required_faults_hit,
    "trace_complete": trace_complete,
}


def judge(run: Run) -> dict:
    """The status of every assertion of the scenario, by name."""
    statuses = {}
    for name in run.scenario["assertions"]:
        decide = ASSERTIONS.get(name)
        if decide is None:
            statuses[name] = Status(INCONCLUSIVE, ["the oracle does not decide this assertion"])
        elif run.trace.unreadable:
            statuses[name] = Status(INCONCLUSIVE, list(run.trace.unreadable))
        elif run.trace.scenario_id not in (None, run.scenario["id"]):
            # A run is judged only against its own scenario.
            statuses[name] = Status(
                INCONCLUSIVE, [f"the trace is a run of {run.trace.scenario_id}"]
            )
        else:
            statuses[name] = decide(run)
    if run.trace.incomplete:
        # Nothing concluded from an incomplete trace is a PASS, and a FAIL
        # stands only where no missing record could undo it.
        for name, status in statuses.items():
            if status.status == PASS or (status.status == FAIL and name not in MONOTONE):
                statuses[name] = Status(INCONCLUSIVE, status.reasons + ["the trace is incomplete"])
    return statuses
