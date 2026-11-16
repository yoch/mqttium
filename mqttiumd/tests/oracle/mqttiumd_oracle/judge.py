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
from . import semantic
from .semantic import InventoryOrder
from .strict import Refused, parse
from .trace import BENCH, Trace
from .trace import lines_of
from .trace import read as read_trace

PASS, FAIL, INCONCLUSIVE, OUT_OF_SCOPE = "PASS", "FAIL", "INCONCLUSIVE", "OUT_OF_SCOPE"

# A violation of these can stay a violation whatever records are added to
# the trace, so it can stand even when the trace is incomplete. For the
# others a missing record could be the very one that justifies what was
# observed. Within these, a status says itself whether its reason is of
# that kind (``Status.stands``).
MONOTONE = {
    "scenario_reaches_its_end",
    "fenced_operation_never_executed",
    "one_admission_per_observed_inbound_exchange",
    "same_required_obligations_at_both_checkpoints",
    "same_recipe_for_op_A",
    "no_positive_ack_before_confirmed_commit",
}


# What only independent peers can decide: without bytes nobody observed
# what a client sent or received.
NEEDS_PEERS = {
    "one_admission_per_observed_inbound_exchange",
    "no_pubrec_observed_before_crash",
}


@dataclass
class Status:
    status: str
    reasons: list = field(default_factory=list)
    # Whether a FAIL rests only on what no record added to the trace could
    # undo. Read for the assertions of ``MONOTONE`` on an incomplete trace.
    stands: bool = True


# The components an identity can have, in the order they are compared. An
# identity is these named members, in whatever order a writer puts them.
_COMPONENTS = ("incarnation", "epoch", "owner", "sequence", "session", "generation")


def _key(identity: dict) -> tuple:
    """An identity object of decimal strings, as a tuple of integers."""
    unknown = identity.keys() - set(_COMPONENTS)
    if unknown:
        raise ValueError(f"an identity has the unknown components {sorted(unknown)}")
    return tuple(int(identity[name]) for name in _COMPONENTS if name in identity)


_PACKET_KINDS = {1: "CONNECT", 3: "PUBLISH", 6: "PUBREL"}
_OBSERVED_KINDS = {2: "CONNACK", 5: "PUBREC", 7: "PUBCOMP"}


def _observed(data: bytes):
    """``(kind, Packet Identifier, reason code)`` of one whole packet a client receives.

    Decoded from the MQTT 5 fixed and variable headers; a PUBREC or PUBCOMP
    of remaining length 2 omits its reason, which is then 0 (section
    3.5.2.1). ``None`` when the bytes are not one such packet.
    """
    if len(data) < 2:
        return None
    kind = _OBSERVED_KINDS.get(data[0] >> 4)
    length = data[1]
    if kind is None or length & 0x80 or len(data) != 2 + length:
        return None
    if kind == "CONNACK":
        return (kind, None, data[3]) if length >= 2 else None
    if length < 2:
        return None
    packet_id = int.from_bytes(data[2:4], "big")
    return kind, packet_id, data[4] if length > 2 else 0


def _packet(data: bytes):
    """``(kind, Packet Identifier)`` of bytes that are exactly one packet.

    Read from the MQTT fixed header and, for the identifier, the variable
    header. ``None`` when the bytes are not one whole packet of a kind the
    core is delivered.
    """
    if len(data) < 2:
        return None
    kind = _PACKET_KINDS.get(data[0] >> 4)
    length, shift, at = 0, 0, 1
    while True:
        if at >= len(data) or shift > 21:
            return None
        byte = data[at]
        length |= (byte & 0x7F) << shift
        shift += 7
        at += 1
        if not byte & 0x80:
            break
    if kind is None or len(data) != at + length:
        return None
    if kind == "CONNECT":
        return kind, None
    if kind == "PUBLISH":
        if (data[0] >> 1) & 3 == 0:
            return kind, None
        if at + 2 > len(data):
            return None
        at += 2 + int.from_bytes(data[at : at + 2], "big")
    if at + 2 > len(data):
        return None
    return kind, int.from_bytes(data[at : at + 2], "big")


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
            trace = read_trace(contracts, lines_of(stream), max_bytes=budget)
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
            elif snapshot["run_id"] != self.trace.run_id:
                self.missing[checkpoint] = (
                    f"{cited['path']} is a snapshot of the run {snapshot['run_id']}"
                )
            elif snapshot["backend"] != self.trace.backend:
                self.missing[checkpoint] = (
                    f"{cited['path']} was read from {snapshot['backend']}, "
                    f"the run declares {self.trace.backend}"
                )
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
                if not isinstance(recipe, (recipes.Admit, recipes.Release)):
                    # New product recipes are readable descriptive evidence;
                    # the fixture assertions still describe incoming exchanges.
                    # results.unmet excludes their semantics from qualification.
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
            elif record.kind == "PeerPacketObserved":
                # What the peer says it decoded must be what its bytes hold.
                decoded = _observed(bytes.fromhex(payload["bytes_hex"]))
                stated = (payload["packet_kind"], payload.get("packet_id"), payload["reason_code"])
                if decoded != stated:
                    notes.append(f"record {record.sequence}: the bytes are not the packet stated")
                    continue
                if slot not in open_now:
                    continue
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
    declared = {fault["id"]: fault for fault in run.scenario["faults"]}
    for name, fault in declared.items():
        count = hits.get(name, 0)
        if not semantic.reached(fault, count):
            kind, expected = semantic.expectation(fault)
            wanted = f"at least {expected}" if kind == "at_least" else expected
            reasons.append(f"{name} was reached {count} times, {wanted} expected")
    for fault in hits.keys() - declared.keys():
        reasons.append(f"{fault} was reached and is not declared")
    return Status(INCONCLUSIVE if reasons else PASS, reasons)


def fault_hits(run):
    hits = {}
    for record in run.trace.of_kind("FaultHit"):
        hits[record.payload["fault_id"]] = hits.get(record.payload["fault_id"], 0) + 1
    return hits


def _recovery_after_kill(run, page, kill):
    """Whether the page belongs to a startup observed after this kill.

    A survivor's BrokerRecovered identifies a process startup by run_epoch.
    The child channels supply the matching incarnation; its page carries
    that same startup rank. This is lifecycle evidence, not a product cause
    or the collector's total arrival order. A simulated trace retains the
    total order its backend contract supplies.
    """
    trace = run.trace
    if trace.backend == "sim-store":
        return trace.before(kill, page)
    epoch = int(page.payload["run_epoch"])
    frontier = [parent[1] for parent in kill.parents if parent[0] in CHILD]
    if not frontier or epoch <= max(frontier):
        return False
    if page.origin[1] != epoch:
        return None
    starts = [
        record for record in trace.of_kind("BrokerRecovered")
        if int(record.payload["run_epoch"]) == epoch
    ]
    if len(starts) != 1:
        return None
    startup = starts[0]
    core = {parent[1] for parent in startup.parents if parent[0] == "broker-core"}
    if core != {epoch} or trace.before(kill, startup) is not True:
        return None
    if trace.before(page, kill) is True:
        return False
    return True


def _first_recovery_after_kill(run, kill):
    """Select lifecycle evidence before considering whether any page exists.

    The first restarted child may have failed before producing an inventory.
    A later child's pages cannot replace that observation. The surviving
    supervisor's local order orders its startup/kill records; no collector
    order is added to the product relation.
    """
    trace = run.trace
    starts = [
        startup for startup in trace.of_kind("BrokerRecovered")
        if trace.before(kill, startup) is True
    ]
    first = [
        startup for startup in starts
        if all(other.key == startup.key or trace.before(startup, other) is True
               for other in starts)
    ]
    if len(first) != 1:
        return None
    startup = first[0]
    if any(
        other.key != kill.key
        and trace.before(kill, other) is True
        and trace.before(other, startup) is True
        for other in trace.of_kind("BrokerKilled")
    ):
        # There was another child before this startup, with no matching
        # startup observation. It cannot silently be skipped either.
        return None
    if trace.backend == "sqlite":
        epoch = int(startup.payload["run_epoch"])
        frontier = [parent[1] for parent in kill.parents if parent[0] in CHILD]
        if not frontier or epoch <= max(frontier):
            return None
        same_rank = [record for record in trace.of_kind("BrokerRecovered")
                     if int(record.payload["run_epoch"]) == epoch]
        core = {parent[1] for parent in startup.parents if parent[0] == "broker-core"}
        if len(same_rank) != 1 or core != {epoch}:
            return None
    return startup


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
    placeable = set()
    statuses, reasons = [], []
    # Two admissions of one exchange stay two whatever record is added. An
    # admission that is missing, or an exchange that is, may be the very
    # record an incomplete trace lacks.
    stands = False

    def attribute(operation, commit, *, peer_source=False):
        """Place a durable admission by its commit or its observed PUBLISH source."""
        kind, session, packet_id = slots.get(operation, (None, None, None))
        if kind != "AdmitPublication" or session not in clients:
            statuses.append(INCONCLUSIVE)
            reasons.append(f"the admission {operation} cannot be attributed: no readable recipe")
            return
        slot = (clients[session], packet_id)
        owner, unknown = None, []
        for exchange in exchanges:
            if (exchange.client, exchange.packet_id) != slot:
                continue
            opened = (
                True if peer_source and exchange.opened.key == commit.key
                else run.trace.before(exchange.opened, commit)
            )
            ended = exchange.closed is not None and run.trace.before(exchange.closed, commit)
            if opened is None or ended is None:
                unknown.append(exchange)
            elif opened and not ended:
                owner = exchange
        if owner is not None:
            counts[id(owner)].append(operation)
        elif unknown:
            # The admission may belong to any of these exchanges.
            placeable.update(id(exchange) for exchange in unknown)
            statuses.append(INCONCLUSIVE)
            reasons.append(f"the trace does not order the admission {operation} with its exchange")
        else:
            statuses.append(FAIL)
            reasons.append(f"the admission {operation} belongs to no exchange {slot} had open")

    stable = [
        commit
        for commit in run.trace.of_kind("StoreCommitStable")
        if commit.payload["command_kind"] == "AdmitPublication"
    ]
    for commit in stable:
        attribute(_key(commit.payload["operation_id"]), commit)
    # A commit can be durable without its StoreCommitStable when the store's
    # evidence dies with the child: a real kill inside the transaction after
    # its commit frame (K2b of the plan of lot L7). The recovery that follows
    # the kill then shows the operation Committed; that is its admission,
    # once, unless a StoreCommitStable of it came before. In-process the
    # evidence of the store is never killed: a commit without it did not
    # happen, whatever a recovery says.
    kills = run.trace.of_kind("BrokerKilled") if run.trace.broker == "child-process" else []
    reads = run.trace.of_kind("RecoveryPageRead")
    decisions = {}
    for _, snapshot in run.snapshots.values():
        for row in snapshot["operations"]:
            decisions.setdefault(_key(row["operation_id"]), set()).add(row["decision"]["kind"])
    found = set()
    for page in run.trace.of_kind("RecoveryPageDelivered"):
        follows = [_recovery_after_kill(run, page, kill) for kill in kills]
        if True not in follows:
            if None in follows:
                for fact in page.payload["facts"]:
                    if fact["kind"] != "Operation" or fact["knowledge"] != "Committed":
                        continue
                    operation = _key(fact["operation_id"])
                    kind, session, packet_id = slots.get(operation, (None, None, None))
                    if kind != "AdmitPublication":
                        continue
                    statuses.append(INCONCLUSIVE)
                    reasons.append(f"the recovered admission {operation} has no unambiguous startup")
                    placeable.update(
                        id(exchange) for exchange in exchanges
                        if (exchange.client, exchange.packet_id) == (clients.get(session), packet_id)
                    )
            continue
        for fact in page.payload["facts"]:
            if fact["kind"] != "Operation" or fact["knowledge"] != "Committed":
                continue
            operation = _key(fact["operation_id"])
            if operation in found or slots.get(operation, (None,))[0] != "AdmitPublication":
                continue
            if any(
                _key(commit.payload["operation_id"]) == operation
                and run.trace.before(commit, page) is not False
                for commit in stable
            ):
                continue
            # A core-delivered page is an observation of the store only when
            # its read exists in the same incarnation and precedes delivery.
            # Missing evidence can still belong to the acknowledged exchange;
            # it makes that conclusion unknown rather than inventing absence.
            source = next(
                (
                    read for read in reads
                    if read.origin[1] == page.origin[1]
                    and read.payload == page.payload
                    and run.trace.before(read, page) is True
                ),
                None,
            )
            if source is None:
                statuses.append(INCONCLUSIVE)
                reasons.append(f"the recovered admission {operation} has no matching page read")
                _, session, packet_id = slots[operation]
                placeable.update(
                    id(exchange) for exchange in exchanges
                    if (exchange.client, exchange.packet_id) == (clients.get(session), packet_id)
                )
                continue
            if decisions.get(operation, set()) - {"Committed"}:
                statuses.append(FAIL)
                reasons.append(f"the recovered admission {operation} contradicts its durable decision")
                stands = True
                continue
            found.add(operation)
            if run.trace.backend == "sqlite":
                # Recovery establishes the durable outcome, while the
                # acknowledged submission and its peer PUBLISH establish
                # which client exchange issued this immutable recipe. The
                # recovery page need not be causally after that peer record.
                submitted = [
                    command for command in run.trace.of_kind("CommandSubmitted")
                    if _key(command.payload["operation_id"]) == operation
                    and command.payload["command_kind"] == "AdmitPublication"
                    and any(
                        order is True and run.trace.before(command, kill) is True
                        for kill, order in zip(kills, follows)
                    )
                ]
                source = None
                if submitted:
                    command = min(submitted, key=lambda record: (record.origin[1], record.origin_sequence))
                    ancestors = run.trace.ancestors(command)
                    _, session, packet_id = slots[operation]
                    sent = [
                        record for record in run.trace.of_kind("PeerPacketSent")
                        if record.key in ancestors
                        and record.payload["packet_kind"] == "PUBLISH"
                        and (record.payload["client"], record.payload["packet_id"])
                        == (clients.get(session), packet_id)
                    ]
                    latest = [
                        record for record in sent
                        if not any(run.trace.before(record, other) is True for other in sent)
                    ]
                    if len(latest) == 1:
                        source = latest[0]
                if source is None:
                    statuses.append(INCONCLUSIVE)
                    reasons.append(f"the recovered admission {operation} has no observed PUBLISH source")
                    _, session, packet_id = slots[operation]
                    placeable.update(
                        id(exchange) for exchange in exchanges
                        if (exchange.client, exchange.packet_id) == (clients.get(session), packet_id)
                    )
                    continue
                attribute(operation, source, peer_source=True)
            else:
                attribute(operation, page)
    for exchange in exchanges:
        admissions = counts[id(exchange)]
        name = f"exchange {exchange.ordinal} of {exchange.client}, Packet Identifier {exchange.packet_id}"
        if len(admissions) > 1:
            stands = True
            statuses.append(FAIL)
            reasons.append(f"{name} has {len(admissions)} admissions: {admissions}")
        elif not admissions and exchange.positive_pubrec is not None:
            if id(exchange) in placeable:
                # An admission the trace cannot place may be this one.
                statuses.append(INCONCLUSIVE)
                reasons.append(f"{name} was acknowledged; its admission cannot be placed")
            else:
                statuses.append(FAIL)
                reasons.append(f"{name} was acknowledged and has no admission")
    return Status(_worst(statuses), reasons, stands)


def _obligations(snapshot):
    """DeliveryId -> what identifies the obligation, payload reference included.

    The payload is the object the publication refers to, with its length and
    digest when the store still holds it.
    """
    stored = {
        _key(row["payload_id"]): (row["length"], row["sha256"]) for row in snapshot["payloads"]
    }
    payloads = {
        _key(row["publication_id"]): (
            _key(row["payload_id"]),
            stored.get(_key(row["payload_id"])),
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


def _published(data: bytes):
    """``(topic, payload)`` of the bytes of one MQTT 5 PUBLISH, or ``None``."""
    try:
        length, shift, at = 0, 0, 1
        while True:
            byte = data[at]
            length |= (byte & 0x7F) << shift
            shift, at = shift + 7, at + 1
            if not byte & 0x80:
                break
        end = at + length
        if data[0] >> 4 != 3 or end != len(data):
            return None
        topic_length = int.from_bytes(data[at : at + 2], "big")
        topic = data[at + 2 : at + 2 + topic_length]
        at += 2 + topic_length
        if (data[0] >> 1) & 3:
            at += 2
        properties, shift = 0, 0
        while True:
            byte = data[at]
            properties |= (byte & 0x7F) << shift
            shift, at = shift + 7, at + 1
            if not byte & 0x80:
                break
        at += properties
        return (topic, data[at:end]) if at <= end else None
    except IndexError:
        return None


def _broken_references(name, snapshot):
    """What a snapshot names and does not hold: the store lost it, or never had it."""
    publications = {_key(row["publication_id"]): row for row in snapshot["publications"]}
    payloads = {_key(row["payload_id"]) for row in snapshot["payloads"]}
    reasons = []
    for row in snapshot["obligations"]:
        if _key(row["publication_id"]) not in publications:
            reasons.append(
                f"{name}: the obligation {_key(row['publication_id'])}/{row['index']} "
                "names a publication the store does not hold"
            )
    for publication, row in publications.items():
        if _key(row["payload_id"]) not in payloads:
            reasons.append(
                f"{name}: the publication {publication} names a payload the store does not hold"
            )
    return reasons


def _acknowledged_publications(run, names):
    """Reasons a checkpoint lacks what a client was acknowledged for, and where order is unknown.

    An exchange whose positive PUBREC a peer observed before an inspection
    was committed: the snapshot of that inspection holds a publication of
    the topic the client sent, whose payload has the length and digest of
    the bytes the client sent. The expectation comes from what the client
    did, not from what survives in the snapshot.
    """
    exchanges, _ = run.exchanges()
    missing, unknown = [], []
    for exchange in exchanges:
        if exchange.positive_pubrec is None:
            continue
        sent = _published(bytes.fromhex(exchange.opened.payload["bytes_hex"]))
        if sent is None:
            unknown.append(f"record {exchange.opened.sequence}: the PUBLISH cannot be read")
            continue
        topic, payload = sent
        digest = hashlib.sha256(payload).hexdigest()
        for name in names:
            inspection, snapshot = run.snapshots[name]
            order = run.trace.before(exchange.positive_pubrec, inspection)
            if order is None:
                unknown.append(
                    f"{name}: not ordered with the PUBREC of record {exchange.positive_pubrec.sequence}"
                )
                continue
            if not order:
                continue
            payloads = {
                _key(row["payload_id"]): (row["length"], row["sha256"])
                for row in snapshot["payloads"]
            }
            held = [
                payloads.get(_key(row["payload_id"]))
                for row in snapshot["publications"]
                if row["topic_hex"] == topic.hex()
            ]
            if (str(len(payload)), digest) not in held:
                missing.append(
                    f"{name}: no publication holds the payload the client published on "
                    f"{topic!r} and was acknowledged for (record {exchange.positive_pubrec.sequence})"
                )
    return missing, unknown


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
    # Every checkpoint hangs together: an obligation names a publication the
    # store holds, a publication a payload it holds. A missing reference is
    # never a value that compares equal to another missing one.
    for name in names:
        reasons += _broken_references(name, run.snapshots[name][1])
    # What a client was acknowledged for is held, whatever else survives.
    acknowledged, unordered = _acknowledged_publications(run, names)
    reasons += acknowledged
    # What the fixture requires of each publication, computed here: the
    # exact-topic route and the lower of the two QoS.
    sessions = {client: session for session, client in run.client_of_session().items()}
    routes = {route["topic"].encode().hex(): route["targets"] for route in run.fixture["routes"]}
    for row in first["publications"]:
        publication = _key(row["publication_id"])
        targets = routes.get(row["topic_hex"], [])
        lost = sorted(target["client"] for target in targets if target["client"] not in sessions)
        if lost:
            # Nothing can be owed to a session the store does not hold.
            reasons.append(
                f"{names[0]}: publication {publication} is routed to {lost}, "
                "and the store holds no session for them"
            )
            continue
        required = sorted(
            (sessions[target["client"]], "LiveExact", min(row["qos"], target["qos"]))
            for target in targets
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
        # A publication of the first checkpoint owes what it owed there and
        # nothing more. What a publication admitted since owes is the
        # matter of its own admission, not of this comparison.
        known = {publication for publication, _ in lot}
        known.update(_key(row["publication_id"]) for row in first["publications"])
        for delivery in sorted(later.keys() - lot.keys()):
            if delivery[0] in known:
                reasons.append(f"{name}: the obligation {delivery} appeared")
    if reasons:
        return Status(FAIL, reasons)
    if unordered:
        return Status(INCONCLUSIVE, unordered)
    return Status(PASS)


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
    reasons, unknown, observed = [], [], 0
    for name, (inspection, snapshot) in run.snapshots.items():
        present = any(_key(row["operation_id"]) == operation for row in snapshot["operations"])
        observed += present
        if present:
            continue
        orders = [run.trace.before(commit, inspection) for commit in commits]
        if any(orders):
            reasons.append(f"{name}: the committed operation {operation} has no recipe")
        elif None in orders:
            # The trace does not say whether the commit precedes this
            # inspection: the absence of the recipe proves nothing either way.
            unknown.append(f"{name}: the trace does not order the commit of {operation} with it")
    if reasons:
        return Status(FAIL, reasons)
    if unknown:
        return Status(INCONCLUSIVE, unknown)
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
    statuses, reasons, established = [], [], False
    reads = run.trace.of_kind("RecoveryPageRead")
    for origin in sorted(
        {record.origin for record in run.trace.records if record.origin[0] == "broker-core"}
    ):
        learned = {"PUBREC": {}, "PUBCOMP": {}}
        unattributed = False
        pending, next_page, inventory_epoch = {}, 0, None
        inventory, inventory_valid = InventoryOrder(), True
        sources = set()
        next_record, prefix_complete = 0, True
        for record in run.trace.records:
            if record.origin != origin:
                continue
            if record.origin_sequence != next_record:
                prefix_complete = False
            next_record = record.origin_sequence + 1
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
                # A partial recovery cannot make the owner ready. Facts
                # become usable only when its contiguous inventory closes,
                # and only if the adapter's read corroborates each page.
                number = int(payload["page"])
                if number == 0:
                    pending, next_page = {}, 0
                    inventory_epoch = payload["run_epoch"]
                    inventory, inventory_valid = InventoryOrder(), True
                    sources = set()
                inventory_valid &= number == next_page and payload["run_epoch"] == inventory_epoch
                next_page += 1
                source = next((read for read in reads
                               if read.origin[1] == origin[1]
                               and read.key not in sources and read.payload == payload
                               and run.trace.before(read, record) is True), None)
                if source is None:
                    unattributed = True
                    inventory_valid = False
                else:
                    sources.add(source.key)
                for fact in payload["facts"]:
                    inventory_valid &= inventory.admit(fact) is None
                    if fact["kind"] == "InboundExchange" and fact["phase"] == "AwaitPubrel":
                        slot = (int(fact["exchange"]["session"]), fact["packet_id"])
                        pending[slot] = record
                if payload["last"] and inventory_valid:
                    learned["PUBREC"].update(pending)
                    pending = {}
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
                    established |= prefix_complete
                    reasons.append(
                        f"record {record.sequence}: {kind} of Packet Identifier {slot[1]} is "
                        "eligible before the core knew of a commit"
                    )
    return Status(_worst(statuses), reasons, stands=established)


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

    by_key = {record.key: record for record in trace.records}
    duplicating = {
        fault["id"] for fault in run.scenario["faults"] if fault["action"] == "duplicate_result"
    }
    cores = sorted(
        {record.origin[1] for record in trace.records if record.origin[0] == "broker-core"}
    )
    for incarnation in cores:
        submitted = {
            _key(record.payload["effect_id"]): record
            for record in side("store-adapter", incarnation, "CommandSubmitted", "ResolveSubmitted")
        }
        # Each submission comes from an effect the core emitted: its parent
        # is the record of the core, same incarnation, that consumed the
        # input the effect answers.
        for record in submitted.values():
            if not any(
                parent in by_key and by_key[parent].origin == ("broker-core", incarnation)
                for parent in record.parents
            ):
                blame(record, "the adapter submitted what no record of the core caused")
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

        # K1: what the core consumed is what the adapter handed over, and
        # that is what the store produced, on content and identities. A
        # delay or a hold changes nothing of it; only a declared
        # duplicating injection hands one result over twice.
        produced = trace.of_kind("StoreResultReady")
        for record in side("broker-core", incarnation, "StoreResultDelivered"):
            if not any(
                by_key[parent].kind == "StoreResultHandedOver"
                and by_key[parent].payload == record.payload
                for parent in record.parents
                if parent in by_key
            ):
                blame(record, "the core consumed a result the store adapter did not hand over")
        handed_from = {}
        for record in side("store-adapter", incarnation, "StoreResultHandedOver"):
            causes = trace.causes(record)
            sources = [
                ready
                for ready in produced
                if ready.key in causes
                and ready.payload["effect_id"] == record.payload["effect_id"]
                and ready.payload["operation_id"] == record.payload["operation_id"]
            ]
            if not sources:
                blame(record, "the adapter handed over a result the store did not produce")
                continue
            source = max(sources, key=lambda ready: ready.sequence)
            if source.payload != record.payload:
                blame(
                    record,
                    f"the adapter handed over {record.payload['outcome']}; the store produced "
                    f"{source.payload['outcome']} (record {source.sequence})",
                )
            handed_from.setdefault(source.key, []).append(record)
        for source, handovers in handed_from.items():
            duplicated = any(
                hit.payload["fault_id"] in duplicating and source in trace.causes(hit) | {hit.key}
                for hit in trace.of_kind("FaultHit")
            ) or any(
                hit.payload["fault_id"] in duplicating and hit.key in trace.causes(handover)
                for hit in trace.of_kind("FaultHit")
                for handover in handovers
            )
            if len(handovers) > 1 and not duplicated:
                blame(handovers[1], "one result of the store is handed over twice")

        # The reply to an opening answers the command the adapter submitted,
        # and a committed one names an epoch the store opened for it.
        opened = {
            (record.payload["incarnation"], record.payload["epoch"]): record.payload["attempt"]
            for record in trace.of_kind("StoreEpochOpened")
        }
        for record in side("broker-core", incarnation, "OpeningReplyDelivered"):
            command = {key: record.payload[key] for key in ("attempt", "incarnation", "after")}
            if not any(
                by_key[parent].kind == "OpeningSubmitted" and by_key[parent].payload == command
                for parent in record.parents
                if parent in by_key
            ):
                blame(record, "the opening reply answers no command the adapter submitted")
            if record.payload["outcome"] == "Committed":
                where = (record.payload["incarnation"], record.payload["epoch"])
                if opened.get(where) != record.payload["attempt"]:
                    blame(record, f"the store opened no epoch {where[1]} for this attempt")

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
        # The pages the core received are one inventory (record 0014), in
        # the order it received them: the order holds from page to page,
        # and a route belongs to the table that precedes it. How often and
        # in what order the adapter read them is not what the core was given.
        # A route is listed once, and when the inventory closes every
        # route targets a session it recovers (the rules of the core).
        inventory, table = InventoryOrder(), False
        routes, sessions = {}, set()
        for page in side("broker-core", incarnation, "RecoveryPageDelivered"):
            for index, fact in enumerate(page.payload["facts"]):
                refusal = inventory.admit(fact)
                if refusal is not None:
                    blame(page, f"fact {index} is out of place in the inventory: {refusal[2]}")
                elif fact["kind"] == "RouteTable":
                    table = True
                elif fact["kind"] == "Route" and not table:
                    blame(page, f"fact {index} is a route and no route table precedes it")
                elif fact["kind"] == "Route":
                    route = (fact["topic_hex"], int(fact["target"]))
                    if route in routes:
                        blame(page, f"fact {index} repeats the route of {routes[route]}")
                    routes.setdefault(route, f"record {page.sequence}, fact {index}")
                elif fact["kind"] == "Session":
                    sessions.add(int(fact["session"]))
            if page.payload["last"]:
                for (_, target), where in routes.items():
                    if target not in sessions:
                        blame(
                            page, f"the route of {where} targets {target}, which is not recovered"
                        )
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

        if trace.perimeter != "full":
            # Without bytes there is no transport: the packets the core
            # consumed are the validated packets of the scenario.
            continue
        received = {
            record.key: record for record in side("transport", incarnation, "ClientBytesReceived")
        }
        for record in received.values():
            if not any(
                cause.kind == "PeerPacketSent"
                and cause.payload["bytes_hex"] == record.payload["bytes_hex"]
                for cause in map(by_key.get, record.parents)
                if cause is not None
            ):
                blame(record, "the transport received bytes that no peer it names sent")
        for record in side("broker-core", incarnation, "PacketDelivered"):
            sources = [
                received[parent]
                for parent in record.parents
                if parent in received
                and received[parent].payload["connection_generation"]
                == record.payload["connection_generation"]
            ]
            delivered = (record.payload["packet_kind"], record.payload.get("packet_id"))
            if not sources:
                blame(record, "the packet comes from no bytes the transport received")
            elif not any(
                _packet(bytes.fromhex(source.payload["bytes_hex"])) == delivered
                for source in sources
            ):
                blame(record, f"the bytes received are not the packet delivered, {delivered}")

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

    for record in trace.of_kind("InputRefused"):
        causes = [by_key[key] for key in trace.causes(record) if key in by_key]
        if not any(
            cause.kind == "FaultHit" and cause.payload["fault_id"] in duplicating
            for cause in causes
        ):
            blame(record, f"an input was refused: {record.payload['reason']}")
    return Status(_worst(statuses), reasons)


def _closures(run):
    """(inspection, OperationIds) per snapshot: what it shows closed, fenced or refused by receipt."""
    found = []
    for inspection, snapshot in run.snapshots.values():
        closed = {_key(row["operation_id"]) for row in snapshot["fences"]}
        closed.update(
            _key(row["operation_id"])
            for row in snapshot["operations"]
            if row["decision"]["kind"] == "RejectedCertain"
        )
        found.append((inspection, closed))
    return found


def fenced_operation_never_executed(run):
    """A fenced or refused identity is never committed (DEM-010, V3-006)."""
    statuses, reasons = [], []
    commits = {}
    for record in run.trace.of_kind("StoreCommitStable"):
        commits.setdefault(_key(record.payload["operation_id"]), []).append(record)
    for fence in run.trace.of_kind("StoreFenceStable"):
        operation = _key(fence.payload["operation_id"])
        # One identity is fenced or committed, never both: whichever came
        # first, the second is a violation, and the order is not needed.
        for commit in commits.get(operation, []):
            order = run.trace.before(fence, commit)
            when = {True: "after", False: "before", None: "unordered with"}[order]
            statuses.append(FAIL)
            reasons.append(
                f"record {commit.sequence}: {operation} is committed {when} its fence, "
                f"record {fence.sequence}"
            )
    for name, (_, snapshot) in run.snapshots.items():
        closed = {_key(row["operation_id"]) for row in snapshot["fences"]}
        for row in snapshot["operations"]:
            if row["decision"]["kind"] == "Committed" and _key(row["operation_id"]) in closed:
                statuses.append(FAIL)
                reasons.append(f"{name}: {_key(row['operation_id'])} is both fenced and committed")
    unusable = [name for name in run.checkpoints() if name not in run.snapshots]
    if unusable and FAIL not in statuses:
        return Status(
            INCONCLUSIVE, [f"{name}: {run.missing.get(name, 'no snapshot')}" for name in unusable]
        )
    return Status(_worst(statuses), reasons)


def certain_rejection_has_a_closure(run):
    """A certain refusal rests on a fence or a receipt, never on a read absence (DEM-MU-01)."""
    closed = _closures(run)
    unusable = [name for name in run.checkpoints() if name not in run.snapshots]
    statuses, reasons = [], []
    fences = run.trace.of_kind("StoreFenceStable")
    for record in run.trace.of_kind("StoreResultReady", "StoreResultDelivered"):
        if record.payload["outcome"] != "RejectedCertain":
            continue
        operation = _key(record.payload["operation_id"])
        # A closure justifies the refusal only if it was established before
        # it: a fence recorded earlier, or a snapshot inspected earlier that
        # holds it. One that appears afterwards is what DEM-MU-01 catches.
        fenced_before = [
            run.trace.before(fence, record)
            for fence in fences
            if _key(fence.payload["operation_id"]) == operation
        ] + [
            run.trace.before(inspection, record)
            for inspection, operations in closed
            if operation in operations
        ]
        if any(fenced_before):
            continue
        if None in fenced_before or unusable:
            statuses.append(INCONCLUSIVE)
            reasons.append(
                f"record {record.sequence}: the closure of {operation} cannot be established"
            )
        else:
            statuses.append(FAIL)
            reasons.append(
                f"record {record.sequence}: {operation} is refused with certainty and nothing closes it"
            )
    return Status(_worst(statuses), reasons)


def refusal_carries_the_reason_of_its_rejection(run):
    """A refusing PUBREC says why the admission was refused (ARC-011).

    The rejection a frame answers is the latest certain rejection among its
    causes, delivered to the same incarnation of the core. Its reason gives,
    through the transition catalogue, the reason the frame must carry.
    """
    table = run.contracts.catalogs["refusal_reasons"]
    if table is None:
        return Status(INCONCLUSIVE, ["no transition catalogue states the reasons of a refusal"])
    by_key = {record.key: record for record in run.trace.records}
    statuses, reasons = [], []
    for frame in run.trace.of_kind("FrameEligible"):
        payload = frame.payload
        if payload["packet_kind"] != "PUBREC" or payload["reason"] == "Success":
            continue
        causes = [
            by_key[key]
            for key in run.trace.causes(frame)
            if key in by_key
            and by_key[key].kind == "StoreResultDelivered"
            and by_key[key].origin == frame.origin
            and by_key[key].payload["outcome"] == "RejectedCertain"
        ]
        if not causes:
            statuses.append(FAIL)
            reasons.append(f"record {frame.sequence}: a refusing PUBREC answers no rejection")
            continue
        rejection = max(causes, key=lambda record: record.origin_sequence)
        owed = table[rejection.payload["reject_reason"]]
        if payload["reason"] != owed:
            statuses.append(FAIL)
            reasons.append(
                f"record {frame.sequence}: the PUBREC carries {payload['reason']}; "
                f"a rejection for {rejection.payload['reject_reason']} owes {owed}"
            )
    return Status(_worst(statuses), reasons)


def scenario_reaches_its_end(run):
    """The run reached the last step of its scenario (point d of the second wave).

    A DriveUntil step asserts that the awaited event comes with no
    stimulus beyond what the scenario has already injected. The bench
    says why it stopped earlier. Nothing eligible while the wait is unmet
    refutes that assertion for this run: a FAIL, which no missing record
    undoes, and which does not say the product is stuck for ever. A
    budget of steps that ran out establishes nothing. What still waited
    at the stop is reported for diagnosis; the status depends on the
    cause alone.
    """

    def waiting(payload):
        held = payload["holding_faults"]
        faults = f"faults holding an item: {', '.join(held)}" if held else "no fault holds an item"
        return f"{payload['armed_deadlines']} deadlines armed and not fired, {faults}"

    blocks = run.trace.of_kind("StepBlocked")
    stuck = [record for record in blocks if record.payload["cause"] == "quiescent"]
    if stuck:
        return Status(
            FAIL,
            [
                f"record {record.sequence}: nothing was eligible and the wait of step "
                f"{record.payload['step']} ({record.payload['step_kind']}) was unmet, which "
                "refutes for this run that the awaited event comes with no further stimulus; "
                f"{waiting(record.payload)}"
                for record in stuck
            ],
        )
    if blocks:
        return Status(
            INCONCLUSIVE,
            [
                f"record {record.sequence}: the budget of steps ran out; {waiting(record.payload)}"
                for record in blocks
            ],
        )
    return Status(PASS)


def terminal_knowledge_never_regresses(run):
    """Knowledge of an operation never goes back (record 0001, step 6; DEM-MU-05).

    Once the core consumed a terminal result of an operation, a later
    unknown result of it changes nothing: no record of the product names
    it as its cause, and the operation is never resolved again. A record
    of the bench that names it says the bench had read it: an order, no
    reaction.
    """
    trace = run.trace
    # A reaction is a record of the product that names the answer among its
    # parents; the bench orders its records after what it read, and that
    # is no reaction.
    followers = {}
    for record in trace.records:
        if record.origin[0] in BENCH:
            continue
        for parent in record.parents:
            followers.setdefault(parent, []).append(record)
    reasons = []
    for origin in sorted(
        {record.origin for record in trace.records if record.origin[0] == "broker-core"}
    ):
        terminal = {}
        for record in trace.records:
            if record.origin != origin or record.kind != "StoreResultDelivered":
                continue
            operation = _key(record.payload["operation_id"])
            if record.payload["outcome"] in ("Committed", "RejectedCertain"):
                terminal.setdefault(operation, record)
            elif record.payload["outcome"] == "PendingOrUnknown" and operation in terminal:
                for follower in followers.get(record.key, []):
                    reasons.append(
                        f"record {follower.sequence} ({follower.kind}) follows from the unknown "
                        f"answer of record {record.sequence}, after the terminal one of record "
                        f"{terminal[operation].sequence}"
                    )
        for record in trace.of_kind("ResolveSubmitted"):
            operation = _key(record.payload["operation_id"])
            known = terminal.get(operation)
            if known is not None and record.origin[1] == origin[1] and trace.before(known, record):
                reasons.append(
                    f"record {record.sequence}: {operation} is resolved again after the "
                    f"terminal result of record {known.sequence}"
                )
    return Status(FAIL if reasons else PASS, reasons)


# The origins that run in the child process of a broker (record 0019): all
# but the bench's. The evidence of the store is written by the child and
# changes incarnation with it.
CHILD = frozenset(
    {
        "broker-core",
        "transport",
        "timer",
        "store-adapter",
        "store-evidence",
        "dispatcher",
        "fault-injector",
    }
)


def transactional_kill_matches_recovery(run):
    """Match the interrupted command, its acknowledged point and complete recovery.

    This checks observed operation identities. The full DurableState equality
    against the deterministic store is an independent store test, not a claim
    calculated by the runner or inferred from a commit counter.
    """
    points = {"store.commit.prepared", "store.commit.logged", "store.commit.applied"}
    faults = [fault for fault in run.scenario["faults"] if fault["point"] in points]
    if not faults:
        return Status(OUT_OF_SCOPE, ["the scenario declares no deterministic transaction point"])
    if run.trace.broker != "child-process":
        return Status(FAIL, ["a deterministic transaction kill requires a child process"])
    statuses, reasons = [], []
    for fault in faults:
        hits = [
            hit for hit in run.trace.of_kind("FaultHit")
            if hit.payload["fault_id"] == fault["id"]
        ]
        if len(hits) != 1 or "operation_id" not in hits[0].payload:
            statuses.append(INCONCLUSIVE)
            reasons.append(f"{fault['id']}: no unique FaultHit identifies its command")
            continue
        hit = hits[0]
        if hit.payload["point"] != fault["point"]:
            statuses.append(FAIL)
            reasons.append(f"{fault['id']}: FaultHit does not match its declared point")
            continue
        operation = _key(hit.payload["operation_id"])
        submitted = [
            command for command in run.trace.of_kind("CommandSubmitted")
            if command.origin[1] == hit.origin[1]
            and _key(command.payload["operation_id"]) == operation
            and run.trace.before(command, hit) is True
        ]
        if not submitted:
            statuses.append(INCONCLUSIVE)
            reasons.append(f"{fault['id']}: the hit names no submitted command of its incarnation")
            continue
        kills = [kill for kill in run.trace.of_kind("BrokerKilled") if run.trace.before(hit, kill) is True]
        if not kills:
            statuses.append(INCONCLUSIVE)
            reasons.append(f"{fault['id']}: no kill follows its acknowledged point")
            continue
        kill = min(kills, key=lambda record: record.sequence)
        if kill.payload["wait_status"]["signal"] != 9:
            statuses.append(FAIL)
            reasons.append(f"{fault['id']}: wait status does not report signal 9")
        stable = [
            record for record in run.trace.of_kind("StoreCommitStable")
            if _key(record.payload["operation_id"]) == operation
            and run.trace.before(record, kill) is True
        ]
        if fault["point"] == "store.commit.applied":
            if len(stable) != 1 or run.trace.before(stable[0], hit) is not True:
                statuses.append(FAIL)
                reasons.append(f"{fault['id']}: no StoreCommitStable before the applied point")
        elif stable:
            statuses.append(FAIL)
            reasons.append(f"{fault['id']}: StoreCommitStable before kill at {fault['point']}")
        ready = [
            record for record in run.trace.of_kind("StoreResultReady")
            if _key(record.payload["operation_id"]) == operation
            and run.trace.before(record, kill) is True
        ]
        if ready:
            statuses.append(FAIL)
            reasons.append(f"{fault['id']}: the interrupted command already has StoreResultReady")
        # Only the first new incarnation's complete inventory answers this
        # kill; another restart or a later retransmission cannot supply it.
        startup = _first_recovery_after_kill(run, kill)
        if startup is None:
            statuses.append(INCONCLUSIVE)
            reasons.append(f"{fault['id']}: no unambiguous first recovery after the kill")
            continue
        incarnation = int(startup.payload["run_epoch"])
        pages = [
            page for page in run.trace.of_kind("RecoveryPageDelivered")
            if page.origin[1] == incarnation
            and int(page.payload["run_epoch"]) == incarnation
            and _recovery_after_kill(run, page, kill) is True
        ]
        pages.sort(key=lambda page: int(page.payload["page"]))
        complete = (
            bool(pages)
            and [int(page.payload["page"]) for page in pages] == list(range(len(pages)))
            and pages[-1].payload["last"]
            and not any(page.payload["last"] for page in pages[:-1])
        )
        reads = run.trace.of_kind("RecoveryPageRead")
        corroborated = all(
            any(
                read.origin[1] == page.origin[1]
                and read.payload == page.payload
                and run.trace.before(read, page) is True
                for read in reads
            )
            for page in pages
        )
        if not complete or not corroborated:
            statuses.append(INCONCLUSIVE)
            reasons.append(f"{fault['id']}: no complete corroborated recovery inventory for first startup {incarnation}")
            continue
        found = [
            fact["knowledge"] for page in pages for fact in page.payload["facts"]
            if fact["kind"] == "Operation" and _key(fact["operation_id"]) == operation
        ]
        if fault["point"] == "store.commit.prepared":
            if found:
                statuses.append(FAIL)
                reasons.append(f"{fault['id']}: interrupted operation {operation} was recovered at K1")
        elif found != ["Committed"]:
            statuses.append(FAIL)
            reasons.append(f"{fault['id']}: interrupted operation {operation} was not recovered Committed")
    return Status(_worst(statuses), reasons)


def broker_killed_for_real(run):
    """A run that claims its broker runs as a child process shows its kill (record 0019).

    Every BrokerCrashed follows a BrokerKilled of that crash with the
    status of signal 9, and no record of the killed incarnation of the
    child comes after the kill. A missing kill is missing evidence. A run
    in-process has nothing to show, and records no kill.
    """
    trace = run.trace
    kills = trace.of_kind("BrokerKilled")
    if trace.broker != "child-process":
        if kills:
            return Status(
                FAIL,
                [
                    f"record {record.sequence}: a kill recorded by a run whose broker "
                    "runs in the process of the bench"
                    for record in kills
                ],
            )
        return Status(PASS)
    statuses, reasons = [], []
    supervisor = sorted(
        (
            record
            for record in trace.records
            if record.origin[0] == "bench-supervisor"
            and record.kind in ("BrokerKilled", "BrokerCrashed")
        ),
        key=lambda record: (record.origin[1], record.origin_sequence),
    )
    pending = None
    for record in supervisor:
        if record.kind == "BrokerKilled":
            pending = record
        elif pending is None:
            statuses.append(INCONCLUSIVE)
            reasons.append(
                f"record {record.sequence}: a crash of a broker run as a child process "
                "follows no recorded kill"
            )
        else:
            pending = None
    child = [record for record in trace.records if record.origin[0] in CHILD]
    for kill in kills:
        signal = kill.payload["wait_status"]["signal"]
        if signal != 9:
            statuses.append(FAIL)
            reasons.append(
                f"record {kill.sequence}: the child ended by signal {signal}, not by SIGKILL (9)"
            )
        killed = [record.origin[1] for record in child if trace.before(record, kill)]
        if not killed:
            continue
        incarnation = max(killed)
        for record in child:
            if record.origin[1] > incarnation:
                continue
            order = trace.before(kill, record)
            if order:
                statuses.append(FAIL)
                reasons.append(
                    f"record {record.sequence} ({record.kind}) of the killed incarnation "
                    f"{incarnation} comes after the kill of record {kill.sequence}"
                )
            elif order is None and not trace.before(record, kill):
                statuses.append(INCONCLUSIVE)
                reasons.append(
                    f"record {record.sequence} ({record.kind}) of the killed incarnation "
                    f"{incarnation} is not ordered with the kill of record {kill.sequence}"
                )
    return Status(_worst(statuses), reasons)


def one_grant_per_opened_epoch(run):
    """At most one creation authority per opened epoch (DEM-012, record 0011).

    A grant follows, among its causes in the same incarnation of the core,
    a committed reply for its epoch, and the store opened that epoch for
    the attempt the reply answers. An abandoned or unknown opening alone
    never justifies one: only a committed reply taken before or after it.
    """
    trace = run.trace
    grants = trace.of_kind("EpochGrantHandedOver")
    if not grants:
        return Status(INCONCLUSIVE, ["the trace records no grant of an epoch"])
    by_key = {record.key: record for record in trace.records}
    opened = {
        (record.payload["incarnation"], record.payload["epoch"]): record.payload["attempt"]
        for record in trace.of_kind("StoreEpochOpened")
    }
    reasons, seen = [], {}
    for grant in grants:
        epoch = (grant.payload["incarnation"], grant.payload["epoch"])
        if epoch in seen:
            reasons.append(
                f"record {grant.sequence}: epoch {epoch[1]} was already granted by record {seen[epoch]}"
            )
        seen.setdefault(epoch, grant.sequence)
        replies = [
            by_key[key]
            for key in trace.causes(grant)
            if key in by_key
            and by_key[key].kind == "OpeningReplyDelivered"
            and by_key[key].origin == grant.origin
            and by_key[key].payload["outcome"] == "Committed"
            and (by_key[key].payload["incarnation"], by_key[key].payload["epoch"]) == epoch
        ]
        # Justified only by a committed reply the store's ledger confirms:
        # without any committed reply there is nothing to confirm.
        if not any(opened.get(epoch) == reply.payload["attempt"] for reply in replies):
            why = (
                "the store opened it for no attempt whose committed reply the core took"
                if replies
                else "no committed reply for it precedes the grant"
            )
            reasons.append(f"record {grant.sequence}: epoch {epoch[1]}: {why}")
    return Status(FAIL if reasons else PASS, reasons)


def durable_reads_complete(run):
    """A refusal to establish the state is missing evidence, never absence."""
    failures = [record for record in run.trace.records
                if record.kind in {"DurableReadFailed", "ProcessControlFailed"}]
    if not failures:
        return Status(PASS)
    return Status(INCONCLUSIVE, [
        f"record {record.sequence}: {record.kind} {record.payload['code']} "
        f"during {record.payload['phase']} of process {record.payload['process_incarnation']}"
        for record in failures[:64]
    ])


ASSERTIONS = {
    "fenced_operation_never_executed": fenced_operation_never_executed,
    "certain_rejection_has_a_closure": certain_rejection_has_a_closure,
    "refusal_carries_the_reason_of_its_rejection": refusal_carries_the_reason_of_its_rejection,
    "one_admission_per_observed_inbound_exchange": one_admission_per_observed_inbound_exchange,
    "same_required_obligations_at_both_checkpoints": same_required_obligations_at_both_checkpoints,
    "same_recipe_for_op_A": same_recipe_for_op_A,
    "no_positive_ack_before_confirmed_commit": no_positive_ack_before_confirmed_commit,
    "no_pubrec_observed_before_crash": no_pubrec_observed_before_crash,
    "broker_inputs_have_provenance": broker_inputs_have_provenance,
    "one_grant_per_opened_epoch": one_grant_per_opened_epoch,
    "terminal_knowledge_never_regresses": terminal_knowledge_never_regresses,
    "scenario_reaches_its_end": scenario_reaches_its_end,
    "broker_killed_for_real": broker_killed_for_real,
    "transactional_kill_matches_recovery": transactional_kill_matches_recovery,
    "all_required_faults_hit": all_required_faults_hit,
    "trace_complete": trace_complete,
    "durable_reads_complete": durable_reads_complete,
}


def judge(run: Run) -> dict:
    """The status of every assertion of the scenario, by name."""
    trace = run.trace
    required = run.scenario.get("perimeter", "full")
    statuses = {}
    # The assertions of the scenario, then those every run owes.
    owed = list(run.scenario["assertions"])
    owed += [name for name in run.contracts.catalogs["every_run"] if name not in owed]
    for name in owed:
        decide = ASSERTIONS.get(name)
        if decide is None:
            statuses[name] = Status(INCONCLUSIVE, ["the oracle does not decide this assertion"])
        elif trace.unreadable:
            statuses[name] = Status(INCONCLUSIVE, list(trace.unreadable))
        elif trace.scenario_id not in (None, run.scenario["id"]):
            # A run is judged only against its own scenario.
            statuses[name] = Status(INCONCLUSIVE, [f"the trace is a run of {trace.scenario_id}"])
        elif trace.perimeter not in (None, required):
            # Nor against a scenario of another perimeter: a run without
            # bytes never passes a full scenario.
            statuses[name] = Status(
                OUT_OF_SCOPE,
                [f"the run is {trace.perimeter}; the scenario requires {required}"],
            )
        elif trace.perimeter != "full" and name in NEEDS_PEERS:
            statuses[name] = Status(OUT_OF_SCOPE, [f"no peer observes a {trace.perimeter} run"])
        else:
            statuses[name] = decide(run)
    if trace.incomplete:
        # Nothing concluded from an incomplete trace is a PASS or a scope
        # decision, and a FAIL stands only where no missing record could
        # undo it.
        for name, status in statuses.items():
            if status.status in (PASS, OUT_OF_SCOPE) or (
                status.status == FAIL and not (name in MONOTONE and status.stands)
            ):
                statuses[name] = Status(INCONCLUSIVE, status.reasons + ["the trace is incomplete"])
    return statuses
