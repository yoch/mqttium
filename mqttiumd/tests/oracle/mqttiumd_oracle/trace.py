"""Reading a trace as a whole (DEM-016).

A trace is JSON Lines: a header, records, a footer. Each line is a valid
``mqttiumd.trace-record``. On top of that, the trace must be complete: one
header first, one footer last that counts the records and declares
completeness, a gapless collector sequence, a gapless sequence per origin,
and every causal parent present.

A record that is not a valid record makes the trace unreadable. A trace that
is readable but incomplete can still be looked at, and nothing concluded from
it is ever a PASS.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .strict import Refused, parse

MAX_LINE_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class Record:
    """One record, with its identities as integers."""

    position: int
    sequence: int
    origin: tuple
    origin_sequence: int
    parents: tuple
    kind: str
    payload: dict

    @property
    def key(self):
        return (*self.origin, self.origin_sequence)


@dataclass
class Trace:
    records: list = field(default_factory=list)
    backend: str | None = None
    scenario_id: str | None = None
    perimeter: str | None = None
    run_id: str | None = None
    # Why the trace cannot be read at all.
    unreadable: list = field(default_factory=list)
    # Why the trace is not complete.
    incomplete: list = field(default_factory=list)
    _ancestors: dict = field(default_factory=dict)
    _by_key: dict = field(default_factory=dict)
    # Key of a record -> key of the record its origin stated just before.
    _previous: dict = field(default_factory=dict)

    @property
    def complete(self) -> bool:
        return not self.unreadable and not self.incomplete

    def of_kind(self, *kinds):
        return [record for record in self.records if record.kind in kinds]

    def _predecessors(self, key):
        """What precedes a record directly: its explicit causes and its origin's previous record."""
        record = self._by_key.get(key)
        found = list(record.parents) if record is not None else []
        previous = self._previous.get(key)
        if previous is not None:
            found.append(previous)
        return found

    def ancestors(self, record):
        """Keys of every record that happened before ``record``.

        One relation (finding F5): the local order of each origin and the
        explicit causes together, closed transitively.
        """
        found = self._ancestors.get(record.key)
        if found is None:
            found = set()
            pending = self._predecessors(record.key)
            while pending:
                key = pending.pop()
                if key in found:
                    continue
                found.add(key)
                pending.extend(self._predecessors(key))
            self._ancestors[record.key] = found
        return found

    def before(self, first, second):
        """Whether ``first`` happened before ``second``.

        ``True`` or ``False`` when the trace establishes it: two records of
        one origin are ordered by that origin, a cause precedes its
        consequence, and a simulated run has a known total order. ``None``
        when the trace does not say: the order of arrival at a collector is
        not a causality, and the oracle does not invent one.
        """
        if first.key == second.key:
            return False
        if first.key in self.ancestors(second):
            return True
        if second.key in self.ancestors(first):
            return False
        if self.backend == "sim-store":
            return first.sequence < second.sequence
        return None


def lines_of(stream, limit=None):
    """The lines of a binary ``stream``, each read with a bound.

    A line is never read past ``MAX_LINE_BYTES + 1`` bytes: a longer one is
    yielded cut, and ``read`` refuses it, so no line is allocated whole
    before its size is known.
    """
    bound = (MAX_LINE_BYTES if limit is None else limit) + 1
    while line := stream.readline(bound):
        yield line
        if len(line) == bound and not line.endswith(b"\n"):
            # A line longer than the bound: nothing after it is read.
            return


def read(contracts, lines, max_bytes=None) -> Trace:
    """The trace held by ``lines``, an iterable of ``bytes`` lines."""
    trace = Trace()
    total = 0
    for position, line in enumerate(lines):
        total += len(line)
        if max_bytes is not None and total > max_bytes:
            trace.incomplete.append(f"the trace exceeds its budget of {max_bytes} bytes")
            break
        if len(line) > MAX_LINE_BYTES:
            trace.unreadable.append(f"line {position}: longer than {MAX_LINE_BYTES} bytes")
            break
        try:
            document = parse(line)
        except Refused as refusal:
            trace.unreadable.append(f"line {position}: {refusal.diagnostic}")
            break
        if not isinstance(document, dict) or document.get("format") != "mqttiumd.trace-record":
            trace.unreadable.append(f"line {position}: not a trace record")
            break
        problems = contracts.check_value(document)
        if problems:
            trace.unreadable.append(f"line {position}: {problems[0]}")
            break
        trace.records.append(
            Record(
                position=position,
                sequence=int(document["record_sequence"]),
                origin=(document["origin_component"], int(document["origin_incarnation"])),
                origin_sequence=int(document["origin_sequence"]),
                parents=tuple(
                    (
                        parent["origin_component"],
                        int(parent["origin_incarnation"]),
                        int(parent["origin_sequence"]),
                    )
                    for parent in document["causal_parents"]
                ),
                kind=document["kind"],
                payload=document["payload"],
            )
        )
        if trace.run_id is None:
            trace.run_id = document["run_id"]
        elif document["run_id"] != trace.run_id:
            trace.unreadable.append(f"line {position}: a record of another run")
            break
    if not trace.unreadable:
        _check_whole(trace)
    return trace


def _cycle(trace: Trace):
    """The collector sequence of a record on a cycle of the order, or ``None``.

    Kahn's algorithm over the explicit causes and the local order of each
    origin, in time and memory linear in the records and their causes.
    """
    waiting = {key: 0 for key in trace._by_key}
    followers = {key: [] for key in trace._by_key}
    for key in trace._by_key:
        for before in trace._predecessors(key):
            if before in waiting:
                waiting[key] += 1
                followers[before].append(key)
    ready = [key for key, count in waiting.items() if count == 0]
    while ready:
        key = ready.pop()
        for follower in followers[key]:
            waiting[follower] -= 1
            if waiting[follower] == 0:
                ready.append(follower)
    left = [trace._by_key[key].sequence for key, count in waiting.items() if count]
    return min(left) if left else None


def _check_whole(trace: Trace) -> None:
    records = trace.records
    problems = trace.incomplete
    if not records or records[0].kind != "TraceHeader":
        problems.append("the trace does not start with its header")
    else:
        trace.backend = records[0].payload["backend"]
        trace.scenario_id = records[0].payload["scenario_id"]
        trace.perimeter = records[0].payload["perimeter"]
    if any(record.kind == "TraceHeader" for record in records[1:]):
        problems.append("a second header")

    for position, record in enumerate(records):
        if record.sequence != position:
            problems.append(
                f"record {position} has the collector sequence {record.sequence}: "
                "a record is missing or out of place"
            )
            break
    expected = {}
    for record in records:
        wanted = expected.get(record.origin, 0)
        if record.origin_sequence != wanted:
            problems.append(
                f"{record.origin[0]} (incarnation {record.origin[1]}) jumps from "
                f"{wanted - 1} to {record.origin_sequence}: a record is missing"
            )
            break
        expected[record.origin] = wanted + 1

    trace._by_key = {record.key: record for record in records}
    last = {}
    for record in records:
        if record.origin in last:
            trace._previous[record.key] = last[record.origin]
        last[record.origin] = record.key
    cycle = _cycle(trace)
    if cycle is not None:
        problems.append(
            f"the local orders and the causes form a cycle through record {cycle}: "
            "no order of the records is consistent with both"
        )
        # Every relation computed over a cycle would be false.
        trace._previous.clear()
    for record in records:
        for parent in record.parents:
            cause = trace._by_key.get(parent)
            if cause is None:
                problems.append(
                    f"record {record.sequence} names the cause {parent}, which is not in the trace"
                )
            elif cause.key == record.key:
                problems.append(f"record {record.sequence} is among its own causes")
            elif trace.backend == "sim-store" and cause.sequence > record.sequence:
                # A simulated run has a total order: a cause recorded after
                # its consequence is not an interleaving, it is an error.
                problems.append(
                    f"record {record.sequence} precedes its cause, record {cause.sequence}"
                )
            elif cause.origin == record.origin and cause.origin_sequence > record.origin_sequence:
                problems.append(f"record {record.sequence} precedes its cause in its own origin")

    footers = [record for record in records if record.kind == "TraceFooter"]
    if not footers or records[-1].kind != "TraceFooter":
        problems.append("the trace does not end with its footer: it was cut")
    elif len(footers) > 1:
        problems.append("a second footer")
    else:
        footer = footers[0].payload
        if int(footer["record_count"]) != len(records):
            problems.append(
                f"the footer counts {footer['record_count']} records, the trace holds {len(records)}"
            )
        if footer["complete"] is not True:
            problems.append("the footer does not declare the trace complete")
