"""Absolute expectations on mqttiumd's transcript.

Comparing two brokers says where they differ, not which one is right:
Mosquitto is not the oracle, the MQTT specification is. An expectation is
what one normative statement requires of the broker in one scenario,
written as patterns over the records the scenario names with `dsl.mark`:

    expectation("suback", statement="MQTT-3.8.4-1",
                requires="A SUBACK with the Packet Identifier of the SUBSCRIBE",
                then=[at("answer", {"got": [{"type": "SUBACK", "packet_id": 7}], "end": "ok"})])

It is evaluated on the raw transcript (normalization would hide the
capabilities a `when` condition reads) of mqttiumd, and of the reference
for information only. The result is `pass`, `fail`, or `not-applicable`
when a `when` condition does not hold, with one line of what was observed.

A pattern is matched structurally:

- a mapping matches a mapping that has each of its keys, with a matching
  value; other keys are free;
- a list matches a list of the same length, element by element;
- `one_of(a, b, ...)` matches what one alternative matches; `CLOSED` is
  `one_of("eof", "reset")`;
- `Each(count, allowed)` matches a list of `count` values of `allowed`;
- `in_any_order(a, b, ...)` matches a list of as many values, each one
  matching a pattern of its own: what a broker may send in any order
  (the retained messages of several topics), each once and nothing else;
- `at_most(bound)` matches an integer no greater than `bound` (the
  `wire_bytes` of a packet against a Maximum Packet Size);
- anything else matches an equal value of the same type.

`expectations-open.json` lists the expectations mqttiumd is known to
fail, the compatibility backlog seen from the specification:

    [{"id": "<scenario>#<name>", "statement": "...", "observed": "one line"}]

sorted by `id`. An expectation that fails without being listed fails the
run, and so does a listed one that passes, does not apply, does not exist,
or cites another statement than the scenario declares.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from classify import STATEMENT

PASS, FAIL, NOT_APPLICABLE = "pass", "fail", "not-applicable"
RESULTS = (PASS, FAIL, NOT_APPLICABLE)

# Problems that make a run fail.
UNLISTED = "the expectation fails and expectations-open.json does not list it"
STALE = "expectations-open.json lists the expectation, and its result is now `{result}`"
UNKNOWN = "expectations-open.json names an expectation that no scenario declares"
OTHER_STATEMENT = "expectations-open.json cites `{listed}`, the scenario declares `{declared}`"


class OpenError(ValueError):
    """`expectations-open.json` is not usable."""


class OneOf:
    """Matches what one of `alternatives` matches."""

    def __init__(self, *alternatives):
        self.alternatives = alternatives


class Each:
    """Matches a list of exactly `count` values, each one of `allowed`."""

    def __init__(self, count: int, allowed):
        self.count, self.allowed = count, tuple(allowed)


class AnyOrder:
    """Matches a list of as many values as `patterns`, in any order."""

    def __init__(self, *patterns):
        self.patterns = patterns


class AtMost:
    """Matches an integer no greater than `bound`."""

    def __init__(self, bound: int):
        self.bound = bound


def one_of(*alternatives) -> OneOf:
    return OneOf(*alternatives)


def in_any_order(*patterns) -> AnyOrder:
    return AnyOrder(*patterns)


def at_most(bound: int) -> AtMost:
    return AtMost(bound)


# How a read finds a connection the broker closed (harness.CLOSED).
CLOSED = one_of("eof", "reset")
# A record of more packets than this is shown by its counts per type.
SHOWN = 8


def paired(patterns, values) -> bool:
    """Whether each value can be given a pattern of its own that it
    matches: a perfect matching, found by augmenting paths, so that the
    answer does not depend on which pattern a value is tried against
    first."""
    if len(patterns) != len(values):
        return False
    fits = [[index for index, pattern in enumerate(patterns) if matches(pattern, value)] for value in values]
    owner: dict[int, int] = {}

    def place(value: int, tried: set) -> bool:
        for index in fits[value]:
            if index in tried:
                continue
            tried.add(index)
            if index not in owner or place(owner[index], tried):
                owner[index] = value
                return True
        return False

    return all(place(value, set()) for value in range(len(values)))


def matches(pattern, value) -> bool:
    if isinstance(pattern, OneOf):
        return any(matches(alternative, value) for alternative in pattern.alternatives)
    if isinstance(pattern, AnyOrder):
        return isinstance(value, list) and paired(pattern.patterns, value)
    if isinstance(pattern, AtMost):
        return type(value) is int and value <= pattern.bound
    if isinstance(pattern, Each):
        return (isinstance(value, list) and len(value) == pattern.count
                and all(any(matches(allowed, item) for allowed in pattern.allowed) for item in value))
    if isinstance(pattern, dict):
        return isinstance(value, dict) and all(key in value and matches(inner, value[key])
                                               for key, inner in pattern.items())
    if isinstance(pattern, (list, tuple)):
        return (isinstance(value, list) and len(value) == len(pattern)
                and all(matches(inner, item) for inner, item in zip(pattern, value)))
    return type(pattern) is type(value) and pattern == value


@dataclass(frozen=True)
class Expectation:
    name: str
    statement: str      # the normative statements it rests on, as in divergences.json
    requires: str       # one line: the conformant behaviour
    then: tuple         # ((mark, pattern), ...): all must hold
    when: tuple = ()    # ((mark, pattern), ...): unless all hold, not applicable

    same_packet_id: tuple = ()  # two (mark, index in got) references, compared on raw packets

    @property
    def marks(self) -> tuple:
        """The records it reads, in order, each once."""
        return tuple(dict.fromkeys(mark for mark, _ in (*self.when, *self.then, *self.same_packet_id)))


def at(mark: str, pattern) -> tuple:
    """A condition: the record named `mark` matches `pattern`."""
    return mark, pattern


def expectation(name, *, statement, requires, then, when=(), same_packet_id=()) -> Expectation:
    """An expectation of a scenario; `dsl.scenario(..., expectations=[...])`."""
    if not isinstance(name, str) or not name or "#" in name:
        raise ValueError(f"{name!r}: an expectation is named without `#`")
    for key, value in (("statement", statement), ("requires", requires)):
        if not isinstance(value, str) or not value.strip() or "\n" in value:
            raise ValueError(f"{name}: `{key}` must be one non-empty line")
    if not STATEMENT.search(statement):
        raise ValueError(f"{name}: `statement` cites no statement identifier such as MQTT-3.1.4-2")
    if not then:
        raise ValueError(f"{name}: `then` names at least one record")
    if same_packet_id and (len(same_packet_id) != 2 or any(type(i) is not int or i < 0 for _, i in same_packet_id)):
        raise ValueError(f"{name}: same_packet_id needs two (mark, nonnegative packet index) pairs")
    return Expectation(name=name, statement=statement, requires=requires, then=tuple(then), when=tuple(when),
                       same_packet_id=tuple(same_packet_id))


def declared(registry: dict) -> dict:
    """`{"<scenario>#<name>": Expectation}` of every scenario of `registry`."""
    return {f"{name}#{item.name}": item for name in sorted(registry) for item in registry[name].expectations}


# -- evaluation ---------------------------------------------------------------

# The fields of a packet that are always shown, each in its own form.
ALWAYS = ("type", "packet_id", "reason", "codes", "properties")


def mentioned_properties(pattern, out: dict) -> dict:
    """`{packet type: names}` that the patterns read: the properties of a
    packet of that type, and under `(type, "fields")` its other fields
    that are not shown anyway."""
    if isinstance(pattern, (OneOf, AnyOrder)):
        for inner in (pattern.alternatives if isinstance(pattern, OneOf) else pattern.patterns):
            mentioned_properties(inner, out)
    elif isinstance(pattern, dict):
        if isinstance(pattern.get("type"), str):
            if isinstance(pattern.get("properties"), dict):
                out.setdefault(pattern["type"], set()).update(pattern["properties"])
            out.setdefault((pattern["type"], "fields"), set()).update(set(pattern) - set(ALWAYS))
        for inner in pattern.values():
            mentioned_properties(inner, out)
    elif isinstance(pattern, (list, tuple)):
        for inner in pattern:
            mentioned_properties(inner, out)
    return out


def show_packet(packet: dict, properties: dict) -> str:
    parts = [packet["type"]]
    if "packet_id" in packet:
        parts.append(f"packet_id={packet['packet_id']}")
    if isinstance(packet.get("reason"), int):
        parts.append(f"reason=0x{packet['reason']:02x}")
    if "codes" in packet:
        parts.append("codes=" + ",".join(f"0x{code:02x}" for code in packet["codes"]))
    for name in sorted(properties.get((packet["type"], "fields"), ())):
        parts.append(f"{name}={json.dumps(packet[name])}" if name in packet else f"{name} absent")
    for name in sorted(properties.get(packet["type"], ())):
        found = packet.get("properties", {})
        parts.append(f"{name}={json.dumps(found[name])}" if name in found else f"{name} absent")
    return " ".join(parts)


def show(record, properties: dict) -> str:
    if record is None:
        return "no such record"
    packets = record.get("got", ())
    if len(packets) > SHOWN:
        counts: dict[str, int] = {}
        for packet in packets:
            counts[packet["type"]] = counts.get(packet["type"], 0) + 1
        got = f"{len(packets)} packets: " + ", ".join(f"{count} {kind}" for kind, count in counts.items())
    else:
        got = ", ".join(show_packet(packet, properties) for packet in packets)
    return f"{record['op']} [{got}] {record.get('end', '')}".rstrip()


def evaluate(item: Expectation, transcript) -> tuple:
    """`(result, observed)` of one expectation on one raw transcript.

    `observed` is one line: the records the expectation reads, reduced to
    packet types, identifiers, reason codes and the fields and properties
    its patterns name; a record of more than `SHOWN` packets is reduced
    to its counts per type. A record that the transcript does not have
    matches nothing.
    """
    records = {record["mark"]: record for record in transcript if "mark" in record}
    properties = mentioned_properties([pattern for _, pattern in (*item.when, *item.then)], {})

    def hold(conditions) -> bool:
        return all(mark in records and matches(pattern, records[mark]) for mark, pattern in conditions)

    applies = hold(item.when)
    marks = item.marks if applies else tuple(dict.fromkeys(mark for mark, _ in item.when))
    observed = "; ".join(f"{mark}: {show(records.get(mark), properties)}" for mark in marks)
    if not applies:
        return NOT_APPLICABLE, observed
    passed = hold(item.then)
    if passed and item.same_packet_id:
        found = []
        for mark, index in item.same_packet_id:
            got = records.get(mark, {}).get("got", [])
            found.append(got[index].get("packet_id") if index < len(got) else None)
        passed = type(found[0]) is int and found[0] == found[1]
    return (PASS if passed else FAIL), observed


# -- the list of open expectations and the verdict ------------------------------

def check_open(data) -> dict:
    """Validate the list and return it keyed by expectation id."""
    if not isinstance(data, list):
        raise OpenError("expectations-open.json must be a list of entries")
    out: dict = {}
    for entry in data:
        if not isinstance(entry, dict) or not isinstance(entry.get("id"), str):
            raise OpenError("every entry is an object with an `id`")
        name = entry["id"]
        scenario, _, rest = name.partition("#")
        if not scenario or not rest:
            raise OpenError(f"{name}: `id` is `<scenario>#<expectation>`")
        if name in out:
            raise OpenError(f"{name}: listed twice")
        if set(entry) != {"id", "statement", "observed"}:
            raise OpenError(f"{name}: an entry has `id`, `statement` and `observed`")
        for key in ("statement", "observed"):
            if not isinstance(entry[key], str) or not entry[key].strip() or "\n" in entry[key]:
                raise OpenError(f"{name}: `{key}` must be one non-empty line")
        if not STATEMENT.search(entry["statement"]):
            raise OpenError(f"{name}: `statement` cites no statement identifier such as MQTT-3.1.4-2")
        out[name] = entry
    if list(out) != sorted(out):
        raise OpenError("entries are sorted by `id`")
    return out


def load_open(path: Path) -> dict:
    return check_open(json.loads(path.read_text()))


def check_statements(known_expectations: dict, versions: dict, known) -> list:
    """Cited identifiers that the specification of the scenario does not
    contain. `versions` maps a scenario id to its protocol version and
    `known` is `classify.known_statements`."""
    problems = []
    if known is None:
        return problems
    for name, item in known_expectations.items():
        version = versions.get(name.partition("#")[0])
        pools = [known[version]] if version in known else list(known.values())
        for cited in STATEMENT.finditer(item.statement):
            if not any(cited.group(0) in pool for pool in pools):
                problems.append((name, f"statement {cited.group(0)} is not in the MQTT {version} specification"))
    return problems


def judge(evaluated: dict, listed: dict, known_expectations: dict) -> list:
    """The `(expectation id, problem)` pairs that fail the run.

    `evaluated` maps the expectations of the scenarios played to their
    result on mqttiumd's transcript, `listed` is `expectations-open.json`
    and `known_expectations` every expectation that exists.
    """
    problems = []
    for name in sorted({*evaluated, *listed}):
        entry, result = listed.get(name), evaluated.get(name)
        if entry is not None and name not in known_expectations:
            problems.append((name, UNKNOWN))
        elif entry is not None and entry["statement"] != known_expectations[name].statement:
            problems.append((name, OTHER_STATEMENT.format(listed=entry["statement"],
                                                         declared=known_expectations[name].statement)))
        elif entry is None and result == FAIL:
            problems.append((name, UNLISTED))
        elif entry is not None and result in (PASS, NOT_APPLICABLE):
            problems.append((name, STALE.format(result=result)))
    return problems
