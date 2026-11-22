"""Turn a raw transcript into the form the two brokers are compared in.

The raw transcript is what the player recorded: every packet received, in
the order it came, and how each read ended. It is written next to the
comparable form (`<id>.<backend>.raw.json`) and never compared.

Kept as observed: packet types and their order per step, flags, QoS,
topics, payloads, reason codes, the packet identifiers the client chose
(and their echo in the broker's acknowledgements), property values,
whether a connection was closed, and how: by end of stream (`eof`) or by
a reset (`reset`).

Normalized, because the value cannot be equal or is not what a scenario
compares:

- a packet identifier the broker chose (PUBLISH at QoS 1 or 2, PUBREL,
  and the client's answers to them) becomes `b1`, `b2` ... per connection
  name, in order of first use; an identifier keeps its label until the
  client completes the exchange, so a redelivery under another identifier
  shows, and reusing a completed identifier does not;
- `assigned_client_identifier` becomes `<assigned>`;
- `reason_string` is removed: it is free text for a human;
- the capabilities a broker advertises in a MQTT 5 CONNACK are removed,
  except in a scenario declared with `capabilities=True` (which compares
  them; every MQTT 5 scenario would differ otherwise);
- a numeric property named in `Scenario.buckets` becomes the interval it
  falls in (the remaining Message Expiry Interval depends on the clock);
- the packets a `tally` step counted become two verdicts: whether all the
  expected ones came (`arrived`: `all`, `fewer`, `more`) and, when the
  step declares the sequence sent, in which order (`order`: `as-sent`,
  `permuted`, `other`). How many came and which permutation are in the
  raw transcript; they may vary from one run to the next;
- the packets a `measure` step counted become what the protocol
  guarantees about them whatever their number: whether each is one of
  the values that could lawfully come and came once (`values`:
  `distinct`, `repeated`, `foreign`) and, when the step asks for it,
  their order. How many came is not compared at all: it is a measurement,
  printed in the matrix for both brokers;
- in a `drain` step the client acknowledged each QoS 1 PUBLISH as it read
  it, so each one gets a label of its own, whatever identifier the broker
  used again;
- the size of a packet on the wire (`wire_bytes`), which is in the raw
  transcript for the expectations, is removed;
- the packets of a step declared with `sort=True` are put in one order;
- in a scenario declared with `closure="any"`, `eof` and `reset` both
  become `closed`: the kind of closure depends there on how much of the
  client's writing the broker had read when it closed;
- a `mark`, which names a record for `expectations.py`, is removed;
- no time is recorded;
- properties are a mapping, so their wire order is not compared; user
  properties keep their order, which the specification makes significant.
"""
from __future__ import annotations

import copy
import hashlib
import json

ASSIGNED = "<assigned>"
CLOSED = ("eof", "reset")

# CONNACK properties that describe the broker, not the exchange.
CAPABILITIES = frozenset({
    "receive_maximum", "maximum_qos", "retain_available", "maximum_packet_size",
    "topic_alias_maximum", "wildcard_subscription_available",
    "subscription_identifier_available", "shared_subscription_available",
})


def bucket(value: int, edges) -> str:
    """Name the interval of the sorted `edges` that `value` falls in.

    With edges `[0, 5, 9, 10]`: 0 -> "0", 3 -> "1..5", 8 -> "6..9",
    10 -> "10", 11 -> ">10".
    """
    low = 0
    edge = None
    for edge in sorted(edges):
        if value <= edge:
            return str(edge) if low >= edge else f"{low}..{edge}"
        low = edge + 1
    return f">{edge}"


class BrokerIdentifiers:
    """Labels for the packet identifiers the broker chose on one connection name."""

    def __init__(self):
        self.active: dict[int, str] = {}
        self.count = 0

    def delivered(self, packet: dict) -> None:
        """A PUBLISH at QoS 1 or 2, or a PUBREL, read from the broker."""
        raw = packet["packet_id"]
        if packet["type"] == "PUBLISH" and raw not in self.active:
            self.count += 1
            self.active[raw] = f"b{self.count}"
        packet["packet_id"] = self.active.get(raw, raw)

    def acknowledged(self, raw: int) -> None:
        """The client acknowledged the QoS 1 PUBLISH `raw` as it read it
        (`drain`): the identifier is free for another delivery."""
        self.active.pop(raw, None)

    def answered(self, packet: dict) -> None:
        """A PUBACK, PUBREC or PUBCOMP the client sent."""
        raw = packet["packet_id"]
        packet["packet_id"] = self.active.get(raw, raw)
        if packet["type"] != "PUBREC" or packet.get("reason", 0) >= 0x80:
            self.active.pop(raw, None)


def normalize_received(packet: dict, *, buckets: dict, capabilities: bool) -> None:
    packet.pop("wire_bytes", None)
    props = packet.get("properties")
    if props is None:
        return
    props.pop("reason_string", None)
    if packet["type"] == "CONNACK":
        if "assigned_client_identifier" in props:
            props["assigned_client_identifier"] = ASSIGNED
        if not capabilities:
            for name in CAPABILITIES:
                props.pop(name, None)
    for name, edges in buckets.items():
        if isinstance(props.get(name), int):
            props[name] = bucket(props[name], edges)
    if not props:
        del packet["properties"]


def packet_order(packet: dict):
    """A total order of decoded packets, for steps that ask for one. It
    ignores packet identifiers, which the broker may choose."""
    rest = {key: value for key, value in packet.items() if key != "packet_id"}
    return packet["type"], json.dumps(rest, sort_keys=True), str(packet.get("packet_id"))


def order(seen, sequence) -> str:
    """How the values `seen` stand against the `sequence` that was sent:
    `as-sent` when they come in its order (some may be missing),
    `permuted` when they are all of it and distinct but not in its order,
    `other` when one is repeated or is not of the sequence."""
    if any(value not in sequence for value in seen) or any(value in seen[:index] for index, value in enumerate(seen)):
        return "other"
    places = [sequence.index(value) for value in seen]
    return "as-sent" if places == sorted(places) else "permuted"


def tally(record: dict) -> dict:
    """The comparable form of a raw `tally` record: its verdicts, and the
    packets of another type than the one counted."""
    kind, expected = record["kind"], record["expected"]
    counted = [packet for packet in record["got"] if packet["type"] == kind]
    out = {"conn": record["conn"], "op": "tally", "kind": kind, "expected": expected,
           "arrived": "all" if len(counted) == expected else "fewer" if len(counted) < expected else "more"}
    if "sequence" in record:
        out["order"] = order([packet.get(record["field"]) for packet in counted], record["sequence"])
    return {**out, "got": [packet for packet in record["got"] if packet["type"] != kind], "end": record["end"]}


def values(seen, sequence) -> str:
    """How the values `seen` stand against the `sequence` that could
    lawfully come: `foreign` when one is not of it, else `repeated` when
    one came twice, else `distinct`."""
    if any(value not in sequence for value in seen):
        return "foreign"
    return "repeated" if len(set(seen)) != len(seen) else "distinct"


def measure(record: dict) -> dict:
    """The comparable form of a raw `measure` record: what the protocol
    guarantees about the packets counted, whatever their number, and the
    packets of another type. How many came is not in it."""
    kind = record["kind"]
    seen = [packet.get(record["field"]) for packet in record["got"] if packet["type"] == kind]
    out = {"conn": record["conn"], "op": "measure", "kind": kind, "values": values(seen, record["sequence"])}
    if record["ordered"]:
        out["order"] = order(seen, record["sequence"])
    return {**out, "got": [packet for packet in record["got"] if packet["type"] != kind], "end": record["end"]}


REDUCED = {"tally": tally, "measure": measure}


def normalize(transcript, scenario) -> list:
    """A deep copy of `transcript` in comparable form."""
    out = [REDUCED[record["op"]](record) if record["op"] in REDUCED else record
           for record in copy.deepcopy(transcript)]
    identifiers: dict[str, BrokerIdentifiers] = {}
    for record in out:
        record.pop("mark", None)
        if record.pop("sort", False):
            record["got"].sort(key=packet_order)
        if scenario.closure == "any" and record.get("end") in CLOSED:
            record["end"] = "closed"
        ids = identifiers.setdefault(record.get("conn"), BrokerIdentifiers())
        for packet in record.get("got", ()):
            normalize_received(packet, buckets=scenario.buckets, capabilities=scenario.capabilities)
            if (packet["type"] == "PUBLISH" and packet.get("qos")) or packet["type"] == "PUBREL":
                raw = packet["packet_id"]
                ids.delivered(packet)
                if record["op"] == "drain" and packet["type"] == "PUBLISH" and packet["qos"] == 1:
                    ids.acknowledged(raw)
        sent = record.get("packet")
        if record["op"] == "send" and sent and sent["type"] in ("PUBACK", "PUBREC", "PUBCOMP"):
            ids.answered(sent)
    return out


def canonical(transcript) -> str:
    return json.dumps(transcript, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def same(left, right) -> bool:
    return canonical(left) == canonical(right)


def fingerprint(reference, broker) -> str:
    """A short name for one pair of normalized transcripts."""
    return hashlib.sha256((canonical(reference) + "\n" + canonical(broker)).encode()).hexdigest()[:16]
