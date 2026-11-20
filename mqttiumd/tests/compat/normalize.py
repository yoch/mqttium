"""Turn a raw transcript into the form the two brokers are compared in.

Kept as observed: packet types and their order per step, flags, QoS,
topics, payloads, reason codes, the packet identifiers the client chose
(and their echo in the broker's acknowledgements), property values,
whether a connection was closed.

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
- a closure by end of stream and one by reset are both `closed`
  (done by the player);
- no time is recorded;
- properties are a mapping, so their wire order is not compared; user
  properties keep their order, which the specification makes significant.
"""
from __future__ import annotations

import copy
import hashlib
import json

ASSIGNED = "<assigned>"

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

    def answered(self, packet: dict) -> None:
        """A PUBACK, PUBREC or PUBCOMP the client sent."""
        raw = packet["packet_id"]
        packet["packet_id"] = self.active.get(raw, raw)
        if packet["type"] != "PUBREC" or packet.get("reason", 0) >= 0x80:
            self.active.pop(raw, None)


def normalize_received(packet: dict, *, buckets: dict, capabilities: bool) -> None:
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


def normalize(transcript, scenario) -> list:
    """A deep copy of `transcript` in comparable form."""
    out = copy.deepcopy(transcript)
    identifiers: dict[str, BrokerIdentifiers] = {}
    for record in out:
        ids = identifiers.setdefault(record.get("conn"), BrokerIdentifiers())
        for packet in record.get("got", ()):
            normalize_received(packet, buckets=scenario.buckets, capabilities=scenario.capabilities)
            if (packet["type"] == "PUBLISH" and packet.get("qos")) or packet["type"] == "PUBREL":
                ids.delivered(packet)
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
