"""Vocabulary of the scenarios: steps over named raw connections.

A scenario is a list of steps. A step names a connection (`"a"`, `"pub"`,
`"sub"` ...) and does one thing: open it, send bytes, read packets, state
that nothing arrives, wait for the broker to close, close it, sleep. The
helpers below build the steps; the longer ones (`connect`, `subscribe`,
`publish_ack` ...) expand to the primitive steps they name.

Nothing here talks to a broker: `harness.Player` plays the steps.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import mqttwire as wire

AREAS = ("connect", "session", "subscribe", "publish", "retained", "properties", "will", "errors")
VERSIONS = {"v4": "3.1.1", "v5": "5.0"}

# An expected packet is waited for this long. "Nothing arrives" is stated
# over QUIET seconds after a PINGREQ/PINGRESP round trip on the connection.
WAIT = 3.0
QUIET = 0.3
# The silent window starts again at each packet; a connection on which
# packets keep coming is given up after LIMIT seconds.
LIMIT = 10.0
# A `drain` gives up when nothing has come for IDLE seconds, or after
# DRAIN seconds in all.
IDLE = 1.0
DRAIN = 15.0


@dataclass(frozen=True)
class Scenario:
    name: str
    area: str
    version: str
    doc: str
    steps: tuple
    # Extra global lines of the reference's configuration file, for a
    # scenario that needs the two brokers to announce the same limit.
    reference_config: tuple = ()
    # {property name: [edges]}: a time-dependent numeric property is
    # replaced by the interval of `edges` it falls in (normalize.bucket).
    buckets: dict = field(default_factory=dict)
    # Keep the capabilities advertised in a MQTT 5 CONNACK (normalize.py).
    capabilities: bool = False
    # `exact`: a closure by end of stream and one by reset differ. `any`:
    # they compare equal, for a scenario in which the broker closes while
    # the client is still writing, so that the kind depends on timing.
    closure: str = "exact"
    # What the specification requires of mqttiumd here (expectations.py).
    expectations: tuple = ()
    reference_persistence: str = "none"


REGISTRY: dict[str, Scenario] = {}


def scenario(name, *, doc, steps, version=None, reference_config=(), buckets=None, capabilities=False,
             closure="exact", expectations=(), reference_persistence="none") -> Scenario:
    """Register a scenario.

    `name` is `area/what-vN`: the suffix `-v4` or `-v5` gives the protocol
    version every connection of the scenario speaks. A scenario that mixes
    versions, or speaks neither, states `version` itself.

    `expectations` are built with `expectations.expectation`; each reads
    records that the steps name with `mark`.
    """
    if closure not in ("exact", "any"):
        raise ValueError(f"{name}: closure is `exact` or `any`")
    area, _, rest = name.partition("/")
    if area not in AREAS or not rest:
        raise ValueError(f"{name}: unknown area")
    if name in REGISTRY:
        raise ValueError(f"{name}: defined twice")
    if version is None:
        version = VERSIONS.get(name.rsplit("-", 1)[-1])
        if version is None:
            raise ValueError(f"{name}: no -v4 or -v5 suffix and no version")
    flat = []

    def flatten(items):
        for item in items:
            if isinstance(item, list):
                flatten(item)
            elif item["op"] == "mark":
                # An `open` that succeeds leaves no record to name.
                if not flat or flat[-1]["op"] == "open" or "mark" in flat[-1]:
                    raise ValueError(f"{name}: mark `{item['name']}` does not follow a step with a record of its own")
                if any(step.get("mark") == item["name"] for step in flat):
                    raise ValueError(f"{name}: mark `{item['name']}` is used twice")
                flat[-1] = {**flat[-1], "mark": item["name"]}
            else:
                flat.append(item)

    flatten(steps)
    if reference_persistence not in ("none", "builtin", "sqlite"):
        raise ValueError(f"{name}: unsupported reference persistence {reference_persistence!r}")
    if any(step["op"] == "restart" for step in flat) and reference_persistence == "none":
        raise ValueError(f"{name}: restart requires reference persistence")
    marks = {step["mark"] for step in flat if "mark" in step}
    expectations = tuple(expectations)
    if len({item.name for item in expectations}) != len(expectations):
        raise ValueError(f"{name}: two expectations have one name")
    for item in expectations:
        if set(item.marks) - marks:
            raise ValueError(f"{name}: expectation `{item.name}` reads records that no step marks: "
                             f"{sorted(set(item.marks) - marks)}")
    entry = Scenario(name=name, area=area, version=version, doc=" ".join(doc.split()), steps=tuple(flat),
                     reference_config=tuple(reference_config), buckets=buckets or {}, capabilities=capabilities,
                     closure=closure, expectations=expectations, reference_persistence=reference_persistence)
    REGISTRY[name] = entry
    return entry


def persistent(level, expiry=3600) -> dict:
    """CONNECT options that ask for a session kept after the connection."""
    if level == 4:
        return {"clean": False}
    return {"clean": False, "props": {"session_expiry_interval": expiry}}


# -- primitive steps --------------------------------------------------------

def restart():
    """Stop the broker with clients still connected and reopen the same data."""
    return {"op": "restart"}


def open_(conn, level=4):
    """Open the TCP connection `conn`; `level` is how its input is decoded."""
    return {"op": "open", "conn": conn, "level": level}


def send(conn, data, note=None):
    """Send bytes. `note` replaces the decoded packet in the transcript, for
    bytes that are not one well-formed packet."""
    step = {"op": "send", "conn": conn, "data": bytes(data)}
    if note is not None:
        step["note"] = note
    return step


def ack(conn, kind="puback", *, nth=-1, reason=None):
    """Answer a PUBLISH the broker sent on `conn` with its own identifier.

    `kind` is `puback`, `pubrec` or `pubcomp`. `nth` indexes the QoS 1 and
    QoS 2 PUBLISH packets received on the connection name so far, in order
    of arrival; the default is the latest. The broker chooses those packet
    identifiers, so a scenario never writes one down.
    """
    return {"op": "ack", "conn": conn, "kind": kind, "nth": nth, "reason": reason}


def recv(conn, count=1, *, timeout=WAIT, sort=False):
    """Read `count` packets, or record that they did not come in time.

    `sort` orders the packets read by this step when the two orders are
    both lawful and not what the scenario is about.
    """
    return {"op": "recv", "conn": conn, "count": count, "timeout": timeout, "sort": sort}


def quiet(conn, *, window=QUIET, ping=True, timeout=WAIT, sort=False, limit=LIMIT):
    """State that nothing more arrives; whatever does is recorded.

    The step sends PINGREQ, reads up to the PINGRESP and then until
    nothing has come for `window` seconds: the window starts again at each
    packet, so that an answer in several packets is read to its end
    however slow the machine is. A connection on which packets still come
    after `limit` seconds ends the step as `still-receiving`. `ping=False`
    is for a connection that cannot answer PINGREQ (not connected yet, or
    expected to be closed).
    """
    if limit < window:
        raise ValueError("the limit of a silent window is at least the window")
    return {"op": "quiet", "conn": conn, "window": window, "ping": ping, "timeout": timeout, "sort": sort,
            "limit": limit}


def collect(conn, *, sort=False, window=QUIET, timeout=WAIT, limit=LIMIT):
    """Read whatever the broker has for `conn`, when how many packets come
    is part of the question: the same round trip and window as `quiet`.

    Only where the count cannot depend on timing. What a broker may shed
    (QoS 0) is counted with `measure`; what it owes (QoS 1) is read with
    `drain`."""
    return quiet(conn, window=window, timeout=timeout, sort=sort, limit=limit)


def drain(conn, count, *, idle=IDLE, timeout=DRAIN, sort=False):
    """Read until `count` PUBLISH packets came, acknowledging each QoS 1
    PUBLISH with its PUBACK as it is read.

    For deliveries the broker owes whatever the timing: a subscriber that
    acknowledges is never the reason why one is missing, and a broker that
    sends only a window of unacknowledged messages at a time goes on. The
    step ends as `ok` at the last expected PUBLISH, as `idle` when nothing
    has come for `idle` seconds, as `timeout` after `timeout` seconds in
    all; every packet read is recorded. `sort` when the order of the
    deliveries is not the question (retained messages of several topics).
    Follow it with `quiet` to state that nothing comes twice.
    """
    return {"op": "drain", "conn": conn, "count": count, "idle": idle, "timeout": timeout, "sort": sort}


def measure(conn, sequence, *, kind="PUBLISH", field=None, ordered=True, window=QUIET, timeout=WAIT, limit=LIMIT):
    """Count the packets of type `kind` that arrive, where how many do is
    lawful either way and depends on timing: QoS 0 deliveries, which a
    broker may shed for a subscriber it cannot write to fast enough.

    The count is a measurement. It is kept in the raw transcript and
    printed in the matrix for both brokers, and never compared: a
    scenario does not fail because a machine was slow. What is compared
    is what the protocol guarantees whatever the count. `sequence` is
    what could lawfully arrive, as the counted packets show it in `field`
    (`payload` for PUBLISH, else `packet_id`): the transcript says whether
    each value that came is one of them and came once (`values`:
    `distinct`, `repeated`, `foreign`) and, with `ordered`, whether they
    came in the order of `sequence` (`order`, as for `tally`). `ordered`
    is False where no order is guaranteed, as between the retained
    messages of several topics. The same round trip and window as `quiet`.
    """
    if limit < window:
        raise ValueError("the limit of a silent window is at least the window")
    return {"op": "measure", "conn": conn, "kind": kind, "sequence": list(sequence),
            "field": field or ("payload" if kind == "PUBLISH" else "packet_id"), "ordered": ordered,
            "window": window, "timeout": timeout, "limit": limit}


def tally(conn, expected, *, kind="PUBLISH", sequence=None, field=None, window=QUIET, timeout=WAIT, limit=LIMIT):
    """Count the packets of type `kind` that arrive, and compare only whether
    all `expected` came (`all`), fewer (`fewer`) or more (`more`).

    For an exchange in which how many packets a broker lets through before
    it sheds or refuses depends on timing, and whether it sheds any does
    not. The same round trip and window as `quiet`; packets of another
    type are recorded as they are.

    `sequence` is what the client sent, in order, as the counted packets
    show it in `field` (`packet_id`, or `payload` for PUBLISH): the
    transcript then also says whether they came `as-sent`, `permuted`, or
    with a repeated or foreign value (`other`). The raw transcript keeps
    every packet.
    """
    step = {"op": "tally", "conn": conn, "expected": expected, "kind": kind, "window": window, "timeout": timeout,
            "limit": limit}
    if sequence is not None:
        step.update(sequence=list(sequence), field=field or ("payload" if kind == "PUBLISH" else "packet_id"))
    return step


def until_close(conn, *, timeout=WAIT):
    """Read until the broker closes the connection, or record it still open."""
    return {"op": "until_close", "conn": conn, "timeout": timeout}


def close(conn, *, wait_peer=0.0):
    """Close the TCP connection without DISCONNECT.

    With `wait_peer`, first wait that long for the broker to close its
    end; packets that arrive meanwhile are recorded.
    """
    return {"op": "close", "conn": conn, "wait_peer": wait_peer}


def sleep(seconds):
    return {"op": "sleep", "seconds": seconds}


def mark(name):
    """Name the record of the step before it, for the expectations of the
    scenario. After a composed step it names the record of its last
    primitive step (the `recv` of `connect`, `subscribe` or `publish_ack`).
    The name is kept in the raw transcript only: it changes nothing in
    what is played or compared."""
    return {"op": "mark", "name": name}


# -- composed steps ---------------------------------------------------------

def connect(conn, client_id, *, level=4, expect=1, **options):
    """Open `conn`, send CONNECT and read `expect` packets (the CONNACK,
    and with a larger `expect` what a resumed session sends after it)."""
    steps = [open_(conn, level), send(conn, wire.connect(client_id, level=level, **options))]
    if expect:
        steps.append(recv(conn, expect))
    return steps


def subscribe(conn, packet_id, filters, *, level=4, props=None, expect=1, sort=False):
    """Send SUBSCRIBE and read `expect` packets (the SUBACK, and with a
    larger `expect` the retained messages; `sort` when their order with
    SUBACK is not the question). A filter is `(filter, options byte)`,
    and a bare QoS is a valid options byte."""
    steps = [send(conn, wire.subscribe(packet_id, filters, level=level, props=props))]
    if expect:
        steps.append(recv(conn, expect, sort=sort))
    return steps


def publish(conn, topic, payload=b"", *, level=4, **options):
    """Send PUBLISH without reading anything."""
    return send(conn, wire.publish(topic, payload, level=level, **options))


def publish_sync(conn, topic, payload=b"", *, level=4, **options):
    """Send a QoS 0 PUBLISH, then PINGREQ and read the PINGRESP: the broker
    has read the message when the next step starts."""
    return [send(conn, wire.publish(topic, payload, level=level, **options)), send(conn, wire.pingreq()), recv(conn)]


def publish_ack(conn, topic, payload=b"", *, packet_id=1, level=4, **options):
    """Send a QoS 1 PUBLISH and read its PUBACK."""
    return [send(conn, wire.publish(topic, payload, qos=1, packet_id=packet_id, level=level, **options)),
            recv(conn)]


def publish_qos2(conn, topic, payload=b"", *, packet_id=1, level=4, **options):
    """Play a complete incoming QoS 2 exchange: PUBLISH, PUBREC, PUBREL, PUBCOMP."""
    return [send(conn, wire.publish(topic, payload, qos=2, packet_id=packet_id, level=level, **options)),
            recv(conn), send(conn, wire.pubrel(packet_id, level=level)), recv(conn)]


def disconnect(conn, *, level=4, **options):
    """Send DISCONNECT, give the broker the time to close its end, close.

    The wait orders this disconnection before the next step for the
    broker too. Whether the broker closes is recorded by the scenarios
    `connect/disconnect-*` only.
    """
    return [send(conn, wire.disconnect(level=level, **options)), close(conn, wait_peer=WAIT)]
