# Protocol conformance

What MQTTium has been *verified* to satisfy in MQTT 3.1.1 and 5.0, how it was
verified, and what has not been checked. The source material is the vendored
statement index in [`docs/spec/`](spec/README.md), extracted from the numbered
OASIS conformance statements with reproducible archive provenance.

This is a contract in the sense of `docs/README.md`: it describes current
behaviour and must be updated by any change that contradicts it. It is **not** a
certification, and it deliberately does not claim full conformance — see
[Coverage](#coverage) for the honest boundary.

## Why the index exists

A conformance claim is only as good as its citation. While fixing an inbound
packet-identifier collision, the governing statement was cited from memory as
`[MQTT-2.2.1-3]`; that is the *Client's* obligation to allocate an unused
identifier, whereas the peer at fault was the broker, making `[MQTT-2.2.1-4]`
the governing statement. The error was caught only because the citation was
questioned.

Two properties of the specifications make this easy to get wrong, and both are
now handled mechanically rather than by care:

- **A bare label is ambiguous across versions.** 119 labels appear in both
  documents and **112 of them say different things**. `[MQTT-3.8.3-2]` is "the
  Payload MUST contain at least one Topic Filter and Subscription Options pair"
  in 5.0, and a rule about wildcard support in 3.1.1. Always state the version.
- **Quotes drift.** `tests/unit/test_conformance_statements.py` re-checks every
  `[MQTT-…]` quotation in its own docstrings against the extracted text, and
  fails on a label that exists in neither document. A test cannot claim to
  enforce a statement it misquotes.

## Method

Conformance is asserted three ways, in decreasing order of strength:

1. **Executable checks** — `tests/unit/test_conformance_statements.py` builds
   the wire condition and asserts the observable behaviour, naming the statement
   it exercises. This is the only category that constitutes evidence.
2. **Behavioural suites** — the existing unit, fuzz and integration suites cover
   large parts of the protocol (QoS 1/2 flows, session state, keepalive,
   reconnect, topic matching, the MQTT 5 property table) without citing
   statement numbers. `tests/resilience/test_hostile_broker.py` additionally asserts
   the client survives malformed and hostile input from a peer.
3. **Reading** — the areas surveyed while auditing, recorded below with their
   result but without a dedicated test.

## Result

### Gaps found and fixed

Five of the six are in what MQTTium **sends**, and that asymmetry is
structural rather than accidental: inbound frames pass through one validating
decoder — which refused every one of the nine malformed shapes probed — whereas
outbound rules live at the call sites that build packets, where a rule specific
to a direction or a protocol version has nowhere central to be enforced. One is
a peer-behaviour check that was simply absent.

**`[MQTT-3.1.3-7]` (MQTT 3.1.1)** — *"If the Client supplies a zero-byte
ClientId, the Client MUST also set CleanSession to 1."*

MQTTium sent that combination. The broker is required to answer `0x02`
(Identifier rejected) and close ([MQTT-3.1.3-8]), so the connection could never
succeed — a durable session is keyed by the client identifier, which an empty
one cannot provide. `begin_connect` refuses it, checking the *effective* Clean
Start: resuming a session rewrites it to 0, which is exactly when this would
otherwise slip through. MQTT 5 allows the pairing and assigns an identifier.

Four persistence tests were constructing this impossible configuration and had
to be given a client identifier, which is the more realistic setup anyway.

**`[MQTT-3.15.2-1]` / `[MQTT-4.12.0-3]` (MQTT 5.0)** — *"The sender of the AUTH
Packet MUST use one of the Authenticate Reason Codes"*, and *"The Client
responds to an AUTH packet from the Server by sending a further AUTH packet.
This packet MUST contain a Reason Code of 0x18 (Continue authentication)."*

`queue_auth()` accepted any reason code the AUTH encoder considered valid,
including `0x00` (Success) — which Table 3-11 assigns to the **Server**. Only
`0x18` and `0x19` may come from a Client, and `queue_auth` now enforces that.

This one was encoded in MQTTium's own test suite:
`test_async_client_auth_handler_exchange` had the client's auth handler answer a
server challenge with `0x00`, so the non-conformant flow was the one being
asserted. The test now answers `0x18`, which is what `[MQTT-4.12.0-3]` requires.

**MQTT 5 §3.14.2.2.2** — *"If the Session Expiry Interval in the CONNECT packet
was zero, then it is a Protocol Error to set a non-zero Session Expiry Interval
in the DISCONNECT packet sent by the Client."* (Prose, not a numbered
statement.)

`begin_disconnect` accepted and encoded it. A broker can treat the invalid
DISCONNECT as ungraceful, which may publish a configured Will — the opposite of
what a clean shutdown intends. An absent interval in CONNECT means zero
(§3.1.2.11.2), so both the absent and explicit-zero cases are refused, while a
session that really was durable can still have its expiry adjusted. Validation
uses the value actually encoded in CONNECT, not an application-owned
`Properties` object that may have been mutated since.

**`[MQTT-3.2.2-1]` (MQTT 3.1.1) / `[MQTT-3.2.2-2]` (MQTT 5.0)** — after
accepting CleanSession/Clean Start 1, the Server must set Session Present to 0.

A broker answering `session_present=1` to a Clean Start connection was accepted
and the session continued, leaving the two sides disagreeing about what state
exists. `_on_connack` now tears the connection down with `0x82`.

This applies even if SQLite still contains inflight records. Local stale state
cannot turn a clean connection into a resumed one; replaying it would contradict
the flag the Client sent. The previous implementation inferred the MQTT Session
State concept from publication rows alone, although broker-side subscriptions
are also Session State and cannot be discovered through that store. Resume tests
now request a non-clean connection explicitly.

**`[MQTT-3.1.2-22]` (MQTT 3.1.1)** — *"If the User Name Flag is set to 0, the
Password Flag MUST be set to 0."*

MQTTium sent a CONNECT with the password flag set and the username flag clear
whenever a password was configured without a username, which a 3.1.1 broker may
reject or misparse. MQTT 5 lifted the restriction, so `begin_connect` now
refuses the combination for 3.1.1 only rather than degrading silently. Note the
label means something entirely different in the 5.0 document — see the
version-ambiguity warning above.

**`[MQTT-3.3.4-6]` (MQTT 5.0)** — *"A PUBLISH packet sent from a Client to a
Server MUST NOT contain a Subscription Identifier."*

MQTTium accepted `subscription_identifier` on an outbound `publish()` and put it
on the wire. The MQTT 5 property table in `codec/properties.py` validates
properties *per packet type*, and the identifier is legal on the PUBLISH the
broker sends us — the restriction is on the direction, which a packet-type table
structurally cannot express. Rejected now in
`OutboundSession._validate_publish_request`, the single choke point every
publish entry point passes through, so it fails before anything is queued or
encoded. The inbound direction is unchanged and covered by its own test.

### Verified by executable check

| Statement | Version | Subject |
| --- | --- | --- |
| `MQTT-3.1.3-7` | 3.1.1 | An empty ClientId requires a clean session |
| `MQTT-1.5.3-1` / `-2` | 3.1.1 | CONNECT strings reject U+0000 and ill-formed UTF-8 |
| `MQTT-3.1.3-1` | 3.1.1 | CONNECT payload field order |
| `MQTT-3.15.2-1` | 5.0 | A Client cannot send a Server-only AUTH reason code |
| `MQTT-4.12.0-3` | 5.0 | A Client answers a Server AUTH with 0x18 |
| §3.14.2.2.2 | 5.0 | DISCONNECT cannot extend a session that was never durable |
| `MQTT-3.14.4-1` | 5.0 | Nothing is sent after DISCONNECT |
| `MQTT-4.3.3-6` | 5.0 | Replay sends PUBREL, never the PUBLISH again |
| `MQTT-4.9.0-3` | 5.0 | A full send quota does not delay SUBSCRIBE or PINGREQ |
| `MQTT-4.7.3-2` / `-3` | 5.0 | Null character and 65,535-byte limits on topics and filters |
| `MQTT-4.8.2-2` | 5.0 | ShareName rules for `$share/` filters |
| `MQTT-3.2.2-1` / `-2` | 3.1.1, 5.0 | Session Present after a clean connection closes the connection |
| `MQTT-3.3.2-8` | 5.0 | Topic Alias 0 is refused |
| `MQTT-3.15.1-1` | 5.0 | Reserved AUTH fixed-header bits |
| `MQTT-4.6.0-2` | 5.0 | PUBACK order follows PUBLISH arrival order |
| `MQTT-3.1.2-22` | 3.1.1 | No password flag without a username flag (and MQTT 5 still allows it) |
| `MQTT-3.1.2-21` | 5.0 | Server Keep Alive replaces the requested value |
| `MQTT-3.3.2-2` | 5.0 | No wildcard in a PUBLISH topic name |
| `MQTT-3.3.4-6` | 5.0 | No Subscription Identifier on an outbound PUBLISH |
| `MQTT-3.8.3-2` | 5.0 | SUBSCRIBE carries at least one filter |
| `MQTT-3.3.1-4` | 3.1.1, 5.0 | PUBLISH must not set both QoS bits |
| `MQTT-3.3.1-2` | 3.1.1, 5.0 | QoS 0 PUBLISH must not set DUP |
| `MQTT-2.1.3-1` | 5.0 | Reserved fixed-header flags, over PUBACK, PUBREL, SUBACK, PINGRESP and DISCONNECT |
| `MQTT-3.14.1-1` | 5.0 | Reserved bits on DISCONNECT |
| `MQTT-2.2.1-3` / `-4` | 5.0 | Receiving mirror: a QoS > 0 PUBLISH with identifier 0 is refused |

Receiver-side validation was probed across nine malformed-frame shapes and
refused every one, answering with a protocol error rather than a crash or a
silent accept.

### Surveyed by reading, no dedicated statement test

Recorded as observations, not as evidence. All were probed and held:

- **CONNECT construction** — Will Flag 0 forces Will QoS and Will Retain to 0
  and omits the will payload; a configured will encodes its QoS and retain
  faithfully; credential flags always describe the payload that follows them.
- **Nothing but AUTH or DISCONNECT reaches the wire before CONNACK**
  (`[MQTT-3.1.2-30]`). A QoS 1 publish issued while connecting is admitted to
  the offline queue but emits no frame until the session is established; QoS 0,
  SUBSCRIBE and PINGREQ are refused outright.
- **Topic and filter validation** — wildcards refused in a publish topic,
  zero-length topics and filters refused, `sport/#/x`, `sport#` and `sp+rt/a`
  refused while `sport/+/x`, `sport/#`, `+/tennis/#`, `#` and `+` are accepted.
- **UNSUBSCRIBE requires at least one filter.**
- **Acknowledgement reason codes** — every PUBACK, PUBREC, PUBREL and PUBCOMP
  MQTTium emits carries a value defined for that packet, including the `0x92`
  it answers an orphan PUBREC with.
- **SUBACK reason codes** are surfaced in the order of the filters that were
  subscribed.
- **Packet identifier ownership.** An inbound PUBLISH whose identifier is still
  held by an unfinished exchange of the other QoS is refused with `0x82`
  (`protocol/inbound.py`). The identifier is provably still the broker's:
  reuse is permitted only once the sender has processed the acknowledgement,
  and a record in `WAIT_PUBREL` or `WAIT_USER_ACK` proves no PUBCOMP was seen.
- **Topic and filter rules** (`topics.py`) — wildcard placement, zero-length
  names, `$`-prefixed topics, UTF-8 validation.
- **Property table** (`codec/properties.py`) — per-packet-type allowlists,
  duplicate detection, `subscription_identifier` cardinality on SUBSCRIBE.
- **Negotiation** (`protocol/negotiated.py`) — `maximum_qos`, `retain_available`,
  `maximum_packet_size`, `topic_alias_maximum`, `receive_maximum`, all enforced
  before a publish is admitted rather than by silent degradation.
- **No in-session retransmission** — PUBLISH/PUBREL are replayed only on
  reconnect with a present session, which is the behaviour MQTT 5 expects of a
  client that is not using a retransmit timer.

## Coverage

The two documents contain **390 numbered conformance statements** (139 in 3.1.1,
251 in 5.0). The MQTT 5 appendix typo documented in [`spec/`](spec/README.md)
does not create a second rule. A crude attribution pass puts roughly **290** of
them within reach of a client implementation; the remainder bind the Server
only.

Of those, the statements above are individually verified — §4 (operational
behaviour: QoS flows, ordering, flow control, topics, shared subscriptions) and
the CONNECT properties were swept systematically and, apart from the DISCONNECT
session-expiry rule, held throughout. **The rest are not individually
audited.** Much of the protocol is exercised by the behavioural
suites and by live interoperability against Mosquitto, and no counter-example is
known — but "no known counter-example" is not the same as conformance, and this
document does not present it as such.

The honest summary: six violations were found and fixed, five of them in what
the client sends; the receiver-side validation that was probed held up without
exception; and the remaining statements are covered by behaviour rather than by
citation. The discovery rate is not yet zero, so no claim of full conformance is
made.

## Extending this

Add a test to `tests/unit/test_conformance_statements.py` whose docstring opens
with the statement label followed by its verbatim text; the self-check will
verify the quote and reject a label that does not exist. Then add a row above.

Regenerate the index with `python tools/extract_spec_statements.py` when OASIS
publishes a new archive, and re-read this file against the new statement set.
