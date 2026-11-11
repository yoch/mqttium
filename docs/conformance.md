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

Six of the seven are in what MQTTium **sends**, and that asymmetry is
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
uses the value actually encoded on the wire in CONNECT, rather than retaining
the caller's configuration state.

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

**Deliberate reading: a new Session and unacknowledged QoS 1** —
`[MQTT-3.1.2-6]` (MQTT 3.1.1): *"State data associated with this Session MUST
NOT be reused in any subsequent Session"*; `[MQTT-3.1.2-4]` and
`[MQTT-3.2.2-5]` (MQTT 5.0): the Client *"MUST discard"* its Session State.

When the Server reports no Session (Clean Start, or Session Present 0), the
Client discards its QoS 2 exchanges, which fail, and everything it held about
inbound exchanges. An unacknowledged QoS 1 Application Message is not
dropped: it is published again with DUP 0, as a new message, under the
Packet Identifier it already had. MQTTium reads the statements as covering
the Session's record of an exchange, not the application's message, and
prefers a possible duplicate, which QoS 1 allows, to a lost message. A
stricter reading would count the retained identifier as reused state. The
tests of these three statements assert this behaviour; they pin the reading,
they do not settle it.

**Deliberate deviation: `[MQTT-3.2.2-4]` (MQTT 5.0)** — *"If the Client does
not have Session State and receives Session Present set to 1 it MUST close the
Network Connection."*

MQTTium accepts Session Present whenever it sent Clean Start 0, even when it
holds no incomplete QoS exchange. A restarted process that asks to resume its
durable Session has, by definition, lost the in-memory knowledge of it; the
broker-side subscriptions it asked to keep are exactly what Session Present
reports. Closing the connection, as the letter of the statement requires, made
every durable subscriber fail on its first connection after a restart. MQTT
3.1.1 has no equivalent statement, and Session Present after Clean Start 1 is
still refused as above.

Persisted Client Session State includes every stored inbound row, a QoS 1 row
too. MQTT 5 section 4.1 lists only inbound QoS 2, but a stored QoS 1 row
(manual acknowledgement, or one recovered into an automatic session) completes
only when the broker resends its PUBLISH on a resumed session. Rejecting
Session Present for it would reject every reconnect and strand the row.

**`[MQTT-3.1.2-23]` (MQTT 3.1.1)** — *"It is the responsibility of the Client
to ensure that the interval between Control Packets being sent does not exceed
the Keep Alive value."*

The keep-alive loop sent PINGREQ once the Keep Alive had elapsed since the
last packet, so the PINGREQ left a few milliseconds (2 to 7, measured) after
the interval. No Server disconnects for that, since its own limit is one and a
half times the Keep Alive, but the interval was exceeded. The PINGREQ now
leaves a margin before the interval ends: a tenth of the Keep Alive, at most
one second.

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

The statements a test verifies are listed, per specification version, in the
generated [conformance matrix](conformance-matrix.md). It replaces the table
this page used to keep by hand, which had drifted from the tests: a statement
is listed there only when a test's docstring quotes it in full and the
quotation matches the vendored text of that version.

What "verified" means is narrow. The matrix establishes that a collected,
unskipped test carries the statement's exact words; it cannot establish that
the test's assertions cover every obligation in them. A test named
`test_mqtt311_…` or `test_mqtt5_…` counts for that version only. Statements
that bind the Server are classified as such and are not counted for the
client, even where a test checks how the client reacts to a Server that
breaks them.

Receiver-side validation was probed across nine malformed-frame shapes and
refused every one, answering with a protocol error rather than a crash or a
silent accept.

### Surveyed by reading, no dedicated statement test

Recorded as observations, not as evidence. All were probed and held:

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
  Automatic QoS 1 and QoS 2 exchanges keep the identifier until their
  acknowledgement is handed to the writer. After that handoff, and after a
  manual `ack()`, a broker that reuses the identifier before receiving the
  acknowledgement is not detected (#541); a conforming broker never does so.
- **Topic and filter rules** (`topics.py`) — wildcard placement, zero-length
  names, `$`-prefixed topics, UTF-8 validation.
- **Property table** (`codec/properties.py`) — per-packet-type allowlists,
  duplicate detection, `subscription_identifier` cardinality on SUBSCRIBE.
- **No in-session retransmission** — PUBLISH/PUBREL are replayed only on
  reconnect with a present session, which is the behaviour MQTT 5 expects of a
  client that is not using a retransmit timer.

## Coverage

The two documents contain **390 numbered conformance statements** (139 in 3.1.1,
251 in 5.0). The MQTT 5 appendix typo documented in [`spec/`](spec/README.md)
does not create a second rule. The same label often names a different
statement in each document, so coverage is counted per version.

[`spec/applicability.json`](spec/README.md) records which party each statement
binds. The [conformance matrix](conformance-matrix.md) gives, for the
statements that bind a client, how many a test verifies, how many are only
cited, and how many have no evidence, by section. Those generated figures
replace the earlier estimate of "roughly 290" statements within reach of a
client.

Of those, the statements above are individually verified — §4 (operational
behaviour: QoS flows, ordering, flow control, topics, shared subscriptions) and
the CONNECT properties were swept systematically and, apart from the DISCONNECT
session-expiry rule, held throughout. **The rest are not individually
audited.** Much of the protocol is exercised by the behavioural
suites and by live interoperability against Mosquitto, and no counter-example is
known — but "no known counter-example" is not the same as conformance, and this
document does not present it as such.

The honest summary: seven violations were found and fixed, six of them in what
the client sends; the receiver-side validation that was probed held up without
exception; and the remaining statements are covered by behaviour rather than by
citation. The discovery rate is not yet zero, so no claim of full conformance is
made.

## Extending this

Add a test to `tests/unit/test_conformance_statements.py` whose docstring opens
with the statement label followed by its complete verbatim text; the
self-check will verify the quote and reject a label that does not exist, and
a quotation that stops early is refused. Then run
`python tools/build_conformance_matrix.py`: the matrix is generated, and
`tests/project/test_conformance_matrix.py` fails while it is stale.

A statement naming both the Client and the Server needs an entry in
`docs/spec/applicability.json`; the generator refuses to guess. A statement
naming one party, or none, is classified by that mention without a warning,
which is wrong for passive constructions: read it and add an entry when the
party it names is not the one it binds.

Regenerate the index with `python tools/extract_spec_statements.py` when OASIS
publishes a new archive, and re-read this file against the new statement set.
