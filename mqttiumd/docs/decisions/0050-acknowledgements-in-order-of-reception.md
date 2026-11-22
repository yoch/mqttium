# ADR 0050 — The broker acknowledges in the order of reception

Status: **Accepted** (22 November 2026, by the repository owner). It adds
a variant to a public type of the core tier (`ExchangePhase::AnswerHeld`)
and changes when the core makes a PUBACK or a PUBREC eligible. It changes
no durable encoding and no schema. It amends record 0046, whose "Left
open" recorded the opposite behaviour as one a client may observe.

## Context

The core made the PUBACK or the PUBREC of a received publication eligible
in the transition that applied the result of its admission. The single
store writer answers in order, so the acknowledgements of a connection
left in the order of its publications, except when a result stayed
unknown and was resolved later: the acknowledgement of a later
publication then left first. The independent audit of `8e4303d` observed
8 then 7 for publications received as 7 then 8.

MQTT 3.1.1 states the order as rules of the client (MQTT-4.6.0-2 for
PUBACK, MQTT-4.6.0-3 for PUBREC) and extends "the rules listed above" to
a server "when delivering messages to each of its subscribers"
(MQTT-4.6.0-6). MQTT 5 states them for the client only. Whether they bind
a server for the publications it receives can be read both ways. Client
libraries are written against brokers that answer in order, and some
match acknowledgements to their oldest unacknowledged publication.

## Decision

**The PUBACK and PUBREC of a connection leave in the order in which the
broker received their PUBLISH packets, whatever the reading of the
statements.** The rule is the core's: every runtime built on it has it,
and the deterministic bench shows it.

**An answer that would overtake is held.** When the result of an
admission is applied and an earlier publication of the same connection is
not answered yet, the core consumes the result as before and keeps the
answer: the exchange is `AnswerHeld`. "Earlier" is the ingress order the
adapter gave each packet of the connection, which the exchange already
carries; an exchange read back from durable data, or whose PUBLISH came
on another connection, has no place in that order: it neither waits nor
makes another wait. Publications with the same ingress order do not wait
for each other.

**The PUBREC of a PUBLISH received again follows the same rule.** A
client that resumes its session sends its unacknowledged publications
again, in order. The PUBREC of one whose admission is already committed
waits for the answers of those sent again before it. Such an exchange
keeps its right to a PUBREL: the client may hold a PUBREC of an earlier
connection or of an earlier copy.

**A held answer leaves with the next thing that happens.** There is no
timer and no self-addressed input. The oldest held answer that nothing
precedes any more is made eligible by the transition that answers or
ends the exchange in front of it, by the receipt of any acknowledgement
written to its connection, or by a PUBLISH received again; one held
answer per transition, so that a transition keeps its bound of three
effects. Each answer released brings a receipt, which releases the next.

**A held answer does not outlive its connection.** When the connection
ends, a held PUBACK ends its exchange as a PUBACK that could not be
written does; a held PUBREC that accepts leaves its exchange waiting for
PUBREL, to be answered when the client sends the PUBLISH again; a held
PUBREC that refuses ends its exchange. This is what the core already did
for a result applied without a connection.

## Consequences

- With a result that stays unknown, the acknowledgements of the later
  publications of that connection wait for its resolution: seconds, the
  deadline of a resolution. Publications of other connections are not
  concerned, and deliveries to subscribers do not wait: they follow the
  commits (MQTT-4.6.0-5, -6).
- A held QoS 1 answer keeps its exchange credit until its PUBACK is
  written, as an unwritten PUBACK already did. A client that fills its
  Receive Maximum behind an unknown result waits with it (record 0046).
- `ExchangePhase` gains `AnswerHeld`. A PUBREL for such an exchange is
  the protocol error it is while the admission is pending, unless the
  PUBREC is one sent again.
- Record 0046, "Left open": the paragraph on the order of the broker's
  own PUBACK and PUBREC no longer describes the product.
- MQTT-4.6.0-2 and -3 of MQTT 3.1.1 are covered in the conformance
  register; no statement stays open there.

## Left open

- The order is that of one connection. Across a takeover by another
  connection of the same client, each connection has its own order.
- The scenarios of the bench that hold two exchanges of one client keep
  their results in order; the rule is shown by tests of the core and of
  the runtime, and by one scenario if the vocabulary allows it (see the
  open conditions).
