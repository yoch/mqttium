# 0060 — The reader goes on behind a publication that waits for a slot

Status: **Accepted with amendments** (25 November 2026, by the integration owner). Applied to `mqttiumd` (runtime
tier, ADR 0044). It follows the external audit of `47e1d12`, whose verdict
on upstream Mosquitto ticket 3809 was partial; the test that completed it
found the defect this record fixes.
[Report 0033](../reports/0033-delivery-and-acknowledgement-obligations.md)
holds the evidence. No public type of the core tier, durable encoding,
injection point or schema changes.

Amended by 0061 (accepted), which decides the point this record left
open: a session holds at most eight inbound QoS 2 exchanges and the table
is sized for the sessions.

## Context

A connection has a window of eight inbound exchanges (`receiving::MAXIMUM`).
MQTT 5 announces it as Receive Maximum 8, and a ninth exchange is a
violation, answered with DISCONNECT 0x93. MQTT 3.1.1 has nothing to
announce: a burst beyond the window is pressure, never a violation (record
0046). The runtime holds the PUBLISH that has no slot (`deferred_publish`)
and, until this record, stopped reading the connection.

At QoS 1 the broker's own PUBACK frees the slot. At QoS 2 the slot is freed
by the PUBCOMP, which needs the client's PUBREL, and that PUBREL is behind
the held PUBLISH in the client's stream. A 3.1.1 client with nine QoS 2
exchanges in flight therefore received eight PUBREC and nothing more,
until Keep Alive ended the connection. Common client libraries keep ten to
twenty exchanges in flight by default.

Record 0046 closed the same cycle for a client that respects an announced
window. Its list of open points keeps: "an acknowledgement behind a held
publication of its own connection waits with it".

## Decision

While a publication waits for a slot, the runtime goes on reading its
connection.

- **The PUBREL of another exchange goes ahead.** It completes an exchange
  and frees a slot. A PUBREL commutes with every packet of its connection
  except the PUBLISH of its own identifier: the publication was routed
  when it was acknowledged with PUBREC, and the PUBREL only releases the
  exchange.
- **Every other packet keeps its place.** It waits behind the publication,
  in order: a PUBLISH of any QoS, a SUBSCRIBE, an UNSUBSCRIBE, a PINGREQ, a
  DISCONNECT, an acknowledgement of a delivery, and a PUBREL whose
  identifier is the one of the waiting PUBLISH or of a PUBLISH kept behind
  it. The order of the publications of a topic is the order of reception
  (MQTT-4.6.0-5 in MQTT 5, -6 in 3.1.1).
- **The wait is bounded.** At most one lane of packets (`INPUT_SLOTS`, 32)
  and the bytes of one packet of the largest size are kept behind. Past
  that bound the reader keeps the next packet at the head of the transport
  work. A packet that brings the prefix exactly to its bound is accepted.
  An independent PUBREL at the head can still pass.
- **Order of taking.** When the publication gets its slot, the packets kept
  behind it are taken first, then the held packet, then the lane. A packet
  kept behind that is a PUBLISH can wait for a slot in its turn.

A 3.1.1 client can therefore have 41 small QoS 2 exchanges in flight:
the window of eight, the waiting publication and thirty-two packets behind
it. Large packets can fill the byte budget earlier.

This amends record 0046: the cycle is closed for a client that was
announced no window, up to the bound above.

## Consequences

- A 3.1.1 publisher that keeps up to 41 QoS 2 exchanges whose prefix fits both budgets completes
  them all, and its subscribers receive the messages once and in order.
- A connection can hold thirty-two more packets, and the bytes of one
  largest packet, than its lane already could.
- MQTT 5 retains its Receive Maximum and DISCONNECT 0x93.

When delivery shards are enabled, record 0066 assigns subscriber PUBACK,
PUBREC and PUBCOMP to the delivery owner for that connection generation.
These acknowledge traffic in the opposite direction. The coordinator's
bounded reader still orders incoming publications, their PUBREL and the
rest of its lane as above. [Delivery owner routing and close fences](0066-lot10-delivery-owners-sharding.md)

## Amendment for a saturated MQTT 3.1.1 lookahead

When the next non-overtaking packet would exceed either prefix budget, the
reader waits for local work that can return a slot. A pending admission,
PUBACK, PUBREC write or an already observed PUBREL keeps that possibility
open. The reader does not repeatedly dequeue the same held packet as progress.

If all eight exact PUBLISH occurrences have a written successful PUBREC and
none has an observed PUBREL, only a subsequent client packet can release a
slot. The reader can no longer reach it. The broker closes this connection
with local event reason `receive_lookahead_capacity`. MQTT 3.1.1 sends no
server DISCONNECT. A persistent session retains its admitted exchanges for
PUBREL after reconnect. The client can repeat a PUBLISH that had received
no PUBREC once a slot is available. Other connections keep their own budgets.
MQTT 3.1.1 §4.8 explicitly names an internal buffer full condition as
an example of a transient error preventing packet processing. In that
case [MQTT-4.8.0-2] requires closing the affected network connection.
Ordinary bursts that fit the budgets continue through the normal path.

## Open: one session can hold every exchange

Closed by record 0061. What follows is the state this record left.

Found by the same tests, not fixed here. An inbound QoS 2 exchange at
PUBREC keeps its lease of the core's exchange table until its PUBREL. The
window bounds a connection, not a session: a session that reconnects
leaves the exchanges of its earlier connections at PUBREC and gets a new
window. The table has `max(128, connections × 8)` entries.

With `--max-connections 16`, one 3.1.1 session that leaves eight exchanges
at PUBREC on each of sixteen connections holds all 128 entries. From then
on a QoS 1 or QoS 2 PUBLISH of any other client is not acknowledged and
its connection is closed after five seconds (`input_capacity`), for as
long as that session lives. With the default limit the table has 32,768
entries and the same takes 4,096 connections.

A bound per session belongs to the admission of the core, with a table
sized for the sessions; it is a change of the core tier and of the
capacities of record 0048, and is left to a decision.

## Not done

- A DISCONNECT kept behind a publication that never gets its slot is not
  applied. The Will follows the eventual transport close, including a
  close at the lookahead bound.
- `MReadBehind` checks ordering, bounded reads and progress with a local or remote release in [the flow-pressure models](../../models/flow-pressure/README.md).
