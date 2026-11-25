# 0063 — QoS 0 is paced per connection

Status: **Proposed** (23 November 2026). Applied to `mqttiumd` (core tier,
ADR 0044). It follows report 0037, which found that a subscriber that
reads could lose the end of a QoS 0 burst, and the decision of the
repository owner to correct the broker.
[Report 0038](../reports/0038-qos0-paced-per-connection.md) holds the
evidence.

No public type, input, effect, drop reason, durable encoding, injection
point or schema changes. In the runtime one constant changes: the output
queue of a connection has 256 places instead of 64. The contract digest
that `validate` prints moves only because the register lists this record
and the records it amends.

## Context

The output queue of a connection held 64 frames. An acknowledgement or a
QoS 1 or 2 publication that finds it full stays with the owner and leaves
when the queue has room. A QoS 0 frame that finds it full was dropped
(`output_saturated`): records 0026 and 0046 decided that drop for a
subscriber the broker cannot write to fast enough.

But the owner never waited for that queue. It emitted every volatile
delivery it held, one per step of its continuation, and the runtime runs
that continuation ahead of every other input. A burst was handed whole to
the queue. Whether the I/O thread of the subscriber drained it meanwhile
was a matter of scheduling: the wake-ups of the I/O threads are held during
a turn of the core. With one I/O thread, a subscriber that reads received
300 to 450 of a thousand messages written at once by eight publishers, in
ten plays of ten.

The same happened to durable publications delivered to a QoS 0
subscription. Such a delivery is consumed in the store before its frame is
emitted; a frame dropped for a full queue was a message the store no longer
owed.

## Decision

### A window of volatile frames per connection

The owner lets one connection have at most `QOS0_WINDOW` = 224 volatile
QoS 0 frames emitted and not yet written, failed or refused. The output
queue of a connection grows to 256 places (`net::OUTPUT_SLOTS`), so
thirty-two stay for acknowledgements and QoS 1 and 2 publications.

The sizes come from a measurement (report 0038). The ends of writes reach
the owner once in a turn of the runtime, and one publisher adds up to
thirty-two publications in a turn. A window of thirty-two on a queue of
sixty-four, the first form of this record, emitted at most what arrived:
a connection that fell one turn behind never caught up, its deliveries
piled up to the bound below and were dropped. A stream of bursts then
lost nine messages in a hundred, and a continuous one almost half, where
the broker before this record lost a few in a thousand. The window must
be several times what a turn adds.

- A delivery whose connection has its window full **waits**, in a queue of
  that connection. Once one waits, every later delivery of the connection
  waits behind it: a subscriber receives the publications in their order.
- The end of a frame (`FrameResult`, or the refusal of its emission)
  returns a place. If a delivery waited for it, the connection is served
  by the continuation: the one that runs, or a new one. Nothing runs while
  every connection that has deliveries waiting has its window in flight.
- Connections that can be served are served one delivery in their turn. A
  connection that waits costs the others nothing.

### What still drops, and with which reason

- A connection keeps at most `QOS0_PARKED` = 1,024 deliveries waiting. Past
  it the newest is dropped with `output_saturated`; those that wait keep
  their place.
- Deliveries that wait never hold more than half of the volatile queue or
  of its payload, whatever the number of connections: the publications of
  clients whose subscribers read are not refused for those that do not.
- A whole fan-out that does not fit the volatile queue is dropped with
  `capacity_exceeded`, as before.
- A subscriber that does not read is closed by the write deadline of 250 ms
  of its oldest frame, as before. What waited for it ends with
  `connection_closed`.
- The runtime still drops a QoS 0 frame that finds the output queue full:
  it can happen when more than thirty-two frames of other kinds are in it.

### Consumed deliveries

A connection has at most `QOS0_CONSUMED_WINDOW` = 16 consumed QoS 0
deliveries being written. The next one is not consumed until a write
ends, which brings the work of the session back. The frame of a consumed
delivery therefore finds a place.

This amends records 0026 (output saturation is no longer met by a burst
alone) and 0046 (what it says of output: a QoS 0 publication now waits for
the output of a subscriber, up to a bound).

## Consequences

- A subscriber that reads receives a QoS 0 burst whole and in order, up to
  1,248 messages ahead of what it has been written.
- In the six shapes report 0038 measured, nothing was lost and the rate is
  that of the broker before this record or higher.
- A connection that does not read can have 256 frames in its output queue,
  where it had 64, until its write deadline closes it. The queue is
  allocated when it is used: an idle connection costs what it did.
- The readiness event reports `output_slots` 256.
- Connections that do not read can hold up to half of the volatile queue
  until their write deadline closes them; they held nothing before.
- The compatibility scenario `publish/burst-qos0-v4` compares its count
  again.

## Not chosen

- **The runtime keeps the refused frame for a retry**, as it does for an
  acknowledgement. The owner would go on emitting, and one stalled
  subscriber would fill the effect box every connection shares.
- **A larger output queue alone, or releasing the wake-ups during a
  turn.** Both make the loss rarer and remove nothing: the owner would
  still hand a burst whole to a queue that drops.
- **A window of fifty-six on the queue of sixty-four**, which leaves the
  runtime unchanged. Measured: about what the broker did before with one
  subscriber, less with eight (94.5 % received against 98.5 %).
- **A new input by which the runtime reports room.** The end of a write is
  already reported.

## Not done

- The window is a constant of the core and the size of the output queue a
  constant of the runtime: nothing ties them at compilation. The runtime
  test that holds the output of a subscriber is what checks them together.
- A retained replay to a QoS 0 subscription is a consumed delivery and is
  paced as one; no test plays it under a full queue.
- No model has the window.
- The TLS write path was not read again; the tests of the TLS listener
  cover it as before.

## Implementation amendment

The runtime now checks the equality of the core's volatile QoS 0 window
plus its 32 reserved output slots and the 256-slot socket queue at compile
time. A change to just one limit fails the build. The `ready` event reports
all three values.

The runtime test
`retained_qos0_replay_reports_the_full_output_and_reaches_an_independent_subscriber`
fills a subscriber's output with control frames, leaving its final slot for
SUBACK. It checks the resulting consumed QoS 0 replay's `output_saturated`
drop against that subscriber and observes the complete retained publication
on an independent subscriber while the first queue remains full.
