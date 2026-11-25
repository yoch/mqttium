# 0063 — QoS 0 is paced per connection

Status: **Accepted with amendments** (25 November 2026, by the integration owner).
Applied to the core and runtime (ADR 0044). Proposed on 23 November 2026. It follows report 0037, which found that a subscriber that
reads could lose the end of a QoS 0 burst, and the decision of the
repository owner to correct the broker.
[Report 0038](../reports/0038-qos0-paced-per-connection.md) holds the
evidence.

The core exports the existing consumed-delivery window and the contract
exposes the wire generation of an effect so the adapters can preserve its
order. The packet format, durable representation and QoS 0 handshake are
unchanged. The requirement register includes this decision.

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
- The runtime may drop an ordinary live QoS 0 frame when other kinds of
  traffic fill the output queue. The retained replay required on a new
  subscription follows the rule below.

### Consumed deliveries

A connection has at most `QOS0_CONSUMED_WINDOW` = 16 consumed QoS 0
deliveries being written. The next one is not consumed until a write
ends, which brings the work of the session back. Control traffic can fill
the socket queue even when the consumed-delivery window has room.

This amends records 0026 (output saturation is no longer met by a burst
alone) and 0046 (what it says of output: a QoS 0 publication now waits for
the output of a subscriber, up to a bound).

## Consequences

- A connection can have 224 volatile QoS 0 frames outstanding and 1,024
  waiting. The aggregate fanout and payload budgets still apply. Report
  0038 records the bursts observed within those limits.
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

## Implementation amendment

The runtime now checks the equality of the core's volatile QoS 0 window
plus its 32 reserved output slots and the 256-slot socket queue at compile
time. A change to just one limit fails the build. The `ready` event reports
all three values.

For a new MQTT 3.1.1 subscription, the server must send each matching
stored retained message. MQTT 5 applies this obligation with Retain
Handling 0, and with Retain Handling 1 on a new subscription. Handling 2
suppresses replay. QoS 0 uses one attempted send with no acknowledgement.
A full internal socket queue after a successful subscription still owes
that send. [MQTT 3.1.1 §3.3.1.3, MQTT-3.3.1-6](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html) · [MQTT 5 §3.3.1.3, MQTT-3.3.1-9 and -10](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)

The coordinator and each delivery shard keep a refused retained replay
on its original connection generation. They retry in FIFO order when
room becomes available. Later wire effects from the same owner on that
generation wait behind it. Coordinator and shard share the socket queue in
admission order. Each generation gets a turn, so a different subscriber can receive
its replay while the first output is full. The existing window of 16
bounds the number held per generation and their shared payload stays
charged until its write or terminal refusal. Closing an old generation
releases its pending send. A subsequent qualifying subscription obtains
its own replay from the retained store.

The coordinator test fills the output with control frames, admits SUBACK,
and checks 18 required replays for both the slow and the independent
subscriber. The two-shard test also closes and resumes the slow session,
checking the terminal outcome for the old generation. [Coordinator tests](../../crates/mqttiumd/src/runtime_tests.rs) · [Shard test](../../crates/mqttiumd/src/runtime_tests/sharded_retained.rs)

`MQos0Window` checks the per-generation window, FIFO order and progress
of the independent subscriber with a reduced queue. The executable load
profiles measure delivery over TCP and mTLS. [Model, assumptions and witnesses](../../models/flow-pressure/README.md)
