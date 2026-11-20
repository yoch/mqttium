# ADR 0046 — Input pressure slows a peer; it does not disconnect it

Status: **Proposed** (20 November 2026, runtime tier of record 0044). It
changes no public type of the core tier, no durable encoding and no
contract; the contract digest that `validate` prints does not move.

## Context

Two bounded waits of the first product adapter ended a connection under
input pressure:

- a transport task that found the four-slot input lane full for 100 ms
  closed its connection with `input_queue_full`;
- a packet the core refused with `NeedCapacity { EffectSlots }` closed its
  connection with `input_capacity`, "rather than reorder its positive
  acknowledgements".

With sixteen connections and the polling reactor neither was reached in
the receptions. With the connection limit of record 0044 raised to 128
they are: forty publishers bursting QoS 0 publications towards one
subscriber lose connections although nothing is wrong with them. The
reports of dropped QoS 0 publications fill the 128-slot effect box
between two dispatches, and the next publisher served is closed. A broker
that replaces Mosquitto cannot disconnect a sensor because a dashboard is
slow.

## Decision

**A packet the core cannot take yet waits, in order.** A packet refused
for effect slots is kept in the deferred inputs, as a packet refused for
result or receipt slots already was. The rule that keeps one connection
in order is unchanged: a later packet of a connection never passes an
earlier deferred one. The dispatch that follows the pass empties the
effect box; when it accepted something, the deferred packets return to
the head of the input lane.

**Deferred packets never take more than a quarter of the ordinary lane.**
The packets that release what a full box waits for (a PUBACK frees a
delivery window) arrive on the same lane. A packet refused for effect
slots while a quarter of the lane already waits is closed as before, with
`input_capacity`: the lane stays open for the packets that end the
pressure, and the broker stays live.

**The budgets sized for sixteen connections grow with the limit.** The
effect box, the ordinary input lane, the outgoing identifier bindings,
the timers and the terminal inputs are multiplied for every further
sixteen connections (`runtime::Limits`). At sixteen they are what every
earlier reception ran with.

**A full input lane is backpressure.** The task holds the one packet the
lane refused, reads nothing more from its socket, and is run again when
the core takes a value. It sets no deadline of its own. The peer is
slowed by TCP. Every deadline of a connection stays with the runtime:
the CONNECT deadline, Keep Alive, the partial-packet budget of a frame
that is being read, the write budget of a frame that is being written.

**Receipts keep their deadline.** A receipt the core does not take within
100 ms still ends the task (`write_result_queue_full`,
`wire_progress_unavailable`): a core that stopped taking receipts cannot
be told what was written, and that is a fault, not pressure.

## Consequences

- `input_queue_full` is no longer a terminal reason. `input_capacity`
  remains for a packet refused for a resource other than effect, result
  or receipt slots, and for a packet refused for effect slots beyond the
  quarter of the lane. Measured with 32 publisher and subscriber pairs
  over mTLS: at 1,500 QoS 1 publications per second offered nothing is
  closed; at 3,000, beyond what the store commits, 12 of 64 connections
  are.
- The net test that required the closure after 100 ms is replaced by one
  that requires the opposite: a saturated lane closes nothing and every
  packet arrives in order once the core takes values. The executable test
  `a_fan_in_burst_closes_no_publisher` drives forty publishers against
  one subscriber.
- The observation time of a held packet is still the instant its last
  byte left the socket. A packet that waits is older when the core sees
  it; an expiry computed from it is earlier, never later.
- A peer that sends faster than the broker serves it now waits. QoS 0
  publications towards a saturated subscriber are still dropped and
  counted; this record changes nothing for output.

## Left open

- Pressure on QoS 1 and 2 beyond the store's rate still ends in refusals
  (reason 0x97) and, past the quarter of the lane, in closures. A
  reservation of effect slots for the packets that release resources
  belongs to the core's cost model and to another record.

- The model `MTransportCredit` of the runtime tier states the lane
  bounds; it must drop the input deadline and state that a held packet is
  eventually delivered or its task cancelled.
- A per-connection read budget (bytes per second) is not provided.
