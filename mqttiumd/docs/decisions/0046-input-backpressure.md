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

**The last places of the lane are kept for the packets that release
capacity.** PUBLISH, SUBSCRIBE and UNSUBSCRIBE ask the broker for new
work; every other packet completes or ends work it already holds. A
business packet is given to the core only while the ordinary lane has at
least its control reserve free, an eighth of the lane. Acknowledgements,
PINGREQ and DISCONNECT are read down to the last two places. Without the
reserve the lane could fill with new work up to those two places, and the
acknowledgement that frees a window, an identifier or a payload waited
behind it.

**A connection adds no business packet behind one of its own that
waits.** While a business packet of a connection is deferred, its next
business packet stays with its transport. The backlog of a connection
is then in its socket, where TCP slows its peer, and not in the lane it
shares with every other connection.

In both cases the packet has already left the transport's lane, because
its kind is known only once it is taken. The runtime keeps it as the
connection's one held packet, the place a packet behind an unfinished
CONNECT already used, and offers it again on its next turn. Nothing of
that connection passes it: an acknowledgement behind a publication of
its own connection waits with it. Holding a packet is not progress, so a
runtime with nothing else to do still rests.

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
  closed; at 3,000, beyond what the store commits, 8 of 64 connections
  are in one run of two (report 0027; 12 in an earlier run made while
  other jobs used the machine).
- The net test that required the closure after 100 ms is replaced by one
  that requires the opposite: a saturated lane closes nothing and every
  packet arrives in order once the core takes values. The executable test
  `a_fan_in_burst_closes_no_publisher` drives forty publishers against
  one subscriber.
- The observation time of a held packet is still the instant its last
  byte left the socket. A packet that waits is older when the core sees
  it; an expiry computed from it is earlier, never later.
- Keep Alive runs from the last complete packet the transport received,
  whether or not the core has taken it. A peer the broker keeps hearing
  is not expired while its packets wait; a peer silent for one and a half
  Keep Alive is closed although its last packet is still held. Keep Alive
  is therefore the bound on how long a packet can be held: with Keep
  Alive 0 there is none. An earlier form of this record suspended the
  clock while any framed packet was untaken; the independent audit of
  `8e4303d` showed that one held packet then hid any silence.
- The reserve protects places in a lane, not a deadline: the packets it
  lets through are those at the head of their connection's stream. What a
  deferred packet waits for (result, receipt and effect slots) is
  released by the store's results and by the dispatch, never by a packet
  from the network, so neither rule can close a cycle; the audit of
  `8e4303d` observed none either. The tests
  `the_last_places_of_the_lane_are_kept_for_packets_that_release_capacity`
  and `a_connection_with_a_deferred_packet_adds_no_further_business_packet`
  state the two rules on the runtime. Under the load of report 0027 with
  64 pairs, alternated with the previous build on a machine that other
  jobs were using, the two builds are not distinguishable beyond the
  noise of that machine (QoS 0: 6,900 to 8,800 against 1,300 to 7,400
  messages per second; QoS 1: about 1,050 against 700 to 1,100): the
  rules cost no throughput there. They were not measured on a quiet
  machine.
- A peer that sends faster than the broker serves it now waits. QoS 0
  publications towards a saturated subscriber are still dropped and
  counted; this record changes nothing for output.

## Left open

- Pressure on QoS 1 and 2 beyond the store's rate still ends in refusals
  (reason 0x97) and, past the quarter of the lane, in closures. A
  reservation of effect slots for the packets that release resources
  belongs to the core's cost model and to another record. The places of
  the lane are reserved; the slots of the effect box are not.
- Before a business packet of a connection is first deferred, the
  connection may have several queued in the lane: reading is fair between
  connections (four packets a turn each, in rotation) but has no bound
  per connection.
- With Keep Alive 0 a held packet has no bound in time.
- Acknowledgements follow the results of the store. When the result of an
  earlier publication of a connection is known after that of a later one,
  the later PUBACK or PUBREC leaves first; a PUBREL can likewise leave
  before that of an earlier PUBREC. The single store writer answers in
  order, so this needs a result that stays unknown and is resolved later;
  the audit forced it with a test barrier and at the level of the core.
  MQTT 5 states these three orders as rules of the client (MQTT-4.6.0-2
  to -4). MQTT 3.1.1 extends its rules to a server "when delivering
  messages to each of its subscribers" (MQTT-4.6.0-6): that covers the
  order of PUBREL on a delivery, not the acknowledgement of a received
  publication. The order of the deliveries themselves (MQTT-4.6.0-5 in
  MQTT 5, -6 in 3.1.1) held in every probe. The order of PUBREL under a
  reordered result is an open deviation in MQTT 3.1.1; the rest is
  recorded as behaviour a client may observe.

- The model `MLaneOwed` (`models/reactor-wake`) states that a held
  packet is eventually delivered or its task cancelled, and keeps the old
  input deadline as a mutant; `MTransportCredit` states the receipts. They
  have not been reviewed independently.
- A per-connection read budget (bytes per second) is not provided.
