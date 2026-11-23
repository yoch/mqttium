# ADR 0046 — Input pressure slows a peer; it does not disconnect it

Status: **Accepted with amendments** (21 November 2026, decided under the
repository owner's delegation on the advice of the independent audit of
`8e4303d`; proposed on 20 November 2026, runtime tier of record 0044). It
changes no public type of the core tier, no durable encoding and no
contract. The contract digest that `validate` prints moves only because
the register lists the accepted records.

Amended by 0060 (proposed): the reader goes on behind a publication that
waits for a slot of the window, so the PUBREL of a 3.1.1 client that was
announced no window is read too, up to a bound.

Amended by 0061 (proposed): the exchange table is sized for the sessions,
eight entries each, so the broker no longer holds 128 credits for all its
clients; a session holds at most eight QoS 2 exchanges across its
connections.

Amendment of acceptance (21 November 2026). The record is accepted with
the rules of the text below, which differ from its first form on three
points. Keep Alive runs from the last packet the transport heard; it is
not suspended while a packet is held. The last eighth of the ordinary
lane is kept for the packets that release capacity, and a connection
has at most its announced window of business packets waiting in the
core. No count of deferred packets closes a connection: the quarter rule
is withdrawn. What "Left open" lists stays owed.

Amendment of 21 November 2026, after the counter-audit of `bbe1453`.
Three rules of the text accepted that day did not hold and are replaced
below. A connection was allowed one waiting business packet; it is
allowed its window, because one was enough to hold the PUBREL of a
client behind its own publications and to stop the broker. Deferred
packets were all taken up again on every answer of the store; they are
tried from the oldest until one cannot be served, because the former
cost grew with their number and brought the broker to a standstill. The
five-second bound of a wait for a credit ran from the start of the wait;
it runs from the last credit the broker returned.

Amended by 0050 (record 0047): the broker's own PUBACK and PUBREC no
longer follow the results of the store. The paragraph of "Left open" that
records the later acknowledgement leaving first describes the product
before that record.

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

**No count of deferred packets closes a connection.** An earlier form of
this record closed, with `input_capacity`, a packet refused for effect
slots while a quarter of the lane already waited, to keep the lane open
for the packets that end the pressure. The reserve below keeps those
places by not admitting new work into them, so the closure protects
nothing more: with both rules together it closed publishers of a fan-in
burst that the quarter alone left open, and it is removed. A deferred
packet keeps its place in the lane until the dispatch accepts an effect.

**The last places of the lane are kept for the packets that release
capacity.** PUBLISH, SUBSCRIBE and UNSUBSCRIBE ask the broker for new
work; every other packet completes or ends work it already holds. A
business packet is given to the core only while the ordinary lane has at
least its control reserve free, an eighth of the lane. Acknowledgements,
PINGREQ and DISCONNECT are read down to the last two places. Without the
reserve the lane could fill with new work up to those two places, and the
acknowledgement that frees a window, an identifier or a payload waited
behind it.

**A connection has at most its window of business packets waiting.**
Receive Maximum is a promise to every client: eight publications may be
unacknowledged at once. While fewer than eight business packets of a
connection wait for capacity in the core, the next one is taken too, so
a client that respects its window has all its publications in the core
and the PUBREL or PUBACK it sends after them is read. From the eighth
on, the next business packet stays with its transport: the rest of the
backlog is in the socket, where TCP slows its peer, and not in the lane
the connection shares with every other. The lane has a place for the
window of every connection beside its ordinary share, so the control
reserve is never what keeps such a client waiting.

The first form of this rule allowed one waiting packet. The broker holds
128 exchange credits and announces eight to each client: with enough
clients that fill their window together, the first publications read
took every credit, the next of each client waited for one, and the one
after it was held in front of the PUBRELs that would have returned the
credits. No exchange completed, and every client was closed
(`clients_that_fill_their_announced_window_together_all_complete`).

In both cases the packet has already left the transport's lane, because
its kind is known only once it is taken. The runtime keeps it as the
connection's one held packet, the place a packet behind an unfinished
CONNECT already used, and offers it again on its next turn. Nothing of
that connection passes it: an acknowledgement behind a held publication
of its own connection waits with it. Holding a packet is not progress,
so a runtime with nothing else to do still rests.

**A packet that finds a credit lent waits for it.** The core lends an
exchange credit and the bytes of a payload to every admission in flight
and asks to be called again when one is released. The runtime used to
close the connection of such a packet with `input_capacity`; it now
keeps the packet with the deferred ones. Only a business packet waits
this way, so nothing of its connection passes it. (Added on 21 November
2026, after the acceptance of this record.)

**A wake tries the deferred packets from the oldest.** What a deferred
packet waits for can arrive with an answer of the store, with the
terminal receipt of a frame or a closed connection (each can end an
exchange and return its credit), or with effects the adapters accepted.
On each of these the oldest deferred packet is served ahead of
everything else, then the next, until one still cannot be served: that
one keeps its place and the others are not touched. A wake costs one
failed attempt whatever the number of packets that wait, and no packet
overtakes an older one. Two earlier forms are withdrawn. The first took
every deferred packet up again on an answer of the store and on nothing
else: at QoS 1 the credit comes back with the write receipt of the
PUBACK, so a publication waited for a credit that was already free
(`a_write_confirmation_alone_takes_up_a_publication_that_waits_for_a_credit`),
and with several hundred packets waiting each answer of the store put
them all in front of the next answer: a hundred clients filling their
window completed nothing in five seconds, where they now complete in
less than two. The second did the same on every receipt as well.

**A wait for a credit is bounded by the broker's silence.** A credit can
be held by an exchange that a client never finishes. A connection whose
packet waits for a credit is closed with `input_capacity` when the
broker has returned no credit and applied no answer of the store for
five seconds. While exchanges end, the wait is pressure, however long
it lasts: its first form measured the wait itself and closed clients
that were being served slowly.

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
  or receipt slots. The figures that follow were measured while the
  quarter rule still closed connections. Measured with 32 publisher and
  subscriber pairs
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
  lets through are those at the head of their connection's stream. Result,
  receipt and effect slots are released by the store's results and by the
  dispatch, never by a packet from the network. An exchange credit is
  not: at QoS 2 it comes back with the PUBREL of the client that holds
  it. The first form of the window rule closed that cycle, as the
  counter-audit of `bbe1453` showed; with the window, the PUBREL of a
  client that respects Receive Maximum is always read. The tests
  `the_last_places_of_the_lane_are_kept_for_packets_that_release_capacity`
  and `a_connection_keeps_at_most_its_window_of_business_packets_waiting`
  state the two rules on the runtime. Under the load of report 0027 with
  64 pairs, alternated with the previous build on a machine that other
  jobs were using, the two builds are not distinguishable beyond the
  noise of that machine (QoS 0: 6,900 to 8,800 against 1,300 to 7,400
  messages per second; QoS 1: about 1,050 against 700 to 1,100): the
  rules cost no throughput there. They were not measured on a quiet
  machine.
- At 3,000 QoS 1 publications per second offered to 64 connections,
  beyond what the store commits, 20 connections were closed for want of
  an exchange credit in each of two runs on a quiet machine. With the
  wait, none is closed in two runs: every publication is submitted, and
  the excess is refused in its PUBACK (0x97, 44 to 48 % of them). The
  test `a_publication_waits_for_the_exchange_credit_of_an_admission_in_flight`
  states the rule with one credit and three publications.
- A peer that sends faster than the broker serves it now waits. QoS 0
  publications towards a saturated subscriber are still dropped and
  counted; this record changes nothing for output.

## Left open

- Pressure on QoS 1 and 2 beyond the store's rate still ends in refusals
  (reason 0x97). A
  reservation of effect slots for the packets that release resources
  belongs to the core's cost model and to another record. The places of
  the lane are reserved; the slots of the effect box are not.
- The window bounds the business packets of a connection that wait
  deferred; those it has queued in the lane and not yet seen by the core
  are bounded only by the fairness of reading (four packets a turn each,
  in rotation).
- The oldest deferred packet is tried first and holds the others while
  it cannot be served, whatever each of them waits for. A publication
  that waits for a credit can so delay one that waits only for the
  effect box. Nothing a deferred packet waits for is released by another
  business packet, so this delays and does not block.
- The broker announces eight exchanges to each client and holds 128
  credits for all: clients that fill their window together wait for one
  another. Credits that match the promise belong to the scaling of the
  core.
- With Keep Alive 0 a held packet has no bound in time.
- The broker's own PUBACK and PUBREC follow the results of the store.
  When the result of an earlier publication of a connection is known
  after that of a later one, the later acknowledgement leaves first. The
  single store writer answers in order, so this needs a result that
  stays unknown and is resolved later; the audit forced it with a test
  barrier. MQTT 5 states the order of acknowledgements as rules of the
  client (MQTT-4.6.0-2 to -4). MQTT 3.1.1 extends its rules to a server
  "when delivering messages to each of its subscribers" (MQTT-4.6.0-6):
  that covers the order of PUBREL on a delivery, not the acknowledgement
  of a received publication. It is recorded as behaviour a client may
  observe, and the reading of MQTT-4.6.0-2 and -3 stays to be decided.
  The two orders a server owes are kept and have their witnesses: the
  order of the deliveries (MQTT-4.6.0-5 in MQTT 5, -6 in 3.1.1), under a
  late result and across a resumed session; and the order of PUBREL
  after PUBREC on a delivery, which the counter-audit of `bbe1453` found
  reversed under a late result and which the core has kept since
  (`pubrels_keep_the_order_of_the_pubrecs_when_an_earlier_store_result_is_late`).

- The model `MLaneOwed` (`models/reactor-wake`) states that a held
  packet is eventually delivered or its task cancelled, and keeps the old
  input deadline as a mutant; `MTransportCredit` states the receipts. They
  have not been reviewed independently.
- A per-connection read budget (bytes per second) is not provided.
