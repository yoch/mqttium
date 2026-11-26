# MQTT flow control under pressure

Three finite-state models of what the broker does when a client sends more
than a window admits. They are runtime-tier evidence (ADR 0044): each
encodes the predicates of the Rust functions it names, with mutant
configurations. None is a proof of that code, and none replaces its tests.

| Module | Code | Question |
| - | - | - |
| `MReadBehind` | `Peer::overtakes_deferred`, `Peer::keep_behind`, `Peer::lookahead_blocked` in `crates/mqttiumd/src/runtime.rs`; `Window::all_wait_for_remote_pubrel` in `receiving.rs`; record 0060 | What may be read behind an MQTT 3.1.1 PUBLISH that waits for a slot, how much, and when does the wait end? |
| `MSessionQuota` | `Owner::plan_admission` and `slots_by_session` in `crates/mqttiumd-core/src/owner.rs`; record 0061 | Does a session hold more QoS 2 exchanges than its bound across its connections, and when is a publication at the bound refused? |
| `MQos0Window` | `Owner::plan_qos0_continue` and `VolatileQueue` in `crates/mqttiumd-core/src/owner/pubsub.rs`; record 0063 | Is a volatile QoS 0 burst paced by the window of its connection, in order, without one stalled socket holding another? |

## Scope

The three models describe the default runtime, `--delivery-shards 0`: one
owner, on the runtime thread that takes the packets of every connection.

With `--delivery-shards K >= 1` (record 0066):

- `MReadBehind` and `MSessionQuota`: the reader and the admission are the
  same functions, run by the Coordinator. PUBLISH and PUBREL of a publisher
  still reach it (`ConnCore::push_incoming_batch` steers only PUBACK,
  PUBREC and PUBCOMP to a shard). Nothing was examined beyond that, and
  every replay below runs with no delivery shard.
- `MQos0Window` does not describe that mode. The Coordinator hands a
  volatile QoS 0 publication to the shards (`Change::Qos0Delegated` in
  `Owner::plan_qos0_packet`), each shard paces it in an `Owner` of its own
  (`step_shard_qos0` in `owner/shard.rs`) and gets its write receipts through
  `ShardInbox`. None of that is modelled.

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/flow-pressure --list
python ../tools/formal/run_tlc.py --models-dir models/flow-pressure
```

21 configurations, the whole directory in about twenty seconds; the
largest explores 1,230 states.

## MReadBehind

Two occupied receive slots and a third PUBLISH, identifier 3, waiting for
room. Five input streams exercise a PUBREL that may pass and one that may
not, the packet bound, the byte bound and a slot freed by the broker's own
work. Two packets or two byte units fit behind the waiting publication.

The product keeps `BEHIND_PACKETS` = 32 packets and `BEHIND_BYTES` = the
bytes of one largest packet behind it, on a window of
`MAXIMUM` = 8 slots (`receiving.rs`). Byte units stand for encoded body sizes;
the comparison is checked at equality and past it.

A slot is `RecQueued` (the PUBREC is not written yet: `ReceiveStep::Pubrec`),
`RecWritten` (`RemotePubrel`: only the PUBREL of the client frees it),
`Releasing` (`ReleaseObserved`: the PUBREL was read), `OwnAck` (a QoS 1
publication, freed by the broker's own PUBACK) or `Free`.

Abstracted away: the serial of an occurrence, the encoding of a PUBREC, the
store and transport stages inside each action. `FinishRel` spans the
broker's PUBACK, or the commit of a release and the write of its PUBCOMP.
A PUBREL may be read before the core applied the write receipt of its
PUBREC. The model stops at the decision for the waiting publication:
nothing moves after a close. What a persistent client finds when it comes
back is in the socket tests, not here.

| Property | Statement | How it can fail |
| - | - | - |
| `Bounded` | At most two packets and two byte units are kept behind. | `mut-bytes` (`NoByteBound`: a packet is kept whatever the bytes already kept). The bound is the guard of the branch that keeps a packet, read back on what was kept: it guards a regression of `PeerCold::room_behind`. |
| `NoPublishOvertaken` | A packet handed to the core ahead of the waiting PUBLISH is not preceded, in what was read, by a PUBLISH of its own identifier that is still waiting or kept behind. It is stated on the stream and on the positions that went ahead; it names neither `behind` nor `Independent`. | `mut-cross` (`AnyRel`: any PUBREL passes). A scratch run with `Independent` forgetting the packets kept behind violates it too. |
| `CloseOnlyWhenPeerOwes` | A closed connection has every slot at `RecWritten`. | `mut-close` (`CloseEarly`). **This is the closing guard written a second time**, on the state the close leaves, and nothing moves after a close: it guards a regression of that guard or of `AllOwed`, and shows nothing about whether the guard is the right one. |
| `Terminates` | Under weak fairness of the reader, of the writes and of the releases already submitted, the wait ends: admission or close. | `mut-wait` (`WaitForever`: the reader never closes). |

Three `reach-` configurations make TLC exhibit a close at the packet
bound, a close at the byte bound and an admission after a release.

## MSessionQuota

Two sessions, a bound of one QoS 2 exchange each (the product sets
`OwnerConfig::session_qos2_exchanges` to the window of a connection,
`MAXIMUM` = 8 in `receiving.rs`), two packet identifiers, two connection
generations.

An exchange is `Admitting` (the result of its admission is awaited, or its
first PUBREC is held behind an earlier answer), `AwaitRel`
(`SlotState::AwaitPubrel`, or a PUBREC held again for a PUBLISH received
again, `SlotState::AnswerHeld` with `resent`), `Releasing` or `Free`. All
but `Free` count. A reconnection keeps the exchanges and their original
generation; the client may send an owed PUBREL on its current connection.

Abstracted away: a PUBLISH received again for a held exchange (not a new
admission), QoS 1 exchanges (not counted by the code), the lease table and
its bytes, the store. **A wait is the absence of a step**: the model has no
waiting publication. In the runtime the publication is deferred, tried
again when a result of the store is applied, and its connection is closed
with `input_capacity` once `CAPACITY_WAIT` (5 s) passed without progress.
The model asserts nothing about that wait, nor about memory.

| Property | Statement | How it can fail |
| - | - | - |
| `PerSession` | A session never holds more exchanges than its bound. | `mut-reconnect` (`ResetAtConnect`: only the exchanges of the current connection are counted). |
| `RefusedSessionOnlyOwes` | A session whose connection was closed at the bound is at the bound and every exchange it holds awaits the PUBREL of its client, from the refusal until the client is back. It reads the connections and the exchanges only. | `mut-refusal` (`RefuseLocalWork`: refuse whatever the owner is doing). A scratch run with `Owes` also accepting a release in flight violates it too. It is what the refusal guard establishes, checked on every later state: an owner step on an exchange of a refused session would break it. It does not show that waiting is *needed* while an admission is in flight: such a wait ends in the same refusal once the admission commits. |

`reach-peer` exhibits one session full of an exchange of its earlier
connection while its neighbour releases one of its own.

## MQos0Window

A fast and a slow subscriber. Each has a window of one frame, two parking
places and an output queue of two frames; one control frame may enter the
queue of the slow one. An emitted frame holds its place of the window
after the wire write, until its receipt is applied.

The product: `QOS0_WINDOW` = 224 frames in flight per connection,
`QOS0_PARKED` = 1,024 deliveries waiting per connection, within half of
the volatile queue and of its payload limit for all connections together
(`VolatileQueue::may_park`), and `OUTPUT_SLOTS` = 256 places in the output
queue, of which `QOS0_OUTPUT_RESERVE` = 32 are left to other frames (a
compile-time assertion of `net.rs`). The finite inputs stay within the
process-wide limits, which are not modelled.

Abstracted away: the pending queue shared by all connections (`Arrive` is
the continuation taking its oldest delivery), the round-robin among ready
lanes (weak fairness of `Serve` on both lanes stands for it), expiry, a
packet too large, the end of a connection. Deliveries consumed from the
store, retained replay and transport failure have other state. Only the
fast socket and its receipts must progress; the slow one may stall for
ever.

| Property | Statement | How it can fail |
| - | - | - |
| `WithinWindow` | Frames in flight, parked deliveries and queued frames stay within their bounds. | `mut-window-flight` (`NoWindow`). It reads back the bounds the guards of `Arrive` and `Serve` test: a regression guard of those tests. |
| `ControlRoom` | Until the control frame is admitted, the queue of the slow subscriber has a place for it. | `mut-window` (`NoWindow`: data is emitted whatever the window). |
| `Ordered` | On the wire, the publications a connection is sent keep the order of their arrival. It is stated on `sent`, the wire history, which no guard reads. | `mut-order` (`NewestFirst`: the lane serves its newest waiting delivery) and `mut-pass-parked` (`PassParked`: a pending delivery is emitted without regard to those waiting in its lane). **It holds only because of those two rules**: `VolatileQueue::serve_ready` pops the front of the lane, and `Owner::plan_qos0_continue` parks a delivery whose lane is not empty. It is their regression guard, not a property the queues have by themselves. |
| `FastCompletes` | Under weak fairness of the fast lane, its socket and its receipts, the fast subscriber is sent everything. | `mut-stall` (`WaitForSlow`: nothing is taken for the fast lane while the slow window is full). |

`reach-isolation` exhibits the fast lane drained while the slow one holds
its full parking budget and has dropped a newer arrival.

In the unmutated model the test `Len(parked[p]) = 0` of `Arrive` never
decides: a lane with a waiting delivery and room is served first, so
whenever `Arrive(p)` is enabled with room, the lane of `p` is empty (1,230
states, scratch run). The code has both tests as well, and
`mut-pass-parked` removes both.

## The four invariants that were true by construction

An audit found three invariants that restated the guard of the action
they constrain through a variable computed from that same guard, and one
that follows from a single guard of the model.

| Was | Evidence | Now |
| - | - | - |
| `NoPrematureRefusal` (`MSessionQuota`): `refusal # "Premature"`, where the refusal wrote `"Premature"` exactly when `Owes(s)` was false, in a branch guarded by `Owes(s)`. | It failed under `RefuseLocalWork` (8 states) and under nothing else: with `Owes` itself changed to accept a release in flight, the unmutated configuration still passed (1,475 states). | Replaced by `RefusedSessionOnlyOwes`; the variable `refusal` is gone. The same broken `Owes` now violates it. |
| `IndependentOnly` (`MReadBehind`): `~crossed`, where `crossed` recorded `~Independent(p)` in a branch guarded by `Independent(p)`. | It failed under `AnyRel` (37 states) and under nothing else: with `Independent` changed to forget the packets kept behind, the unmutated configuration still passed (164 states). | Replaced by `NoPublishOvertaken` over `passed`, a history of positions; `crossed` is gone. The same broken `Independent` now violates it. |
| `CloseOnlyWhenPeerOwes` (`MReadBehind`): `state = "Closed" => AllOwed`, in a model where the close is guarded by `AllOwed` and nothing moves after it. | It fails under `CloseEarly` (53 states). With `AllOwed` changed to accept a PUBREC not yet written it still passed (156 states). | Kept as the regression guard of that mutant and presented as such above. It no longer goes through `AllOwed`, so the same broken `AllOwed` violates it; it still says no more than the guard. |
| `Ordered` (`MQos0Window`). | It is stated on the wire history and fails under `NewestFirst` (595 states). It is not a restated guard, but it depended on one rule only as far as the configurations showed. | Kept; `mut-pass-parked` (512 states) shows the second rule it depends on, and the table above says so. |

The scratch runs are not configurations of this directory: they change an
operator of the module itself, not the constant `Mutation`.

## Replays

`MReadBehind`. The two closing counterexamples (`reach-packets`,
`reach-bytes`) are `mqtt311_packet_lookahead_closes_only_the_blocked_peer`
and `mqtt311_byte_lookahead_closes_only_the_blocked_peer`: a full window
whose PUBREC are all written, the bound reached, one more packet, the
close with `receive_lookahead_capacity`, another client still served, and
the session resumed with its eight exchanges. The trace `mut-close` would
close in is `mqtt311_saturated_lookahead_waits_for_an_already_observed_release`
(a PUBREL already read: the connection stays open) and
`lookahead_requires_written_pubrec_for_every_live_occurrence` (a PUBREC not
written, a stale write receipt, a PUBREL read before the receipt, a QoS 1
slot: none lets the window say that only the peer can free it). The three
tests of `runtime_tests.rs` stage the packets kept behind through
`Peer::keep_behind`; they do not read them from a socket.

`MSessionQuota`. `a_session_holds_a_bounded_number_of_exchanges_across_its_connections`
drives one owner through the model's steps: an exchange left by a first
connection, a second being admitted while a third waits (`NeedCapacity`),
the close once both await their PUBREL, the client back, its PUBREL on the
new connection, a publication that waits while that release is in flight
and is admitted after it. `stale_qos2_exchanges_of_a_3_1_1_session_are_bounded_and_lose_no_publication`
plays the refusal and the recovery on the executable.
`one_session_cannot_hold_the_inbound_exchanges_of_every_client` shows the
neighbour admitted. No test drives the neighbour's PUBREL of `reach-peer`.

`MQos0Window`. `product_qos0_burst_is_paced_by_the_window_and_resumes_in_order`
gives one owner a burst for two connections, completes one write of the
first and checks that one frame follows, for that connection, the next in
order, while the second holds its window; then every write in turn.
`product_qos0_past_what_a_connection_keeps_waiting_the_newest_is_dropped`
fills the parking budget and checks which deliveries are dropped.
`a_qos0_burst_waits_for_the_output_of_its_subscriber_and_keeps_its_order`
holds the output of a subscriber of the runtime, releases it and checks
the order received.

## Correspondence

| Model action | Module | Code | Replay |
| - | - | - | - |
| `Read` | `MReadBehind` | The ingress loop of `Runtime::connections_at`: `Peer::overtakes_deferred`, then `Peer::keep_behind`, which refuses past `PeerCold::room_behind`; a PUBREL that passes reaches `Window::pubrel_observed`; `Peer::lookahead_blocked` and `Window::all_wait_for_remote_pubrel` decide the close | `only_the_pubrel_of_another_exchange_goes_ahead_of_a_waiting_publication` (what passes, both bounds), `mqtt311_packet_lookahead_closes_only_the_blocked_peer`, `mqtt311_byte_lookahead_closes_only_the_blocked_peer`, `mqtt311_saturated_lookahead_waits_for_an_already_observed_release` |
| `WriteRec` | `MReadBehind` | `Runtime::written_complete`: `Window::pubrec_written` | `lookahead_requires_written_pubrec_for_every_live_occurrence` |
| `FinishRel` | `MReadBehind` | `Peer::receiving_complete` (`Window::complete_frame`), from `Runtime::wire_progress` and `Runtime::written_complete` | `a_3_1_1_client_with_more_qos2_exchanges_than_the_window_completes_them_all` |
| `Resume` | `MReadBehind` | `Runtime::retry_deferred_publish`: `Window::publish_qos` succeeds | `a_3_1_1_client_with_more_qos2_exchanges_than_the_window_completes_them_all` |
| `Terminal` | `MReadBehind` | The end of a run of the model | Not an action of the code |
| `Publish` | `MSessionQuota` | `Owner::plan_admission`: the QoS 2 slots of `slots_by_session`, `OwnerConfig::session_qos2_exchanges`, `CloseReason::CapacityExceeded` or a `NeedCapacity` | `a_session_holds_a_bounded_number_of_exchanges_across_its_connections`, `stale_qos2_exchanges_of_a_3_1_1_session_are_bounded_and_lose_no_publication` |
| `CommitAdmission`, `CommitRelease` | `MSessionQuota` | The result of the store applied to the slot: `SlotState::AwaitPubrel` (or a held answer); the slot removed at the end of a release | `a_session_holds_a_bounded_number_of_exchanges_across_its_connections` |
| `Rel`, `Reconnect` | `MSessionQuota` | A PUBREL of the current connection for an exchange of an earlier one; a CONNECT that resumes the session | `a_session_holds_a_bounded_number_of_exchanges_across_its_connections`, `stale_qos2_exchanges_of_a_3_1_1_session_are_bounded_and_lose_no_publication` |
| `Arrive` | `MQos0Window` | `Owner::plan_qos0_continue` on the oldest pending delivery: `VolatileQueue::promote_front_to_inflight`, `VolatileQueue::park_front_pending` within `VolatileQueue::may_park`, or `VolatileQueue::drop_front_pending` | `product_qos0_burst_is_paced_by_the_window_and_resumes_in_order`, `product_qos0_past_what_a_connection_keeps_waiting_the_newest_is_dropped` |
| `Serve` | `MQos0Window` | `Owner::plan_qos0_continue` on `VolatileQueue::front_ready`, applied by `VolatileQueue::serve_ready` | `product_qos0_burst_is_paced_by_the_window_and_resumes_in_order` |
| `Control` | `MQos0Window` | `ConnCore::try_send_outgoing` for a frame that is not a volatile QoS 0 publication; the room is `QOS0_OUTPUT_RESERVE` | No deterministic replay: no test fills the window and then sends a control frame; the room is a compile-time assertion |
| `Write` | `MQos0Window` | The socket task takes the queue in order (`ConnCore::pop_outgoing_batch`) | `a_qos0_burst_waits_for_the_output_of_its_subscriber_and_keeps_its_order` |
| `ApplyWritten` | `MQos0Window` | The receipt of the write applied by the owner: `VolatileQueue::remove_inflight` | `product_qos0_burst_is_paced_by_the_window_and_resumes_in_order` |
| `Idle` | `MSessionQuota`, `MQos0Window` | Stuttering: a client that sends nothing, a socket that stalls | Not an action of the code |
