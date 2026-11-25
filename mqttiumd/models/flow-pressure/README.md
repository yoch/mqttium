# MQTT flow control under pressure

Run the structural check and the three finite-state models from mqttiumd/.

~~~sh
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/flow-pressure
~~~

### Reading behind a waiting MQTT 3.1.1 publication

MReadBehind starts with two occupied receive slots and a third PUBLISH waiting
for room. Five ordered input streams exercise independent and dependent
PUBREL, packet pressure, byte pressure and local completion. Two packets
or two byte units fit behind the held publication. The product limits this
to 32 packets and the bytes of one maximum-size packet. Byte units represent
encoded body sizes. The model checks the numeric comparison at equality as
well as overflow.

The reader compares the PUBREL identifier with the waiting PUBLISH and every
PUBLISH already parked. It can close at the first unparkable packet only when
every active occurrence has written its positive PUBREC and none has a locally
observed PUBREL. The original two identifiers are unique for the interval
before the held PUBLISH is admitted. This projects away the serial check, the
encoding of a PUBREC, and the store and transport stages inside each action.
The release action spans either the broker's own terminal PUBACK or the commit and wire progress of PUBCOMP. An independent PUBREL may arrive before the core has applied the write receipt of its PUBREC. The model
stops at the decision for the waiting publication. The socket tests replay
the subsequent reconnect, its original persistent obligations and the retry.

Weak fairness of reactor writes, input processing and already submitted
releases is explicit. Three reachability checks require TLC to exhibit
packet-limited closure, byte-limited closure and admission after a release.
The mutations separate waiting forever at the boundary, crossing a dependent
PUBLISH, and closing while local work can still make room.

### Admission across session generations

MSessionQuota gives each of two sessions one QoS 2 slot, two packet
identifiers and two connection generations. The product bound is eight.
Each input chooses a session and an unoccupied identifier. A duplicate
PUBLISH is not a new admission and is outside the model. An admission in
progress, an exchange awaiting PUBREL, and a release in progress all count.
A session at quota is refused only when the peer owes all its PUBRELs.
The first PUBREC held for ordering belongs to the owner-driven admission
phase. A repeated PUBREC for a known exchange belongs to AwaitRel.
A reconnect retains the exchange's original generation. A client may send
the owed PUBREL on its current connection. A witness reaches one session
still holding its old-generation exchange while its neighbour releases
another. Separate mutations forget the older session generation and refuse
before the owner's work can complete. No store timing or whole-process
memory bound is asserted by this state space.

### Volatile QoS 0 scheduling

MQos0Window has a fast and a slow subscriber. Each has an output window
of one, two parking slots and a two-frame socket queue. One additional
control frame may occupy the slow subscriber's queue. Emitted frames
continue to occupy the window after the wire write until its receipt is
applied. The implementation admits 224 volatile frames and up to 1,024
parked messages per connection, subject also to separate process-wide
count and payload limits. The finite inputs stay within those global
limits. This model concerns volatile live QoS 0. Delivery consumed from
the store, retained replay, and transport failure have different state.

Ready parked work runs before new pending work. The choice between ready
lanes is abstracted to weakly fair service of eligible parked work on both lanes. Only the fast socket and its receipt consumer must progress; the slow socket may remain blocked indefinitely. The isolation witness drains the fast lane while the slow one
holds its full parking budget and has dropped a newer arrival. The window
mutation allows data to consume the space for control. The ordering mutation serves the newest parked publication first. The isolation mutation waits for the slow socket before accepting more work for the fast one.

### Replaying the boundary traces

The reader's counterexample with peer-only saturation ends in a stable wait
on the old implementation. Run the two `mqtt311_*_lookahead_closes_only_the_blocked_peer`
tests against the revised implementation to observe closure and recovery
at each boundary. The local-work counterexample corresponds to
`mqtt311_saturated_lookahead_waits_for_an_already_observed_release` and
`lookahead_requires_written_pubrec_for_every_live_occurrence`.
[Runtime test fixture](../../crates/mqttiumd/src/runtime_tests.rs) ·
[Receiving-window test](../../crates/mqttiumd/src/receiving.rs)

The session quota's reconnect mutant admits a second obligation while the
first still belongs to the session. The peer-service trace first fills one
session, reconnects it, then admits and releases a publication on the other.
[Core admission test](../../crates/mqttiumd-core/src/owner.rs) ·
[Executable quota and pipeline tests](../../crates/mqttiumd/tests/reactor_wake.rs)

The ordering mutant emits the newer of two parked sequence numbers first.
The socket test holds the writer until a burst has built up and checks the
wire order after allowing it to drain.
[QoS 0 socket test](../../crates/mqttiumd/src/runtime_tests.rs)

## Correspondence

| Model action | Product transition | Replay |
| --- | --- | --- |
| `Read` | Runtime reads the next packet, tests Peer::overtakes_deferred, then applies the packet and byte bounds in Peer::keep_behind. The current window's wire progress and received PUBREL decide if local progress remains. | `only_the_pubrel_of_another_exchange_goes_ahead_of_a_waiting_publication`, `mqtt311_packet_lookahead_closes_only_the_blocked_peer`, `mqtt311_byte_lookahead_closes_only_the_blocked_peer` |
| `WriteRec`, `FinishRel`, `Resume` | Positive PUBREC written, PUBREL completed by PUBCOMP, and the waiting publication obtains its slot. | `a_3_1_1_client_with_more_qos2_exchanges_than_the_window_completes_them_all` |
| `Terminal` | End observation when closure or admission has been decided. | Both lookahead-bound tests continue with a persistent reconnect and a retry. |
| `Publish`, `CommitAdmission`, `CommitRelease`, `Rel`, `Reconnect` | Owner admission consults slots_by_session across connections. AwaitPubrel or a repeated held answer is peer-owed. Store admission/release and an initial held answer remain owner work. | `a_session_holds_a_bounded_number_of_exchanges_across_its_connections`, `one_session_cannot_hold_the_inbound_exchanges_of_every_client` |
| `Arrive`, `Serve` | Owner::plan_qos0_continuation serves an eligible parked head before a new pending delivery. A full lane parks in FIFO order or drops the newest when its budget is full. | `product_qos0_burst_is_paced_by_the_window_and_resumes_in_order`, `product_qos0_past_what_a_connection_keeps_waiting_the_newest_is_dropped` |
| `Control`, `Write`, `ApplyWritten` | OutgoingState accepts a control frame; the socket drains a FIFO frame; its written result releases the volatile window through the core. | `a_qos0_burst_waits_for_the_output_of_its_subscriber_and_keeps_its_order` holds the subscriber's output, releases it and checks the received sequence. One control frame represents competition for the queue. |
| `Idle` | Stuttering observation, including an indefinitely stalled slow reader. | Fairness is a model premise. |
