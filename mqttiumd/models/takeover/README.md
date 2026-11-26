# Session takeover with frames in flight

`MTakeoverInFlight` models a client that connects again while frames of
its session are in flight on its previous connection. It is runtime-tier
evidence (ADR 0044): its actions are the steps of `connect`,
`start_takeover`, `pump_will_connects`, `close`, `finish_close` and
`collect_written` in `crates/mqttiumd/src/runtime.rs`, of `ConnCore::stop`
and `ConnCore::finish_task` in `net.rs`, and of `plan_frame_result` in
`crates/mqttiumd-core/src/owner.rs`. `MDisconnectQueue` models the order
of the packets the old connection writes once a DISCONNECT is queued for
it. Neither is a proof of that code.

## Scope

`MTakeoverInFlight` describes the product run with the single owner
(`--delivery-shards 0`, the default): the coordinator is the only producer
of frames and the only consumer of receipts, and `finish_close` is one
step of its thread. With `--delivery-shards K`, `K >= 1`, the following
exists in the code and is **not** in that model:

- a delivery shard writes to the old connection from its own thread
  (`OutgoingHandle::try_send_outgoing`), under a bound of its own
  (`ShardIoRoute::reserve_write`), and stops when it reads
  `OutgoingHandle::is_cancelled`;
- the receipts of the writes a shard owns and the acknowledgements of a
  subscriber go to a `ShardInbox`, not to the coordinator;
- `finish_close` is no longer one step: it returns until
  `Connection::is_finished` reads true, asks the shard to detach the
  session (`ShardPool::detach_session`) and returns again until the shard
  has done so. The peer stays in the table meanwhile, so the open of the
  new generation waits longer; the model says nothing of that wait or of
  what the shard does during it.

`MDisconnectQueue` has two producers on one output queue and covers both
modes for the one thing it states: see its section below.

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/takeover --list
python ../tools/formal/run_tlc.py --models-dir models/takeover
```

The directory contains 20 configurations of `MTakeoverInFlight` and four
of `MDisconnectQueue`; the whole directory runs in about a minute and the
largest configuration explores 227,656 states. To check only the ordering
of outgoing MQTT packets, run
`python ../tools/formal/run_tlc.py --models-dir models/takeover MDisconnectQueue`.

## What is modelled

Two generations of one session: generation 1, to which the session is
bound, and generation 2, which takes it over. A frame belongs to the
generation it was submitted on; its receipt and its result carry that
`ConnectionKey`.

- The core submits a frame only to a peer that is not `closing` and to
  which the session is bound (`Sink::try_submit`), and only while the
  output queue of that peer accepts frames: `ConnCore::finish_task` seals
  it when the task ends.
- `NewConnect` is `connect` and `start_takeover`: the new peer enters the
  table and the old one is marked `closing`.
- A close is `ConnCore::stop` on the core thread, in its three steps:
  `cancelled` is set, the socket is shut down, and only then the terminal
  reason is recorded. The task may run between any two of them.
- With `Deferred` the connection is a TLS one. Its task owns the state
  that seals the close_notify alert, so `Connection::close` only sets
  `cancelled`: the task shuts the socket and records the reason when it
  ends (`Driven::finish` of the TLS task), and `Runtime::close` leaves the
  peer in the table until `Connection::is_finished`. The alert is not a
  frame and is not modelled.
- The plain task checks `cancelled`, then writes; a write on a socket that
  was shut down fails and ends the task. The TLS task reads `cancelled`
  at the start of a turn only: with `Deferred` a frame it took is written
  whatever the flag says, until the next turn ends the task.
- A task that ends runs `ConnCore::stop` on its own thread, then
  `ConnCore::finish_task`: `TaskSeal` under the output lock, `TaskFinish`
  under the lock of the lanes towards the core, where the receipt of the
  frame in progress and Failed for each frame that was queued are offered
  once and dropped if the lane has no room.
- `finish_close` fails every frame the generation still has in
  `Runtime::frames`, queues `Input::ConnectionClosed` and removes the
  peer. Its lanes are not read again. `Preserve` selects the path that
  reads the lane first, and waits for the task if a frame is left.
- `pump_will_connects` starts the open of generation 2 only once the peer
  of generation 1 is gone from the table.
- The owner applies a frame result to its frame, and ends a connection on
  Failed only when the session is bound to the generation the result
  names. `Input::ConnectionClosed` detaches only that generation.

## What is abstracted away

- The owner's inputs sit in several lanes (`terminal_inputs`,
  `closing_inputs`, `inputs`) whose relative order the model does not
  fix: `inbox` is a set and the owner takes any input. The one order kept
  is that of `pop_closing_input`: `Input::ConnectionClosed` waits for an
  older ordinary input of its generation. The retiring pass of
  `connections_at` also waits until the owner has taken the frame results
  of the generation before it runs `finish_close`; the model does not.
- In `MTakeoverInFlight`, the MQTT 5 DISCONNECT sent to the old
  connection, the 300 ms bound of `closing`, the Will barrier and every
  store operation. A takeover close is a close that is due once `closing`
  is set.
- A frame the core gives up without submitting it: `Sink::try_submit`
  answers a frame for a peer that is closing, or for a generation the
  session is no longer bound to, with a Failed result at once
  ("unusable_generation"). The model has no such frame: its results are
  those of submitted frames.
- `StalledState::reports`, Started, NotStarted, the receipts a turn holds
  and the writes that carry several frames, which `MTransportCredit`
  models. A frame is written in one step, one frame per write, and its
  receipt is published before the next frame is taken.
- The flag `Link::finished` is set in the step that publishes the
  terminal receipts; `mut-finished-first` of `MTransportCredit` is the
  order of the two.
- More than one takeover, more than one session, and the reuse of a
  packet identifier across generations (`models/pi-reuse`).

## Properties

| Property | Meaning |
| - | - |
| `OneResultPerFrame` | A submitted frame is in exactly one place: the core's table, the owner's inputs as its one result, or applied. |
| `RetiredFramesResolved` | Once the peer of a generation is removed none of its frames is left in the table; each has its terminal result. |
| `NoForeignEnd` | A result ends only the generation it names: nothing of the old generation is applied to the new one. |
| `OldClosedBeforeNewBound` | The session is bound to the new generation only after the old peer was removed and its socket shut down. |
| `OldSilentOnceNewBound` | Nothing is written to the old socket once the session is bound to the new generation. |
| `ClosedIsShut` | A closed peer has `cancelled` set, its socket shut down and its reason recorded before the core does anything else. |
| `ClosedIsCancelled`, `RemovedIsShut` | With `Deferred`, in place of `ClosedIsShut`: a closed peer is cancelled before the core does anything else, and a peer that left the table has its socket shut down and its reason recorded. |
| `RecordedIsShut`, `NoWriteAfterRecord` | Once the terminal reason of a connection can be read its socket is shut down, and no byte leaves on it after that. |
| `EveryFrameResolved`, `TakeoverCompletes` | Under weak fairness of the core, the owner and the tasks: every accepted frame gets its result at the owner, and a takeover ends bound to the new generation unless the new connection was closed first. |

## The order of `ConnCore::stop`

`ConnCore::stop` runs `cancelled.store(true)`, then `stream.shutdown`,
then `ConnCore::record`. The flag alone would not stop a write that
already passed its check in `write_attempt`; the shutdown does, and the
reason is readable only after it. `NoWriteAfterRecord` therefore holds for
a close decided by the core as for a task that ends by itself.

`mut-record-before-shutdown` is the order the function had before
`967e95d` (record, flag, shutdown), which an earlier version of this model
reported as a reachable write after the reason:

1. core: `ConnCore::record` sets `terminal` (and `observed_ns`);
2. task: `is_cancelled()` reads false;
3. task: `stream.write` accepts the bytes of the frame;
4. core: `cancelled.store(true)`, `stream.shutdown`.

The reason kept is the first recorded. With the present order a task
that ends on the shutdown itself (its write or read fails) may record its
own code before the core records the one it closed with; the model keeps
no code, only whether one is recorded.

## Mutants

| Configuration | Change | Violated |
| - | - | - |
| `mut-no-generation-check` | The owner ends whatever generation the session is bound to on a Failed result. | `NoForeignEnd`: a Failed of generation 1, applied after the open, ends generation 2 |
| `mut-leave-frames-at-close`, `mut-leave-frames-at-close-liveness` | `finish_close` removes the peer and leaves its frames in the table. | `RetiredFramesResolved`; `EveryFrameResolved` |
| `mut-open-without-waiting`, `mut-open-without-waiting-write` | The open of the new generation does not wait for the old peer to leave the table. | `OldClosedBeforeNewBound`; `OldSilentOnceNewBound` |
| `mut-cancel-without-shutdown` | A close sets `cancelled` and leaves the socket open. | `OldSilentOnceNewBound`: the task had checked the flag before it was set |
| `mut-record-before-shutdown` | `ConnCore::stop` records the reason, then sets the flag, then shuts the socket. | `NoWriteAfterRecord`: a write that passed its check leaves after the reason |
| `mut-deferred-without-waiting`, `mut-deferred-without-waiting-write` | With `Deferred`, `finish_close` removes the peer without waiting for its task. | `OldClosedBeforeNewBound`; `OldSilentOnceNewBound`: the socket is still open when the new generation is bound |

The `reach-` configurations of `MTakeoverInFlight` show a Failed result of
the old generation applied while the session is bound to the new one and
absorbed, a frame in flight failed by the close, and one old frame written
while another failed.

## Correspondence

| Model action | Code | Replay |
| - | - | - |
| `CoreSubmit` | `Sink::try_submit`: `peer.closing().is_none()` and `bindings.get(&connection.session) == Some(&generation)`, then `Connection::try_send_outgoing` (`runtime.rs`) | `a_real_puback_write_receipt_returns_its_receive_credit` (`runtime_tests.rs`) for a frame accepted on the bound generation; `a_finished_task_seals_its_queue_and_fails_each_admitted_frame_once` (`net.rs`) for the refusal once the task has ended |
| `CoreCollect` | `collect_written` | `a_real_puback_write_receipt_returns_its_receive_credit` (`runtime_tests.rs`) |
| `NewConnect` | `connect`, `start_takeover` | `stale_puback_during_takeover_with_a_blocked_close_cannot_stop_other_connections` (`runtime_tests.rs`): the old peer is `closing` and the new one is not bound |
| `CoreCloseDue`, `CoreCloseOther` | `Runtime::close`, up to the call of `Connection::close` | `stale_puback_during_takeover_with_a_blocked_close_cannot_stop_other_connections` for the close a takeover makes due; `closing_qos0_without_collecting_written_retires_the_exact_attempt` (`runtime_tests.rs`) for another reason |
| `CoreStopCancel`, `CoreStopShut`, `CoreStopRecord` | `cancelled.store(true)`, `stream.shutdown` and `ConnCore::record` in `ConnCore::stop` | `a_closed_plain_connection_is_shut_when_its_reason_reads_and_writes_nothing_more` (`net.rs`), the three steps with no task step between them; no test places a write between two of them |
| `CoreFinishClose` | `finish_close`; with `Deferred`, after the `is_tls() && !is_finished()` return of `Runtime::close` and the retiring pass of `connections_at` | `closing_qos0_without_collecting_written_retires_the_exact_attempt` (`runtime_tests.rs`); `a_tls_client_reads_close_notify_after_its_disconnect` (`runtime_tests.rs`) for `Deferred` |
| `CoreOpenNew` | `pump_will_connects`: a pending connect is ready only when its `previous` peer is no longer in `peers` | `stale_puback_during_takeover_with_a_blocked_close_cannot_stop_other_connections`; `takeover_waits_for_old_will_admission_and_old_close_cannot_publish_new_will` (`runtime_tests.rs`) |
| `OwnerFrameResult` | `plan_frame_result` (`owner.rs`) | `a_failed_frame_ends_the_connection_of_its_session`: a Failed of another generation or session is absorbed; `qos1_complete_write_releases_once_partial_or_wrong_receipt_never_does` |
| `OwnerClosed` | `Input::ConnectionClosed`; `pop_closing_input` | `copied_lifecycle_effect_and_actual_old_close_cannot_rebind_a_replaced_client_id` (`runtime_tests.rs`, feature `test-hooks`) |
| `OwnerOpen` | `Input::OpenSessionWillAt` applied | `takeover_waits_for_old_will_admission_and_old_close_cannot_publish_new_will` |
| `TaskTake`, `TaskReport` | `ConnCore::pop_outgoing_batch`; `Task::push_completion` in `output_turn` | `output_is_confirmed_only_after_all_bytes_are_written` (`net.rs`) |
| `TaskCheck`, `TaskWrite` | `is_cancelled()` then `stream.write` in `output_turn` and `write_attempt` of the plain task; the TLS task writes without that check | No deterministic replay of the window of `mut-record-before-shutdown` |
| `TaskCancelled` | `is_cancelled()` at the start of `turn` | `a_closed_plain_connection_is_shut_when_its_reason_reads_and_writes_nothing_more`, `cancellation_reports_every_accepted_output_frame` (`net.rs`) |
| `TaskError` | Any other `Err` of `turn` | `eof_in_body_is_distinct_from_clean_eof` (`net.rs`) |
| `TaskSeal`, `TaskFinish` | `Driven::finish`: `ConnCore::stop`, then `ConnCore::finish_task` in its two critical sections; `retire`. A TLS task attempts close_notify first: `a_tls_client_reads_close_notify_after_its_disconnect` (`runtime_tests.rs`) | `a_finished_task_seals_its_queue_and_fails_each_admitted_frame_once`, `cancellation_reports_every_accepted_output_frame` (`net.rs`); `a_link_does_not_read_finished_while_its_task_publishes_its_terminal_facts` (`net/reactor.rs`) |
| `Quiescent` | The end of a run of the model | Not an action of the code |
| `Admit`, `RejectSealed` | `ConnCore::try_send_outgoing`, reached by the coordinator through `Connection::try_send_outgoing` (`start_takeover`) and by a shard through `OutgoingHandle::try_send_outgoing`. Under one mutex, `OutgoingState::enqueue` places DISCONNECT and closes admission to later submissions. | `a_queued_disconnect_is_terminal_for_every_outgoing_handle` (`net.rs`) submits on both handles. |
| `TakeBatch`, `WriteOne` | `ConnCore::pop_outgoing_batch` takes one frame or the first frames of the queue. The socket task writes them in order. | `a_queued_disconnect_is_terminal_for_every_outgoing_handle` reads the admitted PUBLISH followed by DISCONNECT on a TCP socket. |
| `ReaderEnds` | `Driven::finish`: `ConnCore::stop` shuts the socket, then `ConnCore::finish_task` seals the queue and empties it under the output mutex. | `a_finished_task_seals_its_queue_and_fails_each_admitted_frame_once` (`net.rs`): the queued frames never reach the socket and a later submission is refused. |
| `Finished` | The finite scenario has exhausted both producers and drained the socket queue. | The end of the same tests. |

The sidecars list under `replays` the tests that drive the code through
the counterexample of a mutant or a trace of the model: the generation
check of the owner, the close that retires frames whose receipts were not
collected, the takeover that waits for the old peer, the close of a plain
connection and the end of a task. No test replays
`mut-record-before-shutdown`, `mut-cancel-without-shutdown`,
`mut-open-without-waiting` or `mut-deferred-without-waiting` with a frame
in flight: they need a chosen schedule of the core and reactor threads.
`a_closed_plain_connection_is_shut_when_its_reason_reads_and_writes_nothing_more`
fails when `ConnCore::stop` does not shut the socket, with no write in
flight; `cancellation_ends_idle_and_partial_socket_tasks` does not.

## MQTT 5 DISCONNECT on the old connection

`MDisconnectQueue` checks MQTT-3.14.4-1 on one connection from the moment
a DISCONNECT is submitted for it. The coordinator has the DISCONNECT; a
second producer has two PUBLISH packets in order, and the queue holds two
frames. With `--delivery-shards K`, `K >= 1`, the second producer is the
delivery shard on its own thread. With the single owner it stands for a
later submission of the coordinator: every submission takes the same
mutex. The reader can hold a batch outside the mutex while either
producer runs, and may end at any time (`ReaderEnds`).

`TerminalWire` requires DISCONNECT to be the last packet in every wire
prefix that contains it. It does not say that a queued DISCONNECT is
written: `reach-no-disconnect` reaches a run that ends without it, because
the task ended first. The `mut-no-seal` configuration keeps admission open
as in the original sharded queue. The socket can then transmit the shard's
PUBLISH after the coordinator's DISCONNECT. The `reach-drained-prefix`
configuration witnesses an earlier PUBLISH reaching the wire, DISCONNECT
being written next, and the shard's later submission being refused. On the
original implementation the deterministic test detects the late admission
at the shard handle. TLC follows that admitted frame through the socket
write to exhibit the wire-order violation.

What this model leaves out:

- the bound on the writes a shard owns (`ShardIoRoute::reserve_write`). Its
  refusal answers Full, like a full queue: a submission that does not
  happen;
- what a producer does with a refusal. A shard keeps a refused PUBLISH and
  submits it again, or drops a QoS 0 one; `start_takeover` does not submit
  a refused DISCONNECT again, it closes the connection. The model lets
  any producer submit again, which is more than the coordinator does;
- receipts, and a second DISCONNECT (the test shows it refused);
- TCP fragmentation and TLS records: the model observes whole MQTT
  packets;
- the close of the connection and the open of the new generation, which
  `MTakeoverInFlight` models without the DISCONNECT.
