# Frames and receipts of one connection

`MTransportCredit` models the bounded queues between the core and the
transport task of one connection of the runtime of ADR 0044: the
correlated frames the core submits and the receipts that come back. It is
runtime-tier evidence: its actions are the steps of `ConnCore`,
`OutgoingState`, `StalledState` and `Task` in `crates/mqttiumd/src/net.rs`,
of the TLS task in `net/tls.rs`, of `retire` in `net/reactor.rs` and of
`Sink::try_submit`, `collect_written`, `close` and `finish_close` in
`runtime.rs`. It is not a proof of that code.

## Scope

One connection of the product run with the single owner
(`--delivery-shards 0`, the default): the coordinator is the only producer
of frames and the only consumer of receipts. With `--delivery-shards K`,
`K >= 1`, the following exists in the code and is **not** in this model:

- a second bound on the writes a shard owns: `ShardIoRoute::reserve_write`
  takes a slot of the shard before `OutgoingState::enqueue`, and a refusal
  answers Full although the queue has room;
- the receipts of those writes go to a `ShardInbox`
  (`ShardIoRoute::written_event`, in `ConnCore::flush_reports` and in
  `ConnCore::finish_task`), not to `ToCoreState::written`, and the
  acknowledgements of a subscriber go there too;
- the shard is a second producer on another thread
  (`OutgoingHandle::try_send_outgoing`);
- `finish_close` returns until the task has finished and the shard has
  detached the session (`ShardPool::detach_session`).

The name of the directory comes from an earlier sketch in which a frame
held a credit until the core consumed its receipt. The runtime has no such
counter, and the model has none:

- for one connection the output queue (`OutgoingState::queue`,
  `OUTPUT_SLOTS`) is the only bound of the transport on frames in flight;
  `ConnCore::try_send_outgoing` answers Full when it is full. The runtime
  adds bounds that span every connection (`limits.frames`, the two
  terminal inputs reserved per accepted frame, `limits.outgoing_bindings`
  in `Sink::try_submit`): their refusal is also a submission that does not
  happen, and they are not modelled;
- a turn of the task writes frames and collects their receipts, then
  offers them to the lane in order, under one lock, without blocking
  (`ConnCore::flush_reports`). It offers them at the latest when `BURST`
  of them wait;
- what the lane refuses stays in `StalledState::reports`, and no frame is
  taken while a receipt is there;
- one write may carry up to `MAX_COALESCE_FRAMES` frames that have no
  expiry (`ConnCore::pop_outgoing_batch`): they complete or fail together;
- a receipt refused for `RESULT_WAIT`, like a write that exceeds
  `WRITE_TIMEOUT`, ends the task.

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/transport-credit --list
python ../tools/formal/run_tlc.py --models-dir models/transport-credit
```

23 configurations, the whole directory in about half a minute; the
largest explores 69,888 states.

## What is modelled

The core submits frames in order, reads the receipt lane one receipt at a
time, may close the connection at any time, learns that the task ended,
and runs `finish_close`. The task takes one frame, or the first frames of
the queue for one write, disposes of a single frame with NotStarted before
its first byte, or writes: Started at the first byte of a frame that
carries an expiry, Complete for each frame at the last byte. Its receipts
wait in `reports`; `stalledN` counts those the lane has refused. It ends
on cancellation, on one of its deadlines, on frames it took and cannot
start, or on a transport error at any time.

`ConnCore::finish_task` is two steps. `FinishSeal`, under the output lock:
the queue stops accepting (`OutgoingState::accepting_frames`) and its
frames are taken. `FinishPublish`, under the lock of the lanes towards the
core: the pending receipts are offered once, in order, then Failed for the
frames in progress and for the frames taken from the queue; what the lane
has no room for is dropped. `retire` sets `Link::finished` after that
(`FinishFlag`).

`Preserve` selects the path of `Runtime::close`. With a correlated PUBLISH
above QoS 0 pending (`must_preserve_write_facts`) the core reads the lane
first and closes at once only if that left the connection no frame;
otherwise it waits for `Connection::is_finished` and reads the lane up to
its first Failed before `finish_close`. Without it a plain connection runs
`finish_close` at once. A TLS connection never does: `Runtime::close`
returns while its task has not finished, so it always takes the waiting
path. The plain and the TLS task take the same steps here otherwise: a
frame that cannot be started stays a frame in progress in both
(`Task::active`, `Machine::unsealed`).

## What is abstracted away

- `WireProgress` and its lane. Product builds request no wire progress
  (`Sink::try_submit` asks for it under `cfg(test)` or `test-hooks` only).
  In those builds a progress the full lane refuses stalls the receipts
  behind it exactly as a refused receipt does, and at the end of the task
  it makes `finish_task` drop them although the receipt lane has room:
  `TerminalWhenRoom` reads "no report was refused", which this model can
  only reach through the receipt lane.
- The input lane and its `Incoming::Closed` fact. A full input lane is
  backpressure and ends no task (record 0046): a held packet is delivered
  once the core takes values or its task is cancelled, which `MLaneOwed`
  in `models/reactor-wake` states. The only wait bound a task holds on a
  lane is `RESULT_WAIT`, on its receipts.
- Uncorrelated frames (a DISCONNECT, a PUBLISH the runtime does not
  track): they take a place of the queue, and of the lane when they
  report, and belong to no entry of `Runtime::frames`.
- The scheduling of the task and the limits of a turn: a task step may
  happen at any time, and the model lets the task defer any offer up to
  `Burst` receipts. The code defers only the receipts of writes that left
  whole in one call (`output_turn`), and offers at once after NotStarted,
  after a partial write and after a write that needed several calls; the
  TLS task offers after every write. `MReactorWake` covers the scheduling.
- Time: a deadline is a step that is enabled while its cause lasts.
- `collect_written` takes the whole lane in one call and, on a plain
  connection, stops at the first Failed and discards the rest of what it
  took: only Failed receipts can follow it, and their frames get Failed
  from `finish_close`. The model reads one receipt per step and may run
  `finish_close` before the others: the same results. On a TLS connection
  a Failed receipt retires the peer only once the input lane has ended
  (`CoreObserveEnd`) or if the peer is already `closing`; the model
  retires at the receipt in both cases, which adds no accepted frame
  since the queue is sealed by then.
- The three steps of `ConnCore::stop` are in `MTakeoverInFlight`; the
  seal a DISCONNECT puts on the queue is in `MDisconnectQueue`.

## Properties

| Invariant | Meaning |
| - | - |
| `LanesFit` | The queue and the lane hold at most their capacity. |
| `ReportsBounded` | The task holds at most `Burst - 1` receipts of a turn plus those of one more write outside the lane: `BURST - 1 + MAX_COALESCE_FRAMES`, 95, in a product build. |
| `AtMostOneTerminal` | The task produces at most one of NotStarted, Complete, Failed for a frame. |
| `StartedBeforeTerminal` | Started is published only before the terminal receipt of its frame. |
| `NothingAfterFinished` | Nothing is published once `Link::finished` reads true. |
| `TerminalWhenRoom` | A task that ended with room in the receipt lane left a terminal receipt for every accepted frame. |
| `OneResultPerFrame` | An accepted frame is in `Runtime::frames` until its one terminal result is queued; none is queued twice, none for a receipt that correlates with nothing. |
| `RetiredFramesResolved` | Once the peer is removed every accepted frame has its terminal result. |
| `FactsPreserved` | With `Preserve`, a terminal fact the task published is the result of its frame. |
| `EveryFrameResolved` | Under weak fairness of the task, its deadlines and the core's reads, every accepted frame gets its terminal result. |

"Exactly one terminal receipt per accepted frame" is therefore two
statements. The task alone gives at most one, and exactly one when the
receipt lane has room at its end. The core gives exactly one terminal
result in every case, because `finish_close` fails what is left in its
table.

`TerminalWhenRoom` has no exception any more. The transport this model
first described left a window in which a frame submitted after the ending
task had emptied its lane was accepted and got no receipt. `finish_task`
seals the queue under the lock that empties it, so such a frame is
refused: `reach-sealed-refusal` reaches that refusal, and
`mut-no-seal-at-finish` is the earlier behaviour.

## What the implementation does that a reader may not expect

| Configuration | Behaviour |
| - | - |
| `reach-refused-at-end` | `finish_task` offers each terminal fact once and waits for nothing. A task that ends while the core has left the receipt lane full (it ends for that very reason after `RESULT_WAIT`) drops its pending receipts and the Failed of its frames. `RetiredFramesResolved` holds all the same. |
| `reach-fact-replaced` | Without `Preserve` the core may fail a frame whose Complete is already in the lane. Failed says that the write is not known to have completed, never that it did not happen. |
| `reach-receipts-of-two-frames`, `reach-coalesced-write` | The task holds the receipts of several frames at once, and one write carries two frames. An earlier version of this model stated the opposite of the first (at most the two receipts of one frame), which was true of the transport it described. |
| `reach-sealed-refusal` | The core has a frame for a peer it has not closed and the queue is sealed: `Sink::try_submit` gets Disconnected and closes the connection. |

`reach-backpressure` and `reach-mixed-outcome` show a full output queue
with another frame waiting, and one frame written while another failed.

## Mutants

| Configuration | Change | Violated |
| - | - | - |
| `mut-double-receipt`, `mut-double-receipt-at-core` | Complete is queued and the frames stay in progress, so the end of the task fails them as well. | `AtMostOneTerminal`; at the core, `OneResultPerFrame` |
| `mut-lost-receipt-on-close` | `finish_task` empties the queue and fails the frames in progress only. | `TerminalWhenRoom` |
| `mut-drop-unsealed-frame` | Frames the task took and cannot start are forgotten, as the TLS task did before `967e95d`, when `next_write` held the frame in a local variable. | `TerminalWhenRoom`: the frame gets no receipt although the lane has room |
| `mut-no-seal-at-finish` | `finish_task` empties the queue and leaves it open. | `TerminalWhenRoom`: a frame accepted afterwards gets no receipt |
| `mut-no-close-sweep`, `mut-no-close-sweep-liveness` | `finish_close` removes the peer and leaves its frames in the table. | `RetiredFramesResolved`; `EveryFrameResolved` |
| `mut-finished-first` | `Link::finished` is set before the terminal facts are published. | `NothingAfterFinished` |
| `mut-take-while-pending` | A frame is taken while a receipt the lane refused is pending. | `ReportsBounded` |
| `mut-flush-newest-first` | `flush_reports` offers the newest pending receipt first. | `StartedBeforeTerminal` |

## Correspondence

| Model action | Code | Replay |
| - | - | - |
| `CoreSubmit` | `Sink::try_submit`: `Connection::try_send_outgoing`, which is `ConnCore::try_send_outgoing` (`accepting_frames`, then `OUTPUT_SLOTS`), then `frames.insert` (`runtime/sink.rs`) | `a_full_output_queue_refuses_a_frame_until_the_task_takes_one` (`net.rs`) for the bound of the queue. `a_temporarily_full_output_retries_the_same_durable_publish_without_closing` (`runtime_tests.rs`) keeps the refused effect in the outbox and submits it again, with a full substitute for the queue. `accepted_frames_reserve_their_terminal_fact_before_reactor_backpressure` replays the reservation of terminal inputs, which is not this bound |
| `CoreCollect` | `collect_written` | `a_real_puback_write_receipt_returns_its_receive_credit` (`runtime_tests.rs`) |
| `CoreClose` | `Runtime::close`, `Connection::close` | `a_closed_plain_connection_is_shut_when_its_reason_reads_and_writes_nothing_more` (`net.rs`); `closing_qos0_without_collecting_written_retires_the_exact_attempt` (`runtime_tests.rs`) |
| `CoreObserveEnd` | `Incoming::Closed`, or `DrainStatus::Disconnected` from the input lane, in `connections_at` | `eof_in_body_is_distinct_from_clean_eof` (`net.rs`) reads the fact from the lane; in `a_publish_retained_before_enqueue_reports_failure_and_resumes_the_same_claim` (`runtime_tests.rs`) the runtime removes a peer whose client shut its socket |
| `CoreFinishClose` | `finish_close`, from `Runtime::close` and from the retiring pass of `connections_at`. A second sweep at the end of `connections_at` (`stale_frames_dirty`) fails any frame whose peer left the table | `closing_qos0_without_collecting_written_retires_the_exact_attempt` (`runtime_tests.rs`), without `Preserve`. No test pins the path with a QoS 1 PUBLISH pending |
| `TaskTake` | `ConnCore::pop_outgoing_batch`, behind `has_stalled_reports` in `output_turn` and behind the same test in `next_write` of the TLS task | `a_turn_offers_its_receipts_together_and_takes_no_frame_while_one_is_refused` (`net.rs`): a write of two frames, two single writes, then no frame while their receipts are refused |
| `TaskNotStarted` | `WriteFailure::NotStarted` in `output_turn`; `next_write` and `output_turn` of the TLS task | `expired_queued_publication_is_disposed_without_closing_the_transport` (`net.rs`) |
| `TaskStart` | The `started` callback of `write_attempt`; `output_turn` of the TLS task at its first ciphertext bytes | `a_started_receipt_and_peer_ack_precede_the_local_completion_receipt` (`net.rs`) |
| `TaskComplete` | `Task::push_completion`, from `output_turn` of both tasks | `output_is_confirmed_only_after_all_bytes_are_written` (`net.rs`); `a_turn_offers_its_receipts_together_and_takes_no_frame_while_one_is_refused` for a write of two frames |
| `TaskRelease` | None: only the mutant `DoubleReceipt` reaches it | No deterministic replay |
| `TaskFlush` | `ConnCore::flush_reports` | `fresh_reports_preserve_lane_capacity_and_retry_in_order` (`net.rs`): the refused receipts lead, in order; `a_turn_offers_its_receipts_together_and_takes_no_frame_while_one_is_refused` |
| `TaskCancelled` | `is_cancelled()` at the start of `turn` of both tasks, and before a write of the plain task (`output_turn`, `write_attempt`) | `a_closed_plain_connection_is_shut_when_its_reason_reads_and_writes_nothing_more`, `cancellation_reports_every_accepted_output_frame` (`net.rs`) |
| `TaskDeadline` | `Active::deadline` in `write_attempt` and `Encoded::deadline` in the TLS task; `StalledState::stalled_since` against `RESULT_WAIT` in `flush_reports` | `nonreading_peer_expires_one_absolute_write_budget` (`net.rs`) for the write; `a_receipt_refused_for_the_whole_wait_ends_the_task_and_a_full_lane_drops_its_facts` (`net.rs`) for the receipt, with the start of the wait set by the test |
| `TaskTransportError` | Any other `Err` of `turn` | `eof_in_body_is_distinct_from_clean_eof` (`net.rs`) |
| `TaskSealFails` | `WriteFailure::Failed` of `prepare_expiry` in `write_attempt`, the frame staying in `Task::active`; `next_write` in `net/tls.rs`, which keeps the frames in `Machine::unsealed` when `prepare_expiry` or `frame_ciphertext` fails | `a_frame_that_cannot_start_fails_once_and_the_frame_before_it_keeps_its_completion` (`net.rs`) for the plain task; no test makes a TLS frame fail to seal |
| `FinishSeal`, `FinishPublish` | `ConnCore::finish_task`, called by `Driven::finish` of both tasks | `a_finished_task_seals_its_queue_and_fails_each_admitted_frame_once`, `cancellation_reports_every_accepted_output_frame`; `a_receipt_refused_for_the_whole_wait_ends_the_task_and_a_full_lane_drops_its_facts` for a lane with no room (`reach-refused-at-end`) |
| `FinishFlag` | `retire` in `net/reactor.rs`: `finished.store(true)` after `task.finish(code)` | `a_link_does_not_read_finished_while_its_task_publishes_its_terminal_facts` (`net/reactor.rs`) |
| `SocketChanges` | The peer reads or does not | `nonreading_peer_expires_one_absolute_write_budget` |
| `Quiescent` | The end of a run of the model | Not an action of the code |

The sidecar lists under `replays` the tests that drive the code through
a trace of the model. Seven mutants were checked against the test cited
for them, by applying the change to the code, seeing the test fail and
restoring the code: `mut-no-seal-at-finish` and `mut-lost-receipt-on-close`
(`a_finished_task_seals_its_queue_and_fails_each_admitted_frame_once`),
`mut-take-while-pending`
(`a_turn_offers_its_receipts_together_and_takes_no_frame_while_one_is_refused`),
`mut-flush-newest-first` (the same test and
`fresh_reports_preserve_lane_capacity_and_retry_in_order`),
`mut-drop-unsealed-frame` on the plain task
(`a_frame_that_cannot_start_fails_once_and_the_frame_before_it_keeps_its_completion`),
`mut-finished-first`
(`a_link_does_not_read_finished_while_its_task_publishes_its_terminal_facts`)
and `mut-no-close-sweep`
(`closing_qos0_without_collecting_written_retires_the_exact_attempt`).
`a_terminal_turn_publishes_its_facts_before_the_link_reads_finished`
(`net/reactor.rs`) passes with the flag set first and is not a replay.

No test replays the path of `Preserve`, the liveness property, a TLS frame
that cannot be sealed, or `mut-double-receipt` as a change of the code:
the test of the frame that cannot start asserts one receipt for the frame
completed in the turn that ends the task, which is where the mutant gives
two.
