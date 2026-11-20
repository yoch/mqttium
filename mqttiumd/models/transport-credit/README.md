# Frames and receipts of one connection

`MTransportCredit` models the bounded lanes between the core and one
transport task of the runtime of ADR 0044: the frames the core submits
and the receipts that come back. It is runtime-tier evidence: its actions
are the steps of `Lanes`, `Task` and `Lanes::finish` in
`crates/mqttiumd/src/net.rs`, of `tls::Task` in `net/tls.rs`, of `retire`
in `net/reactor.rs` and of `collect_written`, `close` and `finish_close`
in `runtime.rs`. It is not a proof of that code.

The name of the directory comes from an earlier sketch in which a frame
held a credit until the core consumed its receipt. The runtime has no such
counter, and the model has none:

- the frame lane (`sync_channel(OUTPUT_SLOTS)`) is the only bound on
  frames in flight; `Outbound::try_send` refuses when it is full;
- the task queues its receipts in `Lanes::reports`, in publication order,
  publishes them without blocking, and `Lanes::next_frame` takes no frame
  while one is pending;
- a receipt refused for `RESULT_WAIT`, like a write that exceeds
  `WRITE_TIMEOUT`, ends the task.

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/transport-credit --list
python ../tools/formal/run_tlc.py --models-dir models/transport-credit
```

19 configurations, the whole directory in about twenty seconds; the
largest explores 27,582 states.

## What is modelled

The core submits frames in order, reads the receipt lane one receipt at a
time, may close the connection at any time, learns that the task ended,
and runs `finish_close`. The task takes a frame, disposes of it with
NotStarted before its first byte, or writes it: Started at the first byte
of a frame that carries an expiry, Complete at the last. Its receipts go
through `reports`. It ends on cancellation, on one of its deadlines, on a
frame it took and cannot start, or on a transport error at any time;
`Lanes::finish` then offers each pending receipt once, then Failed for
the frame in progress and for each frame still in the lane, and stops at
the first refusal; `retire` sets `Link::finished` after that.

`Preserve` selects the path of `Runtime::close`. With a QoS 1 PUBLISH
pending (`must_preserve_write_facts`) the core waits for `is_finished` and
reads the lane up to its first Failed before `finish_close`; otherwise it
runs `finish_close` at once. The plain and the TLS task take the same
steps here: a frame that cannot be started stays the frame in progress
in both (`Task::active`, `tls::Machine::unsealed`).

## What is abstracted away

- `WireProgress` and its lane, the input lane and its `Closed` fact. A
  full input lane is backpressure and ends no task (record 0046): a held
  packet is delivered once the core takes values or its task is
  cancelled, which `MLaneOwed` in `models/reactor-wake` states. The only
  wait bound a task holds on a lane is `RESULT_WAIT`, on its receipts.
- The scheduling of the task: a task step may happen at any time, which
  is more than the reactor allows. `MReactorWake` covers the scheduling.
- Time: a deadline is a step that is enabled while its cause lasts.
- The contents of frames, uncorrelated frames, and the events the core
  emits. `Started` changes nothing in the core's table.
- The three steps of `Control::stop` are in `MTakeoverInFlight`.

## Properties

| Invariant | Meaning |
| - | - |
| `LanesFit`, `ReportsBounded` | The lanes hold at most their capacity; the task holds at most the two receipts of one frame. |
| `AtMostOneTerminal` | The task produces at most one of NotStarted, Complete, Failed for a frame. |
| `StartedBeforeTerminal` | Started is published only before the terminal receipt of its frame. |
| `NothingAfterFinished` | Nothing is published once `Link::finished` reads true. |
| `TerminalWhenRoom` | A task that ended with room in the receipt lane left a terminal receipt for every accepted frame, but for one accepted after it last looked at the frame lane. |
| `OneResultPerFrame` | An accepted frame is in `Runtime::frames` until its one terminal result is queued; none is queued twice, none for a receipt that correlates with nothing. |
| `RetiredFramesResolved` | Once the peer is removed every accepted frame has its terminal result. |
| `FactsPreserved` | With `Preserve`, a terminal fact the task published is the result of its frame. |
| `EveryFrameResolved` | Under weak fairness of the task, its deadlines and the core's reads, every accepted frame gets its terminal result. |

"Exactly one terminal receipt per accepted frame" is therefore two
statements. The task alone gives at most one, and exactly one when the
receipt lane has room at its end. The core gives exactly one terminal
result in every case, because `finish_close` fails what is left in its
table.

## What the implementation does that a reader may not expect

Two configurations declare a violation that is the behaviour of the code,
not of a mutant. `Lanes::finish` is one offer without waiting, as its
comment says; `RetiredFramesResolved` holds in both.

| Configuration | Behaviour |
| - | - |
| `reach-refused-at-end` | `Lanes::finish` uses `try_send` once per receipt and stops at the first refusal. A task that ends while the core has left the receipt lane full (it ends for that very reason after `RESULT_WAIT`) drops its pending receipts and the Failed of its frames. |
| `reach-late-submit` | A frame the core submits after `Lanes::finish` saw the frame lane empty and before the task's lanes are dropped is accepted and gets no receipt. |

`reach-fact-replaced` shows the other path of `close`: without `Preserve`
the core may fail a frame whose Complete is already in the lane. Failed
says that the write is not known to have completed, never that it did not
happen.

## Mutants

| Configuration | Change | Violated |
| - | - | - |
| `mut-double-receipt`, `mut-double-receipt-at-core` | Complete is queued and the frame stays in progress, so the end of the task fails it as well. | `AtMostOneTerminal`; at the core, `OneResultPerFrame` |
| `mut-lost-receipt-on-close` | `Lanes::finish` fails the frame in progress only and leaves the frames of the lane. | `TerminalWhenRoom` |
| `mut-drop-unsealed-frame` | A frame the task took and cannot start is forgotten, as the TLS task did before `967e95d`, when `next_write` held it in a local variable. | `TerminalWhenRoom`: the frame gets no receipt although the lane has room |
| `mut-no-close-sweep`, `mut-no-close-sweep-liveness` | `finish_close` removes the peer and leaves its frames in the table. | `RetiredFramesResolved`; `EveryFrameResolved` |
| `mut-finished-first` | `Link::finished` is set before the terminal facts are published. | `NothingAfterFinished` |
| `mut-take-while-pending` | `next_frame` takes a frame while a receipt is pending. | `ReportsBounded` |
| `mut-flush-newest-first` | `flush` publishes the newest pending receipt. | `StartedBeforeTerminal` |

`reach-backpressure` and `reach-mixed-outcome` show a full frame lane with
another frame waiting, and one frame written while another failed.

## Correspondence

| Model action | Code | Replay |
| - | - | - |
| `CoreSubmit` | `Sink::try_submit`: `outgoing.try_send`, then `frames.insert` (`runtime.rs`) | `accepted_frames_reserve_their_terminal_fact_before_reactor_backpressure` (`runtime_tests.rs`) |
| `CoreCollect` | `collect_written`, one receipt | `a_real_puback_write_receipt_returns_its_receive_credit` (`runtime_tests.rs`) |
| `CoreClose` | `Runtime::close`, `Connection::close` | `cancellation_reports_every_accepted_output_frame` (`net.rs`) |
| `CoreObserveEnd` | `Incoming::Closed` or a disconnected lane read in `connections` | `noncanonical_header_is_a_malformed_packet_not_clean_eof` (`net.rs`) reads `Closed` from the lane; no test names this step in the runtime alone |
| `CoreFinishClose` | `finish_close`; with `Preserve`, the `retire_code` branch of `connections` | `closing_qos0_without_collecting_written_retires_the_exact_attempt` (`runtime_tests.rs`) |
| `TaskTake` | `Lanes::next_frame` | `expired_queued_publication_is_disposed_without_closing_the_transport` (`net.rs`): three frames, receipts in order |
| `TaskNotStarted` | `WriteFailure::NotStarted` in `output_turn`; `next_write` and `output_turn` of `tls::Task` | `expired_queued_publication_is_disposed_without_closing_the_transport` |
| `TaskStart` | The `started` callback of `write_attempt` | `a_started_receipt_and_peer_ack_precede_the_local_completion_receipt` (`net.rs`) |
| `TaskComplete` | `Attempt::Done` in `output_turn` | `output_is_confirmed_only_after_all_bytes_are_written` (`net.rs`) |
| `TaskRelease` | None: only the mutant `DoubleReceipt` reaches it | No deterministic replay |
| `TaskFlush` | `Lanes::flush` | `a_started_receipt_and_peer_ack_precede_the_local_completion_receipt` |
| `TaskCancelled` | `control.is_cancelled()` in `turn` and in `write_attempt` | `cancellation_ends_idle_and_partial_socket_tasks`, `cancellation_reports_every_accepted_output_frame` (`net.rs`) |
| `TaskDeadline` | `write_timeout` in `write_attempt`; `RESULT_WAIT` in `Lanes::flush` | `nonreading_peer_expires_one_absolute_write_budget` (`net.rs`) for the write; no test replays the receipt deadline |
| `TaskTransportError` | Any `Err` of `turn` | `noncanonical_header_is_a_malformed_packet_not_clean_eof` (`net.rs`) |
| `TaskSealFails` | `WriteFailure::Failed` of `prepare_expiry` in `write_attempt`, the frame staying in `active`; `next_write` in `net/tls.rs`, which keeps the frame in `machine.unsealed` when `prepare_expiry` or `frame_ciphertext` fails | `a_bad_expiry_value_offset_closes_without_touching_socket_bytes` (`net.rs`) for `write_attempt`; no test makes a TLS frame fail to seal |
| `FinishReports`, `FinishFrames` | `Lanes::finish` | `cancellation_reports_every_accepted_output_frame`; no test fills the receipt lane first (`reach-refused-at-end`) |
| `FinishFlag` | `retire` in `net/reactor.rs`: `finished.store(true)` after `task.finish(code)` | `a_terminal_turn_publishes_its_facts_before_the_link_reads_finished` (`net/reactor.rs`) |
| `SocketChanges` | The peer reads or does not | `nonreading_peer_expires_one_absolute_write_budget` |
| `Quiescent` | The end of a run of the model | Not an action of the code |

The sidecar lists under `replays` the tests that drive the code through
a trace of the model: every accepted frame failed once on cancellation,
Started before Complete, NotStarted between two Complete, the write
budget, the terminal facts before `finished`, and the close that retires
a frame whose receipt was never collected. No test replays
`reach-refused-at-end`, `reach-late-submit`, or the counterexamples of
`mut-drop-unsealed-frame`, `mut-double-receipt`, `mut-take-while-pending`
and `mut-flush-newest-first`.
