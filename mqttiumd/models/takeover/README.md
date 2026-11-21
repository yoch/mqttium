# Session takeover with frames in flight

`MTakeoverInFlight` models a client that connects again while frames of
its session are in flight on its previous connection. It is runtime-tier
evidence (ADR 0044): its actions are the steps of `connect`,
`start_takeover`, `pump_will_connects`, `close`, `finish_close` and
`collect_written` in `crates/mqttiumd/src/runtime.rs`, of `Control::stop`
and `Lanes::finish` in `net.rs`, and of `plan_frame_result` in
`crates/mqttiumd-core/src/owner.rs`. It is not a proof of that code.

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/takeover --list
python ../tools/formal/run_tlc.py --models-dir models/takeover
```

14 configurations, the whole directory in about thirty-five seconds; the
largest explores 208,572 states in about ten.

## What is modelled

Two generations of one session: generation 1, to which the session is
bound, and generation 2, which takes it over. A frame belongs to the
generation it was submitted on; its receipt and its result carry that
`ConnectionKey`.

- The core submits a frame only to a peer that is not `closing` and to
  which the session is bound (`Sink::try_submit`).
- `NewConnect` is `connect` and `start_takeover`: the new peer enters the
  table and the old one is marked `closing`.
- A close is `Control::stop` on the core thread, in its three steps:
  `cancelled` is set, the socket is shut down, and only then the terminal
  reason is recorded. The task may run between any two of them.
- The task checks `cancelled`, then writes; a write on a socket that was
  shut down fails and ends the task. A task that ends runs
  `Control::stop` on its own thread and `Lanes::finish`.
- `finish_close` fails every frame the generation still has in
  `Runtime::frames`, queues `ConnectionClosed` and removes the peer. Its
  lanes are not read again. `Preserve` selects the path that first waits
  for the task and reads its facts.
- `pump_will_connects` starts the open of generation 2 only once the peer
  of generation 1 is gone from the table.
- The owner applies a frame result to its frame, and ends a connection on
  Failed only when the session is bound to the generation the result
  names. `ConnectionClosed` detaches only that generation.

## What is abstracted away

- The owner's inputs sit in several lanes (`terminal_inputs`,
  `closing_inputs`, `inputs`) whose relative order the model does not
  fix: `inbox` is a set and the owner takes any input. The one order kept
  is that of `pop_closing_input`: `ConnectionClosed` waits for an older
  ordinary input of its generation.
- The MQTT 5 DISCONNECT sent to the old connection, the 300 ms bound of
  `closing`, the Will barrier and every store operation. A takeover close
  is a close that is due once `closing` is set.
- `Lanes::reports`, Started and NotStarted, which `MTransportCredit`
  models. A frame is written in one step.
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
| `RecordedIsShut`, `NoWriteAfterRecord` | Once the terminal reason of a connection can be read its socket is shut down, and no byte leaves on it after that. |
| `EveryFrameResolved`, `TakeoverCompletes` | Under weak fairness of the core, the owner and the tasks: every accepted frame gets its result at the owner, and a takeover ends bound to the new generation unless the new connection was closed first. |

## The order of `Control::stop`

`Control::stop` runs `cancelled.store(true)`, then `shutdown`, then
`record(code)`. The flag alone would not stop a write that already passed
its check in `write_attempt`; the shutdown does, and the reason is
readable only after it. `NoWriteAfterRecord` therefore holds for a close
decided by the core as for a task that ends by itself.

`mut-record-before-shutdown` is the order the function had before
`967e95d` (record, flag, shutdown), which an earlier version of this model
reported as a reachable write after the reason:

1. core: `Control::record` sets `terminal` (and `observed_ns`);
2. task: `control.is_cancelled()` reads false;
3. task: `stream.write` accepts the bytes of the frame;
4. core: `cancelled.store(true)`, `stream.shutdown(Shutdown::Both)`.

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
| `mut-record-before-shutdown` | `Control::stop` records the reason, then sets the flag, then shuts the socket. | `NoWriteAfterRecord`: a write that passed its check leaves after the reason |

The `reach-` configurations show a Failed result of the old
generation applied while the session is bound to the new one and
absorbed, a frame in flight failed by the close, and one old frame written
while another failed.

## Correspondence

| Model action | Code | Replay |
| - | - | - |
| `CoreSubmit` | `Sink::try_submit`: `peer.closing.is_none()` and `bindings.get(&connection.session) == Some(&generation)` (`runtime.rs`) | `accepted_frames_reserve_their_terminal_fact_before_reactor_backpressure` (`runtime_tests.rs`) |
| `CoreCollect` | `collect_written` | `a_real_puback_write_receipt_returns_its_receive_credit` (`runtime_tests.rs`) |
| `NewConnect` | `connect`, `start_takeover` | `takeover_waits_for_old_will_admission_and_old_close_cannot_publish_new_will` (`runtime_tests.rs`) |
| `CoreCloseDue`, `CoreCloseOther` | `Runtime::close`, up to the call of `Connection::close` | `stale_puback_during_takeover_with_a_blocked_close_cannot_stop_other_connections` (`runtime_tests.rs`) |
| `CoreStopCancel`, `CoreStopShut`, `CoreStopRecord` | `cancelled.store(true)`, `stream.shutdown` and `record` in `Control::stop` | `cancellation_ends_idle_and_partial_socket_tasks` (`net.rs`); no test places a write between the steps |
| `CoreFinishClose` | `finish_close` | `closing_qos0_without_collecting_written_retires_the_exact_attempt` (`runtime_tests.rs`) |
| `CoreOpenNew` | `pump_will_connects`: a pending connect is ready only when its `previous` peer is no longer in `peers` | `takeover_waits_for_old_will_admission_and_old_close_cannot_publish_new_will` |
| `OwnerFrameResult` | `plan_frame_result` (`owner.rs`) | `a_failed_frame_ends_the_connection_of_its_session`: a Failed of another generation or session is absorbed; `qos1_complete_write_releases_once_partial_or_wrong_receipt_never_does` |
| `OwnerClosed` | `Input::ConnectionClosed`; `pop_closing_input` | `copied_lifecycle_effect_and_actual_old_close_cannot_rebind_a_replaced_client_id` (`runtime_tests.rs`, feature `test-hooks`) |
| `OwnerOpen` | `Input::OpenSessionWillAt` applied | `takeover_waits_for_old_will_admission_and_old_close_cannot_publish_new_will` |
| `TaskTake`, `TaskReport` | `Lanes::next_frame`; Complete in `output_turn` | `output_is_confirmed_only_after_all_bytes_are_written` (`net.rs`) |
| `TaskCheck`, `TaskWrite` | `control.is_cancelled()` then `stream.write` in `write_attempt` | No deterministic replay of the window of `mut-record-before-shutdown` |
| `TaskCancelled` | `control.is_cancelled()` at the start of `turn` | `cancellation_reports_every_accepted_output_frame` (`net.rs`) |
| `TaskError` | Any `Err` of `turn` | `noncanonical_header_is_a_malformed_packet_not_clean_eof` (`net.rs`) |
| `TaskFinish` | `Driven::finish`: `Control::stop`, `Lanes::finish`; `retire` | `cancellation_reports_every_accepted_output_frame`; `a_terminal_turn_publishes_its_facts_before_the_link_reads_finished` (`net/reactor.rs`) |
| `Quiescent` | The end of a run of the model | Not an action of the code |

The sidecar lists under `replays` the tests that drive the code through
the counterexample of a mutant or a trace of the model: the generation
check of the owner, the close that retires frames whose receipts were not
collected, and the takeover that waits for the old peer. No test replays
`mut-record-before-shutdown`, `mut-cancel-without-shutdown` or
`mut-open-without-waiting` with a frame in flight: they need a chosen
schedule of the core and reactor threads.
