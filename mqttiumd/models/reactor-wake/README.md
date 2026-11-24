# Wake-up protocols of the reactor runtime

Three models of the places where one thread of the runtime of ADR 0044
gets another to run without a timer. They are runtime-tier evidence: TLA+
models of the thread and queue protocol with mutant configurations. Each
encodes the order of the atomic steps of the Rust code it names. None is a
proof of that code, and none replaces its tests.

| Module | Code | Question |
| - | - | - |
| `MWake` | `crates/mqttiumd/src/wake.rs` (`Wake`, `Signalled`), the main loop of `runtime.rs`, the worker loop of `writer.rs` | Can a value published towards a waiting thread stay unseen until the timeout of `Wake::wait`? |
| `MReactorWake` | `crates/mqttiumd/src/net/reactor.rs` (`Link::notify`, `Shared::{send, kick, hold, release, take, sleep}`, `Reactors::hold`, `run`), `Outbound` in `net.rs` | Can a frame the core published stay in its lane until readiness, a tick or the timeout of the poll, once the core's hold is released? |
| `MLaneOwed` | `publish`, `Lanes::deliver`, `Lanes::flush` in `net.rs`; `Link::owe`, `Link::consumed`; `Inbound::try_recv`; record 0046 | Is a value a full lane refused published, and by what? Does input pressure ever end a task? |

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/reactor-wake --list
python ../tools/formal/run_tlc.py --models-dir models/reactor-wake
```

48 configurations, each under a few seconds; the largest explores 52,150
states.

## MWake

A producer publishes its value, then calls `Wake::wake`:
`pending.swap(true)`, and only when the flag was clear, lock,
`notify_one`, unlock. The consumer makes passes and enters `Wake::wait`
only after a pass without progress; `wait` locks, consumes `pending` with
`swap(false)` under the lock, and otherwise blocks in the condition
variable, which releases the lock in the same step.

Modelled: any number of producers, a bounded lane, the lock as a variable,
the three producer steps and the consumer loop. A pass is one step that
takes everything published or finds nothing.

Abstracted away: the timeout of `wait` is absent, so a value that would
need it is a violation here. Spurious returns of the condition variable
and lock poisoning are absent. The core's many sources are one lane.
There is one waiter, as in the runtime, where the core alone waits on
`runtime.wake` and the store worker alone on `requests`; `notify_one` and
the single `pending` flag do not serve two. The short timer of the store
worker for a reply lane the core has not drained (`stalled`) is not a
wake-up path and is not modelled.

| Property | Meaning |
| - | - |
| `NoLostWakeup` | A consumer blocked in the condition variable with a value published has a notification on its way. |
| `NoSleepOnSignal` | A blocked consumer never sits on a `pending` it did not consume. |
| `EveryValueConsumed` | Under weak fairness of the consumer and of the steps that finish a signal, every published value is consumed. |

| Mutant | Change | Counterexample |
| - | - | - |
| `mut-wake-before-publish` | The producer signals, then publishes. | The consumer is woken, makes a pass that finds nothing and waits again; the value arrives with no signal left. |
| `mut-check-outside-lock` | `wait` tests `pending` before it takes the lock and does not look again. | The producer sets `pending` and notifies between the test and the wait; nobody is waiting yet. |
| `mut-clear-after-pass` | The consumer clears `pending` after its pass (`Wake::take`) instead of inside `wait`. | A value published after the pass looked at its source has its signal erased before the wait. |

Each mutant has a safety configuration (`NoLostWakeup`) and a liveness
configuration (`EveryValueConsumed`). The three `reach-` configurations
show that the passing ones are not vacuous: a signal raised during a pass
is found by the check under the lock, a producer notifies a blocked
consumer, and a producer finds `pending` set and skips the lock.

## MReactorWake

`Outbound::try_send` publishes a frame, then `Link::notify` marks the link
with `dirty.swap(true)` and, when it was clear, `Shared::send` pushes
`Message::Run`, records a debt with `owing.store(true)`, and calls `kick`
unless `held` reads set. `kick` takes the debt with `owing.swap(false)`
and, if it took one, wakes the mio waker when `sleeping` reads set.
`Reactors::hold` sets `held`; the drop of its guard clears `held` and then
calls `kick`. `Runtime::turn` holds for the length of one pass, so each
reactor thread is woken once per pass. The reactor takes its queue, clears
`dirty` for each `Run` before it queues the task, runs the tasks, sets
`sleeping`, looks at its queue again, polls with a zero timeout if a
message is there, and clears `sleeping`.

Modelled: several links on one reactor thread, several producers of which
some hold, every producer step from the publication to the wake, the hold
and its release, the seven reactor steps, the waker as one bit a poll
consumes.

Abstracted away: socket readiness, the tick of a waiting task and the
timeout of the poll are absent, so a frame that would need one of them is
a violation here. A hold is released under fairness: it lasts one pass of
its producer. A message queued under a hold waits for the release or, in
the code, for the reactor's next timeout if that comes first; only the
release is in the model. One reactor thread stands for the pool: `hold`
and its release go over each thread in turn with the same two steps. A
turn of a task is one step that takes every frame of its lane. `Register`
and `Stop`, the fairness bound `TURNS` and panics are absent.

| Property | Meaning |
| - | - |
| `RunOnce`, `DirtyHasRun` | A marked link has exactly one `Run` on its way: `notify` is idempotent until the task runs, and the queue is bounded by the number of links. |
| `NoStrandedFrame` | A frame in a lane has a `Run` queued or taken, its task in `ready`, or a producer still notifying. |
| `DebtHasPayer` | A recorded debt has someone who will pay it: a producer between its record and its look at the hold, a kick that has not taken the debt yet, or a live hold. |
| `NoLostWake` | A reactor blocked in its poll with a message in its queue has a wake on its way: a kick that took the debt, a producer that has not settled its debt, or a debt with a payer. |
| `EveryFrameTaken` | Under weak fairness of the reactor, of the steps that finish a notification and of the release of a hold, every published frame is taken. |

| Mutant | Change | Counterexample |
| - | - | - |
| `mut-check-before-push` | The producer reads `sleeping` before it pushes its message. | It reads clear; the reactor sets `sleeping`, finds its queue empty and blocks; the message is pushed with no wake. |
| `mut-poll-without-recheck` | The reactor polls right after it set `sleeping`. | A producer pushed and read `sleeping` clear just before; nothing ends the poll. |
| `mut-clear-dirty-after-run` | `dirty` is cleared after the tasks ran. | A frame published after its task took its turn finds the link still marked and sends nothing; the mark is then erased. |
| `mut-debt-read-before-release` | The release kicks, then clears `held`. | The kick finds no debt; a second producer pushes, records its debt and reads `held` still set; the hold is then cleared and nobody pays. |
| `mut-hold-read-before-debt` | `send` reads `held` before it records the debt. | It reads the hold set; the holder releases and its kick finds no debt; the debt is recorded after, with no hold left to pay it. |

Each mutant has a safety and a liveness configuration. The two debt
mutants lose a message only when a second thread sends while another
holds: with one sending thread the steps of `send` and of the release
cannot interleave. In the executable the core thread is the only one that
sends and holds, so the order the code keeps is a margin there; it is
needed as soon as two threads share a pool, as the unit tests of the
crate do through `Reactors::shared`.

The `reach-` configurations show a wake of a blocked reactor, the re-check
of the queue saving a message whose producer read `sleeping` clear, a
coalesced notification, a second `Run` sent while the tasks run, and a
message queued under a hold whose wake-up is given at the release.

## MLaneOwed

`publish` offers a value to a lane, marks `Link::owed` when it is refused
and offers it once more. A value refused twice is held.
`Inbound::try_recv` calls `Link::consumed` after it took a value:
`owed.swap(false)`, then `notify` if it was set. A task that holds a value
reports `waiting` and is run again every `TICK`.

What follows a refusal depends on the value (decision record 0046):

- a packet for the input lane has no bound. The task holds it, reads
  nothing more from its socket, and the peer is slowed by its transport.
  The deadlines of a connection stay with the runtime, which cancels the
  task when one fires (`Connection::close` sets `cancelled` and notifies
  the link);
- a receipt has one: held for `RESULT_WAIT` it ends the task with
  `write_result_queue_full` or `wire_progress_unavailable`.

Modelled: one lane, the three steps of `publish`, the two steps of the
consumer, cancellation by the core, the wait bound of a receipt as a bit
that time sets, the tick as a constant. `Bounded` selects receipts or
packets.

Abstracted away: `scheduled` stands for a notification on its way to the
task; `MReactorWake` is the model that shows it runs the task. One lane
stands for the three that share the mark, on the argument that a run which
still finds a lane full marks it again. The runtime's own deadlines are
one step, `CoreCancel`, about which nothing is assumed.

| Property | Meaning |
| - | - |
| `NoLostOwed` | A task resting on a held value has its next run assured without the tick: a notification on its way, or the mark set with the lane still full or the consumer between its take and its look at the mark. |
| `EndedOnlyAfterDeadline` | The queue-full end happens only after the wait bound elapsed. |
| `PacketNeverEndsTask` | Input pressure never ends a task: only a receipt has a wait bound. |
| `CancelRunsTask`, `CancelledEnds` | A cancelled task has its run assured and ends, with no tick and whatever it holds. |
| `HeldEndsOrPublishes` | A refused value does not stay held. `liveness-packet-consumer`: a held packet is delivered once the core takes values, with no tick and no deadline. `liveness-receipt-tick`: a held receipt is published or ends the task, with the tick and time passing, whatever the core does. |

So a held packet is eventually delivered or its task is cancelled, and
nothing else ends its wait:

| Configuration | What it shows |
| - | - |
| `packet-stalled-core` | A packet held while the core takes nothing and cancels nothing stays held for ever, tick or not. This is record 0046, not a defect: the property is declared violated for the unmutated protocol. In the runtime the step that ends such a wait is a deadline of the connection; Keep Alive is one, and `last_packet` is refreshed when the core takes a packet, not when its bytes arrive. |
| `receipt-no-tick-stalled-core` | A receipt held with no tick and a stalled core never reaches its deadline: the tick is what makes `RESULT_WAIT` fire. This is not the implementation, where `Lanes::pending` makes the task report `waiting`. |
| `mut-input-deadline` | A packet with a wait bound, as before record 0046, ends its task under pressure: `PacketNeverEndsTask` is violated. |
| `mut-single-attempt`, `mut-single-attempt-liveness` | Without the second attempt and without the tick the wake-up is lost: the consumer empties the lane between the first attempt and the mark, sees no mark, and never takes another value. |
| `mut-single-attempt-ticked` | The same mutant passes with the tick: a timing test cannot see this defect, only 5 ms of delay. |

`liveness-packet-consumer` passes without the tick. The code still ticks
a task that holds a packet (`Lanes::pending` reports it as waiting),
although such a task has no deadline of its own since record 0046: the
tick is then a second way to the same run, not a requirement.

## Correspondence

| Model action | Module | Code | Replay |
| - | - | - | - |
| `ProducerPublish` | `MWake`, `MReactorWake` | `Signalled::try_send` before its `wake` (`wake.rs`); `Outbound::try_send` before its `notify` (`net.rs`) | `no_published_item_waits_for_the_timeout` (`wake.rs`), a stress run; `a_publication_is_delivered_without_waiting_for_a_timer` (`tests/reactor_wake.rs`) on the executable |
| `ProducerSwap` | `MWake`, `MReactorWake` | `pending.swap(true)` in `Wake::wake`; `dirty.swap(true)` in `Link::notify` | `a_signal_raised_before_the_wait_is_consumed_without_waiting` (two signals, one consumed); `a_notification_runs_the_task_once_and_a_waiting_task_is_ticked` (two notifications, at most two turns) |
| `ProducerNotify` | `MWake` | lock, `notify_one`, unlock in `Wake::wake` | `a_signal_from_another_thread_ends_the_wait_early` |
| `ProducerLate` | `MWake` | None: only the mutant `WakeBeforePublish` reaches it | No deterministic replay: the counterexample needs a chosen thread schedule |
| `ConsumerPass` | `MWake` | `runtime.turn()` and the `progress` counter in `run_inner`; the request lanes of the worker loop in `writer.rs` | `no_published_item_waits_for_the_timeout` |
| `ConsumerClear` | `MWake` | None: only the mutant `ClearAfterPass` reaches it (`Wake::take` exists and is not called by the loops) | No deterministic replay |
| `ConsumerLock`, `ConsumerCheck` | `MWake` | `lock.lock()` and `pending.swap(false)` in `Wake::wait` | `a_signal_raised_before_the_wait_is_consumed_without_waiting` |
| `ConsumerRelock` | `MWake` | The return of `ready.wait_timeout` in `Wake::wait` | `a_signal_from_another_thread_ends_the_wait_early` |
| `ProducerHold` | `MReactorWake` | `Reactors::hold` through `Shared::hold`: `held.store(true)`; `net::hold_wakes` in `Runtime::turn` | `a_held_wake_up_is_given_when_the_hold_ends` (`net/reactor.rs`) |
| `ProducerPreload` | `MReactorWake` | None: only the mutant `CheckBeforePush` reaches it | No deterministic replay |
| `ProducerPush`, `ProducerOwe`, `ProducerReadHeld` | `MReactorWake` | `Shared::send`: push under the queue lock, `owing.store(true)`, `held.load()` | `a_held_wake_up_is_given_when_the_hold_ends`: a notification under a hold; the lost-message interleavings have no deterministic replay |
| `ProducerRelease` | `MReactorWake` | `Drop for Held` through `Shared::release`: `held.store(false)`, then `kick` | `a_held_wake_up_is_given_when_the_hold_ends` |
| `ProducerKick`, `ProducerLoad`, `ProducerWake` | `MReactorWake` | `Shared::kick`: `owing.swap(false)`, `sleeping.load()`, `waker.wake()` | `a_notification_runs_the_task_once_and_a_waiting_task_is_ticked`; `a_held_wake_up_is_given_when_the_hold_ends` |
| `ProducerUnhold` | `MReactorWake` | None: only the mutant `DebtReadBeforeRelease` reaches it | No deterministic replay: it needs a second sending thread |
| `ReactorTake`, `ReactorHandle` | `MReactorWake` | `Shared::take`: `std::mem::take(&mut *queue)`; the `Message::Run` arm of `run` | `a_notification_runs_the_task_once_and_a_waiting_task_is_ticked` |
| `ReactorRun` | `MReactorWake` | The `ready` loop of `run` and `Driven::turn` | `a_task_runs_on_readiness_and_rests_between_events` |
| `ReactorLateClear` | `MReactorWake` | None: only the mutant `ClearDirtyAfterRun` reaches it | No deterministic replay |
| `ReactorSleep`, `ReactorRecheck`, `ReactorPoll`, `ReactorAwake` | `MReactorWake` | `Shared::sleep`, called by `run`: `sleeping.store(true)`, `pending`, `poll.poll`, `sleeping.store(false)` | `a_task_runs_on_readiness_and_rests_between_events` (an idle task is not run); `an_idle_broker_rests` (`tests/reactor_wake.rs`) on the executable |
| `TaskProduce` | `MLaneOwed` | `Lanes::hold` then `Lanes::deliver` in `input_turn`; `Lanes::report` then `Lanes::flush` | `input_saturation_is_backpressure_and_loses_no_packet` (`net.rs`) |
| `TaskRun` | `MLaneOwed` | `Message::Run` after `Link::consumed` or `Connection::close`, or the tick of a task that reported `waiting`; `control.is_cancelled()` at the start of `turn` | `a_notification_runs_the_task_once_and_a_waiting_task_is_ticked`: a waiting task is run without an event; `consumed` runs the task only after `owe`. `cancellation_ends_idle_and_partial_socket_tasks` (`net.rs`) |
| `TaskTry1`, `TaskOwe`, `TaskTry2` | `MLaneOwed` | `publish` in `net.rs` | `input_saturation_is_backpressure_and_loses_no_packet`: the lane is full, one packet is held, and every packet arrives in order once the core takes values; the lost wake-up of `SingleAttempt` has no deterministic replay |
| `DeadlineElapses` | `MLaneOwed` | `stalled` and `RESULT_WAIT` in `Lanes::flush`; nothing in `Lanes::deliver` | `input_saturation_is_backpressure_and_loses_no_packet` shows that a packet has no bound; no test lets a receipt wait for `RESULT_WAIT` |
| `ConsumerTake`, `ConsumerConsumed` | `MLaneOwed` | `Inbound::try_recv`, `Link::consumed` | `a_notification_runs_the_task_once_and_a_waiting_task_is_ticked` |
| `CoreCancel` | `MLaneOwed` | `Connection::close`: `Control::stop`, `link.notify()` | `cancellation_ends_idle_and_partial_socket_tasks` |
| `Quiescent` | all three | The end of a run of the model | Not an action of the code |

The sidecars list under `replays` only tests that exist in the files they
name. The interleavings that lose a wake-up are not replayed by any
deterministic test: they need a chosen schedule of two or three threads,
which the crate has no harness for. `no_published_item_waits_for_the_timeout`
and the two tests of `tests/reactor_wake.rs` observe the property on real
threads and on the executable; they would not reliably fail on a mutant.
