# Wake-up protocols of the reactor runtime

Three models of the places where one thread of the runtime of ADR 0044
gets another to run without a timer. They are runtime-tier evidence: TLA+
models of the thread and queue protocol with mutant configurations. Each
encodes the order of the atomic steps of the Rust code it names. None is a
proof of that code, and none replaces its tests.

| Module | Code | Question |
| - | - | - |
| `MWake` | `crates/mqttiumd/src/wake.rs` (`Wake`, `Signalled`), the main loop of `runtime.rs`, the worker loop of `writer.rs` | Can a value published towards a waiting thread stay unseen until the timeout of `Wake::wait`? |
| `MReactorWake` | `crates/mqttiumd/src/net/reactor.rs` (`Link::notify`, `Shared::send`, `Shared::kick`, `Shared::release`, `Shared::sleep`, `Reactors::hold`, `run`), `ConnCore::try_send_outgoing` in `net.rs` | Can a frame the core published stay in its lane until readiness, a tick or the timeout of the poll, once the core's hold is released? |
| `MLaneOwed` | `ConnCore::push_incoming_batch`, `ConnCore::flush_held_incoming`, `ConnCore::flush_reports`, `IncomingBatch::finish` in `net.rs`; `Link::owe`, `Link::consumed`; record 0046 | Is a value a full lane refused published, and by what? Does input pressure ever end a task? |

## Scope

The three models describe the default runtime, `--delivery-shards 0`: one
core thread, which alone publishes frames, notifies links, holds the
wake-ups and takes what the transport tasks publish; the reactor threads;
and the store worker where the storage mode has one.

With `--delivery-shards K >= 1` (record 0066) more threads take part, and
the models were not extended to them:

- `MWake`: each shard worker waits on a `Wake` of its own
  (`DeliveryShardWorker::run_loop`, the `wake` of its `ShardStoreHandle`).
  It is one waiter per signal, its loop waits only after a pass without
  progress, and the producers that were read publish before they signal
  (`ShardInbox::push_batch`, the `Signalled` lane of the Coordinator, the
  reply lanes of the store worker). That was read in the code, not
  searched exhaustively; no test replays it.
- `MReactorWake`: the shard workers send to the reactors and hold as well,
  and release their hold before they wait. At `9dfd568` a shard kept it
  through its wait; the section "A hold kept through a wait" below says
  what that broke and how it is checked now.
- `MLaneOwed`: the acknowledgements of a subscriber (PUBACK, PUBREC,
  PUBCOMP) and the write receipts of frames a shard emitted do not take
  the lanes of the model. They go to the `ShardInbox` of the shard, under
  a budget (`ShardAckBudget::reserve`) whose return notifies the task
  (`ShardAckBudget::release`). Without credit a read turn reads one
  packet: one that needs no credit goes on, an acknowledgement waits in
  the task until credit returns, and nothing behind it is read meanwhile
  (`ConnCore::plan_read`, report 0046). That register, re-check and
  notify protocol is not modelled.

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/reactor-wake --list
python ../tools/formal/run_tlc.py --models-dir models/reactor-wake
```

49 configurations, the whole directory in about a minute and a half; the
largest explores 52,150 states.

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
There is one waiter per signal, as in the runtime, where the core alone
waits on `runtime.wake` and the store worker alone on `requests`;
`notify_one` and the single `pending` flag do not serve two. The short
timer of the store worker for a reply lane its owner has not drained
(`stalled`) is not a wake-up path and is not modelled.

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

`ConnCore::try_send_outgoing` publishes a frame, then `Link::notify` marks
the link with `dirty.swap(true)` and, when it was clear, `Shared::send`
pushes `Message::Run`, records a debt with `owing.store(true)`, and calls
`kick` unless `held` reads set. `Shared::kick` takes the debt with
`owing.swap(false)` and, if it took one, wakes the mio waker when
`sleeping` reads set. `Reactors::hold` sets `held` on every reactor thread
(`Shared::hold`); the drop of its guard calls `Shared::release`, which
clears `held` and then calls `kick`. `Runtime::turn` holds for the length
of one pass, so each reactor thread is woken once per pass. The reactor
takes its queue (`Shared::take`), clears `dirty` for each `Run` before it
queues the task, runs the tasks, and in `Shared::sleep` sets `sleeping`,
looks at its queue again, polls with a zero timeout if a message is
there, and clears `sleeping`.

Modelled: several links on one reactor thread, several producers of which
some hold, every producer step from the publication to the wake, the hold
and its release, the seven reactor steps, the waker as one bit a poll
consumes. `held` is one flag whoever holds, as in the code: the release of
one holder ends the hold of all (`safety-two-holders`).

Abstracted away: socket readiness, the tick of a waiting task and the
timeout of the poll are absent, so a frame that would need one of them is
a violation here. A hold is released under fairness: the model assumes it
lasts one pass of its producer, which is what `Runtime::turn` does. A
message queued under a hold waits for the release or, in the code, for
the reactor's next timeout if that comes first; only the release is in
the model. One reactor thread stands for the pool: `hold` and its release
go over each thread in turn with the same two steps. A turn of a task is
one step that takes every frame of its lane. `Register` and `Stop`, the
fairness bound `TURNS`, panics, and a reactor thread that left its loop
(`dead`, `Shared::abandon`) are absent.

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
cannot interleave. In the default runtime the core thread is the only one
that sends and holds, so the order the code keeps is a margin there. It
is needed as soon as two threads share a pool: the unit tests of the
crate do through `Reactors::shared`, and so do the shard workers of the
sharded mode.

The `reach-` configurations show a wake of a blocked reactor, the re-check
of the queue saving a message whose producer read `sleeping` clear, a
coalesced notification, a second `Run` sent while the tasks run, and a
message queued under a hold whose wake-up is given at the release.

### A hold kept through a wait

At `9dfd568` the assumption "a hold lasts one pass of its producer" was
false of one caller. `DeliveryShardWorker::run_loop`, which runs only with
`--delivery-shards K >= 1`, took `hold_wakes` of `net.rs` at the top of each
iteration and waited at the bottom of the same iteration, when the pass
made no progress, with the guard still alive: `Wake::wait` for at most
`IDLE_TICK` (20 ms), or `BACKPRESSURE_TICK` (1 ms) while it had work it
could not place. The hold ended when that wait returned.

What this broke, in the terms of the model:

- **No invariant.** `DebtHasPayer` and `NoLostWake` count a live hold as
  a payer and say nothing of how long it lives. `Shared::release` clears
  the hold before it reads the debt, whoever set the hold, and every
  release kicks: no wake-up was lost.
- **The premise of `EveryFrameTaken`.** The property is checked under
  weak fairness of the release, which the shard discharged only by the
  timeout of its wait. The configuration `hold-kept-liveness` checks the
  property without that fairness and is declared violated: a holder takes
  its hold and keeps it, a message is queued for a blocked reactor under
  it, and nothing gives the wake-up. In the code it was a delay of at most
  the wait of the holder, 20 ms, for a sender that holds nothing: a
  reactor thread that returns unused shard acknowledgement credit
  (`ShardAckBudget::release`) to a reader on another reactor thread.

Since the validation of the lot 10 head the guard covers the work of a
pass and drops before the wait, as in `Runtime::turn`, and the fairness
of `FairSpec` holds for every holder. Test builds assert, where a shard
waits, that it holds nothing:
`an_idle_shard_waits_without_holding_the_reactors_wake_ups` fails on
`9dfd568` and passes since. `hold-kept-liveness` stays as the
counterexample of the earlier behaviour.

## MLaneOwed

A transport task offers what it has for the core to a lane, marks
`Link::owed` when the lane has no room for all of it, and offers it once
more; what is refused twice is held in the task (`StalledState`). The
packets take this loop in `ConnCore::push_incoming_batch` and, for what
is already held, in `ConnCore::flush_held_incoming`; the write receipts
and the wire progress take it in `ConnCore::flush_reports`. The consumer
calls `Link::consumed` after it made room: `owed.swap(false)`, guarded by
a load, then `notify` if it was set. A task that holds anything reports
`waiting` and is run again every `TICK`.

For packets the core borrows a batch (`IncomingBatch::take`): the packets
it took still count against the lane until `IncomingBatch::finish` puts
back those it did not consume and clears `incoming_borrowed`. That store
is the step that makes room, `ConsumerTake` in the model, and
`Link::consumed` follows it. The task reads the count under the lock of
the lane, after its mark: the order the model needs is kept.

What follows a refusal depends on the value (decision record 0046):

- a packet for the input lane has no bound. The task holds it, reads
  nothing more from its socket, and the peer is slowed by its transport.
  The deadlines of a connection stay with the runtime, which cancels the
  task when one fires (`Connection::close` sets `cancelled` and notifies
  the link);
- a receipt has one: held for `RESULT_WAIT` it ends the task with
  `write_result_queue_full` or `wire_progress_unavailable`.

Modelled: one lane, the three steps of the loop, the two steps of the
consumer, cancellation by the core, the wait bound of a receipt as a bit
that time sets, the tick as a constant. `Bounded` selects receipts or
packets.

Abstracted away: `scheduled` stands for a notification on its way to the
task; `MReactorWake` is the model that shows it runs the task. One value
stands for what a batch still holds after an attempt. One lane stands for
the three that share the mark, on the argument that a run which still
finds a lane full marks it again. The wait bound of a receipt runs from
the first refusal with no receipt published since (`stalled_since`); the
model lets it elapse at any time a value is in hand, which is more than
the code allows and changes no invariant. The runtime's own deadlines are
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
| `packet-stalled-core` | A packet held while the core takes nothing and cancels nothing stays held for ever, tick or not. This is record 0046, not a defect: the property is declared violated for the unmutated protocol. In the runtime the step that ends such a wait is a deadline of the connection. Keep Alive is one when the client asked for it: its clock runs from the last complete packet the transport received (`Connection::last_heard`), and a task that holds a packet reads no other, so the clock is not refreshed while the wait lasts. |
| `receipt-no-tick-stalled-core` | A receipt held with no tick and a stalled core never reaches its deadline: the tick is what makes `RESULT_WAIT` fire. This is not the implementation, where a task that holds a receipt reports `waiting`. |
| `mut-input-deadline` | A packet with a wait bound, as before record 0046, ends its task under pressure: `PacketNeverEndsTask` is violated. |
| `mut-single-attempt`, `mut-single-attempt-liveness` | Without the second attempt and without the tick the wake-up is lost: the consumer empties the lane between the first attempt and the mark, sees no mark, and never takes another value. |
| `mut-single-attempt-ticked` | The same mutant passes with the tick: a timing test cannot see this defect, only 5 ms of delay. |

`liveness-packet-consumer` passes without the tick. The code still ticks
a task that holds a packet (it reports `waiting` while its `StalledState`
holds anything), although such a task has no deadline of its own since
record 0046: the tick is then a second way to the same run, not a
requirement.

## Correspondence

| Model action | Module | Code | Replay |
| - | - | - | - |
| `ProducerPublish` | `MWake`, `MReactorWake` | `Signalled::try_send` before its `wake` (`wake.rs`); the enqueue of `ConnCore::try_send_outgoing` before its `notify` (`net.rs`) | `no_published_item_waits_for_the_timeout` (`wake.rs`), a stress run; `a_publication_is_delivered_without_waiting_for_a_timer` (`tests/reactor_wake.rs`) on the executable |
| `ProducerSwap` | `MWake`, `MReactorWake` | `pending.swap(true)` in `Wake::wake`; `dirty.swap(true)` in `Link::notify` | `a_signal_raised_before_the_wait_is_consumed_without_waiting` (two signals, one consumed); `a_notification_runs_the_task_once_and_a_waiting_task_is_ticked` (two notifications, at most two turns) |
| `ProducerNotify` | `MWake` | lock, `notify_one`, unlock in `Wake::wake` | `a_signal_from_another_thread_ends_the_wait_early` |
| `ProducerLate` | `MWake` | None: only the mutant `WakeBeforePublish` reaches it | No deterministic replay: the counterexample needs a chosen thread schedule |
| `ConsumerPass` | `MWake` | `runtime.turn()` and the `progress` counter in `run_inner`; the request lanes of the worker loop in `writer.rs` | `no_published_item_waits_for_the_timeout` |
| `ConsumerClear` | `MWake` | None: only the mutant `ClearAfterPass` reaches it (`Wake::take` exists and is not called by the loops) | No deterministic replay |
| `ConsumerLock`, `ConsumerCheck` | `MWake` | `lock.lock()` and `pending.swap(false)` in `Wake::wait` | `a_signal_raised_before_the_wait_is_consumed_without_waiting` |
| `ConsumerRelock` | `MWake` | The return of `ready.wait_timeout` in `Wake::wait` | `a_signal_from_another_thread_ends_the_wait_early` |
| `ProducerHold` | `MReactorWake` | `Reactors::hold`: `Shared::hold`, `held.store(true)`; `hold_wakes` of `net.rs`, called by `Runtime::turn` | `a_held_wake_up_is_given_when_the_hold_ends` (`net/reactor.rs`) |
| `ProducerPreload` | `MReactorWake` | None: only the mutant `CheckBeforePush` reaches it | No deterministic replay |
| `ProducerPush`, `ProducerOwe`, `ProducerReadHeld` | `MReactorWake` | `Shared::send`: push under the queue lock, `owing.store(true)`, `held.load()` | `a_held_wake_up_is_given_when_the_hold_ends`: a notification under a hold; the lost-message interleavings have no deterministic replay |
| `ProducerRelease` | `MReactorWake` | The drop of the guard of `Reactors::hold`: `Shared::release`, `held.store(false)`, then `kick` | `a_held_wake_up_is_given_when_the_hold_ends`; no holder waits before its release: `an_idle_shard_waits_without_holding_the_reactors_wake_ups` (`hold-kept-liveness` is the earlier behaviour) |
| `ProducerKick`, `ProducerLoad`, `ProducerWake` | `MReactorWake` | `Shared::kick`: `owing.swap(false)`, `sleeping.load()`, `waker.wake()` | `a_notification_runs_the_task_once_and_a_waiting_task_is_ticked`; `a_held_wake_up_is_given_when_the_hold_ends` |
| `ProducerUnhold` | `MReactorWake` | None: only the mutant `DebtReadBeforeRelease` reaches it | No deterministic replay: it needs a second sending thread |
| `ReactorTake`, `ReactorHandle` | `MReactorWake` | `Shared::take` and the `Message::Run` arm of `run` | `a_notification_runs_the_task_once_and_a_waiting_task_is_ticked` |
| `ReactorRun` | `MReactorWake` | The `ready` loop of `run` and `Driven::turn` | `a_task_runs_on_readiness_and_rests_between_events` |
| `ReactorLateClear` | `MReactorWake` | None: only the mutant `ClearDirtyAfterRun` reaches it | No deterministic replay |
| `ReactorSleep`, `ReactorRecheck`, `ReactorPoll`, `ReactorAwake` | `MReactorWake` | `Shared::sleep`: `sleeping.store(true)`, `pending`, `poll.poll`, `sleeping.store(false)` | `a_task_runs_on_readiness_and_rests_between_events` (an idle task is not run); `an_idle_broker_rests` (`tests/reactor_wake.rs`) on the executable |
| `TaskProduce` | `MLaneOwed` | The packets framed by one input turn, handed to `ConnCore::push_incoming_batch`; the reports of one turn, handed to `ConnCore::flush_reports` | `input_saturation_is_backpressure_and_loses_no_packet` (`net.rs`) |
| `TaskRun` | `MLaneOwed` | `Message::Run` after `Link::consumed` or `Connection::close`, or the tick of a task that reported `waiting`; `ConnCore::is_cancelled` at the start of the turn | `a_notification_runs_the_task_once_and_a_waiting_task_is_ticked`: a waiting task is run without an event; `consumed` runs the task only after `owe`. `cancellation_ends_idle_and_partial_socket_tasks` (`net.rs`) |
| `TaskTry1`, `TaskOwe`, `TaskTry2` | `MLaneOwed` | The two attempts around `Link::owe` in `ConnCore::push_incoming_batch`, `ConnCore::flush_held_incoming`, `ConnCore::push_incoming_one` and `ConnCore::flush_reports` | `a_borrowed_ingress_batch_keeps_capacity_for_its_unconsumed_suffix`: a packet refused twice while the core holds a borrowed batch, and taken once the batch is finished; `input_saturation_is_backpressure_and_loses_no_packet`: the lane is full, what was read is held, and every packet arrives in order once the core takes values. The lost wake-up of `SingleAttempt` has no deterministic replay |
| `DeadlineElapses` | `MLaneOwed` | `stalled_since` and `RESULT_WAIT` in `ConnCore::flush_reports`; nothing in the packet functions | `input_saturation_is_backpressure_and_loses_no_packet` shows that a packet has no bound; no test lets a receipt wait for `RESULT_WAIT` |
| `ConsumerTake`, `ConsumerConsumed` | `MLaneOwed` | `IncomingBatch::finish` (the room, then `Link::consumed`); `ConnCore::drain_written` and `ConnCore::drain_wire_progress` (the pop, then `Link::consumed`) | `a_borrowed_ingress_batch_keeps_capacity_for_its_unconsumed_suffix` (`net.rs`); `a_notification_runs_the_task_once_and_a_waiting_task_is_ticked` |
| `CoreCancel` | `MLaneOwed` | `Connection::close`: `cancelled` set (`ConnCore::stop` for a plain task), `link.notify()` | `cancellation_ends_idle_and_partial_socket_tasks` |
| `Quiescent` | all three | The end of a run of the model | Not an action of the code |

The sidecars list under `replays` only tests that exist in the files they
name. The interleavings that lose a wake-up are not replayed by any
deterministic test: they need a chosen schedule of two or three threads,
which the crate has no harness for. `no_published_item_waits_for_the_timeout`
and the two tests of `tests/reactor_wake.rs` observe the property on real
threads and on the executable; they would not reliably fail on a mutant.
