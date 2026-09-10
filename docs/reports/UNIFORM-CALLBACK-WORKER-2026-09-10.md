# Uniform message callback worker: history and experimental design

Base: `9ad1f01857306ac5079ffb1d073a59fdb60e1931`.
This is a separate experiment, not a continuation to merge rejected PR #456.
No release decision or universal no-breakage claim is made.

## Historical evidence: this family of designs is not new

The original native architecture already used an isolated callback worker.
[Issue #39](https://github.com/yoch/mqttium/issues/39) explicitly compared that
architecture with batched worker draining and synchronous inline callbacks.
The [2026-08-06 findings](https://github.com/yoch/mqttium/issues/39#issuecomment-5198964120)
reported worker batch-draining at 71.7k versus 71.1k messages/s (about +0.9%).
It was rejected for insufficient measured benefit, not a demonstrated correctness
failure. Removing effect suspensions increased queue occupancy and event-loop
lag; fewer suspensions did not automatically mean more stable execution.
The historical performance audit records the batch-drain idea as rejected twice.
These are historical reported results, not measurements rerun for this branch.

[PR #43](https://github.com/yoch/mqttium/pull/43), recreating #38 on main,
retained producer-side admission batching with isolated callbacks. Its reported
+11.45% direct/+9.34% broker-fed ingress gain came from amortizing queue transfer,
not from executing user callbacks in the reader. Later #220/#221/#223 also
optimized immediate admission while retaining worker isolation. In particular,
"inline delivery" in #221 did not mean inline user callback execution.

[PR #323](https://github.com/yoch/mqttium/pull/323), merged 2026-08-20, replaced
individual callback queue jobs with one physical job per adjacent message batch.
Its inspected diff introduced logical callback reservations and queue-capacity
adjustments. The rationale was approximately 17 Python calls/message saved;
reported paired throughput was +16.37%, seven positive pairs, with CPU/message
5.02 to 4.30 us. This is direct evidence that merely restoring per-message queue
entries may cost throughput and is a risk to measure, not hand-wave away.

[PR #402](https://github.com/yoch/mqttium/pull/402), merged 2026-08-28, introduced
idle synchronous callback execution in the reader/effect path. The inspected
diff changes the prior documentation from worker-only to opportunistic inline
execution. Reported ARM64 evidence included QoS1 +2.29% and paced PUBACK p50
-14.58%; that combined publish/message change is not an isolated on_message gain.
[PR #433](https://github.com/yoch/mqttium/pull/433) subsequently removed dynamic
awaitable continuations, selected topic-router form on configuration, and added
exactly-two-message inline dispatch. Its historical A/A-adjusted figures are
not the estimator used for this new experiment.

The later #454 rejected an always-worker routed-callback variant in a mission
whose contract was to preserve inline behavior and throughput. Its current
candidate retains the fast path and is not changed here. #457 uses a serial
worker too, but also removes many APIs and freezes routes: its combined slowdown
cannot be attributed only to the callback policy. #456 itself accumulated
ownership machinery, lost 5.33% on the measured network batch-2 cell, and was
rejected; it is preserved as historical evidence.

Thus worker isolation was **progressively optimized away on eligible paths for
measured latency/throughput**, not abandoned because a serial worker is invalid.
This experiment revisits that trade-off under a different objective: fewer
ownership states and steadier service may justify a modest capacity reduction.
It does not claim that worker draining is a newly discovered optimization.

## Design choices

Message callbacks have one execution owner regardless of callable form or burst
size. The existing effect/reader fast path performs only bounded queue admission,
so no callback exception must repair an in-progress transfer. There is no new
public setting, dependency, detached list, protocol effect, or per-message task.
`EffectPump`, protocol engines, persistence models and writer code are unchanged.

Each notification is one ordinary `asyncio.Queue` entry. The configured maximum
is fixed. `qsize()` is the number waiting and there is at most one active job.
Byte tokens use the existing shared reference accounting, including iterator
copies in `both`. No `_callback_batch_reserved`, batch token, private `_maxsize`
change, `_putters` access, or pair-inline helper remains.

Admission still amortizes mode/capacity checks over a prefix. The worker caches
callable classification for consecutive calls and invokes synchronous callbacks
directly, without creating/awaiting an invocation coroutine for each one. These
are simple attempts to recover some of #323's cost without restoring its queue
representation. Their net performance is an empirical question.

At the start of a worker turn, snapshot its queue length including the first
job. Process no more than that count, leaving unstarted jobs in the queue. New
arrivals cannot extend the turn indefinitely. Yield when work remains; awaiting
an empty queue supplies the idle suspension. This is a count-based fairness rule,
not a time budget or a universal one-user-function rule. Blocking synchronous
callbacks remain forbidden; all topic matches for one message form one ordered
notification. There is no arbitrary new batch threshold or timer knob.

The controller, not a particular worker task, owns unstarted notifications.
Cancelling a worker propagates cancellation in that task and retires its active
notification; a replacement resumes pending jobs without replaying the active
one. Cancellation before coroutine entry is handled by the same task completion
owner. A cold eager-start handoff ensures the task is registered before user code
can run. A late old completion cannot discard a replacement's queue.

Shutdown rejects new callback admissions, wakes blocked producers and drains or
discards according to its existing drain argument. A callback cannot join itself.
Reopening retires old queued work and increments the callback generation; a
reconnecting active callback remains the sole consumer. A prior shutdown cannot
clear the new generation. Pending byte/callback-capacity waiters reject stale
admission. One callback waiting on its own full queue fails rather than deadlocks.
Indirect application cycles involving network progress under delivery pressure
remain a limitation: a bounded worker does not create unlimited network read-ahead.

The stable topic dispatcher snapshots live matches at each notification's start.
Sync-to-async changes therefore cannot strand a captured synchronous router.
Callback failure is isolated per match; direct on_message capture and per-message
route changes retain their intentional distinction. No global route freeze and
no exceptional queue prepend are needed.

`on_publish` deliberately retains the current inline policy and receipt boundary.
`iterator` keeps its queue semantics; `both` keeps destination and byte ownership,
although its formerly-inline singleton callback is now queued. The same shared
worker also services queued lifecycle/publish callbacks with bounded rounds.

## Observable changes, not concealed compatibility claims

The public signatures/defaults, protocol rules and strict def/async-def return
contract are preserved. Message callback timing, task identity, and the fate of
unstarted jobs after private worker cancellation differ from main. A canceled
message worker does not cancel the reader or drop all later notifications.
Explicit lifecycle shutdown determines discard. The reference/migration/changelog
state this clearly; stable-release policy still applies before any integration.
This draft is not an authorization to release an incompatible Stable behavior.

Legacy tests that pinned immediate message execution/physical batch size are
adapted to assert the new queue boundary and fixed size instead. Tests retaining
FIFO, byte release, callable rejection, isolated errors, packet acknowledgements
and publish-only inline behavior are not weakened. New tests exercise normal
and eager factories, sizes 1/2/3/8/32, all relevant delivery modes, real
codec/engine/writer on scripted transport, cancellation, reconnect, manual ACK,
MQTT 3.1.1/5 and memory/SQLite persistence. Real-broker tests are a separate gate.

## Initial performance protocol

`benchmarks/uniform_callback_probe.py` is an initial same-host diagnostic, not a
replacement for the independent cross-client benchmark or a release threshold.
On a GitHub-hosted Linux runner, pin clients and an isolated TCP_NODELAY Mosquitto
to distinct available CPUs. Freeze exact baseline/candidate trees and use clean
same-length source paths; native publisher and subscriber share the client CPU.

Run A/A before A/B, four balanced AB/BA pairs each, fresh processes and matched
shuffled cell order. Nine QoS1 64-byte cells: sync message batches 1/2/8/32,
async8, filtered-sync8, iterator8, both8, publish-only8. Each cell has 250ms
warmup and exactly two full measurement seconds, followed by its final receipt
barrier. Retain every execution, stderr, identity, and result; no result-based
retry, drift subtraction, host pooling or adaptive load calibration.

Report delivered count in **all** full 100ms and 1s windows, including zeros,
mean and low-window throughput, within-run coefficient of variation, callback
first/tail p50/p95/p99, batch completion, loop-lag observations, CPU/completion,
and callback queue occupancy sampled at delivery. Timer lag has its own 10ms
observer and is not a hard real-time bound. Counts after the fixed window belong
to final drain and are excluded from window rate. CPU/completion includes that
finite final drain. Callback time sampling and queue sampling add symmetric
observer cost. Latency percentiles use nearest rank.

Two seconds and four pairs are screening, not reliable long-tail or long-regime
qualification. A VM's between-run or scheduler variation may exceed a small
code effect. Compare stability and low-window rate as well as means; do not
interpret lower variability caused by a lower average as an unconditional win.
The workload is closed-loop burst delivery, not an externally paced 3942 RTT/s
comparison or a library saturation ceiling. Any positive conclusion needs longer
independent controls and the reference workload afterward. No performance outcome
is predicted in this report.
