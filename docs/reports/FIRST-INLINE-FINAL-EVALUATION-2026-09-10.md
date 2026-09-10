# First-inline callback scheduler: final evaluation

Date: 2026-09-10. Experiment: PR #456.

## Decision: REJECT this implementation

The corrected prototype is executable and passes the covered correctness gates,
but it is not a better demonstrated architecture/performance compromise than
its baseline. On the real-broker, fixed-work two-message cell, throughput falls
**5.33%**, CPU per message rises **5.61%**, and the second callback's median
latency rises **43.32%**. The first callback becomes substantially earlier for
larger network bursts, but this does not improve overall throughput in those
cells. The implementation adds **109 net runtime lines** and more ownership and
exception paths rather than removing state-space.

The reference external-pacer experiment did run, but its stimulus qualification
is incomplete. It cannot supply the missing proof of real-workload equivalence.
The task-context compatibility difference and the narrower-than-requested
fairness scope also remain. Do not force acceptance by weakening those criteria.

This concludes the evaluation of the measured candidate. Preserve the source,
tests and evidence in the draft; do not merge it as a production simplification.
This is not a claim that every possible first-inline implementation is inferior.

## Exact source and evidence

| Item | Identity |
|---|---|
| Baseline A | `9ad1f01857306ac5079ffb1d073a59fdb60e1931` |
| Measured candidate B | `b8a70b09a4202278d3c5811be2e7f6b375e716e2` |
| Candidate root | `ed46ecb3108ad0cec714413659949654524a9eb8` |
| Candidate `src` tree | `4af1fe4b544d17c1ae544d1bd43295975b2d5505` |
| External harness repository | `yoch/mqtt-python-client-bench` |
| Frozen harness | `ef2382cf8ed20e5df4288102c1ae17e791a25d68` |
| Measurement tooling | `3e6681b312fac1fa8ff6ebc40c182aa2d3495615` |
| Pi run | [34452046014](https://github.com/yoch/mqttium/actions/runs/34452046014) |
| Artifact | `first-inline-final-34452046014`, ID `10143547045` |
| Original ZIP bytes | 31,870,093 |
| Original ZIP SHA256 | `d72cf524bd3c70a504ce2afc2e5638b68fa3661147a202a396fb33b75cbfb59c` |

The run contains 266 raw files. The original archive was downloaded and its
hash verified. Source SHAs, source trees and the three runtime file hashes were
checked before and after the campaign. Fixed-work subprocesses also returned
their imported source paths and hashes. The final report-only commit does not
change the measured runtime.

Actual host: runner `rpi5`, AArch64, Python 3.14.7, Mosquitto 2.1.2,
Linux `6.18.39+rpt-rpi-2712`. All four governors were `performance`; initial
reported temperature was 60.6 C. Reference CPU assignment was SUT 0, broker 1,
load generator 2, orchestrator 3. ASLR remained in the system configuration.
Other Pi campaigns were serialized through the existing shared runner lock.

The frozen harness includes its pacer-starvation and trace-sampling fixes. Its
historical `telemetry256` label does not describe the actual RTT payload: this
reference exchanges **40 bytes**. That existing workload was retained rather
than silently replaced with a different payload.

## Correctness finalization before measurement

The earlier ownership corrections are retained: cancellation before worker
entry retires queued reservations; active retirement occurs before awakened
producers resume; stale completions cannot erase a replacement worker; an
interrupted admitted prefix is consumed without replay and the remaining effect
suffix retains an execution owner. See the earlier
[hardening report](FIRST-INLINE-HARDENING-2026-09-10.md) for its exact revision.

One more real defect was reproduced before the final measurement. With an eager
task factory, the first flusher could execute and cancel A before `schedule()`
had assigned `self.task`. A nested successor then lost its reference when the
outer assignment completed. The final fix suspends eager entry until its owner
is registered. Normal task startup does not gain an extra scheduling hop; no
new state, queue, public option or swallowed cancellation was introduced.

`test_effect_eager_ownership.py` checks ownership while the successor is actually
suspended, and verifies that public disconnect cancels it. Eight eager cases
failed before the fix and eight ordinary-factory controls passed. All sixteen
pass afterward, covering bursts 1/2/3/8 and normal completion versus disconnect.

On measured B:

- The **316 focused** first-inline/ownership cases pass locally on Python 3.13.5
  and on the actual Pi on Python 3.14.7, with warnings treated as errors.
- Fresh [CI 34451553294](https://github.com/yoch/mqttium/actions/runs/34451553294)
  and [soak 34451553644](https://github.com/yoch/mqttium/actions/runs/34451553644)
  completed successfully. Normal CI includes Python 3.11-3.14, mandatory
  Mosquitto integration, Windows/macOS, quality, coverage, fuzz and resilience.
- [Distribution validation 34451553303](https://github.com/yoch/mqttium/actions/runs/34451553303)
  succeeded; actual publication and PyPI verification were skipped.
- The original two adverse resource-ownership tests pass without changing their
  assertions. The third original test, requiring the connection to remain alive
  after self-cancellation in a large burst, still distinguishes A from B. It is
  a disclosed behavior difference, not a falsely reported third fix.

The earlier 2,073-test/91.19%-coverage qualification belongs to `b0440f5a`, not
to the final report or an alleged identical post-fix source. Fresh CI above is
reported separately. Passing tests do not establish arbitrary downstream
compatibility or acceptable performance.

## Reference workload: completed but NOT QUALIFIED

MQTT 3.1.1, QoS1, `application_rtt_fixed_rate`, external pacer, frozen 3942 RTT/s,
standard timing: 3 s warmup, 12 s measurement, 6 s drain. A/A preceded A/B, with
8 alternating ABBA/BAAB blocks per phase: **64 executions** in total. No block
was retried and no A/A drift was subtracted.

The harness accepts 7/8 A/A blocks and 6/8 A/B blocks. Both phases have
`qualification.ok=false` and the verdict is `inconclusive`. Three executions
exceeded the frozen 0.2% catch-up fraction limit:

| Phase / execution slot | Arm | Catch-up fraction | Offers missed due to backpressure |
|---|---|---:|---:|
| A/A / 13 | A | 0.9111% | 82 |
| A/B / 10 | B | 0.7906% | 125 |
| A/B / 29 | A | 0.7991% | 98 |

All 64 executions and all invalid-stimulus attempts remain in the artifact.
There were **zero timeouts**, but there were **305 pre-admission missed offers**
in these three executions. Do not describe this as zero backpressure incidents.
The completed-in-window totals are 1,513,644 for A/A and 1,513,504 for A/B.
Window-boundary differences are not automatically classified as message loss.

The unchanged harness reports the following primary p50 estimates from its
complete usable blocks; they are still unqualified:

| Phase | Harness p50 effect | Harness 95% interval |
|---|---:|---:|
| A/A | -0.019% | [-0.823%, +1.412%] |
| A/B | -0.441% | [-1.874%, +0.890%] |

An independent *descriptive* summary retaining all 32 executions per phase,
including invalid stimulus, gives these A/B observations:

| Metric | A median of run observations | B median of run observations | All-run paired change |
|---|---:|---:|---:|
| RTT p50 | 0.217648 ms | 0.217954 ms | -0.36% |
| RTT p95 | 0.319593 ms | 0.315213 ms | -2.23% |
| Completed RTT/s | 3940.496 | 3940.492 | approximately 0% |
| Initiator CPU per completed RTT | 225.655 us | 223.856 us | -0.46% |

These medians are not pooled latency percentiles, and their quotient is not the
paired estimator. The all-run table is not a replacement for the harness's
filtered-block verdict. The all-run p95 interval is [-34.94%, +46.13%], and its
A/A p95 movement is -10.36%. This does not exclude a material tail change.

Independent verification recomputed all **192 reported p50/p95/p99 values** from
**3,027,148 raw RTT samples** and checked the worker source paths, native-async
execution flags, process exit codes and final delivery/effect snapshots. Callback
queues and delivery/effect waiters were clear at those snapshots. A publication
still pending at a window boundary is not by itself called a leak.

Conclusion for this workload: similar central observations, **no causal
performance or equivalence conclusion**. A green workflow means the collection
and cleanup completed, not that benchmark qualification passed.

## Real broker, fixed-work message bursts

This separate experiment uses a native publisher and subscriber in one process
on CPU0, the real Mosquitto broker on CPU1, QoS1, and 64-byte payloads. Each
iteration submits N publications without awaiting between submissions, waits
for delivery, then for publish receipts before the next iteration. These are
**one-way publication-to-delivery measurements with a receipt barrier**, not the
reference application request/response RTT and not a saturation ceiling.

Eight balanced pairs per phase, A/A then A/B; 500 measured batches after 100
warmup batches per cell; nine cells per subprocess, shuffled in matched order.
All 32 subprocesses and 288 cell measurements completed, covering **1,248,000
measured publications**. Message counts, sequence, callback errors, queue joins,
reservations and cleanup were checked. Sending N publications does not force
all N to arrive in exactly one reader batch; no such claim is needed for the
observed workload comparison.

Paired B/A changes:

| Sync burst | Throughput | First callback p50 | First callback p95 | Tail p50 | Tail p95 | Completion p50 | Completion p95 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | -0.54% | +0.75% | +0.66% | n/a | n/a | +0.65% | +0.54% |
| 2 | **-5.33%** | +2.81% | +3.14% | **+43.32%** | **+44.22%** | **+5.78%** | **+5.87%** |
| 3 | -2.52% | +2.23% | -26.96% | +1.05% | +3.13% | +3.09% | +3.10% |
| 8 | -1.46% | **-25.96%** | **-25.67%** | -3.15% | +1.31% | +1.45% | +1.51% |
| 32 | -0.84% | **-16.92%** | **-16.90%** | +1.26% | +0.82% | +0.88% | +0.89% |

The two-message cost is well separated from its own same-source control in this
campaign. A/A throughput changes -0.22%, with individual pair changes from
-0.84% to +0.48%. **All eight A/B pairs are slower**, ranging from -5.70% to
-4.69%. The paired throughput estimate is -5.33% with a 95% interval
[-5.54%, -5.10%]. CPU per publication increases 5.61% [5.36%, 5.84%].

For that cell, median run throughput is 9823.98 versus 9284.84 publications/s.
The second callback's median rises from **141.85 us to 203.79 us**, while batch
completion rises from **198.94 us to 210.72 us**. The tail callback is charged
its worker handoff, whereas batch completion already includes other scheduling
work. Those two percentages should not be conflated.

The larger-burst first-callback improvements are real positive observations and
are not discarded. They do not offset the measured batch-2 regression through
an invented workload-weighted average: actual callback-batch frequencies remain
unavailable. First-callback p50 at N=3 does not improve, unlike the in-process
case; real packet arrival grouping matters.

Network negative controls, throughput: async callback N=8 **-0.39%**, iterator
N=8 **-0.03%**, both N=8 **-0.08%**, publish-only N=8 **-0.04%**. They are much
closer to neutral than the affected batch-2 path. Fixed-work CPU includes both
clients in that process, unlike the reference's initiator-only metric.

## In-process measurements

Twelve balanced pairs per phase, A/A then A/B; 4,000 measured batches after 200
warmup batches; nine cells. **48 subprocesses**, 432 cell measurements,
**14,976,000 measured message effects**. This isolates delivery/effect handling;
it is not a prediction of the same percentage change on a network workload.

| Sync burst | Throughput | First p50 | First p95 | Tail p50 | Tail p95 | Completion p50 | Completion p95 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | -2.08% | +2.92% | +2.91% | n/a | n/a | +2.47% | +2.51% |
| 2 | **-70.69%** | +75.32% | +74.90% | +225.57% | +221.57% | +263.74% | +258.29% |
| 3 | -4.85% | **-42.70%** | -42.40% | +8.07% | +7.77% | +5.30% | +5.14% |
| 8 | -3.60% | **-36.05%** | -35.86% | +5.95% | +5.01% | +3.86% | +3.77% |
| 32 | -1.50% | **-21.16%** | -21.15% | +2.51% | +1.77% | +1.65% | +1.58% |

Median completion in microseconds, A to B: N=1 3.241 to 3.315; N=2 4.528 to
16.435; N=3 17.277 to 18.194; N=8 23.685 to 24.611; N=32 54.306 to 55.186.
The large relative batch-2 micro cost therefore corresponds to about 12 us
extra per completed micro batch, not a 70% application-network regression.

Micro throughput negative controls: async N=8 -1.31%, iterator -0.49%, both
+0.02%, no callback -0.88%. The async control is not literally free; it remains
in the evidence rather than being declared unchanged because it was intended
as a negative control.

## Statistics, instrumentation and remaining limits

Fixed-work central estimates are geometric means of paired ratios. Conditional
95% intervals resample the 8 network or 12 micro pairs, not individual messages,
with 20,000 draws and seed 45620260910. Reference all-run summaries use four
complementary units, 2,000 draws and seed42. No noise subtraction or
multiple-testing correction is applied. These single-host, single-campaign
intervals are not universal guarantees; the batch-2 result is also supported by
all eight pairs, its same-source control and the localized handoff signature.
The reference uses nearest-rank percentiles; the separate fixed-work probe uses
linear interpolation. They are deliberately not pooled together.

The timing phases were completed before a separate instrumentation attempt.
That attempt produced no callback-count files. Its timings are excluded and
**missing counters do not mean zero multi-message batches**. A counter-only
recovery was queued, then cancelled before any job started when finalizing the
rejected experiment: [34456892905](https://github.com/yoch/mqttium/actions/runs/34456892905).
No primary timing sample was retried or removed. No conclusion about the
frequency of batch-2 calls in the reference workload is claimed.

Raw callback/effect statistics are available where the harness exposes them.
`multi_effect_batches` is not a histogram of callback-message burst sizes.
Maximum throughput, whole-system CPU, other machines and every possible
application callback are not established by this campaign.

The fixed-work probe is retained in the raw artifact and at the immutable
measurement-tooling commit. Its SHA256 is
`6c61a2b709281461fd9e0f9f9a6c73fbfc7cf30b6fd2d7e79ded456e4eb41501`.
The analytical companion contains the independent calculation, all per-cell
p50/p95 and CPU tables, raw percentile verification and source manifests.

## Architecture and compatibility balance

Using Git's final source diff relative to A:

| Runtime file | Added | Removed | Net |
|---|---:|---:|---:|
| `_delivery.py` | 134 | 63 | +71 |
| `_effects.py` | 35 | 5 | +30 |
| `async_client.py` | 17 | 9 | +8 |
| Total | 186 | 77 | **+109** |

`_dispatch_sync_message_pair_inline` disappears, but batch reservations remain
necessary for ordinary worker batches and both-mode accounting. The final
three-file AST contains 12 more `if` nodes, four more `try` nodes and four more
exception handlers. These are structural counts, not claimed execution costs.
The admitted-prefix cancellation carrier, drain allowance and worker-retirement
paths expand the set of cases a maintainer must understand.

The fixes preserve tested FIFO A/B/C/X, hard queue accounting, isolated ordinary
errors, captured direct callbacks and the strict sync/async callable contract.
They do not eliminate the separate pre-existing live-routing/reconnect work in
PR #454 or prove its composition with this candidate.

Two deliberate differences remain unsuitable for a blanket compatibility claim.
First, executing A in the reader/effect task makes that task the target of
`current_task().cancel()`, including larger bursts that previously began in the
worker; a connection may now terminate in that case. Cancellation is propagated,
not swallowed. Second, the quota is a message notification on the eligible
callback-only drain path, not a universal one-user-function budget: multiple
matching filters, on_publish and both-mode retain their existing grouping or
rules. The worker does not promise a yield between every synchronous callback.

Documenting these differences is necessary, but does not make them neutral.
Given the localized network regression, absence of a demonstrated overall gain,
increased machinery and incomplete requested fairness/compatibility guarantees,
**REJECT is the final architecture/performance recommendation**. Keep the
current production scheduler. The completed experiment, its useful tests and
its exact source remain available without a main write, merge or release.
