# Per-waiter futures for `PublishReceipt.wait()` — 2026-09-06

Base commit: `4560e44aeb24b8a4f7f6ce903766579fac481cbd` (`main`).
Candidate: one independent future per active waiter, replacing one shared
future wrapped in `asyncio.shield()`.

Host: Intel i7-3770, 4 physical cores with SMT (sibling pairs `(0,4) (1,5)
(2,6) (3,7)`), Linux 6.8 lowlatency, `performance` governor, no `isolcpus`.
Python 3.12.13. Mosquitto 2.0.18.

## Why this report exists

The change is a hot-path edit to a load-bearing correctness mechanism. PR #76
introduced the shared future plus `asyncio.shield()` for one reason: cancelling
one waiter must not cancel receipt completion nor contaminate other waiters.
Any replacement must keep that guarantee and must not move any path it does not
target. This report records what was measured, including the cells that stayed
neutral and the two that moved the wrong way.

## Runner isolation

The host is a desktop that could not be made idle (a browser had to stay open).
Measurements are therefore pinned by *physical* core, not by logical CPU:

| Actor | CPUs | Physical core |
| --- | --- | --- |
| mosquitto broker (fresh, port 21883, known config) | 2 | core 2, sibling idle |
| mqttium publisher worker | 3 | core 3, sibling idle |
| harness parent, observer, `mosquitto_sub` | 1,5 | core 1 |
| desktop, browser, shell and everything else | 0,4 | core 0 |

Every pre-existing mosquitto instance was destroyed first: port 11883 had been
served by two different leftover processes with unknown configuration, and an
early smoke run measured against one of them. That run is discarded.

## Method

Every number below is a paired ABBA measurement between two source trees on the
same host and broker. Every A/B is preceded by an **A/A** — the same code in
both arms — and no A/B cell is read except against its own A/A band. Cells whose
A/A band did not permit a conclusion are reported as unusable rather than
quietly dropped.

## Correctness

| Gate | Result |
| --- | --- |
| `ruff format --check src tests benchmarks` | pass |
| `ruff check src tests benchmarks` | pass |
| `mypy src/mqttium` | pass, 63 files |
| `pytest tests/unit tests/project` | 1601 passed |
| `pytest tests/integration tests/resilience` (`MQTTIUM_REQUIRE_BROKER=1`) | 40 passed |
| `pytest tests/fuzz` | 123 passed |
| `tests/fuzz/fuzz.py --seed 1 --iterations 20000` | codec / engine / websocket, 0 crashes, 0 invariant violations |

`tests/unit/test_publish_receipt_waiters.py` adds the waiter invariants:
never-awaited receipts allocate nothing, settled-before-wait allocates nothing,
single and N concurrent waiters, cancellation isolation, a waiter created after
a cancellation, settlement with already-cancelled futures listed, exact error
identity across every waiter, repeated `wait()` and repeated `_settle()`, QoS 0
semantics, waiter futures belonging to the running loop, `wait_for` timeout
retirement, and no waiter future retained after either settlement or a
cancellation storm.

### Test adaptations

`tests/unit/test_nonreplayable_failures.py` asserted on the private `_future`
field in six places. Those assertions pinned the representation, not the
behaviour. They now assert the invariants the representation exists to provide:
that no waiter structure is built when nobody waits, that the parked-waiter
count is what it should be, that a cancelled waiter is retired while the receipt
stays completable, and that settlement releases the collection. No test was
weakened to accommodate the candidate.

## Memory

`tracemalloc` deltas around a receipt with N parked waiters, plus retention
after settlement and after cancellation.

| Waiters | Baseline bytes | Candidate bytes | Baseline blocks | Candidate blocks |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 1152 | 1152 | 12 | 12 |
| 1 | 2804 | 2244 (−20.0%) | 31 | 25 |
| 2 | 4640 | 3232 (−30.3%) | 54 | 33 |
| 8 | 14400 | 8056 (−44.1%) | 173 | 75 |
| 32 | 53424 | 27688 (−48.2%) | 647 | 243 |
| 128 | 212200 | 109424 (−48.4%) | 2606 | 979 |

Retention, at every N tested (1, 8, 32, 128):

- after settlement — baseline retains the shared future; candidate retains
  nothing and its `_waiters` is back to `None`;
- after every waiter cancels — same: baseline retains the shared future, the
  candidate returns to the lazy shape a never-awaited receipt has.

High-volume cycling (6 cycles × 20 000 awaited receipts): RSS growth 0 KiB and
object growth 0 on both arms. The candidate's steady-state RSS is 500 KiB lower.

## Micro

Pinned, ABBA, fresh source-isolated workers; batched so one event-loop iteration
is amortised over 64 waiters instead of charged to each operation.

| Cell | A/A | A/B |
| --- | ---: | ---: |
| pending waiter | −0.88% | **−38.18%** |
| 2 concurrent waiters | +0.45% | **−36.47%** |
| 8 concurrent waiters | +0.00% | **−37.81%** |
| 32 concurrent waiters | −0.55% | **−37.46%** |
| cancellation storm, 32 waiters | −1.02% | **−27.01%** |
| already-settled fast path | −1.67% | +0.98% |
| never awaited | −0.67% | −0.35% |
| QoS 0 | −0.89% | +1.00% |

A/A band ±1.7%. The three non-target cells sit inside it.

## In-process paired regression

The repository's `paired_regression` suite had **no cell that awaits a
receipt** — `publish_complete_receipt` and `receipt_settle_unawaited` both
settle without a waiter parked. Two scenarios were added to close that gap:

| Scenario | A/A | A/B | range | cv |
| --- | ---: | ---: | --- | ---: |
| `receipt_wait_single` | −0.19% | **+59.18%** | [+45.50%, +62.52%] | 1.65% |
| `receipt_wait_concurrent` (8 waiters) | +0.12% | **+58.90%** | [+50.45%, +63.61%] | 1.51% |

Fourteen existing scenarios were re-measured at `--repeat 21` (A/A band ±1.89%)
and the ambiguous ones again at `--repeat 51`. All are neutral:

| Scenario | A/A | A/B |
| --- | ---: | ---: |
| `publish_complete_receipt` | −0.31% | −0.21% |
| `receipt_settle_unawaited` | +0.02% | −0.23% |
| `publish_complete_callback` | −0.07% | −0.32% |
| `writer_enqueue_async` | −0.44% | −0.44% |
| `compat_publish_qos0_batch` | +0.32% | +0.34% |
| `native_publish_nowait_qos0` | −0.23% | −0.13% |
| `async_publish_nowait_qos0` | +0.53% | +0.49% |
| `encode_qos0_v5` | −0.27% | +0.26% |
| `qos1_cycle_memory` | −0.63% | +0.48% |
| `qos1_cycle_sqlite` | +1.10% | −0.09% |
| `qos2_cycle_sqlite` | −0.02% | −0.43% |
| `delivery_callback` | +0.50% | +0.87% |
| `effect_send_inline` | +0.31% | +0.80% |
| `websocket_mask_4k` | −0.03% | +1.32% |

A first pass at `--repeat 5` produced apparent regressions of −6.25%
(`writer_enqueue_async`) and −3.35% (`compat_publish_qos0_batch`), and apparent
gains of +5.18% on `websocket_mask_4k` and +5.02% on `encode_qos0_v5` — paths
the change cannot touch. At `--repeat 21` and `--repeat 51` all four collapse.
That pass measured noise and is recorded here only as a caution.

`websocket_mask_4k` keeps a +1.32% A/B reading while its A/A is −0.03%. The A/A
compares one worktree against itself, so it cannot express a tree-dependent
bias; the A/B compares two different trees. **Treat ~1.3% as this harness's A/B
floor**, not as a gain. Under that reading every cell above is neutral.

`qos1_cycle_sqlite` and `qos2_cycle_sqlite` carry cv 20–33% with per-pair ranges
spanning −78% to +422%. Their medians converge to neutral but the cells are
disk-bound and should not be cited for anything finer.

## Network — Mosquitto

`paired_network.py`, QoS 1, 64-byte payloads, `--repeat 8`. A/A band **±0.5%**
on ACK throughput and **±0.01 ms** on p50.

### Publisher ACK throughput, candidate / base

| | window 1 | window 32 | window 64 |
| --- | ---: | ---: | ---: |
| MQTT 3.1.1 | **+6.55%** | +0.63% | +0.19% |
| MQTT 5 | **+6.15%** | +1.39% | +0.56% |

Window 1 is the sequential case where every publication awaits its own receipt,
and it is where the change is expected to matter most. It is 12× the A/A band
and reproduced across three independent runs (+9.2%/+7.3%, then +6.55%/+6.15%).
Windows 32 and 64 are pipelined and broker-bound: the gain is real but small.

### Receipt-completion (ACK) latency

| Cell | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| 311 w1 | −7.25% | −4.10% | −7.43% |
| 5 w1 | **−13.56%** | −5.78% | −5.97% |
| 311 w32 | −1.19% | −0.91% | +0.98% |
| 5 w32 | −2.29% | −0.30% | −2.73% |
| 311 w64 | −0.51% | −1.52% | −0.56% |
| 5 w64 | −1.43% | +1.38% | −1.81% |

ACK latency does not degrade in any cell at any percentile.

### Callback completion — negative control

`--completions callback` never awaits a receipt. A/A band ±2%.

| | w1 | w8 | w32 | w64 |
| --- | ---: | ---: | ---: | ---: |
| MQTT 3.1.1 | −1.1% | +1.7% | +0.4% | +2.3% |
| MQTT 5 | +0.3% | −0.2% | −0.0% | −0.2% |

### Unusable cells

Window 8 is bimodal on this host: its **A/A** returned a ratio of **0.8587** on
MQTT 5 — −14% between two copies of the same code — with per-arm cv 7–10%. The
A/B readings there (+23.9% and +16.8% on 3.1.1, +4.2% and +5.3% on MQTT 5) are
**not evidence** and are recorded only so they are not mistaken for a result
later. Mosquitto's default `max_inflight_messages` is 20, which puts w8 and w32
in different regimes; the instability is plausibly that boundary.

## Fixed-rate, non-saturating

`paired_open_loop.py`, MQTT 3.1.1, window 32, receipt completion, `--repeat 8`.
A/A band: cpu/msg ±1.2%, p50 ±0.12%, p95 ±0.4%, p99 ±1.5%.

| Target rate | cpu µs/msg | ack p50 | ack p95 | ack p99 | pairs favouring candidate |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2500 msg/s | +1.27% | **1.795 → 0.505 ms (−71.8%)** | −4.74% | −4.96% | 8/8 |
| 5000 msg/s | −15.42% | +7.14% | −6.01% | −4.82% | 1/8 |

The p50 collapse at 2500 msg/s is the strongest network signal in the campaign:
reproduced across two independent runs (−64.5%, then −71.8%), 8 pairs out of 8,
against an A/A that held both arms at 1.787 / 1.786 ms. It is consistent with
the mechanism — `shield()` costs one extra event-loop hop per completion, which
at this pacing is roughly the observed difference.

No CPU claim was made from this pass alone: cpu µs/msg read +1.27% at one rate
and −15.42% at the other. The matched-load campaign below reproduces both
readings and resolves them — the CPU saving is real and rate-dependent.

The harness marks these runs `invalid` on per-arm p50 cv (8–23%). The paired
medians are nevertheless tight, which is what ABBA pairing is for; the raw cv is
retained as a diagnostic, not as grounds to discard the paired estimator.

## Negative controls against the broker

Publisher-side cells that must not move, `--blocks 4`, 15 000 messages each,
A/A band **±1.6%** on ops/s.

| Cell | MQTT 3.1.1 | MQTT 5 |
| --- | ---: | ---: |
| QoS 0 | +0.69% | +0.20% |
| QoS 0 `publish_nowait` | −0.60% | −0.58% |
| **QoS 1, receipt never awaited** | **−0.34%** | **+0.05%** |
| QoS 2, receipt never awaited | +0.24% | −0.05% |
| QoS 1, receipt awaited (target) | +1.08% | +0.92% |
| QoS 2, receipt awaited (target) | **+2.50%** | **+2.42%** |

p50, p95 and p99 are 0.00% on all four never-awaited cells, which is what the
lazy allocation predicts: no waiter is parked, so no structure is built.

An earlier pass at `--blocks 1` reported **−6.41%** on QoS 1 never-awaited under
MQTT 3.1.1. That pass had an A/A band of ±6%, the MQTT 5 arm moved the other way
(+1.71%), and the tightened re-run puts the cell at −0.34%. It was noise. It is
recorded because it would have been a merge blocker had it held.

## Matched-load validation (2026-09-06, second campaign)

The closed-loop result below was re-examined because `paired_network.py` is
windowed: the candidate completes receipts faster, so the two arms do not offer
the same load over time. A delivery-latency difference measured there cannot be
separated from the throughput difference that caused it.

`paired_open_loop.py` answers the question directly. Its pacer is an absolute
deadline schedule (`started + sequence * interval`) and receipts are observed in
separate tasks, so completion speed does not gate publication. In
`absolute_rate` mode both arms receive the identical `target_rate` and identical
`count`, with no per-arm calibration. The delivery metric is the same
`mosquitto_sub` QoS 0 probe that produced the closed-loop number.

Runner isolation as above. `--repeat 8`, payload 64, receipt completion.

### Validity

| Cell | offered vs target (base / cand) | offered skew cand/base | completion ratio |
| --- | --- | ---: | ---: |
| 311 w32 @2500 | +0.00% / −0.01% | −0.010% | 1.0000 |
| 311 w32 @5000 | +0.01% / −0.00% | −0.011% | 1.0000 |
| 5 w32 @2500 | +0.01% / +0.01% | −0.004% | 1.0000 |
| 5 w32 @5000 | +0.00% / +0.00% | +0.001% | 1.0000 |

Load matching is exact. Every cell is valid.

A first attempt without desktop confinement produced an A/A band spanning −73%
to +27% on delivery percentiles and was discarded without being read as a
result; the numbers here are from the confined re-run.

### The accused cell is cleared

**MQTT 5, receipt, window 32, 2500 msg/s** — the cell that carried the closed-loop
+7.43%:

| Metric | A/A band | A/B |
| --- | ---: | ---: |
| delivery p50 | +0.64% | **−1.61%** |
| delivery p95 | −0.42% | **−0.51%** |
| ACK p95 | −0.11% | −6.15% |
| ACK p99 | +0.35% | −5.62% |

At matched load the delivery difference disappears. The closed-loop +7.43% was
an artefact of the candidate offering more throughput in a windowed benchmark,
not a latency cost at equal work.

### A different regression is confirmed

At **5000 msg/s** the candidate degrades median latency on every cell measured —
both protocols, windows 8, 32 and 64:

| Cell | offered skew | delivery p50 (A/A) | delivery p95 | ACK p50 | cpu µs/msg |
| --- | ---: | ---: | ---: | ---: | ---: |
| 311 w8 | −0.006% | **+32.93%** (+0.01%) | −0.58% | +5.70% | −16.25% |
| 311 w32 | −0.006% | **+27.50%** (+0.22%) | −0.13% | +9.21% | −16.91% |
| 311 w64 | −0.001% | **+26.84%** (+1.75%) | −0.52% | +10.05% | −16.97% |
| 5 w8 | −0.001% | **+32.99%** (+2.11%) | −0.30% | +17.98% | −15.01% |
| 5 w32 | +0.001% | **+25.55%** (−0.12%) | +0.36% | +11.36% | −18.99% |
| 5 w64 | +0.005% | **+31.76%** (+0.00%) | +0.79% | +7.82% | −17.52% |

Absolute delivery p50: **0.134 ms → 0.167–0.178 ms**. Completion ratio 1.0000
everywhere. A/A band for delivery p50 is ±2.1%.

Reproduced across two independent campaigns hours apart. For 311 w32:

| Metric | first campaign | second campaign |
| --- | ---: | ---: |
| @2500 ACK p50 | −71.84% | −73.30% |
| @2500 delivery p50 | −12.60% | −13.39% |
| @5000 ACK p50 | +7.14% | +9.21% |
| @5000 delivery p50 | **+25.79%** | **+27.50%** |
| @5000 cpu µs/msg | −15.42% | −16.91% |

### Localisation

- **Not protocol-specific.** MQTT 3.1.1 shows it as strongly as MQTT 5.
- **Not window-specific.** Present at windows 8, 32 and 64.
- **Not delivery-specific.** Publisher ACK p50 degrades too (+5.7% to +18.0%).
- **Rate-dependent.** At 2500 msg/s every metric improves, ACK p50 by −73%.
- **Medians only.** delivery p95 stays neutral (−0.58% to +0.79%, A/A ±0.6%) and
  ACK p95/p99 improve by 5–10%.

### Mechanism

`effect_inline` is identical between arms (30001). `effect_enqueued` rises from
7 400–9 200 to 11 600–12 900 — about **+40% more effects deferred to the
`EffectPump`** instead of applied inline. The candidate moves work off the inline
path onto the deferred path: the aggregate costs 15–19% less CPU, but a message
whose completion is deferred waits an extra pump turn, which lifts the median
while leaving the tail unaffected or better.

This is a measured counter difference, not an inference from timings alone. It
does not by itself say whether the shift is inherent to per-waiter futures or an
interaction with the pump's batching thresholds; that is not resolved here.

### CPU

The matched-load campaign does support a CPU claim at 5000 msg/s: −15.0% to
−19.0% across six cells, against A/A bands of −2.7% to +2.1%, reproduced across
both campaigns. At 2500 msg/s cpu µs/msg is neutral to slightly worse (+1.21%,
A/A +0.57%).

## Superseded reading — closed-loop delivery interaction

The first campaign measured, with `paired_network.py` at window 32 on MQTT 5:

| Metric | A/A | A/B |
| --- | ---: | ---: |
| delivery p50 | +0.21% | +7.43% (0.559 → 0.600 ms) |
| delivery p95 | +0.89% | +4.62% (0.850 → 0.889 ms) |
| delivery p99 | +0.59% | +3.38% (0.904 → 0.934 ms) |

Reproduced across three runs of that harness. The measurement is retained: it is
a correct observation of a **closed-loop throughput/delivery interaction**, in
which the candidate also raised ACK throughput +1.39% on the same cell.

It is *not* evidence of a matched-load regression, and the earlier wording in
this report — "a throughput/latency trade at fixed offered load" — was wrong:
`paired_network.py` does not hold offered load fixed. The matched-load campaign
above shows that cell is neutral at equal absolute rate, and locates a real
regression elsewhere.

## Not used as justification

The cross-client mqttium/gmqtt RTT figures are excluded. The corrected campaign
in `yoch/mqtt-python-client-bench#32` had not landed, and the superseded bridged
results were invalidated by adversarial review. Nothing here rests on them.

## Verdict

**Blocked.** At matched absolute load the candidate degrades median end-to-end
delivery latency by 25–33% at 5000 msg/s, on both protocols and at every window
tested, against an A/A band of ±2.1%, with completion ratio 1.0000 and offered
load matched to better than 0.01%. Publisher ACK p50 degrades on the same cells.
The effect is reproduced across two independent campaigns and carries a
mechanism marker: about 40% more effects deferred to the `EffectPump`.

Everything else in this report stands: correctness, the ~59% in-process gain on
the awaited-receipt path, +6.2 to +6.6% sequential ACK throughput, the −73% ack
p50 at 2500 msg/s, a real 15–19% CPU saving at 5000 msg/s, strictly lower memory
with no retained waiter futures, and neutrality on QoS 0, QoS 1 never-awaited,
QoS 2, callback completion, persistence and writer paths.

The change buys aggregate CPU at the cost of median latency once the completion
rate is high enough for deferral to bind. That trade is not obviously wrong, but
it is a user-visible behaviour change on a path the change did not intend to
touch, and it is not what the pull request currently claims.
