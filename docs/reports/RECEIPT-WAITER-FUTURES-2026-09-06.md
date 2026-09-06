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

## Scheduling experiment — prototype C (deferred batched wake)

The matched-load regression above raised an obvious suspicion: the change removed two things at
once, not one. `asyncio.shield()` costs allocations *and* it inserts a scheduling hop.

| Arm | Wake path from `_settle()` to the application waiter | Hops | Callbacks (N waiters) |
| --- | --- | ---: | --- |
| **A** `main` | `set_result(shared)` → `call_soon` ×N `_inner_done_callback` → `outer.set_result` ×N → `call_soon` ×N `Task.__wakeup` | 2 | 2N |
| **B** PR #432 | `set_result(waiter)` ×N → `call_soon` ×N `Task.__wakeup` | 1 | N |
| **C** prototype | `call_soon` ×1 `_resolve_receipt_waiters` → `set_result` ×N → `call_soon` ×N `Task.__wakeup` | 2 | N+1 |

C keeps per-waiter futures, lazy allocation, cancellation isolation and the absence of `shield`,
while restoring A's wake phase with one scheduled callback per receipt instead of one per waiter.
It is **not committed**; the runtime on this branch remains B. The patch, against B:

```python
def _resolve_receipt_waiters(waiters: list[asyncio.Future[None]]) -> None:
    for waiter in waiters:
        if not waiter.done():
            waiter.set_result(None)

# in _settle(), replacing the inline resolve loop:
        waiters = self._waiters
        if waiters is not None:
            self._waiters = None
            waiters[0].get_loop().call_soon(_resolve_receipt_waiters, waiters)
```

Seven targeted tests covered the races the deferred wake introduces (cancellation between
settlement and wake, a fully cancelled list, exactly one wake batch per receipt under repeated
settlement, error identity, list release proven by weakref, loop confinement, and `is_done()`
still observable synchronously). All passed.

### Micro — how much of B survives in C

A/A noise floor ±0.87 %. Time per waiter; "C keeps" is C's share of B's absolute gain over A.

| Cell | A/A | A→B | A→C | A ns | B ns | C ns | C keeps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| pending, 1 waiter | −0.85% | −36.10% | **−28.60%** | 12907 | 8247 | 9367 | 76% |
| 2 concurrent | −0.10% | −36.53% | **−31.12%** | 12779 | 8110 | 8797 | 85% |
| 8 concurrent | −0.12% | −36.45% | **−35.59%** | 12286 | 7808 | 7942 | 97% |
| 32 concurrent | −0.53% | −36.93% | **−36.25%** | 12139 | 7656 | 7785 | 97% |
| cancellation storm 32 | −0.64% | −27.79% | **−27.80%** | 12379 | 8939 | 8994 | 98% |
| already settled | −0.24% | −0.50% | −0.72% | 174.6 | 173.7 | 173.9 | — |
| never awaited | +0.87% | −0.01% | +0.73% | 426.8 | 426.7 | 433.5 | — |
| QoS 0 | −0.44% | −2.50% | −1.04% | 168.9 | 164.7 | 165.8 | — |

The 76 → 85 → 97 % progression with waiter count is the predicted signature: A schedules one
callback per waiter, C schedules one per receipt. Memory is unchanged from B byte for byte
(1152 / 2244 / 3232 / 8056 / 27688 / 109424 bytes at 0/1/2/8/32/128 waiters), zero futures
retained after settlement or after a cancellation storm, zero RSS and object growth.

### Fixed-rate, matched absolute load

Window 32, payload 64, receipt completion, `--repeat 8`, offered skew ≤ 0.017 %, completion ratio
1.0000 in every cell. A/A rows are the band for the rows beneath them. p99 omitted: its A/A band
reaches ±5.9 %.

| Cell | Pair | ACK p50 | ACK p95 | deliv p50 | deliv p95 | cpu µs/msg |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 311 @2500 | A/A | +1.45% | −0.15% | +1.69% | −0.38% | −1.26% |
| | **A→C** | −8.78% | −2.12% | **−4.00%** | −2.98% | −1.76% |
| 5 @2500 | A/A | +0.95% | +0.09% | +2.04% | −0.12% | −1.65% |
| | **A→C** | −8.53% | −2.47% | **−3.57%** | −3.36% | −3.03% |
| 311 @5000 | A/A | +1.53% | +0.10% | +0.55% | −0.28% | −0.28% |
| | A→B | +9.21% | −5.98% | **+27.50%** | −0.13% | −16.91% |
| | **A→C** | +34.57% | +15.16% | **+22.10%** | +0.40% | −16.50% |
| | B→C | +7.06% | +1.10% | −4.13% | −2.10% | +6.44% |
| 5 @5000 | A/A | −0.71% | +0.65% | −0.02% | −0.18% | +0.26% |
| | A→B | +18.41% | −6.66% | **+24.17%** | +2.27% | −17.96% |
| | **A→C** | +32.23% | −1.06% | **+17.54%** | +1.92% | −12.82% |
| | B→C | +11.02% | +4.10% | −3.26% | −6.72% | +3.44% |

**C does not remove the regression.** It recovers about a fifth of it and keeps the CPU saving.
At 2500 msg/s C is better than baseline on every metric on both protocols.

There is also a structural reason C could not have fixed *this* benchmark: it parks exactly one
waiter per receipt, and at N=1 arms A and C are identical — two hops, two callbacks. C's advantage
over A only exists for N>1, which the micro exercises and the network harness does not.

### EffectPump counters

The inline fast path requires `len(effects) == 1 and not self.pending`: an effect rides it only if
it arrives alone.

| Cell · arm | batches | multi-effect | multi % | enqueued | high water | deliv p50 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 311 @2500 A | 10785 | 2716 | 25.2% | 6950 | 6 | 0.208 ms |
| 311 @2500 C | 10952 | 2940 | 26.8% | 7014 | 8 | 0.200 ms |
| 311 @5000 A | 24864 | 1828 | 7.3% | 6964 | 54 | 0.132 ms |
| 311 @5000 C | 22068 | 2813 | 12.7% | 10748 | 36 | 0.161 ms |
| 311 @5000 B | 21480 | 3008 | 14.0% | 11554 | 36 | 0.166 ms |
| 5 @5000 A | 24344 | 2112 | 8.7% | 7769 | 28 | 0.133 ms |
| 5 @5000 C | 22290 | 2754 | 12.4% | 10464 | 36 | 0.156 ms |

`inline_effects` is identical (30001) in every arm and `apply_suspensions` is 0 throughout.

### A hypothesis, and its falsification

Across twelve cells at fixed rate, `effect_enqueued` tracks delivery p50 at **r = +0.969**
(multi-effect batches r = +0.904, cpu/msg r = −0.919), and the relation holds in both directions:
B→C *reduces* grouping and *reduces* delivery p50. That suggested the regression was the CPU
saving pushing effects off the inline path onto the deferred pump.

The control falsifies it. Sweeping **main alone** across six rates moves its own multi-effect
share further than any patch does, and delivery p50 does not follow:

| main @ rate | offered | batches | multi % | enq/msg | deliv p50 | ACK p50 | cpu µs/msg |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1500 | 1500.1 | 7313 | 23.0% | 0.750 | 0.160 ms | 0.425 ms | 210.6 |
| 2500 | 2500.2 | 10831 | 25.5% | 0.926 | 0.207 ms | 0.558 ms | 159.5 |
| 3500 | 3500.1 | 15197 | 18.9% | 0.826 | 0.243 ms | 0.570 ms | 144.8 |
| 5000 | 5000.4 | 23705 | 9.5% | 0.570 | 0.134 ms | 0.521 ms | 152.4 |
| 6500 | 6500.5 | 31874 | 19.8% | 0.691 | 0.100 ms | 0.509 ms | 155.5 |
| 8000 | 8000.6 | 33630 | 28.4% | 1.000 | 0.130 ms | 0.636 ms | 126.8 |

Within main, multi % versus delivery p50 gives **r = +0.101**. Main's highest grouping (28.4 % at
8000 msg/s) sits at 0.130 ms — better than several of its lower-grouping points, and better than
the candidate at 5000 msg/s with 12.7 % grouping. The cross-arm correlation was measured with the
rate held fixed and only the code varying; it does not survive the reciprocal control. **The
batching-cliff explanation is withdrawn.** Grouping is a co-symptom.

`collect_from_engine()` is called from 16 sites in `async_client.py` but only 3 of them
(the publish paths) follow it with `drain_inline()`; the ingress paths do not. That asymmetry is
recorded here as an observation, not as a diagnosis — the control above forbids concluding from it.

### What survives

- The regression is real, matched-load, reproduced in three campaigns, both protocols, three
  window sizes.
- It is **rate-dependent**, appearing between 2500 and 5000 msg/s. At 2500 every arm beats
  baseline.
- It moves the **median** only: delivery p95 stays inside its A/A band, ACK p95/p99 usually
  improve.
- It is **monotone in how much of the shield is removed**: at 311@5000, A → C → B is
  0.132 → 0.161 → 0.166 ms, with C the intermediate arm by construction.
- CPU is genuinely lower: 13–18 % at 5000 msg/s against an A/A band under 1.7 %.

Main's own curve carries an unexplained discontinuity between 3500 and 5000 msg/s — delivery p50
goes 0.243 → 0.134 ms while its grouping collapses from 18.9 % to 9.5 %. Whatever regime main
enters there, the faster arms may be failing to enter it. That would invert the framing: not "the
candidate is pushed onto a slow path" but "the candidate no longer qualifies for a fast one".
It is the next thing to probe, on main alone, before any further arm comparison.

### Mechanism verdict

`DEFERRED WAKE HYPOTHESIS DISPROVEN` — the wake phase accounts for roughly a fifth of the
regression and cannot account for the rest. The batching mediator proposed to explain the
remainder is disproven by its own control. The mechanism is open.

## Rate-regime diagnosis — the pacer, not the receipt

The matched-load campaigns above matched the *average* offered rate to better than 0.01 %. They
did not check whether the two arms presented the same arrival process. `paired_open_loop.py`
paces from inside MQTTium's own event loop, so they need not, and a dense main-only sweep shows
they do not.

`benchmarks/rate_regime_probe.py` (diagnostic only, no runtime change) records per publication the
schedule deadline, the moment of admission, the resulting lateness, the real inter-arrival
interval, whether the pacer was already behind, the ACK latency, and — joined by sequence through
the subscriber — the delivery latency.

### main alone, 3000 → 6000 msg/s, MQTT 3.1.1, window 32, payload 64, 4 repeats

| rate | d p25 | d p50 | d p75 | d p95 | ack p50 | cpu µs/msg | lag p50 | lag p95 | catch-up | burst p95 | eager/msg | items/batch | enq/msg |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 3000 | 0.136 | 0.229 | 0.249 | 0.270 | 0.534 | 156.1 | 0.454 | 1.079 | 51.7% | 3.0 | 0.483 | 2.99 | 0.854 |
| 3250 | 0.154 | 0.242 | 0.266 | 0.292 | 0.595 | 139.9 | 0.356 | 1.095 | 60.3% | 4.0 | 0.397 | 3.73 | 0.890 |
| 3500 | 0.134 | 0.242 | 0.269 | 0.289 | 0.557 | 143.3 | 0.368 | 1.089 | 54.7% | 4.0 | 0.453 | 3.98 | 0.819 |
| 3750 | 0.134 | 0.181 | 0.268 | 0.292 | 0.630 | 143.4 | 0.223 | 1.081 | 53.7% | 4.5 | 0.463 | 3.10 | 0.773 |
| 4000 | 0.134 | 0.265 | 0.285 | 0.318 | 0.587 | 139.5 | 0.277 | 1.099 | 53.2% | 5.0 | 0.468 | 4.93 | 0.744 |
| 4250 | 0.133 | 0.180 | 0.281 | 0.316 | 0.650 | 138.8 | 0.190 | 1.101 | 54.1% | 5.0 | 0.459 | 3.21 | 0.735 |
| 4500 | 0.132 | 0.153 | 0.297 | 0.342 | 0.632 | 144.5 | 0.144 | 1.103 | 46.8% | 6.0 | 0.532 | 4.62 | 0.632 |
| 4750 | 0.132 | 0.159 | 0.298 | 0.342 | 0.650 | 142.2 | 0.141 | 1.111 | 49.1% | 6.0 | 0.509 | 3.33 | 0.659 |
| 5000 | 0.123 | 0.136 | 0.290 | 0.356 | 0.528 | 148.9 | 0.104 | 1.109 | 41.9% | 7.0 | 0.581 | 2.96 | 0.583 |
| **5250** | 0.109 | 0.121 | 0.149 | 0.272 | 0.393 | 175.4 | 0.039 | **0.596** | **15.8%** | 7.0 | **0.842** | 2.96 | 0.221 |
| **5500** | 0.107 | 0.111 | 0.131 | 0.141 | 0.356 | 182.2 | 0.026 | **0.103** | **3.6%** | 5.0 | **0.964** | 2.32 | 0.063 |
| 6000 | 0.089 | 0.107 | 0.110 | 0.149 | 0.461 | 167.6 | 0.092 | 0.111 | 12.4% | 1.0 | 0.876 | 1.14 | 0.243 |

**Transition: between 5000 and 5500 msg/s, complete by 5250.**

The 6000 row is at capacity — cpu/msg 0.168 ms against a 0.167 ms target interval — so its
per-class split is not usable (the classifier puts 87.6 % of messages in "late" and leaves the
on-time class with too few samples). Its aggregate figures are fine; its decomposition is not.

### The pacer, not the client

`lag p95` is **1.08–1.11 ms at every rate from 3000 to 5000**, independent of the target interval,
which ranges 0.333 → 0.200 ms. A lateness floor that does not scale with the interval is the
signature of the event loop's millisecond timer granularity: a sub-millisecond `asyncio.sleep()`
overshoots to about 1 ms. `late_fraction > 0` is **100.0 %** at every rate — every publication is
late.

The arrival process is consequently not paced at all below the frontier. Median real inter-arrival
is ~0.09 ms where the target is 0.235–0.333 ms, while p95 is ~1.31 ms: the pacer sleeps about
1.3 ms and then fires a burst. Interval CV is 1.36–1.73. At 5250–5500 the median interval converges
on the target (0.187 vs 0.190; 0.181 vs 0.182), p95 collapses to 0.19–0.25 ms and CV falls to 1.01.

The mechanism runs through the eager write permit established in
[#254](https://github.com/yoch/mqttium/pull/254) — one eager write per loop turn:

1. a sub-millisecond sleep overshoots to ~1.1 ms;
2. the pacer wakes late and admits a burst of catch-up publications inside one loop turn;
3. only the first frame of that turn takes the eager path; the rest queue;
4. queued frames wait for the writer task and deliver in ~0.30 ms instead of ~0.133 ms;
5. `eager/msg` follows exactly: 0.46–0.58 in the bursty regime, **0.964** at 5500.

Above the frontier the per-message CPU cost (~180 µs) approaches the target interval (~185 µs),
the loop always has work, `epoll` returns promptly, timers become accurate and bursts disappear.
The fast regime is the *expensive* one: cpu/msg rises 148.9 → 182.2 while latency falls.

### main does not get faster per message

| rate | on-time share | on-time p50 | late share | late p50 | catch-up share | catch-up p50 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 3000 | 10.6% | 0.133 | 37.8% | 0.140 | 51.7% | 0.247 |
| 4000 | 11.1% | 0.133 | 35.9% | 0.135 | 53.2% | 0.283 |
| 5000 | 18.8% | 0.112 | 39.7% | 0.132 | 41.9% | 0.302 |
| 5250 | 55.8% | 0.111 | 28.5% | 0.121 | 15.8% | 0.319 |
| 5500 | 74.7% | 0.110 | 22.5% | 0.113 | 3.6% | 0.319 |

An on-time message costs 0.133 → 0.110 ms across the whole sweep. What moves is the **share**. The
delivery histogram agrees: at 3000 msg/s it is multi-modal (28.0 % in 0.125–0.150, 28.2 % in
0.200–0.250, 23.7 % in 0.250–0.350); at 5500 it is one tight mode with 96.6 % under 0.150 ms.

### Pacer control — main versus candidate B at identical rates

| rate | arm | cpu µs/msg | catch-up | lag p95 | eager/msg | on-time share | deliv p50 | deliv p95 | on-time p50 | catch-up p50 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4500 | A | 142.8 | 48.2% | 1.104 | 0.518 | 12.2% | 0.160 | 0.347 | 0.132 | 0.305 |
| 4500 | B | 129.0 | **55.5%** | 1.100 | **0.445** | 12.2% | 0.270 | 0.351 | **0.132** | **0.303** |
| 5000 | A | 151.8 | 40.4% | 1.099 | 0.596 | 21.6% | 0.135 | 0.366 | 0.113 | 0.306 |
| 5000 | B | 127.6 | **57.3%** | 1.115 | **0.427** | 13.9% | 0.171 | 0.382 | 0.130 | **0.302** |
| 5250 | A | 164.6 | 25.3% | 1.023 | 0.748 | 40.9% | 0.123 | 0.359 | 0.111 | 0.311 |
| 5250 | B | 136.0 | **45.7%** | 1.115 | **0.543** | 17.7% | 0.143 | 0.377 | **0.111** | 0.320 |
| 5500 | A | 182.1 | 2.9% | **0.084** | 0.971 | 82.8% | 0.110 | 0.136 | 0.110 | 0.328 |
| 5500 | B | 158.4 | **24.2%** | **0.906** | **0.759** | 30.6% | 0.124 | 0.356 | **0.110** | 0.313 |

At every rate the candidate spends 10–17 % less CPU per message, and at every rate it shows a
**higher** catch-up fraction (+7.3 to +21.2 points) and a **lower** eager share (−14 to −28 %).

The decisive column is the last two. **Per class, the two arms are the same speed**: on-time p50 is
0.132 / 0.132, 0.111 / 0.111, 0.110 / 0.110; catch-up p50 is 0.305 / 0.303, 0.311 / 0.320,
0.328 / 0.313. The candidate is not slower for any kind of message. Only the mix differs.

The 5500 row is the clearest case: main has crossed into the accurate-timer regime
(lag p95 0.084 ms, 82.8 % on-time) while the candidate has not (lag p95 0.906 ms, 30.6 % on-time).
Their aggregate delivery p50 differs by 12.2 % and p95 by 162 % for that reason alone.

### What this establishes, and what it does not

Established: the harness paces from inside the loop under test; which timer regime an arm occupies
depends on how much wall time it spends in that loop; a cheaper arm occupies it less and therefore
stays in the bursty regime at rates where main has left it. **Average rate matched, temporal shape
differs.** The aggregate delivery-p50 gap is explained by class mix, with identical per-class
latency.

Not established: that an external producer would show no difference at all. That requires a pacing
source outside the loop under test, which did not exist when the rate-regime diagnosis was written.
Nothing in that diagnosis proved the candidate free of a smaller real effect underneath the
artefact. The control is the next section.

**Mechanism classification: `PACER REGIME TRANSITION`.**

The earlier verdict in this report — that the matched-load campaign confirmed a regression — must
therefore be read with its scope: it matched average offered rate, not offered-load shape, and the
difference in shape accounts for the observed gap. The measurements are retained unchanged; their
interpretation is corrected here.

## External-pacer control

Question: when A (`main@4560e44`) and B (runtime of `5f2dd2d`, unchanged) receive the same
temporal process from a clock **outside** MQTTium's event loop, does the candidate still show a
user-visible latency regression?

`benchmarks/external_pacer.py` is that clock. A dedicated process sleeps until
`start + sequence * interval − 150 µs` then spins to the absolute deadline, and emits a 24-byte
token `(seq, deadline_ns, emit_ns)` over a Unix datagram socketpair. The publisher loop never
calls `asyncio.sleep` to pace. The same pacer binary, margin, CPUs and broker are used for every
arm. Runtime `src/` is byte-identical to `5f2dd2d`.

This control was **not** run on the original i7-3770. It ran on the Cloud Agent host that executed
this section. Absolute milliseconds are therefore not comparable to the tables above; the
causal question is. The in-loop 1.1 ms timer artefact **does** reproduce here, so the host is a
valid place to ask whether that artefact was the blocker.

### Host

| | |
| --- | --- |
| CPU | `Intel(R) Xeon(R) Processor` (KVM), 4 vCPU, 1 thread/core, no SMT siblings |
| Governor | unavailable (`cpu0/cpufreq/scaling_governor` absent) |
| Python | 3.12.3 |
| Mosquitto | 2.1.2, fresh instance, `127.0.0.1:22883`, `allow_anonymous`, no persistence |
| Affinity | harness + `mosquitto_sub`: CPU 0; broker: CPU 1; publisher: CPU 2; pacer: CPU 3 |
| Transport | `socket.socketpair(AF_UNIX, SOCK_DGRAM)` after a 8000-token micro against stream pair and filesystem `AF_UNIX` datagram |
| Safety margin | 150 µs for every arm (calibrated once: 50 µs overshoots, 100–150 µs give lateness p95 ≈ 0.2 µs) |

`time.sleep(200 µs)` p50/p95 = 257 / 258 µs. `asyncio.sleep(200 µs)` p50/p95/p99 = **1084 / 1200 / 1273 µs**.
The millisecond loop timer exists on this host too.

### Pacer qualification (no MQTT)

4.0 s at each rate, lightweight receiver, same affinity as the MQTT campaign.

| rate | target µs | emission p50/p95/p99 | receiver p50/p95/p99 | late p95 µs | transport p95 µs | burst % |
| ---: | --------: | -------------------: | -------------------: | ----------: | ---------------: | ------: |
| 4500 | 222.2 | 222.22 / 222.26 / 222.27 | 222.20 / 223.18 / 344.90 | 0.19 | 13.02 | 0.07 |
| 5000 | 200.0 | 200.00 / 200.04 / 200.05 | 200.00 / 200.51 / 201.11 | 0.20 | 6.19 | 0.00 |
| 5250 | 190.5 | 190.48 / 190.52 / 190.53 | 190.47 / 191.14 / 191.63 | 0.20 | 6.24 | 0.00 |
| 5500 | 181.8 | 181.82 / 181.87 / 182.04 | 181.81 / 182.73 / 184.48 | 0.30 | 7.19 | 0.00 |

Emission jitter is three orders of magnitude below the 1.1 ms `asyncio.sleep` floor. Receiver p99
at 4500 (345 µs) is still well under a millisecond and is a single-run tail, not a catch-up burst.
The source is accepted.

### A/A noise floor

MQTT 3.1.1, QoS 1, receipt completion, window 32, payload 64, 4.0 s samples, 5 A/A pairs per rate.
Completion ratio 1.0000 and zero lost tokens on every sample.

| rate | pair Δ ack p50 | pair Δ delivery p50 | pair Δ cpu/msg |
| ---: | --- | --- | --- |
| 4500 | −0.10 % (−0.65 .. +0.48) | +0.28 % (−1.24 .. +3.67) | +0.11 % (−1.49 .. +2.95) |
| 5000 | +0.14 % (−0.39 .. +0.31) | +0.02 % (−0.78 .. +0.63) | +0.44 % (−0.49 .. +0.90) |
| 5250 | −0.12 % (−0.25 .. +1.50) | +0.64 % (−0.49 .. +18.25) | −0.08 % (−1.27 .. +3.30) |
| 5500 | −0.13 % (−0.55 .. −0.01) | −0.37 % (−1.52 .. −0.28) | +0.90 % (−1.68 .. +0.96) |

5000 msg/s — the cell that blocked the PR — is stable to well under 1 % on delivery p50. 5250 has
one host step (0.038 → 0.045 ms) that lands on both copies of A; it is not a code delta. Delivery
p99 is not a usable metric on this host (A/A at 5000 spans 0.049–0.094 ms).

Eager share under the external pacer is **0.996** on every A/A cell. The in-loop pacer at these
rates produced 0.43–0.60. Independent stimulus puts both arms on the eager path.

### A/B, interleaved ABBA × 2

Same pacer, same CPUs, same broker, same rates. A is `PYTHONPATH=/tmp/mqttium-main-4560e44/src`.
B is `PYTHONPATH` of this tree. Medians of four samples per arm.

| rate | arm | CPU µs/msg | ACK p50/p95/p99 | delivery p50/p95/p99 | eager/msg | token→publish p50/p95 µs |
| ---: | --- | ---------: | --------------: | -------------------: | --------: | -----------------------: |
| 4500 | A | 116.5 | 0.313 / 0.328 / 0.366 | 0.060 / 0.067 / 0.239 | 0.996 | 0.69 / 0.77 |
| 4500 | B | 111.0 | 0.308 / 0.318 / 0.349 | 0.060 / 0.065 / 0.245 | 0.996 | 0.68 / 0.74 |
| 5000 | A | 114.2 | 0.289 / 0.302 / 0.336 | 0.046 / 0.049 / 0.061 | 0.996 | 0.68 / 0.76 |
| 5000 | B | 107.1 | 0.284 / 0.291 / 0.330 | 0.045 / 0.050 / 0.077 | 0.996 | 0.69 / 0.75 |
| 5250 | A | 110.2 | 0.279 / 0.291 / 0.327 | 0.045 / 0.050 / 0.060 | 0.996 | 0.69 / 0.77 |
| 5250 | B | 108.4 | 0.275 / 0.286 / 0.320 | 0.045 / 0.049 / 0.061 | 0.995 | 0.68 / 0.76 |
| 5500 | A | 111.4 | 0.271 / 0.283 / 0.325 | 0.046 / 0.049 / 0.058 | 0.996 | 0.68 / 0.76 |
| 5500 | B | 106.4 | 0.266 / 0.278 / 0.318 | 0.046 / 0.052 / 0.065 | 0.996 | 0.67 / 0.74 |

A/B ratio, (B − A) / A:

| rate | CPU Δ | ACK p50 Δ | ACK p95 Δ | ACK p99 Δ | delivery p50 Δ | p95 Δ | p99 Δ | eager Δ |
| ---: | ----: | --------: | --------: | --------: | -------------: | ----: | ----: | ------: |
| 4500 | −4.7 % | −1.6 % | −3.2 % | −4.7 % | −0.3 % | −2.7 % | +2.7 % | +0.0 % |
| 5000 | −6.2 % | −1.7 % | −3.6 % | −1.8 % | −0.5 % | +1.4 % | +24.8 % | −0.0 % |
| 5250 | −1.6 % | −1.3 % | −1.6 % | −1.9 % | +0.0 % | −1.8 % | +1.2 % | −0.1 % |
| 5500 | −4.4 % | −1.9 % | −1.7 % | −2.1 % | −0.2 % | +4.7 % | +11.5 % | −0.0 % |

Delivery p50 at 5000 is −0.5 % against an A/A pair band of ±0.8 %. The in-loop campaign's
**+25–33 %** on this cell is gone. ACK p50/p95/p99 are slightly better on B at every rate.
Eager share is identical. The +24.8 % delivery p99 at 5000 is one B sample at 0.164 ms; the other
three B samples are 0.092 / 0.061 / 0.053, inside the A/A p99 span 0.049–0.094. It is not a
stable residual.

### Temporal equivalence

Pacer emission p50 is exact on both arms (222.222 / 200.000 / 190.476 / 181.818 µs). Receiver
inter-arrival p50 differs by at most 0.003 µs. Token→publish p50 is 0.67–0.69 µs on both arms;
p95 0.74–0.77 µs. Transport p50 is 22–25 µs, pacer-dominated, not client-dominated. Catch-up
fraction is 0.01–0.04 %. Lost tokens: 0. Completion ratio: 1.0000. Sequence integrity: ok.

The two arms received the same arrival process. The remaining latency gap is inside A/A noise.

### CPU, counted separately

Publisher `process_time` only. Pacer CPU is a dedicated spinning core (~1.9–2.3 s per 4 s sample)
and is **not** in `publisher cpu/msg`. Broker CPU from `/proc/<pid>/stat` is ~0.24–0.27 s on both
arms. B's publisher CPU saving survives (about −5 to −6 % at 4500/5000/5500, −1.6 % at 5250) and
is not a short-run artefact: completion is full.

The in-loop campaigns reported −10 to −17 % because they also counted time the loop spent
sleeping in the pacer. An independent clock shrinks that gap; it does not remove it.

### What this does not yet cover

MQTT 5, windows 8 and 64, and a repeat on the original i7-3770 were not part of this control.
Those are a small confidence set, not a new investigation: the 3.1.1 / w32 matrix at the four
rates that defined the blocker is enough to classify the in-loop signal.

**Outcome: `IN-LOOP PACING ARTEFACT CONFIRMED`.** The previous 5000 msg/s adverse delivery-p50
reading measured a coupled pacer/runtime system. Under a temporally independent load it is not a
demonstrated runtime regression.

## External-pacer control — repeat on the original i7-3770

The section above ran on the Cloud Agent host and asked for this repeat. Same harness
(`benchmarks/external_pacer.py`, unchanged), same runtime (`src/` byte-identical to `5f2dd2d`),
same question, on the desktop that produced every earlier table in this report — so these
milliseconds *are* comparable to the in-loop campaigns.

### Host and placement

| | |
| --- | --- |
| CPU | Intel i7-3770, 4 physical cores, **SMT** — sibling pairs (0,4) (1,5) (2,6) (3,7) |
| Governor | `performance` |
| Python | 3.12.13 · Mosquitto 2.0.18, fresh instance on `127.0.0.1:21883` |
| Affinity | desktop CPU 0 · harness + `mosquitto_sub` CPU 4 · broker CPU 1 · publisher CPU 2 · pacer CPU 3 |

The cloud host has no SMT, so its 0/1/2/3 map is four whole cores. Here it is not: five consumers
share four physical cores. Broker, publisher and pacer each get a core with an **idle sibling**;
the desktop and the harness share core 0. A first attempt paired the broker with the subscriber on
core 1 and produced 8–10 ms ACK p99 tails; that placement is superseded by the one above.

### Pacer qualification (no MQTT)

| rate | target µs | emission p50 | emission p95 | emission p99 | receiver p50 | receiver p95 | lateness p95 | transport p95 | catch-up | lost |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4500 | 222.2 | 222.222 | 222.350 | 222.744 | 222.188 | 224.521 | **0.388 µs** | 13.9 µs | 0.000% | 0 |
| 5000 | 200.0 | 199.999 | 200.099 | 200.316 | 199.971 | 202.130 | **0.344 µs** | 15.9 µs | 0.007% | 0 |
| 5250 | 190.5 | 190.476 | 190.573 | 190.856 | 190.435 | 192.616 | **0.350 µs** | 18.7 µs | 0.025% | 0 |
| 5500 | 181.8 | 181.817 | 181.929 | 182.220 | 181.755 | 184.068 | **0.391 µs** | 15.3 µs | 0.133% | 0 |

Worst lateness p95 is **0.391 µs** against the ~1100 µs of the in-loop `asyncio.sleep` pacer:
**2813× tighter**, with zero lost tokens and intact sequences. The acceptance gate is met.

### What this host cannot qualify

`publisher_cpu_seconds` is **2.99999 s for a 3.000 s sample** — the publisher sits at 100.00 % of
one core at every rate, so `cpu/msg` reads the target interval exactly (ratio 1.003 at all four
rates) for both arms. The cause is the harness's own per-token receive path (~50–55 µs/msg on top
of MQTTium's ~145–150 µs): the transport backlog means a token is always already buffered,
`sock_recv` never blocks, and `process_time` degenerates to wall time. **No CPU conclusion can be
drawn on this host** — neither for nor against. The in-loop campaigns and the cloud host remain the
CPU evidence.

The same ceiling widens the tail bands. A/A max-min spread over 8 identical samples:

| rate | deliv p50 | deliv p95 | deliv p99 | ACK p50 | ACK p95 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4500 | +4.97% | +36.3% | +131.7% | +11.7% | +18.1% |
| **5000** | **+1.57%** | +10.4% | +19.5% | +3.7% | +16.2% |
| **5250** | **+2.09%** | +86.6% | +36.4% | +8.7% | +102.6% |
| 5500 | +10.86% | +38.9% | +37.9% | +5.0% | +21.1% |

Only **delivery p50 at 5000 and 5250** has a band tight enough to read (±1.6 % / ±2.1 %). Those are
precisely the rates that defined the blocker, so the control still answers its question — but
p95, p99 and ACK are reported below as descriptive, not as claims.

### A/B — main (A) versus PR #432 (B), external pacer

Validity: pacer emitted rate and publisher received rate agree to 0.1 msg/s between arms,
completion ratio 1.0000, zero lost tokens, sequences intact, at all four rates.

| rate | arm | deliv p50 | deliv p95 | ACK p50 | ACK p95 | eager/msg | transport p50 | recv interval p50 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4500 | A | 0.111 | 0.154 | 0.585 | 0.691 | 0.954 | 124.5 µs | 223.3 µs |
| 4500 | B | 0.112 | 0.135 | 0.410 | 0.470 | 0.985 | 40.2 µs | 220.5 µs |
| | **B/A** | **+0.65%** | −12.2% | −29.9% | −32.0% | +3.2% | −67.7% | −1.3% |
| 5000 | A | 0.113 | 0.194 | 0.595 | 0.779 | 0.753 | 109.2 µs | 216.9 µs |
| 5000 | B | 0.112 | 0.185 | 0.509 | 0.615 | 0.828 | 106.4 µs | 210.8 µs |
| | **B/A** | **−0.31%** | −5.0% | −14.5% | −21.0% | +10.0% | −2.6% | −2.8% |
| 5250 | A | 0.114 | 0.211 | 0.647 | 0.926 | 0.654 | 103.0 µs | 215.8 µs |
| 5250 | B | 0.112 | 0.184 | 0.507 | 0.642 | 0.740 | 100.4 µs | 210.0 µs |
| | **B/A** | **−1.88%** | −12.9% | −21.6% | −30.7% | +13.1% | −2.6% | −2.7% |
| 5500 | A | 0.116 | 0.209 | 0.704 | 0.898 | 0.587 | 102.6 µs | 215.3 µs |
| 5500 | B | 0.113 | 0.184 | 0.519 | 0.646 | 0.636 | 95.3 µs | 210.6 µs |
| | **B/A** | **−2.23%** | −11.7% | −26.3% | −28.0% | +8.2% | −7.2% | −2.2% |

**Delivery p50 is neutral to better at every rate**, inside the A/A band at the two rates where
that band is trustworthy: **−0.31 % at 5000** against an in-loop **+22 to +27 %**, and −1.88 % at
5250. The residual intra-class difference the previous section flagged (on-time p50 0.113 vs 0.130
at 5000) is also gone: 0.113 vs 0.112.

ACK p50 improves 14–30 %, beyond its A/A band of 3.7–11.7 %. Delivery p95/p99 improve 5–13 % but
their bands are far wider, so that is descriptive only.

### Temporal equivalence

| quantity | skew B vs A |
| --- | ---: |
| pacer emitted rate | ≤ 0.002 % |
| publisher received rate | ≤ 0.002 % |
| receiver inter-arrival p50 | −1.3 % to −2.8 % |
| receiver inter-arrival p95 | −3.6 % to −9.2 % |
| token → publish p50 | −1.0 % to −3.7 % |
| transport delay p50 | −2.6 % to **−67.7 %** (4500) |

Not perfectly identical, but two orders of magnitude closer than the in-loop pacer, whose
inter-arrival p95 differed by a factor of five between regimes. One asymmetry deserves naming: at
4500 msg/s the candidate drains the token socket in 40 µs where main takes 124 µs. Transport delay
still depends on how promptly the publisher loop picks work up, so a residual coupling survives —
it is small at 5000–5500 and large at 4500.

### Agreement between hosts

| | Cloud Agent host | i7-3770 |
| --- | ---: | ---: |
| delivery p50 at 5000 msg/s, B/A | −0.5 % | **−0.31 %** |
| in-loop equivalent | +25–33 % | +22–27 % |

Two different CPUs, two brokers, two Python builds, same conclusion.

**Outcome on this host: `IN-LOOP PACING ARTEFACT CONFIRMED`**, with CPU explicitly not qualifiable
here and tails not qualifiable here.

## Verdict

The matched-load campaigns remain in this report as closed-loop and fixed-average-rate
observations. They showed a real 25–33 % delivery-p50 gap at 5000 msg/s when the pacer lived in
MQTTium's event loop. The rate-regime diagnosis showed that gap was a class-mix effect driven by
`asyncio.sleep` overshooting to ~1.1 ms.

The external-pacer control then gave A and B the same arrival process. On the Cloud Agent host,
MQTT 3.1.1 QoS 1 window 32 payload 64 at 4500/5000/5250/5500:

- delivery p50/p95 sit inside the A/A noise floor (5000 p50 **−0.5 %** vs an in-loop **+25–33 %**);
- ACK latency is neutral to slightly better;
- eager/msg is 0.996 on both arms;
- publisher CPU remains lower on B (−4 to −6 % at three of four rates);
- pacer, transport and token→publish distributions match.

**`IN-LOOP PACING ARTEFACT CONFIRMED`.** The i7-3770 repeat asked for here has since been run and
agrees: delivery p50 at 5000 msg/s is −0.31 % against an in-loop +22 to +27 %. What that host
cannot settle is CPU — its publisher saturates one core under the harness's own per-token cost, so
`cpu/msg` reads the target interval for both arms — and the tail bands, whose A/A spread reaches
tens of percent there.

Do not merge until a small MQTT 5 / window confidence set lands. Nothing here authorises a runtime
change; the candidate is still the `5f2dd2d` waiter implementation.

Everything else in this report stands: correctness, the ~59 % in-process gain on the
awaited-receipt path, +6.2 to +6.6 % sequential ACK throughput, the −73 % ack p50 at 2500 msg/s
under in-loop pacing, a real CPU saving, strictly lower memory with no retained waiter futures,
and neutrality on QoS 0, QoS 1 never-awaited, QoS 2, callback completion, persistence and writer
paths.
