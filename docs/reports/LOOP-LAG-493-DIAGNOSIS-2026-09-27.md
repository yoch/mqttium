# Loop-lag diagnosis for issue #493

Date: 2026-09-27. Scope: [issue #493](https://github.com/yoch/mqttium/issues/493),
+15–20 % `loop_lag_p95` at 64-byte saturation from RC14 (`c194597`) to the
lean-native rewrite, on the dedicated Pi 5 runner. Question: is it a defect of
the measurement or of the software?

Evidence: the raw artifacts of 13 strict open-loop gate runs, re-read by
`benchmarks/open_loop_lag_analysis.py` in the `Loop-lag diagnosis` workflow
(pull request #587):

- the 2026-09-23 campaign against `ed52965` (RC14, three runs; the #457
  bisection commits `6fd09d8`, `7eaed83`, `12d1b79` and `3ba3880`; three
  same-code A/A runs);
- the RC15 → RC16 gate (36025710297) and the RC16 → RC17 gate (36267803906).

In total 241 cells and 1,812 samples.

## What `loop_lag_p95` measures

The paced publisher in `paired_open_loop.py` records, for each publication, how
late it starts against its own schedule `start + k / target`. That is neither
the event-loop latency of the client nor a closed-loop latency:

- while the publisher sleeps between publications, the value sits on a timer
  plateau (about 1 ms);
- once it no longer sleeps, the value is how long the loop keeps other work
  (acknowledgement reads, settlement, receipt waiters, writer) before the
  publisher's next turn: a queueing delay that grows like `ρ/(1 − ρ)` in the
  utilisation `ρ`;
- if the publisher cannot keep its offered rate at all, the delay accumulates
  into a backlog, `0.95·count·(1/offered − 1/target)`.

## Findings

### 1. No backlog

No sample fell behind its offered rate: offered/target is 0.999–1.000 in every
cell, including load 1.00. The backlog explanation considered first is
refuted. All 76 cells with a lag ratio above 1.05 are paced cells.

### 2. The absolute lag follows the calibration level, not the code

The target at a given load is a fraction of a closed-loop capacity calibrated
at the start of each run. That capacity settles on distinct levels for every
commit, RC14 included: about 20.5k, 22.3k and 26k msgs/s for 64 B QoS 1. The
26 runner samples of the RC16 and RC17 runs show every CPU at a constant
2.4 GHz with no time spent at lower frequencies, so the levels are not
frequency steps. Firmware throttling was not reported (`null`) by those
samples.

The lag at loads 0.90 and 1.00 moves with the level far more than with the
code. Same commit (`ed52965` A/A, run 35848620365):

| Protocol | Target at load 1.00 | `loop_lag_p95`, both arms |
| --- | ---: | --- |
| MQTT 5 | 22,295 msgs/s | 0.19 ms |
| MQTT 3.1.1 | 26,336 msgs/s | 0.71–0.78 ms |

Across all runs, load-1.00 cells with targets of 20.5k–22.6k msgs/s report
0.14–0.24 ms; cells with targets of 25.3k–26.5k msgs/s report 0.59–0.85 ms.

### 3. The lag ratio amplifies cost differences near saturation

Near `ρ ≈ 0.9` the queueing delay amplifies a per-message cost difference about
tenfold: a 2 % cost gap reads as a lag ratio near 1.2. A run whose calibration
lands on the high level compares the arms much closer to saturation than one
that lands on a lower level, so the same code pair produces very different
ratios from run to run. The same-code A/A initial cells reach 1.13.

### 4. CPU per message is not measurable with this harness

The publisher is busy for at least 95 % of its wall time in most samples,
including many at load 0.50 on MQTT 5. Its CPU time then equals its wall time,
identical in both arms by construction. The few cells where both arms still
slept (MQTT 3.1.1, load 0.50) show a CPU-per-message ratio of 1.03–1.06 for
same-code A/A and 1.04–1.10 for RC14 → `ed52965`: no conclusion is possible at
that noise.

### 5. A real difference remains between RC14 and the rewrite

Paired cells at the high calibration level (targets ≥ 25.3k msgs/s), 64 B:

| Base → `ed52965` | Run | Protocol | Lag ratio at load 0.90 / 1.00 |
| --- | --- | --- | --- |
| RC14 | 35843620810 | 5 | 1.27 / 1.24 (confirmation 1.20) |
| RC14 | 35848449531 | 3.1.1 | 1.22 / 1.25 (confirmation 1.24) |
| RC14 | 35848449531 | 5 | 1.21 / 1.30 |
| RC14 | 35850177883 | 5 | 1.20 / 1.31 |
| same code | 35845226479 | 5 | 1.08 / 1.06 |
| same code | 35848620365 | 3.1.1 | 1.08 / 1.06 (confirmation 1.09) |
| same code | 35850729484 | 5 | 1.13 / 1.04 |
| `6fd09d8` (#457 start) | 35846178400 | 5 | 1.06 / 1.13 |
| `3ba3880` (#457 end) | 35846212211 | 5 | 1.15 / 1.07 |
| `7eaed83` (#457, commit 24) | 35846972388 | 3.1.1 / 5 | 0.96 / 0.94, 0.95 / 0.96 |
| `12d1b79` (#457, commit 36) | 35847538941 | 5 | 0.97 / 0.99 |

Later gates, same method: RC15 → RC16 (36025710297) 0.86 / 0.89 on MQTT 5; RC16
→ RC17 (36267803906) 1.07 / 0.99 on MQTT 5.

RC14 → `ed52965` exceeds the same-code spread in every high-level cell of the
three runs. Under the queueing amplification of finding 3, a lag ratio of
1.2–1.3 corresponds to a per-message cost difference of roughly 2–3 % near
saturation. That size is a model estimate, not a measurement: finding 4 rules
out measuring it with this harness. RC14 has never been compared directly with
RC16 or RC17.

## Verdict

Both, in different proportions.

- **Measurement.** The strict open-loop gate cannot quantify or bound this
  effect, for three reasons:
  - it anchors its loads to a multi-level calibration whose levels are not
    frequency steps;
  - its lag ratio near saturation amplifies small cost differences roughly
    tenfold and changes with that level;
  - its CPU time cannot show per-message cost once the publisher never sleeps.
  
  The "+15–20 %" is a lag ratio at the high level, not a measured latency or
  cost.
- **Software.** The data show a small, reproducible extra cost of the rewrite
  over RC14, visible only near saturation (about 26k msgs/s on the Pi 5). It is
  a throughput cost, not an event-loop stall: no backlog, unchanged completed
  throughput, and no difference below about 75 % load. Its present size for
  RC17 is unknown.

## What closes #493

1. Run the `Loop-lag diagnosis` sweep on the ARM64 runner with RC14
   (`c194597bcf5af4951fbec2b560600eef3cb84b3c`) against RC17
   (`c9bab1ad93dd2e875c07706aecdeaa69eb8dce89`):
   - fixed absolute rates from 5,000 to 26,000 msgs/s;
   - MQTT 3.1.1 and 5;
   - read CPU per message only at rates where both arms stay below 95 % busy;
   - compare schedule-lag curves rate by rate.
2. Decision for 1.0:
   - if RC17 costs at most about 3 % more per message than RC14 and its lag
     curve matches below saturation, document the saturation cost and close;
   - otherwise profile the acknowledgement and receipt path, the suspect since
     the rewrite.
3. Separately, fix the gate:
   - absolute target rates, or reject a calibration that changes level;
   - no lag ratio at load ≥ 0.90 as a release criterion;
   - a per-message cost measurement at a rate where the publisher sleeps.
