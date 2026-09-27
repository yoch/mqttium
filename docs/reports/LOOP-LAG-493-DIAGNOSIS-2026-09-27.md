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

## Addendum: fixed-rate sweep, RC14 against RC17 (2026-09-27)

Run [36351522154](https://github.com/yoch/mqttium/actions/runs/36351522154)
of the `Loop-lag diagnosis` sweep, on the dedicated Pi 5 runner:

- RC14 `c194597b` as base, RC17 `c9bab1ad` as candidate;
- `rate_regime_probe.py`, 64 B QoS 1, window 100, receipt completion;
- two ABBA blocks per protocol (four samples per arm and rate), each block
  preceded by a recorded runner probe. Both MQTT 3.1.1 probes were eligible;
  both MQTT 5 probes were **ineligible** (one-minute load per CPU 0.303 and
  0.257 against a 0.250 limit). Instantaneous CPU use was 0.3 % and 0.0 %.
  The workflow recorded eligibility without enforcing it. Residual load from
  the preceding sweep is a possible explanation, not an established cause;
  the MQTT 5 measurements remain diagnostic, not strict qualification.

Medians per arm; ratios are RC17 over RC14.

| Protocol | Rate (msgs/s) | CPU/message RC14 / RC17 (µs) | Ratio | Schedule lag p95 RC14 / RC17 (ms) | Ratio | ACK p50 RC14 / RC17 (ms) |
| --- | ---: | --- | ---: | --- | ---: | --- |
| 3.1.1 | 5,000 | 59.9 / 60.1 | 1.004 | 1.029 / 1.023 | 1.00 | 0.291 / 0.296 |
| 3.1.1 | 10,000 | 55.8 / 55.5 | 0.996 | 1.010 / 1.012 | 1.00 | 0.453 / 0.444 |
| 3.1.1 | 15,000 | busy | — | 0.087 / 0.086 | 0.98 | 0.272 / 0.273 |
| 3.1.1 | 20,000 | busy | — | 0.127 / 0.126 | 0.99 | 0.405 / 0.412 |
| 3.1.1 | 22,000 | busy | — | 0.155 / 0.160 | 1.03 | 0.460 / 0.473 |
| 3.1.1 | 24,000 | busy | — | 0.238 / 0.233 | 0.98 | 0.585 / 0.592 |
| 3.1.1 | 26,000 | busy | — | 1.984 / 1.656 | 0.83 | 0.752 / 0.777 |
| 5 | 5,000 | 60.8 / 61.7 | 1.015 | 1.026 / 1.030 | 1.00 | 0.297 / 0.302 |
| 5 | 10,000 | 56.5 / 56.7 | 1.004 | 1.011 / 1.012 | 1.00 | 0.449 / 0.443 |
| 5 | 15,000 | busy | — | 0.088 / 0.087 | 0.99 | 0.286 / 0.294 |
| 5 | 20,000 | busy | — | 0.129 / 0.128 | 0.99 | 0.417 / 0.420 |
| 5 | 22,000 | busy | — | 0.173 / 0.170 | 0.98 | 0.493 / 0.535 |
| 5 | 24,000 | busy | — | 0.285 / 0.265 | 0.93 | 0.593 / 0.655 |
| 5 | 26,000 | busy | — | 2.094 / 1.757 | 0.84 | 0.842 / 0.888 |

"Busy" marks rates where CPU per message times the rate is at least 1.0 for
both arms: the publisher never slept, so its CPU time is its wall time and the
per-message cost is not measured there (18,000 msgs/s behaves the same and is
omitted). Every sample kept its offered rate within 0.06 % of target, and all
publications completed. Values above are recomputed medians from the retained
JSON samples (four per arm and rate), rounded only for display.

Results:

- **Per-message cost.** At the rates where both arms still sleep, RC17 costs
  0.996–1.015 times RC14 per message. That is within 1.5 %, below the 3 %
  decision threshold of this report.
- **Schedule lag.** At fixed absolute rates the curves match: 0.98–1.03 up to
  24,000 msgs/s on MQTT 3.1.1 and up to 22,000 on MQTT 5. RC17 is lower near
  saturation: 0.93 at 24,000 on MQTT 5, and 0.83–0.84 at 26,000 on both
  protocols.
- **Residual.** RC17's median acknowledgement latency is within −2 % to +4 % of
  RC14's at most rates, and 5–10 % higher at 22,000–26,000 msgs/s on MQTT 5
  (up to 0.06 ms). Subscriber delivery p50 is 2–8 % higher. Both are small,
  near-saturation differences with no throughput or schedule-lag cost.

The sweep ran no same-code control; the ABBA spread of each cell is its only
noise estimate. In this sweep, the "+15–20 %" loop-lag increase of #493
does not reproduce at fixed rates; measured per-message cost is within 1.5 %
of RC14. This supports the report's decision rule for the observed workload,
not a general equivalence claim. Strict MQTT 5 qualification still needs an
eligible rerun; the follow-up gate must enforce preflight eligibility and
retain same-code controls.


## Addendum: corrected gate qualification (2026-09-28)

The corrected gate was tested on the dedicated Pi 5 with Python 3.14.7,
publisher CPU 2, MQTT 3.1.1 and 5, 64-byte and 4096-byte payloads, QoS 1
receipt completion, window 100, and fixed rates of 5k, 10k, 15k, 20k, 22k,
24k and 26k messages/s. The harness commit is
`d29a8462bf971198a477925c8f4cf36123530cf7`; its entire `src/` tree is identical
to RC17 `c9bab1ad93dd2e875c07706aecdeaa69eb8dce89`.

- [Same-code control 36353058646](https://github.com/yoch/mqttium/actions/runs/36353058646):
  **passed**, 28 cells, 224 initial worker samples and 5,856,000 publications,
  all completed. All three final preflights were eligible; MQTT 5 needed five
  attempts before eligibility was stable. CPU/message ratios at 5k/10k were
  0.9852–1.0028 across both payload sizes. Saturation lag ratios reached 1.3984
  for identical code; those cells correctly remained diagnostic.
- [RC14 → RC17 comparison 36353060011](https://github.com/yoch/mqttium/actions/runs/36353060011):
  **failed**, with all five final preflights eligible, including both
  confirmation preflights. The confirmed failure is MQTT 5, 4096 bytes,
  24k messages/s: completed-rate ratio **0.9326**, below the unchanged 0.97
  threshold. The 26k cell also required confirmation and recovered above the
  threshold. No publication was lost.

The gate uses `paired_open_loop.py`, whereas the preceding diagnostic sweep
used `rate_regime_probe.py`. Compare the arms within each acquisition; do not
interpret differences between the two harnesses as runtime changes.

Only 5k and 10k provided eligible sleeping, unsaturated comparisons in every
scenario. The following CPU figures are ratios of per-arm medians; lag figures
are the gate's median paired ratios, candidate over RC14:

| Protocol | Payload | CPU/message at 5k / 10k | Schedule lag at 5k / 10k |
| --- | ---: | --- | --- |
| 3.1.1 | 64 B | 1.0013 / 1.0110 | 0.9979 / 1.0009 |
| 5 | 64 B | 0.9953 / 1.0239 | 1.0000 / 1.0015 |
| 3.1.1 | 4096 B | 0.9949 / 1.0167 | 1.0006 / 1.0019 |
| 5 | 4096 B | 0.9620 / 1.0386 | 1.0071 / 1.0018 |

For the original 64-byte scope of #493, no +15–20% lag increase reproduces at
fixed rates, and the measured CPU cost remains within the report's 3% decision
threshold. That result does **not** establish general RC14/RC17 equivalence.
The extended 4096-byte scope exposes a separate outstanding qualification
failure, and its MQTT 5 CPU/message ratio at 10k is also above 1.03 (diagnostic,
not an automatically confirmed CPU regression).

At the failing 24k point, eight paired samples were retained after confirmation.
Seven candidate samples have lower completed throughput than their paired
baseline by more than 3%; one initial pair is effectively equal. The offered
rate remains near 24k in both arms, but the candidate takes longer to finish
all receipts. Median ACK latency across the eight per-arm sample medians is
about 1.25 ms for RC14 and 99 ms for RC17. This is a completion/drain-time
finding, not a missing-message finding. Its implementation cause has not been
established, and it has not been accepted as a release trade-off.

PR [#588](https://github.com/yoch/mqttium/pull/588) retains the report and gate
changes together. Qualification remains open until this completed-throughput
finding is explained and resolved or explicitly accepted. The correction keeps
the failure visible; neither its rate nor its threshold was removed to obtain
a passing result.

Limits: this is one same-code campaign and one RC14/RC17 campaign, with targeted
confirmation inside the latter, not a cross-run equivalence study. Firmware
throttling telemetry was unavailable (`null`); the recorded CPU frequencies
were 2.4 GHz, and eligibility follows the existing probe's enforced criteria.

Retained `open-loop.json` SHA256 digests:

- same-code: `eb6015c7adc7994d6118b80366c0eea4ba9a68b2cfbc59ec2fdef2880ce49b81`;
- RC14/RC17: `d741331e148d0c0ed5f8515f13de41db61cf352ecf264157616defb4f82ac407`.
