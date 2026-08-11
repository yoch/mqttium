# Performance report — 1.0.0rc1

All ratios in this report compare the candidate worktree with `fbf1887` in
fresh local processes and ABBA order. GitHub runners are not evidence.

## Exact profiling and simplification result

The first generic `ApplicationDelivery` extraction regressed `delivery_both` to
roughly `0.74×`. cProfile attributed the loss to eight extra Python calls per
accounted message (`deliver_inline`, `deliver`, mode/size checks and reservation
helpers). That candidate was rejected.

The retained design chooses one mode-specialised admission function at client
construction. It keeps one authoritative controller and removes repeated
per-message mode branches. The old duplicate `deliver`/`deliver_inline` code was
deleted rather than tested as unreachable compatibility code.

`benchmarks/hotpath_profile.py` records exact calls/operation, primitive calls,
top self/cumulative functions and tracemalloc peaks for every retained micro
scenario. `paired_open_loop.py` adds paced capacity fractions and end-to-end
latency/loop-lag/completeness evidence.

## Reproducible micro gains

Final run: 11 CPU-pinned local pairs per scenario. Only rows with baseline CV
at most 5% are interpreted. The acceptance rule for a micro gain is at least
2% with at least 8/11 favourable pairs.

| Scenario | Candidate/base | Baseline CV | Favourable pairs | Decision |
| --- | ---: | ---: | ---: | --- |
| delivery iterator | 1.1202 | 3.88% | 10/11 | retained |
| delivery both | 1.0983 | 3.62% | 11/11 | retained |
| WebSocket mask 4 KiB | 1.0298 | 3.36% | 10/11 | retained (existing path, no new complexity) |
| exact ingress QoS 1 | 1.0084 | 4.83% | 8/11 | neutral; below gain threshold |
| compat publish QoS 1 | 0.9968 | 4.89% | 4/11 | neutral; within guardrail |
| ordered EffectPump batch | 1.0161 | 2.83% | 8/11 | neutral; below gain threshold |

Callback delivery measured `1.1596×`, but its baseline CV was 6.22%; it is
directionally consistent and not a numeric claim. SQLite, several EffectPump
cells, QoS 0 batch compatibility and unawaited receipts were likewise too noisy
for interpretation. No candidate was introduced to chase them.

## Network and open-loop status

### Harness overhead audit

The first network harness read and timestamped `mosquitto_sub` output in a
Python thread inside the measured publisher process. An A/A control at MQTT
3.1.1, 4 KiB, window 32 and eight pairs exposed the interference:

| Harness | Median ACK/s | Baseline CV | Delivery p50 | Publisher CPU |
| --- | ---: | ---: | ---: | ---: |
| in-process reader thread | 11,580 | 4.23% | 29.31 ms | 0.168 s |
| separate observer process, same QoS 1 workload | 10,332 | 3.38% | 0.76 ms | 0.169 s |

The reader did not consume measurable publisher CPU time, but GIL scheduling
inflated observed delivery latency by roughly 28.6 ms and let subscriber work
lag behind the publisher. The apparent higher throughput was therefore not a
valid improvement: the observer was failing to keep pace with the workload it
was meant to confirm.

The retained harness timestamps subscriber output in a separate process and
uses a QoS 0 observer, which checks exact delivery and sequence without adding a
second PUBACK stream to the publisher-PUBACK measurement. Every cell now
calibrates a common message count toward a target duration and records the
actual duration. Open-loop calibration uses the same subscriber, completion,
and telemetry path as its paced samples. A small A/A open-loop smoke after the
change measured completion ratio `0.9966` and loop-lag ratio `0.9156`; it was a
functional control on an ineligible host, not release evidence.

The decisive closed-loop A/A control ran after a successful runner preflight,
using both MQTT versions, both payload sizes, window 32 and eight pairs. Ratios
were `1.0133`, `1.0100`, `1.0710` and `1.0170`, but baseline CVs were `8.49%`,
`11.49%`, `15.96%` and `23.05%`. Since all four cells exceeded the 5% validity
limit, the benchmark is rejected as a release gate. It remains useful as an
advisory diagnostic, with its invalid status visible in JSON and Markdown.

A targeted strict open-loop run at 75% calibrated load (MQTT 3.1.1, 64 bytes,
receipt completion, four pairs) passed with completeness ratio `1.0004` and
candidate/base loop-lag ratio `0.7767`.

The former strict closed-loop sweep was **invalid**, not regressed. One
run against a broker using Mosquitto's default subscriber queue lost observer
messages when the faster publisher outran that observer. Repeating on the
dedicated configuration (`max_queued_messages=100000`) delivered 1500/1500 and
measured candidate/base `0.9663`, but baseline CV was 5.06%, just above the 5%
validity threshold. No end-to-end gain or regression is claimed from this
harness; the later A/A rejection makes further A/B sweeps unjustified.

## Rejected or deliberately unchanged work

- The generic delivery-controller call cascade was rejected and replaced by a
  simpler construction-time strategy.
- Experiments already rejected in issue #39 were not replayed: inline user
  callbacks, weakened isolation, removing fairness yields and bypassing bounded
  writer/effect ownership remain out of scope because their premises did not
  change.
- No threshold was raised, no runtime dependency was added, and no new
  complexity exemption was introduced.
