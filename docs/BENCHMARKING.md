# Benchmarking contract

Benchmark results are build artefacts, not source-code claims.

- `perf_sprint.py` detects local implementation regressions.
- `compare_libs.py` compares only equivalent public contracts. A library is
  reported as `N/A` instead of receiving artificial synchronization barriers.
- `realworld.py` uses fresh publisher processes plus an independent
  `mosquitto_sub`, verifies every sequence number, and reports broker ACK and
  confirmed delivery separately.
- `application_stress.py` measures ordered callbacks, iterator backpressure,
  and memory/SQLite inflight persistence, including batched versus autocommit
  transactions.

The scheduled workflow covers local TCP, TLS, and a controlled `netem` profile.
Every result records Python, platform, package versions, payload, QoS, inflight
window, transport/profile, CPU, RSS and latency percentiles. PUBACK confirms
broker acceptance; it never proves consumer delivery.

Comparisons must use equivalent public completion semantics and rotate execution
order where warm-up could matter. A result is omitted rather than manufactured
with library-specific barriers. CI uploads JSON artefacts and never commits or
pushes generated numbers.

## Interprétation de la latence

`realworld.py` horodate immédiatement avant l'appel applicatif à `publish()`.
La latence publiée inclut donc l'admission locale et le temps passé dans les
files, pas seulement le trajet réseau. Une grande fenêtre inflight augmente le
débit en permettant le batching, mais peut mécaniquement augmenter les
percentiles de latence. Le paramètre `--window` doit être balayé avant de
qualifier une variation de régression.

Pour une modification sensible aux chemins chauds,
`paired_regression.py` et `paired_network.py` exécutent `main` et le candidat
sur le même runner, en ordre alterné et dans des interpréteurs frais. Les
mesures réseau à fenêtre élevée restent sujettes aux pauses du runner et du
subscriber ; les ratios micro appariés et les tendances sur plusieurs fenêtres
priment sur une valeur isolée.

## Memory regression thresholds

`benchmarks/memory_profile.py` is guarded by `benchmarks/check_memory_thresholds.py`,
which the benchmarks workflow runs immediately after the profile and which fails
the build on a breach.

`benchmarks/memory_thresholds.json` is versioned deliberately. It holds
*limits*, not measurements, so it does not violate the artefact-only rule above:
no generated number is committed. It bounds the tracemalloc peak — a count of
Python allocations, comparable across runners, unlike absolute RSS — and
asserts each scenario's logical counters exactly, so a benchmark that quietly
stopped doing equivalent work cannot pass as an improvement.

Reference values live in `docs/MEMORY-RESULTS.md`. Raising a threshold is a
reviewable change and needs a reason.
