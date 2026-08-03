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
