# Release candidate report — 1.0.0rc11

Candidate source before release metadata:
`a9050562f5186dec0f9b0d081f43419dfa4794c8` (`main`, 2026-08-30).
Release metadata and hosted-candidate commit:
`c6ffabc24653778f35112cfb8f153456c20f2835`.
Final evidence/harness commit:
`222971802589d303cb5c46bc1baa3e4254d49a0d`.

## Scope

RC11 collects the reviewed callback and MQTT 5 correctness work merged after
`v1.0.0rc10`. Its principal changes are:

- invoke an isolated synchronous `on_publish` or eligible `on_message`
  callback in the receipt/delivery turn while retaining the bounded worker for
  bursts, asynchronous callbacks and reentrancy;
- add Stable topic-filtered native message callbacks while keeping the direct
  callback-free and no-filter paths lazy;
- enforce the Receive Maximum, Maximum Packet Size and Topic Alias Maximum
  values actually advertised in MQTT 5 CONNECT;
- reuse a broker-assigned Client Identifier for durable reconnects owned by the
  same engine, and fail closed when persisted QoS state cannot be associated
  with a stable identity after process restart;
- preserve fatal packet-error classifications through the runtime boundary;
- validate outbound MQTT 5 payloads whose Payload Format Indicator is 1 before
  wire transmission or QoS persistence; and
- correct the maintained Paho compatibility explanation of successful and
  failed PUBREC effects on Receive Maximum.

No Stable API was removed or changed incompatibly. Persisting a broker-assigned
Client Identifier remains outside the `InflightStore` contract; applications
that resume durable broker sessions after a process restart must configure an
explicit stable ClientID.

The package version is `1.0.0rc11`. Its release tag must be exactly
`v1.0.0rc11`, and the GitHub release must be marked as a prerelease.

## Local functional and robustness evidence

The clean local command

```text
python benchmarks/local_release.py quick --base-ref v1.0.0rc10
```

passed at `c6ffabc` on Linux x86-64, Python 3.12.13 and Mosquitto 2.0.18. Its
manifest recorded 12 successful commands in 336.1 seconds:

- Ruff format/lint, mypy and Bandit;
- 1,522 unit tests with 90.49% source coverage, above the 87.36% floor;
- all 16 required Mosquitto integration tests;
- the hot-path call/allocation profile;
- all 15 memory scenarios and their retained thresholds;
- application stress; and
- 30-second forced-reconnect soaks for MQTT 3.1.1 and MQTT 5.

Each soak published and received 15,500 messages. The MQTT 3.1.1 run completed
29 measured cycles with 31 forced reconnects; MQTT 5 completed 29 cycles with
30 forced reconnects. Both resource assessments were stable with no discrete
violations.

The host was under unrelated load, so no local timing result is used as release
performance evidence. The call/allocation profile and memory thresholds are
structural/resource gates, not a small-regression timing claim.

Additional local checks passed: 18 project tests, 60,000 deterministic fuzz
iterations across codec, engine and WebSocket targets, 52 Hypothesis/stateful
tests, and the strict documentation build.

## Hosted matrix and exact distributions

The exact `2229718` PR head passed repository quality, Python 3.11–3.14,
Linux MQTT 3.1.1/MQTT 5 soaks, resilience, fuzz, package validation, macOS
3.11/3.14, Windows 3.11/3.14, Codecov, Read the Docs and the `CI required`
aggregate. The pull-request publication workflow built one wheel and one sdist,
then passed wheel smokes on Python 3.11–3.14, the Python 3.12 sdist smoke and
the installed-artifact resilience smoke. Publication jobs were correctly
skipped.

The downloaded Actions artifact passed strict Twine and wheel-content checks.
Its SHA-256 values are:

- wheel: `d98c52fb3e96beea03a524d0796c7b2fc9cc46060c0582c7450865b29b6e0675`;
- sdist: `7c379e45df8a6383100fbfe5d1c8d7681c848201aeba6c8799fab28d85569629`.

## ARM64 network-gate validity

The failed controls were traced to a process-layout-sensitive allocation mode,
not to competing runner load or CPU throttling. In the slow mode each inbound
read caused roughly two additional minor faults per message around asyncio's
256 KiB receive allocation. It reproduced on Python 3.13 and 3.14 on the same
host, so it is not a Python 3.14 regression. Pinning the benchmark process
layout with `setarch -R` removes the bimodality without changing MQTTium, the
scenario or any acceptance threshold. The runner remains checked for active
CPU use, temperature, governor and frequency before every block; only the
one-minute historical-load check is limited to the initial preflight so a
completed block cannot reject its successor.

[Strict run 33381427389](https://github.com/yoch/mqttium/actions/runs/33381427389)
compared exact baseline `1cd4dce6d3e6169323faa1c1d6a19f9d4597402a`
(`v1.0.0rc10`) with exact candidate `2229718`. It passed with two independent
control blocks and two A/B blocks:

- baseline A/A throughput ratio 0.9997, 95% CI [0.9953, 1.0041], and ACK-p50
  ratio 1.0008, 95% CI [0.9972, 1.0045];
- candidate A/A throughput ratio 1.0008, 95% CI [0.9964, 1.0053], and ACK-p50
  ratio 0.9996, 95% CI [0.9952, 1.0040];
- candidate/base throughput ratio 1.0344, 95% CI [1.0185, 1.0505]; and
- candidate/base ACK-p50 ratio 0.9375, 95% CI [0.9362, 0.9388].

The result represents a 3.44% geometric-mean closed-loop throughput gain and a
6.25% ACK-p50 reduction at QoS 1, callback completion and window 1. All 24 A/B
paired samples completed, and neither same-code control showed meaningful
bias. The scope is deliberately the latency-sensitive window-1 callback path;
the unchanged functional, resource and cross-platform gates cover the broader
release.

## Release decision

RC11 is recommended for merge, tagging and prerelease publication. Functional,
robustness, cross-platform, artifact and strict ARM64 hot-path gates all pass;
the measured callback change improves both window-1 throughput and ACK latency
with valid same-code controls.

This evidence-report addition changes no runtime source. The exact distributions
above were built from `2229718`; the following report-only commit therefore does
not alter the validated runtime package contents.
