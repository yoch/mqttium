# Release candidate report — 1.0.0rc11

Candidate source before release metadata:
`a9050562f5186dec0f9b0d081f43419dfa4794c8` (`main`, 2026-08-30).
Release metadata and hosted-candidate commit:
`c6ffabc24653778f35112cfb8f153456c20f2835`.

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

The exact `c6ffabc` PR head passed repository quality, Python 3.11–3.14,
Linux MQTT 3.1.1/MQTT 5 soaks, resilience, fuzz, package validation, macOS
3.11/3.14, Windows 3.11/3.14, Codecov, Read the Docs and the `CI required`
aggregate. The pull-request publication workflow built one wheel and one sdist,
then passed wheel smokes on Python 3.11–3.14, the Python 3.12 sdist smoke and
the installed-artifact resilience smoke. Publication jobs were correctly
skipped.

The downloaded Actions artifact passed strict Twine and wheel-content checks.
Its SHA-256 values are:

- wheel: `d98c52fb3e96beea03a524d0796c7b2fc9cc46060c0582c7450865b29b6e0675`;
- sdist: `42eaa9b6ede91f9c9972d8043622709ce1f8c37954e1101b6d14e3782d5446bd`.

## ARM64 network-gate validity

Two fresh strict ARM64 workflow attempts compared exact baseline
`1cd4dce6d3e6169323faa1c1d6a19f9d4597402a` (`v1.0.0rc10`) with exact
candidate `c6ffabc`. Both passed the dedicated-runner, CPU-governor, broker
isolation and tool preflight, but both were invalidated by the baseline
same-code A/A control before candidate controls or A/B acquisition began:

- [run 33335615137](https://github.com/yoch/mqttium/actions/runs/33335615137):
  window 1 throughput estimate 0.9076 and ACK-p50 estimate 1.1116, with wide
  equivalence intervals; window 64 was stable at 0.9983 and 1.0027;
- [run 33335763668](https://github.com/yoch/mqttium/actions/runs/33335763668):
  window 1 estimates moved in the opposite direction to 1.0372 and 0.9437,
  while confidence intervals at windows 1, 20 and 64 exceeded their strict
  equivalence bands.

Both artifacts contain zero candidate-control scenarios and zero A/B
scenarios. They therefore establish neither a regression nor equivalence for
RC11. The opposite movement between same-code attempts is environmental
instability, not product evidence. No threshold, sample duration, scenario or
policy was changed, and no third attempt was consumed.

## Release decision

RC11 is prepared as a functionally and cross-platform validated source and
artifact candidate. It is not yet recommended for tagging or publication:
because the release includes callback hot-path work, the repository's required
performance evidence must come from a future strict run whose A/A controls are
valid and whose candidate A/B verdict passes. The invalid controls above must
not be reinterpreted as either failure or success.

This evidence-report addition changes no runtime source. The release PR should
remain open until that performance condition is satisfied or the maintainer
explicitly makes and records a different release decision.
