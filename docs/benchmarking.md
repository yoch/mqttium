# Benchmarking contract

Benchmark results are build artefacts, not permanent source-code claims. Raw
outputs belong under `/tmp` or another external artefact directory and must not
be committed.

## What each benchmark answers

- `hotpath_profile.py` counts calls, primitive calls, and allocations. These
  exact measurements are the first place to look for redundant work.
- `paired_regression.py` compares isolated implementation paths in fresh
  processes and alternating order.
- `paired_network.py` records advisory closed-loop QoS 1 capacity and PUBACK
  latency. It is not a release gate because its raw-arm A/A stability is not
  reliable enough to use its legacy CV rule as a general release decision.
- `network_release_gate.py` wraps a calibrated `paired_network.py` subset in
  mandatory baseline/candidate A/A controls and confidence-bound ABBA-cycle
  evaluation. It is a deep manual release/audit gate; see
  [`network-release-gate.md`](network-release-gate.md).
- `paired_open_loop.py` measures completion and loop lag at calibrated or fixed
  absolute load. It can sweep outbound windows while calling `AsyncClient`
  directly; no cross-client adapter participates in the measurement.
- `open_loop_release_gate.py` anchors fractional load to baseline capacity and
  uses a two-stage loop-lag decision. The initial ABBA screen must exceed the
  relative threshold with relative and additive 95% lower bounds above the
  no-effect boundary before it consumes one of the bounded same-code A/A
  confirmation slots. Final rejection still requires the additive increase to
  exceed the measured same-code noise envelope.

The open-loop loop-lag rule is a targeted regression detector, not an
equivalence proof. Its initial screen uses necessary conditions from the final
pre-existing verdict: a point estimate above 1.05 whose relative or additive
95% interval still crosses the no-effect boundary remains diagnostic. It cannot
fail the release or consume scarce same-code controls by itself. Throughput,
runner eligibility, exact completion, and the deeper controlled network gate
remain separate evidence.
- `paired_writer_capacity.py` protects the native `publish_nowait` closed-loop
  writer regime for QoS 0/1. It yields once per application outstanding window
  and yields/retries on synchronous backpressure, matching the scheduling shape
  used by the external native capacity harness. Its primary metric is the
  candidate/base completed-rate ratio, not an absolute cross-machine rate.
- `paired_writer_waiter_contention.py` isolates `WritePump.enqueue()` waiters
  against a tight writer message window. It is the contention harness for the
  targeted-wake experiment; default concurrency is 1/4/16 (64/256 are opt-in).
  It does not replace `paired_writer_capacity.py`.
- `paired_protocol_responses.py` isolates the event-loop hop for inbound PUBACK,
  PUBREC and PUBCOMP. The effects come from real engine transitions,
  pass through `AsyncClient` and `WritePump`, and are measured with the normal
  producer eager throttle disarmed. An untimed segmented-write race makes every
  worker fail if a response can overtake a header or payload.
- `application_stress.py` exercises callbacks, iterators, backpressure, memory,
  and SQLite persistence.
- `memory_profile.py` enforces versioned tracemalloc and logical-counter limits.

Comparisons must exercise equivalent public completion semantics. If a library
cannot express a contract, report `N/A`; do not manufacture equivalence with an
extra barrier that changes only one side.

## Valid local evidence

`runner_probe.py` records CPU affinity, model, governor, load, temperature,
Python, and broker metadata. A strict performance run requires `--enforce`. An
ineligible host produces no release evidence, even if its ratio looks good.

Paired measurements use fresh interpreters and alternate base/candidate order in
complete ABBA cycles. Read the pair distribution as well as the aggregate.
Benchmark-specific validity rules still apply: the legacy strict harnesses that
explicitly gate raw arms retain their CV limits and neutral-control budgets.
`network_release_gate.py` is deliberately different: raw arm CV is diagnostic,
and validity comes from bounded same-code bias plus 95% equivalence intervals on
complete ABBA-cycle ratios. A benchmark that cannot satisfy its own declared A/A
control is diagnostic, regardless of whether it exposes a strict exit mode.

Hosted GitHub runners are useful for functional coverage and advisory numbers.
They are not authoritative for latency or small throughput changes.

### Closed-loop writer-capacity regression gate

The eager-write optimisation has two intentionally different regimes: paced
traffic should keep the zero-hop first write, while a synchronous producer burst
must seed once and then let the writer task batch the rest. Open-loop latency
cells cannot prove the latter. `paired_writer_capacity.py` therefore runs the
native producer on its own event loop with the same application discipline as
the external capacity harness: MQTT 3.1.1, 256-byte payloads, protocol inflight
20, application outstanding 64, and one cooperative yield per 64 successful
submissions. A `FlowControlError` yields once and retries the same unit of work.
QoS 0 counts successful native admission, then drains the writer outside the
timed interval; QoS 1 counts actual publish completion.

For a writer-regime change, first validate the harness as A/A on one source
tree, then compare the approved baseline and candidate on the same eligible
host. Record the exact baseline commit in the release manifest. The strict
sequence is:

```bash
python benchmarks/runner_probe.py \
  --output /tmp/mqttium-runner.json --enforce

# Harness control: BASELINE is a checkout/worktree of the recorded baseline.
python benchmarks/paired_writer_capacity.py \
  --base-root "$BASELINE" --candidate-root "$BASELINE" \
  --protocol 311 --qos-values 0,1 --payload-bytes 256 \
  --inflight 20 --outstanding 64 --max-queued 200 --repeat 8 \
  --policy strict --preflight-report /tmp/mqttium-runner.json \
  --output /tmp/mqttium-writer-capacity-aa.json

python benchmarks/paired_writer_capacity.py \
  --base-root "$BASELINE" --candidate-root . \
  --protocol 311 --qos-values 0,1 --payload-bytes 256 \
  --inflight 20 --outstanding 64 --max-queued 200 --repeat 8 \
  --policy strict --preflight-report /tmp/mqttium-runner.json \
  --output /tmp/mqttium-writer-capacity-ab.json
```

The A/A median completed-rate ratio must stay within 2% and baseline CV at or
below 5%. The A/B candidate must retain at least 95% of the recorded baseline
completed rate for both QoS 0 and QoS 1. This is a regression floor, not a
cross-client performance claim. The paced open-loop acceptance cells at 2,500 and 7,500
messages/s remain separate evidence and must still be retained; recovering
capacity by simply disabling eager writes would fail that side of the contract.

The GitHub `Paired Regression` workflow runs a shorter version of this cell as
**advisory** functional/diagnostic coverage. Its hosted-runner numbers are not a
substitute for the strict eligible-host A/A and A/B sequence above.

### Protocol-response eager regression gate

Run `paired_protocol_responses.py` after a writer response-path change. Its
worker generates the three inbound QoS response packet types through the
sans-io engine, then times their runtime admission in finite 256-frame reader
batches. The JSON records the immediate-versus-queued decision as well as
p50/p95/p99 latency, so the target branch is verified independently of timer
noise. Strict A/B requires every candidate response to reach an idle transport
inline, a rate gain of at least 10%, and a p50 no higher than 85% of baseline.
As with other paired gates, an eligible-host A/A control must pass first:

```bash
python benchmarks/runner_probe.py \
  --output /tmp/mqttium-runner.json --enforce

python benchmarks/paired_protocol_responses.py \
  --base-root "$BASELINE" --candidate-root "$BASELINE" \
  --repeat 8 --cpu 2 --policy strict \
  --preflight-report /tmp/mqttium-runner.json \
  --output /tmp/mqttium-protocol-response-aa.json

python benchmarks/paired_protocol_responses.py \
  --base-root "$BASELINE" --candidate-root . \
  --repeat 8 --cpu 2 --policy strict \
  --preflight-report /tmp/mqttium-runner.json \
  --output /tmp/mqttium-protocol-response-ab.json
```

This is a runtime scheduling microbenchmark, not a broker RTT claim. Retain the
QoS 1/2 network capacity and application RTT cells as end-to-end evidence.

## Keeping the harness out of the result

A benchmark can become its own bottleneck. MQTTium's network harness follows
five rules to prevent that:

1. The process running `AsyncClient` contains no subscriber reader thread.
   `mosquitto_sub` output is timestamped in a separate observer process, so
   payload parsing cannot take the publisher's GIL.
2. The observer subscribes at QoS 0. It verifies delivery and ordering without
   adding a second PUBACK stream to a benchmark whose target is publisher
   PUBACK capacity.
3. Every closed-loop cell calibrates its message count toward a configured
   target duration and records the actual duration. The target is not presented
   as a guarantee because fresh-process and broker rates can change after
   calibration.
4. Open-loop calibration runs the same subscriber, completion tracking, and
   telemetry path as the paced sample. The only difference is pacing itself.
5. Callback completion timestamps are correlated FIFO per MQTT packet
   identifier. Packet identifiers may be reused before the observer consumes an
   earlier queued callback, so one timestamp slot per MID is not sufficient.

When a CPU is selected, the publisher worker is pinned only after the subscriber
and observer have started. The observer therefore does not inherit the
publisher's single-CPU affinity.

Harness changes require an A/A control on one source tree before they can support
an A/B claim. Record throughput, CPU time, delivery latency, CV, and the A/A
ratio. If instrumentation changes the scenario materially, retain both the old
and new controls and explain why the new result is more representative.

## Latency semantics

Network latency starts immediately before the application calls `publish()`.
It includes local admission and queue residence as well as broker and transport
time. PUBACK proves broker acceptance; independent subscriber completion proves
delivery to the observer.

Receipt completion is observed by an awaiting task, so its timestamp includes
that task's scheduling delay. At high rates this can have substantially higher
CV than callback observation and must not be used as a neutral latency control
unless its own A/A cell passes. Exact call/allocation profiles are the preferred
neutral control for changes limited to callback handoff.

Larger inflight windows can improve throughput through batching while increasing
latency. Sweep the window before calling a high-window latency change a protocol
regression. Open-loop measurements are preferable when the question is latency
at a known fraction of capacity.

Use fixed absolute rates when the question names a concrete workload rather
than a fraction of the implementation's capacity. For example, an independent
MQTT 3.1.1 diagnostic at 5,000 and 10,000 messages/s can use:

```bash
python benchmarks/runner_probe.py \
  --output /tmp/mqttium-runner.json --enforce

python benchmarks/paired_open_loop.py \
  --base-root . --candidate-root . \
  --protocols 311 --payloads 64,4096 \
  --completions receipt,callback --windows 8,32,64,128 \
  --target-rates 5000,10000 --repeat 12 \
  --policy strict --preflight-report /tmp/mqttium-runner.json \
  --output /tmp/mqttium-open-loop-aa.json
```

When `--target-rates` is present, fixed-rate points run by themselves unless
`--fractions` is also supplied explicitly. With neither option, the retained
`0.50,0.75,0.90,1.00` capacity-fraction sweep remains the default. Passing the
same source root on both sides is an A/A control and enforces the neutral
completed-rate ratio within 2% in addition to the normal CV checks.

## A practical optimisation order

1. Count exact calls and allocations per operation.
2. Isolate the suspicious operation with a microbenchmark.
3. Confirm the change in paired A/B runs on an eligible, idle host.
4. Include a neutral control and verify non-targeted paths.
5. Use CI last, for reproducibility and portability rather than precise timing.

This order distinguishes demonstrated redundant work from attractive but
unmeasured allocation theories. Several retained MQTTium optimisations removed a
duplicate call or conversion. Several allocation-motivated rewrites were
reverted because the replacement operation cost more.

## Acceptance thresholds

A micro optimisation is retained when it removes demonstrated work, improves by
at least 2%, and favours the candidate in at least 8 of 11 pairs. A network
optimisation requires a reproducible gain of at least 5% at two load points.

Unless a benchmark-specific contract explicitly replaces them, the generic
acceptance checks are:

- baseline CV at most 5%;
- neutral control within 2%;
- non-targeted throughput not down by more than 3%;
- loop lag not up by more than 5%;
- memory limits and public semantics unchanged;
- added complexity justified by an explicit, measured trade-off.

`network_release_gate.py` is a no-regression release gate rather than the generic
optimisation-acceptance test above. Its calibrated same-code equivalence and A/B
confidence-bound contract, including diagnostic-only raw CV, is defined in
[`network-release-gate.md`](network-release-gate.md).

The loop-lag ratio is only readable when both arms sit in the same pacing
regime, and it silently penalises the faster one when they do not.
`loop_lag_p95` measures how late the paced publisher wakes relative to its own
deadline, so it is bimodal: while the publisher still has slack it genuinely
sleeps between messages and the value sits on a plateau set by timer wake-up
granularity (~1 ms on the reference host); once its per-iteration work exceeds
the pacing interval it stops sleeping and the value collapses by roughly 5× to
something that measures loop congestion. A candidate that changes publisher
per-iteration cost moves the rate at which that transition happens, so at rates
inside the transition band the ratio compares a plateau value against a
collapsed one and reports a large regression for what is in fact an
improvement. Baseline CV inflates in the same band, because the slower arm
flips between modes from sample to sample.

Before trusting a lag verdict, compare the two arms' **absolute**
`loop_lag_p95`: values near the plateau mean the publisher is still sleeping and
the number is a timer artifact, not congestion. Choose load points where both
arms are on the same side of the transition. A worked example, including the
sweep that identified the band, is in
[the historical native writer-hop report](https://github.com/yoch/mqttium/blob/main/docs/reports/NATIVE-WRITER-HOP-2026-08-16.md).

## Memory thresholds

`check_memory_thresholds.py` validates `memory_profile.py` immediately after the
profile. `memory_thresholds.json` contains reviewed limits, not generated
measurements. It gates tracemalloc peaks and exact logical counters; RSS remains
diagnostic because allocators and kernels retain pages differently.

Reference measurements live in
[the historical memory-results report](https://github.com/yoch/mqttium/blob/main/docs/reports/MEMORY-RESULTS.md). Raising a threshold is a
reviewable source change and requires a documented reason.
