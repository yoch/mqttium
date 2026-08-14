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
  latency. It is not a release gate because its A/A control exceeded the noise
  budget even on a preflight-eligible host.
- `paired_open_loop.py` measures completion and loop lag at calibrated or fixed
  absolute load. It can sweep outbound windows while calling `AsyncClient`
  directly; no cross-client adapter participates in the measurement.
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
complete ABBA cycles. Read the pair distribution as well as the median. A result
is invalid when baseline CV exceeds 5%, and a control scenario that should not
move must remain within 2%. A benchmark that cannot satisfy its A/A control is
diagnostic, regardless of whether it exposes a strict exit mode for experiments.

Hosted GitHub runners are useful for functional coverage and advisory numbers.
They are not authoritative for latency or small throughput changes.

## Keeping the harness out of the result

A benchmark can become its own bottleneck. MQTTium's network harness follows
four rules to prevent that:

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

In both cases:

- baseline CV must be at most 5%;
- the neutral control must stay within 2%;
- non-targeted throughput must not fall by more than 3%;
- loop lag must not rise by more than 5%;
- memory limits and public semantics must remain unchanged;
- added complexity requires an explicit, measured trade-off.

## Memory thresholds

`check_memory_thresholds.py` validates `memory_profile.py` immediately after the
profile. `memory_thresholds.json` contains reviewed limits, not generated
measurements. It gates tracemalloc peaks and exact logical counters; RSS remains
diagnostic because allocators and kernels retain pages differently.

Reference measurements live in
[`reports/MEMORY-RESULTS.md`](reports/MEMORY-RESULTS.md). Raising a threshold is a
reviewable source change and requires a documented reason.
