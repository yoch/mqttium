# Network release gate

`benchmarks/paired_network.py` remains the low-level acquisition engine. It records
closed-loop QoS 1 throughput, publisher ACK latency and independent subscriber
latency, and it intentionally remains useful as an advisory diagnostic on its own.

`benchmarks/network_release_gate.py` adds the release decision layer. The two jobs
are deliberately separate: acquisition should not silently change statistical
policy, and statistical policy should be testable without a broker.

## Required sequence

A release-grade run is fail-closed and always executes these phases in order:

1. fresh dedicated-runner preflight;
2. baseline same-code A/A;
3. fixed 60-second quiet period, then a fresh preflight;
4. candidate same-code A/A;
5. fixed 60-second quiet period, then a fresh preflight;
6. baseline-versus-candidate A/B, only if both controls passed.

The quiet periods are deterministic. The gate never probes repeatedly until it
happens to find an eligible instant. This matters because `runner_probe.py` uses
the one-minute load average: an immediate post-benchmark preflight measures work
performed by the benchmark phase itself.

A failed A/A invalidates the experiment. The A/B phase is not run, because a
benchmark that cannot demonstrate bounded same-code bias and enough precision
cannot support a release claim about different code.

The calibrated default network cell is MQTT 3.1.1, callback completion, 64-byte
payloads, windows **1/20/64**, 12 paired samples and a target of approximately two
seconds per sample.

On the dedicated four-core ARM64 runner, the recommended isolation is:

- Mosquitto broker: CPU 0;
- gate, subscriber and observer: CPUs 1 and 3;
- publisher worker: CPU 2.

This can be achieved by launching the gate under `taskset -c 1,3` and passing
`--cpu 2`; the broker is started separately under `taskset -c 0`.

### Why window 8 is not a release point

During pre-A/B same-code calibration, `window=8` repeatedly showed a second
execution regime: individual publisher workers sometimes consumed materially
more CPU and fell from roughly 18-19k ACK/s into the 15-17k range. The effect
survived publisher/observer CPU isolation and doubling sample duration from about
two to four seconds. Longer samples therefore did not solve it.

`window=20`, by contrast, passed the same-code first control together with
windows 1 and 64 at the original approximately two-second duration. Window 8 is
retained for advisory diagnostics; it is not silently discarded and should not
be promoted back into release evidence until its multimodality is understood or
shown to be stable under repeated same-code controls.

## Statistical unit: complete ABBA cycles

`paired_network.py` alternates pair order:

- pair 1: base, candidate;
- pair 2: candidate, base;
- pair 3: base, candidate;
- pair 4: candidate, base;
- ...

Two adjacent opposite-order pairs form one complete ABBA cycle. The release gate
therefore does **not** pretend that 12 pairs are 12 independent observations.
For each metric it first computes the candidate/base ratio for every pair, then
collapses each adjacent pair into one cycle ratio using the geometric mean:

`cycle_ratio = sqrt(pair_ratio_forward * pair_ratio_reverse)`

The confidence interval is computed on the log of those cycle ratios with a
two-sided 95% Student-t interval. With the default `--repeat 12`, the statistical
sample contains six complete ABBA cycles.

This construction cancels first-order position/drift effects that reverse with
measurement order and keeps the estimator aligned with the benchmark's actual
experimental design.

## Same-code control gates

A same-code control must demonstrate **small systematic bias** and **enough
precision to protect the release margin**. Requiring a 95% CI to contain exactly
`1.0` is deliberately not used: with enough precision, an immaterial same-code
offset can exclude exactly 1 while still ruling out every materially relevant
regression. That is a difference test, not an equivalence requirement.

Publisher ACK throughput must satisfy both:

- geometric-mean candidate/base estimate inside `[0.98, 1.02]`;
- entire 95% CI inside `[0.95, 1.05]`.

Publisher ACK p50 latency must satisfy both:

- geometric-mean candidate/base estimate inside `[0.95, 1.05]`;
- entire 95% CI inside `[0.90, 1.10]`.

The tighter point-estimate bands prevent a narrow but systematically biased
same-code result from consuming most of the later no-regression budget. The wider
confidence bands prove that uncertainty itself is small enough to rule out the
regression size the A/B gate is intended to protect.

Both the historical baseline and candidate source trees must pass their own
same-code control before A/B is allowed.

## A/B no-regression gates

After both controls pass, the candidate is accepted only when every selected
scenario satisfies:

- throughput: lower 95% confidence bound of candidate/base is at least `0.95`;
- publisher ACK p50 latency: upper 95% confidence bound of candidate/base is at
  most `1.10`.

The decision therefore uses uncertainty around the paired estimator rather than
only a median ratio.

Independent subscriber delivery p50 is retained in the output but is diagnostic
for this gate. It includes observer and subscriber scheduling noise in addition
to the client path. It should become a blocking metric only after its own A/A
precision has been demonstrated and a separate acceptance margin has been
predeclared.

## Why raw arm CV is diagnostic, not the decision statistic

The acquisition engine records CV for the absolute base and candidate ACK-rate
samples. Those values remain visible in every scenario and are useful health
telemetry.

They are not, by themselves, the release decision statistic. In a paired ABBA
design, common-mode machine or broker variation can move both absolute arms while
the within-cycle candidate/base estimator remains precise. Rejecting solely
because one raw arm crosses a 5% CV threshold throws away exactly the pairing
that the experiment was designed to exploit.

Conversely, a low raw-arm CV does not rescue a biased or imprecise paired
estimator. The mandatory same-code bias and confidence-interval gates prove that
the complete measurement chain can distinguish neutrality at the required
resolution.

`network_release_gate.py` therefore invokes `paired_network.py` with its legacy
raw-CV and point-ratio rejection thresholds disabled, while retaining those raw
values in the artifacts. Worker failures, malformed output, incomplete callback
accounting and failed fresh preflights remain hard invalidations.

## Example

Run Mosquitto separately on the dedicated benchmark host, then compare two exact
checkouts/worktrees:

```bash
taskset -c 1,3 python benchmarks/network_release_gate.py \
  --base-root "$BASE" \
  --candidate-root "$CANDIDATE" \
  --protocols 311 \
  --completions callback \
  --payloads 64 \
  --windows 1,20,64 \
  --repeat 12 \
  --target-sample-seconds 2.0 \
  --cpu 2 \
  --policy strict \
  --output /tmp/mqttium-network-release.json
```

The first preflight runs immediately before baseline A/A. The gate then waits the
fixed `--inter-phase-quiet-seconds` value (60 seconds by default) before each
subsequent fresh `runner_probe.py --require-temperature --enforce` invocation.
Each report is stored beside the raw acquisition JSON.

The top-level JSON and Markdown summary record exact base/candidate SHAs, control
results, A/B results, bias budgets and 95% confidence intervals. The raw phase
artifacts are kept in a sibling directory named `<output-stem>-raw/`.

## Retry and evidence policy

Do not rerun a statistical **failure** until it passes and then report only the
favourable result. A failed no-regression bound is evidence against the candidate.

An **invalid** run caused by an ineligible host or acquisition failure may be
repeated once after the external cause is corrected and a fresh preflight passes.
Retain both attempts. If the repeated experiment is still invalid, stop and fix
the environment or harness before making a release claim.

Changes to release points, sample duration or statistical thresholds must be
calibrated on same-code controls **before** viewing the A/B result they will judge.
The calibration history that led to windows 1/20/64 is retained in the PR
conversation and ARM64 artifacts; no pre-#252 versus post-#252 A/B was run while
those choices were being made.

## Scope

The default gate uses a real Mosquitto broker and real TCP sockets on loopback.
It covers the complete client/broker PUBACK path and independent subscriber
observation without introducing uncontrolled LAN jitter.

For changes whose risk specifically involves kernel/network-device behaviour,
Nagle/coalescing, physical-network jitter or remote-broker interaction, add a
separate two-host LAN experiment. Do not silently reinterpret a loopback result
as proof of wide-area network behaviour.
