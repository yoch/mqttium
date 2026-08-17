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
3. fresh preflight;
4. candidate same-code A/A;
5. fresh preflight;
6. baseline-versus-candidate A/B, only if both controls passed.

A failed A/A invalidates the experiment. The A/B phase is not run, because a
benchmark that cannot prove neutrality on identical code cannot support a release
claim about different code.

The default network cell is MQTT 3.1.1, callback completion, 64-byte payloads,
windows 1/8/64, 12 paired samples and a target of approximately two seconds per
sample. The publisher may be pinned to a dedicated CPU; the broker and subscriber
must not inherit that affinity.

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

A control must demonstrate both neutrality and enough precision to protect the
claimed regression floor.

For publisher ACK throughput, the 95% confidence interval must:

- contain `1.0`;
- lie entirely inside `[0.95, 1.05]`.

For publisher ACK p50 latency, the 95% confidence interval must:

- contain `1.0`;
- lie entirely inside `[0.90, 1.10]`.

These are equivalence checks, not requests for a favourable point estimate. A
same-code run whose CI is narrow but systematically excludes `1.0` is invalid,
as is a same-code run whose CI contains `1.0` but is too wide to exclude the
regression size the A/B gate claims to protect.

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
estimator. The mandatory same-code confidence intervals are the gate that proves
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
python benchmarks/network_release_gate.py \
  --base-root "$BASE" \
  --candidate-root "$CANDIDATE" \
  --protocols 311 \
  --completions callback \
  --payloads 64 \
  --windows 1,8,64 \
  --repeat 12 \
  --target-sample-seconds 2.0 \
  --cpu 2 \
  --policy strict \
  --output /tmp/mqttium-network-release.json
```

The gate runs `benchmarks/runner_probe.py --require-temperature --enforce`
immediately before each of the three measurement phases and stores each preflight
beside the raw acquisition JSON.

The top-level JSON and Markdown summary record exact base/candidate SHAs, control
results, A/B results and 95% confidence intervals. The raw phase artifacts are
kept in a sibling directory named `<output-stem>-raw/`.

## Retry and evidence policy

Do not rerun a statistical **failure** until it passes and then report only the
favourable result. A failed no-regression bound is evidence against the candidate.

An **invalid** run caused by an ineligible host or acquisition failure may be
repeated once after the external cause is corrected and a fresh preflight passes.
Retain both attempts. If the repeated experiment is still invalid, stop and fix
the environment or harness before making a release claim.

## Scope

The default gate uses a real Mosquitto broker and real TCP sockets on loopback.
It covers the complete client/broker PUBACK path and independent subscriber
observation without introducing uncontrolled LAN jitter.

For changes whose risk specifically involves kernel/network-device behaviour,
Nagle/coalescing, physical-network jitter or remote-broker interaction, add a
separate two-host LAN experiment. Do not silently reinterpret a loopback result
as proof of wide-area network behaviour.
