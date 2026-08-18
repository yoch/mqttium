# Network release gate

`benchmarks/paired_network.py` remains the low-level acquisition engine. It records
closed-loop QoS 1 throughput, publisher ACK latency and independent subscriber
latency and remains useful as an advisory diagnostic on its own.

`benchmarks/network_release_gate.py` adds the release decision layer. Acquisition
and statistical policy stay separate so that instrumentation changes do not
silently redefine release criteria.

## Intended use and cost

This is a **deep, manual release/audit gate**, not routine CI. On the dedicated
Raspberry Pi 5 ARM64 runner, the protocol takes roughly 10-15 minutes: a short
same-code smoke for each tree, then the full A/B. Do not add it to push,
pull-request or normal `main` workflows.

GitHub provides `.github/workflows/arm64-network-release-gate.yml` as an explicit
`workflow_dispatch` entry point. It runs only when a maintainer deliberately
supplies a baseline and candidate ref.

For day-to-day investigation, use the cheaper advisory network sweep or focused
microbenchmarks instead.

## Required sequence

A release-grade run is fail-closed and executes these phases in order:

1. fresh dedicated-runner preflight;
2. baseline same-code A/A **smoke** (one block, three ABBA cycles);
3. fixed 30-second quiet period and fresh preflight;
4. candidate same-code A/A **smoke** (one block, three ABBA cycles);
5. fixed 30-second quiet period and fresh preflight;
6. baseline-versus-candidate A/B, only if both controls passed (two blocks,
   six seeds, twelve ABBA cycles).

Same-code controls exist to catch a drunk host or a broken acquisition chain.
They keep the same bias and equivalence numeric bands as A/B, but they do not
repeat the full 12-cycle schedule. The no-regression decision is the A/B phase.

The quiet periods are deterministic. The gate never retries preflight until the
host happens to look eligible. `runner_probe.py` uses the one-minute load average,
so a 30-second pause is a compromise: it is long enough to drop the worst of a
short control, and leftover load still fails closed at the next preflight.

A failed A/A invalidates the experiment. The A/B phase is not interpreted when
the measurement chain cannot demonstrate bounded same-code bias and adequate
precision.

## Calibrated default cell

The validated default **A/B** cell is:

- MQTT 3.1.1;
- callback completion;
- 64-byte payloads;
- windows **1/20/64**;
- two blocks;
- deterministic `PYTHONHASHSEED` values `0,1,2,3,4,5` per block;
- one complete ABBA cycle per seed and block;
- therefore **12 complete ABBA cycles / 24 paired samples per scenario**;
- approximately two seconds target duration per low-level sample;
- 30-second fixed quiet periods between A/B blocks and between phases.

Same-code A/A controls use the same cells, thresholds and sample duration, but
only **one block** and seeds `0,1,2` (three ABBA cycles). That is enough to
reject a biased or broken host without spending two-thirds of the wall clock on
same-code repetition.

On the four-core ARM64 runner, use:

- Mosquitto broker: CPU 0;
- gate, subscriber and observer: CPUs 1 and 3;
- publisher worker: CPU 2.

Launch the gate under `taskset -c 1,3`, pass `--cpu 2`, and start Mosquitto
separately under `taskset -c 0`.

### Why window 8 is not a release point

Same-code calibration repeatedly exposed a second execution regime at
`window=8`: individual publisher workers could consume materially more CPU and
fall from the normal ACK-rate regime. The effect survived CPU isolation and
longer samples.

A later full prospective same-code gate also failed its throughput-equivalence
confidence interval at window 8, while windows 1 and 64 remained precise.
`window=20` subsequently passed the complete prospective same-code protocol with
windows 1 and 64.

Window 8 remains available for advisory diagnostics. It must not be promoted
back into release evidence unless a new same-code calibration, chosen before any
A/B result is viewed, demonstrates that the second regime is no longer relevant.

## Statistical unit: complete ABBA cycles

`paired_network.py` alternates pair order:

- pair 1: base, candidate;
- pair 2: candidate, base;
- pair 3: base, candidate;
- pair 4: candidate, base;
- ...

Two adjacent opposite-order pairs form one complete ABBA cycle. The release gate
therefore does **not** treat each pair as an independent statistical observation.
For every metric it computes candidate/base per pair and collapses each adjacent
opposite-order pair into one cycle ratio:

`cycle_ratio = sqrt(pair_ratio_forward * pair_ratio_reverse)`

The confidence interval is computed on log cycle ratios with a two-sided 95%
Student-t interval. With the validated defaults, each scenario contributes 12
complete cycles.

This construction cancels first-order position/drift effects that reverse with
measurement order and keeps the estimator aligned with the experiment design.

## Same-code control gates

A same-code control must demonstrate both small systematic bias and enough
precision to protect the later no-regression margin. A CI is deliberately not
required to contain exactly `1.0`; that would be a difference test rather than
an equivalence requirement.

Publisher ACK throughput must satisfy both:

- geometric-mean candidate/base estimate inside `[0.98, 1.02]`;
- entire 95% CI inside `[0.95, 1.05]`.

Publisher ACK p50 latency must satisfy both:

- geometric-mean candidate/base estimate inside `[0.95, 1.05]`;
- entire 95% CI inside `[0.90, 1.10]`.

Both baseline and candidate source trees must pass their own same-code controls
before A/B is allowed.

## A/B no-regression gates

After both controls pass, every selected scenario must satisfy:

- throughput lower 95% confidence bound of candidate/base >= `0.95`;
- publisher ACK p50 upper 95% confidence bound of candidate/base <= `1.10`.

The decision therefore uses uncertainty around the paired estimator rather than
only a median or point ratio.

Independent subscriber delivery p50 remains recorded but diagnostic. It includes
observer/subscriber scheduling noise in addition to the client path and should
become blocking only after its own A/A precision and acceptance margin have been
validated separately.

## Raw arm CV is diagnostic here

`paired_network.py` records CV for absolute base and candidate ACK-rate samples.
Those values remain useful health telemetry, but they are not the release
decision statistic in this wrapper.

In a paired ABBA design, common-mode host or broker variation may move both
absolute arms while the within-cycle candidate/base estimator remains precise.
Conversely, a low raw-arm CV does not rescue a biased or imprecise paired
estimator. The mandatory same-code bias and confidence-interval gates are the
validity test for `network_release_gate.py`.

The wrapper therefore disables the low-level engine's legacy raw-CV and point
ratio rejection thresholds while preserving raw values in artifacts. Worker
failures, malformed output, incomplete callback accounting and failed fresh
preflights remain hard invalidations.

## Command-line example

Run Mosquitto separately on the dedicated benchmark host, then compare exact
checkouts/worktrees:

```bash
taskset -c 1,3 python benchmarks/network_release_gate.py \
  --base-root "$BASE" \
  --candidate-root "$CANDIDATE" \
  --protocols 311 \
  --completions callback \
  --payloads 64 \
  --windows 1,20,64 \
  --control-blocks 1 \
  --control-cycle-seeds 0,1,2 \
  --ab-blocks 2 \
  --cycle-seeds 0,1,2,3,4,5 \
  --target-sample-seconds 2.0 \
  --cpu 2 \
  --inter-phase-quiet-seconds 30 \
  --policy strict \
  --output /tmp/mqttium-network-release.json
```

The top-level JSON and Markdown summary record exact base/candidate SHAs, control
results, A/B results, thresholds and 95% confidence intervals. Raw phase
artifacts live beside them in `<output-stem>-raw/`.

## Retry and evidence policy

Do not rerun a statistical **failure** until it passes and report only the
favourable result. A failed no-regression bound is evidence against the
candidate.

An **invalid** run caused by an external host/provisioning or acquisition failure
may be repeated once after that cause is corrected and a fresh preflight passes.
Retain both attempts. If the repeated experiment is still invalid, fix the
environment or harness before making a release claim.

Changes to release points, sample duration, seed schedule or statistical
thresholds must be calibrated on same-code controls **before** viewing the A/B
result they will judge.

## Scope

The default gate uses a real Mosquitto broker and real TCP sockets on loopback.
It covers the complete client/broker PUBACK path and independent subscriber
observation without uncontrolled LAN jitter.

For changes whose risk specifically involves kernel/network-device behaviour,
Nagle/coalescing, physical-network jitter or remote-broker interaction, add a
separate two-host LAN experiment. Do not reinterpret a loopback result as proof
of wide-area network behaviour.
