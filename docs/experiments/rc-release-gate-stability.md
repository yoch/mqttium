# Release experiment: RC preflight and open-loop gate stability

Tracking issue: [#318](https://github.com/yoch/mqttium/issues/318).

Baseline: `main@bf0d80ea0cb0fa9be2c22ac47bf2ddff2ca07fee`.

Status: **investigation only**. This branch must not change `src/` runtime code or relax release thresholds without evidence.

## Trigger

A local RC run against `v1.0.0rc8` repeatedly failed the strict runner preflight before finally reaching performance measurement. Once open-loop ran, all 32 printed completed-rate ratios remained near 1.0 (minimum `0.9941`, maximum `1.0084`), while several relative loop-lag ratios exceeded the strict `1.05` ceiling, including values near `2x`.

The measured candidate commit and current `main` have the same Git tree, so this is a release-harness investigation rather than an analysis of stale source content.

## Questions

1. Does the `rc` profile self-pollute its own `load1/cpu` preflight by running quality/unit/integration work immediately beforehand?
2. What is the natural A/A distribution of `loop_lag_p95_ms` and its candidate/base ratio on the intended runner?
3. Are large relative loop-lag ratios occurring around negligible absolute lag, or do they correspond to material scheduler delay?
4. How much does publisher CPU pinning change the distribution?
5. Does one preflight before the whole performance campaign adequately establish runner eligibility, or is requalification needed between major gates?

## Diagnostic tooling

`benchmarks/open_loop_report.py` reads an existing `paired-open-loop.json` and reports absolute loop-lag values for every relative loop-lag failure without changing gate semantics.

Example:

```bash
python benchmarks/open_loop_report.py \
  /tmp/mqttium-release/<sha>/rc/paired-open-loop.json \
  --output /tmp/open-loop-diagnostic.json
```

Use `--all` to print every scenario. The diagnostic records:

- configured relative loop-lag threshold;
- median baseline and candidate `loop_lag_p95_ms`;
- absolute median delta;
- median and maximum per-pair absolute delta;
- raw per-pair lag values and ratios;
- original invalidations and regressions from the source report.

This tool is observational only. It does not reinterpret a failed or invalid release gate as passing evidence.

## Phase A: preserve the original failure evidence

For the triggering RC output, retain:

- `runner.json`;
- `paired-micro.json`;
- `paired-network.json`;
- `paired-open-loop.json`;
- generated markdown summaries;
- the command line and candidate/base SHAs.

Run `open_loop_report.py` over the original JSON before collecting replacement evidence. The original failure remains part of the record even if later runs pass.

## Phase B: open-loop A/A control

Use the same immutable source root for both arms so `paired_open_loop.py` marks the campaign as A/A.

Run at least two campaigns:

1. current runner configuration without publisher CPU pinning;
2. the same runner with a dedicated publisher CPU (`--cpu`) and the broker kept off that CPU where practical.

Use `repeat >= 8`. Keep the existing protocol/payload/completion/load matrix unless a smaller diagnostic subset is explicitly labeled non-release evidence.

For each campaign record:

- completed-rate ratio distribution;
- baseline and candidate completed-rate CV;
- ACK p50 latency CV;
- relative loop-lag ratio;
- absolute baseline/candidate loop-lag p95 and delta;
- runner preflight metadata;
- CPU affinity and governor.

A/A measurements that exceed the current relative loop-lag threshold are evidence about the gate's false-positive/noise behavior, not evidence that the threshold should automatically be raised.

## Phase C: preflight carryover

Characterize the runner immediately after the same quality workload used by `local_release.py rc`.

After quality finishes, sample at regular intervals until the existing strict preflight passes. Record at least:

- instantaneous CPU percentage;
- `load_1m_per_cpu`;
- `load_5m_per_cpu` if available from the raw sample;
- governor;
- temperature;
- elapsed time since quality completion.

The key diagnostic is whether instantaneous CPU is already below the strict ceiling while `load1/cpu` alone remains above `0.25` and decays predictably afterward.

Do not replace `load1` with another criterion during this phase; first demonstrate whether the current sequencing causes self-rejection.

## Phase D: eligibility lifetime

Re-run the major performance gates with an immediately preceding strict preflight and a post-campaign requalification:

1. preflight -> paired micro;
2. preflight -> paired network;
3. preflight -> paired open-loop;
4. final preflight/requalification.

If eligibility changes during the campaign, the affected performance evidence is invalid and must not be counted as a pass.

## Decision rules

### Runtime regression

Open a runtime follow-up only if a degradation survives eligible, pinned, repeated same-host A/B evidence and is material in absolute terms as well as relative terms.

### Gate calibration defect

A gate change requires A/A evidence showing that the current decision rule has a meaningful false-positive rate under otherwise eligible conditions. Prefer a rule tied to measured noise/absolute materiality over an arbitrary larger ratio ceiling.

### RC orchestration defect

A quiescence/retry/requalification change is justified if the RC's own preceding work is shown to cause strict preflight rejection after instantaneous load has returned to an otherwise acceptable state.

## Constraints

- No `src/` changes in this investigation PR.
- No release threshold relaxation in this investigation PR.
- No silent conversion of invalid measurements into passes.
- Do not discard the original failed campaign after collecting cleaner evidence.
- Keep throughput and scheduler-jitter conclusions separate.
- Any eventual release-gate change must remain fail closed.

## Exit

Close the investigation only after the evidence supports one or more explicit conclusions:

1. stable runtime regression;
2. statistically/physically unsupported gate threshold;
3. preflight orchestration defect;
4. stale eligibility across a long campaign;
5. no defect found after controlled reruns.

Any production harness change should be reviewed as a separate, narrowly justified commit or PR once the measurement outcome is known.
