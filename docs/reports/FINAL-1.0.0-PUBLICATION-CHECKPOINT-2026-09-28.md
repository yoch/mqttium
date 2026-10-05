# 1.0.0 publication checkpoint — 2026-09-28

The final release is prepared in [#594](https://github.com/yoch/mqttium/pull/594).
After reviewing the fresh results, the maintainer explicitly authorized
publication with the performance reservation on September 28. The current
performance gate does not have valid closing evidence. This is an accepted
measurement qualification limitation, not a confirmed regression or functional
defect in MQTTium, and not a passing gate result.

This report supersedes the release-qualification conclusion of the
"closing controls with the Nagle-free broker" addendum in the
[issue #493 diagnosis](LOOP-LAG-493-DIAGNOSIS-2026-09-27.md). Its observations
about the broker tail and overload remain useful diagnostic evidence. That
historical report is preserved unchanged.

## Source and distribution identity

- Reviewed main: `9a2b90686e1c416567828e538a059e6ad996038e`.
- Release metadata commit: `5d774ec30e8f4e09531d36ae08d8d1a116a75d74`.
- RC14 comparison baseline: `c194597bcf5af4951fbec2b560600eef3cb84b3c`.
- RC17 reference: tag `v1.0.0rc17`,
  `c9bab1ad93dd2e875c07706aecdeaa69eb8dce89`.

All 63 tracked files under `src/` in reviewed main are byte-identical to RC17.
In the prepared 1.0.0 tree, the only source change is the `__version__`
assignment. There is no API or SQLite schema migration from RC17. The stores
and statistics remain Provisional; their tier is not promoted by the release
number or the Production/Stable packaging classifier.

The [candidate CI](https://github.com/yoch/mqttium/actions/runs/36449627557)
passed, including its required aggregate check. The
[distribution validation](https://github.com/yoch/mqttium/actions/runs/36449627375)
built and exercised the wheel and sdist through the existing publication
workflow without publishing. The wheel's 62 Python modules match the prepared
source, and its metadata contains no mandatory runtime dependency.

| Candidate validation artifact | SHA-256 |
| --- | --- |
| Wheel | `1028b278dffaabe2235de927077f7d4689fb98a12ff564705d76c0eb3f270d42` |
| Source distribution | `56554cc6125f13afbcc797b898f1a80be01c127137a81902afeed293b134ef02` |

These are validation artifacts, not a PyPI publication record. A final
publication must validate and publish its own exact build through the existing
trusted-publishing workflow. The earlier
[long campaign evidence](RC17-LONG-CAMPAIGN-CLOSURE-2026-09-27.md) remains
applicable to the unchanged runtime. This checkpoint does not widen its scope
into a full production-application migration claim.

## Why the previous green controls do not qualify the current gate

The ARM64 workflow checks out the benchmark harness at `candidate_ref`, just
as it does the candidate runtime. Dispatching the workflow from current main
does not select the current harness if `candidate_ref` names an old release.

The closing [A/A run](https://github.com/yoch/mqttium/actions/runs/36428349624)
and [A/B run](https://github.com/yoch/mqttium/actions/runs/36432884735) selected
RC17 for `candidate_ref`. Their broker included `set_tcp_nodelay true`, but
their harness was RC17's historical implementation, before #588's confidence
screen, confirmation controls and pending-observer retention changes.

The artifacts record `passed` under that historical policy. They do not record
a pass under the current policy. In particular, these A/B completed-rate
intervals cross the current 0.97 lower threshold without confirmation:

| Protocol / payload / target | Median cycle ratio | Geometric mean | 95% interval |
| --- | ---: | ---: | --- |
| 3.1.1 / 4096 B / 24k | 1.0001 | 0.9876 | [0.8407, 1.1602] |
| 5 / 4096 B / 24k | 0.9729 | 0.9644 | [0.8630, 1.0777] |
| 5 / 4096 B / 26k | 0.9907 | 0.9237 | [0.3540, 2.4102] |

| Retained `open-loop.json` | SHA-256 |
| --- | --- |
| Historical-harness A/A, 36428349624 | `42c8627f07dc1e46a5967b08556d2b5044be1d60e5fb5c422825a2f3dd235e0e` |
| Historical-harness A/B, 36432884735 | `25c588c24baab6e364bdb0e9ad3b489e3ebce04614363f1203d06404dd36f613` |

A wide interval is not evidence that the runtime regressed. It is insufficient
evidence for the non-regression conclusion required by this gate.

## Current-harness A/A control

[Run 36449083212](https://github.com/yoch/mqttium/actions/runs/36449083212)
compared reviewed main with itself, using that same commit for the harness.
It retained both protocols, 64 B and 4096 B payloads, and all seven fixed
rates from 5k to 26k messages/s. The isolated broker used
`set_tcp_nodelay true`; the publisher was pinned to CPU 2 on the dedicated
ARM64 runner. No workstation timing was used.

The artifact records **`invalid`**, with **no regression failures**. All six
retained preflight probes were eligible. Across 512 worker samples, all
16,512,000 publications completed (completion ratio 1.0 in every worker).
The observer mode is `pending`. All eight 5k/10k protocol/payload cells are
eligible for lag and CPU diagnostics; their CPU-per-message ratios range from
0.9868 to 1.0075.

Three cells triggered confirmation. Each retained 20 A/A pairs (four initial
plus 16 confirmation pairs), and each source-tree control acquired 16 pairs.
The MQTT 5 / 4096 B / 22k suspect cleared with valid controls. The two 26k /
4096 B cells did not qualify:

| Protocol | Main comparison 95% interval | Baseline control 95% interval | Candidate control 95% interval |
| --- | --- | --- | --- |
| 3.1.1 | [0.9754, 1.0028] | [0.9902, 1.0448] | [0.9718, 1.0265] |
| 5 | [0.9680, 1.0585] | [0.9546, 1.0452] | [0.9752, 1.0968] |

Every interval in the two control columns exceeds the required [0.98, 1.02]
equivalence band. MQTT 5 also has a control geometric mean of 1.0342 and a
main comparison interval crossing 0.97. Both source labels refer to exactly
the same code, so these outcomes cannot establish a code regression.

The fresh A/A `open-loop.json` SHA-256 is
`aef5495b04f840adf984cb00cb7be3594d8604e2f360f6502b8ed450c5f327ab`.
Worker and publication totals count confirmation `ab_pairs` once, because
those already include the initial pairs.

## Current-harness RC14 comparison

The independent A/B acquisition,
[run 36449338953](https://github.com/yoch/mqttium/actions/runs/36449338953),
ran after the A/A control on the same serialized runner. Its candidate and
harness are reviewed main, with RC14 as the baseline.

Its artifact also records **`invalid`**, with **no regression failures**. All
five retained preflight probes were eligible. All 13,344,000 publications
completed across 416 workers, each with completion ratio 1.0 and `pending`
observer retention. The eight 5k/10k cells were eligible for lag and CPU
diagnostics. The other lag cells remain diagnostic under the existing rules.

The 26 cells outside 4096 B / 26k passed the initial completed-rate screen:
their median-cycle ratios range from 0.9998 to 1.0002 and their lowest 95%
lower bound is 0.9973. Both 4096 B / 26k cells required confirmation and
same-code controls, with 20 main-comparison pairs and 16 pairs per control:

| Protocol | RC17/RC14 geometric mean | RC17/RC14 95% interval | RC14 A/A 95% interval | RC17 A/A 95% interval |
| --- | ---: | --- | --- | --- |
| 3.1.1 | 0.9889 | [0.9773, 1.0006] | [0.9792, 1.0166] | [0.9695, 1.1002] |
| 5 | 0.9288 | [0.8843, 0.9756] | [0.9492, 1.0575] | [0.9414, 1.0416] |

The control intervals exceed the equivalence band in both cells. MQTT 5's
main comparison also crosses 0.97: its point estimate is lower, but its
interval and invalid controls do not establish a regression or clear the
suspect. MQTT 3.1.1 clears the A/B threshold but still has invalid controls.

The eight eligible CPU-per-message diagnostic ratios range from 0.9867 to
1.0395. The largest values are 1.0344 (3.1.1) and 1.0395 (5) at 4096 B / 10k.
These include harness bookkeeping and are not a CPU-equivalence verdict; this
campaign cannot be cited as reproducing the older sweep's maximum 1.5% CPU
difference. No CPU confidence-based qualification was performed here.

The fresh A/B `open-loop.json` SHA-256 is
`bda6cc52f893c4bd41987befc8658791d84f1e276640eab954b2a1256809432c`.
Together, the current-harness A/A and A/B campaigns retained 928 worker samples
and 29,856,000 completed publications, without qualifying the saturation cells.

## Release disposition

The diagnostic explanation for unstable, unbounded overload is consistent with
the fresh same-code result. It does not turn an invalid qualification artifact
into a passing one. The executable gate and its confirmation contract still
apply throughput controls at saturation. The later diagnostic guidance to
compare sustained rates and qualify overload with a bounded publisher has not
been implemented and qualified as a replacement release gate.

The current rules initially held publication. After the fresh A/A and A/B
outcomes were presented, the maintainer explicitly accepted this scoped
performance reservation and authorized the final release. Publication may
therefore proceed with the limitation in its release notes. The raw artifacts
and their `invalid` statuses remain unchanged; no threshold, workload or
qualification rule was weakened to obtain a pass.

This is a release-specific acceptance, not a replacement measurement policy.
Future non-regression claims require valid qualification with a reviewed
measurement method. The separate collector-cost investigation in
[#592](https://github.com/yoch/mqttium/issues/592) remains a follow-up rather
than a newly established runtime blocker. This checkpoint records preparation
and the release decision; the GitHub release and publication workflow record
the subsequent publication outcome.
