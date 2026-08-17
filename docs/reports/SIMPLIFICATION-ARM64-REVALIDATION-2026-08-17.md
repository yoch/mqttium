# Simplification audit — final ARM64 revalidation

Date: 2026-08-17

This report supersedes the performance-acceptance conclusion of
`SIMPLIFICATION-AUDIT-2026-08-16.md` for PR #252 after rebasing onto the
issue-#253 writer correction and the post-#254/#255 main tree. The older report
is intentionally retained as historical evidence rather than rewritten.

## Final verdict

**Performance acceptance: PASS.**

The comparison baseline is current main
`0999d8abfe44a568209209403d7f215b18cc4eb7`. The corrected runtime tree is
`b0bbdfb21baeb7fb15f1428cf26d25ab492f731a`; current PR HEAD
`fd750be3012e3878b12449624bce2c71747623ba` differs from it only by the
benchmark-validity commit `bench: reject unstable open-loop candidate arms`.

The final evidence is deliberately split by question instead of forcing one
noisy network harness to answer all of them:

- hosted exact-HEAD functional CI, fuzz and finalization are green;
- strict native writer capacity on the Pi 5 is **favourable** for both QoS 0
  and QoS 1;
- the exact `PUBLISH_COMPLETE -> receipt -> on_publish` path is **2.43% faster**
  in a direct paired microbenchmark with stable A/A controls;
- receipt completion is **5.84% faster** and ordered effect batching is
  **0.82% faster** in the same final run;
- the fixed-rate open-loop callback p50 cells are retained as diagnostic data
  only because their same-tree A/A controls are not reproducibly valid.

No accepted performance conclusion below depends on a failed A/A control.

## 1. Original regression and runtime correction

The rebased tree at `3ee2a3c5a78b0ea487ddbdf7927665b519fddd45`
passed functional CI (`32014996281`), long fuzz (`32014996232`) and
finalization (`32014996294`). Strict Pi-5 current-main-vs-candidate validation
(`32015071488`) nevertheless found an apparent scheduling regression: capacity
A/A and A/B passed and latency A/A passed, but at 2,500 msg/s callback p50 moved
from about 0.334 ms on current main to about 1.361 ms on #252 (~4.07x, 0/8
favourable pairs). At 10,000 msg/s p50 remained slightly favourable while
loop-lag moved into a different pacing regime.

A linear scan (`32015752809`) located the transition at rebased commit
`832f808e413dd47fd922469110316751ba205906`, `perf: remove Python frames from
the publish and acknowledgement paths`. Source and interaction isolation
(`32016075623`, `32016317775`, `32017494910`, `32018097989`, `32018272639`)
showed an interaction rather than one independently bad line:

- writer-only, inbound-only and outbound-only substitutions were neutral in
  isolation;
- either engine micro-optimization alone still left the slow 2,500/s plateau;
- restoring outbound forwarding wrappers was worse;
- reverting the writer's once-per-task `write_many` lookup was worse.

The retained correction restores two useful semantic/scheduling boundaries
while keeping the independent simplifications:

1. the engine effect-emission boundary (`_send` delegates to `_emit` and the
   `EngineEffect` construction remains explicit);
2. the outbound `_try_launch` admission/compensation boundary;
3. the writer `write_many` cache, inbound specialization and direct outbound
   effect forwarding remain retained.

That corrected runtime is the tree later committed as `b0bbdfb...`.

## 2. Strict writer capacity is favourable

ARM64 run `32021888079` compared exact main `0999d8ab...` to the corrected PR
runtime on the dedicated Raspberry Pi 5 after a strict preflight.

The writer-capacity A/A controls were valid. The A/B completed-rate ratios were:

| QoS | candidate / main |
| --- | ---: |
| QoS 0 | **1.0383x** |
| QoS 1 | **1.0351x** |

Both exceed the retained >=0.95 regression floor and are in fact about
3.5--3.8% favourable. This closes the synchronous producer/batching side of the
performance contract independently from paced latency.

## 3. Why fixed-rate open-loop p50 is not release evidence here

The original callback regression was found with `paired_open_loop.py`, so the
first attempt was to preserve that exact instrument. That became untenable only
after its same-tree controls failed reproducibility checks.

The important sequence was fail-closed:

1. The first corrected-head ARM64 final run (`32021888079`) passed writer
   capacity, but candidate A/A open-loop stability failed at 5,000 msg/s.
2. Under the pre-declared one-retry policy, fresh-preflight run `32022387003`
   made 5,000 msg/s stable but failed candidate A/A at 10,000 msg/s. The
   instability band moved between identical-tree runs, so no A/B result was
   selected from the favourable cells.
3. This exposed a harness defect: `paired_open_loop.py` validated variability
   only on the baseline arm. Commit `fd750be...` now rejects candidate-only
   completed-rate or p50 variability as well.
4. The absolute pacer was also tested with an unconditional cooperative yield
   when already late (`sleep(0)` semantics). Run `32022595894` still failed
   **main A/A itself**: callback-p50 CV was roughly 14--28% across the tested
   2.5k/5k/10k cells while completed-rate ratios remained near 1.0.

The unconditional yield is therefore not presented as a validated fix, and it
was not committed. More importantly, a fixed-rate p50 result from this harness
cannot be promoted to release evidence for #252 while identical code fails its
own p50 stability contract.

This is not a relaxed threshold chosen after seeing A/B. The A/B comparison was
explicitly skipped whenever A/A failed.

## 4. A second network-harness defect found during diagnosis

A separate closed-loop `paired_network.py` experiment appeared to run briefly
and then go idle for tens of seconds at `window > 1`. The cause was in the
benchmark, not MQTTium runtime.

Callback-mode accounting used one scalar timestamp per MQTT packet identifier.
A packet identifier may be reused after protocol ACK settlement while the older
application `on_publish(mid)` callback is still queued. With concurrent
publishes, a newer generation could overwrite the older timestamp and the
benchmark could then wait forever for a completion its own accounting had lost.

The reproducer was exact:

- `window=1`: completed normally;
- `window=8` and `window=64`: initial activity followed by no progress until a
  75 s watchdog killed the worker;
- a temporary FIFO-per-MID-generation tracker made `window=8` finish in about
  6.7 s on both main and #252 (`32024268957`).

That probe exposed the same second-order validity problem as open-loop: the
network harness recorded candidate CV but rejected only baseline CV. The #252
A/A candidate arm reached 8.19% CV and the old script still reported `passed`.

Both benchmark defects are fixed independently in PR #256 (`0cd7bc1...`): FIFO
callback generations plus symmetric arm-variability validation. Its full hosted
CI is green. PR #256 changes benchmark infrastructure only and is not a runtime
dependency of #252.

A final test using the corrected network harness (`32025380830`) again failed
closed on **main A/A**, this time at `window=8`: raw baseline throughput CV was
6.23%. The paired throughput-ratio CV was only 4.09%, illustrating that the
ABBA pairing removed much of the host drift, but the pre-existing strict arm-CV
gate correctly prevented promotion of the network result. No A/B network p50
claim is made from that run.

## 5. Direct callback-path measurement

The repository already contains a more appropriate instrument for the exact
question: `paired_regression.py` scenario `publish_complete_callback`.

The scenario creates an `AsyncClient`, registers QoS-1 receipts, emits
`PUBLISH_COMPLETE`, collects effects, drains the effect pump and joins the
callback queue in controlled batches. Each worker measures 128,000 completions.
There is no broker, fixed-rate pacer, subscriber or network timer in the
measurement. Fresh workers are pinned to the same CPU and base/candidate order
alternates between pairs.

Final ARM64 run **`32026128566`** used:

- baseline `0999d8abfe44a568209209403d7f215b18cc4eb7`;
- candidate `fd750be3012e3878b12449624bce2c71747623ba`;
- the main version of `paired_regression.py` as the common external harness;
- 20 paired repetitions per scenario on CPU 2;
- a fresh strict runner preflight: eligible, performance governor, ~59.5 C CPU,
  load 0.132/core and 3.7% sampled CPU before measurement.

The main harness was used intentionally. #252 changes `_paired_scenarios.py`
only by adding process-wide WebSocket XOR-table priming; the three scenarios
used here are otherwise unchanged. Using main's harness prevents that unrelated
module-level priming from becoming part of the parent process while every
worker still imports the runtime from the exact source root being measured.

### A/A controls

| Tree measured against itself | median ratio | paired-ratio CV | range |
| --- | ---: | ---: | ---: |
| main | **1.00006x** | **0.56%** | 0.9871--1.0087 |
| #252 | **0.99849x** | **0.57%** | 0.9860--1.0073 |

All 20/20 pairs in each A/A control stayed within +/-10%. The pre-declared A/A
gate required median 0.95--1.05, paired-ratio CV <=5%, and at least 18/20 pairs
within +/-10%; both controls passed comfortably before A/B was allowed to run.

### A/B direct-path result

| Scenario | candidate / main | paired-ratio CV | range | candidate faster |
| --- | ---: | ---: | ---: | ---: |
| `publish_complete_callback` | **1.0243x** | **0.50%** | 1.0172--1.0361 | **20/20** |
| `publish_complete_receipt` | **1.0584x** | **0.87%** | 1.0406--1.0708 | **20/20** |
| `effect_batch_ordered` | **1.0082x** | **1.09%** | 0.9826--1.0295 | 14/20 |

The release floor was candidate/main >=0.95 with paired-ratio CV <=5%. All three
cells pass by a wide margin. Most importantly, the exact callback-completion
path that had appeared ~4x slower in the unstable fixed-rate network regime is
**2.43% faster** under a direct, reproducible measurement.

## 6. Hosted exact-HEAD validation

For PR HEAD `fd750be3012e3878b12449624bce2c71747623ba`, the hosted workflows had
already passed before this final ARM64 diagnosis:

- CI: `32021762119` — success;
- long fuzz: `32021762039` — success;
- finalization / soak / interoperability: `32021762088` — success;
- publish workflow validation: `32021762023` — success.

The final documentation commit changes no runtime or benchmark logic. It still
requires fresh exact-HEAD hosted checks before merge because CI evidence is tied
to the PR HEAD, not merely to an equivalent runtime tree.

## Acceptance conclusion

The performance blocker that held PR #252 is resolved.

The evidence supports all of the following simultaneously:

- no functional regression in hosted CI/fuzz/finalization;
- native writer capacity is not merely preserved but ~3.5--3.8% favourable;
- direct publish callback completion is +2.43% with exceptionally stable A/A
  and A/B distributions;
- receipt completion is +5.84%;
- ordered effect batching is effectively neutral/slightly favourable;
- no release conclusion relies on the unstable fixed-rate p50 cells or on the
  previously deadlocking network harness.

The earlier open-loop measurements remain useful diagnostic evidence about
scheduler/timer interaction, but they are explicitly **not** release evidence
for this PR. The final release decision should therefore use the strict writer
capacity plus the direct publish-completion microbenchmark, with the hosted
exact-HEAD functional gates as the final merge barrier.
