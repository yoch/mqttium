# Lean native API and fairness follow-up — 2026-09-13

The corrected candidate passes 1,979 local tests and removes 3,347 Python source lines (19.5%) relative to original main. The complete 96-cell comparison has a descriptive delivered-rate change of **-9.1%**, CPU/message **+9.4%**, and traced Python allocation peak **-17.4%**. These aggregates do not describe every workload; the complete per-cell results and residual regressions are below.

This is the second follow-up to the incompatible native experiment in
[PR #457](https://github.com/yoch/mqttium/pull/457). It corrects three API
contracts, extends the existing ready QoS 0 writer admission to progressive
batches, bounds callback-worker monopolization, and removes unused private
paths. Local gains and residual regressions against original main are separate
comparisons. This report does not authorize a merge or release.

## Source, recovery and scope

- Original main: `9ad1f01857306ac5079ffb1d073a59fdb60e1931`.
- Previous follow-up reference: `6c76a4821aabb32a7dc8077efab6289435041b0e`.
- Measured final candidate: `515fd3cd2593371b4388ab03cb1fe559c4c8870b`.
- Final `src/mqttium` tree: `e6a3f409542e565c733b48834a18f238523e8c9d`.
- The complete local functional qualification ran at `817d99a`; `515fd3c`
  changes only the diagnostic benchmark. Their runtime and test trees agree.
- Branch: `codex/lean-native-experiment`. Original main, release tags, version,
  runtime dependency policy and the primary checkout's existing audit remain
  unchanged. Later report commits must identify any difference from this source.

The September 10 power interruption cleared `/tmp`, including the earlier raw
samples and local evidence archives. Git implementation commits and hosted CI
survived. The new campaign uses persistent storage, complete-block checkpoints,
source/argument fingerprints and atomic completion records. A restart reuses
only verified complete blocks. Incomplete attempts remain separate and are not
pooled into summaries. Previously displayed measurements or rounded peer-review
tables are not reconstructed as raw samples.

The peer dossier supplied again on September 13 reviews `6c76a48`, not this
candidate. Its relevant findings are the starting point for these changes; it
is not an independent validation of the new source. Historical reports remain
unchanged and are superseded in the report index.

Retained experimental contracts include explicit iterator/callback delivery,
permanently frozen routes, immutable properties/configuration, progressive
committed-prefix receipts and schema-5-only SQLite. This work does not add a
second callback scheduler, restore compatibility surfaces, group individual
SEND effects, merge receipt registries or remove `on_publish`. The settlement
barrier before packet-identifier reuse remains, including inside a batch.

## API corrections

### Manual acknowledgement belongs to an active logical exchange

A delivered manual-acknowledgement handle carries private identity for its
active inbound exchange. Equality of visible fields and reuse of a packet
identifier do not grant acknowledgement ownership. Stale, foreign and
reconstructed handles raise `ProtocolError` before durable state or wire output
changes. Repeated use is allowed while the same acknowledgement remains pending.

A transport reconnect, including an explicit reconnect, preserves identity when
the broker resumes the same session. Session replacement and successful
completion invalidate it. Failed durable transitions preserve the prior owner.
Restart replay binds fresh delivered handles to the recovered exchange; no
SQLite schema change or serialized token is needed.

Automatic-acknowledgement mode allocates no token or identity dictionary. The
slotted `Message` still gains one field: shallow object size on this CPython
3.12 runtime rises from 88 to 96 bytes. This is not a claim about total retained
application memory. The field is excluded from construction, repr and equality.

### Iterator lifetime is captured at call time

`messages()` captures the delivery generation when called and returns the
internal asynchronous iterator. Even an iterator never advanced before an
explicit disconnect/reconnect belongs to its original generation. Creating it
does not make it a handle to some future connection. The ordinary async-for
interface is preserved.

### Broker disconnect information remains available

`ReconnectPolicy.follow_server_reference` is removed. MQTT 5 reasons `0x9C`
and `0x9D` are terminal; an application explicitly chooses any replacement
endpoint. The existing `on_disconnect(error)` interface receives
`BrokerDisconnectError` for nonzero broker DISCONNECT information, including
its reason code and immutable received properties.

This replaces only a generic connection-closure cause. More precise transport,
local, protocol or connection-negotiation errors remain intact. No callback
signature is changed. The [migration guide](../migration.md) and
[API contract](../api-stability.md) describe the incompatible experimental API.

## Batch admission, fairness and private cleanup

The ready QoS 0 path is shared by unit and aggregate publication. Aggregate
ownership is registered before the existing writer can expose bytes, without a
disposable receipt per element. Only an unambiguous `False` writer refusal
rolls back tentative aggregate registration and falls back to ordinary
admission. A writer exception may follow a partial write: the committed prefix
remains owned and is never retried. Validation, aliases, queue/byte bounds,
segmented wire order and progressive input consumption remain enforced.

The serial callback worker retains its initial suspension and yields after
64 completed jobs if queued work remains. It releases completed-job credits
before yielding. A deterministic test queues a heartbeat behind a gated first
callback and verifies the next turn after the remaining 63 jobs, with normal
and eager task factories. Error, cancellation, boundary and reconnect cases
protect the same accounting. This is a fairness bound in jobs, not a wall-time
latency guarantee; slow individual user callbacks can still take arbitrary time.

Exact-only topic matching avoids building a candidate list. Dispatch avoids
an outer tuple around the already selected matches. Their separate measured
retention decisions appear below. Registration order, multi-match dispatch,
fallback, asynchronous handlers and error isolation remain tested.

Unused `EffectPump.record_inline_batch`, `WritePump.try_enqueue_many`,
`refusal_many` and borrowed QoS 0 decoder paths are removed. Active owned-byte
specialized decoding and the existing bounded writer remain. Cleanup is not
presented as a speed claim.

| Structural measure | Original main | Current candidate | Difference |
| --- | ---: | ---: | ---: |
| Tracked Python source files | 64 | 58 | −6 |
| Physical Python source lines | 17,186 | 13,839 | −3,347 (−19.5%) |
| `AsyncClient` method definitions | 119 | 75 | −44 |
| Private `AsyncClient` method definitions | 93 | 53 | −40 |

These counts include comments, blank lines and property accessors. They indicate
structural reduction, not a measured maintenance-cost reduction.

## Functional and CI qualification

| Check | Result |
| --- | --- |
| Unit, project, mandatory Mosquitto integration, resilience and all fuzz tests | 1,979 passed; no skips; 219.25 s |
| Line and branch coverage, combined selection | 93.07%; required threshold remains 89% |
| Deterministic codec, engine and WebSocket fuzz; seeds 1/2/3 | 180,000 cases; zero crashes or invariant violations |
| Ruff formatting and lint (`src tests benchmarks tools`) | Passed; 304 files |
| mypy | Passed; 58 source files |
| Bandit | Passed |
| Strict MkDocs build | Passed |
| Git integrity after recovery | Full check passed |

The local CPython 3.12.13 test run uses warnings as errors, strict markers and
configuration, a 30-second per-test timeout and `MQTTIUM_REQUIRE_BROKER=1`.
Regressions cover stale/reused packet identifiers, resumed/replaced sessions,
SQLite recovery and failed completion, unstarted iterators, disconnect-reason
precedence, callback quantum boundaries, tight mixed-QoS batch bounds, eager
scheduling, ambiguous writes, and owned decoder bytes.

The initial diagnostic smoke after recovery exposed a harness defect: the
shared scripted broker drained at most 100 packets after a writer batch. The
publication benchmark now drains every complete frame with a local transport
subclass. The exact publication-count assertion remains; the shared unit-test
transport and client runtime are unchanged. Both old and current sources pass
publication, receive-iterator, receive-callback and callback-only checks at
1,024 and 4,096 operations, including separate profiling phases. The failed
smoke and passing checks are retained. A complete smoke restart preserves all
completed-result hashes; an independent audit checks sample order, arithmetic
and every retained paced clock.

Hosted qualification at `817d99a` has **23 successful checks and five
workflow-policy skips**, with no failed check:

- [CI 34492053025](https://github.com/yoch/mqttium/actions/runs/34492053025):
  Python 3.11–3.14, Windows/macOS, quality, resilience, fuzz, package, coverage
  and the required aggregate check pass.
- [Soak 34492053013](https://github.com/yoch/mqttium/actions/runs/34492053013):
  both Linux protocol jobs pass; scheduled/macOS/interoperability jobs are
  skipped for this event.
- [Distribution validation 34492053067](https://github.com/yoch/mqttium/actions/runs/34492053067):
  the single build and installed wheel/sdist smoke jobs pass. Publication and
  PyPI verification are skipped; no release took place.

The raw check-run record and all three downloaded log archives are retained.
`515fd3c` has identical runtime and test trees, but changes the diagnostic
harness. At this report's preparation, GitHub has no new CI run registered for
that commit and reports the PR as conflicting with the now-advanced remote
`main` (`c194597bcf5af4951fbec2b560600eef3cb84b3c`). This is explicitly
**qualification of the recorded runtime source at `817d99a`**, not a claim of
a green latest-head check or a mergeable PR. The prescribed original-main
comparison remains pinned to `9ad1f018`; upstream reconciliation is outside
this exact-source experimental evidence.


## Measurement method and limits

Every qualified cell contains two same-source A/A ABBA cycles and three A/B
ABBA cycles, using fresh client processes. The two inner B positions load A's
source during A/A. Each cycle compares geometric means of its two B and two A
observations; the cell ratio is the geometric mean of complete cycle ratios.
Cross-cell aggregates weight each cell equally and are descriptive. A/A shows
variation in the same source; it is not subtracted from A/B. All complete
samples are retained, with no result-dependent retries.

Network comparisons use self-subscribed combined publication and reception,
256-byte payloads, flow window 20 and identical queue/byte limits. Callback
cells use `on_message` without topic-specific route registration; routed
dispatch is measured separately. A private
Mosquitto listener on loopback port 11884 uses `set_tcp_nodelay true` for every
stage and both arms. Client processes are pinned to CPU 4. The timing target is
0.5 seconds, with 128–8,192 messages (at least 2,048 for long lots); actual
finite durations and any count ceiling remain in the raw records. Warmup is
32 messages. Completion verifies ordered payloads, receipts and the final
inbound QoS 2 handshake.

Timing excludes tracing. A separate phase with at most 2,048 messages measures
peak traced Python allocation; it excludes native SQLite allocations, kernel
buffers and pre-existing objects. CPU belongs to the client process, not the
broker. Network latency runs from producer element construction to application
delivery, including local admission and queue residence. Waiting before element
construction is included in overall elapsed throughput time, but not in that
latency metric. Long lots exercise each revision’s `publish_many()`, including
the old chunk behavior versus the new progressive behavior; bursts 1, 2 and 8 use individual publications and a delivery
barrier per burst. These are not independent-client or pure-receive benchmarks.

The host is an Intel Core i7-3770 (nominal 3.40 GHz), eight logical CPUs, Linux
6.8.0-138-lowlatency, CPython 3.12.13 and Mosquitto 2.0.18. The user restored
all CPU governors to `performance` after reboot. Each measured stage starts
only after three consecutive eligible one-second samples: one-minute load per
CPU at most 0.25, instantaneous CPU use at most 20%, recorded temperature at
most 80 °C, and the required governor. The failed earlier `schedutil` preflight
is retained and produced no qualified benchmark cells.

This is preflight eligibility, **not exclusive CPU reservation or continuous
idle-host certification**. Client CPU 4 and pacer CPU 5 belong to different
physical cores (SMT sibling sets 0/4 and 1/5); the broker is not pinned. No
heavy validation runs concurrently with measurements. Ending probes are also
retained: several exceed the one-minute load threshold. That historical load
includes the workload just measured and cannot alone identify unrelated load.
It does not justify silently discarding or selecting trials. The A/A ranges,
including excursions exceeding 20% in a cleanup control, remain part of the
evidence and limit claims about small performance differences.


## Isolated stages against their immediate predecessor

| Stage | Baseline → candidate | Cells |
| --- | --- | ---: |
| Three API corrections and accounting fixture | `6c76a48` → `4c54962` | 16 |
| Shared aggregate QoS 0 writer admission | `4c54962` → `41738a6` | 8 |
| Callback worker quantum 64 | `41738a6` → `54fd201` | 8 |
| Exact matcher allocation | `54fd201` → `bad92ad` | 5 routing scenarios |
| Dispatcher tuple allocation | `bad92ad` → `e5b2ba0` | 5 routing scenarios |
| Unused private paths | `e5b2ba0` → `c2d57c6` | 8 |

The functional comparison covers both protocols, QoS 0/1, iterator/callback,
memory and unit/long publication. Batch, quantum and cleanup use MQTT 5 with
the same other dimensions. Routing is a separate packet-aware in-memory
microbenchmark, not a network throughput claim.

All rows use memory; `311` denotes MQTT 3.1.1 and `5` denotes MQTT 5. Labels are protocol/QoS/mode/burst (`I` iterator, `C` callback). Rate, CPU, p95 and peak are B/A ratios.

| Stage | Cell | Rate | CPU/msg | p95 | Python peak | AA rate cycle change | AA A CV |
| --- | --- | --- | --- | --- | --- | --- | --- |
| functional | 311/0/I/1 | 0.941 | 1.039 | 1.042 | 1.016 | -3.7% to +10.1% | 7.5% |
| functional | 311/0/I/long | 0.993 | 1.007 | 1.118 | 1.003 | -5.8% to +6.9% | 5.1% |
| functional | 311/0/C/1 | 1.013 | 0.990 | 0.957 | 1.001 | -1.3% to +0.8% | 4.3% |
| functional | 311/0/C/long | 1.025 | 0.975 | 1.024 | 1.003 | -5.3% to +0.6% | 3.1% |
| functional | 311/1/I/1 | 1.007 | 1.003 | 0.996 | 1.002 | -1.4% to +5.8% | 3.0% |
| functional | 311/1/I/long | 0.973 | 1.028 | 1.040 | 0.982 | -4.2% to +2.4% | 4.0% |
| functional | 311/1/C/1 | 1.018 | 0.987 | 1.007 | 1.000 | -3.7% to +1.7% | 2.6% |
| functional | 311/1/C/long | 0.991 | 1.009 | 1.004 | 0.991 | -3.2% to -0.7% | 3.5% |
| functional | 5/0/I/1 | 0.982 | 1.017 | 1.042 | 1.001 | -0.0% to +3.5% | 5.1% |
| functional | 5/0/I/long | 0.948 | 1.055 | 0.991 | 1.003 | -1.7% to +0.5% | 3.2% |
| functional | 5/0/C/1 | 0.968 | 1.027 | 1.078 | 1.001 | -4.6% to +2.1% | 2.4% |
| functional | 5/0/C/long | 0.964 | 1.023 | 1.153 | 1.003 | -2.8% to -2.5% | 0.6% |
| functional | 5/1/I/1 | 0.980 | 1.020 | 1.065 | 1.002 | -3.5% to -1.7% | 1.8% |
| functional | 5/1/I/long | 0.929 | 1.076 | 1.144 | 1.013 | -1.8% to +1.5% | 3.3% |
| functional | 5/1/C/1 | 0.989 | 1.011 | 1.029 | 1.002 | -2.0% to -0.0% | 1.6% |
| functional | 5/1/C/long | 0.951 | 1.052 | 1.091 | 1.001 | +1.4% to +3.1% | 2.0% |
| batch | 5/0/I/1 | 0.965 | 1.038 | 1.073 | 1.000 | -9.4% to -1.4% | 2.6% |
| batch | 5/0/I/long | 1.254 | 0.798 | 0.741 | 1.000 | -6.5% to +3.7% | 2.8% |
| batch | 5/0/C/1 | 0.969 | 1.024 | 1.025 | 1.000 | -0.9% to +0.3% | 3.0% |
| batch | 5/0/C/long | 1.219 | 0.820 | 0.776 | 1.000 | -0.8% to +4.8% | 0.7% |
| batch | 5/1/I/1 | 1.010 | 0.996 | 0.996 | 1.001 | -1.7% to +0.6% | 1.6% |
| batch | 5/1/I/long | 1.030 | 0.971 | 0.921 | 0.983 | -3.2% to -0.8% | 2.3% |
| batch | 5/1/C/1 | 1.042 | 0.966 | 0.985 | 1.000 | -1.3% to +1.5% | 3.0% |
| batch | 5/1/C/long | 0.981 | 1.021 | 1.003 | 1.002 | +0.6% to +2.5% | 2.1% |
| quantum | 5/0/I/1 | 0.989 | 1.015 | 1.046 | 1.000 | -1.5% to +12.1% | 10.0% |
| quantum | 5/0/I/long | 1.039 | 0.963 | 0.907 | 1.000 | -2.0% to +1.1% | 1.0% |
| quantum | 5/0/C/1 | 1.000 | 0.995 | 1.037 | 1.000 | -5.6% to -1.8% | 2.0% |
| quantum | 5/0/C/long | 0.952 | 1.050 | 1.309 | 1.571 | -3.3% to -2.3% | 2.9% |
| quantum | 5/1/I/1 | 1.009 | 0.991 | 0.971 | 1.000 | -2.9% to +1.2% | 0.4% |
| quantum | 5/1/I/long | 1.040 | 0.962 | 0.899 | 0.998 | -6.3% to +7.1% | 11.1% |
| quantum | 5/1/C/1 | 1.040 | 0.970 | 0.992 | 1.001 | -0.9% to +0.2% | 1.1% |
| quantum | 5/1/C/long | 0.998 | 1.002 | 1.015 | 1.006 | -1.9% to -0.4% | 2.1% |
| cleanup | 5/0/I/1 | 1.009 | 0.996 | 0.960 | 1.000 | -4.0% to -1.0% | 3.5% |
| cleanup | 5/0/I/long | 0.985 | 1.015 | 0.927 | 1.000 | -11.6% to +0.1% | 1.1% |
| cleanup | 5/0/C/1 | 0.989 | 1.010 | 1.010 | 1.000 | -1.9% to -0.3% | 1.8% |
| cleanup | 5/0/C/long | 1.007 | 0.993 | 0.995 | 1.001 | -1.7% to +3.7% | 3.1% |
| cleanup | 5/1/I/1 | 0.996 | 1.000 | 0.973 | 0.998 | -0.8% to +4.2% | 1.8% |
| cleanup | 5/1/I/long | 1.002 | 0.998 | 1.021 | 0.997 | +2.4% to +22.0% | 14.5% |
| cleanup | 5/1/C/1 | 1.019 | 0.980 | 0.937 | 1.003 | -4.1% to -1.7% | 1.6% |
| cleanup | 5/1/C/long | 1.014 | 0.986 | 0.980 | 1.004 | +0.2% to +2.7% | 3.6% |

The three API corrections change delivered rate by −7.1% to +2.6% across the
16 cells. These corrections are retained for contract correctness, not as
optimizations. The MQTT 3.1.1 unit-QoS-0 iterator aggregate (−5.9%) includes
one unusually slow B/A cycle (−19.2%) between two slightly positive cycles;
the same-source controls also vary substantially. It should not be read as a
stable six-percent cost. Conversely, the MQTT 5 long-lot iterator costs are
larger than their observed A/A ranges (QoS 0 −5.2%, QoS 1 −7.1%). They are not
hidden by a global average or dismissed merely because some other cells are
noisy. The extra private `Message` slot and changes on the ingress path are
part of this combined functional comparison; these measurements do not isolate
a causal cost for each edit.

The shared batch path raises long-lot QoS 0 delivered rate by **25.4% for
iterator delivery and 21.9% for callbacks**, with CPU/message ratios 0.798 and
0.820. Every A/B cycle improves by 16–28%, exceeding the observed same-source
cycle ranges. Long-lot p95 ratios are 0.741 and 0.776; peak traced allocation is
essentially unchanged. This is a repeatable local batch benefit, not proof of
lower peak retained memory. Unit QoS 0 delivered rates decline by 3.5% and
3.1%; the iterator's A/A control varies more than that, while the callback
aggregate includes one −7.0% cycle and two near −1%. Those unit-path results
remain visible. The aggregate path is retained for its clear target-workload
gain and tested admission/ownership guarantees.

The 64-job quantum has a **−4.8%** long-lot QoS 0 callback rate change, with
all three A/B cycles negative (−3.4% to −5.7%). Its A/A cycles also decline
by 2.3–3.3%; that variation is shown rather than subtracted. Long-lot QoS 1
callback throughput is nearly unchanged (−0.2%). The quantum remains for its
deterministically tested fairness, not as a throughput optimization. Iterator
cells also move by up to about +4%, although message callbacks are absent;
this is a useful warning against attributing every cell's variation to the
worker edit. No public time-budget guarantee follows from a job-count quantum.

Removing unused private paths changes delivered rates by −1.5% to +1.9% in
this matrix. Same-source controls include excursions as large as −11.6% and
+22.0%. There is no defensible speed claim here; the cleanup is retained for
removing uncalled paths while the functional suite protects active behavior.


| Change | Scenario | Rate B/A | AB cycle change | AA cycle change | CPU/msg B/A |
| --- | --- | --- | --- | --- | --- |
| route-exact | route_exact | 1.117 | +8.3% to +16.2% | -4.9% to -1.5% | 0.895 |
| route-exact | route_async | 1.119 | +8.8% to +14.8% | -6.4% to -4.2% | 0.893 |
| route-exact | route_overlap | 0.990 | -2.5% to +0.8% | -1.3% to +1.0% | 1.010 |
| route-exact | route_fallback | 1.019 | -1.7% to +3.7% | +0.2% to +0.6% | 0.982 |
| route-exact | route_error | 0.953 | -9.7% to -1.5% | +0.8% to +1.2% | 1.050 |
| route-dispatch | route_exact | 1.107 | +6.2% to +13.9% | -3.3% to +2.2% | 0.903 |
| route-dispatch | route_async | 1.136 | +10.4% to +15.3% | +2.9% to +2.9% | 0.881 |
| route-dispatch | route_overlap | 1.041 | +2.1% to +8.2% | -0.4% to +1.3% | 0.960 |
| route-dispatch | route_fallback | 1.147 | +10.8% to +19.4% | -0.7% to +0.6% | 0.872 |
| route-dispatch | route_error | 1.044 | +0.3% to +7.4% | -1.2% to +2.8% | 0.962 |

Both changes are retained. Exact-only matching improves the synchronous exact
case by **11.7%** and the asynchronous exact case by **11.9%**. In each case,
every A/B cycle's gain exceeds the largest absolute deviation in its observed
A/A cycles. That supports a real target-path improvement in this diagnostic;
it is not a statistical confidence interval or a network-capacity claim.
The overlap result is −1.0%, fallback +1.9%, and the error-isolation path
**−4.7%**, including one −9.7% cycle. The error-path cost is larger than its
same-source controls and is explicitly accepted alongside the target gain.
This is not a uniform routing win.

Removing the dispatch tuple adds **10.7%** on synchronous exact routes and
**13.6%** on asynchronous exact routes, again with every target A/B cycle
outside the largest observed absolute A/A deviation. Overlap is +4.1%,
fallback +14.7%, and the error path +4.4%; the error case includes a cycle
near parity and is not the basis for retention. Both primary target benefits
are distinguishable from the observed controls, so neither optimization is
removed under the agreed noise-based retention rule. Stage ratios are not
multiplied to manufacture a separately unmeasured combined routing result.


## Complete 96-cell comparison with original main

The final matrix covers MQTT 3.1.1/5 × QoS 0/1/2 × memory/SQLite ×
iterator/callback × bursts 1/2/8/long. Ratios are final candidate divided by
original main. Higher delivered rate is better; lower CPU/message, latency and
Python allocation peak are better.

| QoS | Mode | Bursts | Cells | Rate | CPU/msg | p95 | Python peak |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | iterator | 1/2/8 | 12 | 0.992 | 1.007 | 1.023 | 1.051 |
| 0 | iterator | long | 4 | 0.700 | 1.428 | 0.827 | 0.904 |
| 0 | callback | 1/2/8 | 12 | 0.859 | 1.161 | 1.192 | 0.970 |
| 0 | callback | long | 4 | 0.502 | 1.991 | 1.612 | 1.381 |
| 1 | iterator | 1/2/8 | 12 | 0.992 | 1.008 | 1.004 | 1.037 |
| 1 | iterator | long | 4 | 0.790 | 1.229 | 0.053 | 0.135 |
| 1 | callback | 1/2/8 | 12 | 0.959 | 1.028 | 1.107 | 1.081 |
| 1 | callback | long | 4 | 0.820 | 1.231 | 0.051 | 0.139 |
| 2 | iterator | 1/2/8 | 12 | 1.005 | 0.995 | 0.988 | 1.145 |
| 2 | iterator | long | 4 | 0.903 | 1.092 | 0.055 | 0.303 |
| 2 | callback | 1/2/8 | 12 | 1.000 | 0.991 | 1.005 | 1.251 |
| 2 | callback | long | 4 | 0.909 | 1.088 | 0.057 | 0.331 |

The 96-cell descriptive aggregate is **−9.1% delivered rate, +9.4%
CPU/message and −17.4% peak traced Python allocation**. It does not make the
simplified architecture a general performance improvement. QoS 0 callbacks
are **14.1% slower across the 12 small-burst cells** (both protocols, both
stores, bursts 1/2/8), and **49.8% slower across the four long-lot cells**.
Those four individual long-lot declines range from 47.4% to 52.6%.
Long-lot QoS 0 iterator throughput is 30.0% lower; its small-burst aggregate
is nearly unchanged (−0.8%).

Long-lot QoS 1 rates decline by 21.0% for iterator delivery and 18.0% for
callbacks; QoS 2 declines by 9.7% and 9.1%. Small-burst QoS 2 throughput is
near parity. These are distinct workload segments, not weighted predictions
of a particular application's traffic mix. None of the local optimization
ratios is substituted for the final original-main comparison.

Memory and latency also have opposing movements. Long-lot QoS 0 callbacks
use roughly twice the client CPU per message, **38.1% more peak traced Python
allocation**, and have a p95 ratio of **1.612**. For MQTT 5/memory specifically,
per-run median p95 rises from 75.11 to 129.81 ms. A lower all-cell allocation
aggregate must not hide that regression or the higher small-burst QoS 2 peaks.

Conversely, the progressive QoS 1/2 long-lot path has much smaller traced
peaks and construction-to-delivery p95s. For example, MQTT 5/SQLite/QoS 1
iterator p95 falls from 67.10 to 3.60 ms while delivered throughput declines
by 25.5%. The old chunk and new progressive producer do not create all
message timestamps at the same stage of backlog accumulation. These figures
measure their specified API behavior; they are not evidence of a universal
95% improvement in externally scheduled end-to-end latency. The fixed offered
schedule below retains waiting before the publication call and is interpreted
separately. Pooled or cross-cell p95 ratios are not a workload-wide percentile.


| Protocol/QoS/store/mode/burst | Rate A/s | Rate B/s | Rate B/A | CPU B/A | p95 B/A | Peak B/A | AA rate cycle change |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 311/0/memory/I/1 | 7629 | 7803 | 1.023 | 0.975 | 1.009 | 1.057 | -5.5% to -0.2% |
| 311/0/memory/I/2 | 9217 | 9140 | 0.978 | 1.010 | 1.108 | 1.054 | -3.6% to -2.0% |
| 311/0/memory/I/8 | 15942 | 16330 | 1.026 | 0.976 | 0.967 | 1.049 | -0.1% to +0.6% |
| 311/0/memory/I/long | 51991 | 36877 | 0.712 | 1.404 | 0.909 | 0.879 | -6.3% to -3.5% |
| 311/0/memory/C/1 | 9373 | 7553 | 0.805 | 1.226 | 1.275 | 1.050 | -0.3% to +4.3% |
| 311/0/memory/C/2 | 10718 | 9259 | 0.874 | 1.144 | 1.178 | 0.947 | -4.4% to +2.5% |
| 311/0/memory/C/8 | 17990 | 16258 | 0.904 | 1.107 | 1.170 | 0.903 | -3.0% to -0.9% |
| 311/0/memory/C/long | 61118 | 31904 | 0.520 | 1.918 | 1.578 | 1.278 | +5.9% to +7.6% |
| 311/0/sqlite/I/1 | 7269 | 7308 | 1.003 | 0.994 | 0.992 | 1.072 | -0.5% to +1.7% |
| 311/0/sqlite/I/2 | 9013 | 9025 | 0.996 | 1.004 | 1.041 | 1.052 | -1.1% to +1.4% |
| 311/0/sqlite/I/8 | 15997 | 15661 | 0.970 | 1.031 | 1.007 | 1.055 | -3.0% to -1.5% |
| 311/0/sqlite/I/long | 49453 | 35085 | 0.720 | 1.389 | 0.821 | 0.879 | -1.2% to +2.3% |
| 311/0/sqlite/C/1 | 9062 | 7318 | 0.823 | 1.200 | 1.178 | 1.066 | -5.4% to +2.8% |
| 311/0/sqlite/C/2 | 10163 | 8768 | 0.882 | 1.134 | 1.243 | 0.959 | -2.2% to +1.0% |
| 311/0/sqlite/C/8 | 17587 | 15939 | 0.901 | 1.110 | 1.109 | 0.910 | -1.2% to +2.3% |
| 311/0/sqlite/C/long | 62353 | 32532 | 0.526 | 1.901 | 1.553 | 1.278 | -2.9% to +1.0% |
| 311/1/memory/I/1 | 4835 | 4821 | 0.949 | 1.023 | 1.030 | 1.073 | -3.8% to -0.7% |
| 311/1/memory/I/2 | 5059 | 5171 | 1.060 | 0.959 | 0.884 | 1.040 | +2.5% to +3.7% |
| 311/1/memory/I/8 | 7864 | 8097 | 1.009 | 0.988 | 0.968 | 1.050 | +1.5% to +3.6% |
| 311/1/memory/I/long | 15090 | 12537 | 0.832 | 1.200 | 0.059 | 0.111 | +5.9% to +12.1% |
| 311/1/memory/C/1 | 4981 | 4825 | 0.971 | 1.026 | 1.088 | 1.167 | -2.6% to +0.2% |
| 311/1/memory/C/2 | 5751 | 5566 | 0.963 | 1.038 | 1.197 | 1.173 | +2.0% to +3.5% |
| 311/1/memory/C/8 | 8880 | 8583 | 0.953 | 1.050 | 1.171 | 1.088 | +0.7% to +2.4% |
| 311/1/memory/C/long | 15041 | 12095 | 0.807 | 1.238 | 0.063 | 0.116 | -1.2% to +3.4% |
| 311/1/sqlite/I/1 | 2950 | 3040 | 1.036 | 0.980 | 0.991 | 1.017 | -3.9% to -3.6% |
| 311/1/sqlite/I/2 | 3388 | 3342 | 0.980 | 1.025 | 1.041 | 1.017 | +0.6% to +1.4% |
| 311/1/sqlite/I/8 | 5386 | 5398 | 0.980 | 1.003 | 0.999 | 1.015 | -0.8% to +2.3% |
| 311/1/sqlite/I/long | 8268 | 6327 | 0.759 | 1.233 | 0.048 | 0.142 | -11.4% to -2.0% |
| 311/1/sqlite/C/1 | 3117 | 2991 | 0.939 | 1.029 | 1.093 | 1.039 | -0.5% to +1.8% |
| 311/1/sqlite/C/2 | 3467 | 3339 | 0.941 | 1.028 | 1.138 | 1.039 | -0.9% to -0.5% |
| 311/1/sqlite/C/8 | 5384 | 5413 | 1.029 | 0.983 | 0.949 | 1.056 | +0.7% to +0.8% |
| 311/1/sqlite/C/long | 7811 | 6332 | 0.934 | 1.199 | 0.034 | 0.146 | -3.5% to +8.4% |
| 311/2/memory/I/1 | 2755 | 2785 | 1.010 | 0.988 | 0.966 | 1.499 | -1.9% to -1.4% |
| 311/2/memory/I/2 | 3187 | 3260 | 1.053 | 0.962 | 0.900 | 2.275 | -1.1% to +0.9% |
| 311/2/memory/I/8 | 5495 | 5390 | 0.980 | 1.019 | 1.053 | 0.875 | -3.5% to +2.6% |
| 311/2/memory/I/long | 8574 | 7848 | 0.907 | 1.102 | 0.050 | 0.383 | -3.4% to +0.1% |
| 311/2/memory/C/1 | 2680 | 2753 | 1.030 | 0.972 | 0.966 | 1.443 | -1.0% to -0.9% |
| 311/2/memory/C/2 | 3303 | 3276 | 0.963 | 1.042 | 1.020 | 1.911 | -0.9% to +0.4% |
| 311/2/memory/C/8 | 5318 | 5268 | 0.991 | 1.007 | 1.017 | 1.581 | -9.3% to -6.1% |
| 311/2/memory/C/long | 8247 | 7804 | 0.931 | 1.077 | 0.051 | 0.444 | -2.4% to +2.4% |
| 311/2/sqlite/I/1 | 1413 | 1425 | 1.032 | 0.961 | 0.919 | 1.250 | -1.9% to +0.8% |
| 311/2/sqlite/I/2 | 1679 | 1706 | 1.047 | 0.963 | 0.895 | 0.948 | +1.4% to +6.1% |
| 311/2/sqlite/I/8 | 2731 | 2738 | 0.987 | 0.993 | 1.009 | 1.249 | -0.9% to +5.2% |
| 311/2/sqlite/I/long | 3700 | 3276 | 0.898 | 1.094 | 0.072 | 0.251 | -0.5% to +2.3% |
| 311/2/sqlite/C/1 | 1414 | 1414 | 0.975 | 0.992 | 1.015 | 0.821 | -2.1% to +0.9% |
| 311/2/sqlite/C/2 | 1689 | 1722 | 1.049 | 0.980 | 0.981 | 1.000 | -5.1% to +0.6% |
| 311/2/sqlite/C/8 | 2642 | 2722 | 1.113 | 0.970 | 0.955 | 0.702 | -0.0% to +3.6% |
| 311/2/sqlite/C/long | 3606 | 3173 | 0.872 | 1.104 | 0.060 | 0.299 | -0.2% to +3.9% |
| 5/0/memory/I/1 | 7416 | 7382 | 0.995 | 1.002 | 1.012 | 1.041 | -0.6% to +2.9% |
| 5/0/memory/I/2 | 8971 | 9049 | 1.032 | 0.980 | 0.916 | 1.039 | -4.0% to -0.2% |
| 5/0/memory/I/8 | 16221 | 15143 | 0.937 | 1.067 | 1.101 | 1.053 | -4.2% to -1.1% |
| 5/0/memory/I/long | 47479 | 32693 | 0.684 | 1.463 | 0.781 | 0.931 | -4.6% to -1.1% |
| 5/0/memory/C/1 | 9125 | 7265 | 0.816 | 1.214 | 1.252 | 1.028 | +1.0% to +5.1% |
| 5/0/memory/C/2 | 10184 | 8604 | 0.840 | 1.190 | 1.253 | 0.963 | -6.4% to -0.3% |
| 5/0/memory/C/8 | 16994 | 14802 | 0.875 | 1.143 | 1.197 | 0.910 | -1.4% to +3.3% |
| 5/0/memory/C/long | 60944 | 28819 | 0.474 | 2.112 | 1.690 | 1.490 | +1.3% to +2.0% |
| 5/0/sqlite/I/1 | 7135 | 6880 | 0.976 | 1.018 | 1.073 | 1.045 | -3.3% to +3.0% |
| 5/0/sqlite/I/2 | 8603 | 8569 | 0.992 | 1.008 | 1.025 | 1.038 | -4.6% to -0.2% |
| 5/0/sqlite/I/8 | 15474 | 14976 | 0.976 | 1.024 | 1.039 | 1.061 | -2.0% to +1.8% |
| 5/0/sqlite/I/long | 46757 | 32055 | 0.687 | 1.457 | 0.802 | 0.931 | -4.7% to +0.5% |
| 5/0/sqlite/C/1 | 8845 | 7055 | 0.805 | 1.231 | 1.273 | 1.051 | -4.3% to +2.6% |
| 5/0/sqlite/C/2 | 9714 | 8384 | 0.872 | 1.147 | 1.184 | 0.965 | -0.9% to +5.6% |
| 5/0/sqlite/C/8 | 16800 | 15245 | 0.928 | 1.100 | 1.020 | 0.908 | -2.1% to -0.1% |
| 5/0/sqlite/C/long | 60176 | 29444 | 0.490 | 2.042 | 1.629 | 1.497 | -7.8% to -0.6% |
| 5/1/memory/I/1 | 4730 | 4632 | 0.974 | 1.026 | 1.042 | 1.045 | -1.3% to +1.5% |
| 5/1/memory/I/2 | 5429 | 5319 | 0.983 | 1.018 | 1.061 | 1.074 | -2.9% to +3.2% |
| 5/1/memory/I/8 | 8388 | 8297 | 1.001 | 1.010 | 0.961 | 1.056 | -4.5% to -0.3% |
| 5/1/memory/I/long | 14725 | 11976 | 0.827 | 1.208 | 0.056 | 0.129 | -0.1% to +3.7% |
| 5/1/memory/C/1 | 4814 | 4574 | 0.948 | 1.057 | 1.138 | 1.100 | +0.7% to +1.8% |
| 5/1/memory/C/2 | 5465 | 5279 | 0.972 | 1.031 | 1.175 | 1.109 | +1.8% to +5.1% |
| 5/1/memory/C/8 | 8634 | 8248 | 0.964 | 1.035 | 1.158 | 1.087 | -0.6% to +2.4% |
| 5/1/memory/C/long | 13861 | 11084 | 0.802 | 1.247 | 0.064 | 0.132 | +0.6% to +4.3% |
| 5/1/sqlite/I/1 | 2987 | 3014 | 0.992 | 1.009 | 1.002 | 1.018 | -0.7% to +0.9% |
| 5/1/sqlite/I/2 | 3334 | 3271 | 0.982 | 1.018 | 1.015 | 1.024 | +0.1% to +3.7% |
| 5/1/sqlite/I/8 | 5350 | 5189 | 0.960 | 1.044 | 1.066 | 1.023 | -1.5% to -1.2% |
| 5/1/sqlite/I/long | 8131 | 6037 | 0.745 | 1.278 | 0.052 | 0.162 | -7.8% to +2.5% |
| 5/1/sqlite/C/1 | 2951 | 2873 | 0.966 | 1.029 | 1.064 | 1.039 | -4.5% to -0.5% |
| 5/1/sqlite/C/2 | 3399 | 3291 | 0.949 | 1.036 | 1.102 | 1.036 | -0.6% to +2.4% |
| 5/1/sqlite/C/8 | 5217 | 5164 | 0.917 | 0.995 | 1.034 | 1.051 | -2.4% to -1.4% |
| 5/1/sqlite/C/long | 7941 | 6114 | 0.750 | 1.242 | 0.050 | 0.168 | -0.6% to +0.8% |
| 5/2/memory/I/1 | 2645 | 2628 | 0.990 | 1.011 | 1.035 | 1.040 | -3.8% to -0.4% |
| 5/2/memory/I/2 | 3244 | 3234 | 1.001 | 0.999 | 1.007 | 1.191 | -4.4% to -0.3% |
| 5/2/memory/I/8 | 5358 | 5245 | 0.974 | 1.025 | 1.011 | 0.820 | -4.7% to +0.6% |
| 5/2/memory/I/long | 8174 | 7608 | 0.920 | 1.088 | 0.048 | 0.318 | +1.8% to +3.7% |
| 5/2/memory/C/1 | 2639 | 2678 | 1.023 | 0.980 | 0.956 | 1.351 | -3.7% to +1.1% |
| 5/2/memory/C/2 | 3212 | 3189 | 1.039 | 0.962 | 1.000 | 1.607 | -5.7% to +0.0% |
| 5/2/memory/C/8 | 5231 | 5179 | 0.997 | 1.003 | 1.000 | 1.501 | -0.9% to +0.8% |
| 5/2/memory/C/long | 7995 | 7421 | 0.928 | 1.077 | 0.051 | 0.249 | -0.8% to +0.3% |
| 5/2/sqlite/I/1 | 1386 | 1403 | 1.025 | 0.995 | 1.043 | 0.704 | -4.7% to -3.3% |
| 5/2/sqlite/I/2 | 1669 | 1659 | 0.970 | 1.014 | 1.019 | 1.573 | -4.2% to -2.0% |
| 5/2/sqlite/I/8 | 2651 | 2646 | 0.990 | 1.009 | 1.016 | 1.019 | -5.9% to -1.6% |
| 5/2/sqlite/I/long | 3617 | 3207 | 0.887 | 1.086 | 0.054 | 0.275 | -5.0% to -1.8% |
| 5/2/sqlite/C/1 | 1378 | 1400 | 1.012 | 0.985 | 1.028 | 1.005 | +0.3% to +1.8% |
| 5/2/sqlite/C/2 | 1642 | 1603 | 0.958 | 1.002 | 1.105 | 0.704 | -36.6% to -0.8% |
| 5/2/sqlite/C/8 | 2642 | 2600 | 0.867 | 1.002 | 1.028 | 2.524 | +0.1% to +2.8% |
| 5/2/sqlite/C/long | 3571 | 3109 | 0.905 | 1.096 | 0.068 | 0.361 | -2.1% to +2.7% |

## Fixed offered-load diagnostics

The reference is the exact previous follow-up `6c76a48`, not original main.
Its long-lot median from the four A-labelled observations in the new functional
matrix’s two A/A cycles is calibrated once and frozen for both arms. Offered rates are 50%, 75% and 90% of that value, over
both protocols, QoS 0/1, memory, and both delivery modes: 24 cells. The source
predates the new corrections; the recalibration occurs after their
implementation. It is not an earlier-in-time pre-edit capacity observation.

A separate process on CPU 5 emits scheduled tokens. Each token causes one
`publish()` call; the long-lot label in the reference data identifies the
calibration cell. The aggregate-only admission speedup belongs to the saturated
long-lot comparison. Samples retain each
message's planned arrival, pacer timestamp immediately before token send, call start, admission return,
application delivery and receipt-observed completion. The token socket is
blocking on the pacer side: a full buffer can delay later emissions. Planned
arrivals remain fixed, while pre-send lateness and planned-to-call lag expose
that pressure. The offered rate is the prescribed schedule, not a promise that
actual token emission maintains that rate under overload. Delivery may precede
admission return; the API return is an observable boundary, not the internal
commit instant. Residual time after return is clamped at zero and early delivery
is counted explicitly. Planned-to-call delay includes producer lag; the
scheduled-to-delivery metric retains that lag instead of hiding it.

Samples target two seconds but are bounded to 1,024–20,000 messages, so higher
rates can have a shorter scheduled span. A fraction of saturated long-lot
throughput is a workload definition, not demonstrated sustainable paced
capacity or a steady-state service guarantee. The pending receipt list is
bounded by the finite sample. Pacer emission/loss and delivery counts must
agree. Reported p95s below are medians of per-run p95s, not pooled quantiles.

Each latency entry is A / B in milliseconds. Pacer pre-send p95 uses the external pacer’s linear-interpolation percentile; other latency p95s use the nearest-rank definition verified from the clocks.

| Protocol/QoS/mode/fraction | Offered/s | Messages | Planned→call p95 | Call→return p95 | Planned→delivery p95 |
| --- | --- | --- | --- | --- | --- |
| 311/0/I/50% | 12824.9 | 20000 | 0.284 / 0.314 | 0.038 / 0.039 | 1.193 / 1.317 |
| 311/0/I/75% | 19237.3 | 20000 | 0.519 / 0.474 | 0.035 / 0.035 | 2.369 / 2.405 |
| 311/0/I/90% | 23084.8 | 20000 | 0.742 / 0.963 | 0.034 / 0.034 | 4.247 / 5.748 |
| 311/0/C/50% | 12370.1 | 20000 | 0.290 / 0.324 | 0.039 / 0.039 | 1.208 / 1.372 |
| 311/0/C/75% | 18555.1 | 20000 | 0.496 / 0.476 | 0.035 / 0.035 | 2.368 / 2.482 |
| 311/0/C/90% | 22266.2 | 20000 | 0.869 / 1.019 | 0.034 / 0.034 | 4.527 / 6.224 |
| 311/1/I/50% | 5743.7 | 11488 | 0.383 / 0.395 | 0.062 / 0.064 | 1.376 / 1.424 |
| 311/1/I/75% | 8615.5 | 17231 | 0.564 / 0.613 | 0.053 / 0.054 | 2.445 / 2.480 |
| 311/1/I/90% | 10338.6 | 20000 | 0.738 / 0.840 | 0.052 / 0.052 | 3.425 / 5.023 |
| 311/1/C/50% | 5751.1 | 11503 | 0.402 / 0.380 | 0.064 / 0.062 | 1.429 / 1.341 |
| 311/1/C/75% | 8626.7 | 17254 | 0.582 / 0.625 | 0.054 / 0.054 | 2.434 / 2.802 |
| 311/1/C/90% | 10352.0 | 20000 | 0.972 / 0.956 | 0.051 / 0.052 | 5.938 / 5.414 |
| 5/0/I/50% | 11446.8 | 20000 | 0.312 / 0.290 | 0.042 / 0.040 | 1.276 / 1.190 |
| 5/0/I/75% | 17170.2 | 20000 | 0.523 / 0.530 | 0.036 / 0.036 | 2.361 / 2.350 |
| 5/0/I/90% | 20604.2 | 20000 | 1.100 / 0.837 | 0.035 / 0.035 | 5.785 / 4.605 |
| 5/0/C/50% | 11773.4 | 20000 | 0.328 / 0.344 | 0.039 / 0.039 | 1.344 / 1.378 |
| 5/0/C/75% | 17660.1 | 20000 | 0.690 / 0.660 | 0.036 / 0.036 | 3.044 / 3.026 |
| 5/0/C/90% | 21192.1 | 20000 | 0.710 / 0.939 | 0.035 / 0.033 | 3.401 / 4.742 |
| 5/1/I/50% | 5738.1 | 11477 | 0.404 / 0.386 | 0.061 / 0.061 | 1.455 / 1.286 |
| 5/1/I/75% | 8607.2 | 17215 | 0.669 / 0.664 | 0.054 / 0.053 | 2.833 / 2.794 |
| 5/1/I/90% | 10328.6 | 20000 | 1.106 / 1.003 | 0.051 / 0.052 | 12.907 / 4.840 |
| 5/1/C/50% | 5528.5 | 11058 | 0.412 / 0.422 | 0.063 / 0.063 | 1.436 / 1.454 |
| 5/1/C/75% | 8292.8 | 16586 | 0.661 / 0.658 | 0.054 / 0.054 | 2.723 / 2.692 |
| 5/1/C/90% | 9951.3 | 19903 | 1.070 / 1.252 | 0.052 / 0.051 | 5.939 / 20.442 |

| Cell | Pacer pre-send p95 A/B, ms | Residual p95 A/B, ms | CPU/msg B/A | Early deliveries A/B | Observed messages/arm |
| --- | --- | --- | --- | --- | --- |
| 311/0/I/50% | 0.001 / 0.001 | 0.991 / 1.084 | 0.998 | 0 / 0 | 120000 |
| 311/0/I/75% | 0.001 / 0.001 | 1.949 / 1.990 | 1.001 | 0 / 0 | 120000 |
| 311/0/I/90% | 0.001 / 0.001 | 3.592 / 4.883 | 0.998 | 0 / 0 | 120000 |
| 311/0/C/50% | 0.001 / 0.001 | 0.993 / 1.134 | 0.996 | 0 / 0 | 120000 |
| 311/0/C/75% | 0.001 / 0.001 | 1.989 / 2.107 | 0.986 | 0 / 0 | 120000 |
| 311/0/C/90% | 0.001 / 0.001 | 3.832 / 5.294 | 1.000 | 0 / 0 | 120000 |
| 311/1/I/50% | 0.001 / 0.001 | 1.071 / 1.106 | 1.005 | 0 / 0 | 68928 |
| 311/1/I/75% | 0.001 / 0.001 | 1.996 / 1.991 | 0.999 | 0 / 0 | 103386 |
| 311/1/I/90% | 0.001 / 0.001 | 2.838 / 4.410 | 1.001 | 0 / 0 | 120000 |
| 311/1/C/50% | 0.001 / 0.001 | 1.111 / 1.034 | 1.004 | 0 / 0 | 69018 |
| 311/1/C/75% | 0.001 / 0.001 | 1.985 / 2.277 | 0.997 | 0 / 0 | 103524 |
| 311/1/C/90% | 0.001 / 0.001 | 5.146 / 4.574 | 0.998 | 0 / 0 | 120000 |
| 5/0/I/50% | 0.001 / 0.001 | 1.050 / 0.984 | 0.999 | 0 / 0 | 120000 |
| 5/0/I/75% | 0.001 / 0.001 | 1.954 / 1.915 | 1.010 | 0 / 0 | 120000 |
| 5/0/I/90% | 0.001 / 0.001 | 4.953 / 3.797 | 1.009 | 0 / 0 | 120000 |
| 5/0/C/50% | 0.001 / 0.001 | 1.101 / 1.133 | 0.996 | 0 / 0 | 120000 |
| 5/0/C/75% | 0.001 / 0.001 | 2.458 / 2.496 | 1.001 | 0 / 0 | 120000 |
| 5/0/C/90% | 0.001 / 0.001 | 2.853 / 4.084 | 1.010 | 0 / 0 | 120000 |
| 5/1/I/50% | 0.001 / 0.001 | 1.123 / 0.988 | 0.999 | 0 / 0 | 68862 |
| 5/1/I/75% | 0.001 / 0.001 | 2.285 / 2.273 | 0.997 | 0 / 0 | 103290 |
| 5/1/I/90% | 0.001 / 0.001 | 12.148 / 4.123 | 1.002 | 0 / 0 | 120000 |
| 5/1/C/50% | 0.001 / 0.001 | 1.093 / 1.121 | 1.003 | 0 / 0 | 66348 |
| 5/1/C/75% | 0.001 / 0.001 | 2.212 / 2.198 | 1.004 | 0 / 0 | 99516 |
| 5/1/C/90% | 0.001 / 0.001 | 5.079 / 19.587 | 0.998 | 0 / 0 | 119418 |

| Cell | AA delivery-p95 cycle change | AB delivery-p95 cycle change | AA CPU cycle change |
| --- | --- | --- | --- |
| 311/0/I/50% | -1.6% to +8.6% | +9.8% to +34.8% | -0.2% to -0.1% |
| 311/0/I/75% | -80.3% to -38.7% | -74.1% to +5.0% | -0.2% to +0.3% |
| 311/0/I/90% | +23.7% to +117.1% | -15.4% to +36.8% | -0.1% to +0.3% |
| 311/0/C/50% | -8.6% to -2.9% | +4.8% to +281.6% | +0.0% to +0.3% |
| 311/0/C/75% | +13.3% to +26.2% | -12.5% to +881.9% | -0.1% to +0.0% |
| 311/0/C/90% | -0.9% to +17.8% | -48.0% to +154.8% | -0.0% to +0.2% |
| 311/1/I/50% | -80.7% to +29.3% | -62.5% to +24.0% | -0.3% to +1.5% |
| 311/1/I/75% | -12.0% to +7.8% | -4.2% to +9.4% | -0.1% to -0.0% |
| 311/1/I/90% | +2.7% to +39.9% | -53.3% to +53.2% | -0.0% to +0.0% |
| 311/1/C/50% | -9.4% to +35.3% | -67.0% to +17.5% | -0.6% to +0.1% |
| 311/1/C/75% | -2.1% to -1.6% | -83.3% to +17.4% | -0.0% to +0.0% |
| 311/1/C/90% | +62.9% to +116.3% | -57.1% to +388.1% | -1.1% to -0.1% |
| 5/0/I/50% | -18.1% to -7.3% | -34.3% to +1.8% | -0.0% to +0.1% |
| 5/0/I/75% | -8.2% to +4.3% | -87.0% to +7.8% | -0.1% to -0.0% |
| 5/0/I/90% | -29.5% to +105.8% | -86.3% to -15.5% | -0.1% to +0.1% |
| 5/0/C/50% | -13.2% to +0.7% | -8.0% to +416.8% | +0.0% to +0.1% |
| 5/0/C/75% | -15.4% to +9.3% | -11.0% to +19.9% | +0.1% to +0.1% |
| 5/0/C/90% | +92.0% to +202.3% | -32.5% to +1878.2% | +0.3% to +0.7% |
| 5/1/I/50% | -70.6% to -15.4% | -10.4% to +0.9% | +0.1% to +1.3% |
| 5/1/I/75% | -1.5% to +35.5% | -67.5% to +554.4% | +0.0% to +0.1% |
| 5/1/I/90% | -59.9% to -28.3% | -81.8% to -38.2% | +0.0% to +0.1% |
| 5/1/C/50% | -60.7% to -16.0% | -17.1% to +14.5% | -0.1% to +2.0% |
| 5/1/C/75% | -5.8% to +5.5% | -87.2% to +11.7% | -0.0% to +0.1% |
| 5/1/C/90% | +36.7% to +821.5% | +80.1% to +220.4% | -0.1% to -0.1% |

Client CPU/message ratios across the 24 fixed-schedule cells range from
**0.986 to 1.010** versus the previous follow-up. That is much smaller than
the aggregate publication-only gain: these trials call `publish()` separately.
The median per-run pacer pre-send p95 stays between **0.525 and 0.696 µs**
across displayed A/B arms. No early delivery was observed in the 480 samples;
the clock format and zero-clamped residual still preserve that possibility.

The latency tails are too variable to support a general improvement claim.
For MQTT 5/QoS 0/callback at 90%, median per-run scheduled-to-delivery p95 is
3.40 ms for A and 4.74 ms for B, but one A/B cycle ratio reaches **19.78**.
In that cycle, one B sample has 136.35 ms delivery p95, 10.10 ms planned-to-call
p95 and only about 0.02 ms call-to-return p95. These separately computed
percentiles must not be subtracted as an exact latency decomposition, but they
show why admission-call duration alone is an inadequate end-to-end measure.

MQTT 5/QoS 1/callback at 90% has median per-run delivery p95 of 5.94 ms for A
and 20.44 ms for B, with all three A/B cycle ratios above one. Its same-source
A/A control, however, also has a **9.21** cycle ratio. Other cells show large
apparent improvements alongside similarly volatile controls. These finite
samples expose queue residence and sporadic tail excursions; they neither
establish a stable source-wide latency change nor identify the cause of each
excursion. All outlying samples remain in the raw files and cycle ranges.

The planned-to-call, call-to-return and planned-to-delivery columns describe
observable boundaries. A fast return can coexist with substantial delivery
backlog. The saturated matrix, fixed schedule, CPU measurements and separate
orchestration diagnostics answer different questions and are not combined
into a single claimed capacity or latency guarantee.


## Separated orchestration diagnostics

The in-memory diagnostic separates publication with no message delivery,
receive-only iterator delivery, receive-only callback delivery and callback
queue work without packet parsing. Counts calibrate on A toward 0.5 seconds,
with 2,048–200,000 operations. Every comparison uses the same count and fresh
processes. This isolates costs but does not predict broker/network throughput.

Optional cProfile phases are separate from timing and use 2,048 operations.
Profiles include their own connection setup and teardown. Python call counts
include resumptions; they are not system-call or scheduler context-switch counts. The scripted broker decodes and retains published
packets, so its work participates in the publication diagnostic.

| Scenario | Operations | Rate B/A | AA rate cycle change | CPU/msg B/A |
| --- | --- | --- | --- | --- |
| publish | 18848 | 1.356 | -3.3% to +1.4% | 0.737 |
| receive_iterator | 38248 | 0.941 | -4.9% to +4.7% | 1.062 |
| receive_callback | 35560 | 0.988 | -3.1% to +0.7% | 1.012 |
| callback_only | 112148 | 0.923 | -1.9% to +0.3% | 1.084 |

All four diagnostics compare `6c76a48` with `515fd3c`. Publication-only rate
improves by **35.6%**, with **26.3% less client-process CPU per operation**;
every A/B rate cycle improves by 30–43%, versus A/A variation of about −3.3%
to +1.4%. This supports the intended reduction in local publication work.
The scripted broker's decoding and packet retention remain part of that cost.

Receive-only iterator rate is 5.9% lower, but its three A/B cycles range from
near parity to −11.8%, while A/A spans about ±5%. Receive-only callback rate
is nearly unchanged (−1.2%), comparable with its controls. Callback-queue-only
rate is **7.7% lower**, with all A/B cycles down 6.0–9.0% against a much smaller
A/A range (−1.9% to +0.3%). This makes the fairness-related queue cost visible
apart from packet parsing and network timing. No callback or receive result
is presented as a consequence of the publication gain.


Selected Python call/resumption counts in separate 2,048-operation profiles (largest cumulative-time file/function groups; same-named definitions within a file are aggregated; rows are not additive):

| Scenario | Function | Calls A | Calls B |
| --- | --- | --- | --- |
| publish | api/async_client.py:publish_many | 9 | 9 |
| publish | api/async_client.py:_publish_one | 2048 | 2048 |
| publish | api/_writer.py:_run | 11 | 11 |
| publish | api/async_client.py:_try_direct_qos0_publish | 0 | 2048 |
| publish | api/async_client.py:_commit_publish | 2048 | 0 |
| publish | protocol/outbound.py:queue_publish | 2048 | 0 |
| publish | api/_writer.py:_write_contiguous | 10 | 10 |
| publish | api/_effects.py:drain_inline | 4097 | 2049 |
| receive_iterator | api/async_client.py:_read_loop | 21 | 21 |
| receive_iterator | api/async_client.py:_process_ingress_batch | 17 | 17 |
| receive_iterator | protocol/engine.py:handle_raw | 2049 | 2049 |
| receive_iterator | protocol/inbound.py:_on_publish_v311 | 2048 | 2048 |
| receive_iterator | api/_effects.py:drain | 10 | 10 |
| receive_iterator | api/_effects.py:drain_inline | 9 | 9 |
| receive_iterator | packets/_publish.py:decode_qos0_message_v311 | 2048 | 2048 |
| receive_iterator | api/async_client.py:_apply_effect_inline | 2051 | 2051 |
| receive_callback | api/async_client.py:_read_loop | 21 | 21 |
| receive_callback | api/async_client.py:_process_ingress_batch | 17 | 17 |
| receive_callback | protocol/engine.py:handle_raw | 2049 | 2049 |
| receive_callback | protocol/inbound.py:_on_publish_v311 | 2048 | 2048 |
| receive_callback | api/_delivery.py:_callback_worker | 10 | 34 |
| receive_callback | api/_effects.py:drain | 10 | 10 |
| receive_callback | api/_effects.py:drain_inline | 9 | 9 |
| receive_callback | packets/_publish.py:decode_qos0_message_v311 | 2048 | 2048 |
| callback_only | api/_delivery.py:_callback_worker | 4 | 34 |
| callback_only | api/_delivery.py:accept | 2049 | 2064 |
| callback_only | api/_delivery.py:try_accept | 2048 | 2048 |
| callback_only | api/_delivery.py:invoke_isolated | 2048 | 2048 |
| callback_only | api/_delivery.py:invoke | 2049 | 2049 |
| callback_only | api/_delivery.py:logical_size | 2048 | 2048 |
| callback_only | api/_delivery.py:ensure_callback_worker | 2048 | 2048 |
| callback_only | api/async_client.py:connect | 2 | 2 |

## Evidence and reproducibility

The new campaign's evidence bundle is produced under the persistent local
`.local-evidence/lean-api-2026-09-10` directory. Its exact filename and outer
SHA-256 are returned with the completed report. The bundle includes this report
and its generation inputs; the report cannot embed the checksum of an archive
that contains the report itself without creating a circular dependency.

The earlier `recovery-checkpoint-2026-09-13.tar.gz` is only a recovery snapshot
with qualification and smoke evidence. It is not the final performance
archive. Neither bundle can restore the raw files that the reboot erased from
`/tmp`.


The archive retains source-specific raw blocks, their argument fingerprints,
complete A/A and A/B samples, clock files, runner probes, successful and failed
recovery smoke logs, qualification logs, hosted CI records, source counts and
the campaign/audit/report helpers, and a Git bundle verified through a fresh
local clone. The archive manifest hashes individual
members; an adjacent SHA-256 file checks the archive itself. No generated raw
results, caches or archives are committed to the product repository.

The independent audit verified 174 completed cells, 3,480 comparison samples, 480 paced clock files and 8,674,300 clock rows. It recomputed cycle ratios and paced percentiles from raw data, validated ABBA order, exact counts and reference rates, and checked per-block and clock-file SHA-256 hashes.
