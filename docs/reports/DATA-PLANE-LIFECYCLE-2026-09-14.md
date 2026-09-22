# MQTTium data-plane/lifecycle follow-up — 2026-09-14

This report bounds correctness qualification and final performance to **`21684c9e04cda5b9a095d5c0e72eb754b0f51021`**. The revised callback/lifecycle contract, main reconciliation, local correctness/soak and hosted runtime checks are complete. The saturated comparison contains all 96 cells / 1,920 observations; the final in-memory diagnostics contain all 10 cells / 200 observations. The paced campaign accounts for all 24 planned cases: **23 complete and one failed**, with 474 returned observations, one failed worker and five unattempted positions. It is not a successful 24-cell campaign.

Against RC14, saturated delivered rate is **8.80% lower**, worker CPU/message **9.09% higher**, traced Python allocation peak **36.95% lower**, and worker RSS high-water **3.12% lower**. The largest grouped rate deficit remains QoS0 callback long lots (**42.76% lower**). Broker-confirmed outgoing QoS0 drops at 90% of the main-defined paced reference are a material adverse result, not a sample to retry away. The contract and ownership improvements therefore do not establish performance parity with RC14.

The accepted direction separates synchronous message callbacks from asynchronous lifecycle hooks and separates parsed protocol progress from application-delivery pressure. The selected publication prototype retains bounded QoS0 amortization and removes the additional QoS1 admission branch. That simplification preserves the observed QoS0 gain, with a measured QoS1 allocation and network-throughput cost described below.

## Scope and source identities

The controlling evidence is the new persistent campaign directory. Code-formatted evidence paths below are relative to its archive root; they are not links into the documentation site. Each complete-stage summary is derived from a completed raw result with a matching independent audit. Paced outcomes are reported separately because one scenario failed. Earlier reports remain historical; no result here is reconstructed from rounded peer-review numbers.

| Role | Exact source commit | `src` tree |
| --- | --- | --- |
| Previous PR head | `a7327eae6dc7eb4ab15cdcf6eb5f9acb8868022a` | `0a5bcc7ddeddeb967005a90e444532f26aa23a5d` |
| Revised contract and associated correctness fixes | `432a08f1ee4fd0eafbfb646da930759b02d66fc8` | `b2aeb834e9ae6d5442a1a817458f4271c7f8869d` |
| Full QoS0/QoS1 prefix prototype | `b17010c07416a33dbcc90c204c5a507063b83d29` | `1fc7552a76dee30ceea90d3f449d25569eb025e2` |
| Selected QoS0-only prefix, quantum 64 | `18256da889bca6a39d02a0a89b63e7f997e0f09d` | `f9728b2aad7a6b9d37195d8ae33520dd9db94409` |
| Selected quantum 128 candidate | `a2c4238deccd3938c8326f5587df3dcd293655ce` | `298e6c0d974491b23a4b5b62ab8329b5299161a6` |
| Quantum 256 comparison candidate | `6441897eb7f25de92cebdc576416c09d940edcb6` | `4ccd5b75dcb88696a96555da05982d03d33754a4` |
| Reconciled runtime candidate, quantum 128 | `b530506e2d18744a2f6b811317a6fac8cfef177d` | `35794a15cf563a3532fd19fcb3469d5e5db69a17` |
| Portable diagnostic successor, quantum 128 | `21684c9e04cda5b9a095d5c0e72eb754b0f51021` | `35794a15cf563a3532fd19fcb3469d5e5db69a17` |

These hashes identify the entire `src` tree, whereas some historical reports name `src/mqttium`. Test-tree hashes and isolated source paths are retained in each stage manifest/summary. All completed comparisons below use frozen harness `7eaed8383e83092715047c7204d8303711f3378a`. They compare the named revisions, not an installed editable package.

The clean reconciled candidate is `b530506e2d18744a2f6b811317a6fac8cfef177d`, with tests tree `ae94e86227aaf52bdffd796c33b8ec1990d7b0cc` and benchmark tree `91774c742613575799d93887ae5c9c94173d814c`. It retains selected quantum 128. Its first parent is connection-ownership fix `781435c0f84c20f003fb40cb585c0583c36e7376`; its second parent is current-main reference `c194597bcf5af4951fbec2b560600eef3cb84b3c`. Final measurements use frozen harness `7eaed8383e83092715047c7204d8303711f3378a` on both arms. The candidate benchmark tree differs from that harness only in `paired_qos1_rtt.py`, which these campaigns do not execute.

The successor candidate is `21684c9e04cda5b9a095d5c0e72eb754b0f51021`: its runtime tree is identical to `b530506e`, its tests tree is `804ebcb55deda5e509bd1cab027a4272d9f10888`, and its benchmark tree is `84eb2018ede81da3164ed139e4a15a7c6841d80b`. It fixes the diagnostic script's incidental import of the Unix-only `resource` module through the network harness by using a local Git metadata helper, and adds an import regression with `resource` unavailable. Windows tests remain enabled. The executed campaign harness remains frozen at `7eaed838`; it is not replaced by the maintained diagnostic source.

The successor passed its own local and hosted qualification, recorded below. Final performance now uses `final-portable` (96 cells), `paced-portable` (24) and `final-portable-diagnostics` (10), with the unchanged frozen `7eaed838` harness and audited main-reference calibration. The final diagnostic audit is complete. The independent paced outcome audit validates 23 complete cells and the retained failed cell. This report makes no qualification claim for its later documentation commit: that commit's own checkout/check identity and runtime/test-tree equivalence belong in external `qualification/delivery.json`, avoiding a self-referential source hash. Identical runtime trees do not imply identical test/benchmark trees.

## Accepted contract and liveness design

| Surface | Contract |
| --- | --- |
| `on_message`, topic callbacks | Synchronous only; short application code in one bounded message worker |
| `messages()` | Canonical interface for asynchronous message processing |
| Publication completion | `PublishReceipt` / `PublishBatchReceipt`; `on_publish` removed |
| `on_connect`, `on_disconnect` | Synchronous or asynchronous lifecycle hooks, separate from message delivery and protocol effects |
| `auth_handler` | Protocol-specific synchronous or asynchronous handler supplied at construction, with its existing timeout and response rules |

Message callbacks are rejected when registered as asynchronous functions. An awaitable returned by a synchronous callback is reported as an error; the library does not await or schedule it. An application may explicitly transfer work into its own queue or retain its own task. Fairness counts actual callback invocations, including route fan-out, rather than only message jobs. It does not preempt a slow synchronous callback or promise a wall-clock bound. The quantum remains private.

Successful CONNACK processing applies client state, negotiation and connection setup, completes its protocol work and releases locks before recording `on_connect`. The hook is success-only and can subscribe or publish through the client. It is not a barrier requiring all inbound replay or application delivery to finish. `on_disconnect` is recorded after the retired connection's reader/writer/transport cleanup, with the original terminal cause or `None` for a clean close. `connect()` and `disconnect()` return after their network operation, without waiting for hook completion.

Assigning `on_connect` or `on_disconnect` changes notifications recorded afterward. Each notification captures its callable when recorded, so reassignment does not replace the callable of an active or already pending notification.

Lifecycle ownership consists of one retained supervisor, one active hook task and one pending latest-state notification. Obsolete pending transitions coalesce; this is not a lossless event log. External explicit takeover retires an obsolete active hook. A lifecycle operation awaited directly by the current hook preserves its caller; separately spawned application work is external. The next hook waits for the prior child to finish. Automatic retry waits for the current disconnect hook and rechecks application intent. Hook failures and a user-raised `CancelledError` without actual task cancellation are reported to the loop exception handler. AUTH remains a protocol exchange: its synchronous result or awaited asynchronous result participates in the response, subject to `auth_timeout` and protocol rules. It does not inherit an unrestricted reentrancy promise.

The final connection-ownership correction rejects an overlapping explicit `connect()` before changing the endpoint or superseding the first attempt, including while that attempt waits for the lifecycle lock, transport factory or CONNACK. Cancelling an explicit takeover while it waits preserves notifications from the surviving automatic connection. When a lifecycle hook directly awaits a connection attempt, transport failure, refusal, timeout or cancellation reaches that caller through the operation's ordinary outcome; this preservation expires when `connect()` exits, so later external loss or disconnect can retire the hook normally. These cases have 36 focused regression variants across normal/eager task scheduling; this coverage description is not a final-candidate qualification result.

The `EffectPump` now transfers protocol effects only. A bounded, reader-owned delivery lane carries messages and inbound-replay continuation markers, without another delivery worker. A delivery lot follows its recorded protocol fence; later protocol work does not extend that fence. Replay continuation remains behind its preceding message batch and delivery marks. The reader drains the current lot before reading another, so the split does not introduce speculative reads or unbounded buffering. Epoch checks and terminal cleanup retire stale work and preserve receipt failure ownership.

This removes the dependency of publication admission and already-parsed completions on unrelated blocked application delivery. It does **not** guarantee progress for an ACK still unread behind saturated incoming traffic. Applications with sustained feedback traffic may need a separate producer and an explicit nonblocking overflow policy, or separate connections. Sync-only callbacks solve the callback-worker suspension cycle; they do not remove every possible application backpressure cycle.

Public statistics fields and constructor tuning parameters have not been trimmed in this change. Separate individual/batch receipt registries remain. Public chunk atomicity, automatic server redirection, mutable properties, live route mutation and removed compatibility/borrowed-receive machinery are not restored.

## Structural tradeoff

The source counts compare exact revisions. Method counts use the `AsyncClient` AST; constructor counts exclude `self`. Physical runtime lines are a size measure, not a measure of behavioral complexity.

| Source | Runtime Python files | Physical runtime lines | `AsyncClient` methods / private | Constructor parameters |
| --- | ---: | ---: | ---: | ---: |
| Current main `c194597` | 64 | 17,248 | 119 / 93 | 33 |
| Previous PR head `a7327ea` | 58 | 13,839 | 75 / 53 | 32 |
| Qualified runtime `21684c9` | 60 | 14,145 | 78 / 56 | 32 |

The follow-up adds two files and 306 physical lines relative to the previous PR head while making delivery/lifecycle ownership explicit. It also adds three `AsyncClient` methods, all private. It remains 3,103 lines and four runtime files smaller than current main. The constructor still has 32 parameters; statistics/tuning-surface simplification has not been completed. These counts acknowledge the new ownership code's size cost rather than claiming every follow-up reduces the tree. The older experiment's historical main had 17,186 lines; it is not the current-main comparison basis. Exact structure record (`structure.json`).

## Measurement method and limits

Each cell has two same-source A/A ABBA cycles and three A/B ABBA cycles: 8 AA and 12 AB observations, using fresh worker processes. Both AA labels execute A's source. Each cycle divides the geometric mean of its two B observations by that of its two A observations; cell ratios give cycles equal weight. Grouped ratios give cells equal weight. AA ranges describe observed variation and are not subtracted from AB. Ranges are not confidence intervals, and these experiments do not establish statistical significance.

The intermediate network grid has both protocols, QoS0/1, memory storage, iterator/synchronous-callback delivery, and bursts 1/long: 16 cells. Long lots use `publish_many()`; burst 1 uses individual `publish()` and is an unchanged-path control for the prefix experiments. The workload sends 256-byte payloads through a self-subscribed client, with flow limit 20, and observes every ordered delivery and receipt. It measures combined publication and reception, not pure publisher capacity.

A control pilot selects the common observation count. Nominal target duration is 0.5 seconds, subject to existing count bounds. Pilots, warmups and diagnostic profiles are excluded from comparison counts. Diagnostics are packet-aware in-memory workloads, not network measurements; QoS1 individual/batch cases share window 20 and a full-draining scripted broker, with submitted/completed/wire counts checked.

The metrics use the following observable boundaries:

| Metric | Measured interval or scope |
| --- | --- |
| Network `latency_p50/p95/p99_us` | Payload timestamp creation to observation in `on_message` or the iterator consumer, after the broker forwards the publication to the same subscribed client |
| Paced `scheduled_to_delivery` | Planned external-pacer arrival to that same application observation, including delay before the publish call |
| Paced `call_to_admission_return` | Entry into `publish()` to its return; an API boundary, not an internal commitment or ACK timestamp |
| Raw paced `receipt_observed_done` | Time the bounded FIFO observer notices and awaits the completed receipt; not the instant an ACK arrives |
| CPU/message | Timed worker-process CPU divided by the measured message/operation count; external broker/pacer CPU is excluded, while in-process diagnostic broker/harness work is included |
| Python traced peak | Peak of allocations traced during a separate phase of at most 2,048 messages; tracing starts after client setup and warmup, so earlier allocations are excluded |
| RSS peak | Worker-process high-water RSS read at the end of the untraced performance phase, including interpreter, setup and warmup memory present before that point |

The network latency is an **application publish-to-receive round trip**, including admission, encoding, broker processing and delivery scheduling. It is not PUBLISH-to-PUBACK/PUBCOMP latency or TCP RTT. Individual publications are timestamped just before the call; `publish_many()` items are timestamped when its generator produces each item, not when the batch call starts. Receipts must complete for workload validation, but their completion is not the endpoint of these latency percentiles. Network rate also includes completion of all deliveries/receipts and the final inbound QoS2 tail. Paced rate spans first planned arrival to last delivery; its receipts and writer completion are additionally checked.

Paced `scheduled_to_call` includes pacer delay and queued demand before the API call. `residual_after_return` is `max(0, delivery − admission_return)`; delivery before the return is counted separately rather than described as negative residence. Its components and their percentiles must not be added into an RTT. The raw receipt-observation clock can include observer scheduling and FIFO delay. No isolated ACK-RTT distribution is produced by the final96, paced24 or diagnostics10 campaigns. The separate `paired_qos1_rtt.py` diagnostic is not part of this campaign.

The driver pins sources, harness Python-file hashes, arguments and interpreter path; completed blocks are durable checkpoints. Every measurement block requires eligible runner samples before execution, and ending conditions are retained without filtering adverse samples. Measurements are serial. Adverse ending samples and completed observations are retained. One-minute load at a block's end normally includes the campaign's worker, pacer and broker and can stay elevated after their work. The user reports an idle PC; there is no evidence attributing these readings to unrelated external activity.

Environment identity comes from the completed main-reference calibration, not partial final results: CPython **3.12.13** (Clang 22.1.1 build), Linux **6.8.0-138-lowlatency**, x86-64/glibc **2.39**, Intel **Core i7-3770 @ 3.40 GHz**, eight logical CPUs, and the **performance** governor. Client workers are pinned to logical CPU **4**; the controller reports affinity 0–7. The local broker is **Mosquitto 2.0.18**, on `127.0.0.1:11884`; the interpreter is `/home/yoch/mqttium/.venv/bin/python`. The reference completed from 03:58:36 to 04:02:29 UTC on 2026-09-14. Its eligibility limits were one-minute load per CPU ≤0.25, CPU usage ≤20%, and measured temperature ≤80°C, with three consecutive eligible preflight samples. These are recorded limits and identity, not a claim that every final postflight was eligible. Reference metadata (`stages/main-reference/raw/results.json`), recorded runner identity and limits (`stages/main-reference/raw/parts/001-preflight-20260914T035826804106Z.json`).

Saturated stages, paced stages and diagnostics answer different questions. Latency aggregates compare cell percentile ratios; they are not pooled latency percentiles. Do not multiply ratios from sequential campaigns into a claimed final comparison. Complete cell tables, AA ranges and raw hashes remain available in the archive-relative paths below.

## Contract checkpoint: benefits and regressions

**There is no direct final `21684c9` versus previous-PR `a7327ea` measurement in this campaign.** The direct `a7327ea` comparison is the 16-cell contract checkpoint against `432a08f`, limited to QoS0/1, memory storage, and bursts 1/long. Subsequent prefix and quantum comparisons have other source pairs, and later connection-ownership fixes are present in the final source. Their ratios cannot be added or multiplied into a final-versus-previous-PR result, nor bridged through historical main `9ad1f01` and current RC14 `c194597`. The final96 comparison directly answers final `21684c9` versus RC14 `c194597`.

The contract comparison includes the revised ownership design and associated correctness fixes together; it does not isolate each change. Its 16-cell descriptive aggregate is rate **−1.94%**, CPU/message **+1.94%**, Python peak **+7.32%**, and delivery p99 **+6.40%** against the previous PR head. It is an intermediate comparison, not a comparison against current main.

All protocol-paired QoS/mode/burst groups are retained below. Each row contains two cells, one per protocol.

| QoS / delivery / burst | Rate change | CPU/message change | Python peak change | Delivery p99 change |
| --- | ---: | ---: | ---: | ---: |
| qos=0, mode=iterator, burst=1 | -5.08% | +5.02% | +8.17% | +1.71% |
| qos=0, mode=iterator, burst=long | +1.49% | -1.47% | -0.27% | -2.39% |
| qos=0, mode=callback, burst=1 | -4.09% | +4.23% | +6.43% | +6.76% |
| qos=0, mode=callback, burst=long | +7.08% | -6.62% | -1.30% | +2.36% |
| qos=1, mode=iterator, burst=1 | -7.21% | +7.72% | +6.47% | +2.81% |
| qos=1, mode=iterator, burst=long | -1.96% | +2.00% | +19.70% | +18.46% |
| qos=1, mode=callback, burst=1 | -5.22% | +5.63% | +5.65% | +7.05% |
| qos=1, mode=callback, burst=long | +0.24% | -0.24% | +15.31% | +16.17% |

Evidence: contract (`stages/contract/summary/summary.md`). The QoS1 long-lot p99 and traced-peak increases remain explicit residual costs; the QoS0 long callback gain does not erase the short-burst regressions.
 
## Progressive prefix selection

The full prototype admitted up to 64 immediately ready QoS0/QoS1 items, with independent commitment, completion ownership before wire exposure and individual existing-writer handoff. Its extra QoS1 path added immediate-capacity checks, prepared admission and a per-prefix latency flush. It did not collect a public atomic chunk or park an uncharged payload prefix.

The first comparison recovered QoS0 throughput but did not establish a QoS1 throughput advantage: the publication-only QoS0 batch diagnostic improved **6.78%** (AB cycle ratios 1.059–1.083; AA 1.011–1.025), while QoS1 batch changed **−1.74%** with CPU/message **+1.76%**. The latter overlaps its observed AA rate range, 0.950–1.005. In network long lots the full prototype improved QoS0 rate **3.75% iterator / 4.28% callback**. QoS1 rate changed **+1.60% / +0.49%**, with lower traced peaks **−9.44% / −5.85%**.

The selected simplification retains only the QoS0 ready driver. Each item reaches the existing writer before the iterator is advanced; capacity refusal or a different QoS returns to ordinary admission. QoS1/QoS2 retain their existing pending-receipt wait and publication settlement fence. The prefix limit 64 and outer cooperative boundary 256 remain. The implementation removes 47 net runtime lines relative to the full prototype, including its second QoS1 orchestration branch.

The direct full-versus-simplified comparison is:

| Workload | Rate change | CPU/message change | Python peak change | Interpretation |
| --- | ---: | ---: | ---: | --- |
| QoS0 batch diagnostic | -0.36% | +0.37% | Not measured | In-memory completion workload |
| QoS1 batch diagnostic | +2.39% | -2.33% | Not measured | In-memory completion workload |
| QoS0 long network, iterator | +1.25% | -1.23% | +0.00% | Both protocols |
| QoS0 long network, callback | +1.13% | -1.11% | +0.09% | Both protocols |
| QoS1 long network, iterator | -1.84% | +1.86% | +12.59% | Both protocols |
| QoS1 long network, callback | -1.03% | +1.05% | +4.75% | Both protocols |

QoS0 amortization is retained without a material loss in this direct comparison. The simplification has a real QoS1 tradeoff: roughly 1–2% lower network long-lot rate and higher traced peak. Grouped AB arm medians rise from 41,295 to 47,462 bytes in iterator mode and 43,778 to 47,098 bytes in callback mode, about 6.0 and 3.2 KiB. Those median differences and the geometric cycle ratios use different aggregations. QoS1 median delivery latency also increases approximately 1.9% / 1.4%; p95/p99 remain mixed. The selected implementation is simpler, not uniformly faster or smaller in memory.

Across all 16 cells, full-versus-simplified rate is +0.01%, CPU −0.02%, Python peak +2.05% and RSS −0.33%. The unchanged individual-publication and receive/routing controls vary as well. The initial full prototype remains retained evidence rather than a hidden discarded result.

Evidence: batching (`stages/batching/summary/summary.md`), batching-diagnostics (`stages/batching-diagnostics/summary/summary.md`), batching-qos0-only (`stages/batching-qos0-only/summary/summary.md`), batching-qos0-only-diagnostics (`stages/batching-qos0-only-diagnostics/summary/summary.md`).

## Callback quantum: 128 selected; 256 retained as measured evidence

Both candidates were compared directly with quantum 64 on the same QoS0-only source and frozen harness, in separate serial measurement windows. There was no direct 128-versus-256 campaign. The selection is **128**, following the predeclared criterion of retaining the smallest justified quantum; the coordinator confirmed this decision after reviewing both completed audits. Quantum 128 already improves callback/routing throughput and QoS0 long-lot callback memory/latency. Quantum 256 has stronger measured gains, described below, but doubles the invocation interval again without a measurement of unrelated-task scheduling delay. This is a deliberate fairness-budget tradeoff, not a finding that 256 is ineffective.

The network 16-cell aggregates are rate **−0.61%**, CPU **+0.56%**, Python peak **−3.10%**, p99 **−0.48%** for 128; and rate **+0.52%**, CPU **−0.55%**, Python peak **−4.15%**, p99 **−5.39%** for 256. Aggregate rate AA/AB ranges are 0.989–0.997 / 0.992–0.996 for 128 and 1.002–1.007 / 1.000–1.008 for 256. Neither aggregate throughput ratio establishes a universal improvement.

Callback targets and unchanged diagnostic controls are all retained below. Each rate/CPU pair is an AB percentage change; ranges are AA / AB rate ratios.

| Diagnostic | 128 rate / CPU | 128 AA / AB range | 256 rate / CPU | 256 AA / AB range |
| --- | ---: | --- | ---: | --- |
| publish | -1.47% / +1.49% | 1.012–1.026 / 0.980–0.994 | -0.93% / +0.93% | 0.952–1.009 / 0.976–1.004 |
| publish_qos1_individual | +0.68% / -0.67% | 0.979–0.995 / 0.998–1.015 | +2.65% / -2.58% | 0.988–1.037 / 1.010–1.042 |
| publish_qos1_batch | +2.48% / -2.42% | 0.991–1.022 / 1.013–1.041 | -0.75% / +0.76% | 0.988–1.034 / 0.981–0.999 |
| receive_iterator | -0.41% / +0.41% | 0.991–1.016 / 0.981–1.018 | -0.51% / +0.50% | 0.994–1.025 / 0.987–1.011 |
| receive_callback | +1.78% / -1.75% | 0.991–0.995 / 1.002–1.032 | +1.47% / -1.44% | 1.002–1.007 / 0.999–1.029 |
| callback_only | +3.38% / -3.26% | 0.973–1.011 / 1.020–1.055 | +6.59% / -6.19% | 1.012–1.021 / 1.050–1.083 |
| route_exact | +4.13% / -3.96% | 0.994–0.996 / 1.015–1.073 | +7.23% / -6.74% | 0.979–1.057 / 1.048–1.095 |
| route_overlap | +6.09% / -5.74% | 0.988–1.019 / 1.047–1.083 | +9.93% / -9.02% | 0.984–1.030 / 1.071–1.124 |
| route_fallback | +3.47% / -3.35% | 1.015–1.015 / 1.024–1.045 | +6.02% / -5.68% | 0.969–1.014 / 1.023–1.114 |
| route_error | +3.82% / -3.68% | 0.948–0.997 / 1.017–1.053 | +6.84% / -6.40% | 0.987–1.005 / 1.043–1.094 |

At 128, callback-only and routed diagnostic gains of roughly 3–6% exceed their observed AA ranges, with lower CPU/operation. At 256 those gains are roughly 6–10%. Receive-callback gains are smaller and overlap between the two windows. Publication-only and iterator diagnostics are unchanged-path controls: their movement, including the QoS0 publication decrease in both windows and the opposite QoS1 batch changes, limits attribution of small differences. A larger message-worker quantum is not claimed to accelerate those paths.

The complete network callback groups, each combining both protocols, are:

| Quantum | QoS / callback burst | Rate change | CPU/message change | Python peak change | Delivery p99 change |
| ---: | --- | ---: | ---: | ---: | ---: |
| 128 | QoS0 / 1 | -2.23% | +1.99% | -0.72% | +4.96% |
| 256 | QoS0 / 1 | +0.83% | -0.90% | +0.72% | -2.40% |
| 128 | QoS0 / long | +0.17% | -0.17% | -17.96% | -12.49% |
| 256 | QoS0 / long | +3.73% | -3.60% | -30.60% | -28.04% |
| 128 | QoS1 / 1 | -0.26% | +0.45% | +0.08% | -0.82% |
| 256 | QoS1 / 1 | +0.25% | -0.17% | +0.05% | +0.50% |
| 128 | QoS1 / long | -1.08% | +1.12% | -2.28% | +1.07% |
| 256 | QoS1 / long | -0.10% | +0.13% | -1.41% | -2.52% |

The QoS0 long callback case is the clearest gain. At 128 its rate is approximately unchanged, with Python peak **−17.96%**, p50 **−17.97%**, p95 **−12.75%** and p99 **−12.49%**. Its rate AA range is 0.961–0.963 versus AB 0.992–1.012; notably, the MQTT 5 AA cell itself drifted down 5.8–7.9%, so the small grouped throughput difference is not treated as a stable improvement. The memory/percentile reductions appear in both protocols beyond their observed AA ranges.

At 256 the same grouped workload gains rate **+3.73%**, CPU **−3.60%**, Python peak **−30.60%**, p50 **−38.86%**, p95 **−29.42%** and p99 **−28.04%**. Rate AB 1.026–1.051 exceeds AA 0.998–1.008. This is also true separately by protocol: rate **+1.72%** on MQTT 3.1.1 (AB 1.011–1.029, AA 0.986–0.994) and **+5.79%** on MQTT 5 (AB 1.042–1.075, AA 1.001–1.030). This supports a real targeted benefit from 256 against 64; it does not constitute a direct incremental comparison against 128.

Costs and controls remain visible. At 128, the QoS0 short callback burst records rate **−2.23%**, p95 **+6.83%** and p99 **+4.96%**; its rate AB 0.968–0.986 overlaps AA 0.971–0.992. QoS1 callback rate/latency differences are mixed. Iterator network rate is **−0.37%** at 128 and **−0.12%** at 256, on a path unaffected by the quantum; these observations are controls, not quantum gains. RSS is broadly unchanged (all-cell **−0.06% / −0.14%**) despite the larger traced-memory reductions in the callback target.

Quantum 128 doubles the maximum callback invocations between cooperative yield opportunities relative to 64; 256 quadruples it. The count includes route fan-out, and no value preempts an individual synchronous callback. These campaigns measure application-delivery latency, not unrelated-task loop lag or a wall-clock fairness bound. Thus the selected 128 retains demonstrated benefits with the smaller increased invocation budget. The final integrated source has its own correctness and final96 evidence below; final diagnostics and paced outcomes are reported separately below. No universal latency claim follows from this selection.

Evidence: quantum128 (`stages/quantum128/summary/summary.md`), quantum128-diagnostics (`stages/quantum128-diagnostics/summary/summary.md`), quantum256 (`stages/quantum256/summary/summary.md`), quantum256-diagnostics (`stages/quantum256-diagnostics/summary/summary.md`). The decision record (`quantum-decision.md`) pins the selected value and observed tradeoffs. No further quantum-selection campaign is planned.

## Final saturated network comparison with RC14

The completed `final-portable` stage directly compares qualified runtime **`21684c9`** with RC14 **`c194597`**, using frozen harness **`7eaed838`**: **96 cells and 1,920 comparison observations**, all independently audited. The grid includes both protocols, all three QoS levels, memory/SQLite, iterator/synchronous-callback delivery and bursts 1/2/8/long. These are equal-cell descriptive aggregates, not estimates of a particular production workload mix. Final diagnostics and the retained paced scenario failure are reported separately below.

The aggregate records delivered rate **−8.80%**, worker CPU/message **+9.09%**, traced Python peak **−36.95%**, and RSS **−3.12%**. Application creation-to-delivery p50/p95/p99 change **−32.84% / −30.31% / −23.94%**. Those latency aggregates are driven by long QoS1/2 lots and do not describe short-burst latency or ACK RTT. Grouped AB-arm medians are 46,929 → 29,557 bytes of traced peak, and 32,810 → 31,795 KiB of RSS; these absolute values use a different aggregation from the cycle ratios.

| Metric | AB change | AA cycle-ratio range | AB cycle-ratio range |
| --- | ---: | --- | --- |
| Delivered rate | -8.80% | 0.997–1.006 | 0.912–0.912 |
| Worker CPU/message | +9.09% | 0.998–1.001 | 1.090–1.092 |
| Application round-trip p50 | -32.84% | 0.998–1.000 | 0.671–0.672 |
| Application round-trip p95 | -30.31% | 0.992–1.006 | 0.694–0.701 |
| Application round-trip p99 | -23.94% | 0.969–0.997 | 0.753–0.770 |
| Separate-phase Python traced peak | -36.95% | 0.964–1.014 | 0.620–0.639 |
| Worker RSS high-water | -3.12% | 1.000–1.000 | 0.969–0.969 |

The rate and CPU regressions are well outside the observed aggregate AA ranges. The grouped QoS2 rate difference, however, is only **+0.76%**: AB 1.003–1.013 overlaps AA 1.008–1.017, so it does not establish a general QoS2 speedup. Percentile and allocation variation can be much wider in individual SQLite cells than these aggregate ranges suggest. No confidence interval or significance claim is inferred.

| Group | Cells | Rate change | CPU/message change | Traced peak change | RSS change | Application p99 change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| QoS0 | 32 | -16.67% | +19.61% | +4.62% | -3.21% | +11.08% |
| QoS1 | 32 | -9.65% | +9.78% | -33.37% | -3.08% | -33.40% |
| QoS2 | 32 | +0.76% | -1.13% | -64.05% | -3.07% | -40.54% |
| Iterator | 48 | -6.72% | +6.78% | -37.64% | -3.15% | -28.33% |
| Sync callback | 48 | -10.83% | +11.45% | -36.25% | -3.08% | -19.28% |
| Memory store | 48 | -8.34% | +9.05% | -38.67% | -3.11% | -32.78% |
| SQLite store | 48 | -9.25% | +9.13% | -35.18% | -3.13% | -13.95% |
| Burst 1 | 24 | -5.38% | +5.19% | -0.58% | -2.96% | +7.93% |
| Burst 2 | 24 | -3.40% | +3.52% | -23.11% | -2.99% | +10.40% |
| Burst 8 | 24 | -1.84% | +2.23% | -20.66% | -2.94% | +11.13% |
| Long lot | 24 | -22.88% | +27.22% | -73.94% | -3.58% | -74.73% |

Both protocols retain the throughput cost: MQTT 3.1.1 rate **−8.37%**, CPU **+8.40%**; MQTT 5 rate **−9.22%**, CPU **+9.79%**. Both storage backends and both delivery modes are slower in aggregate. The full QoS/mode/burst breakdown below includes every combination; each row combines four cells, covering both protocols and stores.

| QoS / delivery / burst | Rate | CPU/message | Application p50 | Application p95 | Application p99 | Traced peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| QoS0 / iterator / 1 | -3.28% | +2.46% | +4.69% | +4.35% | +5.82% | +13.54% |
| QoS0 / iterator / 2 | -3.85% | +3.96% | +4.18% | +10.11% | +8.34% | +10.35% |
| QoS0 / iterator / 8 | -2.32% | +2.37% | +3.88% | +2.08% | +4.14% | +8.25% |
| QoS0 / iterator / long | -27.75% | +38.41% | -28.55% | -24.93% | -23.29% | -9.80% |
| QoS0 / callback / 1 | -20.00% | +23.22% | +34.86% | +28.45% | +25.78% | +10.87% |
| QoS0 / callback / 2 | -13.73% | +15.64% | +24.52% | +26.09% | +23.19% | +0.08% |
| QoS0 / callback / 8 | -10.31% | +11.49% | +17.16% | +18.72% | +20.24% | -5.10% |
| QoS0 / callback / long | -42.76% | +74.71% | +28.08% | +35.73% | +35.88% | +11.43% |
| QoS1 / iterator / 1 | -6.53% | +7.04% | +6.78% | +6.25% | +6.00% | +8.44% |
| QoS1 / iterator / 2 | -3.91% | +3.90% | +4.46% | +3.21% | +4.32% | +9.21% |
| QoS1 / iterator / 8 | -3.96% | +4.38% | +5.12% | +4.70% | +4.59% | +4.10% |
| QoS1 / iterator / long | -23.30% | +26.11% | -92.93% | -93.54% | -84.24% | -84.61% |
| QoS1 / callback / 1 | -7.33% | +7.43% | +15.54% | +11.92% | +10.55% | +12.79% |
| QoS1 / callback / 2 | -4.71% | +5.24% | +22.24% | +22.19% | +18.49% | +13.62% |
| QoS1 / callback / 8 | -3.30% | +3.68% | +14.80% | +13.24% | +14.03% | +6.14% |
| QoS1 / callback / long | -21.42% | +22.96% | -92.99% | -93.57% | -85.77% | -84.94% |
| QoS2 / iterator / 1 | +2.29% | -2.77% | +10.34% | +1.13% | -0.67% | -32.24% |
| QoS2 / iterator / 2 | +3.69% | -3.28% | +0.49% | -0.04% | +0.48% | -59.82% |
| QoS2 / iterator / 8 | +4.49% | -2.53% | +1.77% | +3.05% | +5.20% | -53.15% |
| QoS2 / iterator / long | -9.81% | +8.51% | -94.39% | -90.00% | -89.53% | -88.32% |
| QoS2 / callback / 1 | +4.67% | -4.02% | +7.98% | +4.98% | +2.03% | -7.42% |
| QoS2 / callback / 2 | +3.15% | -3.20% | +0.63% | -0.84% | +9.26% | -62.48% |
| QoS2 / callback / 8 | +5.24% | -5.15% | -0.96% | -2.54% | +19.85% | -53.10% |
| QoS2 / callback / long | -6.43% | +4.21% | -94.60% | -90.39% | -89.36% | -88.50% |

The most costly workload is QoS0 callback long lots: rate **−42.76%**, CPU/message **+74.71%**, application p99 **+35.88%**, and traced peak **+11.43%**. Its rate AB range 0.571–0.576 is far below AA 0.999–1.012; its p99 AB 1.334–1.377 is above AA 0.987–1.003. QoS0 callback bursts 1/2/8 also regress in rate and all measured delivery percentiles. QoS0 iterator long lots lose **27.75%** rate despite lower creation-to-delivery percentiles. QoS1 long lots lose **23.30% iterator / 21.42% callback** rate; QoS2 long lots lose **9.81% / 6.43%**. No long-lot throughput improvement is claimed.

The large long-lot latency reductions require an explicit clock interpretation. RC14 materializes `list(islice(iterator, chunk_size=256))` before waiting for pending receipts; the harness timestamps every item as that generator is pulled. Progressive admission consumes the iterable closer to each item's capacity, so fewer already-timestamped items wait ahead of admission. The observed QoS1/2 long-lot p50 reductions of roughly **93–95%** therefore include changed generation/queue residence. They do not establish faster ACK RTT, broker/network transit, or whole-lot completion; the measured whole-lot rate is lower. The fixed-offered-load campaign uses planned external arrival clocks and exposes the separate QoS0 overload result below.

At short bursts, latency generally worsens: aggregate p50 rises **12.94% / 8.98% / 6.76%** at bursts 1/2/8, and p99 rises **7.93% / 10.40% / 11.13%**. SQLite tails are especially variable. For example, MQTT 3.1.1 QoS2 SQLite callback burst 8 has p99 **+77.14%**, with AA 0.363–0.919 and AB 1.016–2.572; MQTT 5 QoS2 SQLite callback burst 2 has **+49.06%**, with AA 1.035–1.062 and AB 1.087–2.219. These adverse cells remain included. The large ranges prevent a blanket tail-latency conclusion.

Memory is also mixed. Traced-peak savings are concentrated in long QoS1/2 lots (**about 85–89%**) and several QoS2 short-burst groups. Every QoS1 short-burst group and the QoS0 iterator short-burst groups has a higher traced peak. The largest cell increase is **+26.61%**, MQTT 3.1.1 QoS1 memory-store callback burst 2 (AA 0.998–1.002, AB 1.251–1.274). QoS0 traced peak rises **4.62%** overall. RSS improves in every QoS/mode/burst group by approximately **2.8–4.6%**; its distinct whole-process scope must remain visible beside the much larger traced-allocation savings.

Final96 runner records contain **96 eligible preflights**, each ending with three consecutive eligible readiness samples. Across their histories there are **296 readiness samples**, including **seven ineligible samples** before eventual admission to blocks 6, 40 and 80. The 96 postflights contain **93 eligible and three above the load limit**, all retained:

| Block | Workload | Postflight load/CPU | Limit | Disposition |
| ---: | --- | ---: | ---: | --- |
| 38 | MQTT 3.1.1 / QoS2 / memory / callback / burst 2 | 0.26556 | 0.25000 | Retained |
| 39 | MQTT 3.1.1 / QoS2 / memory / callback / burst 8 | 0.25208 | 0.25000 | Retained |
| 79 | MQTT 5 / QoS1 / sqlite / callback / burst 8 | 0.28320 | 0.25000 | Retained |

Final selected preflight CPU usage was 5.0–13.7%, temperature 58–76°C and load/CPU 0.085–0.245. Postflight CPU usage was 5.0–9.2% and temperature 65–74°C; only the three recorded load samples exceeded their limit. Eligible starts do not certify every instant of a measurement block. No cell is filtered and no new performance ratio is computed after excluding adverse postflights. The paced and diagnostic readiness/postflight records are reported separately below from these 96 saturated blocks. The elevated one-minute load samples include the campaign's own recent work; they are not evidence of an external workload.

Evidence: complete final96 summary and all cells (`stages/final-portable/summary/summary.md`), independent audit (`stages/final-portable/raw/audit.json`), preflight/postflight records (`stages/final-portable/raw/parts`). Raw SHA256: `eb8caf053a97b0639975229a3b61efb934d237cfafa12aecf1ccedee2551e871`.

## Final diagnostic comparison with RC14

All ten in-memory scenarios completed and passed the independent audit: **200 returned observations**, with the same source pair and frozen harness as final96. The table retains each scenario, including its observed AA rate range and all three AB cycle ratios' range. These packet-aware diagnostics measure operations and timed worker CPU, including the in-process broker/harness. They measure neither latency nor memory peak and must not be substituted for the network results.

| Scenario | Rate change | AA rate range | AB rate range | CPU/operation change |
| --- | ---: | --- | --- | ---: |
| QoS0 batch publication (`publish`) | −13.91% | 0.992–1.055 | 0.845–0.876 | +16.16% |
| QoS1 individual publication | −2.11% | 1.003–1.004 | 0.955–1.000 | +2.14% |
| QoS1 batch publication | −2.69% | 1.030–1.062 | 0.966–0.981 | +2.77% |
| Iterator reception | −17.17% | 0.910–1.070 | 0.786–0.870 | +20.72% |
| Callback reception | −60.22% | 1.017–1.025 | 0.374–0.414 | +151.38% |
| Callback worker only | +2.77% | 0.960–1.007 | 1.012–1.058 | −2.69% |
| Exact route | +25.69% | 0.977–0.982 | 1.212–1.299 | −20.44% |
| Overlapping routes | +9.02% | 0.983–0.984 | 1.061–1.135 | −8.28% |
| Fallback route | +12.22% | 0.986–1.044 | 1.108–1.132 | −10.89% |
| Route error handling | +11.78% | 0.984–0.986 | 1.078–1.149 | −10.54% |

The equal-cell descriptive aggregate is rate **−7.13%**, CPU/operation **+7.67%**. Different operations have different meanings, so this aggregate is not a product throughput score. The callback-reception deficit is much larger than the worker-only result: the generic owned receive pipeline remains expensive beside RC14's specialized receive path, even though sync invocation and route dispatch improve. QoS1 individual and batch diagnostics both show small negative changes in this final source pair; these measurements do not show a large additional final batch-only deficit. No borrowed decoder or reader-inline callback machinery was restored to chase the old receive number.

The ten diagnostic blocks each started after three consecutive eligible samples. Their readiness histories contain 118 samples, including 84 ineligible samples before eventual starts. Four postflights were eligible; six exceeded only the one-minute load/CPU limit: blocks 1/2/3/4/5/8 recorded 0.26508 / 0.32806 / 0.26379 / 0.26794 / 0.26587 / 0.25043, against 0.25. All observations remain included. This records the campaign’s own load and recovery periods without asserting an unrelated external cause.

Evidence: `stages/final-portable-diagnostics/summary/summary.md` and `stages/final-portable-diagnostics/raw/audit.json`. Raw SHA256: `28a67a29b0b4acb80cfac879d72f12750ba1e6434f52221515cfedb036dbc828`.

## Paced outcomes: retained QoS0 overload failure

The offered rate is frozen from the audited RC14 main/main long-lot calibration and applied unchanged to both sources at 50%, 75% and 90%. Each observation requests a nominal two seconds but is bounded to **1,024–20,000 messages**; this target is not a guaranteed observed duration. The external pacer runs on CPU 5 and the client worker on CPU 4. Worker CPU excludes broker and external-pacer CPU.

Block 6, MQTT 5 / QoS0 / memory / synchronous callback, failed at **90%** of its reference. The reference rate is **61,490.76 messages/s**, so the offered rate is **55,341.69 messages/s** and the 20,000-message planned span is approximately **0.361 seconds**. The failing position is the second B of AB cycle 1 (zero-based `cycle=1, position=2`). Its publishing and receipt-observation loops returned, then final application delivery did not reach the expected count within the harness's **120-second** timeout. A QoS0 publication receipt is not a broker/subscriber-delivery acknowledgement.

The owned broker log records `Outgoing messages are being dropped for client lean-paced-1339924.` for that worker. This establishes actual outgoing broker loss in the scenario. It does not recover the failed worker's exact delivered/missing count: that worker returned no result or clock file. The evidence does not justify blaming unrelated PC activity, deriving a failed-sample latency distribution, or silently treating the failed observation as a slow success.

The failed block preserves two complete cells (50% and 75%, each 8 AA + 12 AB observations) and **14 returned observations** in its incomplete 90% cell (8 AA + 6 AB). The timeout observation has no returned metrics; five later scheduled positions in that cell were not attempted. The original raw attempt, log, request and 54 returned clock files remain unchanged. Blocks 1–5 remain complete. The controller did not retry block 6; the only continuation is the two previously untouched QoS1 blocks 7/8, with original commands, manifest, references, source/harness hashes, owned broker and eligibility checks.

The two untouched QoS1 blocks then completed. The independent final outcome audit accounts for all **24 logical cases: 23 complete and one failed**. It validates **474 returned observations and clock files, containing 9,082,500 clock rows**; 460 observations belong to complete cells and 14 to the failed cell. One known worker failed and the five following positions were not attempted, reconciling the planned 480 slots. The partial failed cell has no comparison metric. The tail execution ledger records unchanged preserved inputs and retirement of its owned broker. No aggregate across the 23 survivors is substituted for the planned full grid.

Sample counts range from **14,544 to 20,000 messages**. Actual scheduled-arrival-to-final-delivery durations in complete cells span **0.462–2.290 seconds**; the following table includes each cell's full AA/AB observed duration range. It excludes the failed worker's timeout from successful-duration statistics. Ratios use the same three equal-weight ABBA cycles as the saturated stages. Full AA/AB distributions, arm medians and all four clock decompositions are retained in `stages/paced-portable/summary/outcomes.json`.


| MQTT / QoS / delivery / fraction | Offered msg/s | Observed duration range (s) | Rate change | CPU/msg change |
| --- | ---: | --- | ---: | ---: |
| 311 / 0 / iterator / 50% | 25,622 | 0.781–0.794 | +0.21% | -0.21% |
| 311 / 0 / iterator / 75% | 38,432 | 0.576–0.638 | -3.57% | +3.67% |
| 311 / 0 / iterator / 90% | 46,119 | 0.562–0.651 | -6.61% | +7.11% |
| 311 / 0 / callback / 50% | 29,281 | 0.683–0.694 | -0.44% | +0.21% |
| 311 / 0 / callback / 75% | 43,921 | 0.479–0.645 | -21.30% | +26.84% |
| 311 / 0 / callback / 90% | 52,705 | 0.462–0.626 | -22.91% | +29.59% |
| 311 / 1 / iterator / 50% | 7,932 | 2.000–2.001 | +0.00% | +0.08% |
| 311 / 1 / iterator / 75% | 11,898 | 1.682–1.703 | -0.32% | +0.36% |
| 311 / 1 / iterator / 90% | 14,277 | 1.570–1.683 | -1.05% | +1.11% |
| 311 / 1 / callback / 50% | 7,505 | 2.000–2.001 | -0.00% | -0.04% |
| 311 / 1 / callback / 75% | 11,257 | 1.777–1.786 | -0.08% | +0.10% |
| 311 / 1 / callback / 90% | 13,508 | 1.619–1.750 | +1.72% | -1.39% |
| 5 / 0 / iterator / 50% | 23,426 | 0.854–0.868 | -0.13% | +0.25% |
| 5 / 0 / iterator / 75% | 35,139 | 0.655–0.730 | -6.30% | +6.72% |
| 5 / 0 / iterator / 90% | 42,167 | 0.626–0.714 | -9.67% | +10.66% |
| 5 / 0 / callback / 50% | 30,745 | 0.651–0.779 | -14.19% | +16.39% |
| 5 / 0 / callback / 75% | 46,118 | 0.536–0.742 | -24.18% | +31.65% |
| 5 / 0 / callback / 90% | 55,342 | FAILED: 120 s timeout; no returned duration | — | — |
| 5 / 1 / iterator / 50% | 7,272 | 2.001–2.138 | +0.00% | +0.02% |
| 5 / 1 / iterator / 75% | 10,907 | 1.835–2.290 | -3.96% | +4.05% |
| 5 / 1 / iterator / 90% | 13,089 | 1.841–2.143 | -2.56% | +1.89% |
| 5 / 1 / callback / 50% | 7,353 | 2.001–2.002 | -0.02% | -0.07% |
| 5 / 1 / callback / 75% | 11,030 | 1.881–2.056 | -2.64% | +2.64% |
| 5 / 1 / callback / 90% | 13,236 | 1.881–2.290 | -3.57% | +3.22% |

| MQTT / QoS / delivery / fraction | Scheduled-delivery p50 change | p95 change | p99 change | AA p99 ratio range | AB p99 ratio range |
| --- | ---: | ---: | ---: | --- | --- |
| 311 / 0 / iterator / 50% | +9.97% | -23.99% | -14.59% | 0.691–2.805 | 0.555–1.093 |
| 311 / 0 / iterator / 75% | +15.54% | +12.46% | +11.68% | 0.829–1.064 | 1.063–1.162 |
| 311 / 0 / iterator / 90% | +17.32% | +6.91% | +6.26% | 0.985–1.240 | 1.028–1.107 |
| 311 / 0 / callback / 50% | +284.76% | +580.06% | +382.50% | 0.832–2.857 | 3.070–8.114 |
| 311 / 0 / callback / 75% | +199.06% | +119.30% | +113.25% | 1.023–1.394 | 1.722–2.963 |
| 311 / 0 / callback / 90% | +60.58% | +20.50% | +18.53% | 1.047–1.047 | 1.152–1.233 |
| 311 / 1 / iterator / 50% | +7.18% | +10.14% | +3.67% | 0.882–1.153 | 0.957–1.178 |
| 311 / 1 / iterator / 75% | +24.68% | +44.33% | +48.22% | 0.672–1.954 | 0.854–3.667 |
| 311 / 1 / iterator / 90% | +19.41% | +7.90% | +7.72% | 0.905–1.072 | 0.931–1.309 |
| 311 / 1 / callback / 50% | +15.78% | +10.97% | +14.29% | 0.898–0.915 | 1.112–1.201 |
| 311 / 1 / callback / 75% | -2.45% | -34.04% | -33.54% | 1.606–3.047 | 0.478–1.012 |
| 311 / 1 / callback / 90% | -20.93% | -13.53% | -13.38% | 0.948–1.251 | 0.769–1.041 |
| 5 / 0 / iterator / 50% | +199.42% | +149.32% | +109.40% | 0.575–1.984 | 0.793–5.063 |
| 5 / 0 / iterator / 75% | +55.26% | +27.35% | +24.73% | 0.966–1.245 | 1.148–1.316 |
| 5 / 0 / iterator / 90% | +30.08% | +16.78% | +15.42% | 1.024–1.108 | 1.140–1.161 |
| 5 / 0 / callback / 50% | +7439.22% | +5937.19% | +3697.29% | 1.266–1.847 | 27.467–46.364 |
| 5 / 0 / callback / 75% | +91.71% | +44.66% | +42.59% | 0.779–0.903 | 1.297–1.502 |
| 5 / 0 / callback / 90% | FAILED | — | — | — | — |
| 5 / 1 / iterator / 50% | +11.21% | +2.75% | +5.85% | 0.058–0.973 | 0.649–1.384 |
| 5 / 1 / iterator / 75% | +256.33% | +221.80% | +211.18% | 1.197–1.475 | 1.364–7.615 |
| 5 / 1 / iterator / 90% | +16.42% | +9.12% | +8.67% | 1.052–1.132 | 1.059–1.135 |
| 5 / 1 / callback / 50% | +17.69% | +21.66% | +34.68% | 0.800–1.311 | 1.010–1.658 |
| 5 / 1 / callback / 75% | +51.61% | +37.51% | +36.00% | 0.395–0.871 | 1.145–1.625 |
| 5 / 1 / callback / 90% | +16.95% | +12.85% | +12.67% | 1.052–1.329 | 1.051–1.219 |


The paced observations retain substantial adverse latency results. MQTT 5 QoS0 callback at 50% already has scheduled-to-delivery p50 **+7,439%** and p99 **+3,697%**; AB arm-median p50 moves from **1.50 ms to 114.27 ms**, and p99 from **4.89 ms to 211.06 ms**. These arm medians are not the operands of the equal-cycle percentage. Its p99 AB ratios are 27.47–46.36 against AA 1.27–1.85, so it cannot be dismissed merely as overlapping AA variation. At 75% this mode completes but loses **24.18%** effective delivered rate with **31.65%** higher worker CPU/message, before the separate 90% drop/timeout outcome. MQTT 3.1.1 QoS0 callbacks also show adverse tails and rate at elevated fractions.

QoS1 results are less uniform: several low-fraction rate/CPU cells are near parity, but MQTT 5 iterator at 75% has p99 **+211.18%**, with a wide AB ratio range **1.36–7.62**. Other cells improve, such as MQTT 3.1.1 QoS1 callback p99 at 75% (**−33.54%**), where AA varies **1.61–3.05** and AB **0.48–1.01**. These distributions and the ending conditions below do not support a universal latency benefit, a single ACK-RTT conclusion, or confidence-interval claims.

All eight paced blocks started after three consecutive eligible samples; their histories contain **73 readiness samples**, including **49 ineligible** before eventual starts. There are seven postflight records: one eligible and six adverse. The failed block 6 has no postflight record because the original controller stopped at its failed worker; that absence is preserved. The postflight table reports every adverse ending:

| Block | Workload | Ending load/CPU | Ending temperature | Disposition |
| ---: | --- | ---: | ---: | --- |
| 2 | MQTT 3.1.1 / QoS0 / callback | 0.28870 | 73°C | Retained |
| 3 | MQTT 3.1.1 / QoS1 / iterator | 0.35010 | 77°C | Retained |
| 4 | MQTT 3.1.1 / QoS1 / callback | 0.29272 | 76°C | Retained |
| 5 | MQTT 5 / QoS0 / iterator | 0.28912 | 74°C | Retained |
| 7 | MQTT 5 / QoS1 / iterator | 0.49426 | 81°C | Retained; load and temperature exceed limits |
| 8 | MQTT 5 / QoS1 / callback | 0.43372 | 85°C | Retained; load and temperature exceed limits |

The recorded limits are load/CPU 0.25 and temperature 80°C. Block 8 waited through **38 readiness samples** before its eligible start, but still ended at 85°C. Those hot endings are a material limitation for interpreting the final QoS1 cells, including their unstable tails; eligible starts do not certify stable conditions throughout a block. One-minute load includes the campaign's own worker, pacer, broker and their recent work. The user reports an idle PC, and there is no evidence assigning these readings to an unrelated external workload. No hot/adverse result is filtered out or rerun until favorable.

Evidence: `stages/paced-portable/raw/outcome-audit.json`, SHA256 **`7b362a9bc9529e27d9aecd4937294d317ede5e487a5f8249435f8f009ee12a6d`**; `stages/paced-portable/summary/outcomes.json`; `stages/paced-portable/raw/tail-execution-20260914T060421221614Z.json`. The bounded arithmetic companion `development/summarize_paced_outcomes.py` requires the matching outcome audit and reuses `summarize.py` per-cycle arithmetic without any global ratio.

Evidence: `stages/paced-portable/raw/parts/006-20260914T053807929782Z.json`, its `.json.request.json` and `.log`, `development/run_paced_remainder.py`, and the separate `development/audit_paced_outcomes.py` outcome validator. Failed raw SHA256: `fd7ef51da8fd901811b1cd33650a5e45d05ab957dc9bbf1054cd01ccbf93946a`. The original partial `raw/results.json` is not promoted to a completed stage.

## Integration and qualification boundary

Reconciliation with `main@c194597` is complete in `b530506e`. The first-parent merge diff contains only the RC14 version and historical changelog metadata; runtime design, source documentation and tests retain the lean branch after explicit review of upstream routing changes. The merge does not restore live route mutation or sync-to-async routing machinery. Five upstream tests for obsolete routing/callback semantics were omitted. The applicable replacement-connection outcome is covered by `test_application_reconnect_keeps_fresh_callbacks_and_retires_old_delivery`: normal/eager scheduling × both protocols × QoS0/1/2, or 12 variants. See the merge record (`main-reconciliation.md`).

Local qualification of `b530506e` completed all 15 checks: 1,884 unit/project tests, 13 integration tests and 126 resilience/fuzz tests passed, with zero integration skips; the remaining quality, typing/security, codec/runtime/composition/pressure fuzz and strict-documentation checks also passed. Coverage XML records 6,913/7,327 covered lines and 2,205/2,576 branches: **94.35% lines**, **85.60% branches**, or **92.07% combined covered opportunities**. The owned broker was retired. Qualification result (`qualification/reconciled-runtime/20260914T035224816591Z/result.json`).

Local soak on the same commit passed both protocols at 20 cycles × 500 messages, with two warmup cycles and forced reconnect each measured cycle. The result records unchanged source and broker retirement. Soak result (`qualification/soak/20260914T035528851037Z/result.json`). These successful local results remain attached to `b530506e`; they are not automatically attributed to the successor.

Hosted [CI 34804338500](https://github.com/yoch/mqttium/actions/runs/34804338500) failed on Windows Python 3.11 and 3.14: six diagnostic-test fixture imports per job raised `ModuleNotFoundError: No module named resource` through `lean_native_compare.py:22`. Other blocking jobs succeeded, but the required gate failed. The explicit [soak 34804336633](https://github.com/yoch/mqttium/actions/runs/34804336633) succeeded on all six applicable Linux/macOS/protocol and broker-interoperability jobs; its scheduled-only resilience job was skipped. Actual checkout logs verify `b530506e`, while CI artifact labels contain the synthetic merge SHA. Hosted evidence and failure record (`hosted/runtime/README.md`).

The first final96 attempt was paused after two complete cells / 40 observations, before the next measurement block, to fix this diagnostic portability failure. The partial attempt is retained without a final-performance claim. The eight-cell, 160-observation main/main reference completed and passed its independent audit (raw SHA256 `2ab0825318ca6045a2f2f102840995037fcdb09ab592072ef5943a287b7eb81e`). It remains reusable with the same main source and frozen harness. The `21684c9e` successor then passed its own 15-check local qualification: **1,885 unit/project**, **13 integration** and **126 resilience/fuzz** tests, with the same **92.07% combined coverage**. Its separate two-protocol 20 × 500 soak records unchanged source and retired broker. Successor qualification (`qualification/portable-runtime/20260914T040719890070Z/result.json`), successor local soak (`qualification/soak/20260914T041023135449Z/result.json`).

Hosted [CI 34804900490](https://github.com/yoch/mqttium/actions/runs/34804900490) succeeded on all 14 jobs, including both Windows versions and the required gate. The explicit [soak 34804898029](https://github.com/yoch/mqttium/actions/runs/34804898029) succeeded on all six applicable jobs; schedule-only resilience was skipped. The fetched Windows and Linux soak checkout logs identify exact `21684c9e`. Windows 3.11 reports 1,814 passed / 80 skipped; 3.14 reports 1,891 passed / 3 skipped, with no setup errors. Successor hosted evidence (`hosted/portable-runtime/README.md`). The failed `b530506e` run remains preserved rather than relabeled as successful.

| Required completion | Status and required evidence |
| --- | --- |
| Reconcile current main | **Complete.** `b530506e` has `c194597` as its second parent; source/tree identities and reviewed exclusions are recorded above |
| Final local correctness | **Complete for `21684c9e`.** All 15 checks passed; exact source and result links are recorded above |
| Final local soak | **Complete for `21684c9e`.** Both protocols at 20 × 500 passed; source unchanged and owned broker retired |
| Final96 versus current main | **Complete.** `final-portable` independently audited all 96 cells / 1,920 observations; regressions and runner conditions are recorded above |
| Paced reference8 and paced outcomes | **Outcome audit complete.** 23 complete cells, one failed; 474 returned observations / 9,082,500 clock rows verified. Broker drops, timeout, hot endings and five unattempted positions retained. No global paced comparison |
| Final diagnostics | **Complete.** All 10 cells / 200 observations audited against RC14; all scenarios are reported above |
| Hosted runtime qualification | **Complete for `21684c9e`.** All 14 CI and six applicable explicit-soak jobs passed. Failed predecessor evidence is retained; runtime check metadata and fetched checkout evidence are retained for archive delivery |
| Later documentation head | **Outside this report's qualification claim.** Its own checks and exact checkout/runtime/test identities are recorded externally in `qualification/delivery.json` |
| Archive delivery | Archive source/Git history, raw blocks/clocks including failures, manifests/audits, helpers and qualification records; verify member hashes and write the archive checksum to adjacent `.tar.gz.sha256` and `.verification.json` files outside the archive |

The report must not declare a green final head before these checks exist. Workflow artifact labels based on `github.sha` can identify a synthetic PR merge even when checkout explicitly selects the PR head; actual checkout logs and the source record control the qualification claim. No tag, GitHub release, package publication or merge approval is implied by these experiments.

## Audited inputs and preservation

Completed full-stage evidence included in this report totals 234 stage cells and 4,680 comparison observations, including the eight main/main calibration cells and ten final diagnostics. This counts distinct stage observations, not 234 distinct product workloads. Paced outcomes add 23 complete cells (460 observations), plus 14 returned observations in one failed cell: 474 returned observations separately audited, without a global paced performance ratio. Thus all completed stages and returned paced observations total 5,154 observations; the one failed worker and five unattempted positions remain separate. The interrupted final96 attempt is excluded from these completed-evidence totals. The table pins each raw input; complete AA/AB values, all cells, absolute values and source/test/harness identities remain in the named summaries and adjacent manifests.

| Stage | Cells / observations | Raw SHA256 |
| --- | ---: | --- |
| contract (`stages/contract/summary/summary.md`) | 16 / 320 | `14c784770bcf5946767b22da556948dd4fba52a818f9530250171996b87b614f` |
| batching (`stages/batching/summary/summary.md`) | 16 / 320 | `4d79e3fb9fd5252ce3e06141f128a205e83cfa548de494f187480a4b227c14d8` |
| batching-diagnostics (`stages/batching-diagnostics/summary/summary.md`) | 10 / 200 | `fb7d5f010b248ec8c458a20c0d68883a84e428493f705491eb2fce017d880f47` |
| batching-qos0-only (`stages/batching-qos0-only/summary/summary.md`) | 16 / 320 | `b7c50bff7707a7b5f93c66e98ce836ab66070af320da85aea2f1c7a5f4677b91` |
| batching-qos0-only-diagnostics (`stages/batching-qos0-only-diagnostics/summary/summary.md`) | 10 / 200 | `c7131220e8a38c6f04c14af398b5b6084d8c3875a1b43712c286461c51d1b8c2` |
| quantum128 (`stages/quantum128/summary/summary.md`) | 16 / 320 | `e499a1c15fe57993a3ae4cd10e819bbaddf3274897aa366711558393b3e259e7` |
| quantum128-diagnostics (`stages/quantum128-diagnostics/summary/summary.md`) | 10 / 200 | `6c2f59d26ad382d75463963e2ded2126e906c78c9dfe86de1ecc329abfe56680` |
| quantum256 (`stages/quantum256/summary/summary.md`) | 16 / 320 | `0e92b0de0700f26705998842c2e9689d4641b3429c9e08efba7d26b830266b9c` |
| quantum256-diagnostics (`stages/quantum256-diagnostics/summary/summary.md`) | 10 / 200 | `770bed2aeb0a315d6b137b57a4b37f3787e31846273a197d9ec42b611eb4790d` |
| main-reference audit (`stages/main-reference/raw/audit.json`) | 8 / 160 | `2ab0825318ca6045a2f2f102840995037fcdb09ab592072ef5943a287b7eb81e` |
| final-portable (`stages/final-portable/summary/summary.md`) | 96 / 1,920 | `eb8caf053a97b0639975229a3b61efb934d237cfafa12aecf1ccedee2551e871` |
| `stages/final-portable-diagnostics/summary/summary.md` | 10 / 200 | `28a67a29b0b4acb80cfac879d72f12750ba1e6434f52221515cfedb036dbc828` |

This report is new dated evidence, superseding the September 13 report without rewriting its historical body. Generated benchmark data, caches and archives stay outside Git. The archive checksum belongs in adjacent `.tar.gz.sha256` and `.verification.json` files outside the archive. Included `qualification/delivery.json` records only the later documentation source/trees and its own check identities and links; it must not contain a checksum of the archive that includes it.
