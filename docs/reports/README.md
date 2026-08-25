# Historical evidence

This directory preserves dated measurements, audits, experiments, and release
campaigns. A report describes the commit and environment it names; it is not a
current API contract and its body is never rewritten to match later code.

Use current guides and contracts for supported behaviour. Use this index to
understand why a decision was made and whether later evidence superseded it.
Every tracked report body appears exactly once below.

Status meanings:

- **Current evidence** — still useful evidence for an implemented decision or
  an open, explicitly bounded experiment; it does not become a product promise.
- **Superseded** — retained for chronology; use the named newer report or
  current contract instead.
- **Retracted** — a method or conclusion is unsuitable for current citation.

## Release and quality campaigns

| Report | Status | Use instead or interpretation |
| --- | --- | --- |
| [STABLE-RELEASE-EVIDENCE-2026-08-05](https://github.com/yoch/mqttium/blob/main/docs/reports/STABLE-RELEASE-EVIDENCE-2026-08-05.md) | Superseded | Early campaign evidence; require a final-release report for the exact release commit |
| [RELEASE-CANDIDATE-1.0.0rc1](https://github.com/yoch/mqttium/blob/main/docs/reports/RELEASE-CANDIDATE-1.0.0rc1.md) | Superseded | Later release candidates and final release evidence |
| [RELEASE-CANDIDATE-1.0.0rc3](https://github.com/yoch/mqttium/blob/main/docs/reports/RELEASE-CANDIDATE-1.0.0rc3.md) | Superseded | Later release candidates and final release evidence |
| [RELEASE-CANDIDATE-1.0.0rc4](https://github.com/yoch/mqttium/blob/main/docs/reports/RELEASE-CANDIDATE-1.0.0rc4.md) | Superseded | Later release candidates and final release evidence |
| [RELEASE-CANDIDATE-1.0.0rc5](https://github.com/yoch/mqttium/blob/main/docs/reports/RELEASE-CANDIDATE-1.0.0rc5.md) | Superseded | Later release candidates and final release evidence |
| [RELEASE-CANDIDATE-1.0.0rc6](https://github.com/yoch/mqttium/blob/main/docs/reports/RELEASE-CANDIDATE-1.0.0rc6.md) | Superseded | Final release evidence for the exact release commit |
| [QUALITY-AUDIT-0.2.0b4](https://github.com/yoch/mqttium/blob/main/docs/reports/QUALITY-AUDIT-0.2.0b4.md) | Superseded | Later engine-quality audits and current quality gates |
| [QUALITY-AUDIT-1.0.0rc2](https://github.com/yoch/mqttium/blob/main/docs/reports/QUALITY-AUDIT-1.0.0rc2.md) | Superseded | Later engine-quality audits and current quality gates |
| [ENGINE-QUALITY-AUDIT-2026-08-17](https://github.com/yoch/mqttium/blob/main/docs/reports/ENGINE-QUALITY-AUDIT-2026-08-17.md) | Superseded | Closure and independent review below |
| [ENGINE-QUALITY-AUDIT-INDEPENDENT-REVIEW-2026-08-17](https://github.com/yoch/mqttium/blob/main/docs/reports/ENGINE-QUALITY-AUDIT-INDEPENDENT-REVIEW-2026-08-17.md) | Current evidence | Independent review of the same bounded audit |
| [ENGINE-QUALITY-AUDIT-CLOSURE-2026-08-17](https://github.com/yoch/mqttium/blob/main/docs/reports/ENGINE-QUALITY-AUDIT-CLOSURE-2026-08-17.md) | Current evidence | Closure record for the engine-quality findings |
| [ISSUE-253-ARM64-VALIDATION-2026-08-17](https://github.com/yoch/mqttium/blob/main/docs/reports/ISSUE-253-ARM64-VALIDATION-2026-08-17.md) | Current evidence | ARM64 validation for the issue and source it names |
| [SIMPLIFICATION-AUDIT-2026-08-16](https://github.com/yoch/mqttium/blob/main/docs/reports/SIMPLIFICATION-AUDIT-2026-08-16.md) | Current evidence | Reviewed simplification decisions at the named commit |
| [SIMPLIFICATION-ARM64-REVALIDATION-2026-08-17](https://github.com/yoch/mqttium/blob/main/docs/reports/SIMPLIFICATION-ARM64-REVALIDATION-2026-08-17.md) | Current evidence | Independent-architecture revalidation of that campaign |
| [ENGINE-SIMPLIFICATION-2026-08-25](https://github.com/yoch/mqttium/blob/main/docs/reports/ENGINE-SIMPLIFICATION-2026-08-25.md) | Current evidence | Engine-wide structural dedup, the measured merge of the batch-delivery copies deferred in 2026-08-16, and the replay-parked settlement defect |
| [PRE-REFACTOR-FUZZ-PR-REEVALUATION-2026-08-24](https://github.com/yoch/mqttium/blob/main/docs/reports/PRE-REFACTOR-FUZZ-PR-REEVALUATION-2026-08-24.md) | Current evidence | Reproduction and disposition of draft PRs #372–#376 against the post-consolidation baseline |
| [RUNTIME-FUZZER-KEEPALIVE-FINDING-2026-08-24](https://github.com/yoch/mqttium/blob/main/docs/reports/RUNTIME-FUZZER-KEEPALIVE-FINDING-2026-08-24.md) | Current evidence | Runtime schedule reproduction, ownership analysis, and correction of the terminal-EOF keepalive task leak |
| [RUNTIME-FUZZER-GENERATIVE-QUALIFICATION-2026-08-24](https://github.com/yoch/mqttium/blob/main/docs/reports/RUNTIME-FUZZER-GENERATIVE-QUALIFICATION-2026-08-24.md) | Current evidence | State-aware runtime schedule generation, six-mutant qualification, keepalive-cycle validation, explicit-takeover finding, and 2,000-seed reference campaign |
| [RUNTIME-FUZZER-COMPOSITION-QUALIFICATION-2026-08-24](https://github.com/yoch/mqttium/blob/main/docs/reports/RUNTIME-FUZZER-COMPOSITION-QUALIFICATION-2026-08-24.md) | Current evidence | Two-window lifecycle composition, four behavioral-mutant qualifications, closing-transport takeover finding, and 50,000-seed decision-gate campaign |

## Cross-client and performance program

| Report | Status | Use instead or interpretation |
| --- | --- | --- |
| [CROSS-CLIENT-BENCHMARK-2026-08-09](https://github.com/yoch/mqttium/blob/main/docs/reports/CROSS-CLIENT-BENCHMARK-2026-08-09.md) | Retracted | Do not cite its latency column or old-version rankings as current; await reviewed independent results |
| [PERFORMANCE-AUDIT-0.2.0b4](https://github.com/yoch/mqttium/blob/main/docs/reports/PERFORMANCE-AUDIT-0.2.0b4.md) | Superseded | Later native hot-path evidence and the current benchmarking contract |
| [PERFORMANCE-1.0.0rc1](https://github.com/yoch/mqttium/blob/main/docs/reports/PERFORMANCE-1.0.0rc1.md) | Superseded | Later per-decision reports; not a current cross-client claim |
| [FLOORS-NOT-CEILINGS-2026-08-16](https://github.com/yoch/mqttium/blob/main/docs/reports/FLOORS-NOT-CEILINGS-2026-08-16.md) | Superseded | Later compatibility handoff and native writer evidence |
| [COMPAT-PUBLISH-HANDOFF-2026-08-16](https://github.com/yoch/mqttium/blob/main/docs/reports/COMPAT-PUBLISH-HANDOFF-2026-08-16.md) | Current evidence | Architectural handoff decision only; no public parity promise |
| [NATIVE-PUBACK-ATTRIBUTION-2026-08-16](https://github.com/yoch/mqttium/blob/main/docs/reports/NATIVE-PUBACK-ATTRIBUTION-2026-08-16.md) | Superseded | Diagnostic attribution followed by the writer-hop decision |
| [NATIVE-WRITER-HOP-2026-08-16](https://github.com/yoch/mqttium/blob/main/docs/reports/NATIVE-WRITER-HOP-2026-08-16.md) | Current evidence | Native eager-writer decision under its recorded conditions |
| [INBOUND-REPLAY-PERFORMANCE-2026-08-24](https://github.com/yoch/mqttium/blob/main/docs/reports/INBOUND-REPLAY-PERFORMANCE-2026-08-24.md) | Superseded | Initial effect-batch candidate; use the final-candidate report below |
| [INBOUND-REPLAY-PERFORMANCE-FINAL-2026-08-24](https://github.com/yoch/mqttium/blob/main/docs/reports/INBOUND-REPLAY-PERFORMANCE-FINAL-2026-08-24.md) | Current evidence | Exact final-candidate timing, allocation, and cursor-correctness closure |
| [INDEPENDENT-QOS0-LATENCY-AUDIT-2026-08-14](https://github.com/yoch/mqttium/blob/main/docs/reports/INDEPENDENT-QOS0-LATENCY-AUDIT-2026-08-14.md) | Current evidence | Bounded native QoS 0 latency audit |
| [NATIVE-QOS0-CALLBACK-DIRECT-2026-08-14](https://github.com/yoch/mqttium/blob/main/docs/reports/NATIVE-QOS0-CALLBACK-DIRECT-2026-08-14.md) | Current evidence | Direct callback-admission decision |
| [SINGLE-MESSAGE-INLINE-DELIVERY-2026-08-14](https://github.com/yoch/mqttium/blob/main/docs/reports/SINGLE-MESSAGE-INLINE-DELIVERY-2026-08-14.md) | Current evidence | Inline delivery threshold decision |

## Memory evidence

| Report | Status | Use instead or interpretation |
| --- | --- | --- |
| [MEMORY-BASELINE](https://github.com/yoch/mqttium/blob/main/docs/reports/MEMORY-BASELINE.md) | Superseded | Memory results and follow-up below |
| [MEMORY-RESULTS](https://github.com/yoch/mqttium/blob/main/docs/reports/MEMORY-RESULTS.md) | Current evidence | Result for its fixed scenario set and commit |
| [MEMORY-PROFILE-FOLLOW-UP](https://github.com/yoch/mqttium/blob/main/docs/reports/MEMORY-PROFILE-FOLLOW-UP.md) | Current evidence | Closure and regression-guard mapping |

The maintained methodology and thresholds live in
[Memory Benchmark Methodology](../memory-benchmark.md).

## Scheduler experiments

| Report | Status | Use instead or interpretation |
| --- | --- | --- |
| [SCHEDULER-PUBLISH-TARGETED-WAKE-2026-08-18](https://github.com/yoch/mqttium/blob/main/docs/reports/SCHEDULER-PUBLISH-TARGETED-WAKE-2026-08-18.md) | Superseded | 2026-08-19 campaign |
| [SCHEDULER-PUBLISH-TARGETED-WAKE-2026-08-19](https://github.com/yoch/mqttium/blob/main/docs/reports/SCHEDULER-PUBLISH-TARGETED-WAKE-2026-08-19.md) | Current evidence | Candidate accepted and merged in [#285](https://github.com/yoch/mqttium/pull/285) after eligible-runner gates |
| [SCHEDULER-RESIDENT-MESSAGE-BUDGET-2026-08-18](https://github.com/yoch/mqttium/blob/main/docs/reports/SCHEDULER-RESIDENT-MESSAGE-BUDGET-2026-08-18.md) | Superseded | 2026-08-19 campaign |
| [SCHEDULER-RESIDENT-MESSAGE-BUDGET-2026-08-19](https://github.com/yoch/mqttium/blob/main/docs/reports/SCHEDULER-RESIDENT-MESSAGE-BUDGET-2026-08-19.md) | Current evidence | Candidate accepted and merged in [#283](https://github.com/yoch/mqttium/pull/283) after eligible-runner gates |
| [SCHEDULER-WRITER-TARGETED-WAKE-2026-08-18](https://github.com/yoch/mqttium/blob/main/docs/reports/SCHEDULER-WRITER-TARGETED-WAKE-2026-08-18.md) | Superseded | 2026-08-19 campaign |
| [SCHEDULER-WRITER-TARGETED-WAKE-2026-08-19](https://github.com/yoch/mqttium/blob/main/docs/reports/SCHEDULER-WRITER-TARGETED-WAKE-2026-08-19.md) | Current evidence | Candidate accepted and merged in [#284](https://github.com/yoch/mqttium/pull/284) after eligible-runner gates |

An experiment marked current is not release evidence until its own A/A and A/B
validity conditions pass on an eligible runner.

## Implemented hot-path decisions

| Report | Status | Decision recorded |
| --- | --- | --- |
| [PACKET-ID-POOL-PERFORMANCE](https://github.com/yoch/mqttium/blob/main/docs/reports/PACKET-ID-POOL-PERFORMANCE.md) | Current evidence | Outbound packet-identifier allocator |
| [PUBLISH-DECODE-PROFILE](https://github.com/yoch/mqttium/blob/main/docs/reports/PUBLISH-DECODE-PROFILE.md) | Current evidence | Rejected direct-buffer decode expansion |
| [QOS1-FRAME-POLICY](https://github.com/yoch/mqttium/blob/main/docs/reports/QOS1-FRAME-POLICY.md) | Current evidence | QoS 1 encoded-frame retention policy |
| [NOWAIT-WIRE-SIZE](https://github.com/yoch/mqttium/blob/main/docs/reports/NOWAIT-WIRE-SIZE.md) | Current evidence | Exact wire-size admission for `publish_nowait()` |
| [QOS0-MESSAGE-BATCH-DELIVERY](https://github.com/yoch/mqttium/blob/main/docs/reports/QOS0-MESSAGE-BATCH-DELIVERY.md) | Current evidence | Small-message delivery batching |
| [QOS0-V311-DECODE](https://github.com/yoch/mqttium/blob/main/docs/reports/QOS0-V311-DECODE.md) | Current evidence | MQTT 3.1.1 QoS 0 decode path |
| [QOS1-V311-DECODE](https://github.com/yoch/mqttium/blob/main/docs/reports/QOS1-V311-DECODE.md) | Current evidence | MQTT 3.1.1 QoS 1 decode path |
| [QOS2-V311-DECODE](https://github.com/yoch/mqttium/blob/main/docs/reports/QOS2-V311-DECODE.md) | Current evidence | MQTT 3.1.1 QoS 2 decode path |
| [QOS12-LAUNCH-ENCODE](https://github.com/yoch/mqttium/blob/main/docs/reports/QOS12-LAUNCH-ENCODE.md) | Current evidence | Stored QoS 1/2 launch encoding |
| [ACK-SUCCESS-FASTPATH](https://github.com/yoch/mqttium/blob/main/docs/reports/ACK-SUCCESS-FASTPATH.md) | Current evidence | Common successful acknowledgement decode |
| [ACK-SPECIALIZED-PRIMITIVES](https://github.com/yoch/mqttium/blob/main/docs/reports/ACK-SPECIALIZED-PRIMITIVES.md) | Superseded | Broader specialized codec bind-table implementation |
| [RM-SLOT-UNTIL-HANDOFF](https://github.com/yoch/mqttium/blob/main/docs/reports/RM-SLOT-UNTIL-HANDOFF.md) | Current evidence | Inbound Receive Maximum ownership until effect handoff |
| [AUTO-QOS1-MESSAGE-BATCH-DELIVERY](https://github.com/yoch/mqttium/blob/main/docs/reports/AUTO-QOS1-MESSAGE-BATCH-DELIVERY.md) | Current evidence | Automatic QoS 1 delivery batching |

## Adding evidence

1. Put methodology and maintained guarantees in a current contract, not a
   dated report.
2. Give the report a date, exact commit, environment, commands, and limitations.
3. Never commit generated benchmark output as source documentation.
4. Add the new body here exactly once and update the status of anything it
   supersedes or retracts.
