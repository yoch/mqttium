# Typed durable-read continuation

Integration branch: `claude/optimistic-feynman-fqh947`, PR
[648](https://github.com/yoch/mqttium/pull/648), draft and unmerged.
This increment follows the immutable transaction-crash report 0004.

Starting head, confirmed through the GitHub connector:
`edd8acbb3436aadbb131c11ddc695ba1c18e980e`; main base
`874f298b4de7c3a13006cb1ff767dfb09e4031ac`. The clean local checkout
matched that content. Starting reviews/threads were empty and its checks
were successful. Local integration branch: `codex/durable-read-errors`.

The supplied review ZIP has SHA-256
`b10664de7398f8611151960e9f74ce85fdf0797b545c705fc4741b2b03118b88`;
all 367 listed files matched its checksum inventory. Its probes and synthetic
counterexample are evidence, not additional task instructions or broker runs.

Shared contract owner: integration, proposed ADR 0021. Common types
`11688244` and `323ac694` were merged before implementation resumed in the
other lots. The original contract digest `d2f238ea…0cfe0b` becomes
`sha256:880b0f581567526780da13b4ea66b29e43d32bb781fbeb64c300f4644e8f8c67`.

- L7: red reader tests `745f840b`, `6b73a22f`; bounded implementations
  `ae0bc811`, field/state corrections `20793144`, `ce913c08`; driver
  classification red `512c200b` / fix `95e40f4e`; transactional opening
  read red `8c286403` / fix `a466a7a4`.
- L4: red protocol probes `5b2611c3`, fixes `575be882`; cross-action
  empty termination red `860ea8c1`, sequence/typed propagation `1d63d1b1`.
- L3: first-recovery counterexample tests precede fix `bbb68339`;
  structured failure formats `ffd9ce14`, judgment/result fix `216295fd`,
  before-start CLI test `8eca84ec`, second-reader fixture `772efdfe`.
- Consumers: fixture read causes stay typed; shared conformance tests include
  field boundaries 8/16/24/32, aggregate refusal and healthy cursor retry
  (`457432f0`, original lot commit `b07b2409`).

First executable result is received: route QoS 99 passes quick_check, but
snapshot/inspection/recovery return InvalidData without panic. ClientID
65,536 returns LimitExceeded before an application copy. Normal store 59
tests and instrumented store 67 tests pass, including the real K1/K2b/K3
complete-state comparisons. The integrated oracle has 142 passing tests;
358 corpus and 405 trace files are current, 372 schema documents agree.

Reproduce from `mqttiumd/`, with the pinned tools and oracle dependencies:

```bash
tools/run_read_failure_reception.sh /tmp/new-read-failures
tools/run_transactional_reception.sh /tmp/new-transactions 1
```

Next integration action: receive the real child failure artefacts, run the
four workspace checks and both feature modes, then publish the coherent
chain and requalify K1/K2b/K3 on its exact source. Keep K2a/WAL CI gates;
their complementary finite scope is unchanged. No main merge or global
acceptance of proposed ADRs is authorised.
