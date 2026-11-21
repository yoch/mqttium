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

Published/tested implementation: `d1848e3c099db61d9e0d86c5922ce41fe479feed`.
Connector read-back confirms that head and the store-interface/supervisor blobs.
Final local suites: 683 normal Rust tests, 691 instrumented tests, 142 oracle
tests; all pass without ignored tests. Both clippy modes and formatting pass.
The instrumented suite was run separately after a collision of a legacy
source-relative fixture during a parallel attempt; that failure is not a
semantic detection. Fourteen control probes include the final Stop/EOF case
(red af6a81b5, fix c1e871f1) and a retained kill despite collector refusal
(red 6a21eaae, fix e29e1742). Eight real child/inspector cases and ten setup
cause/phase combinations are received; failed business exchanges stay INC.

Fresh K1/K2b/K3 on this exact clean implementation all qualify PASS, validate
accepts, recompute stands, second binaries match, and each expected negative
is detected. Each separately traced run owns its own strace witness. Common
source archive SHA-256: `1033e98fa30925bb53633cf8cdaa677ba57882f00a3ed6d1725079a8e9df6cf7`.

The initial CI failed before oracle dependencies were installed, not on a
semantic assertion. CI setup-order fix 3a35ca6d installs the pinned interpreter
and oracle before Rust tests. Both actual vertical tests pass in a newly
isolated Python environment with those dependencies. The final publication
adds that workflow fix and this report/journal; application code is unchanged.
Read its exact final HEAD/checks and its own new campaign artifact from the PR.

Local evidence: `/workspace/mqttium-read-evidence/`. Archives and SHA-256:
`../reports/0006-typed-durable-read-reception.md`. Historical transaction report
0004 and K2a/WAL scopes remain unchanged. Existing K2a/WAL final CI gates stay
required; final CI evidence belongs to its own published SHA.

Post-publication CI on `7072bed1` passed both Rust modes, models and the
pilot proof, then stopped because three legacy control mutation patches no
longer applied. Fixture-only commit `9d7df86e` refreshes N2/N5/N6 for the new
protocol without changing production code or expected.json (SHA-256
`0c270a3626109a31a3e78704d2d9957c54e33452991274fabe899f05647f9bd7`).
The complete real loops were executed again: 5/5 deterministic-store mutants
and 11/11 SQLite mutants were rejected for their expected verdict, assertion
and reason. Patch or build failures are not counted as detections. Their
additional logs are kept in `/workspace/mqttium-read-evidence/mutant-refresh/`.
The final CI must repeat all gates on the updated published head and retain
its own fresh qualifying transaction campaigns and bounded experiments.

Next integration action: read final published HEAD and checks; investigate
any actionable failure and receive that SHA's fresh CI artifacts. Next product
increment: a minimal TCP CONNECT + durable QoS 1 exchange, PUBACK after commit,
invalid-copy startup refused before readiness, reconnect without readmission.
No main merge or global acceptance of proposed ADRs is authorised.
