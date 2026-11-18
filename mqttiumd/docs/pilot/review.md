# Independent source review and corrective reception

The frozen reference was 49bbaaa4d1d080f458980e3498e90638e317668d, base
874f298b4de7c3a13006cb1ff767dfb09e4031ac. GitHub reported a draft, unmerged PR,
1,571 changed files, 1,324 commits and 36 green checks. The local tree and remote
HEAD matched. Review submissions and inline threads were empty at revalidation.
Four fresh reviewers read the named implementation paths independently. Their
original reports are retained under `reviews/`. The durability review records
its limited coverage and the blocked broader delegation explicitly.

Three P1 runtime findings were independently reproduced. Test-first commits
`319bbd5c` and `a7eeb6ac` contain the red regressions. The correction is `5bcbce21`.
Its runtime and tests are frozen in qualified candidate 51be4079.

| Finding | Discriminating observation | Corrected behavior |
| --- | --- | --- |
|Decoded normal DISCONNECT lost behind TLS terminal writer|Real TLS task decoded five PINGs and DISCONNECT. Requeued bounded input retained its order, while the genuine writer had terminated. The old owner published or retained a pending Will.|Reach the ordered input terminal through bounded turns, including the outgoing-response error path, and preserve connection deadlines.|
|Genuine Started dropped by PUBACK/DISCONNECT retirement|Actual expiring QoS 1 PUBLISH received, genuine Started queued, PUBACK and DISCONNECT processed, same-session resume. Red runs left the exact claim Pending and no exact durable Finish.|Keep frame authority through producer quiescence and consume its final facts before retiring the generation.|
|Overlapping same-ClientID Will Opens|Two real writer barrier replies both reported absence before either Open. The replacement received 0x80 after the first closed.|Preserve earlier Open correlation, recheck the current attachment and let the old Will decision progress before the replacement Open.|

The three red cases compiled and failed on their business assertions. Compile
errors and harness timeouts were excluded as detections. The final 58 reactor
tests and the normal/instrumented workspace suites passed. Independent final
transport review read the frozen git object, runtime SHA-256
c47164b7bcb5dae7e02c80e2c4b4ea0c1f96930371157f33690a0c64cc071b8a.
The independent Open correction review found no remaining concrete issue in
that limited change. A proposed small-Maximum-Packet-Size P1 was retracted after
checking MQTT-3.1.2-4 and 3.1.2-24. Its original assessment is preserved and the
findings register records the correction.

No public MQTT contract, canonical recipe or SQLite representation changed.
Contract hash remained
`bf9bbc0a9d74a4a5b72330f60038b2f51189eca58b5784c6c40a54bfe7e22d1a`.
Local formatting, Clippy, both Rust configurations, 269 oracle tests, 646 schema
agreements and 188 system-driver tests passed. The candidate's 36 CI checks also
passed. Its separate qualifying K1/K2b/K3 gates retain their historical scope.
The new runtime tests and system workloads have the stated implementation scope.

The original candidate's healthy load had complete traffic counts but admitted
six Wills during normal requested closure. The corrected candidate repeated the
full workload on its downloaded release binary and admitted zero such Wills.
The operational NO-GO follows the independently measured outage capacity bound,
with no change to the agreed workload targets or broker quotas.
