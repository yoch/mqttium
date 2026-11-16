# QoS 1 product continuation journal

## Reprise and delivered increments

- Repository/PR: yoch/mqttium #648, draft and unmerged.
- Connector-confirmed start: 1dea5b59864baefebce567729d781af44c80cf22;
  main 874f298b4de7c3a13006cb1ff767dfb09e4031ac; branch
  claude/optimistic-feynman-fqh947. Local HEAD/FETCH_HEAD and inspected blobs
  matched; no review/thread; starting checks successful.
- a219db4a: Proposed ADR 0023, packet-specific PUBACK and frozen PUBLISH
  occurrence on EmitFrame. Contract digest 775794a5…fa5cc →
  a0b4fb2cab5c1eb001083aa897e6c03875489813e19f99da65c550784f966c42.
- Semantic red commits precede codec/core/receive-window/Paho outcome fixes;
  78d4e94a establishes the real PUBACK receive-credit leak before bd9997de.
- 45f308b7 / b9532e37: shared admission/resolve path, bounded AckPending,
  full effect/connection correlation, exact leases and charged close cleanup.
- bfe123c8: one bounded nonblocking internal ACK-receipt hold, two isolated
  product mutation sites; absent from the default build.
- 530c5adfe12a2ca8011a84b99062283936872a82: first coherent published product
  increment. Pre-push remote head was still 1dea5b59. Post-push connector
  comparison/readback matched all 31 changed blobs; PR remained draft.
- bc5599e9: explicitly require same mqttium process, one application publish,
  matching original receipt and Session Present 0→1 in the proxy loss case.
  Targeted case re-executed successfully; no broker behavior change.

## Executed reception

Commands from mqttiumd, with separate build/campaign directories:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo clippy --workspace --all-targets --features mqttiumd-lab/kill-points --locked -- -D warnings
cargo test --workspace --features mqttiumd-lab/kill-points --locked
cargo test -p mqttiumd --features test-hooks --locked
cargo clippy -p mqttiumd --all-targets --features test-hooks --locked -- -D warnings
cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json
python tools/build_corpus.py --check
python tests/oracle/build_traces.py --check
python tools/check_schemas.py
python -m unittest discover -t tests/oracle -s tests/oracle/tests
python -m unittest discover -s tests/system -p 'test_*.py'
tools/run_qos1_reception.sh /workspace/mqttium-q1-evidence/530c5adf-qos1
tools/run_tcp_reception.sh /workspace/mqttium-q1-evidence/530c5adf-qos2
```

Normal/instrumented full suites ran sequentially: 766 / 774 pass; product
hooks 47; oracle 142; pilots 17. Clippy/formatting pass; corpus 367 and traces
405 current; both readers agree on 382 documents. Build targets were explicit
per lot. No proof BODY or French archive changed.

System receipts at 530c5adf: QoS 1 normal3/hooks10 PASS, exact negative reasons
premature_puback and stale_receipt_released; QoS 2 normal5/hooks9 PASS, legacy
negatives unchanged. SIGKILL wait -9 and same-file restarts; the live mqttium
process/original receipt survives the proxy-observed lost PUBACK. Expected
QoS 1 admissions are 1→2, not universal deduplication. Actual TCP pressure
and the focused held four-slot PUBACK queue test have distinct scopes.

Archive hashes and executable/source identities are in report 0008. Each run
retains its own strace; all 461 local retained files match SHA256SUMS.

## Publication and next action

The final documentation/test-assertion integration must be published with
expected-head protection against the already confirmed 530c5adf. Read back
HEAD/diff/blobs and receive that SHA's CI, including fresh K1/K2b/K3, typed
reads, K2a/WAL, models and proof; correct actionable failures before finishing.
The PR description and CI artifacts record the immutable final head/checks.
Do not transfer 530c5adf's local archive identity to a later run.

QoS 1 is a finite system reception, not newly qualified by the fixture oracle.
See open-conditions and product-tcp for retained operational limits. Next:
incoming QoS 0 with no invented packet identifier, then subscriber delivery.
Main merge and global ADR acceptance remain unauthorized.
