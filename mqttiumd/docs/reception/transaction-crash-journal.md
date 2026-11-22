# Transaction crash continuation

Published and received code: `38db7d6ecd7e2ff2f71402480d3b3105e52cd265`,
branch `claude/optimistic-feynman-fqh947`, PR
[648](https://github.com/yoch/mqttium/pull/648), still draft and unmerged.
The connector read back the exact head and evidence-writer blob after push.
The commit adding the final report and this log changes documentation only;
its head and final CI outcomes are to be read from the PR.

Starting remote head was `d76ccc691fcd54324674c07abfb1a5ec8a1312a7`,
base `874f298b4de7c3a13006cb1ff767dfb09e4031ac`; supplied `e0013518` was
already an ancestor. Local integration branch: `codex/transactional-crash`.

- First result executed: existing K1 store child test on `d76ccc69`, whole
  state exactly before the command, signal 9; its own traced repeat retained.
- Existing L7 `1474895d` and L3 `4fa0bce9` work reused. L5 head
  `f5c1ee0d9683f1aff65cb080c1860805f4789d97` was already integrated by
  `c5048300`; all 52 TLC outcomes received again, I3 scope preserved.
- Final K1/K2b/K3 campaigns all use clean `38db7d6e`: qualifying PASS,
  validate accepted, recompute stands, matching second binary construction,
  separate own-run strace witnesses, expected negative controls detected.
- K2a: code `3844d35c`, 64 calibration + 1000 frozen trials, seed 1,
  0–800 us; 600 found, 400 absent, 0 mixed/errors. Of the found, 460 had
  no return observed. No kill is established inside COMMIT.
- WAL: 27 artificial images, 26 exact before / 1 exact after / 0 errors.
  Their copies and scope are separate from process kills.
- Final checks on received code: fmt, clippy both modes, 645 normal and
  653 instrumented Rust tests, 129 oracle tests; 337 corpus + 399 trace files
  current, 351 documents agreed by independent schema readers.
- Integration findings corrected test first: missing/contradictory recovery
  evidence, SQLite startup/source joins, Cargo unification, checkpoint hook
  replacement, external-target fixtures and the real K3 stable-write parent.

Contract digest: `sha256:d2f238ea6214fec13ad9fcf6c324c609eadd408be695bc15101822167a0cfe0b`
(initial `5ab94255…`, intermediate `3c720b16…`). Proposed ADRs retain status.

Reproduce from `mqttiumd/` with the pinned toolchain and oracle dependencies:

```bash
tools/run_transactional_reception.sh /tmp/new-transactions 1
tools/run_k2a_sweep.sh /tmp/new-k2a 1
tools/run_wal_prefix_experiment.sh /tmp/new-wal-images
```

Full commands, scopes, findings and archive SHA-256 values:
`../reports/0004-transactional-crash-reception.md`. Generated evidence is at
`/workspace/mqttium-evidence/`, with `SHA256SUMS`; final deterministic runs
remain at `/tmp/mqttium-transactional-reception-final/`.

Continuation: the typed bounded durable-reader chain and its control fixes
are received in `durable-read-journal.md` and report 0006. This journal retains
the prior transaction dataset and its exact scope. Read the PR for the current
published HEAD and checks. No main merge or global ADR acceptance is authorised.
