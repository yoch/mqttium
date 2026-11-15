# Transaction crash continuation

This is the short continuation log. Generated campaigns live outside the
repository. Decision records retain their stated status; this work does not
authorise a merge into `main`.

## Confirmed starting content

- GitHub connector, PR [648](https://github.com/yoch/mqttium/pull/648): open,
  draft, integration branch `claude/optimistic-feynman-fqh947`, head
  `d76ccc691fcd54324674c07abfb1a5ec8a1312a7`, base `main` at
  `874f298b4de7c3a13006cb1ff767dfb09e4031ac`. No submitted reviews or inline
  threads. CI, Formal models and mqttiumd workflows succeeded on that head.
- The local checkout initially held the base. Git fetch succeeded and the
  integration work starts from the confirmed PR head, on local branch
  `codex/transactional-crash`.
- The supplied reference `e0013518a17de964460e7a63d3049515e2d6197b` is an
  ancestor. R1 to R4, K1 store instrumentation and the first recovery-admission
  oracle correction are already present and are reused.
- L5 branch head `f5c1ee0d9683f1aff65cb080c1860805f4789d97` is already an
  ancestor through integration commit `c5048300`; no second implementation or
  merge of that content is required.
- Starting contract digest:
  `sha256:5ab94255d91bfce1d53d18279399c9b8dc055a6e017bfa334941593808cf51a5`.

## First executed result

Activate the preinstalled tools before the commands below:

```bash
source /workspace/.mqttiumd-tools/env.sh
cd /workspace/mqttium/mqttiumd
CARGO_TARGET_DIR=/tmp/mqttium-target-root cargo test \
  -p mqttiumd-store-sqlite --locked --features kill-points \
  --test kill_points a_kill_before_commit_recovers_the_state_before_the_command \
  -- --nocapture
```

Executed on `d76ccc69`: one test passed. It kills the child at K1 before
`COMMIT`, checks wait signal 9, reopens the same file without intervention and
compares the entire `DurableState` with the deterministic reference before the
interrupted multirow admission. Retransmission matches the reference as a
first submission.

The already compiled test binary was then executed under
`strace -f -qq -e trace=kill,wait4 -e signal=none`; that separate run passed too.
Its own log and system witness are in
`/tmp/mqttium-k1-baseline-d76ccc69/`. Binary SHA-256:
`cd90b968328080cf245f3556cf692596ad5981e9df06663a1053d354a4a041f7`.
That witness establishes only that traced test run.

## Integrated increments awaiting campaign reception

- `1fe8e699`: receives the existing L3 contract chain `36d3d4aa`,
  `4fa0bce9` through `9be213cb`. Proposed ADR 0020 and two injection points;
  contract digest becomes
  `sha256:3c720b161fab91841662b98a22fae898c1ab634d461b209dfd994eaf791d77ed`.
- The store chain through `0385eb52` adds K2b with an explicitly disabled
  autocheckpoint while its callback is armed, restoring the saved threshold
  when disarmed. Four focused store tests pass after integration: K1 exact
  before state, K2b exact after state and deduplication, observed callback
  invocations and restored checkpoint threshold, child entry point.
- `12f39fb9`: second reconstruction selects the same explicit Cargo features
  as the recorded build. The campaign wrappers select `kill-points` for K1
  and K2b, and distinguish the qualifying run from its separately traced run.

Next: integrate the command-in-flight bench and the strengthened independent
oracle, run fresh qualifying K1/K2b/K3 campaigns, receive the bounded K2a and
artificial WAL experiments, then publish the complete checked increments.
