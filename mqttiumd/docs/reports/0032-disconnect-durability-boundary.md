# Where the end of a session becomes durable: observations on the executable

This report records the observations behind Proposed record
[0059](../decisions/0059-disconnect-durability-boundary.md). It answers the
point of the external audit of `47e1d12` that upstream Mosquitto ticket 3761
raised. The audit held one observation: in durable mode, a client lowered
its Session Expiry Interval to zero with a normal DISCONNECT, read the end
of its stream, the process was interrupted at once, and the session was
resumed with Session Present 1 after the restart. It asked for the
guarantee of the durable mode, the event that makes it real, and the state
of the ended session and of a witness at each observable boundary.

No code path of the broker changes in this wave.

## Host and revisions

- **Revision**: branch `perf/lot7-crushing-mosquitto`, on top of `54cf35f`.
- **Host**: Linux x86-64 container, 4 CPUs, kernel `6.18.44`, `ext4`;
  `rustc 1.97.0`, development profile.
- **Executable**: `target/debug/mqttiumd`, built with `test-hooks` for the
  cases that hold the writer at a barrier, without it otherwise.
- **Reference broker**: none. Mosquitto is not installed on this host.

## Method

`crates/mqttiumd/tests/disconnect_durability.rs` runs the executable three
times on one store: `setup` opens the sessions and stops gracefully, `cut`
is killed with SIGKILL at a chosen point (the test requires the process to
have ended by signal 9), `restart` is observed over loopback TCP.

- The target and the witness open their sessions with Session Expiry
  Interval 600 s and a QoS 1 subscription. A monitor holds a persistent
  session subscribed to the Will topic.
- In `cut` the target resumes its session (Session Present 1), with a QoS 1
  Will where the case has one, and sends DISCONNECT `0x00` with Session
  Expiry Interval 0. The test reads the end of the stream before it looks
  for the point of the kill.
- In durable mode the point is a barrier of the writer: `*.prepared` holds
  it inside the transaction, after its writes and before `COMMIT`;
  `*.committed` holds it after `COMMIT` and before the result is returned.
  A client with a Will ends through `EndWill`, so its barriers are
  `will.end.*`; without one they are `session.close.*`.
- The log and volatile modes refuse the barriers
  (`volatile_test_hooks_unsupported`). There the kill follows the
  `session_changed` event with `action` `Closed`, which the runtime writes
  once the store has answered.

## Observations

| Case | Mode | Kill | End of stream read before the kill | Sessions in the store while held | Ended session after restart | Witness after restart | Will |
| - | - | - | - | - | - | - | - |
| No Will | durable | `session.close.prepared` | yes | 3 | **Session Present 1** | Session Present 1 | n/a |
| No Will | durable | `session.close.committed` | yes | 2 | Session Present 0 | Session Present 1 | n/a |
| Will, normal DISCONNECT | durable | `will.end.prepared` | yes | 3 | **Session Present 1** | Session Present 1 | **published once, read by the monitor** |
| Will, normal DISCONNECT | durable | `will.end.committed` | yes | 2 | Session Present 0 | Session Present 1 | not published, no Will row left |
| No Will | log | after `session_changed` | yes | not read | Session Present 0 | Session Present 1 | n/a |
| No Will | volatile, snapshot interval 3600 s | after `session_changed` | yes | not read | **Session Present 1** | Session Present 1 | n/a |
| Will, normal DISCONNECT | durable | none, graceful stop | yes | 2 after the stop | Session Present 0 | Session Present 1 | not published |

The first row reproduces the audit's observation and places it: the client
has read the end of its stream while the transaction that ends its session
is still open. The third row is the consequence the audit did not test: a
Will that a normal DISCONNECT cancelled is published by the restart when
the process is lost before that commit. The volatile row is the store
returning to the snapshot of the graceful stop of `setup`.

Each test passed eight times in a row with `test-hooks` (seven tests) and
five times in a row without (three tests).

## The window, measured

`tools/measure_disconnect_commit_window.py` opens a session, ends it the
same way, and records on one clock when the client read the end of its
stream and when the `store_result` event of the close reached the event
file. The reading includes the time the event takes to be written and seen:
it is an upper reading on this host, not a bound.

| Mode | Samples | Commit after the end of the stream | Minimum | Median | 95th percentile | Maximum |
| - | - | - | - | - | - | - |
| durable | 200 | 200 | 0.59 ms | 1.53 ms | 2.70 ms | 4.07 ms |
| durable, second run | 200 | 200 | 0.78 ms | 1.51 ms | 2.07 ms | 3.93 ms |
| log | 200 | 200 | 0.39 ms | 0.64 ms | 0.90 ms | 22.99 ms |

In every sample the commit came after the end of the stream. A later run of
50 samples in durable mode had a maximum of 24.98 ms: the tail is one
storage operation long, and this host's storage is not a reference.

```sh
cargo build -p mqttiumd --locked
python3 tools/measure_disconnect_commit_window.py --binary target/debug/mqttiumd --samples 200
python3 tools/measure_disconnect_commit_window.py --binary target/debug/mqttiumd --samples 200 --storage-mode log
```

## Verdict proposed for the audit's point

The audit classed ticket 3761 as "a hint after an interruption". The
boundary is now characterised and the guarantee stated:

- the end of a session is durable at the commit of its lifecycle command,
  and the end of the client's stream precedes it;
- a loss of the process in that window returns the session and publishes
  the Will a normal DISCONNECT had cancelled;
- past the commit neither happens, in durable and in log mode;
- in volatile mode an ended session returns until the next snapshot.

Record 0059 keeps this order and names what it would cost to change it.

## Loop mutant m4

The job `contracts` of CI failed on this branch since lot 6 (`4c4e450`)
rewrote `Answers::names`: the diff of `m4-landmarks-ignored` no longer
applied. It is regenerated with the same mutation (`54cf35f`).
`tools/run_loop_mutants.sh` and `tools/run_loop_mutants.sh --backend sqlite`
reject it with the entry of `expected.json` (`scenario_reaches_its_end`,
the wait of step 19 unmet) and reject every other mutation for its reason.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| `cargo clippy -p mqttiumd --all-targets --features test-hooks --locked -- -D warnings` | clean |
| `cargo test --workspace --locked` | pass: 83 result lines, 1341 tests, 0 failed |
| `cargo test -p mqttiumd --features test-hooks --locked` | pass: 13 result lines, 258 tests, 0 failed |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked`, `contracts sha256:ee48f76f…`, unchanged since report 0031 |
| `python3 tools/build_corpus.py --check` | 624 derived files, 0 not up to date, 0 stale |
| `python3 models/check_models.py` | every directory consistent |
| `tools/run_loop_mutants.sh` | every mutation expected to be rejected was rejected for its reason on sim-store |
| `tools/run_loop_mutants.sh --backend sqlite` | every mutation expected to be rejected was rejected for its reason on sqlite |

## Correction to report 0031

Report 0031 gave the workspace run as "39 test binaries, 768 tests". The
command that summarised that output truncated it. The run itself had no
failure; the count was wrong, and report 0031 now says so. The figure above
is from the untruncated output of this wave.

## Not run, not shown

- A kill of the process is not a power loss: what was written survives in
  the page cache. Nothing here is evidence about `fsync` or a torn write.
- No kill inside `COMMIT`, and none before the commit in log and volatile
  modes, which have no barrier.
- No TLS case at the level of the executable.
- No differential run against Mosquitto: ticket 3761 describes Mosquitto's
  batched persistence, which this report does not measure.
- The window is measured with a development build on a container: no
  reference storage, no load.
- The oracle, the campaigns other than the loop mutants, the Kani proof and
  TLC were not run: no file they read changed in this wave.
