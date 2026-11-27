# Two intermittent failures of the CI

Branch `refactor/lot10-validated-base`, after report 0047. Two tests of
the product failed now and then on the CI runner, in the instrumented build
of the job `runtime`, and report 0047 left their cause open. This report
finds it for each: one is a defect of the writer, fixed here; the other is
the load of the machine.

## Method

Both were played in a scratch worktree of `42cc618` with temporary traces
on stderr (the acceptance of a connection, its CONNECT, the will barrier and
the session opening sent and answered, the CONNACK queued, the close and its
code, the time the writer spends on each command). The traces were not
committed. Two loads were applied, alone or together:

- the CPU: the test binary on two cores (`taskset -c 0,1`), another process
  keeping them busy, and eleven test threads;
- the disk: every `fsync` and `fdatasync` of the broker slowed by 10 to 80 ms
  (`strace -f --seccomp-bpf -e inject=fsync,fdatasync:delay_exit=...`).

## The fan-in tests in log mode

`fanin_16x1_qos1_zero_refusals_and_fast_drain_in_log_mode` and its variant
with a Receive Maximum (`tests/concurrent_churn_refusals.rs`, `K = 0`,
`--storage-mode log`) failed at `bb6c30d` and `791cb04`. Sixteen publishers
connect while the first ones already publish; four of them read an end of
stream instead of their CONNACK. The test then panics and its broker is
killed, which ends the streams of the others.

The broker closes a connection whose CONNACK is not queued within 3 s of its
acceptance (`connect_deadline`, record 0022). A CONNECT goes through two
control requests of the writer: a will barrier, then the session opening
(`ControlRequest::Lifecycle`), of which the runtime keeps one in flight
(`LIFECYCLE_IN_FLIGHT`, record 0048). The openings of a burst are thus one
after the other.

Without load, the 17 connections of the test get their CONNACK within 217 ms,
each opening committing in 0.5 ms. With every `fsync` slowed by 30 ms an
opening commits in 31 ms but waits 45 to 135 ms before the writer serves it,
and the last CONNACK comes after 1.7 to 2.2 s. The worker of the writer
served one control request per pass and kept its answer until the next pass;
each pass commits the ordinary commands waiting (the admissions of the
publishers), and in log mode each commit synchronises the journal. A control
request thus waited one ordinary commit for every control request ahead of
it, the barriers of the next connections included, and its answer waited one
more. With every `fsync` slowed by 50 ms the broker closed a connection with
`connect_deadline` in each of five runs, which is the failure of the CI.

| Commit | Change |
| - | - |
| `218c157` | Test: `queued_control_requests_are_answered_before_the_next_ordinary_command` (`writer.rs`, `test-hooks`). A hook (`hold-ordinary`) holds the worker before each ordinary command; three will barriers and a second ordinary command queue behind the first; the three answers must arrive while the second is held. It failed: none arrived |
| `26be2d0` | Fix: in each pass the worker serves the control requests queued, at most the capacity of their queue, and hands each answer over at once. An answer its owner has no room for waits as before and stops that lane until the next pass |

With the fix and every `fsync` slowed by 50 ms, no run of five closed a
connection; the longest wait for a CONNACK fell from 2.88–2.99 s to
1.87–2.20 s. The control requests of a pass now all go before its ordinary
commit. Before, the first already did. No order between the two queues was
promised (record 0029 gives the lifecycle a lane of its own, bounded apart),
and none is assumed by the runtime: the product tests pass in both
configurations.

What stays: the openings remain one at a time. With every `fsync` slowed by
80 ms the fixed broker still closed two connections of the seventeen in
one run (six before the fix). A burst of connections needs about two journal commits per
connection, one after the other, within the 3 s of record 0022. That is a
limit of the design, records 0022 and 0048, not a defect; changing it is the
owner's decision. Options:

- several session operations in flight for distinct sessions;
- the openings of a burst committed together;
- a connect deadline that does not count the time the connection waits for
  the store;
- the limit kept and stated in the product documentation.

## The isolation test of the shards

`sharded_slow_subscriber_isolation_and_fifo_drain` (`tests/delivery_shards.rs`,
`K = 2`) failed at `88f9e99` and `cb77770`. Report 0047 measured that it
exceeds its 550 ms bound under CPU load at the head and before the cleanup
alike. The question left was whether the publisher was then paced by the
slow subscriber, which acknowledges every 8 ms.

Under the CPU load above, twelve runs with the slow subscriber at 8 ms and
twelve at 20 ms, alternating:

| Slow subscriber | Median total | Over 550 ms | Median wait of a PUBACK |
| - | - | - | - |
| 8 ms | 601 ms | 6 of 12 | 6.7 ms |
| 20 ms | 587 ms | 6 of 12 | 6.0 ms |

A publisher paced by a subscriber at 20 ms would take at least 1.56 s. The
times do not follow the slow subscriber: the load of the machine makes the
test exceed its bound, not a coupling between sessions. The spin of report
0046, which this test first caught, has had a deterministic test since
(`a_fenced_session_does_not_keep_its_shard_busy`, `delivery_shard/fence_tests.rs`).
The test is unchanged; giving it room is the owner's decision, as report
0047 says.

## Verification

From `mqttiumd/`, on `26be2d0`, with
`env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `cargo fmt --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings`, also with `mqttiumd-lab/kill-points` and with `-p mqttiumd --features test-hooks` | pass |
| `cargo test --workspace --locked` | 87 binaries, 1,477 tests pass |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked` | 88 binaries, 1,499 tests pass |
| `cargo test -p mqttiumd --locked`; `cargo test -p mqttiumd --features test-hooks --locked` | 343 and 365 tests pass |
| `tools/check_kill_points.sh`, `validate` of P1-02 | pass; digest unchanged, `sha256:cdd2b28f45ffa66c22ff3c556dab4d20396814cb0693fe4f3805c1307b65a300` |
| Corpus, schemas, traces, oracle (279 tests), `conformance/check.py --check`, `models/check_models.py`, TLC on `models/group-commit` | pass |
| The test of `218c157`, ten times; the two fan-in tests in log mode, ten times | ten of ten |

## Performance

The fix changes the loop of the writer's worker, which every ordinary
command goes through. Instructions retired by the broker under callgrind,
alternating `9884c3d` (A, the code of `42cc618`) and `26be2d0` (B), medians
of three runs for x1, means of two for x8:

| Shape | A | B | Change |
| - | - | - | - |
| QoS 1 pairs x1, 20,000 messages | 800.94M | 801.40M | +0.06 % |
| QoS 1 pairs x8, 4,000 per pair | 1,255.47M | 1,254.42M | −0.08 % |
| QoS 0 bursts x1, 20,000 messages | 280.93M | 280.96M | +0.01 % |
| QoS 0 bursts x8, 10,000 messages | 703.80M | 704.24M | +0.06 % |

The gate of report 0043, A `9884c3d`, B `26be2d0`, six passes, broker on
cores 0 and 1, clients on 2 and 3:

| Shape | Median of B/A | Verdict |
| - | - | - |
| QoS 0 bursts, 1 subscriber | 0.981 | pass |
| QoS 0 bursts, 8 subscribers | 1.047 | counts decide (+0.06 %): pass |
| QoS 1 pairs, 1 pair | 1.012 | pass |
| QoS 1 pairs, 8 pairs | 0.980 | pass |
| QoS 0 blast, 1 / 8 subscribers | 1.050 / 0.964 | reported |

## What stays open

In `docs/reception/open-conditions.md`: the serial session openings under a
burst of connections in log mode, with the options above, and the room of the
isolation test, in the row of the sharded mode.
