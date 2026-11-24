# The runtime's ties and the effective settings: deslop lot 6

Report 0039 explains `deslop-please` and the baseline
`.deslop-please.allow`. Lot 6 covers the broker runtime (`crates/mqttiumd`)
and the effective settings of the stores, which report 0041 deferred to
it. It changes no behaviour: every event, failure code, file format and
byte on the wire is the same.

## Host and revisions

- **Revision**: branch `refactor/deslop-layers`, on top of `ad33c98`
  (report 0046): `8f219b5` (the reactor), `bb17ac5` (the frame bytes),
  `200ddde` (the writer), `6b3b306` (the acceptor, the policy plan and the
  policy), `cea5b9c` (the effective settings).
- **Host**: Linux x86-64 container, 4 cores, `rustc 1.97.0`.

## B1: the reactor's shared state takes its own steps

`net::reactor::Shared` is the state a reactor thread shares with the
producers that wake it. The reactor loop, the pool and the hold guard
wrote and read its fields from outside. Each step of the wake protocol
is now a method of `Shared`, with the same `SeqCst` loads and stores in
the same order:

| Method | Step of `models/reactor-wake` |
| - | - |
| `take` | `ReactorTake`; `None` on a poisoned lock, as before |
| `sleep` | `ReactorSleep`, `ReactorRecheck`, `ReactorPoll`, `ReactorAwake`, in one call; awake again before the caller looks at the result of the poll |
| `hold` | `ProducerHold` |
| `release` | `ProducerRelease`: clear the hold, then kick |
| `leave` | not modelled: the thread marks itself dead, then finishes what was registered with it |
| `stop` | not modelled: Stop, then the unconditional wake-up |

`send` and `kick` are unchanged. The README of `models/reactor-wake`
names the methods; the model, its configurations and the tests it replays
are unchanged, and `models/check_models.py` passes.

## B2: the frame bytes belong to the codec

`codec`, `net` and `security` used each other in a circle: `security`
validates with `codec::{utf8, topic}`, `net` reads and writes with
`codec` and `security`, and `codec` built its frames in `net::FrameBytes`,
bounded by `net::MAX_PACKET_BYTES`. `FrameBytes`, its impls,
`INLINE_FRAME_CAP` and `MAX_PACKET_BYTES` moved, unchanged, to
`codec.rs`, since the frame is what the encoder produces. `net`, the
runtime, its tests and three integration tests import them from `codec`.

A new `wire.rs` would have made `crates/mqttiumd/src/` a wide directory
(it holds 15 entries, the most the linter accepts). No conformance
citation, mutant, model or proof names a symbol that moved. Record 0057
names `net::FrameBytes`; it describes the code when it was decided and is
left as it is.

## B3: the writer answers through its methods

`StoreHandle`, `run` and `run_inner` reached into the lanes and the boot
of `writer::Writer`, and into the reply queues of `DirectWriter`. `Writer`
now answers through `try_send`, `try_recv`, `try_send_control`,
`try_recv_control`, `request_recovery`, `request_closed_collection`,
`await_boot`, `await_reply`, `await_control_reply` and `stop_within`.

- `await_reply` and `await_control_reply` run `reply_within`, the former
  `startup_reply` of the runtime, in the same order: a reply already
  produced, then the stop flag, then the deadline, then a wait of at most
  10 ms. It is written in `writer.rs`, so that the writer does not use
  the runtime.
- `stop_within` is the stop loop of `run`, unchanged: Stop once behind the
  accepted work, both reply lanes drained, pauses of 2 ms. It returns
  whether the worker finished.
- `StoreHandle::await_recovery_reply` and `await_collection_reply` wait
  for what `exec_recovery` and `exec_closed_collection` did not answer at
  once. `run_inner` no longer matches the threaded writer. The deadline of
  a recovery page is still taken after its request is sent.

The fields of `Writer` stay public: the tests drive them, among them the
replays of `models/group-commit` in `tests/writer_group_kill.rs`, and the
linter does not judge test code.

## B4: the acceptor, the policy plan and the policy

- `Acceptor::try_accept` gives the next accepted connection, or none, and
  `accept_failed` both when the acceptor failed and when it stopped.
  `run_inner` keeps the check that a handshake is admitted before taking
  one, and the `progress` count.
- `Plan::planned` gives the answer of `mqttiumd policy plan` once the plan
  is written; `Plan::expected_state` gives the state `apply` expects. The
  plan format (`mqttiumd.policy-plan.v1`) and the JSON printed are
  unchanged.
- `Policy::fingerprint` and `Policy::storage_policy` (a copy) replace the
  accessor `Policy::storage`. The mutation sites of
  `tools/run_policy_mutants.py` and `tools/run_secure_mutants.py` in
  `security.rs` are each still found once.

## B5: the effective settings are handed out by copy

`SqliteStore::effective_settings`, `LogStore::sqlite_settings` and
`VolatileHandle::sqlite_settings` lent the settings the engine read back
at opening. Every reader copies them, into the boot record or into JSON,
or compares one field. The three methods now return a copy. The name
`effective_settings` stays, as `tools/check_kill_points.sh` and
`docs/reception/open-conditions.md` name it.

## Count

| | Before | B1 | B2 | B3 | B4 | B5 |
| - | -: | -: | -: | -: | -: | -: |
| Entries of `.deslop-please.allow` | 43 | 38 | 37 | 30 | 27 | 24 |

What is left: `Owner::outbox` (lot 7), the two entries of the lab kept on
purpose, the two shallow crates, and the layout entries (long files and
wide directories).

## Performance

B1 and B2 touch hot paths, so the gate of report 0046 was played on
`dist` builds, each against the head before it.

| Shape | B1 against `10a87fc` | B2 against B1 |
| - | -: | -: |
| QoS 0 bursts, 1 subscriber | 0.988 | 1.066 (six passes), 0.985 (twelve) |
| QoS 0 bursts, 8 subscribers | 0.986 | 0.997 |
| QoS 1, 1 pair | 1.002 | 0.992 |
| QoS 1, 8 pairs | 1.004 | 0.994 |
| QoS 0 blast, 1 (reported) | 1.025 | 1.170 |
| QoS 0 blast, 8 (reported) | 0.999 | 1.025 |

Both pass. B2 moved code of the frame encoder, and `blast` with one
subscriber read 1.170, a shape that ranged from 0.81 to 1.16 against itself
(report 0045). The instructions the broker retires were therefore counted
too (`tools/count_broker_instructions.sh`, 20 000 messages, three plays):
B1 438.1, 438.8 and 439.1 M; B2 436.6, 438.0 and 438.6 M. The medians are
0.17 % apart, B2 lower, within the spread of each side.

## Commands run

From `mqttiumd/`, one command after another, at each of the five
revisions unless a row says otherwise:

| Command | Result |
| - | - |
| `deslop-please --allow .deslop-please.allow` | `ok`; 38, 37, 30, 27, 24 entries |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, also with `--features mqttiumd-lab/kill-points`, and `-p mqttiumd --features test-hooks` | clean |
| `cargo test --workspace --locked` | 1381 passed |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked` | 1403 passed |
| `cargo test -p mqttiumd --features test-hooks --locked` | 288 passed |
| `mqttiumd-lab validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | exit 0 |
| `tools/build_corpus.py --check`, `tests/oracle/build_traces.py --check`, the oracle unit tests | exit 0 |
| `conformance/check.py --check` | exit 0 |
| `models/check_models.py` | exit 0 |
| `git apply --check` of every `tests/mutants/*/*.mutant` | all apply |
| `sha256sum -c` of the files `proofs/credits/BODY.sha256` names | all match |
| `tools/run_loop_mutants.sh`, on the simulated store and with `--backend sqlite` | every mutation rejected for its reason |
| `tools/run_pi_core_mutants.py`, `tools/run_expiry_mutants.py`, `tools/check_will_core_mutants.py` | every mutant detected |
| The digests of the 16 traces of `mqttiumd-lab`, against `9b7c05d` | the same |
| `tests/compat/run.py` against Mosquitto 2.1.4 (release build) | 135 scenarios: 105 same, 15 refused by the profile, 6 where the reference does not conform, 9 unspecified, 0 defects; 21 of 21 expectations pass |
| `tools/run_perf_gate.py` (`dist` builds), B1 and B2 | above |
| `tools/count_broker_instructions.sh`, B1 and B2 | above |
| `tools/run_secure_mutants.py`, `tools/run_policy_mutants.py`, B4 | fail before judging (below) |
| `tools/check_kill_points.sh`, B5 | exit 0: no manifest of the workspace turns them on |

## The mutants of the policy and of the secure listener

`tools/run_secure_mutants.py` and `tools/run_policy_mutants.py` were played
for B4. Both fail at their first case, before the mutation can be judged:
the drivers of `tests/system`, through `tcp_probe.read_events`, refuse an
event with more than 40 detail fields (`product_event_details`), and the
`ready` event of the broker has more. `run_secure_mutants.py` fails the
same way at `9b7c05d`, before lot 5: played from a clean checkout of that
revision, it gives `chain did not detect
invalid_client_certificate_established: product_event_details`.
`run_policy_mutants.py` was not played at `9b7c05d`; at B4 it fails in the
same receiver of `run_secure_mutants.py`, with the same code
(`reduction did not detect non_monotone_plan_accepted`). `tests/system` is
frozen evidence and is left as it is. Their mutation sites in `security.rs` are each found once (above).

## Not run, not shown

- The proofs (Kani): the files `BODY.sha256` hashes are unchanged.
- The mutations of `run_secure_mutants.py` and `run_policy_mutants.py`
  (above), and those whose anchors were already broken (report 0042).
- No qualifying campaign (`run --rebuild`).
