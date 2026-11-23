# QoS 0 paced per connection: a subscriber that reads receives a burst whole

This report records the correction of the loss report 0037 left open: a
subscriber that reads could lose the end of a QoS 0 burst. The repository
owner decided to correct the broker, and chose its sizes after a
measurement. Proposed record
[0063](../decisions/0063-qos0-paced-per-connection.md) holds the decision.

## Host and revisions

- **Revision**: branch `perf/lot7-crushing-mosquitto`, on top of `990a34d`:
  `0a77d44` (failing tests), `d29b356` (the fix), then record 0063 and
  this report.
- **Host**: Linux x86-64 container, `rustc 1.97.0`, loopback.
- **Reference**: Mosquitto 2.1.4 built by `tests/compat/build-reference.sh`.

## Reproduced first

Four tests were committed while they failed (`0a77d44`).

| Tier | Test | On the broker before the fix |
| - | - | - |
| Core | `product_qos0_burst_is_paced_by_the_window_and_resumes_in_order` | A hundred frames emitted at once for one connection |
| Runtime, output of the subscriber held by the test | `a_qos0_burst_waits_for_the_output_of_its_subscriber_and_keeps_its_order` | 36 of 100 QoS 0 publications dropped (`output_saturated`) |
| Runtime, the same | `durable_publications_to_a_qos0_subscription_wait_for_its_output` | 35 of 100 QoS 1 publications delivered to a QoS 0 subscription dropped, after the store consumed them |
| Executable, `--io-threads 1` | `a_reading_subscriber_receives_a_qos0_burst_whole` | Eight publishers, 125 messages each at once: the subscriber, which reads, received 300 to 450 of the thousand, in each of ten plays |

The second runtime test is a path report 0037 did not have: a durable
publication delivered to a QoS 0 subscription is consumed in the store
before its frame is emitted, so the dropped frame was a message the store
no longer owed.

## The fix

All in the core (`crates/mqttiumd-core/src/owner/pubsub.rs`), with one
constant of the runtime.

- The owner lets a connection have at most 224 volatile QoS 0 frames
  emitted and not yet written, failed or refused. Later deliveries of that
  connection wait in a queue of their own, in order, and leave as writes
  end. Connections that can be served are served one delivery in turn.
- A connection keeps at most 1,024 deliveries waiting; past it the newest
  is dropped with `output_saturated`. Deliveries that wait never hold more
  than half of the volatile queue or of its payload.
- At most 16 consumed QoS 0 deliveries of a connection are being written;
  the next is not consumed before a write ends.
- `net::OUTPUT_SLOTS`, the output queue of a connection, is 256 instead of
  64.

No input, effect, public type or drop reason is added.

## The sizes were changed by a measurement

The plan was a window of 32 frames on the queue of 64, the runtime
unchanged. `tools/measure_qos0_delivery.py` (new) sends a stream of QoS 0
messages of 64 bytes from one publisher and counts what each subscriber
receives: `bursts` writes bursts and waits for a PINGRESP after each,
`blast` writes everything at once. Share received by the least served
subscriber, three runs each, release executables:

| Shape | Before the fix | Window 32, queue 64 | Window 56, queue 64 | Window 224, queue 256 |
| - | - | - | - | - |
| Bursts of 100, 1 subscriber | 99.6 to 100 % | 90.6 to 91.1 % | 100 % | 100 % |
| Bursts of 1,000, 1 subscriber | 99.9 to 100 % | 57.4 to 60.2 % | 99.7 to 100 % | 100 % |
| Continuous, 1 subscriber | 97.6 to 99.9 % | 54.0 to 56.8 % | 97.5 to 99.0 % | 100 % |
| Continuous, 8 subscribers | 98.3 to 98.9 % | 54.5 to 55.3 % | 93.8 to 94.9 % | 100 % |

The window of 32 was a regression. The ends of writes reach the owner once
in a turn of the runtime, and a publisher adds up to 32 publications in a
turn: a connection that fell one turn behind could emit only what arrived,
never caught up, and its deliveries piled up to the bound and were
dropped. The owner chose the last column. Record 0063 states the reason
and names the two other forms as not chosen.

## The final build

Six shapes, three runs each, the executable of `990a34d` against the fix
(sha256 `4c82a071…`). "Received" is the least served subscriber, lowest of
the three runs; the rate is deliveries per second over all subscribers,
lowest to highest.

| Shape | Received, before | Received, after | Rate before | Rate after |
| - | -: | -: | - | - |
| Bursts of 100, 1 subscriber | 99,692 of 100,000 | 100,000 | 275 k to 281 k | 233 k to 257 k |
| Bursts of 1,000, 1 subscriber | 99,470 | 100,000 | 233 k to 272 k | 244 k to 296 k |
| Bursts of 100, 8 subscribers | 97,861 | 100,000 | 442 k to 523 k | 443 k to 507 k |
| Bursts of 100, 1 subscriber, one I/O thread | 99,978 | 100,000 | 267 k to 291 k | 240 k to 255 k |
| Continuous, 1 subscriber | 199,853 of 200,000 | 200,000 | 310 k to 330 k | 250 k to 296 k |
| Continuous, 8 subscribers | 196,534 | 199,648 | 571 k to 607 k | 561 k to 607 k |

The rates of one executable vary by about a fifth from one set of runs to
the next on this host, so the two single-subscriber shapes were played
again, the two executables in turn, six times each:

| Shape | Median rate before | Median rate after | Least received, before | Least received, after |
| - | -: | -: | -: | -: |
| Bursts of 100, 1 subscriber | 245,611 | 263,013 | 99,460 | 99,952 |
| Continuous, 1 subscriber | 305,796 | 321,769 | 193,107 | 198,637 |

Read together: no shape shows a rate that is lower beyond the variation of
the host, and the fix loses less in every shape. It still loses when the
subscriber is the slow side: three runs of the final build lost messages
(352 of 200,000, 48 of 100,000, 1,363 of 200,000). The subscribers of the
script are Python threads beside the publisher; when one falls more than
1,248 messages behind, the newest are dropped, as record 0063 says.

An intermediate build kept the queues of the connections in an ordered map
whose entry was created and removed at every frame of a quiet connection;
bursts of 100 to eight subscribers ran a fifth slower (404 k to 423 k
against 525 k to 534 k). The map is now the open-addressed one the core
already uses, and that shape runs as before.

## Compatibility bench

`publish/burst-qos0-v4` compares its count again (`tally`): report 0037
had made it a measured count. Played alone sixty times against Mosquitto
2.1.4: no difference. The whole bench: exit 0, 135 scenarios, 105 `same`,
30 classified.

## Tests added with the fix

- Core: `product_qos0_past_what_a_connection_keeps_waiting_the_newest_is_dropped`,
  `product_qos0_deliveries_that_wait_end_with_their_connection`,
  `product_qos0_serving_a_waiting_delivery_is_handed_back_at_each_boundary`.
- The tests committed failing were adjusted to the sizes that were chosen:
  the core test reads the window from the constant, and the runtime tests
  publish three hundred messages, more than the queue holds, on a table
  wide enough for them to wait. On the broker before the fix they fail as
  they did.

## Contract digest

`validate` prints the digest given under "Commands run"; it printed
`60a032e0…` in report 0037. `contracts/v1/requirements.json` follows
record 0063 and the amendment lines of records 0026, 0046 and 0048.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `-p mqttiumd --features test-hooks` | clean |
| `cargo test --workspace --locked` | pass: 85 result lines, 1379 tests, 0 failed |
| `cargo test -p mqttiumd --features test-hooks --locked` | pass: 15 result lines, 288 tests, 0 failed |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked`; `contracts sha256:21e96cc81c1f48d59841d8424fee2290102c6782b025222f2d6d2fe72ccf9bb5` |
| `python3 tools/build_corpus.py --check` | 624 derived files, 0 not up to date, 0 stale |
| `python3 tests/oracle/build_traces.py --check`; `python3 -m unittest discover -t tests/oracle -s tests/oracle/tests` | 405 files, 0 not up to date, 0 stale; 274 tests, OK |
| `tools/run_campaign.sh` on P1-02 (seed 1) and QABS-B (seed 7), `sim-store` and `sqlite` | four verdicts PASS, provisional: none is a rebuilt run |
| `tools/run_loop_mutants.sh` and `--backend sqlite` | every mutation expected to be rejected was rejected for its reason, on both stores (at `d29b356`) |
| `python3 -m unittest discover -s tests/compat -p "test_*.py"` | 64 tests, OK |
| `tests/compat/run.py` against Mosquitto 2.1.4, whole bench | exit 0 |
| `tests/compat/run.py --only 'publish/burst-qos0-v4'`, sixty times | exit 0 sixty times |
| `python3 tools/measure_qos0_delivery.py`, the shapes above | the tables above |

A run of the workspace suite made while two decision records were being
amended failed in `the_register_lists_exactly_the_accepted_decision_records`:
the register had not been rebuilt yet. It passed once it was.

The job `runtime` of the CI failed once on `990a34d`, in
`real_accept_reserves_both_future_close_facts_behind_a_held_lifecycle`,
which waits one second for a commit of the SQLite store. No Rust code had
changed since `2efe6e6`, where it passed; the test binary passed 25 times
in a row on this host, and the job passed when it was run again.

## Not run, not shown

- `proofs/` (Kani) on this host: its files are unchanged and the CI runs
  it.
- No qualifying campaign (`run --rebuild`).
- The measurements are those of one host with a Python client beside the
  broker. They compare executables; they are no capacity claim, and no
  measurement was made with TLS, with large payloads or on a loaded host.
- A retained replay to a QoS 0 subscription under a full queue was not
  played.
- No model has the window.
