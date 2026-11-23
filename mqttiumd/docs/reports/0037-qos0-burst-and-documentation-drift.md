# A QoS 0 burst loses messages for a subscriber that reads; documents brought back to the code

This report records two follow-ups of the work on the external audit of
`47e1d12`. Report 0036 saw one compatibility scenario change its transcript
from one play to the next; its cause is found, the scenario is made stable
and the loss is recorded as open. The maintained documents that stated the
limits of an earlier profile as current are corrected. No code of the
broker changes.

## Host and revisions

- **Revision**: branch `perf/lot7-crushing-mosquitto`, on top of `2efe6e6`.
- **Broker**: release executable built from `476afc8`; the commits since
  then change no code of the broker.
- **Reference**: Mosquitto 2.1.4 built by `tests/compat/build-reference.sh`.
- **Host**: Linux x86-64 container, loopback.

## The loss

`publish/burst-qos0-v4`: a 3.1.1 subscriber at QoS 0, then a publisher
writes a hundred QoS 0 PUBLISH in one write. In some plays `mqttiumd`
delivered the first sixty-four and dropped the other thirty-six. Mosquitto
2.1.4 delivered the hundred in every play.

The mechanism, read in the code:

1. A QoS 0 frame that finds the output queue of its connection full is
   dropped: `Err(TrySendError::Full(_)) => Err(Qos0DropReason::OutputSaturated)`
   (`crates/mqttiumd/src/runtime.rs:5336`). The queue holds 64 frames
   (`net::OUTPUT_SLOTS`). Acknowledgements and QoS 1 and 2 publications
   that find it full are kept and sent when it has room.
2. That is the decided rule: record 0026 ("output saturation … lead to
   explicit owner Qos0Dropped decisions") and record 0046 ("QoS 0
   publications towards a saturated subscriber are still dropped and
   counted").
3. The core emits a burst without waiting for the queue. It takes at most
   32 packets of a peer in a turn, so the hundred publications are served
   in about four turns, each one emitting its deliveries completely.
4. The wake-ups of the I/O threads are held during a turn of the core
   (`net::hold_wakes`, `runtime.rs:785`). The subscriber's thread can
   drain the queue only between turns, when the system schedules it. When
   it does not run during the first three turns, the queue is full at the
   sixty-fifth frame.

So the subscriber reads, and still loses messages: the queue is the
broker's own, and what fills it is the order in which two of its threads
run. No document promised that a subscriber that reads receives a burst
whole; none said it could lose one either. The budgets of the core are far
from reached: its QoS 0 queue has 32,768 entries and 64 MiB.

The drop is counted. At the default event level it appears in the
`counters` record written about once a second (`qos0_dropped`); at
`--event-level audit` each drop is a record with the reason
`output_saturated`.

## Frequency

| Executable | Plays of the scenario alone | Plays with 64 of 100 delivered |
| - | -: | -: |
| `bb43a99` | 30 | 1 |
| `476afc8`, before this report | 30 | 0 |
| `476afc8`, for this report | 60 | 1 |

It was also seen once in four plays of the whole bench (report 0036). The
host was otherwise idle; a loaded host makes it more frequent, as the
bench's README says of the same instability in another scenario.

## What is done

- The scenario counts with `measure` instead of `tally`. The bench's
  README asks for it: "`tally` suits a count whose verdict does not depend
  on timing …; where a broker sometimes sends all and sometimes fewer, use
  `measure`." The transcript still compares that each message that came is
  one of those sent, came once, and in the order sent; the count is
  printed in the matrix for both brokers and not compared.
  `retained/forty-topics-v4` was converted the same way earlier.
- This is not a test weakened beside a change of behaviour: the broker
  does not change. What the scenario no longer does is fail when the loss
  happens. The loss is therefore written down where it is tracked:
  `docs/reception/open-conditions.md` has a new row, and
  `docs/migration-from-mosquitto.md` a row for the decision.
- In the sixty plays above the scenario differed 0 times; the one play
  with the loss shows as `100 | 100 | 64` in the measured counts.

## What is not done

The broker still drops. The correction that was read as sound is a pacing
of QoS 0 per connection in the core: no emission towards a connection
whose queue is full, resumed when the runtime reports room, a drop only
past a budget. Making the runtime refuse the frame for a later retry,
without the core knowing, would let one stalled subscriber fill the effect
box every connection shares. It is a change of the core tier and of the
contract, amends records 0026 and 0046, and has its own plan.

## Documents brought back to the code

`docs/product-tcp.md` gave, in the present tense, the limits of the bounded
profile of the earlier receptions, which `contracts/v1/product-profile.json`
freezes. Each value was read in the code; the readiness event
(`runtime.rs`, `mqttiumd ready`) lists most of them.

| Limit | The page said | The code has |
| - | -: | -: |
| `--max-connections` | 1 to 128, default 128 | 1 to 4,096, default 4,096 |
| Sessions | 128 | 4,096 |
| Subscriptions | 128 | 16,384 |
| Targets per publication | 16 | 1,024 |
| Packet read / announced, bytes | 65,536 / 32,776 | 1,114,112 / 1,048,584 |
| Payload, bytes | 32,768 | 1,048,576 |
| Input / output queue of a connection | 4 / 4 | 32 / 64 |
| Writer queue; physical receipts | 32; 8,192 | 1,024; 65,536 |
| Receipt window: issued, business, reserve | 1,024 / 512 / 512 | 4,096 / 3,584 / 512 |
| Outgoing table and QoS 0 queue | 128 entries, 4 MiB | 32,768 entries, 64 MiB |
| Retained topics and bytes | 32, 1 MiB | 4,096, 64 MiB |
| Replay candidates | 256 | 32,768 |
| Will intentions and bytes | 256, 8 MiB | 1,024, 32 MiB |
| SQLite schema, loopback / mTLS | 12 / 13 | 14 / 15 |
| Send buffer | 4,096 bytes requested | Not set; the effective size is recorded |
| Profile identifier | `tcp-qos0-exact-pubsub-v1` | `tcp-will-pubsub-v1`, `mtls-will-pubsub-v1` |

The section "Bounds and transport" now has a table with two columns, the
frozen profile and the executable today.

Other maintained documents:

- `conformance/server-statements.json`: the announced Maximum Packet Size
  in the note of MQTT-3.2.2-15.
- `docs/reception/open-conditions.md`: the row "The profile refuses what
  Mosquitto accepts" named bounds record 0051 raised and counted 26
  scenarios, 15 today; the row "Scale" described what record 0051
  implemented and the page already records below, and is replaced by the
  row of the QoS 0 burst; the formula of the exchange credits follows
  record 0061.
- Record 0048 (Accepted) has a dated note: the code has moved past its
  numbers under records 0051 and 0061, which are proposed. Its text is not
  rewritten.
- Record 0055 (Proposed) names the guarantee of the log mode as the
  executable prints it; record 0059 follows.
- Record 0052 (Proposed) has an amendment line for the third storage mode.

## Left as it is

- In `docs/product-tcp.md`, three statements were not checked against the
  code and are unchanged: the 128 positions and rows a receipt maintenance
  pass handles and its 2 MiB and 1 MiB budgets, the 129 probes of an
  outgoing Packet Identifier, and the 128 keyed timers of message expiry.
- Dated statements of records 0022, 0025, 0026, 0030 to 0034 and 0045
  ("initially", "in the product profile"), the reports, `docs/pilot/`,
  `preview/` and `contracts/v1/product-profile.json`.

## Contract digest

`validate` prints the digest given under "Commands run"; it printed
`a8f2d733…` in report 0036. `contracts/v1/requirements.json` follows the
notes and amendment lines of records 0048, 0052, 0055 and 0059. No schema,
type or durable encoding changed.

## Commands run

From `mqttiumd/`:

| Command | Result |
| - | - |
| `python3 -m unittest discover -s tests/compat -p "test_*.py"` | 64 tests, OK |
| `tests/compat/run.py --only 'publish/burst-qos0-v4'`, sixty times | exit 0 sixty times; counts `100 / 100` fifty-nine times, `100 / 64` once |
| `tests/compat/run.py` against Mosquitto 2.1.4, whole bench | exit 0: 135 scenarios, 105 `same`, 30 classified |
| `python3 tools/build_corpus.py --check` | 624 derived files, 0 not up to date, 0 stale |
| `python3 mqttiumd/conformance/check.py --check` (from the repository root) | pass |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked`; `contracts sha256:60a032e04b3a378907b152d28793df117930f7dab558aaa78b7ba8a52aa56897` |
| `cargo fmt --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| `cargo test --workspace --locked` | pass: 85 result lines, 1372 tests, 0 failed |

## Not run, not shown

- The loss was not reproduced under a chosen schedule of the threads; its
  mechanism is read from the code and from the count of sixty-four.
- No measurement with more subscribers, larger messages or a loaded host.
- The instrumented suite, the oracle, the campaigns and the proof were not
  run: no code they read changed.
