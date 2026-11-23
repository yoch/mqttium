# Delivery and acknowledgement obligations: three partial verdicts completed

This report records wave 3 of the work on the external audit of `47e1d12`.
The audit left three upstream Mosquitto tickets as "partial" or "to verify"
on `mqttiumd`: 3809, 3687 and 3542. Each asks whether a message the broker
acknowledged, or an acknowledgement the broker owes, can be lost. Each now
has a test on this broker. One of the tests found a defect next to ticket
3809, fixed test first, and a second exposure that is left open. Proposed
record [0060](../decisions/0060-reading-behind-a-waiting-publication.md)
holds the decision.

## Host and revisions

- **Revision**: branch `perf/lot7-crushing-mosquitto`, on top of `7bdd5bd`.
- **Host**: Linux x86-64 container, `rustc 1.97.0`, development profile.
- **Storage**: durable mode (SQLite) for the tests on the executable; the
  real writer and a SQLite store in a temporary directory for the runtime
  tests.
- **Reference broker**: none. Mosquitto is not installed on this host.

## Verdicts

| Ticket | Question | Test | Result | Verdict |
| - | - | - | - | - |
| 3809 | A QoS 2 PUBLISH acknowledged with PUBREC and PUBCOMP and never delivered, when old exchanges of the session exhaust the receive quota | `stale_qos2_exchanges_of_a_3_1_1_session_do_not_stop_or_lose_a_publication` (`tests/reactor_wake.rs`) | 24 exchanges left at PUBREC on three connections of a 3.1.1 session. Each was delivered when it was acknowledged. A new publication is acknowledged and delivered once. A duplicate of an old PUBLISH is acknowledged again and not delivered again. The 24 old exchanges complete | Not reproduced; excluded by the order "PUBREC after the commit that routes" |
| 3687 | A PUBACK 0x00 for a message then dropped when a subscriber's queue overflows | `every_acknowledged_qos1_message_is_owed_to_and_reaches_each_subscriber` (`tests/reactor_wake.rs`) | 40 messages, each acknowledged 0x00; three QoS 1 subscribers, one reading, one connected and reading nothing, one offline. The reader has each message at once; the store holds 80 obligations for the two others; each receives the 40 in order when it comes back; no obligation is left | Not reproduced |
| 3542 | An UNSUBACK dropped when the client's output is full, although the subscription was removed | `an_unsuback_owed_while_the_output_is_full_is_delivered_when_room_returns` (`src/runtime_tests.rs`) | With the output lane full the unsubscription is committed, nothing leaves, the connection is not closed. Once room returns the client reads the UNSUBACK with its identifier: `0x00` for a filter it had, `0x11` for a filter it never had, nine codes for nine filters in one request | Not reproduced |

For 3687 the refusal at exhausted capacity (PUBACK 0x97, nothing admitted
for any subscriber) is asserted in the core
(`product_capacity_refusals`, `qos1_refusal_uses_a_puback_reason_and_retains_its_credit_until_terminal`)
and was not replayed on the executable: its accounts are 65,536 references
and 256 MiB of payload.

## A defect next to ticket 3809

The ticket describes MQTT 3.1.1 and a full receive quota. On this broker a
full window does not lose a message, but it stopped the connection.

- **Observation.** A 3.1.1 client sends twelve QoS 2 publications before it
  reads. It receives eight PUBREC, answers eight PUBREL, and receives no
  PUBCOMP: `after 0 PUBCOMP of 12: Resource temporarily unavailable` at the
  ten-second read timeout of the test.
- **Cause.** The runtime held the ninth PUBLISH for a slot of the window of
  eight and stopped reading the connection. A slot is freed by a PUBCOMP,
  which needs a PUBREL that is behind the held PUBLISH in the stream. Keep
  Alive ended the connection.
- **Clause.** MQTT 3.1.1 has no Receive Maximum; §4.3.3 and [MQTT-4.5.0-1].
- **Fix.** The reader goes on behind a waiting publication; the PUBREL of
  another exchange goes ahead, every other packet waits behind in order, up
  to 32 packets and the bytes of one largest packet (record 0060).
- **After.** Pipelines of 12, 20 and 41 exchanges complete, and the
  subscriber receives the messages once and in the order of publication.
- **Witness.** `clients_that_fill_their_announced_window_together_all_complete`
  (MQTT 5, a hundred clients) and
  `a_3_1_1_burst_beyond_the_receive_window_is_acknowledged_not_refused`
  (QoS 1) pass before and after.
  `only_the_pubrel_of_another_exchange_goes_ahead_of_a_waiting_publication`
  asserts the rule and its two bounds.

## An exposure left open

An inbound QoS 2 exchange at PUBREC keeps its lease of the core's exchange
table until its PUBREL. The window bounds a connection, not a session.

Observed with a temporary test, not committed, on the executable started
with `--max-connections 16`: one 3.1.1 session opens sixteen connections in
turn and leaves eight exchanges at PUBREC on each, 128 in all. On its
seventeenth connection its own PUBLISH is closed after 5.06 s. A QoS 1
PUBLISH of another client then gets no PUBACK and its connection is closed
after 5.01 s; a QoS 2 PUBLISH of a third client the same after 5.01 s. The
event of the close is `input_capacity`.

The table has `max(128, connections × 8)` entries: 32,768 at the default
limit, which the same session reaches with 4,096 connections. The
condition lasts as long as the session. It is recorded in
`docs/reception/open-conditions.md`; record 0060 says where a bound would
go. Nothing was changed for it.

## Contract digest

`validate` prints `contracts sha256:f6b6352be3edf6f98de10d1069fb78e297410c10f8cdf02f4916a6a81185bc92`;
it printed `ee48f76f…` in report 0032. The only contract file that changed
is `contracts/v1/requirements.json`: record 0046 gained its amendment line.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| `cargo clippy -p mqttiumd --all-targets --features test-hooks --locked -- -D warnings` | clean |
| `cargo test --workspace --locked` | pass: 83 result lines, 1346 tests, 0 failed |
| `cargo test -p mqttiumd --features test-hooks --locked` | pass three times in a row: 13 result lines, 263 tests, 0 failed |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked` |
| `python3 tools/build_corpus.py --check` | 624 derived files, 0 not up to date, 0 stale |

A first run of the instrumented suite failed in the new test for ticket
3687, on its last check ("obligations left after delivery"). The test
closed each subscriber's socket right after its last PUBACK, and an
acknowledgement written just before the end of a transport may not be
applied: the message stays owed, which QoS 1 allows. The test now keeps the
subscribers connected until the store owes nothing. The broker was not
changed for it.

## Not run, not shown

- No differential run against Mosquitto: the three tickets were not
  replayed on the reference.
- The conditions of ticket 3809 beyond 24 old exchanges, and those of the
  tickets on a loaded network, were not replayed.
- A client with more than 41 QoS 2 exchanges in flight still stalls until
  Keep Alive; no test holds that case.
- No model of the runtime has the waiting publication; TLC was not run in
  this wave.
- The oracle, the campaigns, the loop mutants and the Kani proof were not
  run: no file they read changed.
