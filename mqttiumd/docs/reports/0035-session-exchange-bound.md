# A session's QoS 2 exchanges are bounded, and an exchange at PUBREC holds no payload

This report records the fix of the exposure report 0033 left open: one
session could hold the inbound exchanges of every client. The repository
owner decided a bound per session in the admission of the core and a
table sized for the sessions. Proposed record
[0061](../decisions/0061-session-exchange-bound.md) holds the decision.
The work found a second path of the same exposure, through the payload
bytes, and closed it with the first.

## Host and revisions

- **Revision**: branch `perf/lot7-crushing-mosquitto`, commits `1f6b510`
  (failing tests), `3145de4` (fix), `476afc8` (a test helper), `4498d5f`
  (record 0061), on top of `bb43a99`.
- **Host**: Linux x86-64 container, `rustc 1.97.0`.
- **Storage**: durable mode (SQLite) for the tests on the executable.

## The two paths, reproduced first

Two tests on the executable were committed while they failed
(`crates/mqttiumd/tests/reactor_wake.rs`, `--max-connections 16`). In
both, a client that held nothing published one QoS 1 message and was
closed by the broker instead of acknowledged.

| Test | What holds the resource | Before the fix |
| - | - | - |
| `one_session_cannot_hold_the_inbound_exchanges_of_every_client` | One 3.1.1 session leaves eight QoS 2 exchanges at PUBREC on each of sixteen connections: the 128 entries of the table | The other client is closed |
| `exchanges_left_at_pubrec_do_not_hold_the_payload_bytes_of_every_client` | Eight sessions leave eight exchanges of 1 MiB at PUBREC: the 64 MiB the core lends to admissions | The other client is closed |

The second path was not in report 0033. The lease of a live exchange kept
the bytes of its payload until PUBCOMP, while the lease of an exchange
recovered after a restart holds none.

## The fix

- `OwnerConfig::session_qos2_exchanges` bounds the inbound QoS 2 exchanges
  of one session across its connections; the product sets eight. At the
  bound a QoS 2 publication waits while the owner is admitting or
  releasing one of them, and its connection is closed
  (`CapacityExceeded`) when every one awaits a PUBREL.
- The exchange table of the product has `SESSIONS × 8` = 32,768 entries
  whatever `--max-connections`.
- An exchange that comes to await its PUBREL keeps a lease of one
  exchange and no byte (`LeaseTable::lighten`).
- An MQTT 5 connection the core closes for capacity is sent DISCONNECT
  0x97 first.

## A first form of the bound was wrong

The first form counted every exchange of the session, QoS 1 included. The
instrumented suite refused it:
`a_new_identifier_progresses_while_the_old_core_ack_receipt_is_held`
holds the receipt the core gets of a PUBACK already on the wire, and
requires the next publication to progress (record 0024). Counting the
QoS 1 exchange made that publication wait for the held receipt. The bound
now counts QoS 2 exchanges only: a QoS 1 exchange ends by the broker's
own PUBACK and cannot be held by a client. The test was not changed.

## Tests that changed with the behaviour

- Three assertions of the core stated the bytes of an exchange at PUBREC
  (`(7, 1)`); they state `(0, 1)`
  (`rule_inbound_qos2_admission_pending_committed_authorize`,
  `rule_inbound_qos2_release_pending_rejected_prepare_again`,
  `qos1_and_qos2_share_active_packet_identifiers_but_not_old_frame_receipts`).
- The test of upstream ticket 3809 of report 0033
  (`stale_qos2_exchanges_of_a_3_1_1_session_do_not_stop_or_lose_a_publication`:
  twenty-four exchanges left at PUBREC by one session, all acknowledged)
  stated the behaviour this fix ends. It is replaced by
  `stale_qos2_exchanges_of_a_3_1_1_session_are_bounded_and_lose_no_publication`:
  eight exchanges are kept and were delivered; a duplicate of an old
  PUBLISH is acknowledged and not delivered again; a ninth is not
  acknowledged and the connection is closed with `capacity_exceeded`;
  after the eight PUBREL the publication is accepted and delivered once.
- New: `an_mqtt5_session_that_owes_its_window_of_pubrel_is_told_quota_exceeded`
  (a QoS 1 publication is served, a ninth QoS 2 exchange is answered
  DISCONNECT 0x97), and in the core
  `a_session_holds_a_bounded_number_of_exchanges_across_its_connections`,
  `an_exchange_that_awaits_its_pubrel_holds_no_payload`,
  `a_lightened_lease_returns_its_bytes_and_keeps_its_slot`,
  `lightening_never_grows_a_lease_and_changes_nothing_it_cannot_do`.

## Memory

Resident memory of the release executable at readiness, three launches
each, durable mode (`VmRSS` of `/proc/<pid>/status`, kB):

| `--max-connections` | `bb43a99` | With the fix |
| - | - | - |
| 16 | 10,760 / 10,480 / 10,448 | 13,216 / 13,368 / 13,240 |
| 4,096 (the default) | 13,212 / 13,480 / 13,348 | 13,096 / 13,244 / 13,244 |

With sixteen connections the broker now reserves the table of the default
limit: about 2.8 MB more resident, and 43 MB more address space
(`VmSize` 488,708 to 531,984 kB). The default is unchanged.

## Contract digest

`validate` prints `contracts sha256:cddc82afeb9ee2ed3e673ec5b7e63ea2a18ba8404cd144d90ef2e9ce7133da09`
at `4498d5f`; it printed `f6b6352b…` in report 0033. The only contract
file that changed is `contracts/v1/requirements.json`, for record 0061
and the amendment lines of 0046, 0051 and 0060. `OwnerConfig` is a public
type of the core and has one more field.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| `cargo clippy -p mqttiumd --all-targets --features test-hooks --locked -- -D warnings` | clean |
| `cargo test --workspace --locked` | pass: 85 result lines, 1372 tests, 0 failed |
| `cargo test -p mqttiumd --features test-hooks --locked` | pass: 15 result lines, 285 tests, 0 failed |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked` |
| `python3 tools/build_corpus.py --check` | 624 derived files, 0 not up to date, 0 stale |
| `python3 tests/oracle/build_traces.py --check` | 405 files, 0 not up to date, 0 stale |
| `python3 -m unittest discover -t tests/oracle -s tests/oracle/tests` | 274 tests, OK |
| `tools/run_campaign.sh` on P1-02 (seed 1) and QABS-B (seed 7), each on `sim-store` and `sqlite` | four verdicts PASS, provisional: none is a rebuilt run |
| `tools/run_loop_mutants.sh` and `--backend sqlite` | every mutation expected to be rejected was rejected for its reason, on both stores |

A first full run of the workspace suite failed once in
`twelve_offline_deliveries_drain_without_a_discovery_tick_per_message_or_page`:
its helper parsed a line of `events.jsonl` the broker was still writing.
The helper now counts complete lines (`476afc8`); the test alone passed
six times before the change and the suite passed after it.

## Not run, not shown

- `proofs/` (Kani) was not run on this host; its files are unchanged and
  the CI job `pilot proof` runs it. `LeaseTable::lighten` is outside that
  proof and is covered by unit tests.
- No qualifying campaign (`run --rebuild`) was played.
- The bound was not exercised with thousands of sessions, each at its
  bound; the table is sized for that case and no test fills it.
- No model has the bound.
