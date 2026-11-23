# External audit of `47e1d12` against Mosquitto 2.1.4: wave 1

This report records what was done on branch `perf/lot7-crushing-mosquitto`
about the four gaps an external audit established on `mqttiumd` at
`47e1d1294491cfaec701a7ae0d86940dfaa64897`. The audit compared that commit
with Mosquitto 2.1.4 (package `2.1.4-0mosquitto1~trixie1`) and with the
upstream issue tracker of `eclipse-mosquitto/mosquitto`; its ticket numbers
are upstream numbers. The decisions are in Proposed record
[0058](../decisions/0058-client-disconnect-reasons-and-tls-close-notify.md).

Each defect was fixed test first: the discriminating test was committed
while it failed, then the fix. A point of the audit that is not listed here
was not worked on.

## Host and revisions

- **Base**: `47e1d12`, the audited commit and the head of the branch.
- **Host**: Linux x86-64 container, `rustc 1.97.0`, `cargo 1.97.0`,
  development profile, no instrumentation unless `test-hooks` is named.
- **Storage**: the runtime tests use the real writer and a SQLite store in a
  temporary directory; the binary tests use `--db` (durable mode).
- **Reference broker**: none. Mosquitto is not installed on this host: no
  differential run was made, against 2.0.21 or 2.1.4.

## Results by point

| Point | Clause | Observation before | Observation after | Witness kept | Verdict |
| - | - | - | - | - | - |
| DISCONNECT `0x80` / `0x83` with Session Expiry Interval 0 | MQTT 5 §§3.14.2.2.2, 3.1.2.11.2, 4.1; §3.1.2.5 [MQTT-3.1.2-8, -10] | The session and its subscription stay in the store; the client is answered with DISCONNECT `0x83` | Session and route gone from the store, no reply, one Will admitted and read on the monitor's socket, Session Present 0 at the next CONNECT, nothing delivered through the former subscription | `0x00` (no Will) and `0x04` (one Will) pass before and after | Reproduced, fixed |
| #3631, empty Topic Name without Topic Alias | MQTT 5 §§3.3.2.1, 3.3.4 | DISCONNECT `0x90` on the wire, at QoS 0, 1 and 2 | DISCONNECT `0x82` | A wildcard Topic Name, an empty Will Topic and an empty Response Topic keep `0x90`; a Topic Alias keeps `0x94` | Reproduced on the wire, fixed |
| #3629, Will QoS 3 | MQTT 5 §3.1.2.6 [MQTT-3.1.2-12] | `InvalidWillFlags`, reason `0x82` in the `packet_refused` event; no CONNACK | `ReservedWillQos`, reason `0x81` in the event; no CONNACK | Will QoS or Will Retain bits without the Will Flag stay `InvalidWillFlags`, `0x82` | Reproduced, fixed |
| #1204, TLS close | RFC 8446 §6.1 | The rustls client reads `UnexpectedEof` after its DISCONNECT, after the broker's DISCONNECT, and after its own close_notify | The client reads the end of the stream (`Ok(0)`) in the three flows | Reason `0x00` admits no Will, reason `0x04` admits one; a client that closes TLS without DISCONNECT gets its Will published | Reproduced, fixed |

The audit read #3631 in the code only; the wire test confirms it. For
#3629 the audit noted that no `0x82` had been shown on the network: none is
sent, before or after, because a CONNECT that is not validated to its end
supplies no response limit. The class is an internal fact and a field of
the event.

## What changed

- `crates/mqttiumd/src/codec.rs`: `disconnect` serves every reason code a
  client may send; `ReservedWillQos`; `EmptyTopicName` at the PUBLISH call
  site. `Error::in_session` and `disconnect_expiry` are removed.
- `crates/mqttiumd/src/net.rs`, `net/tls.rs`: `Connection::close` cancels a
  TLS task without shutting its socket; `tls::Task::finish` sends
  close_notify before `Control::stop`.
- `crates/mqttiumd/src/runtime.rs`: `close` leaves a TLS peer in the table
  until its task has finished.
- `models/takeover`: constant `Deferred`, mutation `DeferredWithoutWaiting`,
  properties `ClosedIsCancelled` and `RemovedIsShut`, six configurations.
- `conformance/server-statements.json`: rows MQTT-3.1.2-8, -12, MQTT-3.14.4-3,
  MQTT-4.1.0-2 and MQTT-4.7.3-1.
- Records 0029, 0034 and 0035 carry their "Amended by 0058" line; the index
  lists 0057 and 0058; `contracts/v1/requirements.json` is rebuilt for the
  three amended records.

## Contract digest

`validate` printed `contracts sha256:305db0c49a43b1d16269aeeca59aaf203c99521e19360f5dd93fdc7220fe37c8`
at `47e1d12` and prints `ee48f76f754069226c0a28f6aa577d60608be41e4609c53b75cc0c0ed7ab6a5b`
now. The only contract file that changed is `contracts/v1/requirements.json`,
rebuilt by `tools/build_corpus.py`: it records the SHA-256 of each decision
record, and records 0029, 0034 and 0035 gained their amendment line. No
schema, type, durable encoding or example changed.

## A design rejected by the model

The first design for #1204 cancelled the TLS task and let `finish_close`
remove the peer at once. `models/takeover` already rejects a close that
leaves the socket open (`mut-cancel-without-shutdown`): a write that passed
its check leaves on the old socket after the session is bound to the new
generation. The retained design keeps the peer in the table until the task
has shut its socket. The new mutant `mut-deferred-without-waiting` is the
rejected design and violates `OldClosedBeforeNewBound` and
`OldSilentOnceNewBound`.

## Tests

New or changed, all in `crates/mqttiumd`:

| Test | File |
| - | - |
| `every_legal_client_disconnect_reason_carries_its_expiry_and_publishes_the_will` | `src/codec.rs` |
| `a_nonzero_disconnect_expiry_is_decoded_for_the_runtime_context_rule` (replaces `zero_connect_context_rejects_any_nonzero_disconnect_before_profile_refusal`) | `src/codec.rs` |
| `reserved_will_qos_is_a_malformed_packet` | `src/codec.rs` |
| `an_empty_publish_topic_without_an_alias_is_a_protocol_error` | `src/codec.rs` |
| `will_malformed_and_duplicate_properties_keep_their_protocol_causes` (flags `0x08`, `0x10`, `0x20`; `0x1c` moved to the test above it) | `src/codec.rs` |
| `normal_disconnect_with_zero_expiry_ends_the_session_and_discards_the_will` | `src/runtime_tests.rs` |
| `disconnect_with_will_message_and_zero_expiry_ends_the_session_and_publishes_the_will_once` | `src/runtime_tests.rs` |
| `unspecified_error_disconnect_with_zero_expiry_ends_the_session_and_publishes_the_will_once` | `src/runtime_tests.rs` |
| `implementation_error_disconnect_with_zero_expiry_ends_the_session_and_publishes_the_will_once` | `src/runtime_tests.rs` |
| `a_disconnect_cannot_raise_a_zero_session_expiry_whatever_its_reason` | `src/runtime_tests.rs` |
| `a_tls_client_reads_close_notify_after_its_disconnect` | `src/runtime_tests.rs` |
| `a_tls_client_reads_the_broker_disconnect_and_then_close_notify` | `src/runtime_tests.rs` |
| `a_tls_client_that_sends_close_notify_first_is_answered_with_close_notify` | `src/runtime_tests.rs` |
| `an_empty_topic_name_is_a_protocol_error_and_a_wildcard_an_invalid_topic_name` | `tests/reactor_wake.rs` |
| `a_reserved_will_qos_is_closed_without_a_connack_and_recorded_as_malformed` | `tests/reactor_wake.rs` |

The zero-to-non-zero rule lost its codec test with `Error::in_session` and
gained a runtime test over four reason codes, which reads the server
DISCONNECT `0x82` on the socket.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| `cargo clippy -p mqttiumd --all-targets --features test-hooks --locked -- -D warnings` | clean |
| `cargo test -p mqttiumd --locked` | pass: 193 unit tests of the library, 3 of the binary and 45 in the nine integration files, 0 failed |
| `cargo test -p mqttiumd --features test-hooks --locked` | pass: 203 unit tests of the library, 3 of the binary and 45 in the nine integration files, 0 failed |
| `cargo test --workspace --locked` | pass, 0 failed. Corrected on 23 November 2026: the count first written here (39 test binaries, 768 tests) came from a summary command that truncated the output. Report 0032, one test file later, counts 83 result lines and 1341 tests |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked`, `contracts sha256:ee48f76f754069226c0a28f6aa577d60608be41e4609c53b75cc0c0ed7ab6a5b` |
| `python3 tools/build_corpus.py --check` | 624 derived files, 0 not up to date, 0 stale |
| `python3 conformance/check.py --check` (from the repository root) | pass; MQTT 5.0: 251 statements, 148 covered; MQTT 3.1.1: 139 statements, 83 covered |
| `python3 models/check_models.py` | every directory consistent |
| `python3 ../tools/formal/run_tlc.py --models-dir models/takeover` | 20 configurations, each with its declared outcome |

Failing first, on the commit of each test:

- `0x80` and `0x83`: `disconnect_0x80_expiry_zero_kept_the_session`,
  `disconnect_0x83_expiry_zero_kept_the_session`; the codec test fails on
  reason `0x80`.
- #3631: `left: [224, 2, 144, 0]`, `right: [224, 2, 130, 0]` on the wire.
- #3629: `left: (Some("InvalidWillFlags"), Some(130))`,
  `right: (Some("ReservedWillQos"), Some(129))` in the event.
- #1204: `left: ([], Err(UnexpectedEof))`, `right: ([], Ok(0))`, and the same
  after the frame `(224, [129, 0])` of the broker's DISCONNECT.

## Not run, not done

- No differential run against Mosquitto, and no new scenario in
  `tests/compat`: the bench is plain TCP, pinned to 2.0.21, and a scenario
  that differs from the reference needs its classification from a real run.
  Mosquitto 2.1.4 publishes no Will for `0x80` and `0x83` in the audit's
  traces; mqttiumd does, as the specification requires.
- The system receptions under `tests/system/` were not run. Their case
  `disconnect_outside_profile_reason` asserts the former refusal and will
  fail; they are frozen evidence of `51be4079`.
- The oracle, the campaigns, the loop mutants, the Kani proof and the other
  model directories were not run: no file they read was changed.
- No load measurement. A TLS close now costs one reactor turn before
  `finish_close`; its effect on a connection storm is not measured.
- No test drives a TLS takeover with a frame in flight on a chosen schedule
  of the two threads; `models/takeover` is the evidence for that order.
- #3761, #3809, #3687, #3542 and the other points of the audit are not
  touched by this wave.
