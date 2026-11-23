# The compatibility bench replayed against Mosquitto 2.1.4

This report records wave 4 of the work on the external audit of `47e1d12`:
the compatibility bench moves from Mosquitto 2.0.21 to 2.1.4, the version
the audit compared with, and gains scenarios for the audit's observations.
Proposed record
[0062](../decisions/0062-compatibility-reference-mosquitto-2-1-4.md)
holds the decision and the table of what changed.

## Host and revisions

- **Broker**: `mqttiumd` release executable built from `476afc8`
  (sha256 `4108b53a…`), default limits, durable mode.
- **Reference**: Mosquitto 2.1.4 built by `tests/compat/build-reference.sh`
  from the archive of mosquitto.org, SHA-256
  `ca30b26ccd3041c0e11fc407fe4f2e62bc445e8d8dea94a083cc60c09bd5ea30`,
  with cJSON 1.7.17 of the distribution (`libcjson-dev`).
- **Host**: Linux x86-64 container, Python 3, loopback.

The CI job `compat` was green on `bb43a99` against 2.0.21, so every change
below comes from the reference or from a new scenario.

## Result

135 scenarios: 105 `same`, 15 `profile-refused`, 6
`reference-nonconformant`, 9 `unspecified`, no `defect`. 21 expectations
on mqttiumd's transcript: 21 pass. `tests/compat/MATRIX.md` is the matrix
of the third replay.

| Change | Scenarios |
| - | - |
| A difference of 2.0.21 that 2.1.4 no longer has | `errors/packet-at-announced-maximum-v5` (entry removed) |
| The same difference, read again | `connect/connack-capabilities-v5`, `subscribe/sys-explicit-filter-v4` |
| New with 2.1.4, `unspecified` | `subscribe/overlapping-subscriptions-v4`, `connect/bad-protocol-level-v4` |
| A scenario widened | `connect/keepalive-timeout-v4`: ten seconds instead of five |
| New scenarios, `same` | `will/disconnect-normal-expiry-zero-v5`, `will/disconnect-with-will-expiry-zero-v5`, `publish/empty-topic-name-v4` |
| New scenarios, `unspecified` | `publish/empty-topic-name-v5` |
| New scenarios, `reference-nonconformant` | `will/disconnect-unspecified-error-expiry-zero-v5`, `will/disconnect-implementation-error-expiry-zero-v5`, `session/redelivery-pubrel-after-expiry-v5`, `session/ninth-exchange-after-reconnection-v4`, `-v5` |

## What the reference does, as read

- **Will on DISCONNECT 0x80 and 0x83.** Mosquitto 2.1.4 ends the session
  and publishes no Will; its source clears the Will for every Reason Code
  but 0x04 (`src/handle_disconnect.c`). mqttiumd publishes it, as
  MQTT-3.1.2-8 requires for every Reason Code but 0x00. The audit's
  observation is confirmed.
- **Upstream ticket 3744.** After a QoS 2 delivery reached PUBREL and the
  Message Expiry Interval passed, Mosquitto sends nothing when the session
  comes back; mqttiumd sends the PUBREL again. The audit's observation is
  confirmed.
- **Upstream ticket 3809.** With a window of eight
  (`max_inflight_messages 8`), a 3.1.1 session that owes eight PUBREL and
  publishes a ninth QoS 2 message receives PUBREC from Mosquitto, then
  PUBCOMP for its PUBREL, and the subscriber never receives the message.
  Mosquitto has a bound per session too; 3.1.1 cannot carry its refusal
  and the acknowledgement says the opposite. For MQTT 5 the same PUBLISH
  is answered PUBREC 0x97. mqttiumd does not acknowledge and closes
  (record 0061).
- **Keep Alive.** A client with Keep Alive 1 was closed by Mosquitto 2.1.4
  after 6.0 s, and one with Keep Alive 2 after 4.0 s (one measurement
  each, by hand): the check runs on a period of a few seconds. The
  scenario observes the close within ten seconds.
- **An incoming QoS 2 message is delivered at PUBREL** by Mosquitto and at
  PUBREC by mqttiumd. Both are allowed; noted because the new scenario
  had to read its subscriber after the PUBREL.

## Stability

The whole bench was played four times after the classification: exit 0
three times. One replay had one difference, on a scenario this work did
not touch: in `publish/burst-qos0-v4`, mqttiumd delivered 64 of the 100
QoS 0 messages written at once. Played alone thirty times on each
executable, the scenario differed 0 times on `476afc8` and 1 time on
`bb43a99`: the instability was there before this work. Sixty-four is the
size of a connection's output lane; a QoS 0 message that finds it full is
dropped, which MQTT allows. The bench's own rule is that a scenario whose
transcript changes from one run to the next is fixed or removed; neither
was done here.

## Commands run

From `mqttiumd/`:

| Command | Result |
| - | - |
| `sh tests/compat/build-reference.sh <new directory>` | digest OK, `mosquitto version 2.1.4` |
| `python3 -m unittest discover -s tests/compat -p "test_*.py"` | 64 tests, OK |
| `cargo build --release -p mqttiumd --locked` | built |
| `python3 tests/compat/run.py --reference … --broker target/release/mqttiumd --out … --update-fingerprints` | exit 0 |
| The same without `--update-fingerprints`, three times | exit 1 (the burst scenario above), 0, 0 |
| `python3 mqttiumd/conformance/check.py --check` (from the repository root) | pass |
| `python3 tools/build_corpus.py --check` | 624 derived files, 0 not up to date, 0 stale |

`validate` prints `contracts sha256:a8f2d73377a2a245e3618b2c10a7579bc470f98118e8feaa4c270be8df8c5e18`
(`cddc82af…` in report 0035): `contracts/v1/requirements.json` follows
record 0062 and the amendment lines of 0049 and 0058.

## Not run, not shown

- Upstream tickets 3759 and 3760 need a broker restart, and 3760 the
  `persist-sqlite` plugin: the bench has neither. They are not replayed.
- TLS, ACL and password files are outside the bench.
- The new scenarios carry no expectation: their classification is a
  reading, written in `divergences.json`.
- The load tools, the progression tool and the preview kit still use
  Mosquitto 2.0.21 and were not run.
- The CI job `compat` with its new step had not run when this report was
  written.
