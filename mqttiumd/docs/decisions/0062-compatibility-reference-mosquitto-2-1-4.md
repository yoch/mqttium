# 0062 — The reference of the compatibility bench is Mosquitto 2.1.4

Status: **Proposed** (23 November 2026). Applied to the compatibility
bench (`tests/compat/`, runtime tier, ADR 0044) and to its CI job. It
follows the external audit of `47e1d12`, which compared `mqttiumd` with
Mosquitto 2.1.4 while the bench was pinned to 2.0.21.
[Report 0036](../reports/0036-compatibility-bench-on-mosquitto-2-1-4.md)
holds the evidence. No code of the broker, public type, durable encoding
or schema changes.

## Context

Record 0049 names Mosquitto 2.0.21 as the broker the product is meant to
replace, and the bench refused any other version. The audit's observations
were made on 2.1.4, the version a migration starts from today: three of
them (the Will on a DISCONNECT with an error Reason Code, the PUBREL of an
expired message, the announced capabilities) could not be replayed in the
bench.

Mosquitto 2.1 requires cJSON to build and to run, and its make files build
tools the bench does not use.

## Decision

- **The bench is pinned to Mosquitto 2.1.4.** `tests/compat/harness.py`
  names the version and `run.py` refuses another one, as before.
- **`tests/compat/build-reference.sh` builds it**: the source archive of
  mosquitto.org, its SHA-256 checked before it is unpacked
  (`ca30b26ccd3041c0e11fc407fe4f2e62bc445e8d8dea94a083cc60c09bd5ea30`),
  then the common library, TLS broker and SQLite persistence plugin. The
  CI build installs the cJSON and SQLite development headers.
- **`preview/build-mosquitto-reference.sh` is not changed.** It builds
  2.0.21 for the frozen preview kit and historical comparisons. Active
  load comparisons use the pinned 2.1.4 build.
- **A difference is classified as record 0049 says.** The replay changed
  the classification as follows.

| Scenario | Before (2.0.21) | Now (2.1.4) |
| - | - | - |
| `errors/packet-at-announced-maximum-v5` | `reference-nonconformant`: Mosquitto served a packet over its announced Maximum Packet Size | `same`: Mosquitto 2.1.3 measures the whole packet and answers 0x95. The entry is removed |
| `connect/connack-capabilities-v5` | `profile-refused` | The same class; Mosquitto now also announces Maximum Packet Size 2,000,000 |
| `subscribe/sys-explicit-filter-v4` | `profile-refused` | The same difference; the payload of `$SYS/broker/version` names the version |
| `subscribe/overlapping-subscriptions-v4` | `same` | `unspecified`: Mosquitto now sends one copy per subscription by default (`allow_duplicate_messages`), as it did for MQTT 5 |
| `connect/bad-protocol-level-v4` | `same` | `unspecified`: Mosquitto answers a level it does not know with a CONNACK in the MQTT 5 layout, 0x84; mqttiumd with the 3.1.1 return code 1. Both close |
| `connect/keepalive-timeout-v4` | `same` | `same`, observed within ten seconds instead of five: Mosquitto 2.1 checks Keep Alive on a coarse period and closed a client with Keep Alive 1 after six seconds |

- **Nine scenarios are added**, for the audit's observations and for
  record 0061.

| Scenario | Result |
| - | - |
| `will/disconnect-normal-expiry-zero-v5`, `will/disconnect-with-will-expiry-zero-v5` | `same`: Reason Codes 0x00 and 0x04 with Session Expiry Interval 0 |
| `will/disconnect-unspecified-error-expiry-zero-v5`, `will/disconnect-implementation-error-expiry-zero-v5` | `reference-nonconformant` (MQTT-3.1.2-8, MQTT-3.14.4-3): on 0x80 and 0x83 Mosquitto ends the session and publishes no Will; mqttiumd publishes it (record 0058) |
| `publish/empty-topic-name-v4` | `same` |
| `publish/empty-topic-name-v5` | `unspecified`: both close; Mosquitto says 0x81, mqttiumd 0x82 (record 0058) |
| `session/redelivery-pubrel-after-expiry-v5` | `reference-nonconformant` (MQTT-4.4.0-1): upstream ticket 3744. Mosquitto does not send the PUBREL again once the Message Expiry Interval has passed; mqttiumd does |
| `session/ninth-exchange-after-reconnection-v4` | `reference-nonconformant` (MQTT-4.3.3-2, MQTT-4.5.0-1): upstream ticket 3809. Mosquitto acknowledges the ninth message with PUBREC, then PUBCOMP, and never delivers it; mqttiumd does not acknowledge it and closes (record 0061) |
| `session/ninth-exchange-after-reconnection-v5` | `reference-nonconformant` (MQTT-3.3.4-7, MQTT-4.13.1-1): Mosquitto answers PUBREC 0x97 and keeps the connection; mqttiumd sends DISCONNECT 0x97 and closes (record 0061) |

This amends record 0049: the reference is 2.1.4, and of its two standing
cases of `reference-nonconformant` the second, the packet over the
announced Maximum Packet Size, no longer stands.

## Campaigns and limits

Report 0036 preserves the original 135-scenario campaign. The current
matrix adds two graceful restarts on the same data directory, with
separate candidate expectations and the reference SQLite plugin for the
Will case. The nine scenarios introduced in this record also have
independent wire expectations. [Current matrix](../../tests/compat/MATRIX.md)

The listener-profile run checks mTLS certificate verification, exact and
subtree subscriptions and delivery on both brokers. It exercises the
candidate's explicit ClientID grants, and authenticates a password-file
user on the reference while the candidate reports that directive as
blocking. [Listener witnesses](../../tests/compat/listener_profiles.py)

The two brokers may route an incoming QoS 2 message at different moments.
The ninth-exchange scenarios observe deliveries after PUBREL. MQTT 5
section 4.3.3, figure 4.3 permits both routing points.
