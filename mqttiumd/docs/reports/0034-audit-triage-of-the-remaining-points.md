# Triage of the remaining points of the external audit of `47e1d12`

This report records wave 5 of the work on the external audit of `47e1d12`.
The audit listed 87 points to take up again. Eight were treated in reports
0031 to 0033. The 79 others are signalled behaviours of another broker,
taken from the open tickets of `eclipse-mosquitto/mosquitto`, that the
audit had verified in part on `mqttiumd` or left to verify. Each has a
verdict below. The ticket numbers are upstream numbers.

The verdicts say what this broker does. They say nothing about the other
broker: no differential run was made.

## Host and revisions

- **Revision**: branch `perf/lot7-crushing-mosquitto`, on top of `b632982`.
- **Host**: Linux x86-64 container, `rustc 1.97.0`, development profile.
- **Executable**: `target/debug/mqttiumd` over loopback TCP, durable mode.
- **Reference broker**: none.

## Verdicts

| Verdict | Points | Meaning |
| - | - | - |
| Not reproduced | 37 | A test on this broker plays the signalled behaviour and does not show it |
| Out of this environment | 32 | The condition of the ticket (a load, a platform, a network, a privilege) is not available here; nothing is concluded |
| Function absent | 6 | The mechanism the ticket needs does not exist in the profile: a matter for a migration, not a defect |
| Read, not replayed | 4 | The verdict rests on reading the code and its records; nothing was played |

No point of this list showed a defect of the broker. The defect and the
exposure found in wave 3 came from the tests for ticket 3809 (report 0033).

## Reason codes, classes and problem information

| Ticket | Title | Verdict | Evidence or missing condition |
| - | - | - | - |
| 3589 | Mosquitto MQTT v5 Fixed Header Flags Reason Code Mismatch | Not reproduced | `reserved_fixed_header_flags_are_a_malformed_packet_on_every_packet_type` (`tests/wire_classes.rs`): DISCONNECT 0x81 on nine packet types in MQTT 5, the end of the stream in 3.1.1 |
| 3613 | Mosquitto Uses DISCONNECT 0x82 Instead of 0x81 For DISCONNECT Reserved-Bit… | Not reproduced | Same test: a DISCONNECT with reserved flags is answered 0x81 |
| 3614 | CONNECT Reserved Flag Is Checked But Not Classified Through A CONNECT-Phase… | Not reproduced | `a_reserved_connect_flag_is_closed_without_a_connack_and_recorded_as_malformed` (`tests/wire_classes.rs`); the CONNACK is optional |
| 3696 | [BUG REPORT] Mosquitto MQTT 3.1.1 Protocol Non-compliance: Invalid CONNACK… | Not reproduced | `an_unknown_protocol_level_is_refused_with_return_code_one` (`tests/wire_classes.rs`): levels 0, 3, 6 and 255 |
| 3743 | A DISCONNECT carrying a reserved Reason Code is treated as a normal disconn… | Not reproduced | `an_illegal_disconnect_reason_is_a_protocol_error_and_publishes_the_will` (`tests/wire_classes.rs`): DISCONNECT 0x82 and the Will, for an undefined reason and for a server-only one |
| 3588 | Mosquitto Broker Receive Maximum Disconnect Behavior Missing | Not reproduced | `a_ninth_exchange_of_an_mqtt5_client_is_receive_maximum_exceeded` (`tests/wire_classes.rs`): DISCONNECT 0x93 |
| 3611 | Receive Maximum Exceeded Not Reported As MQTT 0x93 | Not reproduced | Same test. The window is fixed at eight; the limit of the ticket is another value |
| 3604 | Request Problem Information Constraint Not Enforced | Not reproduced | `no_property_is_sent_to_a_client_that_refused_problem_information` (`tests/wire_classes.rs`): PUBACK, PUBREC, PUBCOMP, SUBACK, UNSUBACK, the broker's DISCONNECT and a refusing CONNACK |
| 3612 | Request Problem Information Constraint Not Enforced | Not reproduced | Same test |
| 3633 | Request Problem Information = 0 Is Not Enforced on All Server Send Paths | Not reproduced | Same test, on error paths; the broker has no plugin that could add a property |
| 3634 | Capability Declaration and Problem Information Gating Are Incomplete | Not reproduced | Same test for the second part; the CONNACK announces what the profile offers (record 0049) |
| 3594 | Mosquitto MQTT v5 Optional ACK/DISCONNECT Properties Exceed Maximum Packet… | Not reproduced | The broker sends no optional property on any packet (same test), and `a_refusal_never_exceeds_a_validated_connects_tiny_packet_limit` (`src/runtime_tests.rs`) holds the limit of a refusal |
| 3635 | DISCONNECT User Property Maximum Packet Size Gate Is Missing on the Generic… | Not reproduced | Every DISCONNECT of the broker read in these tests is four bytes with an empty property list |
| 2681 | clients with packet identifier set to zero being rejected | Not reproduced | `a_packet_identifier_of_zero_is_a_protocol_error` (`tests/wire_classes.rs`). The broker refuses identifier 0, as the specification requires; a device that sends it is refused here too |
| 1440 | mqtt version 5 subback properties | Not reproduced | Every SUBACK of `tests/session_wire.rs` and `tests/wire_classes.rs` is compared whole: identifier, empty property list, one code per filter |
| 2555 | Callback `on_message` called multiple times when using MQTT v5.0 | Not reproduced | One copy for overlapping subscriptions is the choice of record 0049, held by the compatibility scenarios `subscribe/overlapping-subscriptions-*`; the client program of the ticket was not run |
| 3616 | Session Expiry Override Is Not Reported Back In CONNACK | Function absent | The broker never overrides a Session Expiry Interval and has no `persistent_client_expiration`: the CONNACK has nothing to report |

## Sessions, takeovers and QoS 2

| Ticket | Title | Verdict | Evidence or missing condition |
| - | - | - | - |
| 1650 | Mosquitto 1.6.9, MQTTv5, persistent session, session taken over, messages lost | Not reproduced | `a_session_taken_over_while_messages_flow_loses_none` (`tests/session_wire.rs`): two takeovers with publications in between; nothing lost, nothing acknowledged delivered again |
| 2976 | Broker ends up in an error state where it thinks there is an old connection… | Not reproduced | Same test: a third connection takes over at once a session whose connection was abandoned |
| 1760 | half open broker socket is replaying stale msgs after reconnection | Not reproduced | Same test for the takeover and the redelivery. The half-open radio link and its TCP deadlines were not replayed |
| 1767 | QOS > 1 connection stop as soon as one packet is not released | Not reproduced | `a_delivery_that_is_never_released_does_not_stop_the_other_topics` (`tests/session_wire.rs`) |
| 2553 | LWT not send on multiple connects | Not reproduced | `a_second_connect_and_a_takeover_each_publish_the_will_once` (`tests/session_wire.rs`) |
| 904 | LWT and client takeover - mosquitto broker | Not reproduced | Same test, with a QoS 1 Will; the QoS 0 Will of the ticket is refused by the profile (record 0034) |
| 1165 | mosquitto sends Online as LWT if broker and client died but only the server… | Not reproduced | `a_kill_with_a_client_connected_publishes_its_will_after_the_restart` (`tests/disconnect_durability.rs`): the registered payload, once |
| 2811 | The data has been persisted, and the client can only receive the latest per… | Not reproduced | `queued_messages_survive_a_kill_with_one_delivery_in_flight` (`tests/disconnect_durability.rs`): three messages, a kill of the process; a power cut is not a kill (report 0032) |
| 3056 | Mosquitto is not publishing all received message (since Mosquitto 2.0.16) | Not reproduced | `a_3_1_1_client_with_more_qos2_exchanges_than_the_window_completes_them_all` (`tests/reactor_wake.rs`): 12, 20 and 41 publications before any PUBREL, delivered once and in order. This test found the defect of record 0060 |
| 3294 | QoS 2 messages reach the broker but don't get forwarded | Not reproduced | `seventy_clients_in_rounds_have_every_qos2_message_forwarded_once` (`tests/session_wire.rs`): 420 messages |
| 3599 | Mosquitto Broker QoS2 Duplicate PUBLISH PUBREC Handling Incomplete | Not reproduced | `repeated_duplicates_of_a_qos2_publish_are_acknowledged_and_delivered_once` (`tests/wire_classes.rs`) |
| 3602 | QoS 2 Duplicate PUBLISH Before PUBREL Is Disconnected After Repeated Retries | Not reproduced | Same test: six sendings, six PUBREC, one delivery, the connection goes on |
| 3699 | esp-mqtt clients with MQTT 5 on unreliable networks trigger disconnect erro… | Not reproduced | Same test: a PUBLISH repeated on an open MQTT 5 connection is acknowledged, not refused |
| 3750 | Inverted quota check in db__message_release_incoming() promotes all queued… | Not reproduced | `stale_qos2_exchanges_of_a_3_1_1_session_do_not_stop_or_lose_a_publication` (`tests/reactor_wake.rs`): 24 restored exchanges, three times the window. The window cannot be lowered by configuration |
| 2625 | IMPORTANT) QoS 2 Receive is weired | Not reproduced | A single QoS 2 publication reaches its subscriber with no other traffic: the publication `new` of the test above, and `a_publication_is_delivered_without_waiting_for_a_timer` (`tests/reactor_wake.rs`). The client loop of the ticket was not run |
| 3202 | persistent session do not receive missed messages | Not reproduced | `every_acknowledged_qos1_message_is_owed_to_and_reaches_each_subscriber` (`tests/reactor_wake.rs`) for an offline session; `per_listener_settings` does not exist here |
| 2349 | Mosquitto Broker dropping some messages after subscriber reconnects | Out of this environment | An hour at a hundred messages per second, then a QoS 2 backlog. The short resumptions above pass; that load was not run |
| 2519 | Delay in subscribing persistent messages with clean_session=False | Function absent | MQTT 3.1 is not offered. The catch-up time of the ticket was not measured in another version |

## Retained messages

| Ticket | Title | Verdict | Evidence or missing condition |
| - | - | - | - |
| 2402 | Retain Handling option 0 not working after resubscribe | Not reproduced | `a_retained_message_is_sent_again_after_unsubscribe_and_resubscribe` (`tests/session_wire.rs`): Retain Handling 0, 1 and 2 |
| 3220 | Topics not shown when using a wildcard | Not reproduced | `a_retained_message_is_found_through_every_matching_filter_every_time` (`tests/session_wire.rs`): nine filters, forty rounds |
| 3771 | retained message not received on re-subscription if the broker is busy with… | Not reproduced | `a_retained_message_is_sent_while_large_publications_flow` (`tests/session_wire.rs`): 24 publications of 256 KiB to another client |
| 2887 | messages are no longer retained with 2.0.16+ (with a lot of reained topics?) | Not reproduced | Retained messages survive a restart (`retained_publish_properties_survive_volatile_snapshot_and_durable_restart`, `tests/publish_properties.rs`). An upgrade from a Mosquitto database is another matter: none is read |
| 3096 | Wildcard subscription for a topic with a large number retained messages | Out of this environment | 40,000 retained topics. The profile holds 4,096 (`MAX_RETAINED_TOPICS`) and refuses the next; that scale does not exist here |
| 2538 | 1.5.8 max_queued_bytes and max_inflight_bytes unexpected behaviour | Function absent | `max_queued_bytes` and `max_inflight_bytes` do not exist; the 12,000 retained messages of the ticket exceed the 4,096 of the profile |

## Clock and counters

| Ticket | Title | Verdict | Evidence or missing condition |
| - | - | - | - |
| 1460 | Invalid client count - max_connections exceeds | Not reproduced | `the_connection_limit_stays_exact_after_many_connections_came_and_went` (`tests/reactor_wake.rs`) |
| 2205 | MQTT clients got disconnected after manual date time change using "date -s" | Out of this environment | A change of the civil time. The delays of the broker use monotonic clocks (`Instant`, and `CLOCK_BOOTTIME` for the session deadlines of record 0030); the jump itself needs a privilege this host does not give |
| 2682 | time adjust makes mosquitto freeze | Out of this environment | Same condition |

## Storage and restart

| Ticket | Title | Verdict | Evidence or missing condition |
| - | - | - | - |
| 2934 | Mosquitto: Persistent Client Subscription info is not considered after SIGH… | Function absent | SIGHUP ends the process: there is no reload. A persistent session survives a stop and a restart (`tests/disconnect_durability.rs`) |
| 1558 | Unable to save in-memory data in docker container: Invalid Argument | Out of this environment | The file system of the container volume of the ticket |
| 2469 | Persistent database file corrupted after power loss on Windows | Out of this environment | A power loss on Windows. SQLite replaces the `mosquitto.db` file of the ticket; no power loss was produced (report 0032) |
| 3159 | Database corruption caused by power loss | Out of this environment | Same condition |

## Security, identity and distribution

| Ticket | Title | Verdict | Evidence or missing condition |
| - | - | - | - |
| 3551 | Persistent session state can be destroyed by a spoofed CONNECT with Clean S… | Read, not replayed | On the mTLS listener a ClientID belongs to the certificate that owns it (record 0035). The anonymous listener gives no such protection: any client can take a ClientID |
| 3553 | Authenticated user can hijack another user's session via ClientID and exfil… | Read, not replayed | Same reading |
| 1387 | Not able to deny subscription to topic "#" | Read, not replayed | The grants of an identity are exact filters or subtrees (record 0035); a client is given only what its identity names. Not a reading of a Mosquitto ACL file |
| 647 | Error logging found in stream to client (security issue) | Read, not replayed | Events are written to a file opened by its path, one line per write; nothing is written to a transport but MQTT bytes. The descriptor reuse of the ticket was not replayed |
| 1283 | Mosquitto broker and client default to TLS_AES_256_GCM_SHA384 with tls1.3 | Function absent | No `ciphers` directive: TLS 1.3 only, with the suites of the TLS library |
| 2898 | ssl problem getting key 'private.key' 335544427: error:1400006B:UI routines… | Out of this environment | The key file of the ticket. The broker reads a PEM private key; that file and its protection were not tried |
| 3718 | Shipped container configuration: HTTP API bound to all interfaces, and SBOM… | Function absent | No HTTP listener. The inventory of the distributed image was not checked |

## Load, latency, memory and platforms

All 24 are out of this environment: no load profile, no other
platform and no network of the tickets was available. Where the broker has
a bound or a test that bears on the symptom, the row names it; it does not
close the point.

| Ticket | Title | Condition of the ticket, and what bears on it here |
| - | - | - |
| 1616 | Tuning for real time continuous message delivery | Publications 20 ms apart arriving in groups. Delivery does not wait for a timer on loopback (`a_publication_is_delivered_without_waiting_for_a_timer`); the network of the ticket was not replayed |
| 3117 | QoS 2 handshake messages (PUBREC, PUBREL, PUBCOMP) delayed up to 1 minute | QoS 2 acknowledgements delayed up to a minute on the reporter's network. On loopback every exchange of these tests completes within its ten-second read timeout |
| 1912 | Delayed ACK (40ms) and problems with splitted messages | Delayed TCP ACKs and split segments. `TCP_NODELAY` is set and the parser takes a packet in any split (`tests/codec_properties.rs`); the kernel behaviour was not replayed |
| 2807 | 100% CPU Usage When Client Disconnects | A core busy after an mTLS client disconnects. `an_idle_broker_rests` counts the wake-ups of an idle broker on plain TCP; no TLS case on the executable |
| 2813 | '@alwashe How did you resolve this one? I'm also investigating an issue sim… | A log file that stops in the middle of a line after a long run. Each event is one write of a whole line; no long run was made |
| 3237 | Mosquitto (2.0.18) broker crashes out of memory (OOM) when retaining messages | Out of memory at 500,000 retained topics. The profile holds 4,096 and 64 MiB and refuses the next |
| 3273 | Mosquitto consumes much memory while "Saving in-memory database to /path/to… | Memory during the periodic save of a large database. The durable mode has no such save; the snapshot of the volatile mode was not measured |
| 3110 | Large persist database causes client to disconnect | A Unix-socket client disconnected while a large database is saved. No Unix-socket listener, and no periodic save in durable mode |
| 2095 | mosquitto server taking 97-100% cpu . and cant connect to it | CPU at 97 to 100 % and refused connections under the reporter's load |
| 2101 | Connection and reception are slowed by FIN_WAIT | Connections slowed by sockets in FIN_WAIT |
| 2182 | Mosquitto consume too much CPU and memory | CPU and memory under the load of the ticket |
| 2267 | Mosquitto can not release memory to system | Virtual memory not returned after six persistent sessions drain |
| 2294 | memory is not released to OS after message dequeue. | Memory not returned to the system after messages are dequeued |
| 2455 | Mosquitto memory consumption reaching 100% | Memory reaching 100 % |
| 2539 | Inconsistent message latency from client to client | Uneven latency between clients |
| 2656 | Question：packet__queue function maybe have a memory leak | A suspected leak in a queue function of the other broker |
| 2830 | Memory leak, when cpu usage reaches 100% during connection of many clients | Memory when many clients connect at 100 % CPU |
| 2971 | Inconsistent timing of messages clocked to the second (1110 ms) | Messages clocked to the second arriving 1,110 ms apart |
| 3008 | High Load mosquitto-2.0.15_1 on FreeBSD 13.2 | High load on FreeBSD 13.2 |
| 3641 | Segmentation fault with mosquitto 2.1.2 on FreeBSD | A segmentation fault on FreeBSD |
| 3499 | random segmentation fault: [Tue Feb 10 03:16:09 2026] mosquittov2[3614]: se… | A random segmentation fault in the reporter's environment |
| 2958 | Tests "v3 client message" and "v3 sub" fail on arm | Two tests failing on 32-bit ARM |
| 3409 | indows binary fails to persist messages even with persistence enabled | Persistence failing on a Windows binary |
| 3076 | mosquitto 2.0.18 - Job for mosquitto.service failed because a timeout was e… | A systemd start timeout |

## What the tests showed beside the verdicts

Three observations are not defects and are worth knowing.

- **An acknowledgement written just before the end of a transport may not
  be applied.** A PUBACK the client writes and then closes, or that
  precedes a takeover, can be lost with the connection: the message stays
  owed and is delivered again, marked DUP. QoS 1 allows it. Once the broker
  has answered a later packet of the same connection (a PINGRESP in these
  tests), the acknowledgement is taken and the message is not delivered
  again.
- **A message the broker had sent, and the client had not read, comes back
  marked DUP.** After a kill or a takeover the broker marks as a redelivery
  what it had started to send, whether or not the client read it. The
  client cannot tell the difference.
- **A PUBREC for a publication without subscriber is plain**, where the
  PUBACK carries 0x10. This is the choice of the commit `921df9d`.

## One fix beside the list: `check-config`

The audit observed, outside the 79 points, that a minimal configuration
passed `mqttiumd check-config` and was refused at launch for want of an
event journal. Reproduced: `ok: true`, then `mqttiumd: --events is
required`, status 2. The report of the checker now names in
`launch_requires` the options a launch with the file alone still needs
(`--events`, and `--db` where the storage has a database); `ok` keeps its
meaning. Fixed test first
(`check_config_names_what_the_launch_still_needs_from_the_command_line`,
`tests/mosquitto_conf.rs`); record 0054 says so.

What a deployment outside the profile needs before it can move is listed,
as decisions to take, in [`docs/migration-from-mosquitto.md`](../migration-from-mosquitto.md).

Two rows of the conformance register (MQTT-3.14.1-1, both versions) said
that no test sent a DISCONNECT with reserved bits and that MQTT 5 was
closed without an answer. The new test reads DISCONNECT 0x81: the rows are
now `covered`.

## Tests added in this wave

| File | Tests |
| - | - |
| `crates/mqttiumd/tests/wire_classes.rs` | 8 |
| `crates/mqttiumd/tests/session_wire.rs` | 7 |
| `crates/mqttiumd/tests/disconnect_durability.rs` | 2 more |
| `crates/mqttiumd/tests/reactor_wake.rs` | 1 more |
| `crates/mqttiumd/tests/mosquitto_conf.rs` | 1 more |

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| `cargo clippy -p mqttiumd --all-targets --features test-hooks --locked -- -D warnings` | clean |
| `cargo test --workspace --locked` | pass: 85 result lines, 1365 tests, 0 failed |
| `cargo test -p mqttiumd --features test-hooks --locked` | pass: 15 result lines, 282 tests, 0 failed |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked`, `contracts sha256:f6b6352b…`, unchanged since report 0033 |
| `python3 tools/build_corpus.py --check` | 624 derived files, 0 not up to date, 0 stale |
| `python3 models/check_models.py` | every directory consistent |
| `python3 conformance/check.py --check` (from the repository root) | pass |

Each new file was run several times in a row in both configurations while
it was written: `wire_classes.rs` four times, `session_wire.rs` nine times,
`disconnect_durability.rs` nine times.

Four expectations of the new tests were wrong when first written and were
corrected to what the specification allows, without a change of the
broker: the DUP flag of messages the broker had sent and the client had
not read (twice), a redelivery after an acknowledgement that had not been
taken, and the reason of a PUBREC without subscriber.

## Not run, not shown

- No differential run against Mosquitto.
- No load, no other platform, no TLS on the executable.
- The points read and not replayed stay what they are: a reading.
