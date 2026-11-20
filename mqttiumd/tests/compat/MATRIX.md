# Compatibility matrix: mqttiumd against Mosquitto 2.0.21

Written by `tests/compat/run.py`: regenerate it with the command in
`tests/compat/README.md` and copy `matrix.md` here; do not edit it by hand.

- broker: `mqttiumd` built from commit `aafd408facccf18629ea7db051ee8b71db4bd607`
- reference: Mosquitto 2.0.21, the version `preview/build-mosquitto-reference.sh` builds
- scenarios played: 114

`same` means equal normalized transcripts on the scenario played, nothing
more. A difference is not a failure of mqttiumd by itself: Mosquitto is not
the oracle, the MQTT specification is. Each difference is classified in
`divergences.json`:

- `profile-refused`: mqttiumd deliberately does not support the feature yet;
- `reference-nonconformant`: Mosquitto departs from the specification; mqttiumd follows it;
- `defect`: mqttiumd is wrong or needlessly different: a to-do for the product;
- `unspecified`: both behaviours are allowed by the specification.

## Summary

114 scenarios: 77 `same`, 26 `profile-refused`, 1 `reference-nonconformant`, 4 `defect`, 6 `unspecified`.

| Area | same | profile-refused | reference-nonconformant | defect | unspecified | total |
| - | -: | -: | -: | -: | -: | -: |
| connect | 18 | 7 | 0 | 1 | 0 | 26 |
| session | 9 | 0 | 0 | 0 | 0 | 9 |
| subscribe | 10 | 6 | 0 | 0 | 3 | 19 |
| publish | 15 | 1 | 1 | 2 | 3 | 22 |
| retained | 11 | 1 | 0 | 0 | 0 | 12 |
| properties | 4 | 5 | 0 | 0 | 0 | 9 |
| will | 8 | 4 | 0 | 0 | 0 | 12 |
| errors | 2 | 2 | 0 | 1 | 0 | 5 |
| **all** | 77 | 26 | 1 | 4 | 6 | 114 |

## Scenarios

| Scenario | MQTT | Result | Note |
| - | - | - | - |
| `connect/authentication-method-v5` | 5.0 | defect | Both refuse the CONNECT and close; Mosquitto with CONNACK 0x8C (Bad authentication method), mqttiumd with CONNACK 0x83 (Implementation specific error). |
| `connect/bad-protocol-level-v4` | 3.1.1 | same | CONNECT with the protocol name MQTT and the unknown level 6: return code 1, then closed. |
| `connect/bad-protocol-name-v4` | 3.1.1 | same | CONNECT with the protocol name MQTX and level 4. |
| `connect/clean-start-discards-session-v4` | 3.1.1 | same | Clean Session 1 discards a kept session: Session Present 0, the old subscription no longer routes, and nothing is left for a later resume. |
| `connect/clean-start-v4` | 3.1.1 | same | A first CONNECT with Clean Session/Clean Start 1 is accepted with Session Present 0. |
| `connect/clean-start-v5` | 5.0 | same | A first CONNECT with Clean Session/Clean Start 1 is accepted with Session Present 0. |
| `connect/connack-capabilities-v5` | 5.0 | profile-refused | Mosquitto announces Receive Maximum 20 and Topic Alias Maximum 10 and leaves every other capability at its default; mqttiumd announces Receive Maximum 8, Maximum Packet Size 65 536, Topic Alias Maximum 0, and shared subscriptions and Subscription Identifiers not available. |
| `connect/disconnect-v4` | 3.1.1 | same | After DISCONNECT the broker closes the network connection without sending anything. |
| `connect/disconnect-v5` | 5.0 | same | After DISCONNECT the broker closes the network connection without sending anything. |
| `connect/duplicate-connect-v4` | 3.1.1 | same | A second CONNECT on an established connection is a protocol violation: the connection is closed. |
| `connect/duplicate-connect-v5` | 5.0 | same | A second CONNECT on an established connection is a protocol violation: the connection is closed. |
| `connect/empty-clientid-nonclean-v4` | 3.1.1 | same | MQTT 3.1.1: a zero-length ClientID with Clean Session 0 is refused with return code 2. |
| `connect/empty-clientid-v4` | 3.1.1 | profile-refused | Mosquitto accepts a zero-length ClientID with Clean Session 1 (CONNACK 0); mqttiumd refuses it with return code 2 (Identifier rejected) and closes. |
| `connect/empty-clientid-v5` | 5.0 | profile-refused | Mosquitto accepts a zero-length ClientID and returns an Assigned Client Identifier; mqttiumd refuses it with CONNACK 0x85 (Client Identifier not valid) and closes. |
| `connect/keepalive-timeout-v4` | 3.1.1 | same | Keep Alive 1 and then silence: the broker closes the connection (observed within 5 s, not timed). |
| `connect/keepalive-zero-v4` | 3.1.1 | same | Keep Alive 0 turns the keep alive timeout off: a silent client is still served after 1.5 s. |
| `connect/legacy-mqtt31` | 3.1 | profile-refused | Mosquitto accepts a MQTT 3.1 CONNECT (protocol name MQIsdp, level 3) with CONNACK 0; mqttiumd closes the connection without CONNACK. |
| `connect/ping-v4` | 3.1.1 | same | PINGREQ is answered by PINGRESP, twice in a row. |
| `connect/session-resume-v4` | 3.1.1 | same | A kept session reports Session Present 0 when created and 1 when resumed, and its subscription still routes a message published after the reconnection. |
| `connect/session-resume-v5` | 5.0 | same | A kept session reports Session Present 0 when created and 1 when resumed, and its subscription still routes a message published after the reconnection. |
| `connect/takeover-v4` | 3.1.1 | same | A second connection presents the ClientID of a connected client: the first connection is closed and the second stays usable. |
| `connect/takeover-v5` | 5.0 | same | A second connection presents the ClientID of a connected client: the first connection is closed and the second stays usable. |
| `connect/twenty-connections-v4` | 3.1.1 | same | Twenty clients connected at the same time are all accepted and the last one is served. |
| `connect/user-property-v5` | 5.0 | profile-refused | Mosquitto accepts a CONNECT that carries a User Property; mqttiumd refuses it with CONNACK 0x83 (Implementation specific error) and closes. |
| `connect/username-password-v4` | 3.1.1 | profile-refused | Mosquitto, which accepts anonymous clients here, accepts a CONNECT with User Name and Password; mqttiumd refuses any credentials with return code 4 (Bad user name or password) and closes. |
| `connect/username-password-v5` | 5.0 | profile-refused | Mosquitto, which accepts anonymous clients here, accepts a CONNECT with User Name and Password; mqttiumd refuses any credentials with CONNACK 0x83 (Implementation specific error) and closes. |
| `errors/malformed-remaining-length-v4` | 3.1.1 | same | A fixed header whose Remaining Length runs over a fifth byte: the connection is closed. |
| `errors/malformed-remaining-length-v5` | 5.0 | same | A fixed header whose Remaining Length runs over a fifth byte: the connection is closed. |
| `errors/oversized-packet-v4` | 3.1.1 | profile-refused | Mosquitto, whose default limit is the protocol maximum, acknowledges a 70 000-byte PUBLISH; mqttiumd accepts packets up to 65 536 bytes and closes the connection. |
| `errors/oversized-packet-v5` | 5.0 | profile-refused | Mosquitto, whose default limit is the protocol maximum, acknowledges a 70 000-byte PUBLISH; mqttiumd announced Maximum Packet Size 65 536 and closes the connection (without DISCONNECT: see errors/packet-over-announced-maximum-v5). |
| `errors/packet-over-announced-maximum-v5` | 5.0 | defect | With the same announced Maximum Packet Size, Mosquitto sends DISCONNECT 0x95 (Packet too large) before closing; mqttiumd closes without sending anything. |
| `properties/client-maximum-packet-size-v5` | 5.0 | same | The subscriber announces Maximum Packet Size 40: a PUBLISH that would exceed it is not sent to it, and a smaller one is. |
| `properties/content-type-v5` | 5.0 | profile-refused | Mosquitto acknowledges the PUBLISH and forwards the Content Type to the subscriber; mqttiumd answers the PUBLISH with DISCONNECT 0x83 (Implementation specific error), closes the publisher's connection and delivers nothing. |
| `properties/message-expiry-elapsed-queued-v5` | 5.0 | same | A QoS 1 message with Message Expiry Interval 1 queued for an offline session is not delivered when the session reconnects 2.5 s later; one without expiry is. |
| `properties/message-expiry-elapsed-retained-v5` | 5.0 | same | A retained message with Message Expiry Interval 1 is not sent to a subscription made 2.5 s later. |
| `properties/message-expiry-forwarded-v5` | 5.0 | same | A PUBLISH with Message Expiry Interval 600 is forwarded at once with the property set to the remaining lifetime (compared as the interval 591..600). |
| `properties/payload-format-indicator-v5` | 5.0 | profile-refused | Mosquitto acknowledges the PUBLISH and forwards the Payload Format Indicator to the subscriber; mqttiumd answers the PUBLISH with DISCONNECT 0x83 (Implementation specific error), closes the publisher's connection and delivers nothing. |
| `properties/response-topic-v5` | 5.0 | profile-refused | Mosquitto acknowledges the PUBLISH and forwards Response Topic and Correlation Data to the subscriber; mqttiumd answers the PUBLISH with DISCONNECT 0x83 (Implementation specific error), closes the publisher's connection and delivers nothing. |
| `properties/topic-alias-v5` | 5.0 | profile-refused | Mosquitto (Topic Alias Maximum 10) resolves the alias and delivers both messages; mqttiumd (Topic Alias Maximum 0) answers the first PUBLISH with DISCONNECT 0x94 (Topic Alias invalid) and closes. |
| `properties/user-properties-v5` | 5.0 | profile-refused | Mosquitto acknowledges the PUBLISH and forwards the User Properties to the subscriber in their order; mqttiumd answers the PUBLISH with DISCONNECT 0x83 (Implementation specific error), closes the publisher's connection and delivers nothing. |
| `publish/burst-qos0-v4` | 3.1.1 | profile-refused | Mosquitto delivers all one hundred QoS 0 messages written at once to an online subscriber; mqttiumd delivers fewer (about thirty) and drops the others silently. |
| `publish/cross-version-v5-to-v4` | 3.1.1 + 5.0 | same | A MQTT 5 publisher sets Message Expiry Interval and a MQTT 3.1.1 subscriber receives the message, without any property. |
| `publish/duplicate-qos1-publish-v4` | 3.1.1 | same | A QoS 1 PUBLISH is sent again with DUP 1 and the same Packet Identifier after its PUBACK: it is a new publication, acknowledged and delivered again, and DUP is not propagated to the subscriber. |
| `publish/duplicate-qos2-publish-v4` | 3.1.1 | same | A QoS 2 PUBLISH is sent again with DUP 1 before PUBREL: it is acknowledged again and the subscriber still receives one message. |
| `publish/duplicate-qos2-publish-v5` | 5.0 | same | A QoS 2 PUBLISH is sent again with DUP 1 before PUBREL: it is acknowledged again and the subscriber still receives one message. |
| `publish/no-matching-subscribers-v5` | 5.0 | unspecified | Mosquitto answers PUBACK and PUBREC with 0x10 (No matching subscribers); mqttiumd with 0x00 (Success). The specification makes 0x10 optional. |
| `publish/payload-40000-bytes-v5` | 5.0 | defect | Mosquitto acknowledges and delivers a 40 000-byte message; mqttiumd answers DISCONNECT 0x95 (Packet too large) although the packet is below the Maximum Packet Size 65 536 it announced: its payload limit is 32 768 bytes. |
| `publish/pipelined-qos1-v4` | 3.1.1 | same | A MQTT 3.1.1 client writes twenty QoS 1 PUBLISH packets at once, before reading any PUBACK: MQTT 3.1.1 has no Receive Maximum, and all twenty are acknowledged (the transcript keeps whether all PUBACK came, not how many). |
| `publish/pubrel-retransmit-v4` | 3.1.1 | same | PUBREL is sent again after the QoS 2 exchange completed: it is answered by PUBCOMP again and the message is not delivered a second time. |
| `publish/pubrel-retransmit-v5` | 5.0 | unspecified | For a PUBREL whose Packet Identifier is no longer known, Mosquitto answers PUBCOMP 0x00 (Success) and mqttiumd PUBCOMP 0x92 (Packet Identifier not found). |
| `publish/qos-downgrade-v4` | 3.1.1 | same | A subscription granted QoS 1 receives a QoS 0, a QoS 1 and a QoS 2 message: each is delivered at the lower of the two QoS, in publication order. |
| `publish/qos0-v4` | 3.1.1 | same | A QoS 0 message reaches a QoS 0 subscription once, with DUP 0 and no Packet Identifier. |
| `publish/qos0-v5` | 5.0 | same | A QoS 0 message reaches a QoS 0 subscription once, with DUP 0 and no Packet Identifier. |
| `publish/qos0-with-dup-v5` | 5.0 | defect | Both close the connection on a QoS 0 PUBLISH with DUP 1; Mosquitto first sends DISCONNECT 0x81 (Malformed Packet), mqttiumd sends nothing. |
| `publish/qos1-v4` | 3.1.1 | same | QoS 1 in and out: PUBACK to the publisher, PUBLISH to the subscriber, its PUBACK, then silence. |
| `publish/qos1-v5` | 5.0 | same | QoS 1 in and out: PUBACK to the publisher, PUBLISH to the subscriber, its PUBACK, then silence. |
| `publish/qos2-v4` | 3.1.1 | same | QoS 2 in and out: both four-packet exchanges played to their end, then silence. |
| `publish/qos2-v5` | 5.0 | same | QoS 2 in and out: both four-packet exchanges played to their end, then silence. |
| `publish/receive-maximum-inbound-v5` | 5.0 | reference-nonconformant | On a ninth QoS 2 PUBLISH while eight exchanges are open, Mosquitto answers PUBREC 0x97 (Quota exceeded) and keeps the connection open; mqttiumd sends DISCONNECT 0x93 (Receive Maximum exceeded) and closes. |
| `publish/receive-maximum-outbound-v5` | 5.0 | same | The client announces Receive Maximum 2 and acknowledges nothing: of four QoS 1 messages only two are sent to it; once it acknowledges them the next two follow. |
| `publish/topic-with-wildcard-v4` | 3.1.1 | same | A PUBLISH whose Topic Name contains the wildcard `#`: the connection is closed. |
| `publish/topic-with-wildcard-v5` | 5.0 | unspecified | Both close the connection on a Topic Name with a wildcard; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x90 (Topic Name invalid). |
| `retained/clear-v4` | 3.1.1 | same | A retained message with a zero-length payload removes the retained message of its topic: a new subscriber receives nothing. |
| `retained/forty-topics-v4` | 3.1.1 | profile-refused | Mosquitto keeps forty retained messages and sends them all to a new subscriber; mqttiumd keeps thirty-two, closes the publisher's connection on the thirty-third and sends thirty-two. |
| `retained/live-delivery-clears-retain-flag-v4` | 3.1.1 | same | A message published with RETAIN 1 reaches an already established subscription with RETAIN 0. |
| `retained/qos-downgrade-v4` | 3.1.1 | same | A message retained at QoS 2 is sent to a subscription granted QoS 1 at QoS 1, with RETAIN 1. |
| `retained/replace-v4` | 3.1.1 | same | A second retained message on a topic replaces the first: a new subscriber receives only the second. |
| `retained/retain-as-published-v5` | 5.0 | same | Retain As Published 1: a message published with RETAIN 1 keeps the flag on live delivery, and one published with RETAIN 0 does not get it. |
| `retained/retain-handling-0-v5` | 5.0 | same | Retain Handling 0: the retained message is sent at each SUBSCRIBE. |
| `retained/retain-handling-1-v5` | 5.0 | same | Retain Handling 1: the retained message is sent only when the subscription is new. |
| `retained/retain-handling-2-v5` | 5.0 | same | Retain Handling 2: the retained message is never sent at SUBSCRIBE. |
| `retained/set-and-deliver-v4` | 3.1.1 | same | A retained message is sent, with RETAIN 1, to a subscription made afterwards. |
| `retained/set-and-deliver-v5` | 5.0 | same | A retained message is sent, with RETAIN 1, to a subscription made afterwards. |
| `retained/wildcard-subscription-v4` | 3.1.1 | same | `tree/#` receives the retained messages of the parent and of every level below, and no other. |
| `session/offline-qos0-v4` | 3.1.1 | same | A QoS 0 message published while a persistent session with a QoS 1 subscription is offline: whether it is kept for the reconnection. |
| `session/offline-queue-qos1-v4` | 3.1.1 | same | Two QoS 1 messages published while a persistent session is offline are sent after CONNACK on reconnection, in order and with DUP 0; once acknowledged they are not sent again. |
| `session/offline-queue-qos1-v5` | 5.0 | same | Two QoS 1 messages published while a persistent session is offline are sent after CONNACK on reconnection, in order and with DUP 0; once acknowledged they are not sent again. |
| `session/offline-queue-qos2-v4` | 3.1.1 | same | A QoS 2 message published while a persistent session is offline is delivered on reconnection through the complete PUBLISH, PUBREC, PUBREL, PUBCOMP exchange. |
| `session/offline-queue-qos2-v5` | 5.0 | same | A QoS 2 message published while a persistent session is offline is delivered on reconnection through the complete PUBLISH, PUBREC, PUBREL, PUBCOMP exchange. |
| `session/redelivery-pubrel-v4` | 3.1.1 | same | The client answered a QoS 2 PUBLISH with PUBREC, received PUBREL and lost the connection before PUBCOMP: on reconnection the broker sends PUBREL again, not the PUBLISH. |
| `session/redelivery-pubrel-v5` | 5.0 | same | The client answered a QoS 2 PUBLISH with PUBREC, received PUBREL and lost the connection before PUBCOMP: on reconnection the broker sends PUBREL again, not the PUBLISH. |
| `session/redelivery-qos1-v4` | 3.1.1 | same | A QoS 1 PUBLISH the client received and did not acknowledge before the connection broke is sent again on reconnection with DUP 1 and the same Packet Identifier. |
| `session/redelivery-qos1-v5` | 5.0 | same | A QoS 1 PUBLISH the client received and did not acknowledge before the connection broke is sent again on reconnection with DUP 1 and the same Packet Identifier. |
| `subscribe/dollar-topics-vs-wildcards-v4` | 3.1.1 | same | Subscriptions to `#` and to `+/broker/version` receive a plain topic and nothing that begins with `$`: neither the reference's own `$SYS` tree nor a `$SYS/...` message a client publishes. |
| `subscribe/hash-wildcard-v4` | 3.1.1 | same | `sport/#` matches the parent level and every level below it. |
| `subscribe/invalid-empty-filter-v5` | 5.0 | unspecified | Both close the connection on the invalid Topic Filter; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x82 (Protocol Error). |
| `subscribe/invalid-hash-not-last-v4` | 3.1.1 | same | SUBSCRIBE with the invalid Topic Filter `bad/#/x`: the connection is closed. |
| `subscribe/invalid-hash-not-last-v5` | 5.0 | unspecified | Both close the connection on the invalid Topic Filter; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x82 (Protocol Error). |
| `subscribe/many-subscriptions-v4` | 3.1.1 | profile-refused | Mosquitto grants all 150 subscriptions; mqttiumd grants 128 and answers the other 22 with SUBACK return code 0x80 (Failure). |
| `subscribe/no-local-v5` | 5.0 | same | No Local: a client does not receive its own publication and receives another client's. |
| `subscribe/overlapping-subscriptions-v4` | 3.1.1 | same | Two subscriptions of one client, granted QoS 0 and QoS 1, match one QoS 2 message: how many copies are sent, and at which QoS. |
| `subscribe/overlapping-subscriptions-v5` | 5.0 | unspecified | For one message matching two subscriptions of a MQTT 5 client, Mosquitto sends one copy per subscription (QoS 0 and QoS 1); mqttiumd sends one copy at the highest granted QoS. |
| `subscribe/plus-wildcard-v4` | 3.1.1 | same | `sport/+` matches exactly one level, the empty one included, and not the parent. |
| `subscribe/qos-grants-v4` | 3.1.1 | same | A SUBSCRIBE with three filters at QoS 0, 1 and 2: one granted QoS per filter, in order. |
| `subscribe/qos-grants-v5` | 5.0 | same | A SUBSCRIBE with three filters at QoS 0, 1 and 2: one granted QoS per filter, in order. |
| `subscribe/shared-subscription-v5` | 5.0 | profile-refused | Mosquitto grants `$share/group/shared/t` and delivers to it; mqttiumd answers SUBACK 0x9E (Shared Subscriptions not supported), as its CONNACK announces, and delivers to the plain subscription only. |
| `subscribe/subscription-identifier-v5` | 5.0 | profile-refused | Mosquitto accepts the Subscription Identifier and returns it in the PUBLISH; mqttiumd, whose CONNACK announces them not available, answers the SUBSCRIBE with DISCONNECT 0x83 (the code that names the cause is 0xA1) and closes. |
| `subscribe/sys-explicit-filter-v4` | 3.1.1 | profile-refused | Mosquitto sends its retained `$SYS/broker/version` message; mqttiumd grants the subscription and publishes nothing under `$SYS`. |
| `subscribe/twelve-filters-v4` | 3.1.1 | profile-refused | Mosquitto answers a SUBSCRIBE of twelve Topic Filters with twelve granted QoS; mqttiumd accepts at most eight Topic Filters per SUBSCRIBE and closes the connection without SUBACK. |
| `subscribe/unsubscribe-v4` | 3.1.1 | same | UNSUBSCRIBE removes exactly the filter named, compared character by character: `unsub/#` goes, `unsub/a` stays; an unknown filter is acknowledged too. |
| `subscribe/unsubscribe-v5` | 5.0 | same | An UNSUBSCRIBE with an existing and an unknown filter: one Reason Code each, in order, then no more delivery. |
| `subscribe/user-property-v5` | 5.0 | profile-refused | Mosquitto answers a SUBSCRIBE that carries a User Property with SUBACK; mqttiumd answers DISCONNECT 0x83 (Implementation specific error) and closes. |
| `will/abrupt-close-qos0-v4` | 3.1.1 | profile-refused | Mosquitto accepts a QoS 0 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with return code 3 (Server unavailable). |
| `will/abrupt-close-qos1-v4` | 3.1.1 | same | A client with a QoS 1 Will closes its connection without DISCONNECT: the Will is published to a QoS 2 subscription at QoS 1. |
| `will/abrupt-close-qos1-v5` | 5.0 | same | A client with a QoS 1 Will closes its connection without DISCONNECT: the Will is published to a QoS 2 subscription at QoS 1. |
| `will/abrupt-close-qos2-v4` | 3.1.1 | profile-refused | Mosquitto accepts a QoS 2 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with return code 3 (Server unavailable). |
| `will/abrupt-close-qos2-v5` | 5.0 | profile-refused | Mosquitto accepts a QoS 2 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with CONNACK 0x83 (the code MQTT-3.2.2-12 recommends is 0x9B). |
| `will/delay-interval-v5` | 5.0 | profile-refused | Mosquitto accepts Will Delay Interval 1 and publishes the Will after the delay; mqttiumd refuses the CONNECT with CONNACK 0x83 (Implementation specific error). |
| `will/disconnect-discards-will-v4` | 3.1.1 | same | A normal DISCONNECT discards the Will: the subscriber receives only a later marker message. |
| `will/disconnect-discards-will-v5` | 5.0 | same | A normal DISCONNECT discards the Will: the subscriber receives only a later marker message. |
| `will/disconnect-with-will-v5` | 5.0 | same | DISCONNECT with Reason Code 0x04 (Disconnect with Will Message): the Will is published. |
| `will/retain-v4` | 3.1.1 | same | A QoS 1 Will with Will Retain 1 is published on abrupt close and kept as the retained message of its topic: a later subscriber receives it with RETAIN 1. |
| `will/retain-v5` | 5.0 | same | A QoS 1 Will with Will Retain 1 is published on abrupt close and kept as the retained message of its topic: a later subscriber receives it with RETAIN 1. |
| `will/takeover-v5` | 5.0 | same | A client with a Will is taken over by a new connection with the same ClientID and Clean Start 1: the Will of the first connection is published. |

## profile-refused (26)

- `connect/connack-capabilities-v5` (MQTT 5.0): Mosquitto announces Receive Maximum 20 and Topic Alias Maximum 10 and leaves every other capability at its default; mqttiumd announces Receive Maximum 8, Maximum Packet Size 65 536, Topic Alias Maximum 0, and shared subscriptions and Subscription Identifiers not available.
  - Lot: The capabilities of the v0.1 profile: Topic Alias from client to server (Topic Alias Maximum above 0); Shared subscriptions (CMP-002); Subscription Identifiers; Receive Maximum and Maximum Packet Size of the bounded preview
- `connect/empty-clientid-v4` (MQTT 3.1.1): Mosquitto accepts a zero-length ClientID with Clean Session 1 (CONNACK 0); mqttiumd refuses it with return code 2 (Identifier rejected) and closes.
  - Statement: MQTT-3.1.3-6 makes accepting it optional; MQTT-3.1.3-9 is how mqttiumd refuses
  - Lot: Server-assigned Client Identifier for a zero-length ClientID
- `connect/empty-clientid-v5` (MQTT 5.0): Mosquitto accepts a zero-length ClientID and returns an Assigned Client Identifier; mqttiumd refuses it with CONNACK 0x85 (Client Identifier not valid) and closes.
  - Statement: MQTT-3.1.3-6 makes accepting it optional
  - Lot: Server-assigned Client Identifier for a zero-length ClientID
- `connect/legacy-mqtt31` (MQTT 3.1): Mosquitto accepts a MQTT 3.1 CONNECT (protocol name MQIsdp, level 3) with CONNACK 0; mqttiumd closes the connection without CONNACK.
  - Lot: MQTT 3.1 (MQIsdp): a separate extension, outside the v0.1 profile (chapter 1, section 3)
- `connect/user-property-v5` (MQTT 5.0): Mosquitto accepts a CONNECT that carries a User Property; mqttiumd refuses it with CONNACK 0x83 (Implementation specific error) and closes.
  - Lot: MQTT 5 properties a broker may ignore but must accept: User Property on CONNECT, SUBSCRIBE and UNSUBSCRIBE
- `connect/username-password-v4` (MQTT 3.1.1): Mosquitto, which accepts anonymous clients here, accepts a CONNECT with User Name and Password; mqttiumd refuses any credentials with return code 4 (Bad user name or password) and closes.
  - Lot: Authentication: user name and password checked against a password file (chapter 1, section 3, row Authentification; CFG-004, CFG-005)
- `connect/username-password-v5` (MQTT 5.0): Mosquitto, which accepts anonymous clients here, accepts a CONNECT with User Name and Password; mqttiumd refuses any credentials with CONNACK 0x83 (Implementation specific error) and closes.
  - Lot: Authentication: user name and password checked against a password file (chapter 1, section 3, row Authentification; CFG-004, CFG-005)
- `errors/oversized-packet-v4` (MQTT 3.1.1): Mosquitto, whose default limit is the protocol maximum, acknowledges a 70 000-byte PUBLISH; mqttiumd accepts packets up to 65 536 bytes and closes the connection.
  - Lot: Runtime limits of the bounded preview (ADR 0044), to be raised to Mosquitto's defaults or made configurable: 65 536-byte packets
- `errors/oversized-packet-v5` (MQTT 5.0): Mosquitto, whose default limit is the protocol maximum, acknowledges a 70 000-byte PUBLISH; mqttiumd announced Maximum Packet Size 65 536 and closes the connection (without DISCONNECT: see errors/packet-over-announced-maximum-v5).
  - Lot: Runtime limits of the bounded preview (ADR 0044), to be raised to Mosquitto's defaults or made configurable: 65 536-byte packets
- `properties/content-type-v5` (MQTT 5.0): Mosquitto acknowledges the PUBLISH and forwards the Content Type to the subscriber; mqttiumd answers the PUBLISH with DISCONNECT 0x83 (Implementation specific error), closes the publisher's connection and delivers nothing.
  - Statement: MQTT-3.3.2-20: what the feature requires once it is supported
  - Lot: MQTT 5 PUBLISH properties forwarded to subscribers: User Property, Payload Format Indicator, Content Type, Response Topic, Correlation Data
- `properties/payload-format-indicator-v5` (MQTT 5.0): Mosquitto acknowledges the PUBLISH and forwards the Payload Format Indicator to the subscriber; mqttiumd answers the PUBLISH with DISCONNECT 0x83 (Implementation specific error), closes the publisher's connection and delivers nothing.
  - Statement: MQTT-3.3.2-4: what the feature requires once it is supported
  - Lot: MQTT 5 PUBLISH properties forwarded to subscribers: User Property, Payload Format Indicator, Content Type, Response Topic, Correlation Data
- `properties/response-topic-v5` (MQTT 5.0): Mosquitto acknowledges the PUBLISH and forwards Response Topic and Correlation Data to the subscriber; mqttiumd answers the PUBLISH with DISCONNECT 0x83 (Implementation specific error), closes the publisher's connection and delivers nothing.
  - Statement: MQTT-3.3.2-15 and MQTT-3.3.2-16: what the feature requires once it is supported
  - Lot: MQTT 5 PUBLISH properties forwarded to subscribers: User Property, Payload Format Indicator, Content Type, Response Topic, Correlation Data
- `properties/topic-alias-v5` (MQTT 5.0): Mosquitto (Topic Alias Maximum 10) resolves the alias and delivers both messages; mqttiumd (Topic Alias Maximum 0) answers the first PUBLISH with DISCONNECT 0x94 (Topic Alias invalid) and closes.
  - Statement: MQTT-3.2.2-18: with Topic Alias Maximum 0 the client is the one at fault
  - Lot: Topic Alias from client to server (Topic Alias Maximum above 0)
- `properties/user-properties-v5` (MQTT 5.0): Mosquitto acknowledges the PUBLISH and forwards the User Properties to the subscriber in their order; mqttiumd answers the PUBLISH with DISCONNECT 0x83 (Implementation specific error), closes the publisher's connection and delivers nothing.
  - Statement: MQTT-3.3.2-17 and MQTT-3.3.2-18: what the feature requires once it is supported
  - Lot: MQTT 5 PUBLISH properties forwarded to subscribers: User Property, Payload Format Indicator, Content Type, Response Topic, Correlation Data
- `publish/burst-qos0-v4` (MQTT 3.1.1): Mosquitto delivers all one hundred QoS 0 messages written at once to an online subscriber; mqttiumd delivers fewer (about thirty) and drops the others silently.
  - Lot: Runtime limits of the bounded preview (ADR 0044), to be raised to Mosquitto's defaults or made configurable: the bounded volatile path of QoS 0 messages (8 outgoing messages per connection, 32 writer slots)
- `retained/forty-topics-v4` (MQTT 3.1.1): Mosquitto keeps forty retained messages and sends them all to a new subscriber; mqttiumd keeps thirty-two, closes the publisher's connection on the thirty-third and sends thirty-two.
  - Lot: Runtime limits of the bounded preview (ADR 0044), to be raised to Mosquitto's defaults or made configurable: 32 retained topics
- `subscribe/many-subscriptions-v4` (MQTT 3.1.1): Mosquitto grants all 150 subscriptions; mqttiumd grants 128 and answers the other 22 with SUBACK return code 0x80 (Failure).
  - Lot: Runtime limits of the bounded preview (ADR 0044), to be raised to Mosquitto's defaults or made configurable: 128 subscriptions
- `subscribe/shared-subscription-v5` (MQTT 5.0): Mosquitto grants `$share/group/shared/t` and delivers to it; mqttiumd answers SUBACK 0x9E (Shared Subscriptions not supported), as its CONNACK announces, and delivers to the plain subscription only.
  - Lot: Shared subscriptions (CMP-002)
- `subscribe/subscription-identifier-v5` (MQTT 5.0): Mosquitto accepts the Subscription Identifier and returns it in the PUBLISH; mqttiumd, whose CONNACK announces them not available, answers the SUBSCRIBE with DISCONNECT 0x83 (the code that names the cause is 0xA1) and closes.
  - Lot: Subscription Identifiers
- `subscribe/sys-explicit-filter-v4` (MQTT 3.1.1): Mosquitto sends its retained `$SYS/broker/version` message; mqttiumd grants the subscription and publishes nothing under `$SYS`.
  - Lot: Operations: metrics published under `$SYS` (chapter 1, section 3, row Exploitation)
- `subscribe/twelve-filters-v4` (MQTT 3.1.1): Mosquitto answers a SUBSCRIBE of twelve Topic Filters with twelve granted QoS; mqttiumd accepts at most eight Topic Filters per SUBSCRIBE and closes the connection without SUBACK.
  - Statement: MQTT-3.8.4-5: what the feature requires once the limit is lifted
  - Lot: Runtime limits of the bounded preview (ADR 0044), to be raised to Mosquitto's defaults or made configurable: 8 Topic Filters per SUBSCRIBE or UNSUBSCRIBE
- `subscribe/user-property-v5` (MQTT 5.0): Mosquitto answers a SUBSCRIBE that carries a User Property with SUBACK; mqttiumd answers DISCONNECT 0x83 (Implementation specific error) and closes.
  - Lot: MQTT 5 properties a broker may ignore but must accept: User Property on CONNECT, SUBSCRIBE and UNSUBSCRIBE
- `will/abrupt-close-qos0-v4` (MQTT 3.1.1): Mosquitto accepts a QoS 0 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with return code 3 (Server unavailable).
  - Lot: Will messages beyond QoS 1 without delay: Will QoS 0 and 2, Will Delay Interval
- `will/abrupt-close-qos2-v4` (MQTT 3.1.1): Mosquitto accepts a QoS 2 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with return code 3 (Server unavailable).
  - Lot: Will messages beyond QoS 1 without delay: Will QoS 0 and 2, Will Delay Interval
- `will/abrupt-close-qos2-v5` (MQTT 5.0): Mosquitto accepts a QoS 2 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with CONNACK 0x83 (the code MQTT-3.2.2-12 recommends is 0x9B).
  - Lot: Will messages beyond QoS 1 without delay: Will QoS 0 and 2, Will Delay Interval
- `will/delay-interval-v5` (MQTT 5.0): Mosquitto accepts Will Delay Interval 1 and publishes the Will after the delay; mqttiumd refuses the CONNECT with CONNACK 0x83 (Implementation specific error).
  - Lot: Will messages beyond QoS 1 without delay: Will QoS 0 and 2, Will Delay Interval

## reference-nonconformant (1)

- `publish/receive-maximum-inbound-v5` (MQTT 5.0): On a ninth QoS 2 PUBLISH while eight exchanges are open, Mosquitto answers PUBREC 0x97 (Quota exceeded) and keeps the connection open; mqttiumd sends DISCONNECT 0x93 (Receive Maximum exceeded) and closes.
  - Statement: MQTT-4.13.1-1, with section 3.3.4: a client that exceeds Receive Maximum (MQTT-3.3.4-7) makes a Protocol Error for which the Reason Code 0x93 is given, so the server must close the connection
  - mqttiumd: Sends DISCONNECT 0x93 (Receive Maximum exceeded) and closes the network connection.

## defect (4)

- `connect/authentication-method-v5` (MQTT 5.0): Both refuse the CONNECT and close; Mosquitto with CONNACK 0x8C (Bad authentication method), mqttiumd with CONNACK 0x83 (Implementation specific error).
  - Statement: MQTT-4.12.0-1 allows 0x8C or 0x87 here
  - Lot: MQTT 5 error reporting: send CONNACK or DISCONNECT with the Reason Code that names the cause before closing
- `errors/packet-over-announced-maximum-v5` (MQTT 5.0): With the same announced Maximum Packet Size, Mosquitto sends DISCONNECT 0x95 (Packet too large) before closing; mqttiumd closes without sending anything.
  - Lot: MQTT 5 error reporting: send CONNACK or DISCONNECT with the Reason Code that names the cause before closing
- `publish/payload-40000-bytes-v5` (MQTT 5.0): Mosquitto acknowledges and delivers a 40 000-byte message; mqttiumd answers DISCONNECT 0x95 (Packet too large) although the packet is below the Maximum Packet Size 65 536 it announced: its payload limit is 32 768 bytes.
  - Lot: Runtime limits of the bounded preview (ADR 0044), to be raised to Mosquitto's defaults or made configurable: 32 768-byte payloads; meanwhile announce the limit that is enforced
- `publish/qos0-with-dup-v5` (MQTT 5.0): Both close the connection on a QoS 0 PUBLISH with DUP 1; Mosquitto first sends DISCONNECT 0x81 (Malformed Packet), mqttiumd sends nothing.
  - Statement: MQTT-3.3.1-2 is what the client violates
  - Lot: MQTT 5 error reporting: send CONNACK or DISCONNECT with the Reason Code that names the cause before closing

## unspecified (6)

- `publish/no-matching-subscribers-v5` (MQTT 5.0): Mosquitto answers PUBACK and PUBREC with 0x10 (No matching subscribers); mqttiumd with 0x00 (Success). The specification makes 0x10 optional.
  - Statement: MQTT-3.4.2-1: both are PUBACK Reason Codes
- `publish/pubrel-retransmit-v5` (MQTT 5.0): For a PUBREL whose Packet Identifier is no longer known, Mosquitto answers PUBCOMP 0x00 (Success) and mqttiumd PUBCOMP 0x92 (Packet Identifier not found).
  - Statement: MQTT-4.3.3-11: both answer with PUBCOMP
- `publish/topic-with-wildcard-v5` (MQTT 5.0): Both close the connection on a Topic Name with a wildcard; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x90 (Topic Name invalid).
  - Statement: MQTT-3.3.2-2 is what the client violates; the Reason Code is not prescribed
- `subscribe/invalid-empty-filter-v5` (MQTT 5.0): Both close the connection on the invalid Topic Filter; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x82 (Protocol Error).
  - Statement: MQTT-4.13.1-1: both close; the Reason Code is not prescribed
- `subscribe/invalid-hash-not-last-v5` (MQTT 5.0): Both close the connection on the invalid Topic Filter; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x82 (Protocol Error).
  - Statement: MQTT-4.13.1-1: both close; the Reason Code is not prescribed
- `subscribe/overlapping-subscriptions-v5` (MQTT 5.0): For one message matching two subscriptions of a MQTT 5 client, Mosquitto sends one copy per subscription (QoS 0 and QoS 1); mqttiumd sends one copy at the highest granted QoS.
  - Statement: MQTT-3.3.4-2 requires the copy at the highest QoS; section 3.3.4 allows further copies
