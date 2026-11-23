# Compatibility matrix: mqttiumd against Mosquitto 2.1.4

Written by `tests/compat/run.py`: regenerate it with the command in
`tests/compat/README.md` and copy `matrix.md` here; do not edit it by hand.

- broker: `mqttiumd` built from commit `476afc845877cc1eb2041c990618fc17490b115a`
- reference: Mosquitto 2.1.4, the version `tests/compat/build-reference.sh` builds
- scenarios played: 135

`same` means equal normalized transcripts on the scenario played, nothing
more. A difference is not a failure of mqttiumd by itself: Mosquitto is not
the oracle, the MQTT specification is. Each difference is classified in
`divergences.json`:

- `profile-refused`: mqttiumd deliberately does not support the feature yet;
- `reference-nonconformant`: Mosquitto departs from the specification; mqttiumd follows it;
- `defect`: mqttiumd is wrong or needlessly different: a to-do for the product;
- `unspecified`: both behaviours are allowed by the specification.

## Summary

135 scenarios: 105 `same`, 15 `profile-refused`, 6 `reference-nonconformant`, 9 `unspecified`.

| Area | same | profile-refused | reference-nonconformant | defect | unspecified | total |
| - | -: | -: | -: | -: | -: | -: |
| connect | 19 | 6 | 0 | 0 | 1 | 26 |
| session | 9 | 0 | 3 | 0 | 0 | 12 |
| subscribe | 18 | 4 | 0 | 0 | 4 | 26 |
| publish | 22 | 0 | 1 | 0 | 3 | 26 |
| retained | 14 | 0 | 0 | 0 | 0 | 14 |
| properties | 8 | 1 | 0 | 0 | 0 | 9 |
| will | 10 | 4 | 2 | 0 | 0 | 16 |
| errors | 5 | 0 | 0 | 0 | 1 | 6 |
| **all** | 105 | 15 | 6 | 0 | 9 | 135 |

## Expectations on mqttiumd's transcript

What a normative statement requires of the broker in a scenario, whatever Mosquitto
does there. A `fail` is listed in `expectations-open.json` or fails the run; the
column of the reference is information.

21 expectations: 21 `pass`, 0 `fail` (0 listed as open), 0 `not-applicable`.

| Expectation | MQTT | Statement | Requires | mqttiumd | Observed on mqttiumd | Mosquitto |
| - | - | - | - | - | - | - |
| `errors/packet-at-announced-maximum-v5#limit` | 5.0 | MQTT-4.13.1-1, with section 3.2.2.3.6: a packet larger than the announced Maximum Packet Size is a Protocol Error with the Reason Code 0x95 | Where CONNACK announces Maximum Packet Size 1048584: a PUBLISH of exactly that size is acknowledged, and one a byte larger is answered DISCONNECT 0x95 and the connection is closed | pass | connack: recv [CONNACK reason=0x00 maximum_packet_size=1048584] ok; exact: recv [PUBACK packet_id=1 reason=0x10] ok; over: until_close [DISCONNECT reason=0x95] eof | pass |
| `publish/over-maximum-packet-size-qos1-v5#discarded` | 5.0 | MQTT-3.1.2-25, with MQTT-3.1.2-24 for the size of what is sent | Nothing is sent for the message that would take 114 bytes and the connection stays open; the next message is delivered in a packet of at most 64 bytes, and nothing follows it | pass | large: quiet [] quiet; small: recv [PUBLISH packet_id=1 dup=false payload="small" topic="limit/t" wire_bytes=19] ok; after-small: quiet [] quiet | pass |
| `publish/over-maximum-packet-size-qos1-v5#not-redelivered` | 5.0 | MQTT-3.1.2-25, with MQTT-3.2.2-3 for Session Present | The resumed session (Session Present 1), which now accepts 1024 bytes, is sent nothing for the discarded message: the first PUBLISH it reads is a new message, with DUP 0 | pass | resumed: recv [CONNACK reason=0x00 session_present=true] ok; no-redelivery: quiet [] quiet; next: recv [PUBLISH packet_id=1 dup=false payload="next" topic="limit/t"] ok | pass |
| `publish/over-maximum-packet-size-qos2-v5#discarded` | 5.0 | MQTT-3.1.2-25, with MQTT-3.1.2-24 for the size of what is sent | Nothing is sent for the message that would take 114 bytes and the connection stays open; the next message is delivered in a packet of at most 64 bytes, and nothing follows it | pass | large: quiet [] quiet; small: recv [PUBLISH packet_id=1 dup=false payload="small" topic="limit/t" wire_bytes=19] ok; after-small: quiet [] quiet | pass |
| `publish/over-maximum-packet-size-qos2-v5#not-redelivered` | 5.0 | MQTT-3.1.2-25, with MQTT-3.2.2-3 for Session Present | The resumed session (Session Present 1), which now accepts 1024 bytes, is sent nothing for the discarded message: the first PUBLISH it reads is a new message, with DUP 0 | pass | resumed: recv [CONNACK reason=0x00 session_present=true] ok; no-redelivery: quiet [] quiet; next: recv [PUBLISH packet_id=1 dup=false payload="next" topic="limit/t"] ok | pass |
| `retained/forty-topics-qos1-v4#store` | 3.1.1 | MQTT-3.3.1-6, with MQTT-3.3.1-5 for the store and MQTT-3.3.1-8 for the RETAIN flag | The SUBACK and the 40 retained messages, one for each topic, with its payload and RETAIN 1, each once and in any order; then nothing | pass | retained: drain [41 packets: 1 SUBACK, 40 PUBLISH] ok; after: quiet [] quiet | pass |
| `retained/thirty-two-topics-qos1-v4#store` | 3.1.1 | MQTT-3.3.1-6, with MQTT-3.3.1-5 for the store and MQTT-3.3.1-8 for the RETAIN flag | The SUBACK and the 32 retained messages, one for each topic, with its payload and RETAIN 1, each once and in any order; then nothing | pass | retained: drain [33 packets: 1 SUBACK, 32 PUBLISH] ok; after: quiet [] quiet | pass |
| `subscribe/many-filters-effective-v4#granted` | 3.1.1 | MQTT-3.8.4-4: the responses of the sequence are combined into a single SUBACK | One SUBACK with twelve codes, each the grant of QoS 0 that a SUBSCRIBE of that filter alone receives | pass | answer: recv [SUBACK packet_id=1 codes=0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00] ok | pass |
| `subscribe/many-filters-effective-v4#effective` | 3.1.1 | MQTT-3.8.4-4: every filter is handled as a SUBSCRIBE of its own | The twelfth filter is a subscription: the message published on it is delivered | pass | delivered: recv [PUBLISH payload="twelfth" topic="eff/11"] ok | pass |
| `subscribe/many-filters-effective-v5#granted` | 5.0 | MQTT-3.8.4-5: the responses of the sequence are combined into a single SUBACK | One SUBACK with twelve codes, each the grant of QoS 0 that a SUBSCRIBE of that filter alone receives | pass | answer: recv [SUBACK packet_id=1 codes=0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00] ok | pass |
| `subscribe/many-filters-effective-v5#effective` | 5.0 | MQTT-3.8.4-5: every filter is handled as a SUBSCRIBE of its own | The twelfth filter is a subscription: the message published on it is delivered | pass | delivered: recv [PUBLISH payload="twelfth" topic="eff/11"] ok | pass |
| `subscribe/reserved-option-bits-v5#closure` | 5.0 | MQTT-3.8.3-5, with MQTT-4.13.1-1 for the closure | The connection is closed, and a DISCONNECT sent before closing carries 0x81 (Malformed Packet) | pass | closure: until_close [DISCONNECT reason=0x81] eof | pass |
| `subscribe/share-literal-filter-v4#suback` | 3.1.1 | MQTT-3.8.4-1, with MQTT-3.8.4-2 for the Packet Identifier and MQTT-3.8.4-5 for the return code | A SUBACK with the Packet Identifier of the SUBSCRIBE and one return code, a granted QoS or 0x80 (Failure) | pass | answer: recv [SUBACK packet_id=2561 codes=0x80] ok | pass |
| `subscribe/shared-subscription-v5#refusal` | 5.0 | MQTT-4.13.1-1, with section 3.2.2.3.13: the Protocol Error has the Reason Code 0x9E | Where CONNACK announces Shared Subscription Available 0: the connection is closed, and a DISCONNECT sent before closing carries 0x9E | pass | connack: recv [CONNACK reason=0x00 shared_subscription_available=0] ok; answer: recv [DISCONNECT reason=0x9e] ok; after: quiet [] eof | not-applicable |
| `subscribe/subscription-identifier-v5#refusal` | 5.0 | MQTT-4.13.1-1, with section 3.2.2.3.12: the Protocol Error has the Reason Code 0xA1 | Where CONNACK announces Subscription Identifier Available 0: the connection is closed, and a DISCONNECT sent before closing carries 0xA1 | pass | connack: recv [CONNACK reason=0x00 subscription_identifier_available=0] ok; answer: recv [DISCONNECT reason=0xa1] ok; after: quiet [] eof | not-applicable |
| `subscribe/twelve-filters-v4#suback` | 3.1.1 | MQTT-3.8.4-5 | A SUBACK with twelve return codes, each a granted QoS or 0x80 (Failure) | pass | answer: recv [SUBACK packet_id=1 codes=0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00] ok | pass |
| `subscribe/unsubscribe-nine-filters-v4#acknowledged` | 3.1.1 | MQTT-3.10.4-6: one UNSUBACK for the sequence | One UNSUBACK for the nine filters | pass | answer: recv [UNSUBACK packet_id=3] ok | pass |
| `subscribe/unsubscribe-nine-filters-v4#effective` | 3.1.1 | MQTT-3.10.4-6: every filter is handled as an UNSUBSCRIBE of its own | The ninth subscription is removed: the next message the subscriber reads is the one published on the filter it kept | pass | next: recv [PUBLISH payload="kept" topic="nine/9"] ok | pass |
| `subscribe/unsubscribe-nine-filters-v5#acknowledged` | 5.0 | MQTT-3.10.4-6: one UNSUBACK for the sequence | One UNSUBACK for the nine filters, each with Reason Code 0x00 (Success) | pass | answer: recv [UNSUBACK packet_id=3 codes=0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00] ok | pass |
| `subscribe/unsubscribe-nine-filters-v5#effective` | 5.0 | MQTT-3.10.4-6: every filter is handled as an UNSUBSCRIBE of its own | The ninth subscription is removed: the next message the subscriber reads is the one published on the filter it kept | pass | next: recv [PUBLISH payload="kept" topic="nine/9"] ok | pass |
| `subscribe/unsubscribe-share-filter-v4#unsuback` | 3.1.1 | MQTT-3.10.4-5, with MQTT-3.10.4-4 for the Packet Identifier | An UNSUBACK with the Packet Identifier of the UNSUBSCRIBE, although no subscription is deleted | pass | answer: recv [UNSUBACK packet_id=2562] ok | pass |

## Counts measured, not compared

How many packets of the counted type each broker sent in the steps that count
(`tally`, `measure`), out of how many the step names. These are the numbers of
this run and may differ in the next: where a broker may lawfully shed messages
(QoS 0), how many it sheds depends on timing. No result above rests on them.

| Scenario | Connection | Counted | Named | Mosquitto | mqttiumd |
| - | - | - | -: | -: | -: |
| `publish/burst-qos0-v4` | sub | PUBLISH | 100 | 100 | 100 |
| `publish/pipelined-qos1-v4` | pub | PUBACK | 20 | 20 | 20 |
| `retained/forty-topics-v4` | sub | PUBLISH | 40 | 40 | 40 |

## Scenarios

| Scenario | MQTT | Result | Note |
| - | - | - | - |
| `connect/authentication-method-v5` | 5.0 | same | CONNECT with an Authentication Method neither broker supports. |
| `connect/bad-protocol-level-v4` | 3.1.1 | unspecified | For a CONNECT with protocol level 6, Mosquitto answers a CONNACK in the MQTT 5 layout with Reason Code 0x84 (Unsupported Protocol Version) and closes; mqttiumd answers the MQTT 3.1.1 CONNACK with return code 1 (unacceptable protocol version) and closes. |
| `connect/bad-protocol-name-v4` | 3.1.1 | same | CONNECT with the protocol name MQTX and level 4. |
| `connect/clean-start-discards-session-v4` | 3.1.1 | same | Clean Session 1 discards a kept session: Session Present 0, the old subscription no longer routes, and nothing is left for a later resume. |
| `connect/clean-start-v4` | 3.1.1 | same | A first CONNECT with Clean Session/Clean Start 1 is accepted with Session Present 0. |
| `connect/clean-start-v5` | 5.0 | same | A first CONNECT with Clean Session/Clean Start 1 is accepted with Session Present 0. |
| `connect/connack-capabilities-v5` | 5.0 | profile-refused | Mosquitto announces Receive Maximum 20, Topic Alias Maximum 10 and Maximum Packet Size 2 000 000 and leaves every other capability at its default; mqttiumd announces Receive Maximum 8, Maximum Packet Size 1 048 584, Topic Alias Maximum 0, and shared subscriptions and Subscription Identifiers not available. |
| `connect/disconnect-v4` | 3.1.1 | same | After DISCONNECT the broker closes the network connection without sending anything. |
| `connect/disconnect-v5` | 5.0 | same | After DISCONNECT the broker closes the network connection without sending anything. |
| `connect/duplicate-connect-v4` | 3.1.1 | same | A second CONNECT on an established connection is a protocol violation: the connection is closed. |
| `connect/duplicate-connect-v5` | 5.0 | same | A second CONNECT on an established connection is a protocol violation: the connection is closed. |
| `connect/empty-clientid-nonclean-v4` | 3.1.1 | same | MQTT 3.1.1: a zero-length ClientID with Clean Session 0 is refused with return code 2. |
| `connect/empty-clientid-v4` | 3.1.1 | profile-refused | Mosquitto accepts a zero-length ClientID with Clean Session 1 (CONNACK 0); mqttiumd refuses it with return code 2 (Identifier rejected) and closes. |
| `connect/empty-clientid-v5` | 5.0 | profile-refused | Mosquitto accepts a zero-length ClientID and returns an Assigned Client Identifier; mqttiumd refuses it with CONNACK 0x85 (Client Identifier not valid) and closes. |
| `connect/keepalive-timeout-v4` | 3.1.1 | same | Keep Alive 1 and then silence: the broker closes the connection (observed within 10 s, not timed: Mosquitto 2.1.4 checks Keep Alive on a coarse period and closes after about six seconds). |
| `connect/keepalive-zero-v4` | 3.1.1 | same | Keep Alive 0 turns the keep alive timeout off: a silent client is still served after 1.5 s. |
| `connect/legacy-mqtt31` | 3.1 | profile-refused | Mosquitto accepts a MQTT 3.1 CONNECT (protocol name MQIsdp, level 3) with CONNACK 0; mqttiumd closes the connection without CONNACK. |
| `connect/ping-v4` | 3.1.1 | same | PINGREQ is answered by PINGRESP, twice in a row. |
| `connect/session-resume-v4` | 3.1.1 | same | A kept session reports Session Present 0 when created and 1 when resumed, and its subscription still routes a message published after the reconnection. |
| `connect/session-resume-v5` | 5.0 | same | A kept session reports Session Present 0 when created and 1 when resumed, and its subscription still routes a message published after the reconnection. |
| `connect/takeover-v4` | 3.1.1 | same | A second connection presents the ClientID of a connected client: the first connection is closed and the second stays usable. |
| `connect/takeover-v5` | 5.0 | same | A second connection presents the ClientID of a connected client: the first connection is closed and the second stays usable. |
| `connect/twenty-connections-v4` | 3.1.1 | same | Twenty clients connected at the same time are all accepted and the last one is served. |
| `connect/user-property-v5` | 5.0 | same | A CONNECT carrying a User Property is accepted like one without. |
| `connect/username-password-v4` | 3.1.1 | profile-refused | Mosquitto, which accepts anonymous clients here, accepts a CONNECT with User Name and Password; mqttiumd refuses any credentials with return code 4 (Bad user name or password) and closes. |
| `connect/username-password-v5` | 5.0 | profile-refused | Mosquitto, which accepts anonymous clients here, accepts a CONNECT with User Name and Password; mqttiumd refuses any credentials with CONNACK 0x83 (Implementation specific error) and closes. |
| `errors/malformed-remaining-length-v4` | 3.1.1 | same | A fixed header whose Remaining Length runs over a fifth byte: the connection is closed. |
| `errors/malformed-remaining-length-v5` | 5.0 | unspecified | Both close the connection on a Remaining Length of five bytes; Mosquitto without a packet, mqttiumd after DISCONNECT 0x81 (Malformed Packet). |
| `errors/oversized-packet-v4` | 3.1.1 | same | A QoS 1 PUBLISH of 70 000 bytes with each broker's default limits: both brokers accept it within their advertised packet limits. |
| `errors/oversized-packet-v5` | 5.0 | same | A QoS 1 PUBLISH of 70 000 bytes with each broker's default limits: both brokers accept it within their advertised packet limits. |
| `errors/packet-at-announced-maximum-v5` | 5.0 | same | Both brokers announce Maximum Packet Size 1048584 (the reference by `max_packet_size 1048584`). A PUBLISH that takes exactly that many bytes on the wire, fixed header included, is acknowledged; the next one, a byte larger, is answered DISCONNECT 0x95 and the connection is closed. How the connection ends after the DISCONNECT depends on what was still in flight: the closure is compared as `any`. |
| `errors/packet-over-announced-maximum-v5` | 5.0 | same | Both brokers announce Maximum Packet Size 1048584 (the reference by `max_packet_size 1048584`) and the client sends a PUBLISH of 1 150 000 bytes. Both answer DISCONNECT 0x95. The reference then closes with the rest of the packet unread, which resets the connection; mqttiumd reads and drops it and ends the stream. How the connection ends after the DISCONNECT is outside the protocol and depends on what was still in flight: the closure is compared as `any`. |
| `properties/client-maximum-packet-size-v5` | 5.0 | same | The subscriber announces Maximum Packet Size 40: a PUBLISH that would exceed it is not sent to it, and a smaller one is. |
| `properties/content-type-v5` | 5.0 | same | The Content Type of a PUBLISH is forwarded unaltered to the subscriber. |
| `properties/message-expiry-elapsed-queued-v5` | 5.0 | same | A QoS 1 message with Message Expiry Interval 1 queued for an offline session is not delivered when the session reconnects 2.5 s later; one without expiry is. |
| `properties/message-expiry-elapsed-retained-v5` | 5.0 | same | A retained message with Message Expiry Interval 1 is not sent to a subscription made 2.5 s later. |
| `properties/message-expiry-forwarded-v5` | 5.0 | same | A PUBLISH with Message Expiry Interval 600 is forwarded at once with the property set to the remaining lifetime (compared as the interval 591..600). |
| `properties/payload-format-indicator-v5` | 5.0 | same | Payload Format Indicator 1 on a UTF-8 payload is forwarded unaltered to the subscriber. |
| `properties/response-topic-v5` | 5.0 | same | Response Topic and Correlation Data of a PUBLISH are forwarded unaltered to the subscriber. |
| `properties/topic-alias-v5` | 5.0 | profile-refused | Mosquitto (Topic Alias Maximum 10) resolves the alias and delivers both messages; mqttiumd (Topic Alias Maximum 0) answers the first PUBLISH with DISCONNECT 0x94 (Topic Alias invalid) and closes. |
| `properties/user-properties-v5` | 5.0 | same | User Properties of a PUBLISH are forwarded to the subscriber, repeated names included, in order. |
| `publish/burst-qos0-v4` | 3.1.1 | same | One hundred QoS 0 messages written at once to an online QoS 0 subscriber. A broker may shed QoS 0 for a subscriber it does not write to fast enough, and whether it does depends on timing: mqttiumd drops what finds the subscriber's output queue of sixty-four frames full, and delivered sixty-four of the hundred in about one play in thirty. The count is measured, not compared; the transcript compares that each message that came is one of those sent, came once, and in the order sent. |
| `publish/cross-version-v5-to-v4` | 3.1.1 + 5.0 | same | A MQTT 5 publisher sets Message Expiry Interval and a MQTT 3.1.1 subscriber receives the message, without any property. |
| `publish/duplicate-qos1-publish-v4` | 3.1.1 | same | A QoS 1 PUBLISH is sent again with DUP 1 and the same Packet Identifier after its PUBACK: it is a new publication, acknowledged and delivered again, and DUP is not propagated to the subscriber. |
| `publish/duplicate-qos2-publish-v4` | 3.1.1 | same | A QoS 2 PUBLISH is sent again with DUP 1 before PUBREL: it is acknowledged again and the subscriber still receives one message. |
| `publish/duplicate-qos2-publish-v5` | 5.0 | same | A QoS 2 PUBLISH is sent again with DUP 1 before PUBREL: it is acknowledged again and the subscriber still receives one message. |
| `publish/empty-topic-name-v4` | 3.1.1 | same | A PUBLISH whose Topic Name is empty, with no Topic Alias: it names no topic and the connection is closed. |
| `publish/empty-topic-name-v5` | 5.0 | unspecified | Both close the connection on a PUBLISH with an empty Topic Name and no Topic Alias; Mosquitto after DISCONNECT 0x81 (Malformed Packet), mqttiumd after DISCONNECT 0x82 (Protocol Error). |
| `publish/no-matching-subscribers-v5` | 5.0 | same | A QoS 1 and a QoS 2 PUBLISH that match no subscription: the Reason Codes of PUBACK and PUBREC. |
| `publish/over-maximum-packet-size-qos1-v5` | 5.0 | same | A subscriber with a kept session announces Maximum Packet Size 64 and subscribes at QoS 1. A message of 100 bytes published at QoS 1, which would take 114 bytes on the wire, is accepted from its publisher and not sent to the subscriber, whose connection stays open; the next message, which fits, is delivered and acknowledged. The session then resumes with Maximum Packet Size 1024: nothing is redelivered, and the first PUBLISH it reads is a new message. |
| `publish/over-maximum-packet-size-qos2-v5` | 5.0 | same | A subscriber with a kept session announces Maximum Packet Size 64 and subscribes at QoS 2. A message of 100 bytes published at QoS 2, which would take 114 bytes on the wire, is accepted from its publisher and not sent to the subscriber, whose connection stays open; the next message, which fits, is delivered and acknowledged. The session then resumes with Maximum Packet Size 1024: nothing is redelivered, and the first PUBLISH it reads is a new message. |
| `publish/payload-40000-bytes-v5` | 5.0 | same | A QoS 1 PUBLISH with a 40 000-byte payload, below the Maximum Packet Size either broker announces, is acknowledged and delivered. |
| `publish/pipelined-qos1-v4` | 3.1.1 | same | A MQTT 3.1.1 client writes twenty QoS 1 PUBLISH packets at once, before reading any PUBACK: MQTT 3.1.1 has no Receive Maximum, and all twenty are acknowledged, in the order of the PUBLISH packets (the transcript keeps whether all PUBACK came and whether their Packet Identifiers came as sent, not how many). |
| `publish/pubrel-retransmit-v4` | 3.1.1 | same | PUBREL is sent again after the QoS 2 exchange completed: it is answered by PUBCOMP again and the message is not delivered a second time. |
| `publish/pubrel-retransmit-v5` | 5.0 | unspecified | For a PUBREL whose Packet Identifier is no longer known, Mosquitto answers PUBCOMP 0x00 (Success) and mqttiumd PUBCOMP 0x92 (Packet Identifier not found). |
| `publish/qos-downgrade-v4` | 3.1.1 | same | A subscription granted QoS 1 receives a QoS 0, a QoS 1 and a QoS 2 message: each is delivered at the lower of the two QoS, in publication order. |
| `publish/qos0-v4` | 3.1.1 | same | A QoS 0 message reaches a QoS 0 subscription once, with DUP 0 and no Packet Identifier. |
| `publish/qos0-v5` | 5.0 | same | A QoS 0 message reaches a QoS 0 subscription once, with DUP 0 and no Packet Identifier. |
| `publish/qos0-with-dup-v5` | 5.0 | same | A QoS 0 PUBLISH with DUP 1, which the specification forbids. |
| `publish/qos1-v4` | 3.1.1 | same | QoS 1 in and out: PUBACK to the publisher, PUBLISH to the subscriber, its PUBACK, then silence. |
| `publish/qos1-v5` | 5.0 | same | QoS 1 in and out: PUBACK to the publisher, PUBLISH to the subscriber, its PUBACK, then silence. |
| `publish/qos2-v4` | 3.1.1 | same | QoS 2 in and out: both four-packet exchanges played to their end, then silence. |
| `publish/qos2-v5` | 5.0 | same | QoS 2 in and out: both four-packet exchanges played to their end, then silence. |
| `publish/receive-maximum-inbound-v5` | 5.0 | reference-nonconformant | On a ninth QoS 2 PUBLISH while eight exchanges are open, Mosquitto answers PUBREC 0x97 (Quota exceeded) and keeps the connection open; mqttiumd sends DISCONNECT 0x93 (Receive Maximum exceeded) and closes. |
| `publish/receive-maximum-outbound-v5` | 5.0 | same | The client announces Receive Maximum 2 and acknowledges nothing: of four QoS 1 messages only two are sent to it; once it acknowledges them the next two follow. |
| `publish/topic-with-wildcard-v4` | 3.1.1 | same | A PUBLISH whose Topic Name contains the wildcard `#`: the connection is closed. |
| `publish/topic-with-wildcard-v5` | 5.0 | unspecified | Both close the connection on a Topic Name with a wildcard; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x90 (Topic Name invalid). |
| `retained/clear-v4` | 3.1.1 | same | A retained message with a zero-length payload removes the retained message of its topic: a new subscriber receives nothing. |
| `retained/forty-topics-qos1-v4` | 3.1.1 | same | The content of the retained store: forty topics each keep a retained message (forty is more than the bounded profile of mqttiumd keeps), and a `forty/#` subscription made afterwards at QoS 1 acknowledges every delivery. Each message is owed once, with RETAIN 1, in any order, and nothing after. |
| `retained/forty-topics-v4` | 3.1.1 | same | QoS 0 deliveries of retained messages: forty topics each keep a retained message and a `forty/#` subscription is made afterwards at QoS 0. A broker may shed QoS 0 deliveries for a subscriber it cannot write to fast enough: each message that comes is one of the forty and comes once; how many come is measured, not compared. |
| `retained/live-delivery-clears-retain-flag-v4` | 3.1.1 | same | A message published with RETAIN 1 reaches an already established subscription with RETAIN 0. |
| `retained/qos-downgrade-v4` | 3.1.1 | same | A message retained at QoS 2 is sent to a subscription granted QoS 1 at QoS 1, with RETAIN 1. |
| `retained/replace-v4` | 3.1.1 | same | A second retained message on a topic replaces the first: a new subscriber receives only the second. |
| `retained/retain-as-published-v5` | 5.0 | same | Retain As Published 1: a message published with RETAIN 1 keeps the flag on live delivery, and one published with RETAIN 0 does not get it. |
| `retained/retain-handling-0-v5` | 5.0 | same | Retain Handling 0: the retained message is sent at each SUBSCRIBE. |
| `retained/retain-handling-1-v5` | 5.0 | same | Retain Handling 1: the retained message is sent only when the subscription is new. |
| `retained/retain-handling-2-v5` | 5.0 | same | Retain Handling 2: the retained message is never sent at SUBSCRIBE. |
| `retained/set-and-deliver-v4` | 3.1.1 | same | A retained message is sent, with RETAIN 1, to a subscription made afterwards. |
| `retained/set-and-deliver-v5` | 5.0 | same | A retained message is sent, with RETAIN 1, to a subscription made afterwards. |
| `retained/thirty-two-topics-qos1-v4` | 3.1.1 | same | The content of the retained store: thirty-two topics each keep a retained message (thirty-two is what the bounded profile of mqttiumd keeps), and a `thirty-two/#` subscription made afterwards at QoS 1 acknowledges every delivery. Each message is owed once, with RETAIN 1, in any order, and nothing after. |
| `retained/wildcard-subscription-v4` | 3.1.1 | same | `tree/#` receives the retained messages of the parent and of every level below, and no other. |
| `session/ninth-exchange-after-reconnection-v4` | 3.1.1 | reference-nonconformant | A 3.1.1 session owes the PUBREL of eight exchanges and publishes a ninth QoS 2 message. Mosquitto answers PUBREC, later answers its PUBREL with PUBCOMP, and the subscriber never receives the message (upstream ticket 3809); mqttiumd does not acknowledge the PUBLISH and closes the connection. On both the subscriber ends with the first eight messages. |
| `session/ninth-exchange-after-reconnection-v5` | 5.0 | reference-nonconformant | An MQTT 5 session owes the PUBREL of eight exchanges, the Receive Maximum, and publishes a ninth QoS 2 message. Mosquitto answers PUBREC 0x97 (Quota exceeded), keeps the connection, and later answers the PUBREL of that identifier with PUBCOMP 0x00; mqttiumd sends DISCONNECT 0x97 (Quota exceeded) and closes, and answers that PUBREL with PUBCOMP 0x92 (Packet Identifier not found). |
| `session/offline-qos0-v4` | 3.1.1 | same | A QoS 0 message published while a persistent session with a QoS 1 subscription is offline: whether it is kept for the reconnection. |
| `session/offline-queue-qos1-v4` | 3.1.1 | same | Two QoS 1 messages published while a persistent session is offline are sent after CONNACK on reconnection, in order and with DUP 0; once acknowledged they are not sent again. |
| `session/offline-queue-qos1-v5` | 5.0 | same | Two QoS 1 messages published while a persistent session is offline are sent after CONNACK on reconnection, in order and with DUP 0; once acknowledged they are not sent again. |
| `session/offline-queue-qos2-v4` | 3.1.1 | same | A QoS 2 message published while a persistent session is offline is delivered on reconnection through the complete PUBLISH, PUBREC, PUBREL, PUBCOMP exchange. |
| `session/offline-queue-qos2-v5` | 5.0 | same | A QoS 2 message published while a persistent session is offline is delivered on reconnection through the complete PUBLISH, PUBREC, PUBREL, PUBCOMP exchange. |
| `session/redelivery-pubrel-after-expiry-v5` | 5.0 | reference-nonconformant | A QoS 2 delivery with Message Expiry Interval 1 reached PUBREL and lost its connection before PUBCOMP. When the session comes back after the interval, Mosquitto sends nothing after CONNACK (upstream ticket 3744); mqttiumd sends the PUBREL again and completes on PUBCOMP. |
| `session/redelivery-pubrel-v4` | 3.1.1 | same | The client answered a QoS 2 PUBLISH with PUBREC, received PUBREL and lost the connection before PUBCOMP: on reconnection the broker sends PUBREL again, not the PUBLISH. |
| `session/redelivery-pubrel-v5` | 5.0 | same | The client answered a QoS 2 PUBLISH with PUBREC, received PUBREL and lost the connection before PUBCOMP: on reconnection the broker sends PUBREL again, not the PUBLISH. |
| `session/redelivery-qos1-v4` | 3.1.1 | same | A QoS 1 PUBLISH the client received and did not acknowledge before the connection broke is sent again on reconnection with DUP 1 and the same Packet Identifier. |
| `session/redelivery-qos1-v5` | 5.0 | same | A QoS 1 PUBLISH the client received and did not acknowledge before the connection broke is sent again on reconnection with DUP 1 and the same Packet Identifier. |
| `subscribe/dollar-topics-vs-wildcards-v4` | 3.1.1 | same | Subscriptions to `#` and to `+/broker/version` receive a plain topic and nothing that begins with `$`: neither the reference's own `$SYS` tree nor a `$SYS/...` message a client publishes. |
| `subscribe/hash-wildcard-v4` | 3.1.1 | same | `sport/#` matches the parent level and every level below it. |
| `subscribe/invalid-empty-filter-v5` | 5.0 | unspecified | Both close the connection on the invalid Topic Filter; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x82 (Protocol Error). |
| `subscribe/invalid-hash-not-last-v4` | 3.1.1 | same | SUBSCRIBE with the invalid Topic Filter `bad/#/x`: the connection is closed. |
| `subscribe/invalid-hash-not-last-v5` | 5.0 | unspecified | Both close the connection on the invalid Topic Filter; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x82 (Protocol Error). |
| `subscribe/many-filters-effective-v4` | 3.1.1 | same | A SUBSCRIBE with twelve Topic Filters is handled as twelve SUBSCRIBE packets would be, with one SUBACK: every filter is granted, and a message published on the twelfth is delivered. |
| `subscribe/many-filters-effective-v5` | 5.0 | same | A SUBSCRIBE with twelve Topic Filters is handled as twelve SUBSCRIBE packets would be, with one SUBACK: every filter is granted, and a message published on the twelfth is delivered. |
| `subscribe/many-subscriptions-v4` | 3.1.1 | same | One client makes 150 subscriptions, five Topic Filters per SUBSCRIBE packet: all are granted. |
| `subscribe/no-local-v5` | 5.0 | same | No Local: a client does not receive its own publication and receives another client's. |
| `subscribe/overlapping-subscriptions-v4` | 3.1.1 | unspecified | For one message matching two subscriptions of a MQTT 3.1.1 client, Mosquitto sends one copy per subscription (QoS 0 and QoS 1); mqttiumd sends one copy at the highest granted QoS. |
| `subscribe/overlapping-subscriptions-v5` | 5.0 | unspecified | For one message matching two subscriptions of a MQTT 5 client, Mosquitto sends one copy per subscription (QoS 0 and QoS 1); mqttiumd sends one copy at the highest granted QoS. |
| `subscribe/plus-wildcard-v4` | 3.1.1 | same | `sport/+` matches exactly one level, the empty one included, and not the parent. |
| `subscribe/qos-grants-v4` | 3.1.1 | same | A SUBSCRIBE with three filters at QoS 0, 1 and 2: one granted QoS per filter, in order. |
| `subscribe/qos-grants-v5` | 5.0 | same | A SUBSCRIBE with three filters at QoS 0, 1 and 2: one granted QoS per filter, in order. |
| `subscribe/reserved-option-bits-v5` | 5.0 | same | A SUBSCRIBE whose Subscription Options byte has a reserved bit set (0x40) is a Malformed Packet: the connection is closed. |
| `subscribe/share-literal-filter-v4` | 3.1.1 | profile-refused | Mosquitto, which offers shared subscriptions to MQTT 3.1.1 clients too, grants `$share/group/lit/t` at QoS 0; mqttiumd reads it as an ordinary Topic Filter of MQTT 3.1.1 and refuses to hold it: SUBACK with return code 0x80 (Failure), connection kept. |
| `subscribe/shared-subscription-v5` | 5.0 | profile-refused | Mosquitto grants `$share/group/shared/t` and delivers to it; mqttiumd, whose CONNACK announces Shared Subscription Available 0, answers the SUBSCRIBE with DISCONNECT 0x9E (Shared Subscriptions not supported) and closes; the plain subscription of the other client is served by both. |
| `subscribe/subscription-identifier-v5` | 5.0 | profile-refused | Mosquitto accepts the Subscription Identifier and returns it in the PUBLISH; mqttiumd, whose CONNACK announces Subscription Identifier Available 0, answers the SUBSCRIBE with DISCONNECT 0xA1 (Subscription Identifiers not supported) and closes. |
| `subscribe/sys-explicit-filter-v4` | 3.1.1 | profile-refused | Mosquitto sends its retained `$SYS/broker/version` message; mqttiumd grants the subscription and publishes nothing under `$SYS`. |
| `subscribe/twelve-filters-v4` | 3.1.1 | same | One SUBSCRIBE packet with twelve Topic Filters is answered by a SUBACK with twelve granted QoS. |
| `subscribe/unsubscribe-nine-filters-v4` | 3.1.1 | same | An UNSUBSCRIBE with nine Topic Filters is handled as nine UNSUBSCRIBE packets would be, with one UNSUBACK: the ninth subscription is removed like the first. Of two messages published afterwards, one on the ninth filter and one on a filter that was kept, the subscriber reads the second only. |
| `subscribe/unsubscribe-nine-filters-v5` | 5.0 | same | An UNSUBSCRIBE with nine Topic Filters is handled as nine UNSUBSCRIBE packets would be, with one UNSUBACK: the ninth subscription is removed like the first. Of two messages published afterwards, one on the ninth filter and one on a filter that was kept, the subscriber reads the second only. |
| `subscribe/unsubscribe-share-filter-v4` | 3.1.1 | same | A MQTT 3.1.1 UNSUBSCRIBE of `$share/group/lit/t`, which was never subscribed, is acknowledged like the one of any other Topic Filter. |
| `subscribe/unsubscribe-v4` | 3.1.1 | same | UNSUBSCRIBE removes exactly the filter named, compared character by character: `unsub/#` goes, `unsub/a` stays; an unknown filter is acknowledged too. |
| `subscribe/unsubscribe-v5` | 5.0 | same | An UNSUBSCRIBE with an existing and an unknown filter: one Reason Code each, in order, then no more delivery. |
| `subscribe/user-property-v5` | 5.0 | same | A SUBSCRIBE carrying a User Property is accepted like one without. |
| `will/abrupt-close-qos0-v4` | 3.1.1 | profile-refused | Mosquitto accepts a QoS 0 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with return code 3 (Server unavailable). |
| `will/abrupt-close-qos1-v4` | 3.1.1 | same | A client with a QoS 1 Will closes its connection without DISCONNECT: the Will is published to a QoS 2 subscription at QoS 1. |
| `will/abrupt-close-qos1-v5` | 5.0 | same | A client with a QoS 1 Will closes its connection without DISCONNECT: the Will is published to a QoS 2 subscription at QoS 1. |
| `will/abrupt-close-qos2-v4` | 3.1.1 | profile-refused | Mosquitto accepts a QoS 2 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with return code 3 (Server unavailable). |
| `will/abrupt-close-qos2-v5` | 5.0 | profile-refused | Mosquitto accepts a QoS 2 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with CONNACK 0x83 (the code MQTT-3.2.2-12 recommends is 0x9B). |
| `will/delay-interval-v5` | 5.0 | profile-refused | Mosquitto accepts Will Delay Interval 1 and publishes the Will after the delay; mqttiumd refuses the CONNECT with CONNACK 0x83 (Implementation specific error). |
| `will/disconnect-discards-will-v4` | 3.1.1 | same | A normal DISCONNECT discards the Will: the subscriber receives only a later marker message. |
| `will/disconnect-discards-will-v5` | 5.0 | same | A normal DISCONNECT discards the Will: the subscriber receives only a later marker message. |
| `will/disconnect-implementation-error-expiry-zero-v5` | 5.0 | reference-nonconformant | On DISCONNECT with Reason Code 0x83 (Implementation specific error) and Session Expiry Interval 0 from a client with a Will, Mosquitto ends the session and publishes no Will; mqttiumd ends the session and publishes the Will. |
| `will/disconnect-normal-expiry-zero-v5` | 5.0 | same | A client with a Will and a session of sixty seconds sends DISCONNECT with Reason Code 0x00 and Session Expiry Interval 0. What is recorded: whether the Will is published, and whether the session is still there when the same ClientID comes back with Clean Start 0. |
| `will/disconnect-unspecified-error-expiry-zero-v5` | 5.0 | reference-nonconformant | On DISCONNECT with Reason Code 0x80 (Unspecified error) and Session Expiry Interval 0 from a client with a Will, Mosquitto ends the session and publishes no Will; mqttiumd ends the session and publishes the Will. |
| `will/disconnect-with-will-expiry-zero-v5` | 5.0 | same | A client with a Will and a session of sixty seconds sends DISCONNECT with Reason Code 0x04 and Session Expiry Interval 0. What is recorded: whether the Will is published, and whether the session is still there when the same ClientID comes back with Clean Start 0. |
| `will/disconnect-with-will-v5` | 5.0 | same | DISCONNECT with Reason Code 0x04 (Disconnect with Will Message): the Will is published. |
| `will/retain-v4` | 3.1.1 | same | A QoS 1 Will with Will Retain 1 is published on abrupt close and kept as the retained message of its topic: a later subscriber receives it with RETAIN 1. |
| `will/retain-v5` | 5.0 | same | A QoS 1 Will with Will Retain 1 is published on abrupt close and kept as the retained message of its topic: a later subscriber receives it with RETAIN 1. |
| `will/takeover-v5` | 5.0 | same | A client with a Will is taken over by a new connection with the same ClientID and Clean Start 1: the Will of the first connection is published. |

## profile-refused (15)

- `connect/connack-capabilities-v5` (MQTT 5.0): Mosquitto announces Receive Maximum 20, Topic Alias Maximum 10 and Maximum Packet Size 2 000 000 and leaves every other capability at its default; mqttiumd announces Receive Maximum 8, Maximum Packet Size 1 048 584, Topic Alias Maximum 0, and shared subscriptions and Subscription Identifiers not available.
  - Lot: The capabilities of the v0.1 profile: Topic Alias from client to server (Topic Alias Maximum above 0); Shared subscriptions (CMP-002); Subscription Identifiers; Receive Maximum and Maximum Packet Size of the runtime profile
- `connect/empty-clientid-v4` (MQTT 3.1.1): Mosquitto accepts a zero-length ClientID with Clean Session 1 (CONNACK 0); mqttiumd refuses it with return code 2 (Identifier rejected) and closes.
  - Statement: MQTT-3.1.3-6 makes accepting it optional; MQTT-3.1.3-9 is how mqttiumd refuses
  - Lot: Server-assigned Client Identifier for a zero-length ClientID
- `connect/empty-clientid-v5` (MQTT 5.0): Mosquitto accepts a zero-length ClientID and returns an Assigned Client Identifier; mqttiumd refuses it with CONNACK 0x85 (Client Identifier not valid) and closes.
  - Statement: MQTT-3.1.3-6 makes accepting it optional
  - Lot: Server-assigned Client Identifier for a zero-length ClientID
- `connect/legacy-mqtt31` (MQTT 3.1): Mosquitto accepts a MQTT 3.1 CONNECT (protocol name MQIsdp, level 3) with CONNACK 0; mqttiumd closes the connection without CONNACK.
  - Lot: MQTT 3.1 (MQIsdp): a separate extension, outside the v0.1 profile (chapter 1, section 3)
- `connect/username-password-v4` (MQTT 3.1.1): Mosquitto, which accepts anonymous clients here, accepts a CONNECT with User Name and Password; mqttiumd refuses any credentials with return code 4 (Bad user name or password) and closes.
  - Lot: Authentication: user name and password checked against a password file (chapter 1, section 3, row Authentification; CFG-004, CFG-005)
- `connect/username-password-v5` (MQTT 5.0): Mosquitto, which accepts anonymous clients here, accepts a CONNECT with User Name and Password; mqttiumd refuses any credentials with CONNACK 0x83 (Implementation specific error) and closes.
  - Lot: Authentication: user name and password checked against a password file (chapter 1, section 3, row Authentification; CFG-004, CFG-005)
- `properties/topic-alias-v5` (MQTT 5.0): Mosquitto (Topic Alias Maximum 10) resolves the alias and delivers both messages; mqttiumd (Topic Alias Maximum 0) answers the first PUBLISH with DISCONNECT 0x94 (Topic Alias invalid) and closes.
  - Statement: MQTT-3.2.2-18: with Topic Alias Maximum 0 the client is the one at fault
  - Lot: Topic Alias from client to server (Topic Alias Maximum above 0)
- `subscribe/share-literal-filter-v4` (MQTT 3.1.1): Mosquitto, which offers shared subscriptions to MQTT 3.1.1 clients too, grants `$share/group/lit/t` at QoS 0; mqttiumd reads it as an ordinary Topic Filter of MQTT 3.1.1 and refuses to hold it: SUBACK with return code 0x80 (Failure), connection kept.
  - Statement: MQTT-3.8.4-5 allows a return code that says the subscription failed
  - Lot: Shared subscriptions (CMP-002)
  - Decision record: 0049
- `subscribe/shared-subscription-v5` (MQTT 5.0): Mosquitto grants `$share/group/shared/t` and delivers to it; mqttiumd, whose CONNACK announces Shared Subscription Available 0, answers the SUBSCRIBE with DISCONNECT 0x9E (Shared Subscriptions not supported) and closes; the plain subscription of the other client is served by both.
  - Statement: MQTT-4.13.1-1, with section 3.2.2.3.13: how a broker that announced the feature as not available refuses it
  - Lot: Shared subscriptions (CMP-002)
- `subscribe/subscription-identifier-v5` (MQTT 5.0): Mosquitto accepts the Subscription Identifier and returns it in the PUBLISH; mqttiumd, whose CONNACK announces Subscription Identifier Available 0, answers the SUBSCRIBE with DISCONNECT 0xA1 (Subscription Identifiers not supported) and closes.
  - Statement: MQTT-4.13.1-1, with section 3.2.2.3.12: how a broker that announced the feature as not available refuses it
  - Lot: Subscription Identifiers
- `subscribe/sys-explicit-filter-v4` (MQTT 3.1.1): Mosquitto sends its retained `$SYS/broker/version` message; mqttiumd grants the subscription and publishes nothing under `$SYS`.
  - Lot: Operations: metrics published under `$SYS` (chapter 1, section 3, row Exploitation)
- `will/abrupt-close-qos0-v4` (MQTT 3.1.1): Mosquitto accepts a QoS 0 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with return code 3 (Server unavailable).
  - Lot: Will messages beyond QoS 1 without delay: Will QoS 0 and 2, Will Delay Interval
- `will/abrupt-close-qos2-v4` (MQTT 3.1.1): Mosquitto accepts a QoS 2 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with return code 3 (Server unavailable).
  - Lot: Will messages beyond QoS 1 without delay: Will QoS 0 and 2, Will Delay Interval
- `will/abrupt-close-qos2-v5` (MQTT 5.0): Mosquitto accepts a QoS 2 Will and publishes it on abrupt close; mqttiumd supports QoS 1 Wills only and refuses the CONNECT with CONNACK 0x83 (the code MQTT-3.2.2-12 recommends is 0x9B).
  - Lot: Will messages beyond QoS 1 without delay: Will QoS 0 and 2, Will Delay Interval
- `will/delay-interval-v5` (MQTT 5.0): Mosquitto accepts Will Delay Interval 1 and publishes the Will after the delay; mqttiumd refuses the CONNECT with CONNACK 0x83 (Implementation specific error).
  - Lot: Will messages beyond QoS 1 without delay: Will QoS 0 and 2, Will Delay Interval

## reference-nonconformant (6)

- `publish/receive-maximum-inbound-v5` (MQTT 5.0): On a ninth QoS 2 PUBLISH while eight exchanges are open, Mosquitto answers PUBREC 0x97 (Quota exceeded) and keeps the connection open; mqttiumd sends DISCONNECT 0x93 (Receive Maximum exceeded) and closes.
  - Statement: MQTT-4.13.1-1, with section 3.3.4: a client that exceeds Receive Maximum (MQTT-3.3.4-7) makes a Protocol Error for which the Reason Code 0x93 is given, so the server must close the connection
  - mqttiumd: Sends DISCONNECT 0x93 (Receive Maximum exceeded) and closes the network connection.
  - Decision record: 0049
- `session/ninth-exchange-after-reconnection-v4` (MQTT 3.1.1): A 3.1.1 session owes the PUBREL of eight exchanges and publishes a ninth QoS 2 message. Mosquitto answers PUBREC, later answers its PUBREL with PUBCOMP, and the subscriber never receives the message (upstream ticket 3809); mqttiumd does not acknowledge the PUBLISH and closes the connection. On both the subscriber ends with the first eight messages.
  - Statement: MQTT-4.3.3-2: a PUBREC says the receiver accepted ownership of the message, and MQTT-4.5.0-1 that a message it owns is added to the matching sessions
  - mqttiumd: Does not acknowledge the ninth PUBLISH and closes the network connection: the message stays the client's. A PUBREL for its identifier is answered with PUBCOMP, as 3.1.1 answers any PUBREL.
  - Decision record: 0061
- `session/ninth-exchange-after-reconnection-v5` (MQTT 5.0): An MQTT 5 session owes the PUBREL of eight exchanges, the Receive Maximum, and publishes a ninth QoS 2 message. Mosquitto answers PUBREC 0x97 (Quota exceeded), keeps the connection, and later answers the PUBREL of that identifier with PUBCOMP 0x00; mqttiumd sends DISCONNECT 0x97 (Quota exceeded) and closes, and answers that PUBREL with PUBCOMP 0x92 (Packet Identifier not found).
  - Statement: MQTT-4.13.1-1, with section 3.3.4: a client that exceeds Receive Maximum (MQTT-3.3.4-7) makes a Protocol Error, so the server must close the connection
  - mqttiumd: Sends DISCONNECT 0x97 (Quota exceeded) and closes the network connection; the bound is the session's, whatever the connection.
  - Decision record: 0061
- `session/redelivery-pubrel-after-expiry-v5` (MQTT 5.0): A QoS 2 delivery with Message Expiry Interval 1 reached PUBREL and lost its connection before PUBCOMP. When the session comes back after the interval, Mosquitto sends nothing after CONNACK (upstream ticket 3744); mqttiumd sends the PUBREL again and completes on PUBCOMP.
  - Statement: MQTT-4.4.0-1: the PUBREL of an exchange in progress is resent on reconnection; the Message Expiry Interval of section 3.3.2.3.3 ends the delivery of a message that has not started, not an exchange that has
  - mqttiumd: Sends the PUBREL again with its Packet Identifier after CONNACK and ends the exchange on PUBCOMP.
  - Decision record: 0062
- `will/disconnect-implementation-error-expiry-zero-v5` (MQTT 5.0): On DISCONNECT with Reason Code 0x83 (Implementation specific error) and Session Expiry Interval 0 from a client with a Will, Mosquitto ends the session and publishes no Will; mqttiumd ends the session and publishes the Will.
  - Statement: MQTT-3.1.2-8, with MQTT-3.14.4-3: the Will is published when the connection closes unless a DISCONNECT with Reason Code 0x00 discarded it
  - mqttiumd: Publishes the Will and ends the session.
  - Decision record: 0058
- `will/disconnect-unspecified-error-expiry-zero-v5` (MQTT 5.0): On DISCONNECT with Reason Code 0x80 (Unspecified error) and Session Expiry Interval 0 from a client with a Will, Mosquitto ends the session and publishes no Will; mqttiumd ends the session and publishes the Will.
  - Statement: MQTT-3.1.2-8, with MQTT-3.14.4-3: the Will is published when the connection closes unless a DISCONNECT with Reason Code 0x00 discarded it
  - mqttiumd: Publishes the Will and ends the session.
  - Decision record: 0058

## unspecified (9)

- `connect/bad-protocol-level-v4` (MQTT 3.1.1): For a CONNECT with protocol level 6, Mosquitto answers a CONNACK in the MQTT 5 layout with Reason Code 0x84 (Unsupported Protocol Version) and closes; mqttiumd answers the MQTT 3.1.1 CONNACK with return code 1 (unacceptable protocol version) and closes.
  - Statement: MQTT-3.1.2-2 asks for return code 0x01 and a close; MQTT 5.0 section 3.1.2.2 allows CONNACK 0x84 before the close. A level that is neither 4 nor 5 names neither layout
  - Decision record: 0062
- `errors/malformed-remaining-length-v5` (MQTT 5.0): Both close the connection on a Remaining Length of five bytes; Mosquitto without a packet, mqttiumd after DISCONNECT 0x81 (Malformed Packet).
  - Statement: MQTT-4.13.1-1: both close; the DISCONNECT before the close is recommended, not required
  - Decision record: 0049
- `publish/empty-topic-name-v5` (MQTT 5.0): Both close the connection on a PUBLISH with an empty Topic Name and no Topic Alias; Mosquitto after DISCONNECT 0x81 (Malformed Packet), mqttiumd after DISCONNECT 0x82 (Protocol Error).
  - Statement: MQTT-4.13.1-1: both close; section 3.3.2.1 calls an empty Topic Name without a Topic Alias a Protocol Error
  - Decision record: 0058
- `publish/pubrel-retransmit-v5` (MQTT 5.0): For a PUBREL whose Packet Identifier is no longer known, Mosquitto answers PUBCOMP 0x00 (Success) and mqttiumd PUBCOMP 0x92 (Packet Identifier not found).
  - Statement: MQTT-4.3.3-11: both answer with PUBCOMP
  - Decision record: 0049
- `publish/topic-with-wildcard-v5` (MQTT 5.0): Both close the connection on a Topic Name with a wildcard; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x90 (Topic Name invalid).
  - Statement: MQTT-3.3.2-2 is what the client violates; the Reason Code is not prescribed
  - Decision record: 0049
- `subscribe/invalid-empty-filter-v5` (MQTT 5.0): Both close the connection on the invalid Topic Filter; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x82 (Protocol Error).
  - Statement: MQTT-4.13.1-1: both close; the Reason Code is not prescribed
  - Decision record: 0049
- `subscribe/invalid-hash-not-last-v5` (MQTT 5.0): Both close the connection on the invalid Topic Filter; Mosquitto with DISCONNECT 0x81 (Malformed Packet), mqttiumd with DISCONNECT 0x82 (Protocol Error).
  - Statement: MQTT-4.13.1-1: both close; the Reason Code is not prescribed
  - Decision record: 0049
- `subscribe/overlapping-subscriptions-v4` (MQTT 3.1.1): For one message matching two subscriptions of a MQTT 3.1.1 client, Mosquitto sends one copy per subscription (QoS 0 and QoS 1); mqttiumd sends one copy at the highest granted QoS.
  - Statement: MQTT-3.3.5-1 requires the copy at the highest QoS; section 3.3.5 allows a copy per subscription
  - Decision record: 0049
- `subscribe/overlapping-subscriptions-v5` (MQTT 5.0): For one message matching two subscriptions of a MQTT 5 client, Mosquitto sends one copy per subscription (QoS 0 and QoS 1); mqttiumd sends one copy at the highest granted QoS.
  - Statement: MQTT-3.3.4-2 requires the copy at the highest QoS; section 3.3.4 allows further copies
  - Decision record: 0049
