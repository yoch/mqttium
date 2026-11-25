# 0058 — Client DISCONNECT reason codes, two decoder classes and TLS close_notify

Status: **Proposed** (23 November 2026). Applied to `mqttiumd` (runtime tier,
ADR 0044). It answers an external audit of `47e1d12` against Mosquitto 2.1.4;
[report 0031](../reports/0031-external-audit-47e1d12-wave-1.md) holds the
evidence. No public type of the core tier, durable encoding, injection point
or schema changes. The contract digest changes all the same: the requirement
register records the SHA-256 of the three records this one amends.

## Context

The audit established four gaps on the wire or in the decoder.

1. A client DISCONNECT whose reason code was neither 0x00 nor 0x04 was
   classified `DisconnectOutsideProfile` and refused with a server
   DISCONNECT 0x83. Its Session Expiry Interval was read only for the
   zero-to-non-zero rule, so the close used the CONNECT value: a session
   opened with 600 s and ended with `0x80` or `0x83` and interval 0 was
   resumed with Session Present 1 and its subscription.
2. Will QoS 3 was `InvalidWillFlags`, a Protocol Error. MQTT 5 §3.1.2.6
   [MQTT-3.1.2-12] names it a Malformed Packet.
3. A PUBLISH with an empty Topic Name and no Topic Alias was answered with
   DISCONNECT 0x90, Topic Name Invalid. MQTT 5 §§3.3.2.1 and 3.3.4 make it a
   Protocol Error.
4. The broker never sent a TLS close_notify alert: every close ended in
   `Control::stop`, a TCP shutdown. A TLS 1.3 client that had sent a normal
   DISCONNECT read an unexpected EOF. RFC 8446 §6.1 requires close_notify
   before the write side is closed unless an error alert was sent.

## Decision

### Every legal client DISCONNECT reason is served

The reason codes a client may send (MQTT 5 §3.14.2.1: 0x00, 0x04, 0x80 to
0x83, 0x90, 0x93 to 0x99) all decode to `Packet::Disconnect`. A DISCONNECT is
the last packet of its sender: it is applied, never answered.

- Its Session Expiry Interval replaces the CONNECT value whatever the reason
  (§3.14.2.2.2).
- Only reason 0x00 discards the Will (§3.1.2.5 [MQTT-3.1.2-8, -10]); every
  other reason leads to Pending publication.
- A server-only or undefined reason stays `InvalidDisconnectReason`, a
  Protocol Error, and a session opened with interval 0 still cannot be given
  a non-zero one (Protocol Error 0x82), whatever the reason.

`DisconnectOutsideProfile`, `Error::in_session` and the `disconnect_expiry`
field became unreachable and are removed. This amends the Will table of
record 0034, whose rows named DISCONNECT0 and DISCONNECT4 only, and the
DISCONNECT rule of record 0029.

Mosquitto 2.1.4 ends the session and publishes no Will for `0x80` and `0x83`.
The specification prevails (record 0049): the difference is to be classified
when the compatibility bench has a scenario for it.

### Reserved Will QoS and empty Topic Name

- Will QoS 3 is `ReservedWillQos`, a Malformed Packet (0x81), with or without
  the Will Flag. Will QoS or Will Retain bits without the Will Flag parse and
  are forbidden data: they stay `InvalidWillFlags`, a Protocol Error. Neither
  supplies a validated response limit, so both are closed without a CONNACK;
  the class is what the `packet_refused` event records.
- An empty PUBLISH Topic Name is `EmptyTopicName`, a Protocol Error answered
  with DISCONNECT 0x82. Topic Alias Maximum 0 is announced and any alias is
  refused first with 0x94, so an empty name never designates a topic. The
  check is at the PUBLISH call site: `codec::topic` is shared with the Will
  Topic, the Response Topic and the policy validation, which keep Topic Name
  Invalid (0x90), as does a Topic Name holding a wildcard.

### close_notify on an orderly TLS close

The TLS state lives in its task, on a reactor thread, and only that state can
seal the alert. `Connection::close` therefore no longer shuts a TLS socket on
the core thread: it sets `cancelled` and wakes the task. The task ends at its
next turn, and `tls::Task::finish` sends close_notify, then runs
`Control::stop` (shutdown, then the terminal reason).

- The alert is sent for the close the core decided (`cancelled`) and in
  answer to the peer's own close_notify (`tls_close_notify`), once the
  handshake has completed. It is not sent after a TLS or transport error.
- It is not sent while a sealed frame has not left whole: the alert would
  not decrypt, and the stream is then truncated in fact.
- The attempt never waits. A peer that does not read gets the end of the
  transport alone.

`Runtime::close` keeps a TLS peer in the table until its task has finished;
the retiring pass of `connections` then runs `finish_close`. A takeover opens
the new generation only once the old peer has left the table
(`pump_will_connects`), so the old socket is shut before the session is bound
again. `models/takeover` has this variant as `Deferred`, with two mutants:
removing the peer without waiting for the task violates
`OldClosedBeforeNewBound` and `OldSilentOnceNewBound`.

A plain TCP connection is unchanged: its socket is shut on the core thread.

## Consequences

- A client that ends with an error reason and interval 0 no longer leaves a
  session behind, and no longer receives a DISCONNECT 0x83 after its own.
- `tests/system/session_reception.py` asserts the former refusal in its case
  `disconnect_outside_profile_reason`. That reception is frozen evidence of
  `51be4079` and is not repaired; runtime tests cover the new behaviour.
- The end of a TLS session is committed one reactor turn later than before:
  `finish_close` waits for the task. The durable guarantee is unchanged: a
  DISCONNECT has no acknowledgement, and its effect is durable at its commit.
- A TLS client reads the end of the stream instead of a truncation, and can
  tell the two apart.

## Verification and remaining limit

The differential scenarios `will/disconnect-*-expiry-zero-v5` and
`publish/empty-topic-name-*` exercise the MQTT cases against Mosquitto
2.1.4. The executable test in `crates/mqttiumd/tests/disconnect_durability/recovery.py`
pauses a TLS record after three ciphertext bytes. It exercises both
drain and truncation at takeover, checks the old connection's terminal
observations, and verifies the resumed client's CONNACK and DUP replay.

Keep Alive expiry still closes without a server DISCONNECT 0x8D.
