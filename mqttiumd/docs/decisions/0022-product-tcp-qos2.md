# 0022 — First product TCP composition, durable incoming QoS 2

Status: **Proposed**. Experimental implementation choice, not acceptance of
the historical proposed records or permission to merge main.

## Profile and authority

The product binary depends on contract/core/store-api/SQLite, never on the
lab at runtime. It starts on an absent database path without a fixture,
creates the SQLite data set once, opens one durable epoch, consumes its
grant into OwnerAuthority, creates the bounded Outbox/Owner and completes
ordered recovery before listening or reporting readiness. Existing invalid
files and refused/partial recovery stop with typed causes; they are not
replaced or reset. The epoch belongs to broker startup, not connections.

The accepted experimental profile is MQTT 5, explicit nonempty ClientID,
Clean Start 0, Session Expiry Interval 0xffffffff, incoming non-retained
QoS 2, no Will/authentication/subscriptions/outbound delivery. Keep Alive
and PINGREQ/PINGRESP are implemented. This is not full MQTT 5 conformance:
MQTT 5 requires lower QoS support too. Maximum QoS is omitted, since its
property permits only 0/1; QoS 1 is a separate next increment, not QoS 2
with a different acknowledgement.

## Confirmed session facts

The exclusive writer accepts a bounded, uniquely numbered session request.
It first reads a coherent bounded snapshot to find an existing ClientID,
otherwise applies CreateSession exactly once. Confirmed success becomes:

```rust,ignore
Input::SessionEstablished {
    connection: Some(generation), session, client_id, revision,
    session_present: false, // true for an existing durable session
}
```

The runtime correlates the request/result before constructing this trusted
fact. It is distinct from Recovery, which is never reopened. A late result
whose socket closed uses connection=None: durable knowledge is installed,
but no connection is bound and no CONNACK emitted. WriteFailed is unknown,
not certainly absent: the connection is refused without blind retry. A new
connection can perform a new serialized lookup before any creation.
Session Present reflects confirmed creation/existence. A successful CONNACK
comes from the core only after it applies the confirmed fact.

Input::ConnectionClosed reports actual transport closure; it does not forge
a FrameResult. InboundPacket::Pingreq and OutboundFrame::Pingresp keep valid
PING traffic in core transitions. Plan/apply, reservation-before-mutation,
bounded tables and the existing maximum of two effects per transition stay.

## Runtime and lifecycle

One logical owner and one exclusive writer are used. Connections have bounded
independent input/output queues and socket tasks, with absolute packet/write
deadlines, cancellation and no SQLite call in the network reactor. Store
results remain correlated after socket closure. Unaccepted effects remain
in their Outbox; transport failures resolve accepted frames explicitly.
Generations never repeat. The runtime rejects stale ingress independently
of the core's finite closed-connection window, while reconciling necessary
late writer results. A pending fresh CONNECT holds its bounded ingress until
session confirmation, including pipelined packets. A handed-back packet that
cannot reserve volatile capacity closes its own transport; nonpacket retries
make a bounded fair pass so releases/results cannot be starved. Receive
Maximum completion uses the PUBLISH ingress serial, so an old PUBCOMP write
receipt cannot free a reused Packet Identifier. No global lock is held across
socket I/O.

No publication is retained as an archive: metadata can be removed once its
last inbound marker and obligation are gone. Existing committed admission
receipts protect publication/payload identity reuse within the executable
epoch; collected epochs are closed and scope guards prohibit reusing their
identities in a newer operation. Those scans remain bounded. This requires
no new SQLite schema and does not redesign general administration.

## Evidence boundary

The new TCP reception is a finite system test with real external clients,
client bytes, durable facts, binary/source identity and process receipts.
It is not called qualifying by the fixture-based campaign oracle. Existing
oracle qualification, crash, model and proof gates remain unchanged.
Test-only explicit product hooks expose acknowledged pre-commit and
committed-before-PUBREC barriers; their code is absent by default. Controls
mutate early acknowledgement and stale-ingress admission in isolated builds.
They must be detected for their precise reason, not patch/build failure or
an unexplained timeout. Each system witness belongs to its own run.

The initial product budgets are finite and compatible with durable recovery:
16 connections, 128 sessions/exchanges, packet 65,536 bytes, ClientID/topic
1,024 bytes, payload 32,768 bytes, input/output queues 4 per connection,
writer queue 32, commands in flight 16, timers 512, receipt slots 8,192.
Partial packet deadline 5 seconds, CONNECT deadline 3 seconds, absolute
write deadline 250 milliseconds, Keep Alive timeout 1.5 times its value.
Receipt/epoch-0 collection limits remain explicit; indefinite operation is
not promised. Exact effective settings and narrower test budgets are kept
with the system receipt.
