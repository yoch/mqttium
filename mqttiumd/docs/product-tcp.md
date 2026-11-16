# Experimental durable incoming QoS 1 and QoS 2 over TCP

The product executable receives MQTT 5 publications durably. It does not
subscribe clients or deliver publications to subscribers. This experimental
subset is not a conforming complete MQTT 5 broker or a Mosquitto replacement:
QoS 0 and subscriber delivery are still outside this profile.

## Launch and external client

From `mqttiumd/`, using the pinned Rust toolchain:

```bash
cargo build --locked -p mqttiumd
./target/debug/mqttiumd --db /tmp/mqttiumd.sqlite \
  --listen 127.0.0.1:1883 --events /tmp/mqttiumd.events.jsonl
python -m pip install -r tests/system/requirements.txt
python tests/system/external_mqttium.py --port 1883 --client-id example-client --qos 1 --qos 2
python tests/system/external_paho.py --port 1883 --client-id second-client --qos 1
```

The database path may be absent. Its parent directory must exist. An existing
file is opened and checked; a failed open never falls back to creating an
empty database. The event path must be new, so earlier observations cannot
be overwritten. Readiness is emitted only after the exclusive writer opens
one durable startup epoch and the pure owner consumes its complete recovery.
An epoch belongs to the broker lifetime, not to individual connections.

CONNECT requires MQTT 5, an explicit nonempty UTF-8 ClientID, Clean Start 0,
and Session Expiry Interval `0xffffffff`. A new session is durably created
before a successful CONNACK with Session Present 0. Known sessions resume
with Session Present 1. An unknown creation result closes the connection;
it is never blindly retried. A later connection performs a new durable lookup.

The profile accepts non-retained QoS 1/2 PUBLISH, PUBREL, PINGREQ and ordinary
DISCONNECT. It supports Keep Alive: a valid completed packet resets the
1.5-times-Keep-Alive deadline; fragments do not. Keep Alive 0 disables that
MQTT timer, not the absolute partial-packet or write deadlines. PINGREQ goes
through the core and produces PINGRESP.

No Will, authentication, retained messages, subscriptions, topic aliases,
PUBLISH properties or QoS 0 are implemented. Necessary CONNECT properties
are parsed and validated. Client Maximum Packet Size constrains every emitted
response. Receive Maximum, Topic Alias Maximum and Request Problem Information
are checked; with no outgoing PUBLISH and no diagnostic strings their advertised
client restrictions need no additional output. The CONNACK advertises no retain,
subscription, shared-subscription, wildcard-subscription or topic-alias support,
Receive Maximum 8 and Maximum Packet Size 65,536. Maximum QoS is omitted: MQTT
permits that property's value to be only 0 or 1, not 2.

Malformed encodings, invalid protocol/context and well-formed unsupported
features have distinct stable event/error codes. The codec validates complete
properties before treating a feature as unsupported. The runtime closes an
unusable connection, attempting an appropriately bounded MQTT refusal when
its response limit is known. It never reports success for an ignored feature.

## Bounds and lifecycle

The fixed product profile is `contracts/v1/product-profile.json`:
16 connections, 128 sessions and inbound exchanges, packet 65,536 bytes,
ClientID/topic 1,024 bytes, payload 32,768 bytes, input/output queues 4 per
connection, writer queue 32, commands in flight 16, timers 512, receipts 8,192,
core closed-generation window 8. Packet bodies are reserved only after a
canonical bounded Remaining Length. Socket send buffers request 4,096 bytes;
the effective OS value is recorded per connection.

Each connection has separate reader/writer tasks. SQLite runs on its exclusive
worker, never in the owner reactor. The partial-packet deadline is 5 seconds,
CONNECT deadline 3 seconds, and an entire write has an absolute 250 ms budget,
even when partial writes make progress. Cancellation closes both socket halves.
A full queue cannot silently discard a core effect: the Outbox retains the same
identity until accepted or the closed generation receives an explicit failed
frame result. Store results stay correlated after socket closure.

QoS 1 uses the same owner admission and writer as QoS 2. A positive PUBACK
requires a known admission commit. A certain refusal carries Quota Exceeded
(0x97) or Implementation Specific Error (0x83); an unknown outcome emits
neither success nor certain rejection and retains its OperationId for
resolution. QoS 1 creates no durable AwaitPubrel marker, charges no durable
exchange slot and never submits ReleaseInbound. Its volatile admission and
pending PUBACK still hold one bounded owner lease until their own completion
or generation closure. Publications without obligations are collected normally.

Once PUBACK is eligible, another PUBLISH using that Packet Identifier is a
new occurrence, even with DUP set or identical bytes. This permits a client
that received PUBACK to proceed before its internal write receipt arrives.
Each occurrence has fresh operation/publication/exchange identities. A retained
frame carries its core-frozen PUBLISH ingress order; an old receipt cannot end
the new occurrence or return its receive credit. Receive Maximum completion
also checks packet kind: PUBACK for QoS 1, PUBCOMP or negative PUBREC for QoS 2.
Before an admission's outcome is known, its replay keeps the same operation;
an identifier still active in that phase cannot change QoS.

After a lost PUBACK or broker restart, MQTT QoS 1 retransmission may admit a
second application occurrence. MQTT-4.3.2-5 requires a new publication after
PUBACK is sent, regardless of DUP. No permanent deduplication by Packet
Identifier is promised. Repeating the same internal OperationId remains
idempotent. A local socket write receipt does not establish peer receipt.

Connection generations never repeat. The runtime validates ingress against
its current live map, including immediately before the core consumes it; this
continues after the core forgets an old closed generation. Late durable results
still reconcile knowledge and cannot authorise output on a retired socket.
A packet refused for volatile capacity closes only its transport, without a
positive business acknowledgement or a claim of certain durable rejection.

Publication metadata is collected when no inbound marker or obligation refers
to it. A zero-target admission needs no archived payload. Completed admission
receipts preserve durable identity/replay evidence. Receipt collection and
long-running epoch/session growth remain finite operational limits; this is
not a promise of indefinite service or a throughput target.

SIGINT/SIGTERM closes sockets, releases acknowledged test holds, and drains
accepted writer work before releasing the writer. Accepted commands are not
called certainly rejected. Shutdown has a finite deadline; a stuck storage
operation is reported as an incomplete shutdown with unknown outcome. SIGKILL
has no graceful drain and relies on SQLite recovery plus MQTT retransmission.

## System reception

```bash
tools/run_tcp_reception.sh /tmp/mqttiumd-tcp-reception
tools/run_qos1_reception.sh /tmp/mqttiumd-qos1-reception
```

The receipt is a system test, not a qualifying result of the fixture-based
lab oracle. mqttium 1.3.0 is the primary client, using only its public API;
Paho 2.1.0 remains a second implementation. The receipts retain imported client
source/version/hash, actual executable hashes, commands,
client bytes, product events, database facts and process receipts of its runs.
Test hooks require `--features mqttiumd/test-hooks` and are absent by default;
Cargo's effective SQLite feature is recorded independently. Before-commit and
committed-before-PUBREC holds are acknowledged by synced events; the supervisor
releases a hold or actually kills that same process. Mutant controls run in
isolated worktrees and are rejected for premature ACK/stale-generation reasons.
The QoS 1 receipt additionally holds one internal PUBACK write receipt without
blocking the reactor and detects premature PUBACK/stale-receipt mutations.
Its independent proxy observes a real PUBACK and withholds it from the client;
bounded automatic reconnect preserves the mqttium process and original receipt.
Normal binaries execute nominal, identifier-reuse and mixed-QoS paths too.
Quota interventions are isolated system controls, recorded after readiness,
and restored; their outcomes are not ordinary production configuration changes.
Existing K1/K2b/K3 campaigns retain their separate qualification scope.
