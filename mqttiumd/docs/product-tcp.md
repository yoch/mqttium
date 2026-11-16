# Experimental durable exact-topic pub/sub over TCP

The product receives non-retained MQTT 5 QoS 1/2 publications and delivers them
to persistent exact-topic subscribers at QoS 1. Sessions, subscriptions,
recipient obligations and outgoing claims are durable. This is a bounded
experimental subset, not complete MQTT 5 conformance, a Mosquitto replacement
or reception of all P1/P2 behavior. QoS 0 and outgoing QoS 2 remain outside it.

## Launch and external client

From `mqttiumd/`, using the pinned Rust toolchain:

```bash
cargo build --locked -p mqttiumd
./target/debug/mqttiumd --db /tmp/mqttiumd.sqlite \
  --listen 127.0.0.1:1883 --events /tmp/mqttiumd.events.jsonl
python -m pip install -r tests/system/requirements.txt
```

Run the broker in one terminal, then the following commands in another. They
create two subscriptions, disconnect the subscribers, publish while they are
offline, and resume their sessions **without sending SUBSCRIBE again**:

```bash
python tests/system/external_pubsub.py --client mqttium --action subscribe \
  --port 1883 --client-id demo-sub-mqttium --filter demo/exact --qos 1
python tests/system/external_pubsub.py --client paho --action subscribe \
  --port 1883 --client-id demo-sub-paho --filter demo/exact --qos 2
python tests/system/external_pubsub.py --client mqttium --action publish \
  --port 1883 --client-id demo-pub-mqttium --topic demo/exact --payload first --qos 1
python tests/system/external_pubsub.py --client paho --action publish \
  --port 1883 --client-id demo-pub-paho --topic demo/exact --payload second --qos 2
python tests/system/external_pubsub.py --client mqttium --action receive \
  --port 1883 --client-id demo-sub-mqttium --resume-without-subscribe \
  --expected-session-present 1 --count 2
python tests/system/external_pubsub.py --client paho --action receive \
  --port 1883 --client-id demo-sub-paho --resume-without-subscribe \
  --expected-session-present 1 --count 2
```

Both subscription requests grant QoS 1. To receive online instead, use
`--action receive --filter demo/exact --qos 1 --count 2 --events NEW.jsonl`
for each subscriber and wait for its `subscription_ready` event before
publishing. The helpers use the clients' public APIs, wait for SUBACK and
publication receipts, and submit manual acknowledgements for received messages.
`--hold-acks --release-acks PATH` retains the actual manual acknowledgement
until that file exists, within `--wait-timeout`; without a release path it
disconnects after receiving the requested count without acknowledging them.
The receipt records this distinction. A sleep after auto-ACK delivery is not
used as evidence of a pending obligation.

Remove one durable subscription with:

```bash
python tests/system/external_pubsub.py --client mqttium --action unsubscribe \
  --port 1883 --client-id demo-sub-mqttium --filter demo/exact \
  --expected-session-present 1
```

The examples assume fresh database/event paths and distinct demonstration
ClientIDs. The database path may be absent. Its parent directory must exist. An existing
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

The profile accepts non-retained QoS 1/2 PUBLISH, PUBREL, exact SUBSCRIBE and
UNSUBSCRIBE, recipient PUBACK, PINGREQ and ordinary DISCONNECT. It supports
Keep Alive: a valid completed packet resets the
1.5-times-Keep-Alive deadline; fragments do not. Keep Alive 0 disables that
MQTT timer, not the absolute partial-packet or write deadlines. PINGREQ goes
through the core and produces PINGRESP.

No Will, authentication, retained messages, wildcard/shared subscriptions,
subscription identifiers, topic aliases, PUBLISH properties, incoming QoS 0
or outgoing QoS 2 are implemented. No Local, Retain As Published and
subscription properties are explicitly outside the accepted profile. Since
the profile stores no retained publication, its valid Retain Handling choices
produce no retained replay. Malformed options are distinguished from valid
unsupported options. Necessary CONNECT properties are parsed and validated.
Client Maximum Packet Size constrains every emitted packet, including outgoing
PUBLISH. Client Receive Maximum constrains the outgoing active window in
addition to the local limit. Topic Alias Maximum and Request Problem
Information are checked; no alias or diagnostic string is emitted. The
CONNACK advertises no retain, subscription-identifier, shared-subscription,
wildcard-subscription or topic-alias support,
Receive Maximum 8 and Maximum Packet Size 65,536. Maximum QoS is omitted: MQTT
permits that property's value to be only 0 or 1, not 2.

Malformed encodings, invalid protocol/context and well-formed unsupported
features have distinct stable event/error codes. The codec validates complete
properties before treating a feature as unsupported. The runtime closes an
unusable connection, attempting an appropriately bounded MQTT refusal when
its response limit is known. It never reports success for an ignored feature.

## Bounds and lifecycle

The fixed product profile is `contracts/v1/product-profile.json`,
`tcp-exact-pubsub-v1`:
16 connections, 128 sessions, 128 exact subscriptions globally, eight filters
per request and 16 recipients per publication/topic. Packet limit is 65,536
bytes, ClientID/topic/filter 1,024 bytes and payload 32,768 bytes. Input/output
queues hold four items per connection; the writer queue holds 32, commands
in flight 16, timers 512 and receipts 8,192. The core remembers eight closed
generations. There are 128 volatile incoming entries and a separate 128-entry
outgoing metadata table with a 4 MiB outgoing loaded-payload budget. The durable
account has 4 MiB of payload, 128 references and 128 exchange slots shared
by incoming QoS 2 and claimed outgoing deliveries. Each connection has at most
eight active outgoing slots, bounded further by the peer Receive Maximum.
Packet bodies are reserved only after a
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

Receive Maximum follows bounded transport progress rather than waiting for
the reactor's ordinary Written callback. Completed terminal-ACK facts are
reconciled before charging the next input, with effect/connection/occurrence/QoS
guards. If the socket writer is preempted between sending bytes and publishing
progress, one decoded PUBLISH may wait at most 500 ms for a matching accepted
terminal ACK's progress. Submission alone returns no credit. A genuine ninth
unfinished exchange is still refused; the advertised maximum remains eight.

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

## Persistent filters and the admission cut

SUBSCRIBE accepts exact filters requesting QoS 1 or 2 and grants QoS 1. A
QoS 0 request is refused with a per-filter error. Shared and wildcard filters
are refused explicitly; SUBACK/UNSUBACK reason codes keep request order.
Subscriptions belong to the durable SessionId and its revision, not a socket
or ClientID lookup alone. Re-subscription replaces the same session/filter
atomically without a remove/add gap or duplicate route. An unknown subscription
result retains its OperationId for resolution and permits no positive SUBACK.

The FIFO writer and the pure owner's routing revision define the ordering cut.
Previously submitted admissions precede the mutation. A dependent new admission
cannot cross an unresolved mutation using a stale projection. After its known
commit the owner installs the projection before SUBACK/UNSUBACK and subsequent
admissions. The publisher's positive PUBACK/PUBREC depends on the atomic
publication/payload/recipient-obligation admission, not subscriber PUBACKs.
UNSUBSCRIBE prevents future matching admissions while retaining obligations
already admitted. An absent recipient therefore resumes existing work even
if its filter was later removed.

## Outgoing QoS 1 and restart

The existing obligation progresses through Queued, ClaimPending, AwaitPuback,
FinishPending and Terminal. A correlated canonical ClaimDelivery operation
durably associates its DeliveryId, target session/revision, outgoing Packet
Identifier and claim identity before making PUBLISH eligible. Its claim
OperationId is the durable slot generation. Unknown claim outcomes keep that
operation and reconcile; they do not create a new delivery or authorize output.

Fresh known claims emit DUP 0. A recovered claim retains its identifier and
identity and conservatively emits DUP 1, because its earlier transmission may
have started. Claim intent, local socket write and peer acknowledgement are
distinct facts. There is no periodic retransmission on a live MQTT 5 connection.
Incoming and outgoing identifier spaces are independent. The client's active
PUBLISH/SUBSCRIBE/UNSUBSCRIBE requests share its own space and collisions are
errors. Server identifiers are allocated monotonically without reuse within a
live connection; exhaustion closes that connection instead of wrapping. A new
connection can resume outstanding claims and skips their identifiers when
allocating new ones. Connection generations also fence stale callbacks/bytes.

A valid recipient PUBACK, including a negative reason, starts conditional,
idempotent FinishDelivery. Only its known commit frees the outgoing slot/window
and releases the obligation/reference. A negative PUBACK is terminal refusal,
not an endless retry trigger. Finishing the final reference releases payload
bytes; incoming QoS 2 metadata additionally waits for its own inbound release.
Unknown finish outcomes retain their operation and cannot free resources early.
Peer Maximum Packet Size is enforced before output. An oversized delivery closes
the unusable connection while preserving its durable work for a suitable peer.
A full outgoing window still leaves PUBACK, PING, closure and writer results
runnable; a slow recipient does not own another recipient's window.

Recovery restores subscription, obligation and claim metadata before readiness.
Typed bounded work pages load at most eight items and preserve DeliveryId order
for the current one-owner FIFO product. Notifications are an optimization:
bounded idle scans are scheduled every 100 ms, including when no new traffic
arrives. This is a discovery schedule, not a loaded-writer latency guarantee.
Correlations/stamps invalidate stale pages after finish or takeover. A completed
obligation is not recreated from an old page; a remaining obligation retains
its own identity after restart. Payload loading obeys its separate byte budget.

SQLite schema **2** stores claims and new bounded receipt facts. Schema 1 and
other incompatible versions are refused with typed InvalidData before
write-affecting pragmas; there is no implicit migration, checkpoint, reset or
empty replacement. Use a new file for this product increment. Existing
schema-1 files from earlier reception remain historical inputs until a separate
migration is provided. Invalid/over-budget current files also refuse before
readiness without resetting partial recovery.

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
tools/run_pubsub_reception.sh /tmp/mqttiumd-pubsub-reception
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

The pub/sub command builds normal and instrumented products, runs finite A–H
system cases, and checks three isolated negative controls: emission before
known claim, premature finish/resource release and discovery that depends only
on notifications. It records each run's own source/client identity, binary,
features, bytes and message observations, reached SQL barriers, durable facts,
process statuses and available system witness. Public manual ACK behavior is
checked on the wire; critical crash/collision schedules can use the independent
TCP pilot. The generated receipt's source SHA and case verdicts determine what
was executed. These tests do not qualify all P1/P2 behavior or extend the closed
fixture oracle's semantics to pub/sub. Proposed ADRs 0024/0025 retain that status.

General receipt/epoch collection, indefinite operation, QoS 0, outgoing QoS 2,
wildcards, retained/shared delivery, Will/authentication, finite expiry,
deployment security and performance remain separate work. Process SIGKILL
receipts do not establish power-loss or universal storage guarantees.
