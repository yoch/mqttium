# 0025 — Exact persistent subscriptions and outgoing QoS 1

Status: **Proposed**. A bounded product increment, not full P1/P2 reception,
complete MQTT conformance or acceptance of historical proposed decisions.

Amended by 0028 (record 0047): an outgoing Packet Identifier is reused after
its durable Finish; the rule "never reused within a live connection" below
no longer holds.

Note (21 November 2026): the budgets this record states for sixteen connections are
the bounded profile of `runtime::Limits`. The connection limit is an option
since record 0046 (proposed), and the queues sized for sixteen connections
grow with it.

## Profile and ordering

The existing single logical owner and FIFO SQLite writer add exact SUBSCRIBE
and UNSUBSCRIBE with at most eight filters. Accepted QoS 1/2 subscription
requests grant QoS 1; QoS 0 and unsupported options receive explicit refusal.
Results retain request order. A bounded batch atomically upserts/removes the
accepted filters for one durable SessionId/revision and expected route revision.
It does not change the session lifecycle revision. Re-subscription has no
remove/add gap. Unsubscribe retains obligations already admitted.

ChangeSubscriptions uses the ordinary canonical Recipe, OperationId, receipt
and resolve/fence path. The owner holds dependent publication admissions while
the route mutation is unresolved, then installs its projection/revision before
SUBACK/UNSUBACK and further admissions. Prior FIFO submissions remain ordered
before the cut. Unknown results never authorize a positive acknowledgement.
The runtime retains deferred business packets in their connection order within
its existing 128-input budget and wakes them after store-result application.
PUBACK, PUBREL, PING and closure can progress while a route mutation is pending.

Canonical format 1 gains kinds 3/4/5, preserving kinds 1/2 byte for byte.
Kind 3 holds operation32, session8, session_revision8, route_revision8, count1,
then subscribe1/topic_length2/topic per change. Kinds 4/5 hold operation32,
publication32/delivery_index4, target8, target_revision8, outgoing_packet_id2;
kind 5 additionally names claim_operation32. Integers are big-endian.

## Claim, emission and finish

The pure owner adds Queued → ClaimPending → AwaitPuback → FinishPending →
Terminal. ClaimDelivery conditionally reserves a durable outgoing identifier
and one exchange slot for the existing obligation. The claim OperationId is
also its immutable slot generation. Only its known commit permits EmitPublish.
The claim row means AwaitPuback with an intent that may have started; it is
not proof of socket or peer receipt. Fresh known claims emit DUP 0; recovered
claims retain their identifier/generation and conservatively replay DUP 1.
There is no periodic retransmission on a live MQTT 5 connection.
An unresolved earlier claim prevents a later delivery to the same destination
from becoming eligible first. Other destinations remain independent.

PUBACK, including a valid negative reason, submits FinishDelivery guarded by
DeliveryId, target/revision, outgoing identifier and original claim identity.
Only known finish commit frees the slot/window. It atomically removes claim
and obligation, decrements the reference and releases the last payload through
the existing termination logic. Local write completion never finishes it.
Unknown claim/finish results retain their OperationId and resolve/fence.
After confirmed process death and opening a new epoch, coherent durable facts
and the closed old execution space distinguish remaining from finished work.

Outgoing identifiers are distinct from incoming identifiers and are never
reused within a live connection; exhaustion closes it. Reconnection fences old
bytes, restores outstanding identifiers and skips them during new allocation.
This bounded policy protects wire PUBACK aliasing, which internal DeliveryId
alone cannot solve. Incoming PUBLISH/SUBSCRIBE/UNSUBSCRIBE share their sender's
identifier space and active collisions are explicit errors.

## Bounded work and storage

Startup recovery includes obligation/claim metadata, not payload copies.
Correlated online work pages read at most eight items with typed errors and
a DeliveryId cursor. Generation/stamp checks reject stale pages after finish
or takeover. Bounded periodic discovery finds obligations even if memory
notifications are lost; a known AwaitPuback is not retransmitted by a scan.
DeliveryId ordering is durable source order for this product's one owner 0,
FIFO submissions and monotonically increasing epochs. General multiowner
ordering remains a separate design problem.

Initial product bounds: 128 sessions, 128 exact subscriptions globally,
16 targets per publication/topic, 128 references, 128 inbound and separately
bounded outgoing metadata entries, eight outgoing slots per connection bounded
further by the peer Receive Maximum, 32 KiB payload, 1 KiB topic and eight-item
work pages. Existing receipt/epoch collection limits still bound service life.
Peer Maximum Packet Size applies before emitting an oversized delivery;
the obligation is retained and the connection safely refused, not discarded.

SQLite schema 2 adds durable outgoing claims and bounded receipt fact bytes.
Version 1 is refused with a typed error before write-affecting pragmas; there
is no implicit migration, checkpoint or empty-store replacement. New fact
decisions 4/5/6 encode subscription16 or delivery78 bytes in a bounded BLOB,
while old decision layouts stay readable in schema 2. Both stores implement
the same commands, complete snapshots, recovery and work budgets.

The product uses pure EmitPublish plus fixed-size Copy SUBACK/UNSUBACK results.
ConnectionSettings supplies peer flow/packet bounds before binding. A blocked
outgoing window leaves PING, PUBACK, close and writer reconciliation runnable.
The closed fixture-based oracle profile remains QoS 2: queued recovery metadata
is corroborated, but new claims/commands/emitted pubsub traffic are not silently
qualified. New A–H receipts are explicitly finite system tests with mqttium and
Paho public APIs, independent wire observations, real SQL barriers and SIGKILL.
