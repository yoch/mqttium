# 0025 — Exact persistent subscriptions and outgoing QoS 1

Status: **Accepted with amendments** (21 November 2026, decided under the
repository owner's delegation on the advice of the independent audit of
`8e4303d`, with the change that implements its amendment). A bounded product
increment, not full P1/P2 reception, complete MQTT conformance or acceptance
of historical proposed decisions.

Amendment of acceptance (21 November 2026).
A QoS 1 or 2 delivery whose PUBLISH would exceed the Maximum Packet Size of
the receiving client is not sent and is discarded as a completed delivery
(MQTT-3.1.2-25): its obligation ends, the connection stays open and the
deliveries that follow it are sent. This replaces the rule of the proposed
text, "the obligation is retained and the connection safely refused, not
discarded". The discard reuses the recipe `FinishDelivery`, guarded by the
claim of the delivery; no recipe, stored fact or schema is added. Its commit
publishes the new volatile effect `DeliveryDiscarded { delivery, target,
operation, reason }` with the reason `PacketTooLarge`. No existing effect or
fact fits: `Qos0Dropped` carries a `Qos0Origin` and names QoS 0, the fact
`DeliveryFinished` is the same for an acknowledged delivery, and a new commit
fact would be a durable encoding. The section "A delivery larger than the
peer's Maximum Packet Size" states the rule. Record 0048 states the
capacities of the runtime and their derivation; the sixteen-connection
numbers below describe the bounded qualification profile.

Amended by 0026 and 0041 (record 0047): QoS 0 subscriptions are granted and
a QoS 2 request is granted QoS 2; the grant rule below no longer holds.

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

## A delivery larger than the peer's Maximum Packet Size

A claimed QoS 1 or 2 delivery whose PUBLISH would exceed the Maximum Packet
Size of the receiving connection is never sent (MQTT-3.1.2-24) and ends as a
completed delivery (MQTT-3.1.2-25). The connection is not closed, the session
stays usable and the deliveries that follow are claimed and sent. The message
is lost for that subscriber only: every other destination of the publication
is decided against its own connection. A QoS 0 copy is dropped in the same
case (record 0026).

The size is that of the one PUBLISH the profile encodes: fixed header,
minimal Remaining Length, Topic Name, Packet Identifier, a one-byte Property
Length, the Message Expiry Interval when the message has one, payload. The
profile sends no other property, and MQTT-3.3.2-6 forbids leaving the Message
Expiry Interval out, so there is no smaller encoding and nothing is omitted
to make a packet fit. An MQTT 3.1.1 client, and an MQTT 5 client that states
no Maximum Packet Size, have no limit: nothing that the topic and payload
limits admit is discarded for them.

The decision is taken where the PUBLISH would have been emitted, behind every
gate that holds a delivery (message expiry, connection, claim, PUBREL, Receive
Maximum, an earlier delivery still waiting, loaded bytes). It is therefore
taken in delivery order, against the connection of that moment. A claim commit
that finds its delivery too large emits nothing, and the work turn that
follows submits the discard.

The discard is `FinishDelivery` (kind 5), unchanged. Both stores guard a
finish by DeliveryId, target/revision, outgoing identifier and claim
operation, and by nothing a transmission would have established, so a claimed
delivery that never started is ended by the transaction that ends an
acknowledged one: claim and obligation removed, reference decremented, last
payload released. `ConsumeDelivery` cannot be used, since it only ends an
unclaimed QoS 0 obligation. The owner adds the state DiscardPending beside
FinishPending. Until the finish is known the delivery keeps its Packet
Identifier and its place in the window, as an acknowledged delivery does.

The known commit publishes `DeliveryDiscarded`, whose `operation` is the
committed finish; the runtime writes it as the event `delivery_discarded`
(`operation`, `command_kind` "Finish", details `delivery`, `target` and
`reason` "packet_too_large"), a full record at the audit level and a counter
at the ops level. It is an observation and not a durable fact: a crash between
the commit and its acceptance by an adapter loses the event and keeps the
receipt of the finish.

Until the finish commits, the claim is the durable fact. A restart recovers
it: under the same limit the delivery is discarded again, by a new operation
guarded by the same claim, and under a larger limit it is sent with DUP 1
like every recovered claim. A known claim that was never made eligible, on a
new connection with a larger limit, is sent with DUP 0. After the commit
there is neither obligation nor claim, and a repeated finish returns its
receipt without a second observation. An unknown outcome is resolved under
the same OperationId and the delivery is not sent meanwhile. A fenced discard
closes nothing and is decided again at the next work turn; any other certain
refusal keeps the rule of a refused finish, which closes the connection and
keeps the claim.

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
Peer Maximum Packet Size applies before emitting a delivery; one that exceeds
it is discarded as a completed delivery and the connection stays open (see
"A delivery larger than the peer's Maximum Packet Size").

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
