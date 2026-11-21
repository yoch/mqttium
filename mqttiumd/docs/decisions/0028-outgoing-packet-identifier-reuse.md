# 0028 — Recyclable outgoing Packet Identifiers and immutable PUBACK observations

Status: **Accepted** (21 November 2026, by the repository owner). A local extension
of 0027 (accepted) and of 0025 (accepted with amendments), whose identifier
rule it replaces; not a claim of complete MQTT conformance.

## Resource numbers and durable authority

An outgoing QoS 1 Packet Identifier is a reusable nonzero number in 1..65535,
scoped to the durable target session's outgoing direction. It is not a causal
identity. OperationId, claim operation, DeliveryId, EffectId, connection
generation and receipt frontiers retain their existing checked, non-wrapping
rules. The former cumulative connection cursor is not an identity allocator.

The owner chooses the smallest free number among all target-session claims.
At most the number of live claims plus one candidates need examination; each
is checked against the bounded metadata table, including recovered claims
whose topic/payload has not been loaded. No historical per-message set is
kept. Test-only helper cases may use a smaller number domain without changing
the product's domain or quotas.

ClaimPending, unknown Claim, AwaitPuback, FinishPending and unknown Finish all
reserve the number. Only application of the correlated, known committed
Finish result removes that delivery and makes its number free. A valid
negative PUBACK has the existing terminal Finish behavior. Neither local
write completion nor physical receipt deletion frees a claim. Receipt
retirement and business responsibility remain independent. A crash restores
the same claim/PI if still durable; a committed Finish does not resurrect it.
QoS 0 never enters this number/window/claim accounting.

Existing ClaimDelivery/FinishDelivery recipes, durable guards and SQLite
uniqueness on (target, packet_id) are sufficient. No canonical recipe bytes,
SQLite schema version or recovery fact format changes for this increment.

## Authoritative observation boundary

Product TCP freezes a validated PUBACK association when the runtime dequeues
the complete packet from the reader channel and decodes it, **before putting
an input into the runtime's owner-input queue**. The reader channel still
carries raw framed bytes. Bytes in that channel or in the socket are upstream
of this boundary and are not promised to have an immutable association yet.

Before a PUBLISH can leave the adapter's socket-writer queue, the adapter
registers its accepted emission's bounded target: DeliveryId, claim OperationId
and frame EffectId, under the exact ConnectionKey/PI. A refused queue transfer
does not leave a registration. The runtime corroborates it against the owner
and prunes stale entries as the owner applies results or closes a connection.
This registration does not wait for a local Written callback: the peer may
already have received the frame and sent PUBACK while that callback is delayed.

`Input::ObservedPuback` contains the connection, PI, validated reason and an
immutable `Option<PubackTarget>`. A missing target stays missing. A target is
accepted only if delivery, claim, frame, connection and PI still name that
same eligible owner occurrence. A delayed/duplicated fact cannot be rebound
through a lookup of the PI's newer occupant. Ordinary synchronous adapters
that present a raw `Input::Packet(Puback)` observe at that core transition;
they cannot use that raw entry point to replay an already associated fact.
The asynchronous product TCP path always uses ObservedPuback.

For example, after observing A as target (delivery_A, claim_A, frame_A), the
adapter may retain a bounded injected copy while the original finishes A.
B can then reuse the number with (delivery_B, claim_B, frame_B). Releasing
the old copy cannot produce Finish(B), return B's credit or alter its frame.
An observation captured without a target likewise remains a no-op after B.
Generation and effect/operation guards separately protect takeover, delayed
writer callbacks, store answers and deadlines.

MQTT PUBACK transports only its Packet Identifier and reason, not these
internal identities. Two identical arbitrary ACK packets first observed
after reuse cannot be distinguished by invented claim metadata. This record
protects facts already associated at the stated boundary, not all possible
upstream bytes or a dishonest/malformed peer's indistinguishable future ACK.

## Normative scope and reception

MQTT 5.0 section 2.2.1 permits reuse after processing the corresponding ACK
and gives client/server independent identifier assignments. Section 4.3.2
requires retaining an unacknowledged QoS 1 message until PUBACK; a new
publication starts with DUP 0. Section 4.4 requires reconnect replay with its
original PI, forbids in-session retransmission, and treats valid negative
PUBACK as acknowledged. Section 4.6 requires QoS 1 PUBACKs in PUBLISH receive
order. The product's known durable Finish boundary is conservative with
respect to protocol reuse. It is not a QoS 1 general deduplication promise.

Source: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
sections 2.2.1, 4.3.2, 4.4 and 4.6.

Finite reception requires a red historical-exhaustion test, at least 65537
distinct normal-build outgoing QoS 1 deliveries received and durably finished
on one subscriber connection/process/file, public mqttium and Paho clients,
independent wire PI reuse, short reversed roles, active receipt collection,
reduced-domain occupancy/uncertainty cases, real internal-queue observations,
separate same-file SIGKILL recoveries and targeted mutants. Unknown operations
are not artificially held across the long run; ACK ordering is respected.
New cases are system tests, not new fixture-oracle qualifications. Focused
small models state their assumptions separately from product evidence.

Persistent infinite-expiry session capacity remains 128. General session
collection, migration, wildcard/retained/Will/authentication and outgoing
QoS 2 remain separate increments. This does not establish indefinite service,
general performance, power-loss tolerance or universal MQTT compliance.
