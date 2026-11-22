# 0023 — Incoming QoS 1 occurrences and acknowledgement receipts

Status: **Accepted** (21 November 2026, by the repository owner). It does not
qualify the product as a complete MQTT broker.

## One admission, one occurrence

An incoming non-retained QoS 1 PUBLISH uses the existing owner admission
transition, identities, frozen recipe, store and resolution/fencing path.
An occurrence has a fresh ExchangeId, PublicationId and OperationId, and
records its connection generation and PUBLISH ingress order. An unresolved
admission owns its identifier. Its retransmission does not issue a second
internal command. Unknown outcomes retain their identity and lease until
resolved; absence, timeout and transport closure are never certain rejection.

QoS 1 charges zero **durable** exchange slots and creates no AwaitPubrel
marker. It never submits ReleaseInbound. The existing volatile lease still
charges one bounded owner slot and its payload budget while the admission
or its acknowledgement is pending. Target QoS is bounded by incoming QoS.
Publications without obligations are collected in the admission transaction;
committed receipts continue to guard identity reuse and internal idempotence.

## Commit, emission and reuse

Only a confirmed admission commit makes a positive PUBACK eligible. A
certain refusal uses the packet-specific PubackReason (Success 0x00,
QuotaExceeded 0x97, ImplementationSpecificError 0x83); PUBCOMP's 0x92 cannot
be represented. An unknown result authorizes neither positive nor negative
PUBACK. After eligibility, the occurrence waits for its own frame result.
Its bounded record owns its lease until a complete write, failure or closure
of that generation. Frame effect identity and ConnectionKey select that
record; a Packet Identifier alone never does.

An ACK-pending occurrence no longer excludes a new PUBLISH with the same
identifier. This permits the client to progress after receiving PUBACK even
when the internal write receipt is delayed. The new publication gets fresh
identities and its own lease, including when DUP is set or bytes are equal
(MQTT 5 section 4.3.2, MQTT-4.3.2-5). A duplicate before the ACK becomes
eligible still belongs to the unresolved admission. An active identifier
cannot change QoS. This is not permanent MQTT-level QoS 1 deduplication:
retransmission after an acknowledgement is lost or after broker restart
can admit another occurrence. Repeating an internal OperationId remains
idempotent.

EmitFrame carries an optional `publish_order`, frozen in core planning,
alongside its connection generation. Retransmissions of an active PUBLISH
refresh its correlation through plan/apply. A retained effect must not look
up the current identifier's occurrence when a socket queue later accepts
it. Recovery has no invented ingress order. A result after close is still
reconciled; emission uses only the live current generation, with no receive
credit correlation if its PUBLISH came from a previous generation.

The runtime Receive Maximum window validates both occurrence and terminal
packet kind: PUBACK completes QoS 1, PUBCOMP or negative PUBREC completes
QoS 2. A stale or repeated receipt cannot free a later occurrence, including
reuse across QoS levels. Old socket generations never regain emission
authority. The existing owner, Outbox and transport bounds remain unchanged.

## Reception scope

The finite product reception uses mqttium 1.3.0's public API as its primary
client and Paho 2.1.0 as a second implementation. It waits for publication
receipts and records the client actually imported. Raw TCP controls exact
identifiers, DUP and scheduling. An acknowledged admission barrier and an
independent SQLite read establish commit-before-PUBACK; an isolated early
ACK mutant must fail for `premature_puback`. A delayed internal write receipt
and stale-receipt mutant distinguish occurrences. A proxy that observes and
withholds a real PUBACK distinguishes local write progress from client
receipt. Bounded automatic reconnect keeps the same mqttium receipt alive.

These are system tests, separate from the existing fixture-based qualifying
QoS 2 campaigns. QoS 0, subscriptions, outgoing delivery, finite expiry,
Will/authentication, general receipt collection, performance reception and
universal storage/power-loss guarantees remain outside this increment.
