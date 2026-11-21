# ADR 0038 — MQTT 3.1.1 wire profile

Status: **Accepted** (21 November 2026, by the repository owner).

Amended by 0040 (record 0047): incoming QoS 2 is part of the MQTT 3.1.1
profile.

The product accepts protocol levels 4 and 5 on the same listener. The TCP
adapter records the protocol level in `connect_validated` and uses it
until that exact connection generation closes. Authentication, ownership,
routing, quotas, SQLite commands and the positive-ACK commit fence are shared.

MQTT 3.1.1 Clean Session maps to a fresh session with OnDisconnect expiry
or an indefinite session that can resume. The broker uses explicit ClientIDs.
CONNACK return codes follow MQTT 3.1.1 section 3.2.2.3. A session refusal
does not set Session Present. SUBACK can encode a refusal as 0x80.
UNSUBACK has only a packet identifier and is eligible for successful or
already-absent filters. A publication refusal with no encodable code closes
the connection before PUBACK, as required by chapter 1. Server DISCONNECT
frames belong to the MQTT 5 adapter.

The existing bounded work item and emission effects carry the delivery cause
already stored with each obligation. The wire adapter uses this cause to set
RETAIN only on subscription replay in MQTT 3.1.1, including when a session
previously negotiated MQTT 5 and requested Retain As Published. The work
reader preserves the stored cause and the core checks it against loaded work.
The storage schema and canonical recipes use their existing encodings.

A timed publication from a level-5 publisher still has a write deadline when
its subscriber negotiated level 4. The network task checks the deadline
before beginning the frame even when there is no expiry property to rewrite.
Protocol-level exchange state stays in memory at the transport boundary.

The first level-4 profile includes QoS 0/1 PUBLISH, PUBACK, SUBSCRIBE,
UNSUBSCRIBE, PING, normal DISCONNECT, retained and the existing QoS 1 Will
profile. A requested subscription QoS 2 can be reduced to QoS 1. QoS 2
PUBLISH is outside the level-4 profile. The level-5 incoming QoS 2 path
continues to use its existing durable handshake. The next level-4 profile
extends it in [Proposed 0040](0040-mqtt311-incoming-qos2.md).

The extracted preview reception compares the same Mosquitto 2.0.21 clients
against a pinned Mosquitto broker and the candidate over mTLS. The candidate
receipt separately observes both directions of level-4/level-5 routing,
persistent restart, clean-session removal, authorization and the commit and
capacity boundaries. The applicable normative reference is [MQTT 3.1.1 with
Errata 01](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html).
