# ADR 0049 — Compatibility with the reference broker is functional

Status: **Accepted** (22 November 2026, by the repository owner). It adds
one variant to a public type of the contract tier (`PubackReason`). It
changes no schema and no durable encoding; the contract digest that
`validate` prints moves because the register lists the accepted records.

## Context

The product is meant to replace Mosquitto 2.0.21 for the clients that use
it today. The compatibility bench (`tests/compat/`) plays every scenario
against both brokers and has a person classify each difference. After the
counter-audit of `bbe1453` the differences that are not a missing feature
were of three kinds: two where Mosquitto departs from the specification,
seven where the specification allows both behaviours, and one extension
Mosquitto offers outside the specification. Nothing said which way such a
difference is to be decided, and SPEC-002 only says that Mosquitto is not
the oracle.

## Decision

**Compatibility is functional.** A client application that works with
Mosquitto and stays within MQTT must work with mqttiumd. It is not a goal
to reproduce the bytes of Mosquitto, and never to reproduce a behaviour
of Mosquitto that the specification forbids.

**Where Mosquitto departs from the specification, mqttiumd follows the
specification.** The difference is classified `reference-nonconformant`
with the statement it rests on. Two cases stand today:

- A client that exceeds the Receive Maximum the broker announced is
  disconnected with 0x93 (MQTT-3.3.4-7, MQTT-4.13.1-1). Mosquitto answers
  PUBREC 0x97 and keeps the connection.
- A packet larger than the Maximum Packet Size the broker announced is
  refused with 0x95 (section 3.2.2.3.6, MQTT-4.13.1-1). Mosquitto serves
  it.

**Where the specification allows both behaviours, the choice is made for
the application, not for the transcript.** Each choice is recorded here
and the bench entry names this record.

1. *A QoS 1 publication that matches no subscription is acknowledged
   0x10 in MQTT 5.* The PUBACK of a publication the broker accepted and
   gave to no subscriber carries "No matching subscribers", as Mosquitto's
   does and as section 3.4.2.1 permits. A retained publication without a
   live subscriber is answered 0x10 too; it is stored all the same.
   MQTT 3.1.1 has no reason code: the acknowledgement is the plain one.
   0x10 is a success: every rule that holds for a positive
   acknowledgement holds for it, first of all that it follows the commit.
   *The PUBREC of such a publication stays 0x00.* Section 3.5.2.1 permits
   0x10 there as well, and Mosquitto does not send it: its PUBREC is the
   plain success whatever the subscriptions. Clients written and tested
   against Mosquitto have never received a PUBREC with a reason other
   than a success of one byte or a refusal; sending them one the
   specification does not require would test code that was never run,
   for information the application rarely sees. The first form of this
   choice answered 0x10 on both acknowledgements; the bench showed the
   reference's PUBREC, and the choice was narrowed before it was
   published.
2. *A message that matches several subscriptions of one client is sent
   once, at the highest granted QoS.* MQTT-3.3.4-2 requires that copy and
   allows more; Mosquitto sends one for each subscription. One copy is
   kept while Subscription Identifiers are not offered, since without
   them a client cannot tell the copies apart. The lot that brings
   Subscription Identifiers decides again.
3. *A refusal names its cause as precisely as the specification lets
   it.* PUBCOMP 0x92 for a PUBREL whose identifier is unknown, DISCONNECT
   0x90 for a Topic Name with a wildcard, 0x82 for an invalid Topic
   Filter and a DISCONNECT 0x81 before the close on a malformed packet
   are kept where Mosquitto answers 0x00, 0x81 or closes without a
   packet. In each case both brokers end the exchange or the connection
   the same way; only the code differs.

**An extension of Mosquitto is decided with the feature it extends.**
Mosquitto offers shared subscriptions to MQTT 3.1.1 clients; in 3.1.1 a
filter that begins with `$share/` is an ordinary filter. mqttiumd answers
it 0x80 until the lot of shared subscriptions (CMP-002), which decides
whether 3.1.1 clients are offered them.

**A new difference of these kinds is brought to the owner.** The bench
fails on an unclassified difference; the person who classifies one as
`unspecified` or `reference-nonconformant` asks for the choice and adds
it to a record.

## Consequences

- `PubackReason` gains `NoMatchingSubscribers` (0x10 on the wire). The
  core keeps, with each inbound exchange in flight, whether its
  publication had a target, and its PUBACK says so at the commit. The
  product codec writes 0x10 in MQTT 5 and the plain acknowledgement in
  3.1.1. `AckReason`, the reason of a PUBREC, is unchanged, and so are
  the trace schema and the oracle, whose bench plays QoS 2 only.
- The bench entry `publish/no-matching-subscribers-v5` disappears: the
  two transcripts are equal. An entry of `divergences.json` may carry
  `decision`, the number of the record that decided it.
- A client that counted PUBACK 0x00 as "delivered to someone" was
  already wrong with Mosquitto; with mqttiumd it now sees the same code.
  The load tool counts 0x10 as a positive acknowledgement and leaves it
  out of "acknowledged and not delivered", which counts 0x00 only.
- The order of the broker's own acknowledgements, the last point the
  owner decided on that day, is record 0050: it is a rule of the core,
  not a choice between two brokers.

## Left open

- Shared subscriptions for MQTT 3.1.1 clients, with CMP-002.
- One copy or one for each subscription, with Subscription Identifiers.
- The two `reference-nonconformant` cases close a connection that
  Mosquitto keeps. A deployment whose clients exceed Receive Maximum or
  the announced size by a defect of their own will see disconnections it
  did not see before; no option relaxes either rule.
