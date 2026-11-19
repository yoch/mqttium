# ADR 0040 — Incoming QoS 2 on MQTT 3.1.1

Status: **Proposed**

This extends ADR 0038 with level-4 incoming QoS 2 according to MQTT 3.1.1
with Errata 01, sections 3.3, 3.5–3.7, 4.3.3 and 4.4, and chapter 1
requirements ADM-004 through ADM-006. The adapter uses the existing
incoming exchange, admission and release operations. Schema, canonical
recipes, routing decisions and quotas are shared with MQTT 5.

PUBREC, PUBREL and PUBCOMP carry only a nonzero two-byte Packet
Identifier at level 4. PUBREL requires fixed flags 0b0010, including
on repetition. An unknown or already completed PUBREL gets PUBCOMP
with its Packet Identifier. Outgoing deliveries remain QoS 0 or 1.

A positive PUBREC follows the admission commit for the publication,
recipient obligations and AwaitPubrel marker. Recipient delivery can
progress while the publisher withholds PUBREL. With Clean Session 0,
the marker survives transport loss and broker restart. A repeated
PUBLISH in that exchange repeats PUBREC without a second admission.
PUBREL removes the marker and terminates the exchange. After PUBCOMP,
a new QoS 2 PUBLISH can reuse the identifier and admit a new publication.

MQTT 3.1.1 cannot encode an admission rejection in PUBREC. At quota
exhaustion the adapter closes before PUBREC and leaves no partial
admission. MQTT 5 retains its reason-bearing PUBREC.

[MQTT 3.1.1 with Errata 01](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html)
