# ADR 0037 — Retained zero-delay Will

Status: **Accepted** (21 November 2026, by the repository owner).

MQTT 5 sections 3.1.2.5–3.1.2.8 and 3.3.1.3 govern the CONNECT Will
Retain bit, the resulting publication and empty retained payloads. The
existing QoS 1, zero-delay Will accepts either value of that bit. Will
Message Expiry and other Will properties beyond a zero delay are refused.

## Durable authority

Session lifecycle kind 8 subtype 7 installs a retained Will. Its body has
the layout of subtype 5 with the descriptor required. Subtype 5 continues
to designate an optional non-retained Will. The installing operation still
identifies the exact transport attachment and is the intent identity.

Kind 18 publishes a retained Will. Its body follows kind 17 and appends a
big-endian u16 bitmap of the live destinations whose eligible subscription
contributions request Retain As Published. Unused bits are zero. All kind
17 and earlier lifecycle encodings keep their meaning and exact bytes.
Both Open subtypes use the same bounded Will receipt-byte reserve.

The store checks the Pending intent, original topic and payload, and the
Retain discriminator before staging the ordinary retained admission. The
admission stores the frozen producer ClientID from the intent. This remains
available after deletion of the source session. The transaction installs
or clears the retained value, admits the frozen live obligations, accounts
for references, consumes the intent and records the result. Replacement
releases one retained reference. Each existing delivery keeps its reference.
A zero-byte Will clears the retained value and still creates its eligible
live deliveries. Retained replay uses the current stored producer ClientID,
QoS and payload under the same RH, No Local and overlap rules as ordinary
retained publications. Live delivery retains only when eligible RAP asks
for it. An unknown result uses the same operation and recipe.

## Ordering and capacity

The existing durable Will barrier in the CONNECT path holds a replacement
connection for the same ClientID until the previous Will is consumed or
cancelled. The authority is the bounded writer view of both Armed and
Pending intentions. This includes reconnects after source deletion,
legitimate takeover and startup recovery. A reconnect that reaches the
barrier emits `will_connect_deferred` once per waiting transport. The
connection deadline still applies and the client may retry. A later
publication from that connection cannot take effect before the Will. Other
ClientIDs and existing MQTT connections proceed independently. Topics
shared by multiple ClientIDs follow their admission order.

Capacity is checked against the net retained replacement and the live
references that remain due. All capacities are unchanged. An infeasible
complete admission leaves the intent Pending, with its payload charged to
the Will reserve. A certain capacity refusal can be retried after resource
or route changes. There is no finite completion guarantee while a required
budget remains exhausted.

## Storage formats

SQLite schemas 12 **loopback TCP** and 13 **mTLS** add a binary `wills.retain`
discriminator. Bounded readers validate it before filtering and use typed
errors. Schemas 10 and 11 are refused without migration. The offline policy
tool handles secure schema 13 with the same monotone authority rules.

The retained Will reception is system evidence. Existing fixture-based
K1, K2b and K3 qualification retains its own scope.
