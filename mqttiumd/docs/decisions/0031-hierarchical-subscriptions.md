# 0031 — Hierarchical subscriptions and per-destination aggregation

Status: **Proposed**. This implements a bounded local MQTT 5 preview without
accepting earlier proposals. Retained, shared subscriptions, Subscription
Identifiers and outgoing QoS 2 are separate increments.

## Selection and identity

A shared pure selector scans at most 128 validated filters. `+` matches one
level, including an empty level; terminal `#` includes zero further levels.
A leading wildcard does not match a topic starting with `$`. Matching preserves
UTF-8 bytes, case and empty levels and allocates no matcher state.

Each matching subscription contributes its granted QoS unless No Local excludes
that contribution because source and destination have the same durable SessionId.
At the routing cut, a source is the current session and lifecycle guards permit
one active incarnation per ClientID. Comparing SessionIds therefore implements
No Local without rebinding historical facts through the reusable name cache.
Contributions are aggregated by destination SessionId. The highest eligible
QoS, capped by published QoS and outgoing QoS 1, wins. Targets are ordered by
SessionId, independently of insertion order. A revision conflict is invalid
rather than permission to select a different incarnation.

The 16-target budget applies after aggregation and before any reservation or
output. Durable overflow refuses the complete admission. Volatile overflow
produces one explicit capacity drop before any attempt. Existing offline and
output policies then apply to each selected destination. An admitted recipe's
target set is immutable; resolution and later routing updates never recalculate it.
The historical LiveExact cause/tag 1 denotes this live routing cut for both exact
and hierarchical selection; its existing bytes and identity semantics remain.

## Durable options and ordering

UpdateSubscriptions remains the correlated durable operation. Each change now
carries granted QoS and subscription options (No Local, RAP, RH). Unsubscribe
removes the literal filter only. Replacing the same (session, filter) updates one
row. Known commit installs its prepared owner projection before positive SUBACK
or UNSUBACK. Unknown results keep their operation and exact canonical bytes.

The default-options representation retains kind 7 byte for byte. New kind 9
adds an options byte before each filter length and is used only when at least
one option is nondefault. Kind 9 with all-default options is noncanonical;
removal entries require default options. Legacy kind 3 remains unchanged.
All options and their valid domains are recovered, not inferred from a connection.

SQLite schema 7 adds the bounded route-options column. Older formats are refused
before mutation; no migration or reset is delivered. MemoryStore implements the
same contract. Invalid durable filters or options are typed InvalidData, never
absence of a route. Session termination and expiry use their existing cleanup.

RAP and RH valid values are accepted and preserved. With non-retained publication
input and no retained store, RAP cannot create a retained output and RH cannot
cause retained replay. Reserved bits/RH3 remain protocol errors. Shared and
Subscription Identifier support stay advertised unavailable; wildcard support
is advertised available only with this implementation.

## Evidence and limits

Matcher grammar and independent small tables, permutations, QoS/No Local,
16/17 destinations, durable update boundaries, six QoS combinations, public
clients, restart and mutations are system/implementation evidence. They do not
extend the qualifying oracle's scope to all routing behavior. The preview is
localhost plaintext MQTT 5, not a complete replacement for Mosquitto.
