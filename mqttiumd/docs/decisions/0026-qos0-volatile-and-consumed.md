# 0026 — Volatile QoS 0 and consumed durable obligations

Status: **Proposed**. This bounded product increment preserves ARC-025 and
does not qualify all MQTT/P1/P2 semantics or accept historical proposals.

Amended by 0041 (record 0047): a QoS 2 request is granted QoS 2; "QoS 2
requests grant 1" below no longer holds.

Note (21 November 2026): the budgets this record states for sixteen connections are
the bounded profile of `runtime::Limits`. The connection limit is an option
since record 0046 (proposed), and the queues sized for sixteen connections
grow with it.

Initially QoS 0, non-retained publications use VolatilePublicationId
(source ConnectionKey and ingress order), immutable shared payloads and
bounded owner fanout. No MQTT identifier, store recipe/receipt, receive
credit, durable claim or AwaitPuback is manufactured. Absent destinations
are dropped; capacity, peer packet limit, output saturation and closed/failed
transports lead to explicit owner Qos0Dropped decisions. Accepted effects
are transferred once; a failed or ambiguous write never permits replay on
another connection. An incomplete frame completes or closes that transport.

Durable QoS 1/2 admissions continue to atomically create every obligation,
with effective QoS min(published, granted). QoS 0 obligations survive an
absent recipient as a bounded profile choice, unlike initially QoS 0 traffic.
Online work loads their bytes into bounded RAM, then ConsumeDelivery commits
conditional removal/reference collection and a receipt before one volatile
attempt on the original generation. Unknown outcomes retain OperationId and
resolve/fence. Consumed work never reconstructs emission from a historical
receipt after restart. Consumption may precede a crash/close and lose the
message; it never authorizes duplicate emission. No claim/PI/window is used.

Canonical format 1 preserves kinds 1–5 byte for byte, including kind 3's
legacy implicit QoS 1 mutation. New kind 6 ConsumeDelivery contains operation32,
publication32/index4, target8 and targetRevision8. Its DeliveryConsumed fact
contains publication32/index4/target8 (44 bytes; SQLite decision 7).
Kind 7 UpdateSubscriptions contains operation32/session8/sessionRevision8/
routeRevision8/count1, then action1/topicLength2/topic. Action 0 removes,
1 grants QoS 0 and 2 grants QoS 1. QoS 2 requests grant 1. Route replacement
is atomic and never changes already admitted obligations' QoS. Work and
recovery metadata explicitly carry QoS; a claim is valid only for QoS 1.
SQLite schema 3 refuses schema 2 before write-affecting pragmas; there is no
implicit reset or reinterpretation of existing recipes.

Owner bounds initially reuse 128 entries/references and 4 MiB as a separate
volatile fanout budget; effects remain in the bounded Outbox and carry exact
connection/occurrence identity. The runtime schedules a bounded deduplicated
fair ready-work continuation only after useful facts unblock it. The 100 ms
scan remains a lost-notification fallback, not per-message pacing. No live
MQTT retransmission timer is added. Typed readers validate durable domains
before filtering work; invalid data never becomes empty work.

New QoS 0 reception is system-test-only. Existing oracle qualifications,
proof/model limits and proposed decisions remain unchanged. The product
transition registry continues to declare incomplete guard partitions.
