# 0014 — Routes and limits among the recovered facts, and the order of an inventory

Status: **Accepted** (21 November 2026, by the repository owner;
proposed 15 November 2026, lot L1 with lots L2 and L3, first wave). Recorded by the integration owner.

## Context

After a restart the core must prepare an admission: targets, route
revision, costs, bounds. DEM-012 says the core rebuilds only what the
product can know, so these cannot come from the bench. `RecoveredFact` of
lot L0 had the epoch, the sessions, the inbound exchanges and the
operations, and neither routes nor limits.

## Decision

- `RecoveredFact` gains `Limits { max_targets_per_admission,
  max_topic_bytes, max_payload_bytes }`, `RouteTable { revision }` and
  `Route { topic, target, target_revision, max_qos }`.
- The durable balances (payload bytes, references, exchange slots, receipt
  slots) stay with the store, which checks and commits them with the
  admission. The core gets only the bounds it needs to refuse before any
  mutation.
- An inventory is ordered: `Epoch`, `Limits`, `RouteTable`, `Route`,
  `Session`, `InboundExchange`, `Operation`. Ranks never decrease; `Epoch`,
  `Limits` and `RouteTable` appear at most once; routes are sorted by the
  bytes of their topic, and the targets of one topic keep the order in
  which they were added.
- `RecoveryOrder::admit` is that rule, written once for the store that
  writes the facts, the readers of a trace and the core that consumes
  them. A refusal changes nothing. It checks one page when it starts fresh
  and a whole inventory when it is carried from page to page.

## Limits

- The rule does not require a fact to be present. Whether an inventory may
  lack its epoch, its limits or its route table, and a route without a
  table before it, are refused only by whoever reads the whole inventory:
  the core and the oracle.
- The order in which the targets of one topic were added is not visible in
  the facts, so no reader can check it.

## Consequences

- Lot L2 produces these facts in its recovery pages; lot L3 extends the
  fact union of the trace schema, which changes the contract digest; lot
  L7 stores what they are read from.
- Effect slots, result slots and store operations in flight are parameters
  of construction, not recovered facts.
