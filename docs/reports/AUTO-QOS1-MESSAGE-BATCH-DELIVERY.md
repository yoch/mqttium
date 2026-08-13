# Batch fresh automatic QoS 1 MESSAGE delivery with QoS 0

Date: 2026-08-13

Base commit: `a7f18a7` (current main after #204).

## Question

Can the existing small QoS 0 MESSAGE queue-transfer batch also cover fresh
automatic QoS 1 delivery, and can that path avoid the delivery-mark store write,
without weakening restart or persisted QoS semantics?

## Review finding

The original #205 inferred persistence from `message.qos` plus the current
`manual_ack` setting. That is not sufficient. A durable QoS 1 row created while
manual acknowledgement was enabled can be replayed later by a client currently
using automatic acknowledgement. Such a MESSAGE still needs `delivered=True`.
Treating every current auto-QoS1 message as non-persisted would make that replay
eligible for the fast batch and skip the delivery mark, allowing repeat delivery
on a later restart.

The original patch also moved manual-QoS1 and QoS2 delivery marks into the
synchronous batch path without measured end-to-end value for those persisted
cases. SQLite stores are synchronous, so preserving the established per-effect
path avoids widening event-loop critical work unnecessarily.

## Reviewed change

`InboundSession` now tags each MESSAGE effect with whether application acceptance
must mark a persisted inbound row. The runtime consumes that fact directly:

- QoS 0 and fresh automatic QoS 1 have no delivery mark and may batch;
- manual QoS 1, QoS 2, and persisted replay MESSAGEs require a mark and retain
  the established per-effect path;
- automatic QoS 1 single-message fallback also skips the old absent-row mark;
- `EffectPump.drain_inline` can consume an eligible MESSAGE prefix immediately,
  so the common SEND + auto-QoS1 MESSAGE burst need not spawn
  `mqttium-effect-flush`;
- large/property-bearing messages and queue-full cases keep the awaited path;
- `message_delivery="both"` still checks both capacities before either queue
  mutates, and callbacks remain isolated on the worker.

This makes persistence ownership explicit at the protocol/runtime boundary
instead of reconstructing it from current configuration.

## Validation

Focused tests pin batching for QoS 0 and fresh auto-QoS1, persisted-message
fallback, mixed-prefix stopping, atomic `both` delivery, stale epochs, inline
drain without scheduling, the auto-QoS1 absent-row skip, and—critically—a
persisted QoS1 replay under a current auto-ack configuration that still marks
the durable row delivered.

Performance acceptance additionally requires paired broker-backed measurement on
the final reviewed branch; helper-only timing is not sufficient.
