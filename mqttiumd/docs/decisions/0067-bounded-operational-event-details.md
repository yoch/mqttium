# 0067 — Bounded operational event details

Status: **Proposed** (25 November 2026). Runtime and v1 product-event evidence. This records the
event shapes emitted by the broker, including the QoS 0 pacing evidence of
[decision 0063](0063-qos0-paced-per-connection.md).

The checker applies a separate closed details schema to `ready`. Current
TCP and mTLS profiles publish 50 fields covering admission limits, storage,
protocol capabilities and delivery capacity. The schema accepts their
typed names, including `qos0_window` and `qos0_output_reserve`, and caps a
record at 50 fields. Earlier versions published subsets, including eight
Will fields before they moved to `will_capabilities` in commit `f0884251`.
The receipt policy uses up to 14 independently typed fields. Other ordinary
details retain their 40-field limit and lower-case name syntax.

At operations level, `counters.details.counts` maps event names to decimal
u64 strings. The writer combines at most 96 slow tallies, their overflow
key `other` and 16 fast tallies. A nonempty record can therefore contain at
most 113 counts. TLS profiles pair the unauthenticated handshake capacity
with the per-connection TLS buffer in bytes. The archived pair is
`(8, 98304)`, and the scaled profile in `09632cd0` uses
`(64, 2097152)`.

The [v1 schema](../../contracts/schema/v1/product-event.schema.json)
and [executable checker tests](../../tools/test_product_event_details.py)
exercise these limits. The [corpus](../../contracts/examples/v1/index.json)
includes both TLS revisions and negative examples for misplaced fields,
changed types and overflowing counters.
