# Mission — persistence contract pruning

Base: `main@7d917547b491c8e521d1d82960b1e10810e4bd80`

## Goal

Reduce the persistence contract to the smallest modern interface actually needed by MQTTium, then delete capability detection and legacy whole-object fallbacks that exist only for hypothetical weaker third-party stores.

## Success criteria

- One coherent persistence state-machine path for built-in and supported third-party stores.
- Transition/metadata operations required by the runtime are no longer optional.
- Replay remains bounded in memory; no supported path intentionally falls back to eager whole-store hydration.
- Remove `InboundMessage | InboundRecordMeta` dual-path logic wherever the stronger contract makes it unnecessary.
- Preserve MQTT correctness, restart recovery, durable QoS semantics and store atomicity.
- Measure ACK/replay call counts, timing and memory against the exact base SHA.
- Update Provisional persistence docs, migration guidance and changelog for any contract break.

## Non-goals

- No unrelated persistence feature additions.
- No change to SQLite durability guarantees unless required by the simplified contract.
- No merge until exact-head CI, fuzz/stateful tests and targeted persistence benchmarks are green.
