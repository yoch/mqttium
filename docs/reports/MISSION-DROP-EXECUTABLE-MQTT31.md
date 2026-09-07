# Mission — drop executable MQTT 3.1 support

Base: `main@114ed2429703ec7e8f34bf24b815b1f7aa0ca006`

## Goal

Keep `MQTTProtocolVersion.MQTTv31` importable as the already-documented Stable compatibility relic, but stop executing an unsupported protocol and remove its runtime implementation.

## Success criteria

- `MQTTv31` remains importable with the same enum value.
- `AsyncClient` / `ProtocolEngine` fail fast with a clear unsupported-protocol error when selected.
- Remove MQTT 3.1-only CONNECT and inbound PUBLISH paths plus tests that claim behavioral support.
- Keep MQTT 3.1.1 and MQTT 5 behavior byte-for-byte/conformance-equivalent.
- Update compatibility/migration/changelog documentation.
- Exact-head CI and protocol tests green.

## Non-goals

- Do not remove or renumber the Stable enum member in this mission.
- Do not alter MQTT 3.1.1 or MQTT 5 semantics for cleanup convenience.
