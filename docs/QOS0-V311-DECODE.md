# MQTT 3.1.1 QoS 0 direct message decode

## Problem

For every inbound MQTT 3.1.1 QoS 0 PUBLISH, the generic path decoded and
validated a full `PublishPacket`, then immediately copied its fields into a
`Message` carried by an `EngineEffect`. Profiling showed that packet-model
construction and the subsequent packet-to-message conversion dominate the
subscriber hot path, ahead of callback dispatch.

MQTT 3.1.1 QoS 0 has no properties or packet identifier, so the intermediate
packet object carries no protocol state needed after validation.

## Change

`InboundSession` directly decodes the topic and payload into a `Message` for the
narrow MQTT 3.1.1/QoS 0 case. It preserves the established UTF-8 and publish-topic
validation, rejects QoS 0 with DUP, retains a zero-copy payload view and emits the
same ordered `MESSAGE` effect.

The generic `PublishPacket` path remains authoritative for:

- MQTT 5, including properties and topic aliases;
- QoS 1 and QoS 2 acknowledgement state;
- invalid/reserved QoS flag combinations;
- every non-PUBLISH packet.

## Evidence

Seven-cycle rotated same-runner A/B, 180,000 telemetry256 messages per sample:

| Delivery mode | Direct transport | Mosquitto with raw publishers |
| --- | ---: | ---: |
| Existing isolated callback worker | **+22.14%** | **+21.20%** |
| Experimental synchronous inline callback | **+25.26%** | **+23.41%** |

All seven cycles were positive in every cell. The gain composes with the inline
callback experiment, demonstrating that decode/model construction and callback
isolation are distinct costs.

Experimental run: <https://github.com/yoch/mqttium/actions/runs/31057216680>

## Risks

The main risk is validation drift between the specialized and generic decoders.
The specialized function is deliberately small and restricted to a protocol case
with no properties or packet identifier. Tests compare its result with
`PublishPacket.decode()`, cover Unicode, retain, empty and large payloads, DUP,
empty/wildcard topics, and verify that MQTT 5 and QoS 1 still invoke the generic
path. The full unit and Hypothesis fuzz suites run before publication.
