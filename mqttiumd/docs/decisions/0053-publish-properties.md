# Record 0053: End-to-end MQTT 5 PUBLISH application properties

Status: **Proposed** (22 November 2026).

## Context

Previously, `mqttiumd` validated incoming MQTT 5 PUBLISH properties in the wire
decoder (`codec.rs`), accepted only Message Expiry Interval (`0x02`), and
refused all five end-to-end application properties (`0x01`, `0x03`, `0x08`,
`0x09`, `0x26`) with `PublishPropertiesOutsideProfile` (`0x83`). Modern MQTT 5
clients and request/response workloads rely on Content Type, Response Topic,
Correlation Data, Payload Format Indicator, and ordered User Properties.

## Decision

1. **Contract type (`mqttiumd_contract::properties::PublishProperties`)**:
   - Carries `payload_format_indicator: Option<u8>`, `content_type: Option<SharedBytes>`,
     `response_topic: Option<SharedBytes>`, `correlation_data: Option<SharedBytes>`,
     and ordered `user_properties: Arc<[(SharedBytes, SharedBytes)]>`.
   - Bounded by `MAX_USER_PROPERTIES = 64` and `MAX_PROPERTY_BYTES = 16_384`
     logical bytes; validated before any application-owned copy in `codec.rs`
     and inside the pure core (`no_std + alloc`, `#![forbid(unsafe_code)]`).
   - When `payload_format_indicator == Some(1)`, RFC 3629 UTF-8 validity of the
     payload is enforced (`0x99 PayloadFormatInvalid`, MQTT-3.3.2-5).
   - `response_topic` must be a valid non-empty MQTT topic name with no
     wildcard characters (`+`, `#`; MQTT-3.3.2-14).
   - User Property ordering is preserved end-to-end (MQTT-3.3.2-17).
   - Publications without application properties carry `None`, incurring zero
     additional allocation on the hot path.
2. **Canonical recipe wrapper (kind 23, `Recipe::AnnotatedPublication`)**:
   - Wraps an inner publication recipe (`AdmitPublication`,
     `RetainedPublication`, `RetainedQos0`, or `ExpiringPublication`) with
     non-empty canonical `PublishProperties` bytes.
   - Emitted only when at least one application property is present so all
     existing recipes remain byte-for-byte identical.
3. **Storage (SQLite Schema 14/15 & VolatileStore)**:
   - `publications.properties BLOB` is added as the last column of
     `publications` (`NULL` when absent, non-empty canonical bytes when
     present) and copied verbatim during transactional retained replay and
     atomic `save_state_atomic` snapshots.
   - `VolatileStore` stores `Option<Arc<PublishProperties>>` on `VPublication`
     and shares it via O(1) `Arc::clone` on retained replay and `read_work`.
4. **Protocol encoding and Maximum Packet Size**:
   - Outgoing MQTT 5 PUBLISH encodes Message Expiry Interval (`0x02`, preserving
     its exact fixed-width in-place update offset) followed by the five
     application properties in canonical order with minimal Variable Byte
     Integer `Property Length` and `Remaining Length`.
   - Outgoing MQTT 3.1.1 PUBLISH strips all MQTT 5 properties cleanly.
   - `publish_packet_size` in the core includes exact property wire length and
     multi-byte property-length varint overhead when checking subscriber
     `Maximum Packet Size` (MQTT-3.1.2-25).

## Consequences

- MQTT 5 publishers and subscribers exchange all five application properties
  end-to-end across QoS 0, QoS 1, QoS 2, retained replay, durable SQLite
  restarts, and volatile snapshots.
- Will properties (other than zero Will Delay Interval) remain outside the
  received profile and continue to be refused explicitly.
- Topic Aliases (`0x23`) remain disabled (`Topic Alias Maximum = 0`, refused
  with `0x94`).
