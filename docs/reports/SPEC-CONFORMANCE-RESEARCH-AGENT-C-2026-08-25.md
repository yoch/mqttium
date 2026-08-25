# MQTT Normative Specification Conformance Research (Agent C)

- **Date**: 2026-08-25
- **Audited Commit SHA**: `78c8d4caddacf80d77382a67651174a6a9c8a6f5`
- **Scope / Ownership**: Normative MQTT 3.1.1 and MQTT 5.0 specification coverage mapping to current code and deterministic tests.
- **Companion Deterministic Suite**: `tests/unit/test_spec_conformance_gaps.py`

---

## Executive Summary

An adversarial audit of the normative MQTT 3.1.1 and 5.0 specifications against the MQTTium core state machine (`ProtocolEngine`, `InboundSession`, `OutboundSession`, and specialized codecs) revealed five concrete behavioral gaps between standard normative requirements and current implementation.

All five gaps were mapped to the exact code paths, contrasted against existing test coverage, and codified in deterministic regression tests (`test_gap_1` through `test_gap_5` in `tests/unit/test_spec_conformance_gaps.py`).

---

## Detailed Findings

### Finding 1: Retransmission of `PUBREL` blocked by send quota (`Receive Maximum`) during resumed session replay

- **Normative Requirements**:
  - `[MQTT-4.9.0-2]`: Send quota applies only to QoS > 0 `PUBLISH` packets. When quota reaches zero, no more `PUBLISH` packets with QoS > 0 may be sent.
  - `[MQTT-4.9.0-3]`: The Client and Server MUST continue to process and respond to all other MQTT Control Packets even if the quota is zero.
  - `[MQTT-4.4.0-1]`: When a Client reconnects with a present session, it MUST re-send unacknowledged `PUBREL` Packets.
- **Code Path**:
  `src/mqttium/protocol/outbound.py::OutboundSession._replay_message` (lines 1141–1143) and `replay_session` (lines 1160–1166).
- **Existing Coverage**:
  - `test_mqtt_4_3_3_6_replay_sends_pubrel_not_publish`: asserts `PUBREL` is emitted when replaying `WAIT_PUBCOMP`, but uses unconstrained `Receive Maximum`.
  - `test_mqtt_4_9_0_3_a_full_send_quota_does_not_block_other_packets`: tests live `SUBSCRIBE` / `PINGREQ` under full quota, but not session replay.
- **Root Cause & Gap**:
  In `_replay_message`, all unacknowledged records (including `WAIT_PUBCOMP`) are tested against `self.flow.try_acquire()`. If previous `PUBLISH` messages occupy the broker's `Receive Maximum` quota, `try_acquire()` returns `False`, causing the `WAIT_PUBCOMP` record to be placed in `_queued` without sending `PUBREL`.
- **Deterministic Test**:
  `tests/unit/test_spec_conformance_gaps.py::test_gap_1_resumed_session_replays_pubrel_when_send_quota_exhausted`
- **Classification**: Likely implementation bug.
- **Confidence**: 99%.

---

### Finding 2: Inbound Topic Alias rejected when `topic_alias_maximum` is configured via `connect_properties`

- **Normative Requirement**:
  - `[MQTT-3.3.2-10]`: A Client MUST accept all Topic Alias values greater than 0 and less than or equal to the Topic Alias Maximum value that it sent in the CONNECT packet.
- **Code Path**:
  `src/mqttium/protocol/engine.py::ProtocolEngine.begin_connect` (lines 298–313) vs `src/mqttium/protocol/inbound.py::InboundSession._resolve_topic_fields` (line 1085).
- **Existing Coverage**:
  - `tests/unit/test_inbound_alias_validation.py`
  - `test_mqtt_3_3_2_8_topic_alias_zero_is_refused`
- **Root Cause & Gap**:
  When `connect_properties` specifies `topic_alias_maximum` (e.g. 5) while `EngineConfig.topic_alias_maximum` remains at default 0, `begin_connect` puts `topic_alias_maximum = 5` into the wire `CONNECT` packet. However, `_resolve_topic_fields` checks incoming topic aliases against `self.config.topic_alias_maximum` (which is 0). An inbound `PUBLISH` with `topic_alias = 1` is immediately rejected with `DISCONNECT 0x94 (Topic Alias invalid)` and drops the connection.
- **Deterministic Test**:
  `tests/unit/test_spec_conformance_gaps.py::test_gap_2_inbound_topic_alias_accepted_when_configured_via_connect_properties`
- **Classification**: Likely implementation bug.
- **Confidence**: 99%.

---

### Finding 3: DISCONNECT permitted to send non-zero `Session Expiry Interval` after Server overrode it to 0 on CONNACK

- **Normative Requirement**:
  - MQTT 5.0 §3.14.2.2.2: If the Session Expiry Interval was non-zero in the CONNECT packet and the Session Expiry Interval of the Session is zero at the time the DISCONNECT packet is sent, it is a Protocol Error to set a non-zero Session Expiry Interval in the DISCONNECT packet.
- **Code Path**:
  `src/mqttium/protocol/engine.py::ProtocolEngine._check_disconnect_session_expiry` (lines 481–504).
- **Existing Coverage**:
  - `test_mqtt5_disconnect_cannot_extend_a_session_that_was_never_durable`: tests case where `CONNECT` sent 0 or None, but does not test Server overriding non-zero `CONNECT` expiry to 0 on `CONNACK`.
- **Root Cause & Gap**:
  `_check_disconnect_session_expiry` checks only `if not self._sent_session_expiry_interval:`. When the client sends 600 on `CONNECT` and the broker answers with 0 on `CONNACK` (setting `self.negotiated.session_expiry_interval = 0`), calling `begin_disconnect(properties=Properties({"session_expiry_interval": 300}))` passes local checks and emits the invalid DISCONNECT frame.
- **Deterministic Test**:
  `tests/unit/test_spec_conformance_gaps.py::test_gap_3_disconnect_rejects_session_expiry_after_server_sets_zero_in_connack`
- **Classification**: Likely implementation bug.
- **Confidence**: 98%.

---

### Finding 4: Inbound DISCONNECT packet from Server silently accepted under MQTT 3.1.1

- **Normative Requirement**:
  - MQTT 3.1.1 §2.1 Table 2.1, §3.14, and §4.8 (`[MQTT-4.8.0-1]`): Under MQTT 3.1.1, the `DISCONNECT` packet is strictly Client-to-Server. A Server sending `DISCONNECT` is a protocol violation requiring immediate connection termination with protocol error.
- **Code Path**:
  `src/mqttium/protocol/engine.py::ProtocolEngine._on_disconnect` (lines 785–808) and `src/mqttium/packets/_control.py::decode_disconnect_v311` (lines 28–30).
- **Existing Coverage**:
  - `_on_auth` explicitly rejects MQTT 3.1.1 with `if not self.codec.is_mqtt5: raise ProtocolError("AUTH is not valid in MQTT 3.1.1")`, but `_on_disconnect` lacks this check.
- **Root Cause & Gap**:
  Receiving `b"\xe0\x00"` on an MQTT 3.1.1 connection is decoded by `decode_disconnect_v311` and handled cleanly as `DisconnectInfo(reason_code=0, from_broker=True)`, without triggering `PROTOCOL_ERROR`.
- **Deterministic Test**:
  `tests/unit/test_spec_conformance_gaps.py::test_gap_4_mqtt311_server_disconnect_is_protocol_error`
- **Classification**: Likely implementation bug.
- **Confidence**: 95%.

---

### Finding 5: Inbound MQTT 5.0 PUBLISH with empty topic and no Topic Alias omits sending DISCONNECT 0x82

- **Normative Requirement**:
  - MQTT 5.0 §3.3.2.3.5 / §4.13: A PUBLISH packet with a Topic Alias that is not set and a zero length Topic Name is a Protocol Error. The Client or Server uses DISCONNECT with Reason Code 0x82 (Protocol Error).
- **Code Path**:
  `src/mqttium/protocol/inbound.py::InboundSession._resolve_topic_fields` (lines 1079–1083).
- **Existing Coverage**:
  - `tests/unit/test_inbound_alias_validation.py`: tests invalid alias ID or unknown alias (which properly call `_protocol_disconnect(0x94)`).
- **Root Cause & Gap**:
  When `not topic` and `alias is None`, `_resolve_topic_fields` raises `ProtocolError("PUBLISH with empty topic and no topic alias")` without calling `self._protocol_disconnect(0x82)`. As a result, the engine emits only a local `EffectKind.PROTOCOL_ERROR` and fails to send the required `DISCONNECT 0x82` packet to the broker before teardown.
- **Deterministic Test**:
  `tests/unit/test_spec_conformance_gaps.py::test_gap_5_mqtt5_publish_empty_topic_without_alias_emits_disconnect_0x82`
- **Classification**: Likely implementation bug.
- **Confidence**: 95%.

---

## Handoff Candidates

1. **Concurrency Schedule Candidate**:
   `InboundSession.release_pending_auto_qos1` vs writer pump drain interleaving: when `take_effects()` releases auto-ack Receive Maximum slots, verify if an interleaved reader step could admit a new publish before the preceding auto-PUBACK write item has been committed to the transport socket.
2. **Persistence Crash-Consistency Candidate**:
   `SqliteInflightStore` crash consistency across schema migration: verify whether `in_replay_pages` handles a crash occurring mid-transaction during a large `batch()` deletion of acknowledged QoS 2 records without leaving dangling metadata references.
3. **Performance Opportunity Candidate**:
   `Properties._signature()` in `codec/properties.py`: property cache key builds tuples from dictionary items on each publish; a frozen/immutable property container could eliminate dict iteration and tuple hashing overhead on the hot outbound path.
