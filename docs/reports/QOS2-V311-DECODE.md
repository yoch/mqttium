# MQTT 3.1.1 QoS 2 direct field decode

Date: 2026-08-12

Base commit: `c34a949` (`1.0.0rc2`).

## Question

Should inbound MQTT 3.1.1 QoS 2 PUBLISH use the same field decoder as QoS 1,
instead of constructing a short-lived `PublishPacket`?

## Deviation from a previous report

[`QOS1-V311-DECODE.md`](QOS1-V311-DECODE.md) (2026-08-06) left MQTT 5 and QoS 2
on the generic packet decoder. That was a composition bound for PR #42, not a
measured rejection of a QoS 2 field path.

RC2 already moved MQTT 5 QoS 0/1/2 onto `_decode_v5_publish_fields` (#174).
MQTT 3.1.1 QoS 2 is the leftover: the wire layout is identical to QoS 1
(Topic Name, packet identifier, payload; no property table). MQTT 3.1 stays on
the generic decoder, as RC2 already documented.

This is therefore a completion of the established specialised-decode programme,
not a new ingress architecture.

## Defect

`InboundSession.on_publish` specialised MQTT 3.1.1 QoS 0 and QoS 1, then fell
through to `PublishPacket.decode` for QoS 2, copied the fields into `Message`,
and entered `_on_qos2`. Issue #39 ranked generic packet-model construction as
the dominant avoidable ingress cost. QoS 1 recovered about 4% RTT capacity by
removing that round trip; QoS 2 still paid it.

A unit pin (`test_v311_qos2_remains_generic`) required exactly one
`PublishPacket.decode` call, so the leftover was intentional at the time of
#42 and was not re-evaluated after #174.

## Change

MQTT 3.1.1 QoS 2 now uses `_decode_v311_qos1_fields` (same layout) and the
shared `_on_qos2` state machine. The helper's docstring is updated; its
signature is unchanged. MQTT 3.1 is the only remaining `PublishPacket.decode`
PUBLISH path.

## Impact

- **Correctness.** Field tuples are compared against `PublishPacket.decode` for
  both QoS 1 and QoS 2, including non-ASCII topics, retain, DUP and both MID
  extremes. The inverted pin requires zero generic decodes and checks the
  SEND-then-MESSAGE effect order already required of `_on_qos2`.
- **Performance.** Removes one `PublishPacket` construction and field copy per
  inbound MQTT 3.1.1 QoS 2 PUBLISH. The QoS 2 state machine, PUBREC encode,
  Receive Maximum and persistence are untouched.
- **API.** No public name changes. `_decode_v311_qos1_fields` remains internal.
- **What this is not.** It does not specialise MQTT 3.1, does not add an
  in-buffer decoder (rejected in `PUBLISH-DECODE-PROFILE.md`), and does not
  change callback isolation.

Hosted-runner wall-clock is not claimed. The mechanism is the same redundant
packet-model round trip already removed for MQTT 3.1.1 QoS 0/1 and MQTT 5.
