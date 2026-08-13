# Encode QoS 1/2 launch without a `PublishPacket`

Date: 2026-08-12

Base commit: `c34a949` (`1.0.0rc2`).

## Question

Should `_launch` / `_retransmit` construct a short-lived `PublishPacket` to
produce the wire frame, or call `encode_publish_item` the way QoS 0 already
does?

## Established choice that this keeps

QoS 0 `prepare_qos0` already calls `encode_publish_item` directly, including
the `_topic_bytes` handoff from PR #178. `PublishPacket.encode_write_item` is
a one-line wrapper around that function. The packet dataclass remains the
typed view for tests, fuzz, and standalone codec use.

This is **not** a proposal to pre-encode Topic Name bytes for QoS 1/2. PR #178
explicitly kept that path validation-only: `_encode_stored_publish` still lets
the encoder produce Topic Name bytes at launch, and queued records still do
not carry them.

Frame-retention policy is unchanged (`QOS1-FRAME-POLICY.md`): contiguous
frames are dropped after SEND; segmented frames keep a shared payload;
replay without a retained frame re-encodes.

## Defect

Every connected QoS 1/2 publication that has a flow slot goes through
`OutboundSession._launch`. When `encoded_publish` is unset — the first-launch
case, and replay of a dropped contiguous frame — it built:

```python
PublishPacket(
    topic=..., payload=..., qos=..., retain=..., dup=..., mid=..., properties=...
).encode_write_item(protocol)
```

`encode_write_item` then called `encode_publish_item` with those same fields.
The dataclass existed only for that hop.

Measured on this host, 200 000 iterations, MQTT 3.1.1 QoS 1, 64-byte payload:

| Operation | Time |
| --- | ---: |
| `PublishPacket(...).encode_write_item` | 1924 ns |
| `encode_publish_item` (same fields) | 918 ns |
| `PublishPacket(...)` construction only | 838 ns |

The wrapper is about 1.0 µs, of which 0.84 µs is the dataclass. That is the
same class of leftover #175 removed from success PUBACK (packet object to
emit a frame the encoder already knew how to build), on the path every QoS 1
publication takes.

MQTT 5 with no properties measured the same split (1964 ns vs 936 ns).

## Change

`_encode_stored_publish` in `protocol/outbound.py` calls `encode_publish_item`
with the stored record's fields. `_launch` and `_retransmit` use it when they
do not already have a retained frame. `PublishPacket` is no longer imported
by the outbound session.

## Impact

- **Correctness.** Output is byte-identical to `PublishPacket.encode_write_item`
  for the same fields (pinned). DUP on replay is still forced to `True` at the
  helper, not read back from a packet object.
- **Performance.** Removes one frozen dataclass construction from every QoS 1/2
  first launch, and from replay of a dropped contiguous frame. ~1 µs is a
  meaningful fraction of a few-microsecond encode, and sits on the path
  `publish_nowait(qos=1)` actually takes.
- **API.** `_encode_stored_publish` is internal. `PublishPacket` remains public.
  `encode_publish_item` already existed; this only uses it from the session
  that already imported it for QoS 0.
- **What this is not.** It does not retain Topic Name bytes on QoS 1/2 records
  (#178). It does not change when a frame is retained versus re-encoded
  (`QOS1-FRAME-POLICY.md`). It does not skip `_check_outbound_size` after
  encode.

Hosted-runner wall-clock is not claimed. The evidence is the same
packet-object-to-encode leftover the PUBACK fast path already quantified,
plus a call-count pin that `encode_write_item` is no longer on this path.
