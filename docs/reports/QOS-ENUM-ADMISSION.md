# Skip redundant `QoS()` at outbound admission

Date: 2026-08-12

Base commit: `c34a949` (`1.0.0rc2`).

## Question

Should `_validate_publish_request` re-construct a `QoS` enum when the caller
already passed a member?

## Established choice that this keeps

`encode_publish_item` already special-cases `type(qos) is QoS` because
"re-running the enum call on it costs about 0.32 µs — roughly 4% of a QoS 0
publication — to return its argument" (comment in `packets/publish.py`). The
async client entry points compare before converting so QoS 1/2 never pay for a
rejected direct-path enum (`test_qos1_rejection_constructs_no_qos_enum`).

Invalid integer levels must still raise `ValueError`, not `ProtocolError`.
That is pinned at the client (`test_invalid_qos_still_raises_value_error`) and
is not changed here.

This is **not** a proposal to pre-encode Topic Name bytes for QoS 1/2. PR #178
explicitly kept that path validation-only.

## Defect

`prepare_qos0` always passes `QoS.AT_MOST_ONCE` into `_validate_publish_request`,
which then did `level = QoS(qos)` unconditionally. `queue_publish` did the same
for callers who already passed a `QoS` member. `can_ever_admit` and
`publish_wire_size` repeated the conversion.

Measured on this host, 200 000 iterations:

| Operation | Time |
| --- | ---: |
| `QoS(existing member)` | 171 ns |
| `type(qos) is QoS` | 34 ns |

The 0.32 µs figure in the encoder comment was from an earlier machine; the
ratio is the same class of leftover conversion.

## Change

`_as_qos` in `protocol/outbound.py` returns the argument when it is already a
`QoS` member and otherwise calls `QoS(qos)`, so invalid ints still raise
`ValueError`. Admission, `can_ever_admit*` and `publish_wire_size` use it.

## Impact

- **Correctness.** Invalid levels still raise `ValueError`. Int `0`/`1`/`2` still
  convert. Enum members are identity-preserved (`level is qos`).
- **Performance.** Removes one enum construction from every native QoS 0
  `prepare_qos0` and from QoS 1/2 admission when the caller already passed a
  member. ~140 ns is small next to a multi-microsecond publish; it is the same
  leftover the encoder already removed, on the path that encoder assumed had
  already converted.
- **API.** `_as_qos` is internal. No public signature change.
- **What this is not.** It does not wrap `ValueError` in `ProtocolError` (that
  would be a behaviour change). `encode_publish_item` keeps its own type check
  because it maps invalid values to `ProtocolError`.

Hosted-runner wall-clock is not claimed. The evidence is the same conversion
the encoder comment already quantified, plus a `__new__` call-count pin.
