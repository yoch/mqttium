# Inbound PUBLISH decode profile

Date: 2026-08-05

## Question

Would a specialised PUBLISH parser operating directly on the decoder's reusable
`bytearray` materially reduce the remaining peak memory enough to justify a second
ingress decode path?

## Baseline defect

The ordinary body extraction used:

```python
body = bytes(buf[body_start:body_end])
```

Slicing a `bytearray` creates a second `bytearray`, then `bytes()` copies it again.
The full ingress path therefore allocated about 3.00 times the payload for large
PUBLISH frames: reusable decoder storage, owned packet body and final immutable
payload.

A transient `memoryview` removes the accidental intermediate `bytearray` while
preserving the invariant that callers receive owned immutable bytes. The measured
large-frame allocation falls to about 2.00 times the payload. A 4 KiB threshold
keeps the small-frame path unchanged because the view object costs more there.

## Specialised prototype

A prototype decoded PUBLISH topic, MID and properties while holding a transient view
of the decoder buffer, then copied only the final payload into immutable bytes. It
included:

- MQTT 3.1.1 and MQTT 5;
- QoS 0, 1 and 2;
- contiguous and fragmented input;
- malformed-PUBLISH fallback through the existing engine error boundary;
- explicit checks that no view survived buffer reuse.

Ruff, mypy, Bandit, 26 focused tests and all 438 unit tests passed in workflow run:

https://github.com/yoch/mqttium/actions/runs/31034091727

The profile nevertheless measured approximately 2.00 payload multiples for both the
simple memoryview body copy and the specialised path at 1 MiB and 8 MiB. The decoder
buffer and final immutable payload dominate the peak; the intermediate body no longer
overlaps them in a way that raises the peak after the simple fix.

The prototype showed possible large-frame throughput gains, but the hosted-runner
numbers were allocator-sensitive and not stable enough to justify a second parser on
throughput alone. The release objective was peak-memory reduction, and that gain was
not demonstrated.

## Decision

Ship the simple body-copy correction and its threshold tests. Do not ship the
specialised PUBLISH path.

Reconsider only if a future production profile shows decoder CPU or cumulative memory
bandwidth, rather than peak retained memory, as a material bottleneck. Any future
proposal must preserve malformed-packet isolation and owned immutable payloads and must
be fuzzed as an independent decode path.
