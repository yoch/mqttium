# Exact wire-size admission for `publish_nowait()`

## Problem

`AsyncClient.publish_nowait()` must reject a publication immediately when the
bounded writer can never accept its encoded MQTT frame. The previous admission
check built and encoded a complete preview `PublishPacket` with a placeholder
packet identifier, used only its byte length, then let `OutboundSession` allocate
the real identifier and encode the real frame again.

For QoS 1/2 this meant two complete encodes per publication. The preview could
not be reused safely because the real packet identifier had not yet been allocated.

## Change

`OutboundSession.publish_wire_size()` computes the exact wire size from the same
validated topic/property sizes used by normal outbound admission, including the
fixed header, Remaining Length VBI, packet identifier and MQTT 5 properties.
`publish_nowait()` and `publish_many_nowait()` use that integer for the writer
capacity check and encode only the real publication.

No queue bound, backpressure mode, receipt lifecycle, packet identifier rule or
wire representation changes.

For QoS 0, the native direct-writer path is conditional on `on_publish is None`.
When a publish callback is installed, MQTTium deliberately returns to the
protocol-engine and `EffectPump` path to preserve callback completion ordering.

## Evidence

A seven-cycle rotated native QoS 1 RTT comparison, 12,000 request/response pairs
per sample and 32 outstanding requests, measured:

- isolated callback + `publish_nowait()`: median **+6.12%** throughput; all seven
  cycles positive; p50 latency **-5.67%**, p95 **-4.42%**;
- experimental inline callback + `publish_nowait()`: median **+5.42%**; all seven
  cycles positive; p50 **-5.98%**, p95 **-5.58%**;
- `await publish()` control, whose path is unchanged: noisy and treated as a
  control rather than a claimed gain.

Profiling confirms one `PublishPacket.encode_write_item()` call per real
publication after the change, versus two before it. The retained microbenchmark
compares exact-size arithmetic with preview encoding and also asserts equality.

Experimental run: <https://github.com/yoch/mqttium/actions/runs/31056402920>

## Risks

The principal risk is size-calculation drift from the encoder. Parametrized tests
compare the calculated size with actual encoded frames across MQTT 3.1.1/MQTT 5,
QoS 0/1/2, Unicode topics, MQTT 5 properties, VBI boundaries and segmented payloads.
