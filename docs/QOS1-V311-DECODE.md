# MQTT 3.1.1 QoS 1 direct field decode

## Problem

Every inbound MQTT 3.1.1 QoS 1 PUBLISH was decoded into a generic
`PublishPacket`, then copied into the delivered `Message` before the shared
PUBACK/manual-ack state machine ran. Native RTT profiling identified this
packet-model round trip as a repeated CPU cost on both legs of each application
request/response pair.

## Change

The MQTT 3.1.1 inbound dispatch now has two narrow direct paths: QoS 0 decodes
directly to `Message`, and QoS 1 decodes topic, packet identifier and payload
before invoking a shared `_on_qos1()` state machine. The generic MQTT 5/QoS 1
path invokes that same state machine after property-aware decoding.

Receive Maximum accounting, PUBACK generation, manual acknowledgement, duplicate
replay and persistence remain in one implementation. MQTT 5 and QoS 2 retain the
generic packet decoder.

## Evidence

A seven-cycle rotated capacity A/B, with the exact-size `publish_nowait()` fix
applied to both base and candidate, measured:

- `await publish()`: **+4.78%** median throughput, p50 **-4.82%**;
- isolated callback + `publish_nowait()`: **+4.18%**, p50 **-3.16%**;
- experimental inline callback + `publish_nowait()`: **+4.00%**, p50 **-3.84%**,
  p95 **-4.79%**.

Capacity run: <https://github.com/yoch/mqttium/actions/runs/31058609259>

An additional open-loop test completed every request at both offered loads. Near
90% of base capacity, all seven cycles favored the candidate: p50 improved
**8.37%** and p95 **18.11%**. Near 50%, completion remained 100% and latency was
scheduling-noise dominated, with no consistent benefit claimed.

Open-loop run: <https://github.com/yoch/mqttium/actions/runs/31058890448>

## Composition validation

The rebased tree is validated on top of the already merged QoS 0 direct decoder.
A dedicated test sends adjacent MQTT 3.1.1 QoS 0 and QoS 1 publications and
asserts that neither enters `PublishPacket.decode()`. MQTT 5 QoS 1 and MQTT 3.1.1
QoS 2 explicitly remain on the generic path.

## Risks

The main risk is divergence between specialized parsing and the property-aware
generic path. Parsing is isolated in small helpers and QoS 1 state transitions
are factored into one shared method. Tests compare fields against
`PublishPacket.decode()`, cover packet identifier validation, automatic PUBACK,
manual-ack duplicate state, and prove MQTT 5/QoS 2 still use the generic decoder.
The full unit and fuzz suites run before publication.
