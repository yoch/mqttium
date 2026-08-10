# QoS 1 and pre-PUBREC frame policy

Date: 2026-08-05

## Question

Should an outbound QoS 1 or QoS 2-before-PUBREC record retain its first encoded
PUBLISH frame, always re-encode during session replay, or select by size?

## Existing behavior

The in-memory store retained `encoded_publish` after launch, but `_retransmit()`
re-encoded the packet with DUP=1 instead of using it. SQLite never persisted the
frame. Below the 1 MiB transport segmentation threshold, the retained item is one
contiguous `bytes` object and therefore duplicates the payload. At and above the
threshold it is `(header, payload)`: only the small header is new and the payload
object is shared.

## A/B measurement

`benchmarks/qos1_frame_policy.py` measured retained traced allocation after the
payload objects already existed, plus first and later replay CPU. The retained
workflow run used CPython 3.12.13 on Ubuntu 24.04:

https://github.com/yoch/mqttium/actions/runs/31041975402

Representative medians:

| Protocol/profile | Payload | Records | Current retained frame allocation | Selected policy | Re-encode | Selected first replay | Selected later replay |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| MQTT 3.1.1 | 4 KiB | 4,096 | 16.28 MiB | 32.59 KiB | 4.35 us | 4.35 us | 4.35 us |
| MQTT 3.1.1 | 64 KiB | 512 | 32.04 MiB | 4.44 KiB | 8.53 us | 8.53 us | 8.53 us |
| MQTT 5, property-heavy | 4 KiB | 4,096 | 21.66 MiB | 32.76 KiB | 39.12 us | 39.12 us | 39.12 us |
| MQTT 3.1.1 | 1 MiB | 32 | 4.32 KiB | 4.32 KiB | 4.36 us | 0.44 us | 0.16 us |
| MQTT 5, property-heavy | 8 MiB | 8 | 12.26 KiB | 12.26 KiB | 38.42 us | 0.54 us | 0.17 us |

The contiguous frame provides no replay benefit because it must acquire DUP and
was already being re-encoded. The segmented frame is cheap to retain and saves a
full topic/property encode; after its header has DUP set, subsequent replays
return the same tuple.

## Decision

Use the existing transport representation as the policy boundary:

- do not retain a contiguous `bytes` PUBLISH after its initial SEND;
- retain a segmented `(header, payload)` item because the payload is shared;
- on its first retransmission, replace only byte zero of the small header to set
  DUP and preserve the payload object;
- on later retransmissions, reuse the already-DUP tuple unchanged;
- accept a contiguous item supplied by a legacy or third-party store, patch it
  for that replay, then stop retaining it.

This applies equally to QoS 1 `WAIT_PUBACK` and QoS 2 `WAIT_PUBREC`. QoS 2
`WAIT_PUBCOMP` remains governed by phase-two compaction and retransmits PUBREL.
SQLite behavior is unchanged because its durable schema never stored encoded
frames.
