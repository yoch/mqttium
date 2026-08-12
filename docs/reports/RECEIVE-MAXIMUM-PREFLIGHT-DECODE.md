# Receive Maximum preflight without `PublishPacket.decode`

Date: 2026-08-12

Base commit: `c34a949` (`1.0.0rc2`).

## Question

Must the Receive Maximum preflight construct a `PublishPacket` to recover the
packet identifier of a pipelined inbound QoS 1/2 PUBLISH?

## Established choice that this keeps

Automatic acknowledgements remain counted against Receive Maximum until
`take_effects()` hands the effect batch to the runtime. That contract is
unchanged: a broker that pipelines a second PUBLISH before the first PUBACK can
leave the engine still consumes a slot. Tests in
`tests/unit/test_autoack_receive_maximum_handoff.py` pin the window, duplicate
MID, cross-QoS reuse and handoff-release cases.

This change does **not** reopen inline callbacks, effect-pump partitioning, or
the specialised in-buffer PUBLISH decoder. Those were measured and rejected
earlier.

## Defect

`AsyncClient._read_loop` decodes up to 256 packets under one engine lock, then
collects effects. After the first auto-PUBACK sits in the current batch,
`_check_pending_auto_qos1_receive_maximum` called `PublishPacket.decode` on
every subsequent QoS 1/2 PUBLISH — solely to read `mid` and `qos`.

That undoes the specialised inbound decoders (#41, #42, #174) on the path the
read loop actually takes. A unit pin even documented the workaround: MQTT 5 QoS
2 was fed to a fresh engine "otherwise the Receive-Maximum preflight
intentionally decodes a second PUBLISH".

Exact call counts on this host, 64 MQTT 3.1.1 QoS 1 PUBLISH frames:

| Discipline | `PublishPacket.decode` calls |
| --- | ---: |
| `take_effects()` after each packet | 0 |
| one engine batch (read-loop shape) | **63** |

Median `handle_raw` time, 7 repeats, 64 messages:

| Payload | One batch | Isolated | Ratio |
| --- | ---: | ---: | ---: |
| 64 B | 5.9 µs/msg | 3.7 µs/msg | 1.59× |
| 4 KiB | 6.3 µs/msg | 4.0 µs/msg | 1.58× |

Isolated includes a `take_effects()` per message and is still faster: the extra
decode dominates. A 4 KiB MQTT 5 QoS 1 `PublishPacket.decode` measured 2309 ns
against 433 ns to `unpack_utf8` + `unpack_u16` (no payload copy, no property
table, no packet object).

## Change

`_qos_publish_mid` locates the identifier from the Topic Name length prefix and
the following two bytes. MQTT 3.1 / 3.1.1 / 5 share that layout; properties
follow the MID. QoS is already in the fixed-header flags.

A truncated or malformed Topic Name returns `None`. The preflight then skips
Receive Maximum accounting and the inbound handler reports the malformation, so
a bad packet cannot be turned into DISCONNECT 0x93.

## Impact

- **Correctness.** Window, duplicate, cross-QoS and handoff tests keep the same
  assertions. A new pin forbids `PublishPacket.decode` on an 8-message pipelined
  QoS 1 batch. A malformed follow-up PUBLISH at `local_receive_maximum=1` still
  surfaces as UTF-8 malformation, not Receive Maximum exceeded.
- **Performance.** Removes one full PUBLISH decode — including a payload copy —
  from every QoS 1/2 message after the first in an ingress batch. That is the
  common subscriber shape: many PUBLISH frames per `read()`.
- **API / persistence / layering.** No public name changes. The engine no longer
  imports `PublishPacket`. Inbound session decode remains the sole constructor
  of delivered `Message` objects.
- **What this is not.** Topic-byte reuse for outbound QoS 1/2 is left alone:
  PR #178 explicitly kept that path validation-only so queued records do not
  allocate Topic Name bytes early.

Hosted-runner wall-clock is not claimed. The evidence here is exact call counts
plus an in-process micro-timing of the redundant decode, which is the same class
of proof that retained the specialised inbound decoders.
