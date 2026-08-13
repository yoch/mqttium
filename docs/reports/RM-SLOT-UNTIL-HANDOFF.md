# Hold the auto-ACK Receive Maximum slot until effect handoff

Date: 2026-08-13

Base commit: `1925145` (`1.0.0rc2` plus #198, #199, #201).

## Question

Should automatic inbound QoS 1 acknowledgements release the Receive Maximum
slot inside the handler and then reconstruct occupancy by decoding later
PUBLISH packets, or keep the slot until `take_effects()`?

## Established choice that this keeps

MQTT 5 §3.3.4: a PUBLISH that would exceed the advertised Receive Maximum is
refused with DISCONNECT `0x93`. Auto-PUBACKs that have been emitted but not
yet handed to the runtime still occupy that window, because the broker
pipelined the next PUBLISH before the acknowledgement could leave. That
contract is pinned by `tests/unit/test_autoack_receive_maximum_handoff.py`.

PRs #197 and #202 tried to make the reconstruction cheaper (MID-only parse,
then a shared envelope decoder). #197 was a semantic bug: a wildcard Topic
Name is valid MQTT UTF-8, so Receive Maximum won over topic validation.
#202 restored validation and was closed for insufficient demonstrated value
on the engine path (about 1–2%, sometimes negative). This change does not
revive either parser.

## Defect

`_on_qos1` acquired a slot, sent PUBACK, emitted MESSAGE, then released the
slot in the same handler. `ProtocolEngine` then recovered the count by:

1. scanning new SEND effects for PUBACK frames (`_track_pending_auto_qos1`);
2. on the next QoS 1/2 PUBLISH, calling `PublishPacket.decode` to read the
   MID and decide duplicate vs window vs cross-QoS (`_check_pending_auto_qos1_receive_maximum`);
3. running the specialised inbound decoder afterwards.

`AsyncClient._read_loop` feeds up to 256 packets through `handle_raw` before
one `take_effects()`. After the first auto-ACK in that batch, every following
QoS 1/2 PUBLISH paid a full generic decode — including a payload copy — only
to reconstruct a counter the handler had just thrown away.

Measured on this host, a 64-packet MQTT 5 QoS 1 batch called
`PublishPacket.decode` **63** times. MQTT 3.1.1 was the same.

## Change

`InboundSession` keeps the slot until the effect batch is handed off:

- auto-ACK QoS 1: acquire, send, emit, remember the MID, do not release;
- retransmission of that MID in the same batch: send and emit again, do not
  acquire (the slot is already held);
- a different identifier: `_acquire_slot()` enforces the window;
- the same identifier arriving as QoS 2: DISCONNECT `0x82`, as before;
- `take_effects()` / `transport_closed()` release the held slots.

The engine preflight, the SEND-byte scan, and the extra `PublishPacket.decode`
are deleted. Topic and property validation run in the inbound specialised
decoder, so a pipelined MQTT 5 wildcard cannot be classified as Receive
Maximum exceeded.

## Impact

- **Correctness.** Existing handoff tests still pass. The QoS 2-against-pending
  QoS 1 refusal is now the ordinary acquire diagnostic (`Receive Maximum
  exceeded`) rather than a preflight-specific string; the disconnect reason
  remains `0x93`. Wildcard-before-RM is pinned.
- **Performance.** Removes one generic PUBLISH decode per pipelined QoS 1/2
  packet after the first in an ingress batch. Local engine-path medians
  (40 timed rounds after 8 warmup, one `take_effects()` per batch):

  | Case | Before | After | after/before |
  | --- | ---: | ---: | ---: |
  | MQTT 5, 64 × 64 B | 8382 ns/msg | 5370 ns/msg | 0.64 |
  | MQTT 5, 256 × 64 B | 8499 ns/msg | 5480 ns/msg | 0.64 |
  | MQTT 5, 64 × 4 KiB | 9662 ns/msg | 6310 ns/msg | 0.65 |
  | MQTT 3.1.1, 64 × 64 B | 7322 ns/msg | 4574 ns/msg | 0.62 |

  `PublishPacket.decode` calls in a 64-packet batch: 63 → 0. Hosted-runner
  wall-clock is not claimed. This is the path `_read_loop` actually takes,
  not a helper microbench.
- **API.** `InboundSession.release_pending_auto_qos1` is internal. No
  Stable-tier change. `InboundStats.inflight` now includes auto-ACKs until
  handoff, which matches the advertised window.
- **What this is not.** It does not persist a store row for auto-ACK QoS 1.
  It does not revive `decode_publish_envelope` (#202) or a MID-only
  preflight (#197). It does not change manual-ack accounting.

## Complexity

Net deletion: two engine methods and a `PublishPacket.decode` on the ingress
hot path. The pending-MID set moves next to `_inflight`, which already owned
the counter. That is simpler than a second parser.
