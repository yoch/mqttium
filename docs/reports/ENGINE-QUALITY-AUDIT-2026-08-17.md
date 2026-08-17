# Engine quality audit — `protocol/` core

| | |
| --- | --- |
| Date | 2026-08-17 |
| Commit audited | `4677e55` (Fix paired-network callback MID generation tracking, #256) |
| Scope | `src/mqttium/protocol/` only (engine, directional sessions, packet-id pool, flow control, negotiated settings, config, effects, reconnect); `codec/`, `packets/` and `persistence/memory.py` read where they are on an engine ingress/egress path. No tests, no `compat/`. |
| Method | Full direct read of every module under audit; the principal defect was reproduced by execution against the shipped engine before being fixed (reproducer in the finding). |
| Criteria | Simplification, stability, performance — all decisive; no over-engineering, no regression to established performance results. |
| Fixes | Findings E1–E3 are fixed in the same change, each with a regression test. E4, O1 and the simplification candidates are recommendations only. |

This is a dated report. It records what was true of commit `4677e55` on
2026-08-17. It is not a contract; do not edit it to match later code — write a
new report instead (see [`README.md`](README.md)).

## Baseline

The engine layer entered the audit in strong shape: strict `protocol/` ↔
asyncio separation holds (no asyncio, sockets, callbacks or wall clock under
`protocol/`), directional state has exactly one owner per side, the admission
transaction is snapshot/rollback-shaped, and the hot paths carry deliberate,
measured micro-optimisations (one-shot codec binding, deferred wire encode,
segmented writes, metadata-only acknowledgement settles, deferred auto-PUBACK
slot release). Performance review found **nothing worth changing without
measured evidence** — every candidate either added a Python frame to the
hottest path in the library or traded clarity for sub-1 % effects. The
findings below are therefore stability defects plus simplification
opportunities, not performance work.

## Findings index

| ID | Severity | Location | One-line |
| --- | --- | --- | --- |
| E1 | HIGH | `protocol/outbound.py` (`purge_after_clean_session`) | Flow-blocked WAIT_* entries in `_queued` survive a clean-session purge; `drain()` then re-materialises deleted records, double-releases the byte budget and kills the reconnect with a masked `AssertionError` |
| E2 | MEDIUM | `protocol/engine.py` (`handle_raw`) | Buffered trailing packets after a terminal state surface as `PROTOCOL_ERROR`, which the runtime lets overwrite the real disconnect reason |
| E3 | LOW | `protocol/engine.py` (`begin_connect`) | `begin_connect` accepted from `DISCONNECTING`, flipping the state machine to CONNECTING under a closing transport |
| E4 | LOW (hardening) | `protocol/engine.py` (`_validate_new_outbound_effects`) | A malicious CONNACK advertising `maximum_packet_size` below the mandatory ACK frame size silently drops the paired MESSAGE effect (loss without `PUBLISH_FAILED`) |
| O1 | Observation | `protocol/engine.py` (`handle_raw` except-all) | The internal-error catch-all converts invariant `AssertionError`s into `PROTOCOL_ERROR`, masking accounting bugs (this is exactly how E1 hid) |

---

## E1 — HIGH: stale `_queued` WAIT_* entries survive `purge_after_clean_session`

**Path.** `OutboundSession.replay_session()` re-queues records whose
retransmission the broker's Receive Maximum window could not admit, as their
current state — a blocked record is a `WAIT_PUBACK`/`WAIT_PUBREC` entry in
`_queued`, not a `QUEUED` one (`outbound.py:999-1001`). When a later CONNACK
reports Session Present 0 (the server-side session expired — a normal,
expected event), `purge_after_clean_session()` fails and deletes every
non-QUEUED store record but never touches `_queued`. The stale entries then
flow through `drain()` → `materialize()` → `_retransmit()` on records that no
longer exist.

**Reproduced by execution** (durable v5 session, 5 QoS 1 publications,
reconnect with `receive_maximum=2` leaving 3 blocked WAIT_PUBACK entries in
`_queued`, then a Session Present 0 CONNACK):

```
protocol error detail: ["Internal handler error: AssertionError(
    'outbound message reservation underflow')"]
```

**Consequences.**

- `drain()`'s failure branch releases the record's reservation a second time
  (`_release_reservation` underflow) and emits `PUBLISH_FAILED` for a record
  the purge had already failed — the double failure surfaces as a
  `PROTOCOL_ERROR` effect.
- At the runtime boundary (`api/async_client.py`, `PROTOCOL_ERROR` handling)
  the raised error aborts the just-negotiated connection and poisons
  `_disconnect_exc`: **losing the broker session kills the reconnect**, even
  though every publication was correctly reported failed and the engine state
  was perfectly consistent.
- Leftover stale entries re-poison every subsequent `drain()`.
- Worst variant (object-backed store, e.g. memory): before the record is
  deleted, `_retransmit()` can emit a DUP PUBLISH for a mid whose pool entry
  was just `clear()`ed — a mid that may immediately be re-allocated to a new
  publication, producing a duplicate-identifier collision on the wire.

**Fix (applied).** `purge_after_clean_session` now drops every non-QUEUED
entry from `_queued` before failing the records — those entries index exactly
the records being failed. One cold-path pass, once per clean CONNACK; the
success path and the pool `clear()` fast path are unchanged.

## E2 — MEDIUM: trailing buffered packets after DISCONNECT mask the disconnect reason

`handle_raw` ignored packets while `DISCONNECTING` (correctly: dispatching
them could emit ACKs or user-visible effects after the client's final
DISCONNECT) but, in `DISCONNECTED`, raised `Unexpected X while
state=DISCONNECTED`. The read loop decodes what the peer had already sent
after a broker DISCONNECT, so the resulting `PROTOCOL_ERROR` effect reached
the runtime, which (for engine state DISCONNECTED) overwrites
`_disconnect_exc` with the noise error — replacing the broker's actual reason
code and properties.

**Fix (applied).** `handle_raw` now ignores packets in every terminal state,
symmetric with the `DISCONNECTING` rationale. An existing test asserting the
old rejection (`test_disconnect_ingress.py`) was updated: no documentation
contract required the rejection, and it was the mechanism of the bug.

## E3 — LOW: `begin_connect` accepted from `DISCONNECTING`

The guard blocked `CONNECTED`/`CONNECTING` only. A `begin_connect()` racing
the final DISCONNECT's drain flipped the state machine to CONNECTING under a
transport that is about to disappear; the subsequent `notify_transport_closed`
then emits a spurious `DISCONNECTED` effect for a connection that never
existed.

**Fix (applied).** `DISCONNECTING` added to the guard; connecting again after
the transport actually closes remains legal (covered by the new regression
test).

## E4 — LOW, recommendation only: absurd broker `maximum_packet_size` degrades silently

A malicious CONNACK advertising `maximum_packet_size` below the size of the
smallest mandatory acknowledgement frame (4 bytes for a PUBACK) makes every
auto-ACK non-sendable: `_validate_new_outbound_effects` deletes the whole
handler batch — including the paired MESSAGE effect — while the Receive
Maximum slot and store row were already committed. The application loses the
message with no `PUBLISH_FAILED` and no error. **Recommended fix** (not
applied): reject at negotiation, as a protocol error, any broker
`maximum_packet_size` below the mandatory-ACK floor; fail fast at CONNACK
instead of degrading silently afterwards.

## O1 — Observation: the internal-error catch-all masks invariant failures

`handle_raw`'s blanket `except Exception` converts store/handler failures —
including invariant `AssertionError`s from the reservation accounting — into
`PROTOCOL_ERROR` ("Internal handler error"). That containment is deliberate
for store faults, but for `AssertionError` it converts a provable engine bug
into a peer-attributed disconnect; this is precisely the mechanism that hid
E1. Recommendation: let `AssertionError` propagate (fail-fast), keeping the
containment for `Exception` only. Not applied in this change to keep the fix
set minimal.

## Security review

Ingress parsing was reviewed end to end for attacker-controlled input
(malicious broker or MITM): VBI canonicality and 4-byte bound; MQTT UTF-8
rules (U+0000, surrogates via the encode/decode path, 65535-byte bound);
per-packet property whitelist with duplicate detection; reason-code
whitelists; fixed-header flag validation; QoS 3 and DUP-on-QoS-0 rejection;
topic-alias bounds with DISCONNECT 0x94; Receive Maximum enforcement with
DISCONNECT 0x93; inbound byte quota with DISCONNECT 0x97; 16 MiB decoder
ceiling before negotiation; `password` excluded from `repr`. No injection,
exhaustion or information-disclosure vector was found. The only
attacker-influenced defects identified are E2 (reason masking) and E4
(silent loss), both addressed above. One pre-existing strength worth noting:
`_reject_packet_id_collision` refuses, rather than tolerates, mid reuse
across live exchanges — the only answer that neither corrupts local
accounting nor loses a message silently.

## Performance verdict

No change proposed. Candidates examined and rejected, each for a measured or
architectural reason: inlining `validate_raw_packet` into `handle_raw`
(duplication for one call on a non-hot path); merging the duplicated `Message`
construction between `_on_qos1_auto`'s two branches (risks the documented
~1.5 %/frame hot-path cost); touching the effect pipeline's SEND-first
partition (protected by `EffectStats` decision counters and
`docs/reports/PERFORMANCE-AUDIT-0.2.0b4.md`). The applied fixes touch only
cold paths (once per CONNACK / per terminal packet) and add no hot-path work.

## Simplification candidates (not applied)

1. **Engine compatibility facades** (`engine.py`, `packet_ids` / `flow` /
   `_queued` / `_topic_aliases` / `_inbound_inflight` / …, ~80 lines): pure
   test/benchmark/fuzzer compatibility views over `engine.outbound` /
   `engine.inbound`. Internal tier — migrating the callers then deleting the
   facades is safe and the largest single simplification available in the
   engine.
2. **Three near-identical MESSAGE-emission blocks** in
   `InboundSession._on_qos1_auto` (×2) and `_on_qos1_manual`: a shared helper
   for the cold duplicate-in-batch branch only would keep the hot branch
   untouched.
3. **`_check_publish_size` / `publish_wire_size` / `size_parts` overlap**:
   `_check_publish_size` has exactly one internal caller and could be inlined
   into `validate_against_negotiated`.

## Verification

All fixes were applied with regression tests and the full quality gate run at
the fix commit: 1170 unit tests pass, coverage 90.21 % (gate 80 %),
`ruff format --check`, `ruff check`, `mypy src/mqttium`, `bandit -q -ll -r src`
all clean, Hypothesis fuzz suite passes.
