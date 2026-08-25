# Runtime concurrency fuzzer: adversarial model audit

- Date: 2026-08-25
- Audited base: `78c8d4caddacf80d77382a67651174a6a9c8a6f5` (merge of PR #381, `codex/runtime-concurrency-fuzzer-composition`)
- Agent: runtime concurrency fuzzing (exclusive ownership)
- Scope: expressiveness of generated schedules, scheduling/release decisions,
  concurrency windows, runtime ownership oracles, generation isolation oracles,
  reduction/replay fidelity, missing runtime interleavings.

This report records an adversarial audit of the fuzzing *model* itself: which
classes of real runtime concurrency bug the V1 generative fuzzer
(`tests/fuzz/runtime_fuzzer.py`) and the V2 two-window composition target
(`tests/fuzz/runtime_composition_fuzzer.py`) structurally cannot find, rarely
find, or observe without recognizing as a failure. It is immutable historical
evidence for the exact audited SHA, not a maintained runtime contract.

The three companion probe scripts (`tests/fuzz/audit_eager_write_surface.py`,
`tests/fuzz/audit_duplicate_write_oracle.py`, `tests/fuzz/audit_publish_waiter.py`)
reproduce every experiment against the audited base. They are runnable as
standalone scripts and deliberately not collected by pytest: two of them assert
current *coverage deficiencies*, which must break the moment the deficiency is
fixed.

## Capability map

| Dimension | V1/V2 coverage |
| --- | --- |
| Transport write surface | `write`/`read`/`close` only. No `write_nowait`, no `write_many`, so the eager-write, latency-batch, segmented-write and multi-frame batching paths are dead (`eager_writes=0`, `segmented_writes=0`, `_write_nowait=None` measured). |
| Wire oracle | Checkpoints assert `sum(count(t)) >= target` (cumulative). No exact-multiplicity or terminal outbound-count oracle. |
| QoS breadth | 0 and 1 only. No QoS 2 (PUBREC/PUBREL/PUBCOMP), no session replay (`CONTINUE_INBOUND_REPLAY` never produced). |
| API surface | `connect`/`disconnect`/`publish` only. No subscribe/unsubscribe/auth/manual_ack/publish_many/publish_nowait. |
| Delivery | `message_delivery="callback"`, tiny payloads: `pending_bytes` always 0; iterator queue, `try_reserve`/`reserve_slow`, `_SharedDeliveryReservation`, `reset_stream` never exercised. |
| Producer parking | Engine pending-message/byte limits never saturated: `_publish_waiters` dead (measured 0 over 24 seeds). No terminal oracle on `stats.receipts.publish_waiters`. |
| Callback model | Callback can `block`/`cancel`/`raise`/`disconnect`/`connect` but never `publish`; `on_publish` and `on_connect` never set. No re-entrant publication, no publish-completion callback path. |
| Timers | V1 `keepalive=0`; V2 synthesizes the keepalive timeout by mutating `_ping_pending`/`_ping_deadline` and re-creating the task. `ping_timeout`/`ack_timeout`/`delivery_timeout`/drain timeouts never fire naturally. |
| Sizing | `max_outbound_messages=1`, `max_outbound_bytes=4096`, `max_pending_callbacks=4`. Writer queue saturation, the latency-batch threshold (>=4 frames) and delivery/ingress saturation are never reached. |
| Scheduling granularity | `asyncio.sleep(0)` turns plus a fixed four-turn settle after each operation; V2 makes release order a first-class dimension but only up to two windows at depth 2. |
| Oracles present | Writer resident/byte accounting; dead-writer waiter; effect `applied <= enqueued` and `pending == enqueued - applied`; failing-close `pending == 0`; outbound ledger/packet-id/flow/receipt agreement; inbound Receive Maximum; completed-write epoch isolation; terminal waiter/effect/byte/receipt/task settlement; application-task and loop-exception checking; whole-schedule watchdog. |

## Ranked blind spots

1. **Write surface absent.** The harness transport omits `write_nowait`/`write_many`,
   so the eager-write, latency-batch, segmented-write and multi-frame batching
   paths (the load-bearing single-writer hot paths in `_writer.py`) are
   structurally unreachable in the runtime target.
2. **No outbound-multiplicity oracle.** Wire checkpoints use `>=`; a duplicate
   PUBLISH write passes every oracle (demonstrated by probe B).
3. **Delivery accounting dead.** `pending_bytes`, reservations, token rollback
   and the iterator queue are never exercised; the "pending accounting" and
   "queue bounds" invariants are untested at runtime.
4. **Producer-parking dead + missing `publish_waiters` terminal oracle.** Engine
   flow/pending limits never saturate; a leaked parked producer would be
   invisible (or a coincidental liveness hit).
5. **QoS 2 + inbound session replay absent.** PUBREL/PUBREC/PUBCOMP and
   `CONTINUE_INBOUND_REPLAY` are never generated.
6. **No subscribe/unsubscribe/auth/manual_ack.** SUBACK/UNSUBACK futures, the
   AUTH handler and manual-ack `mark_delivered` are never reached.
7. **Re-entrant publication absent.** A callback can disconnect/connect but never
   publish; `on_publish`/`on_connect` are never set, so the "callback-initiated
   publication must not deadlock" invariant is untested at runtime.
8. **Natural timers collapsed.** Real keepalive/ping/ack/delivery/drain timeouts
   never fire; "timeout + transport failure" is untested.

## Experiments performed

All experiments ran against the unmodified production code on the audited base.

- **A — write-surface coverage + eager generation isolation.** Part 1 measures
  the eager/latency-batch/segmented counters under the unmodified fuzzer. Part 2
  drives the real `AsyncClient` against a `write_nowait`-capable transport
  through connect -> eager publish -> EOF teardown -> reconnect -> eager publish
  and asserts per-transport epoch ownership.
- **B — duplicate-write oracle gap.** A mechanical, test-only "duplicate PUBLISH
  write" mutation is installed in the harness transport and the unmodified V1
  oracles are run against it.
- **C — producer-parking coverage + teardown accounting.** Part 1 confirms
  `_publish_waiters` stays zero across a fuzzer campaign. Part 2 drives the real
  client to park a producer on the pending-message limit and tears down.

## Confirmed bugs

None. No production defect was demonstrated; this is a strong negative result.
The existing oracles did not conceal a live bug that these probes could reach.

## Fuzzer deficiencies without a demonstrated production bug

- **Duplicate-write oracle gap (B).** A transport transmitting a PUBLISH twice
  yields `failure=None`; `completed_packets=[CONNECT, PUBLISH, PUBLISH,
  DISCONNECT]` and every oracle passes. Any future duplicate/extra-write
  regression (latency-batch or replay double-emit) would be silently accepted.
- **Eager/latency-batch/segmented write path is 100% dead (A).** `eager_writes=0`,
  `_write_nowait=None`, `segmented_writes=0` under the runtime target, despite
  unit coverage in `tests/unit/test_write_pump_eager.py` — never under real-client
  teardown/reconnect interleavings.
- **Producer-parking path dead + un-oracled (C).** `_publish_waiters` never
  rises; `stats.receipts.publish_waiters` has no terminal oracle.

## Falsified hypotheses

- "A stale eager-write re-arm can cross a reconnect generation" -> **falsified**.
  Generation 1 writes all carry epoch 2, generation 2 all epoch 5; no shared or
  cross-generation write. The `_eager_generation` guard holds.
- "A producer parked on flow control leaks at teardown" -> **falsified**. The
  parked producer settles with `MQTTError("Connection closed")` and
  `_publish_waiters` returns to 0.

## Exact commands and results

```text
# base sanity (worktree /tmp/mqttium-deepseek-concurrency-fuzzer, venv /tmp/venv-mqttium)
PYTHONPATH=src:. python tests/fuzz/runtime_fuzzer.py --seed 0 --seeds 12 --steps 24
  -> failures=0, operation_traces=12, scheduling_traces=12
python -m pytest tests/fuzz/test_runtime_fuzzer.py tests/fuzz/test_runtime_composition_fuzzer.py -q
  -> 34 passed

# Probe A
PYTHONPATH=src:. python tests/fuzz/audit_eager_write_surface.py
part1: eager_writes=0  eager_bytes=0  _write_nowait=None  segmented_writes=0  batched_items=4
part2: generation-1 eager writes=2, epochs=[2]; generation-2 epochs=[5] -> CLEAN

# Probe B
PYTHONPATH=src:. python tests/fuzz/audit_duplicate_write_oracle.py
failure=None  completed_packets=[CONNECT, PUBLISH, PUBLISH, DISCONNECT]  PUBLISH writes=2
VERDICT: ACCEPTED (oracle gap confirmed)

# Probe C
PYTHONPATH=src:. python tests/fuzz/audit_publish_waiter.py
part1: max _publish_waiters across 24 seeds = 0
part2: _publish_waiters while parked=1 ; parked publish settled="raised MQTTError: Connection closed"
       _publish_waiters after teardown=0
```

## Handoff candidates

- **Persistence / replay agent:** QoS 2 (PUBREC/PUBREL/PUBCOMP) and
  `CONTINUE_INBOUND_REPLAY` are entirely absent from the runtime fuzzer —
  inbound session replay ownership (`_effects.py` `EffectKind.CONTINUE_INBOUND_REPLAY`
  branch, `outbound.replay_session`) is never exercised by any runtime schedule.
- **MQTT compliance / cancellation agent:** SUBSCRIBE/UNSUBSCRIBE/AUTH/manual-ack
  effect paths are absent (`_sub_futs`/`_unsub_futs` always empty;
  `EffectKind.AUTH` in `async_client.py:_apply_effect` never reached; manual-ack
  `mark_delivered` never reached).
- **Writer/perf ownership:** `_writer.py:_try_flush_latency_batch` asserts
  `queued_bytes == 0` after a full-queue flush, but the path needs `write_nowait`
  plus 4-16 queued frames, both absent from the fuzzer; the invariant is only
  reachable at default sizing.

## Single highest-value next improvement

Add an exact-multiplicity outbound-wire oracle and give the harness transport the
real write surface. Concretely: (a) change the wire checkpoint from cumulative
`>= target` to "the scheduled outbound packet appears exactly once" (and assert
the exact total at terminal), converting the demonstrated duplicate/extra-write
class from silently-accepted into detected failures; and (b) have
`_ScheduleTransport` expose `write_nowait` (and `write_many`) so the eager-write,
latency-batch and segmented-write paths become reachable under
teardown/reconnect interleavings. Both are small, targeted changes: (a) is the
correctness fix and (b) is the coverage unlock, together addressing the two most
fundamental structural gaps (blind spots #1 and #2) without expanding the grammar
or adding a third ownership window.
