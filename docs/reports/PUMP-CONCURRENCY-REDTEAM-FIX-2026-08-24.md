# Pump concurrency red-team fix (2026-08-24)

Follow-up to [PUMP-CONCURRENCY-REDTEAM-2026-08-24](PUMP-CONCURRENCY-REDTEAM-2026-08-24.md).
That report remains the bug schedules at `a336f834c66c4b4cb1a612e7c064ae29bb51cb7b`.
This note records the minimal liveness fixes.

## Confirmed bugs closed

### W1 / W2 — writer failure stranded enqueue waiters

`WritePump._run` now calls `advance_epoch(self.epoch + 1)` after dropping the
eager binding and before `on_failure`. Resident capacity from the failed batch
is no longer offered as admission space: waiters observe a stale epoch and
raise `StaleConnectionEffect`.

The client epoch is still advanced only on the reader-owned teardown path.
Bumping it from the writer task would race `advance_epoch`'s assignment with
reader `finally`.

**Schedule (W1):** `max_messages=1` → enqueue held write → park second enqueue →
write fails → waiter completes (stale), `waiters == 0`.

**Schedule (W2):** inbound QoS 1 PUBLISH while the only writer slot is held →
reader parks in PUBACK `enqueue` → held write fails → `on_disconnect` fires
without an external `disconnect()`.

### E1 — collect during failing close orphaned a later drain

`EffectPump` sets `_failing_close` before awaiting transport close. Collect in
that window extends then `discard_connection_effects(settle_publish=True)`, so
`enqueued`/`applied` stay aligned and `drain()` cannot wait on work the flush
will never apply. A `finally` discard covers collects that land after close
returns but before the lock is released.

**Schedule:** failing SEND → close held → `collect_from_engine` of a new SEND →
later `drain()` completes while close is still held.

## Ranking after the fix

- **CONFIRMED BUG:** none remaining from this campaign (W1, W2, E1 closed).
- **PLAUSIBLE BUT UNPROVEN:** unchanged from the original report (QoS 0 success
  after SEND discard; `shield` Event waiter orphans; `reset()` vs local queue).
- **SAFE BY CONSTRUCTION:** unchanged.
- **TEST GAP:** unchanged (`stop()` without client; hung `close()`; `join()`
  after writer death; concurrent `drain()` vs `shield`).
