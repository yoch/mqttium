# Pump concurrency red-team (2026-08-24)

Starting SHA: `a336f834c66c4b4cb1a612e7c064ae29bb51cb7b` (`origin/main`, 1.0.0rc9).

Scope: EffectPump, WritePump, AsyncClient interaction, cancellation,
transport failure, shutdown, concurrent drains/waiters, connection epochs.
No security fuzzing. Adjacent to already-fixed races (eager unbind on writer
failure, EffectPump drain/failure ownership, cancelled-waiter wakeup handoff).

## CONFIRMED BUG

### W1. Writer failure leaves enqueue waiters parked

**Shortest schedule**

1. `WritePump(max_messages=1)` starts on a transport whose `write()` waits.
2. `enqueue(b"hold")` is admitted and occupies the in-flight slot.
3. `enqueue(b"blocked")` parks on `space` (`waiters == 1`).
4. The in-flight write raises `ConnectionResetError`.
5. `_run` releases resident bytes in `finally`, then `await on_failure(exc)`
   with `batch_completed=False`, so it does not `space.notify`.
6. `on_failure` (and `AsyncClient._writer_failed`) neither notifies waiters
   nor advances the epoch.

**Oracle:** waiter never completes; `waiters` stays 1.

**Reproduction:** `test_writer_failure_unblocks_enqueue_waiters`.

### W2. Reader-owned PUBACK enqueue deadlocks after writer failure

**Shortest schedule**

1. Connect; `max_outbound_messages=1`.
2. QoS 0 publish occupies the writer slot; `write()` is held.
3. Inbound QoS 1 PUBLISH is injected. The reader applies a PUBACK SEND and
   parks in `WritePump.enqueue`.
4. The held write fails. `_writer_failed` only closes the transport.
5. The reader is the parked waiter, so it never reaches read-loop `finally`
   (epoch bump / `wake_waiters` / stop).

**Oracle:** `on_disconnect` never fires; connection never reaches quiescence
unless another task calls `disconnect()`.

**Reproduction:** `test_writer_failure_does_not_deadlock_reader_puback_enqueue`.

This is W1 at the AsyncClient boundary: the reader is the waiter that W1
strands.

### E1. Effect collect during failing close orphans a later drain

**Shortest schedule**

1. EffectPump applies a failing SEND; waiters of that failure are woken.
2. Remaining effects are discarded (`applied == enqueued`).
3. `_run_scheduled` still holds `lock` while awaiting
   `_close_transport_after_connection_failure`.
4. Another task `collect_from_engine()` of a new SEND (increments `enqueued`,
   leaves it in `pending`).
5. `drain()` sees `applied < target`, `drain_inline` bails because
   `lock.locked()`, `schedule()` sees the flush task still running, then
   waits on `progress`.

**Oracle:** later `drain()` hangs until close completes, and if close never
completes it hangs forever. Pending work is unreachable while the lock is held.

**Reproduction:** `test_effect_failure_close_does_not_orphan_later_drain`.

Adjacent to the quiesce-before-close fix: that fix made *already queued*
trailing effects unreachable-wait-free, but not effects collected *during*
the close await.

## PLAUSIBLE BUT UNPROVEN

- QoS 0 `publish()` returning success after epoch discard of its SEND during
  reader teardown (`drain()` treats discard as `applied` progress). Engine
  `prepare_qos0` rejects DISCONNECTED, but a window exists after epoch bump
  and before `notify_transport_closed`.
- `asyncio.shield(progress.wait())` in `EffectPump.drain` orphans Event
  waiters on cancel. Harmless with the `applied >= target` recheck, not
  shown to hang.
- `reset()` while a writer task still holds a local `queue = self.queue`
  reference. Client reconnect `force_close`s first; unproven without that.

## SAFE BY CONSTRUCTION

- Eager write vs segmented write: `_writing` is set before the first await
  of a batch; no await between `queue.get()` return and `_writing = True`.
- Latency-batch restore vs `queue.join()`: `join()` rechecks
  `_unfinished_tasks` after wakeup; `put_nowait` clears `_finished`.
- Cancelled `space` waiter handoff: `notify(1)` when `waiters > 1`.
- SEND apply after epoch bump: enqueue compares captured epoch to
  `WritePump.epoch` and raises `StaleConnectionEffect`.
- Direct QoS 0 path: `prepare_qos0` → `NotConnectedError` when the engine
  is not CONNECTED.

## TEST GAP

- WritePump used without AsyncClient: `stop()` does not wake waiters
  (client `force_close` does). Contract is client-owned.
- Hung `transport.close()` that ignores cancellation (malicious, not
  normal operations).
- `join()` after writer death with leftover queued items (client discards
  on `force_close`).
- Concurrent `drain()` cancellation vs `shield` orphaned Event waiters
  (likely safe; not pinned).
