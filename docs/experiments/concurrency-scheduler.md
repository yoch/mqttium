# Cooperative concurrency scheduler (prototype)

Baseline: `main@a336f834c66c4b4cb1a612e7c064ae29bb51cb7b` (1.0.0rc9).

Status: **open prototype**. This is test-only infrastructure for exploring
asyncio interleavings in `AsyncClient`, `WritePump`, `EffectPump`,
`ApplicationDelivery`, reconnect, cancellation, and bounded queues. It is not
security fuzzing and it does not replace the protocol engine tests.

See the dated measurement note
[`docs/reports/CONCURRENCY-SCHEDULER-2026-08-24.md`](../reports/CONCURRENCY-SCHEDULER-2026-08-24.md).

## Problem

MQTTium already has excellent *protocol* determinism: `ProtocolEngine` is a
synchronous state machine, and unit tests can drive it without a broker. The
remaining instability lives in the asyncio runtime:

- several tasks (`mqttium-writer`, `mqttium-reader`, `mqttium-effect-flush`,
  `mqttium-callback-worker`, `mqttium-reconnect`, plus application producers);
- named await points that are *not* equivalent to `asyncio.sleep(0)`;
- adversary events (transport close, write failure, late ACK, cancel) that
  should be injectable at those points;
- failures that must shrink to a compact, printable schedule and replay as a
  normal pytest.

Random yielding cannot name "close the transport after the writer extracted a
batch but before `transport.write` returns". That is the interesting class of
bug.

## Design

The prototype is a **cooperative named-checkpoint scheduler**. It does not
replace the event loop.

1. Tests monkeypatch selected *async* methods on a live `AsyncClient` and its
   pumps. Each wrapper awaits `scheduler.checkpoint(name)` before and/or after
   the original call.
2. When the scheduler is armed and `name` is in the enabled set, the calling
   task parks on a private `Future` and records `(task, checkpoint, occurrence)`.
3. A single driver task waits until at least one task is parked, then either
   resumes one parked task or fires a one-shot adversary action
   (`close_transport`, `fail_write`, `inject_puback`, `inject_inbound`, or a
   scenario-specific cancel).
4. The sequence of decisions is a `Schedule`. It is printable, parseable, and
   replayable. After the interesting prefix is exhausted, remaining parks drain
   in arrival order so the scenario can finish.
5. Bounded timeouts turn deadlocks into explicit failures that include the
   schedule so far.

Production hot paths are unchanged. Instrumentation lives under
`tests/concurrency/` and is installed per client after construction.

### Why not a custom event loop?

A selector loop that records every `call_soon` / task wakeup can in principle
replay *all* asyncio interleavings. In practice those schedules are huge,
Python-version brittle, and mostly uninteresting (internal `Lock` fairness,
`Queue.get` bookkeeping, keepalive `sleep(1)`). MQTTium's bugs are at *named
runtime boundaries*. The checkpoint model matches that vocabulary.

### Why not production hooks?

The interesting await points already exist (`WritePump.enqueue`,
`WritePump._run`'s `queue.get` and `transport.write`, `EffectPump.drain`,
`ApplicationDelivery.put_message`, `_invalidate_connection_epoch`,
`_reconnect_loop`). Monkeypatching those methods from tests is enough to park
around them. Synchronous work under `_engine_lock` is intentionally *not*
split; see limitations.

## Checkpoint catalog

| Name | Boundary |
| --- | --- |
| `publish.enter` / `publish.leave` | `AsyncClient.publish` |
| `publish.wait_space` | parked logical admission waiter |
| `writer.enqueue.before` / `.after` | writer admission |
| `writer.enqueue.wait` | `WritePump.space.wait` under backpressure |
| `writer.batch.extract` | after `queue.get` in the writer task |
| `transport.write.before` / `.after` | `write` / `write_many` |
| `transport.close.before` / `.after` | transport close |
| `effect.drain.before` / `.after` | `EffectPump.drain` |
| `effect.apply.before` / `.after` | `AsyncClient._apply_effect` |
| `delivery.put.before` / `.after` | iterator queue slow-path |
| `delivery.callback.before` / `.after` | callback worker `invoke` |
| `epoch.invalidate.before` / `.after` | connection epoch bump |
| `reconnect.loop.before` | start of `_reconnect_loop` |
| `client.disconnect.enter` / `.leave` | `disconnect()` |

Synchronous events such as `effect.collect` are traced but not preemptible.

## Schedule format

```text
# policy=explicit seed=7
action close_transport
resume mqttium-writer @ transport.write.before #1
resume publish_a @ writer.enqueue.after #1
```

Replay:

```python
result, _ = await run_connected_scenario(
    publish_admit,
    enabled=frozenset({"transport.write.before"}),
    schedule=Schedule.parse(text),
)
```

## Exploration policies

| Policy | Use |
| --- | --- |
| `explicit` / replay | Regression: a compact failing schedule becomes a focused pytest |
| `first` | Deterministic drain; captures one canonical schedule for replay |
| `dfs` | Bounded tree over resume-vs-action choices; state-space evidence |
| `random` | Seeded campaign; same seed must repeat unique-schedule counts |

Adversary actions are offered only while at least one checkpoint is parked, so
a schedule names an interleaving rather than "close the socket at a random
idle moment". Each action is one-shot per run.

## What this fundamentally cannot explore

Be skeptical of the coverage claim. The following interleavings are out of
reach without production changes or a different model:

1. **Synchronous critical sections.** Admission, receipt registration, inline
   SEND, and `try_enqueue` run under `_engine_lock` without an await. The
   prototype cannot insert a disconnect *between* `queue_publish` and
   `_register_publish_receipt`. That window is also the "receipts before wire"
   invariant: if a test needed to split it, the product would already be
   wrong. Treat the section as atomic.
2. **Eager `write_nowait`.** The in-memory broker has no `write_nowait`, so
   the zero-hop writer path is not exercised here. Enabling it would require a
   transport double that can fail *during* a non-awaiting append.
3. **True parallel threads.** The Paho façade's network thread is out of
   scope. This harness is asyncio-only.
4. **OS scheduling and kernel socket buffers.** `ControllableBroker` is an
   in-memory queue. It cannot reproduce TCP half-close, partial writes, or
   `EAGAIN` splitting a segmented frame across two `await transport.write`
   calls unless a future double models that explicitly.
5. **Time.** Keepalive, ACK timeouts, and reconnect jitter are not virtualized.
   Tests set `keepalive=0` and `ReconnectPolicy(initial_delay=0, max_delay=0)`.
   The harness cannot explore "PINGRESP expires while a callback is running"
   without a fake clock.
6. **Uninstrumented awaits.** `_read_loop`'s `await asyncio.sleep(0)` between
   ingress batches, `asyncio.Lock` fairness, and `Queue.get` on the reader
   remain under the real event loop. If a bug lives only in those hops, this
   scheduler will not name it.
7. **Callback-initiated `publish()` re-entrancy** only if the callback path is
   enabled and the test actually publishes from `on_message`. The checkpoint
   around `delivery.invoke` can park the worker, but nested `publish()` still
   uses the same cooperative rules; it cannot preempt inside the engine lock.
8. **Partial-order reduction quality.** DFS here is naive (every parked task
   plus every remaining action). It is not DPOR, not sleep-set, not PCT with
   a proven bound. Branching grows with enabled checkpoints × concurrent
   tasks × adversary actions. Depth and schedule caps are mandatory.

## Integration recommendation

Mirror the existing fuzz split rather than dumping exploration into the 30
second unit job.

| Layer | What runs | Where |
| --- | --- | --- |
| Fast | Scheduler unit tests + a few replayed mqttium schedules | `tests/concurrency/test_*.py` today; later a CI job next to `tests/resilience` |
| Replay | Any failing campaign schedule copied into a focused pytest | `tests/concurrency/` or a regression under `tests/unit` if it pins a product bug |
| Campaign | Seeded `dfs` / `random` with an explicit budget | `python tests/concurrency/explore.py --seed N --max-schedules K` |
| Soak / nightly | Larger depths, more checkpoint sets, multiple seeds | Next to `tests/resilience` and `benchmarks/fuzz_campaign.py`, not in PR CI |

Do **not** add production `await scheduler.checkpoint()` calls. If a future
bug can only be split inside a synchronous section, prefer a tiny, documented,
test-gated hook over polluting the hot path. The first product bug this
harness finds should land as a deterministic pytest; a production fix is a
separate change.

Coverage.py's 89% unit/project gate should ignore this tree until a dedicated
job exists. These tests import mqttium but they are runtime explorations, not
the branch-coverage corpus.

## How to extend a scenario

1. Choose the smallest checkpoint set that names the race.
2. Connect with the scheduler disarmed so CONNECT/CONNACK is not in the
   schedule.
3. Spawn the application actors and let the driver arm.
4. Capture the `FirstChooser` schedule, then edit it into the interesting
   prefix (`action close_transport` before `resume ... write.before`).
5. Replay that prefix as a pytest. If it flakes, a checkpoint is missing or
   the first park is still event-loop ordered — shrink the enabled set.
