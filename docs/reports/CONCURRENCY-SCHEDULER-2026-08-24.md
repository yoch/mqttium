# Cooperative concurrency scheduler prototype

Date: 2026-08-24

Baseline: `a336f834c66c4b4cb1a612e7c064ae29bb51cb7b` (1.0.0rc9)

Branch: `cursor/research-concurrency-scheduler-ec70`

Status: **Current evidence** for an open prototype. This is not release
evidence and not a CI gate.

Maintained design:
[`docs/experiments/concurrency-scheduler.md`](../experiments/concurrency-scheduler.md).

## Goal

Name, replay, and bound-explore asyncio interleavings around `AsyncClient`,
`WritePump`, `EffectPump`, `ApplicationDelivery`, reconnect, cancellation, and
bounded queues — without mutating production hot paths and without treating
malformed packets as in-scope.

## Method

- Cooperative named checkpoints installed by monkeypatching async methods on a
  live client (`tests/concurrency/instrument.py`).
- A single driver resumes one parked task or fires a one-shot adversary action
  (`tests/concurrency/scheduler.py`).
- In-memory `ControllableBroker` with optional held ACKs
  (`tests/concurrency/broker.py`).
- Replay is a compact printable `Schedule`. DFS/random campaigns are budgeted
  (`tests/concurrency/explore.py`).

Commands:

```bash
python -m pytest -q tests/concurrency
PYTHONPATH=. python tests/concurrency/explore.py --policy dfs --max-schedules 24 --max-depth 3
PYTHONPATH=. python tests/concurrency/explore.py --policy random --seed 7 --max-schedules 6
```

## Measurements

Filled from the prototype run on this branch after the tests below. Unique
schedules count distinct printable `Schedule.format()` strings, not raw DFS
queue nodes.

| Campaign | Budget | Unique schedules | Timeouts | Deadlocks | Unexpected errors |
| --- | --- | --- | --- | --- | --- |
| DFS write-boundary, one QoS 1 admit | 24 runs, depth 3 | (pending) | (pending) | (pending) | (pending) |
| Random seed 7, same scenario | 6 runs | (pending) | (pending) | (pending) | (pending) |

Practical reading: one publisher, one enabled write checkpoint, and four
one-shot adversary actions already produces a branching factor around "resume
plus remaining actions". Depth 3 is enough to show the tree is finite and
small; enabling `writer.enqueue.*`, `effect.apply.*`, and
`delivery.callback.*` together is not a PR-CI activity.

## Demonstration scenarios

| Test | Boundary | What it shows |
| --- | --- | --- |
| `test_qos1_publish_completes_when_writer_is_released` | `transport.write.before` | Canonical drain is replayable |
| `test_close_before_write_is_replayable` | close then write | Compact schedule: suspend, fail transport, resume |
| `test_cancel_waiter_under_writer_backpressure` | enqueue wait + cancel | Cancellation of a parked producer |
| `test_delivery_queue_park_then_shutdown` | iterator slow-path | Delivery admission vs shutdown |
| `test_callback_worker_handoff_then_disconnect` | callback invoke | Worker handoff vs disconnect |
| `test_reconnect_after_close_at_write_boundary` | write + reconnect loop | Epoch/reconnect after a suspended write |

## Recommendation

Keep this tree next to resilience/fuzz:

- PR CI: scheduler unit tests + a handful of replayed schedules (fast).
- Nightly/soak: seeded DFS/random with explicit `--max-schedules` and checkpoint
  sets.
- A product bug found by a campaign becomes a focused pytest that replays one
  schedule. Do not "fix it in the harness".

Do not add production checkpoints unless a bug is proven to live inside a
synchronous section that monkeypatching cannot split.

## Limitations recorded with the prototype

See the design note. The headline ones: atomic `_engine_lock` sections, no
eager `write_nowait`, no fake clock, no DPOR, in-memory transport only.
