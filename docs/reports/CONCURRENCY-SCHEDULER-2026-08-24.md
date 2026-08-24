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

Host for the numbers below: the Cloud Agent workspace that ran this prototype
(`python 3.12.3`, in-memory broker, no Mosquitto). Unique schedules count
distinct printable `Schedule.format()` strings, not raw DFS queue nodes.

## Measurements

| Campaign | Budget | Unique schedules | Timeouts | Deadlocks | Unexpected errors | Elapsed |
| --- | --- | --- | --- | --- | --- | --- |
| DFS write-boundary, one QoS 1 admit | 24 runs, depth 3 | 16 | 0 | 0 | 0 | ~3.6 s (pytest) |
| DFS write-boundary, one QoS 1 admit | 80 runs, depth 5 | 43 | 0 | 0 | 0 | 12.1 s |
| DFS enqueue+write, one QoS 1 admit | 40 runs, depth 4 | 24 | 0 | 0 | 0 | 6.0 s |
| Random seed 7, write-boundary | 6 runs | 5 | 0 | 0 | 0 | ~1.8 s (pytest pair) |
| Random seed 1, write-boundary | 20 runs | 15 | 0 | 0 | 0 | included in 12 s batch |

Mean branching on the depth-3 write-boundary tree was **3.86**. That is
roughly "resume the parked writer, or fire one of the remaining one-shot
actions". Depth 5 still grew (16 → 43 unique under a 80-run cap), so the tree
is not collapsed; it is just small enough to finish in seconds when the
enabled set is one or two named boundaries.

Practical reading:

- A focused replay of 1–6 checkpoints is a normal pytest (this tree's 13 tests
  ran in 8.05 s after shrinking the idle budget to 150 ms).
- A nightly campaign of a few hundred schedules is cheap.
- Enabling `writer.enqueue.*`, `effect.apply.*`, `delivery.callback.*`, and
  `epoch.invalidate.*` together is not a PR-CI activity. Naive DFS is not
  DPOR; branching is `parked tasks + remaining actions` at every step.

No product invariant failure was reduced from this first campaign. Close,
failed write, and late ACK are treated as expected `MQTTError` /
`ConnectionError` outcomes unless the run times out, deadlocks, or raises
`AssertionError`.

## Sample schedules

Canonical QoS 1 drain, `FirstChooser`, only `transport.write.before` enabled:

```text
# policy=explicit
resume mqttium-writer @ transport.write.before #1
```

The same publisher, suspend-then-fail-transport prefix:

```text
# policy=explicit
action close_transport
resume mqttium-writer @ transport.write.before #1
```

Both prefixes replayed identically in
`tests/concurrency/test_demo_scenarios.py`.

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
