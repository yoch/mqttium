# Runtime soak and quiescence

The unit suite proves short, deterministic correctness. This harness searches
for lifecycle defects that appear only after many mixed operations: leaked
tasks, receipts, packet identifiers, writer occupancy, delivery queues, and
reconnect/shutdown failures.

Process RSS is diagnostic only. Python's allocator makes RSS a noisy leak
oracle. The failing checks are logical-ownership snapshots.

## What it exercises

Valid MQTT application behaviour only. There is no protocol-mutation fuzzing.

Each seed builds a mixed schedule of:

- connect, subscribe, unsubscribe;
- QoS 0/1/2 `publish`, `publish_nowait`, and `publish_many`;
- publish cancellation;
- callback and `messages()` iterator consumption;
- slow and fast callbacks;
- forced network loss with session present or absent;
- drain and quiescence checkpoints;
- graceful and forced shutdown.

The default backend is an in-memory packet-aware broker. Mosquitto is optional
confirmation (`--backend mosquitto`).

## Logical ownership

At each `quiesce` checkpoint the harness snapshots:

- named `mqttium-*` tasks and per-task running flags;
- publish/subscribe/unsubscribe receipts and futures;
- writer queued/resident messages and bytes;
- pending/applied effects and waiters;
- delivery iterator/callback queues and pending bytes;
- packet identifiers and inflight store rows.

Connected idle requires those counters at zero, reconnect and effect-flush
tasks stopped, and `CONNECTED` state. Disconnected idle also requires every
`mqttium-*` task to have finished. Later connected-idle snapshots are compared
with the first successful baseline; monotonic epoch/reconnect counters are
ignored.

## Profiles

| Profile | Operations | Seeds | Typical use |
| --- | --- | --- | --- |
| `ci` | 64 | 1, 7 | Pull requests and the soak workflow fake-broker job |
| `local` | 8 000 | 1, 7, 13, 42 | Workstation campaign before nightly |
| `nightly` | 50 000 | 1, 7, 13, 42, 99, 256 | Overnight fake-broker search |
| `release` | 200 000 | 1, 7, 13, 42, 99, 256, 1024 | Release-evidence artefact |

Both MQTT 3.1.1 and MQTT 5 run unless `--protocol` selects one. Failures reduce
the schedule by binary prefix search then greedy deletion.

## Commands

```bash
PYTHONPATH=src python benchmarks/runtime_soak.py --profile ci
PYTHONPATH=src python benchmarks/runtime_soak.py --profile local --seed 7 --protocol 5
PYTHONPATH=src python benchmarks/runtime_soak.py --profile ci --backend mosquitto --port 11883
```

A failing run prints `history` and `reduced`. Replay the reduced labels in
order with the same seed, protocol, and profile timeout.

## Nightly and release use

- Pull requests: `ci` on the fake broker (also covered by
  `tests/unit/test_runtime_soak.py`).
- Nightly: `local` on a workstation or self-hosted runner; promote to `nightly`
  when that envelope stays clean.
- Release: `release` against the exact candidate commit, retain the JSON
  artefact, and record the commit, profile, seeds, and outcome. Use Mosquitto
  only as secondary confirmation after the fake-broker campaign is green.

Do not treat RSS growth as a fail. If a 50 000-operation schedule fails,
keep the reduced schedule in the campaign report.
