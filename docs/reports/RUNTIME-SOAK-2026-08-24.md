# Runtime soak campaign — 2026-08-24

Dated campaign record for the deterministic runtime soak / quiescence
harness. This is not a product API contract. The maintained operator
guide is [Runtime soak and quiescence](../runtime-soak.md).

## Environment

- Repository: `yoch/mqttium`
- Branch: `research/runtime-soak`
- Base: `origin/main` at `a336f83` (1.0.0rc9)
- Python: 3.12, in-tree install `pip install -e ".[dev]"`
- Backend: in-memory packet-aware broker (`benchmarks/runtime_soak_lib/broker.py`)
- Oracles: logical ownership only (tasks, receipts, writer occupancy, effects,
  delivery queues, packet identifiers, inflight rows). RSS was not used as a
  fail condition.

## Commands

```bash
python -m pytest -q tests/unit/test_runtime_soak.py --timeout=25
PYTHONPATH=src python benchmarks/runtime_soak.py --profile ci
PYTHONPATH=src python benchmarks/runtime_soak.py --profile local --seed 1 --seed 7 --operations 2000
PYTHONPATH=src python benchmarks/runtime_soak.py --profile local --seed 1 --operations 8000
```

## Observed stability envelope

All listed runs ended `DISCONNECTED` with zero logical-ownership leftovers.

| Profile | Operations | Seeds | Protocols | Checkpoints / run | Wall time / run | Result |
| --- | --- | --- | --- | --- | --- | --- |
| unit pytest | 8–24 | 1 | 3.1.1 and 5 | ≥1 | <0.2 s suite | pass |
| `ci` | 64 (+ shutdown tail → 68) | 1, 7 | 3.1.1 and 5 | 4 | 0.10–0.16 s | pass |
| local subset | 2000 (+ tail → 2004) | 1, 7 | 3.1.1 and 5 | 85 | 4.3–5.2 s | pass |
| `local` | 8000 (+ tail → 8004) | 1 | 3.1.1 and 5 | 335 | 19.5 s | pass |

No reduced failing client schedule remained after harness corrections.

## Harness defects found during the search

These failed the oracles and reduced, but they were incomplete fake-broker
behaviour rather than `AsyncClient` leaks:

1. **SUBACK granted QoS 0** while the client subscribed at QoS 2. Echoed QoS 2
   deliveries could not complete. SUBACK now returns reason `2`.
2. **Session Present reconnect did not replay inbound QoS 2.** After a drop
   mid-handshake the client correctly kept one inbound inflight row; the broker
   never resent PUBLISH/PUBREL. The broker now stores incomplete inbound QoS 2
   and replays it on Session Present.
3. **Reconnect wait raced the dying connection.** `drop_network` now waits for
   `connection_epoch` to increase, then for reader+writer with reconnect idle,
   before the next subscribe.
4. **Reduction called `asyncio.run` inside the campaign loop.** Replay now
   uses a worker thread when a loop is already running.

A reduced schedule that exposed (2) was:

```
subscribe
cancel_publish
drop_network session_present=True
quiesce
```

After the broker replay fix, that prefix is no longer failing. It is retained
as `tests/unit/test_runtime_soak.py::test_reduced_session_present_drop_reaches_idle`.

## Recommendations

- **Pull requests:** keep `ci` (64 ops, seeds 1 and 7, both protocols) on the
  fake broker. The soak workflow job `runtime-soak-fake` and the unit test
  (24 ops) are the PR gate.
- **Nightly:** run `local` (8 000 ops, four seeds, SQLite store) on a
  workstation. Promote to `nightly` (50 000 ops) only after `local` stays
  clean for several consecutive commits.
- **Release:** run `release` (200 000 ops) against the exact candidate commit
  and retain the JSON artefact. Use Mosquitto (`--backend mosquitto`) only
  after the fake-broker campaign is green; it is confirmation, not the
  primary search.
- **Do not fail on RSS.** If a long schedule fails, keep the reduced op list
  in the campaign report and replay it under the same seed and protocol.

## Limitations

- Mosquitto was not used in this campaign.
- `nightly` and `release` profiles were not executed to completion here.
- The fake broker is packet-valid but not a full MQTT server; Session Present
  replay covers inbound QoS 2 that the harness itself generated.
