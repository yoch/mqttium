# Runtime fuzzer V3: pressure and fine-grained interleaving qualification

| | |
| --- | --- |
| Date | 2026-08-25 |
| Base commit | `b0b9018` (main) |
| Scope | Issues #388 and #389 (part B): the V3 pressure profile of the runtime schedule fuzzer |
| Prior work | [`RUNTIME-FUZZER-GENERATIVE-QUALIFICATION-2026-08-24.md`](RUNTIME-FUZZER-GENERATIVE-QUALIFICATION-2026-08-24.md) (V1), [`RUNTIME-FUZZER-COMPOSITION-QUALIFICATION-2026-08-24.md`](RUNTIME-FUZZER-COMPOSITION-QUALIFICATION-2026-08-24.md) (V2), #397 (terminal `publish_waiters == 0` oracle, #389 part A) |

This is a dated report. It records what was true of the named commits and what
was done about it. Do not edit it to match later code — write a new report.

## Problem

V1/V2 find ownership bugs but leave much of the runtime pressure surface
structurally cold: the harness transport has no `write_nowait`/`write_many`,
the writer admits one frame, payloads are tiny, schedules are serialized, and
every operation settles four event-loop turns. Eager writes, eager refusal
and fallback, the latency-batch flush, segmented writes, `write_many`
coalescing, and parked producers were unreachable by construction. #389
additionally required demonstrating a real application publisher in the
parked state *during execution* and qualifying every exit from it.

## Shape

`tests/fuzz/runtime_pressure_fuzzer.py` builds on the V1 harness exactly as
V2 does. It is not a generic asyncio scheduler; the model is bounded and
seed-reproducible. The V1 harness gained two profile seams — `_client_options()`
and a `settle_turns` attribute defaulting to the historical four-turn
convergence — plus one oracle refinement: the receipts-mirror invariant
(`stats.receipts.publish == outbound.pending_messages`) is now guarded by
terminal teardown, because durable session records legitimately outlive
receipts failed by `_fail_pending`. V1 grammar never reaches that state; the
pressure grammar does, deliberately. V1/V2 schedules and regressions are
otherwise byte-identical in behavior.

Varied axes (all from the seed):

- **Transport capabilities**: `write_nowait` present/absent with a
  schedule-controlled deterministic refusal budget; `write_many`
  present/absent. Both axes are composed present *and* absent across a
  campaign.
- **Writer sizing**: 32 frames / 256 KiB (4 frames / small bytes in the
  saturation family). Production defaults are never changed.
- **Producer bursts**: several `publish()` tasks spawned before any of them
  runs, so their admissions land in one event-loop turn.
- **Payload classes**: 16 B, 13 000 B (four cross the 48 KiB latency-batch
  target), 140 000 B (past `SEGMENT_THRESHOLD`).
- **Settlement budget**: `schedule.settle` operations choose 0/1/2/4 turns.
- **One lifecycle overlap**: writer window held + producer saturation + reader
  teardown landing on top, per the one-window-at-a-time bound from #388.

Eight families, one per `seed % 8`: `eager_paced`, `burst_latency_batch`,
`burst_write_many`, `segmented`, `parked_release`, `parked_cancel`,
`parked_teardown`, `writer_pressure_teardown`.

Two ingredients the exact wire-multiplicity obligation (#390) required:
`checkpoint.wire_bulk PUBLISH:n` raises the multiplicity target by a whole
batch, because one batched transport write completes several frames at once
and the per-frame checkpoint cannot straddle that; and `broker.puback_pending`
acknowledges completed *occurrences*, not identifiers, because a settled
identifier is legally reused within one schedule.

## Coverage gate

A campaign records per-run pressure counters and — by default from the CLI —
**fails a green run** that left any required surface cold: `eager_accepted`,
`eager_refused`, `latency_batches`, `write_many_calls`, `segmented_writes`,
`parked_publisher_observed`, `writer_waiters_observed`,
`pressure_lifecycle_overlaps`. This implements the #388 rule that a green
campaign with zero hits on an intended counter is a coverage failure, not
evidence of correctness.

Reference campaign at the audited commit (x86-64 dev host):

```
[DONE] target=runtime-pressure seeds=4096 failures=0 operation_traces=4096
       scheduling_traces=4096 seed_start=0 steps=36 wall_seconds=11.13
[PRESSURE] eager_accepted=17760 eager_refused=1024 latency_batches=512
           parked_publisher_observed=1536 pressure_lifecycle_overlaps=512
           segmented_writes=512 write_many_calls=13237
           writer_waiters_observed=512
```

Every seed produced a unique operation and scheduling trace; every family ran
512 times; every required surface stayed hot.

## Parked-publisher contract (#389 part B)

The three `parked_*` families configure `max_pending_outbound_messages=2` on
the profile's own client and run three concurrent QoS 1 `publish()` tasks, so
the third parks on admission capacity. The harness samples
`stats.receipts.publish_waiters` after every operation into a high-water
counter, and the `publisher_parked` checkpoint blocks until the parked state
is *observed during execution* — the gap #389 identified in PR #382's
after-the-fact probe. Exits qualified:

- **Release** — broker acknowledgements free capacity; the parked publisher
  retries, admits, and its PUBLISH reaches the wire exactly once.
- **Cancellation** — the parked task is cancelled, hands its wakeup on, and
  settles exactly once as cancelled.
- **Terminal teardown** — `disconnect()` wakes the parked publisher, whose
  retry fails terminally with `MQTTError` instead of parking forever.

In every family the Part A oracle proves `publish_waiters == 0` at terminal
quiescence and final task settlement proves exactly-once completion.

## Mutation qualification

Five mechanical breakages, each requiring a new pressure surface to become
visible, over seeds 0–15 (two schedules per family):

| mutation | detected | failing oracle |
| --- | --- | --- |
| `eager_accept_drops_frame` | 8/16 | wire multiplicity / liveness |
| `eager_refusal_lies` | 4/16 | wire multiplicity, latency-flush liveness |
| `segmented_payload_dropped` | 2/16 | wire multiplicity liveness |
| `parked_publisher_not_woken` | 4/16 | wire liveness; terminal waiter oracle |
| `publish_waiter_decrement_lost` | 6/16 | `publish waiter survived terminal teardown` |

Detections equal the number of schedules whose family exercises the mutated
surface — the mutants need their windows, as V1/V2 qualification established
for theirs.

## CI integration

The PR gate runs a 32-seed coverage-gated V3 smoke beside the V1/V2 smokes
and the full pytest qualification. The PR-gate stateful fuzz budget also
rises from 2 × 50 to 6 × 200 (~4 s): the previous budget provably could not
reach the replay-parked settlement defect fixed in #398, which first
manifests at seed 1, step 188 of the default profile.

## Not done, deliberately

- No generic asyncio scheduler and no third-window composition; the overlap
  stays bounded to one lifecycle window, per the #388 handoff.
- The 50k-seed ARM64 nightly (`tools/ci/runtime_campaign.py`) still runs V1
  only. Wiring V3 into the nightly rotation is a separate infrastructure
  decision once a reference campaign size for it is chosen.
