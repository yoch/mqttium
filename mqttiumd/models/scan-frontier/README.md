# Scan-frontier pagination and DirtyDuringLoad invalidation

`MScanFrontier` models the outgoing work-page cursor (`after`, `known_through`,
`rescan_needed`, `DirtyDuringLoad`) of one subscriber session when non-inline
publications, retained replays, and delivery finishes overlap with bounded
`LoadWork` scans (reserve R1 of `docs/missions/L11-optimisation.md`, commit
`76af3a54`).

It is runtime-tier evidence: a TLA+ model of `Connections` and `Owner` page
completion with configurations for Lot 9's lost wake-up, `76af3a54`'s
unconditional fan-in rewind, the repaired `DirtyDuringLoad` frontier, and three
mutants removing each safety guard.

## Questions

- Does `known_through` ever advance past a committed, notified, unloaded
  obligation or a pending lower publication? (`KnownThroughSound`)
- Once all admissions and scans settle, can any committed obligation remain
  invisible? (`NoInvisibleObligation`)
- Under strictly monotonic live fan-in admissions with no invalidation or
  arbitrary-low replay, does a live commit during an in-flight `LoadWork`
  ever discard a non-empty `next` cursor and force a full rescan?
  (`MonotoneFanInProgress`)

## Configurations

| Configuration | Mode | Expected |
| - | - | - |
| `safety` | `dirty_floor`, N = 3, Page = 1, out-of-order commits, low replay, invalidation | `TypeOK`, `KnownThroughSound`, `NoInvisibleObligation` hold |
| `monotone-no-rewind` | `dirty_floor`, N = 4, Page = 2, monotonic fan-in | `TypeOK`, `KnownThroughSound`, `NoInvisibleObligation`, `MonotoneFanInProgress` hold |
| `76af3a5-rewind` | `76af3a5` unconditional rewind under monotonic fan-in | `MonotoneFanInProgress` violated |
| `lot9-lost-wakeup` | `lot9` overwriting `rescan_needed` with `discard` | `NoInvisibleObligation` violated |
| `mut-no-floor-check` | `dirty_floor` without checking missed dirty pubs `<= bound` | `NoInvisibleObligation` violated |
| `mut-no-rescan-on-empty` | `dirty_floor` clearing `rescan_needed` on terminal page (`next = None`) | `NoInvisibleObligation` violated |
| `mut-advance-under-pending` | `dirty_floor` ignoring `pending_non_inline_pubs` | `KnownThroughSound` violated |

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/scan-frontier
```

## Correspondence

| Model action | Code | Replay |
| - | - | - |
| `Admit` | `Owner::plan_admission`, `Owner::apply_change` (`pending_non_inline_pubs`) | `lower_publication_commit_during_load_rewinds_page_cursor` |
| `StoreCommit` | store worker committing `AdmitPublication` | `higher_publication_commit_during_load_preserves_page_cursor_and_known_through` |
| `ApplyCommit` | `Owner::apply_draft`, `Connections::mark_dirty_from` (`DirtyDuringLoad::Pubs`) | `higher_publication_commit_during_load_preserves_page_cursor_and_known_through` |
| `LowReplayCommit` | `Owner::apply_pubsub` (`ControlEnd` / `RetainedDone`), `Owner::shard_notify_dirty`, `Connections::mark_dirty` (`DirtyDuringLoad::Any`) | `a_read_started_before_an_older_obligation_revisits_the_lower_range` |
| `BeginScan` | `Owner::plan_work`, `Connections::begin_scan` | `higher_publication_commit_during_load_preserves_page_cursor_and_known_through` |
| `StoreSnapshot` | store worker answering `Effect::LoadWork` with bounded `WorkPage` | `higher_publication_commit_during_terminal_page_still_rescans_above_known_through` |
| `CompletePage` | `Owner::apply_pubsub` (`Change::Page`), `Connections::complete_page`, `Connections::advance_known_through` | `higher_publication_commit_during_terminal_page_still_rescans_above_known_through` |
| `FinishDelivery` | `Owner::apply_pubsub` (`Change::Finished`), `Connections::invalidate_reads_for` | `a_read_started_before_an_older_obligation_revisits_the_lower_range` |
| `Done` | quiescent state (no pending work) | `higher_publication_commit_during_terminal_page_still_rescans_above_known_through` |
