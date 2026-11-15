# 0015 — Scenario vocabulary for a race without a crash, and the correction of step 3 of P1-02b

Status: **Proposed** (15 November 2026, lot L3 with lot L2, first wave).
Written by lot L3, recorded by the integration owner. It completes record
0001 and DEM-017, and **corrects step 3 of scenario P1-02b in record
0001**. It becomes a requirement only when the owner of the repository
accepts it.

## Context

Record 0001 orders P1-02b by holding a command, firing two deadlines and
delivering an old result late. The scenario format of DEM-017 could only
wait for a fault or a peer packet, crash and restart.

Step 3 of P1-02b in record 0001 says: "The fence cannot be established
while the command is held (the QABS-C condition), so the store answers
`PendingOrUnknown`." Lots L2 and L3 found independently that this
contradicts DEM-018. QABS-C is the conjunction of three conditions:
command still held, no commit known, fence impossible to establish. QABS-B
requires the opposite of what the record derives: the fence commits while
the command is held, and the command is then refused. On a store that
realises QABS-B, step 3 as written gets a certain rejection, the command
released at step 4 is refused, and no positive PUBREC is ever observed.

## Decision

### Steps

- Three steps: `FireDeadline {max_steps}`, `ReleaseHeldCommand {fault}`,
  `DeliverHeldResult {fault, max_steps}`.
- `FireDeadline` drives the broker until no effect waits in an effect box,
  lets the timer adapter deliver the armed deadline that is due first, and
  drives until the core has consumed it. Two deadlines due at the same
  logical time fire in the order they were armed. If no deadline is armed
  the step fails and the run is inconclusive; it is never a no-op. Logical
  time advances in no other way.

### What a fault holds

- A held item is designated by the fault that holds it, never by an
  identity. `hold_command` holds a command and `delay_result` holds a
  result.
- A fault that holds, holds one item in the life of a scenario: a scenario
  waits for it once and its `expected_hits` is at most 1. What is held is
  handed over at most once. A crash of the broker destroys it; a fault no
  step waited for before a crash may still be reached after the restart.
- A held command or result that no step hands over stays held to the end
  of the run. This is a legitimate history (QABS-C keeps its command held).
- Each injection point declares what it `carries`, a command or a result,
  and offers only the actions that act on it.
- `match.result_of` selects, on a point that carries a result, the result
  of one kind of effect. Two faults of a scenario never select the same
  command or result; selections that would count occurrences over
  different sets are refused.

### The fence point, and step 3 of P1-02b

- A new injection point `store.fence.transit`: a resolution read the
  absence of the operation and its fence has not entered the writer. It
  carries a command. A resolution that finds a receipt, a fence or a
  retired identity does not reach it.
- Two actions. `hold_command` holds the fence, which orders QABS-A and
  QABS-B; the fence is written only if the operation is still absent when
  the held fence enters the writer. `refuse_fence` makes the write of that
  one fence impossible while the store stays available: the resolution
  answers `PendingOrUnknown`. The trace records `StoreFenceRefused` where
  `StoreFenceStable` would be, so that a refusal is never inferred from a
  missing record.
- Step 3 of P1-02b becomes: "The first deadline fires. The core emits
  `ResolveAndFence`. The resolution reads the absence of the operation and
  reaches `store.fence.transit`, where a declared injection
  (`refuse_fence`) makes the write of its fence impossible, so the store
  answers `PendingOrUnknown`. The bench holds that answer. A held command
  does not by itself prevent a fence: without the injection the store
  would fence, and answer `RejectedCertain` (QABS-B)."
- Rule of the store, for a resolution on an absent identity: it always
  fences, durably, before it reports a certain rejection, unless the write
  of the fence fails. It never waits for the held command.

## Consequences

- Scenarios `tests/scenarios/qabs/QABS-A.json`, `QABS-B.json` and
  `QABS-C.json`, and two assertions: `fenced_operation_never_executed` and
  `certain_rejection_has_a_closure` (DEM-MU-01).
- Lot L2 realises the fence point in its adapter; lot L5 models the fence
  of an absent identity as succeeding unless a counted fault refuses it.
- `FireDeadline` names no deadline; a scenario that must choose among
  deadlines in another order than their due time needs a further change.
- No bench plays these scenarios yet (lot L4).
