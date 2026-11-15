# 0018 — A permanent delay: repeated faults, a lag, and a wait that fires deadlines

Status: **Proposed** (15 November 2026, lot L3, second wave). Written by
lot L3 at the request of the integration owner, to be recorded by them. It
completes record 0015 without rewriting it, and the open condition of the
audit row F3 ("the vocabulary cannot express a steady delay without
fixing in advance the delivery at which the core progresses"). It becomes
a requirement only when the owner of the repository accepts it.

## Context

`tests/scenarios/audit/F3-steady-lag.json` delays the answer of the first
resolution by six newer ones. It declares seven faults of one occurrence
each, `late_answer_1` to `late_answer_7`, and seven pairs of
`FireDeadline` and `DriveUntil`. The scenario so decides in advance at
which delivery the core progresses: it states a bounded delay, not a
steady one.

## Decision

- **A repeated fault.** A fault names exactly one of `occurrence` (one
  occurrence, as before) and `from_occurrence` (every occurrence from
  that one on). A repeated fault states `expected_hits_at_least`, at
  least 1, instead of `expected_hits`: how often it is reached depends on
  the product. It has no `bind_operation_as`: it selects many items, and a
  binding would be ambiguous.
- **A lag.** `delay_result` on a repeated fault requires `lag`, an
  integer from 1 to 64, and no other fault has one: each delayed answer
  is delivered to the core once `lag` newer answers of the same selection
  are held. The delay is permanent and bounded, and no step delivers an
  answer of it: a step that hands over what a repeated fault holds is
  refused (`held-item-mismatch`).
- **Overlap.** Two faults of one point, command kind and compatible
  selection may not select the same occurrence: a repeated fault from n
  meets an occurrence k when k ≥ n, and two repeated faults always meet
  (`fault-match-invalid`).
- **A wait that fires deadlines.** `DriveUntil` may set `fire_deadlines`.
  While the awaited event has not happened and nothing else is eligible,
  the bench fires the armed deadline due first, as `FireDeadline` does,
  within the step's explicit `max_steps`. Without it, a `DriveUntil` fires
  no deadline, as before.
- **Results.** A fault of a run result carries the expectation of its
  scenario, `expected_hits` or `expected_hits_at_least`, and its hits. A
  fault expected at least so often is reached when its hits are at least
  that; the verdict, `all_required_faults_hit`, `produce` and `recompute`
  count it so. A result whose expectation differs from its scenario's is
  incomplete (`result-incomplete`).
- **Bounds.** Every new number is bounded: `from_occurrence` and
  `expected_hits_at_least` are 64-bit and range-checked by both readers,
  `lag` is at most 64, and `fire_deadlines` acts only within `max_steps`,
  which every `DriveUntil` states.
- **Existing scenarios** keep their meaning and their traces byte for
  byte. F3-steady-lag stays as it is; the permanent delay is a new
  scenario (`contracts/examples/v1/valid/scenario-permanent-delay.json`
  states it: one repeated fault with a lag of 6 and one wait for the
  PUBREC that fires deadlines). It is played under `tests/scenarios/audit/`
  once the bench implements the vocabulary (lot L4).

## Not decided here

- What a repeated `hold_command`, `drop_result`, `duplicate_result`,
  `refuse_fence` or `barrier` does at each of its occurrences. The schema
  admits them repeated; how the bench plays them is left to lot L4 and to
  a later record. Whatever it holds, no step hands over (as for a repeated
  delay).
- A verdict for a permanent delay that the core does not survive: a run
  that stops waiting is judged by `scenario_reaches_its_end` (record
  0016): FAIL on `quiescent`, INCONCLUSIVE on `budget`.
