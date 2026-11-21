# Decision records 0010 to 0048: what was decided

On 21 November 2026 the repository owner answered, record by record, a
page that stated for each of the thirty-six proposed records what it
decides, what accepting it commits the project to, what it leaves open
and a recommendation. This page keeps the outcome. An earlier version of
it was a proposal; two of its statements were wrong and are corrected at
the end.

## Accepted (23)

0010, 0011, 0012, 0014, 0015, 0016, 0017, 0019, 0020, 0021, 0023, 0024,
0027, 0028, 0030, 0031, 0032, 0036, 0037, 0038, 0039, 0040, 0041.

Each now reads "Accepted (21 November 2026, by the repository owner)".
Three of them extend a record that stays proposed and say so in their
status: 0028 (0025), 0030 (0029) and 0036 (0035).

## After the second opinion

Thirteen records waited for an independent reviewer, who audited the tree
at `8e4303d` and recommended a decision for each with the exact text of
its amendments. The owner then delegated the decisions. They follow the
audit, with one difference: 0025 stays proposed until its amendment is
implemented.

| Record | Decision | Why |
| - | - | - |
| 0013 | Accepted with amendments | The amendment is the line-by-line answer the record asked for. On line 12 it chooses the transition registry: after a certain refusal a new attempt gets a fresh identity. Gate G0 closes after the registry, M1a, M1b and an executable witness agree |
| 0022, 0026, 0029, 0033, 0034, 0035, 0043 | Accepted with amendments | Each states numbers of the sixteen-connection profile, now governed by record 0048, and each amendment says which of its rules a later record replaced or what it still owes (0026 keeps its 100 ms rescan until a replacement is shown; 0043 owes an executable progress test) |
| 0042 | Accepted as a historical decision | It binds the frozen kit and the executable built from `51be4079`; a version 0.1 claim needs a new reader and a new reception |
| 0025 | Proposed | Its amendment requires that a delivery larger than the peer's Maximum Packet Size be discarded and its obligation completed with the connection kept open (MQTT-3.1.2-25). The core closes the connection and keeps the claim. The record is accepted with the change that implements it |
| 0018 | Proposed | The bench refuses to play its vocabulary; accepted when a campaign qualifies a scenario and rejects its mutant, withdrawn if none uses it |
| 0045 | Proposed | An unknown answer can hide a commit or an earlier diagnostic, and no oracle campaign runs a group |
| 0046 | Proposed | A quarter of the lane does not reserve the way of an acknowledgement; its Keep Alive rule was corrected after the audit |

Records 0047 (precedence) and 0048 (capacities of the runtime) were
accepted with these decisions. Record 0044 carries an amendment line: two
statements of its paragraph "Runtime" describe an intention the executable
does not meet.

## Three decisions on the records as a whole

- **One commit.** The acceptances are one change; the register was
  regenerated with `python tools/build_corpus.py` and the commit states
  the new contract digest.
- **Stale sentences are corrected** in the records that were still
  proposed, without changing a decision: 0010 (its proof runs in CI),
  0012 (the core emits `Exhaustion::IdentityCounter`), 0015 and 0016 (the
  campaigns exist), 0045 and 0046 (their models exist; the measured
  closures are those of report 0027). The index
  `docs/decisions/README.md` lists every record in one table.
- **The later record prevails** on the point it states, and the earlier
  one carries an "Amended by" line: record 0047.

## What acceptance does not say

- That the broker conforms to MQTT: the statement register under
  `conformance/` is the map of that evidence.
- That a frozen reception still passes on the current executable: they
  are not repaired (record 0044).
- That an accepted record is free of stale text. Record 0044 carries an
  amendment line for two statements the executable does not meet.

## Corrections to the earlier proposal

- It said record 0046 defers a packet "where 0043 closed it". The closure
  is stated by 0022 ("a handed-back packet that cannot reserve volatile
  capacity closes its own transport"), not by 0043.
- It said the closure rules of 0013 are the ones 0046 narrows. Record
  0013 states no closure under pressure; its lines 2 and 3 are closures
  on protocol errors.
- Its text search found two records touched by the new runtime (0022,
  0043). A full reading found six more: 0025, 0026, 0029, 0033, 0034 and
  0035.
