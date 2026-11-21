# Decision records 0010 to 0046: what the owner decided

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

## Left proposed until a second opinion (13)

The owner will put these to an independent reviewer before deciding.

| Record | Why it waits |
| - | - |
| 0013 | It expects a line-by-line answer and leaves lines 9 and 12 undecided; line 12 is a stated disagreement between the transition registry and models M1a/M1b, which blocks gate G0 |
| 0018 | No campaign plays its vocabulary: the bench refuses such a scenario. Accepting it would accept a format, not an execution |
| 0022, 0025, 0026, 0029, 0033, 0034, 0035, 0043 | Each states budgets or a limit for sixteen connections that `runtime::Limits` now derives from an option; 0025 and 0026 also state rules that 0028 and 0041 replace. Each carries a dated note and, where it applies, an "Amended by" line |
| 0042 | Its kit is frozen evidence of the executable built from `51be4079` (record 0044, report 0026) |
| 0045, 0046 | Written with the runtime of record 0044. Their models pass in CI and have been read by nobody but their authors |

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
- That an accepted record is free of stale text. Record 0044, accepted
  on 20 November, says no loop polls on a timer for work; the runtime
  keeps three 100 ms scans (pending work, Will, message expiry). The
  sentence or the code is still to be adjusted.

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
