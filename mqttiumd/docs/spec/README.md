# Specification archive

`fr/` holds the four chapters of the mqttiumd specification, version 0.1 of
14 November 2026, byte for byte as they were delivered. They are in French
and are never edited: a decision that departs from them is recorded in
`../decisions/` and the chapters stay as evidence of what was specified.
`fr/SHA256SUMS` records their digests; `contracts/v1/requirements.json`
repeats them and lists every identifier they define. A test fails when a
chapter, a digest or the register drift apart.

| File | Chapter | Identifiers |
| - | - | - |
| `fr/chapitre-1.md` | Compatibility and recovery guarantees | PRD, SPEC, CMP, CFG, SEC, ID, DUR, FAIL, ACK, ADM, ROUTE, CONC, RES, TIME, WILL, END, IO, MIG, VAL, T01 to T18 |
| `fr/chapitre-2.md` | Concurrent core and reference architecture R1 | ARC, D1 |
| `fr/chapitre-3.md` | Validation protocol of the prototypes | V3, P1 to P3 scenarios, MU, M1 to M6, G0 to G5, T1 |
| `fr/chapitre-4.md` | Contracts and starting lot of P1 | DEM, QABS, DEM-MU, L0 to L10, S0, S1 |

The chapters link to each other through pages that are not reachable from
this repository. Follow the identifiers instead: every cross-reference names
a requirement, and every requirement lives in exactly one file above.

## Precedence

The chapters refine each other and sometimes differ. ADR 0005 fixes the
rule and lists the known differences:

- For what the product guarantees, chapter 1 wins.
- For how it is realised and validated, the later chapter wins: 4, then 3,
  then 2.
- An accepted decision record wins over the chapter it amends.
