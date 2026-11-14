# 0005 — Precedence between chapters and resolved differences

Status: **Accepted with amendments** (14 November 2026, review of PR yoch/mqttium#647).
Applied in lot L0. Completes SPEC-001 and DEM-027.

## Context

Chapter 3 states that the earlier chapters keep their version and that a
later consolidation must carry the decisions back. That consolidation does
not exist. Agents working in parallel from four partly different documents
will not resolve the differences the same way. A precedence rule alone is
not a resolution: where two chapters differ in substance, what remains of
the earlier text must be said.

## Decision

### Rule

- For what the product guarantees, chapter 1 wins.
- For how it is realised and validated, the later chapter wins: 4, then 3,
  then 2.
- An accepted decision record wins over the chapter it amends.

### Sources of identifiers

`contracts/v1/requirements.json` is the closed list of identifiers. Each
has one source: a chapter, or an accepted decision record. The archived
chapters are never edited to hold an identifier that a decision introduced;
`P1-02b` comes from ADR 0001.

### Resolved differences

| Subject | Texts | Resolution |
| - | - | - |
| Store outcomes | IO-001: three results. DEM-010: four | Four outcomes apply. IO-001 keeps its force: a timeout is never a failure. `RetiredOutcomeUnavailable` is a statement about the identity, not a fourth result of the transaction, and IO-003 applies to it: a retired identity is answered "can no longer execute", never "did not happen" |
| `TurnGrant` | ARC-004: bounds work and output resources. DEM-006: work only | The grant measures work only. ARC-004's requirement that capacity is checked before the mutations that need it stands, and is carried by reservations made before the mutation |
| Effect batch | ARC-004: a batch passed to `step`. DEM-007: a box that persists between turns | The box applies. An effect is never returned per step and dropped; it stays in the owner's box until an adapter accepts it |
| Components | ARC-026: eight. DEM-002: four initial crates | Four crates now. The dependency prohibitions of ARC-026 apply to them unchanged: the core has no IO, the store adapter decides nothing, the bench does not replace the core |
| Scenario format | V3-017: a YAML example. DEM-014: JSON | JSON is normative. The YAML of V3-017 illustrates the content of a scenario, not its syntax; its assertions are the ones of the catalogue under other names |
| P1-02 recovery | V3-017: `reconcile_same_operation` after the crash. DEM-017: recovery through the inbound slot | Both paths must exist and neither replaces the other. After a crash, DEM-017 applies: the slot links the replay to the admission. Reconciliation by operation on a live process is exercised by P1-02b (ADR 0001), P1-05 and P1-10 |
| Refusal of an old identity | ARC-019: a closed prefix. V3-006, DEM-010: a fence ordered with the writer | The fence applies for a refusal. The closed prefix of ARC-019 stays as the collection mechanism of full P1. Both forbid execution; neither proves that an operation whose outcome was unknown had been rejected |

## Consequences

A new difference found by a lot is added to this table by decision record,
with its resolution, and not settled locally.
