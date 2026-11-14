# 0005 — Precedence between chapters and known differences

Status: **Proposed**. Completes SPEC-001 and DEM-027.

## Context

Chapter 3 states that the earlier chapters keep their version and that a
later consolidation must carry the decisions back. That consolidation does
not exist. Agents working in parallel from four partly different documents
will not resolve the differences the same way.

## Decision

- For what the product guarantees, chapter 1 wins.
- For how it is realised and validated, the later chapter wins: 4, then 3,
  then 2.
- An accepted decision record wins over the chapter it amends.
- `contracts/v1/requirements.json` is the closed list of identifiers. A
  registry rule, an assertion or a mission cites identifiers from it.

Known differences, resolved by the rule above:

| Subject | Earlier text | Later text that applies |
| - | - | - |
| Store outcomes | IO-001: three results | DEM-010: four, adding `RetiredOutcomeUnavailable` |
| `TurnGrant` | ARC-004: bounds work and output resources | DEM-006: measures work only; reservations are separate |
| Components | ARC-026: eight | DEM-002: four initial crates |
| Scenario format | V3-017: YAML example | DEM-014: JSON is normative in version 1 |
| Effect batch | ARC-004: `BoundedEffectBatch` passed to `step` | DEM-007: a box that persists between turns |
| P1-02 recovery | V3-017: `reconcile_same_operation` after the crash | DEM-017: recovery through the inbound slot; see ADR 0001 |
| Refusal of an old identity | ARC-019: closed prefix | V3-006 and DEM-010: fence ordered with the writer; a closure forbids execution and proves no rejection |

## Consequences

A new difference found by a lot is added to this table by decision record,
not resolved locally.
