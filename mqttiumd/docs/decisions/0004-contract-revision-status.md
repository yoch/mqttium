# 0004 — Revision 1 is strict and provisional, identified by content

Status: **Accepted with amendments** (14 November 2026, review of PR yoch/mqttium#647).
Applied in lot L0. Amends DEM-014 ("close the formats from their first
version").

## Context

DEM-014 closes the formats before any code has produced or consumed them.
Lots L1 to L4 will find missing fields and wrong shapes. A version bump at
each discovery would leave several dead versions before the first run.

## Decision

Revision 1 is strict now and provisional until the S0 reception.

- **Strict**: closed objects, constant discriminators, unknown formats and
  versions refused, local schemas only.
- **Provisional**: until S0 is received, revision 1 may change through a
  decision record and updated examples. After S0, a change is a new version.
- **Identified by exact content**: a provisional revision has no stable
  name, so it is designated by the digest of its files. `mqttiumd-lab
  validate` prints that digest, a manifest records it, and a qualifying
  result names it. A result is evaluated only against the contracts it
  names.

`fixture.schema.json` and `catalogs.schema.json` stay beside the six
schemas of DEM-014: fixtures and catalogues are closed formats too.

Schema files carry no `$id`. Their references are relative to the file and
resolve without one, and a custom URI scheme is not joined by every
validator. An HTTPS identifier would not by itself cause a network
retrieval; it is simply not needed.

### Choices made by L0

Where the chapters were silent or a literal reading was unsafe:

| Subject | Choice | Reason |
| - | - | - |
| `u64` pattern | The schema adds `not: {pattern: "[\n\r]"}` to the pattern of DEM-014 | `$` matches before a trailing line break in some validators; Python `re` accepts `"4096\n"` |
| Number literals | `7.0`, `7e0` and integers beyond 64 bits are refused while parsing | JSON Schema treats `7.0` as an integer; readers would disagree |
| Disposition `FAULT` | Added to the dispositions of DEM-015 | DEM-006 defines `Fatal` and DEM-010 requires a stop on contradictory terminal results |
| Registry `status` | `seed` or `complete` | A partial registry lists its holes and cannot be mistaken for a complete one |
| Coverage budget | At most 4 096 guard points per cell and 262 144 per registry; catalogue lists are bounded | Coverage is a product of dimensions; it is sized before it is enumerated |
| Diagnostics | A report keeps 64 and counts the rest | A hostile document must not produce an unbounded report |
| Documents | Regular files below the root, at most 16 MiB; links are followed only while they stay below the root | The reader's input is bounded and confined |
| Assertion names | They keep the spelling of DEM-017, including `same_recipe_for_op_A` | The scenario file is the text of the specification |
| Run result | `qualification` is `provisional` or `qualifying`; the verdict equals the one its assertions determine | A PASS must not be claimable without evidence |
| Run manifest | `artifact` is certain, `checkout` is observed at execution | A manifest must not suggest a correspondence it has not established |
| Requirement register | Each identifier has a source: a chapter or an accepted decision record | Decisions add identifiers; the archive is never edited |
| `ArmDeadline` | Carries a delay, not an absolute time | ADR 0001: the delay starts at the acceptance of `Persist` |

## Consequences

`contracts/README.md` states the rules. Every lot that changes a contract
file changes the contract digest, and says so in its report.
