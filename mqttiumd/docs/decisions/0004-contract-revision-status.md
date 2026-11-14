# 0004 — Revision 1 is strict and not frozen; deltas made by L0

Status: **Proposed**. Amends DEM-014 ("close the formats from their first
version"). Lot L0 is built on it.

## Context

DEM-014 closes the formats before any code has produced or consumed them.
Lots L1 to L4 will find missing fields and wrong shapes. Bumping the version
at each discovery would leave several dead versions before the first run.

## Decision

Revision 1 is strict now and frozen at the S0 reception.

- Strict: closed objects, constant discriminators, unknown formats and
  versions refused, local schemas only.
- Not frozen: until S0 is received, revision 1 may change through a
  decision record and updated examples. After S0, a change is a new version.

Lot L0 made the following choices where the chapters were silent or where a
literal reading was unsafe. Each is recorded here so that none is silent.

| Subject | Choice | Reason |
| - | - | - |
| `u64` pattern | The schema adds `not: {pattern: "[\n\r]"}` to the pattern of DEM-014 | `$` matches before a trailing line break in some validators; Python `re` accepts `"4096\n"` |
| Number literals | `7.0`, `7e0` and integers beyond 64 bits are refused while parsing | JSON Schema treats `7.0` as an integer; readers would disagree |
| Schema identifiers | Schema files carry no `$id`; references are file-relative | A custom URI scheme is not joined by every validator, and an `https` identifier suggests a download |
| Disposition `FAULT` | Added to the dispositions of DEM-015 | DEM-006 defines `Fatal` and DEM-010 requires a stop on contradictory terminal results; the list had no value for it |
| Registry `status` | `seed` or `complete` | A partial registry must list its holes without being mistaken for a complete one |
| Assertion names | They keep the spelling of DEM-017, including `same_recipe_for_op_A` | The scenario file is the text of the specification |
| Extra schemas | `fixture.schema.json` and `catalogs.schema.json` beside the six of DEM-014 | Fixtures and catalogues are closed formats too |
| Run manifest | Content sections required; experiment sections optional until a lot produces them | L0 runs nothing |

## Consequences

`contracts/README.md` states the rules. If the freeze rule is refused, the
formats are final as written and every later change is a version 2.
