# Contracts, revision 1

Revision 1 is strict and not frozen: unknown members, formats and versions
are errors today, and the formats may still change by decision record until
the S0 reception (ADR 0004).

## Layout

| Path | Content |
| - | - |
| `schema/v1/*.schema.json` | JSON Schema 2020-12. No `$id`; references are file-relative and never fetched |
| `v1/fault-points.json` | Contractual injection points and the actions each offers |
| `v1/profiles.json` | Profiles and their perimeter |
| `v1/assertions.json` | Assertion names, their meaning and the requirements they serve |
| `v1/requirements.json` | Every identifier the specification defines |
| `v1/p1.catalog.json` | Vocabulary of the transition registry: machines, state classes, event classes, guard dimensions, effects |
| `v1/p1.transitions.json` | Transition registry of P1. Status `seed`: its holes are listed, not hidden |
| `examples/v1/index.json` | Every example, and for each invalid one the code and pointer it must be refused with |

## Rules every v1 document follows

- It declares `format` and `schema_version`. Objects are closed; tagged
  unions carry a constant discriminator.
- A 64-bit quantity is a canonical decimal string: no sign, no leading zero,
  no whitespace, no line break, at most `18446744073709551615`.
- A number literal is a small integer. `7.0`, `7e0` and integers beyond 64
  bits are refused while parsing.
- A Packet Identifier is an integer from 1 to 65535.
- Duplicate keys are refused while parsing.
- Paths are relative to the `mqttiumd/` root and stay below it.
- Payloads are lower-case hexadecimal bytes. Nothing is Unicode-normalised.

## Validation

Validation runs in a fixed order and stops at the first stage that refuses:
strict parsing, known format and version, JSON Schema, semantic pass. The
semantic pass checks numeric range, references to catalogues and files,
binding definitions, the causal order of steps, verdict consistency, and
the coverage of a transition registry.

### Registry coverage

For each machine, state class and event class, the guard domain is the
product of the guard dimensions that the catalogue attaches to the event
class. Every point of that domain must be covered by exactly one rule. Two
rules on one point are an overlap: there is no implicit priority. No rule on
a point is a hole: an error in a `complete` registry, a listed note in a
`seed` one. A seed registry cannot support a PASS.

## Example corpus

An invalid example must be refused **for its declared reason**: every
diagnostic carries the declared code and one sits at the declared pointer.
An example refused for another reason fails the suite, and so does a code
that no example exercises. Any other reader of these formats, starting with
the independent oracle, is expected to agree with `index.json`.

The schema-level expectations were cross-checked once with the Python
`jsonschema` package; a permanent second reader belongs to lot L3.
