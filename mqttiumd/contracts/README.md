# Contracts, revision 1

Revision 1 is strict and provisional: unknown members, formats and versions
are errors today, and the formats may still change by decision record until
the S0 reception (ADR 0004).

A provisional revision has no stable name, so it is identified by exact
content. The contract digest is the SHA-256 of one `<sha256>  <path>` line
per JSON file of `v1/` and `schema/v1/`, in path order. `mqttiumd-lab
validate` prints it, a manifest records it, and a qualifying result names
it. The examples illustrate the contract and are not part of the digest.

## Layout

| Path | Content |
| - | - |
| `schema/v1/*.schema.json` | JSON Schema 2020-12. No `$id`; references are file-relative and never fetched |
| `v1/fault-points.json` | Contractual injection points and the actions each offers |
| `v1/profiles.json` | Profiles and their perimeter |
| `v1/assertions.json` | Assertion names, their meaning and the requirements they serve |
| `v1/requirements.json` | Every identifier the specification defines, each with its source: a chapter or an accepted decision record |
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
- Paths are relative to the `mqttiumd/` root and stay below it. Symbolic
  links are followed; the resolved file must be a regular file below the
  resolved root. A document is at most 16 MiB. Evidence cited by digest,
  such as a trace, has no size bound: it is hashed as a stream.
- A document refers to another only as the table of references allows
  (scenario to fixture, registry to catalogue, qualifying result to scenario
  and manifest). The format of the referenced document is checked before it
  is followed; the table is acyclic.
- Payloads are lower-case hexadecimal bytes. Nothing is Unicode-normalised.

## Validation

Validation runs in a fixed order and stops at the first stage that refuses:
strict parsing, known format and version, JSON Schema, semantic pass. The
semantic pass checks numeric range, references to catalogues and files,
binding definitions, the causal order of steps, verdicts, cited evidence,
and the coverage of a transition registry. A report keeps at most 64
diagnostics and 64 notes and counts the others exactly; a truncated report
still refuses.

### Registry coverage

For each machine, state class and event class, the guard domain is the
product of the guard dimensions that the catalogue attaches to the event
class. Every point of that domain must be covered by exactly one rule. Two
rules on one point are an overlap: there is no implicit priority. No rule on
a point is a hole: an error in a `complete` registry, a listed note in a
`seed` one. A seed registry cannot support a PASS.

Coverage is a product of dimensions, so it is sized before it is
enumerated. A cell may have at most 4 096 guard points and a registry
262 144 over all its cells; the sizes are computed with checked arithmetic
and a wider registry is refused (`coverage-budget-exceeded`) before any
point exists. The catalogue schema bounds its lists as well.

### Results

A result is `provisional` or `qualifying`.

- A **provisional** result is a development document. It never carries
  PASS: a run whose checks all pass without the evidence below is
  INCONCLUSIVE.
- A **qualifying** result is offered as evidence. It names its scenario,
  manifest and trace by path, digest and size, and the contract digest. The
  files must exist with those digests, the contracts must be the ones in
  use, its assertions and faults must be exactly those of the scenario, and
  none of them may be out of scope.

For both, the verdict must equal the one the assertions and injections
determine: FAIL if any assertion failed; otherwise INCONCLUSIVE if any is
inconclusive, an injection was not reached as announced, or nothing was
evaluated; otherwise PASS if all passed; otherwise OUT_OF_SCOPE.

This checks that a result is complete and consistent with what it cites,
not that its assertions are true. Recomputing them is the oracle's work:
see `../docs/reception/open-conditions.md`.

### Manifests

A manifest keeps apart `artifact`, what is certain about the binary that
ran (its digest and the build parameters captured when it was compiled),
from `checkout` and `tools_at_execution`, what was observed around it when
it ran. It does not claim that the binary was built from that checkout.

`modifications_sha256` is the digest of one `<content>  <path>` line per
path that differs from the revision or is untracked, in path order, where
the content is the SHA-256 of a regular file, `link:<target>` for a
symbolic link and `deleted` for a missing path. It depends on the work tree
only, not on how git prints a difference. The contract files are listed
once each, in path order, which is the order their digest is defined by.

## Example corpus

The examples and their index are derived: `../tools/build_corpus.py` writes
them, each invalid example being one edit of a valid document, and
`requirements.json` with them. Edit the tool, not the files; CI fails when
they are not up to date. Schemas, catalogues, the registry, the fixture and
scenario P1-02 are sources and are edited directly.

An invalid example must be refused **for its declared reason**: every
diagnostic carries the declared code and one sits at the declared pointer.
An example refused for another reason fails the suite, and so does a code
that no example exercises. Any other reader of these formats, starting with
the independent oracle, is expected to agree with `index.json`.

### Second schema reader

`../tools/check_schemas.py` reads the same schema files with the Python
`jsonschema` package, at the versions pinned in `../tools/requirements.txt`,
and must agree with `index.json` on everything a schema reader can decide,
including the pointer of each schema violation and the strict-parsing
codes. CI runs it.

It is not the independent oracle of lot L3: it reads no trace and no store
and repeats no semantic rule. A second reader of the semantic rules is an
open condition. Its strict parsing matches the Rust reader on the cases the
corpus holds, including `NaN`, a lone surrogate escape and an integer beyond
64 bits.
