# The oracle

An independent judge of mqttiumd runs (DEM-019). It reads contracts,
scenarios, traces and store snapshots, and decides the assertions of a
scenario. It shares no decision logic with the broker: nothing here
imports, runs or was derived from `crates/mqttiumd-core`. It may share
descriptive formats with the product, never the calculation of an expected
answer.

Run from the `mqttiumd` directory:

```bash
python -m pip install -r tests/oracle/requirements.txt
python -m unittest discover -t tests/oracle -s tests/oracle/tests
```

Versions are pinned in `requirements.txt`. Only the JSON Schema level uses a
package; everything else is the standard library.

## What exists

| Module | Role |
| - | - |
| `mqttiumd_oracle/strict.py` | Strict parsing: duplicate keys, non-integer numbers, `-0`, integers beyond 64 bits, `NaN`, lone surrogates; canonical 64-bit decimal strings |
| `mqttiumd_oracle/documents.py` | One snapshot of the contracts, and the validation order of `contracts/README.md`: parsing, format and version, JSON Schema, semantic pass. Files are confined below the root and bounded |
| `mqttiumd_oracle/semantic.py` | The semantic rules of every v1 format, written from `contracts/README.md` |

This is the second reader of the semantic level. It agrees with
`contracts/examples/v1/index.json`, the statement it shares with the Rust
reader: every valid example is accepted and every invalid one is refused
for its declared code, at its declared pointer.

The two readers reach that agreement by different means where it matters:

- The contract files are read once here. The digest is computed from the
  bytes that are then parsed, so the rules applied are the rules the digest
  names.
- 64-bit members are found by walking the schema that applies to a
  document, not by member name: a member is range-checked when its schema
  refers to the `u64` primitive.

## What does not exist yet

Reading a trace as a whole, the oracle's own identity of an exchange, the
assertions of the catalogue, the hand-written corpus of traces and the
recomputation of a result. Until they exist no assertion is decided here,
and `docs/reception/open-conditions.md` keeps their rows.
