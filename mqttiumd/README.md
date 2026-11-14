# mqttiumd

`mqttiumd` is the working name of a standalone MQTT broker in Rust,
associated with MQTTium. Its central contract: in Durable mode, a positive
acknowledgement becomes eligible only after every responsibility it stands
for is durably committed.

**Status: lot L0 (contracts) only.** There is no broker here yet. Nothing in
this tree is a validated prototype, a benchmark result or a conformance
claim.

## What exists

| Path | Content |
| - | - |
| `docs/spec/fr/` | The four specification chapters, archived verbatim in French with their digests |
| `docs/decisions/` | Decisions and proposed amendments to the specification |
| `crates/mqttiumd-contract/` | Identities, bounded quantities, the four store outcomes, the step interface, effect ownership, frozen recipes |
| `crates/mqttiumd-core/` | Empty skeleton that carries the purity boundary; transitions arrive with lot L1 |
| `crates/mqttiumd-lab/` | `mqttiumd-lab validate` and `mqttiumd-lab manifest` |
| `tools/check_schemas.py` | A second, independent reader of the schemas, with pinned versions |
| `docs/reception/open-conditions.md` | What is deliberately not done yet, and which lot closes it |
| `contracts/schema/v1/` | JSON Schema 2020-12 files of every v1 format |
| `contracts/v1/` | Catalogues, requirement register, seed transition registry |
| `contracts/examples/v1/` | Valid and invalid examples with the reason each invalid one must be refused for |
| `tests/scenarios/p1/`, `tests/fixtures/p1/` | Scenario P1-02 and its fixture |

## Commands

Run from this directory. The toolchain is pinned by `rust-toolchain.toml`.

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json
cargo run -q -p mqttiumd-lab -- manifest
```

```bash
python -m pip install -r tools/requirements.txt
python tools/check_schemas.py
```

`validate` prints the digest that identifies the contracts by exact content,
and exits 0 when every document is accepted and 2 otherwise. `run`,
`replay` and `minimize` are specified by DEM-023 and not implemented; they
are refused and report no verdict.

## Reading order

1. `docs/spec/README.md` for the chapters and the precedence between them.
2. `docs/decisions/README.md` for what was decided and what is proposed.
3. `contracts/README.md` for the formats and the example corpus.
4. `docs/reception/open-conditions.md` for what later lots must close.
5. `AGENTS.md` before changing anything.
