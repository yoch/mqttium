# mqttiumd

`mqttiumd` is the working name of a standalone MQTT broker in Rust,
associated with MQTTium. Its central contract: in Durable mode, a positive
acknowledgement becomes eligible only after every responsibility it stands
for is durably committed.

**Status: experimental durable incoming QoS 2 over TCP, plus the S0 validation lab.** The pure Rust core runs
against a deterministic store and SQLite. The lab and independent Python
oracle receive bounded scenarios, including real process kills. This is
a narrow product TCP receiver, not a complete MQTT broker or a conformance or performance claim. See [the product profile and commands](docs/product-tcp.md).

## What exists

| Path | Content |
| - | - |
| `docs/spec/fr/` | The four specification chapters, archived verbatim in French with their digests |
| `docs/decisions/` | Decisions and proposed amendments to the specification |
| `crates/mqttiumd-contract/` | Identities, bounded quantities, the four store outcomes, the step interface, effect ownership, frozen recipes |
| `crates/mqttiumd-core/` | Pure prepared/applied transitions, bounded effects, recovery and incoming exchange state |
| `crates/mqttiumd-store-api/`, `crates/mqttiumd-store-sqlite/` | Durable-store contract, typed bounded readers and SQLite implementation |
| `crates/mqttiumd/` | Product TCP executable, bounded socket adapters and exclusive SQLite worker |
| `crates/mqttiumd-lab/` | Deterministic store, adapters, child supervisor, campaign runner and format reader |
| `tests/oracle/` | Independent Python semantic reader, assertions and campaign qualification |
| `tools/build_corpus.py` | Rebuilds the derived contract files: the requirement register and the examples |
| `tools/check_schemas.py` | A second, independent reader of the schemas, with pinned versions |
| `tools/install-tools.sh` | Installs the proof, model and fuzzing tools at pinned versions |
| `docs/reception/open-conditions.md` | What is deliberately not done yet, and which lot closes it |
| `docs/missions/` | The mission of each lot of the first wave: deliverables, reception, ownership |
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
python tools/build_corpus.py --check
python -m pip install -r tools/requirements.txt
python tools/check_schemas.py
```

`validate` prints the digest that identifies the contracts by exact content,
and exits 0 when every document is accepted and 2 otherwise. It checks the
document; only the oracle's `recompute` checks assertions and qualification
conditions again. `run` produces evidence, never a verdict. `replay` keeps
the backend, process mode and reconstruction scope of its campaign.

```bash
tools/run_read_failure_reception.sh /tmp/read-failures
tools/run_transactional_reception.sh /tmp/transaction-points 1
```

The first command receives typed read/control failures; an interrupted
business exchange remains INCONCLUSIVE or FAIL. The second requires fresh
qualifying K1/K2b/K3 campaigns, recomputation and matching second builds.
See `docs/reception/durable-read-journal.md` for the current continuation.

## Reading order

1. `docs/spec/README.md` for the chapters and the precedence between them.
2. `docs/decisions/README.md` for what was decided and what is proposed.
3. `contracts/README.md` for the formats and the example corpus.
4. `docs/reception/open-conditions.md` for what later lots must close.
5. `docs/missions/README.md` for the lots of the first wave and how their
   contract changes are coordinated.
6. `AGENTS.md` before changing anything.
