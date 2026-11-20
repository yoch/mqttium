# mqttiumd

`mqttiumd` is the working name of a standalone MQTT broker in Rust,
associated with MQTTium. Its central contract: in Durable mode, a positive
acknowledgement becomes eligible only after every responsibility it stands
for is durably committed.

**Status: local MQTT 5 hierarchical pub/sub, durable retained, Message Expiry and zero-delay Will preview, plus the S0 validation lab.** Incoming QoS 0/1/2 feeds subscriptions granted 0/1 and outgoing QoS 0/1. Non-retained QoS 0 follows a bounded volatile path; retained QoS 0 and QoS 1/2 admissions retain durable responsibilities. Message Expiry preserves the original lifetime through offline work and replay; unknown clock continuity suspends fresh delivery instead of renewing that lifetime. The pure Rust core runs
against a deterministic store and SQLite. The lab and independent Python
oracle receive bounded scenarios, including real process kills. This is
a bounded product, not a complete MQTT broker or a conformance or performance claim. Live receipt retirement recovers command capacity without restarting the broker, and outgoing QoS 1 Packet Identifiers are reused after known durable Finish application. The Will preview reports unexpected sensor loss through durable non-retained QoS1 publication. See [the Will decision](docs/decisions/0034-durable-will.md), [the normal package guide](preview/README.md), [Message Expiry reception](docs/reports/0017-message-expiry-preview.md), [CI follow-up](docs/reception/message-expiry-ci-reception.md), [Message Expiry decision](docs/decisions/0033-message-expiry.md), [retained reception](docs/reports/0016-durable-retained-preview.md), [the product profile and commands](docs/product-tcp.md), [QoS 0 reception](docs/reports/0010-qos0-product.md), [receipt retirement reception](docs/reports/0011-live-receipt-retirement.md) and [protected identifier reuse](docs/reports/0012-outgoing-pi-reuse.md).

## Runtime since record 0044

The product executable no longer polls. Each connection is one
non-blocking task on a small pool of `mio` reactor threads, the core and
the store worker wait for a signal, and the store commits the commands
waiting in its lane as one transaction
([0044](docs/decisions/0044-reactor-runtime-and-evidence-tiers.md),
[0045](docs/decisions/0045-group-commit.md),
[0046](docs/decisions/0046-input-backpressure.md)). The connection limit
is an option (`--max-connections`, up to 128), and input pressure slows a
peer instead of disconnecting it. The figures of report 0024 describe
the executable before these records; no throughput claim is made for the
current one.

What the broker accepts of MQTT is still the bounded profile described in
[the product guide](docs/product-tcp.md). The gap to the pinned Mosquitto
is listed scenario by scenario in `tests/compat/` and statement by
statement in `conformance/`. The system receptions, `preview/` and the
pilot kits are frozen evidence of the executable built from `51be4079`
([report 0026](docs/reports/0026-frozen-receptions.md)).

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
