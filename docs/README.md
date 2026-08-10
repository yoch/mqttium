# MQTTium documentation

Documents here fall into exactly two kinds, and the difference decides how you
should read them.

**Contracts** live in this directory. They describe what MQTTium guarantees
today, they are maintained alongside the code, and a change that contradicts
one is a bug in the change or an intentional update of the contract.

**Reports** live in [`reports/`](reports/). Each one records a measurement, an
audit or a decision on the date it was written. They explain *why* the code
looks the way it does; they are never a statement about current behaviour and
are not updated when the code moves on.

## Contracts

### Public surface and compatibility

| Document | Scope |
| --- | --- |
| [`API-STABILITY.md`](API-STABILITY.md) | Stable / Provisional / Internal tiers, deprecation policy. Authoritative for what may change and how. |
| [`COMPAT.md`](COMPAT.md) | The Paho `CallbackAPIVersion.VERSION2` façade: what is supported, what deliberately differs, what is rejected. |
| [`MIGRATION.md`](MIGRATION.md) | Moving to MQTTium from Paho or gmqtt. |

### Architecture and protocol

| Document | Scope |
| --- | --- |
| [`DESIGN.md`](DESIGN.md) 🇫🇷 | Architecture: the engine/client split, ownership, effect pipeline. |
| [`IMPLEMENTATION-GUIDE.md`](IMPLEMENTATION-GUIDE.md) 🇫🇷 | Precise contracts: property table, CONNACK negotiation, keepalive, reconnect, timeouts, QoS decisions, backpressure budgets. |
| [`LOGGING.md`](LOGGING.md) 🇫🇷 | Why `logging` is absent from `src/`, and what to use instead. |

### Verification

| Document | Scope |
| --- | --- |
| [`BENCHMARKING.md`](BENCHMARKING.md) | Validity contract for every benchmark: paired A/B, medians, rotated order, `N/A` over a manufactured comparison. |
| [`MEMORY-BENCHMARK.md`](MEMORY-BENCHMARK.md) | Methodology and harness contract behind `benchmarks/memory_profile.py` and its threshold file. |
| [`FUZZING.md`](FUZZING.md) 🇫🇷 | Fuzzing strategy, seeded corpus and Hypothesis profiles. |
| [`STABILITY.md`](STABILITY.md) | Soak and multi-broker interoperability campaign, with its acceptance criteria. |

### Process

| Document | Scope |
| --- | --- |
| [`RELEASING.md`](RELEASING.md) | Tag, rehearsal, publication and failure handling. Authoritative for the release procedure. |
| [`ROADMAP.md`](ROADMAP.md) | Remaining work before a stable release. |
| [`BETA-REPORTING.md`](BETA-REPORTING.md) | What a usable beta bug report contains, and how reports are triaged. |
| [`CHATGPT-USAGE.md`](CHATGPT-USAGE.md) | Atomic multi-file commit procedure for agents working on this repository through the GitHub API. |

🇫🇷 marks documents written in French. The rest of the repository is English.

## Order of authority

On protocol behaviour, a conflict resolves in this order:

1. the MQTT 3.1.1 / 5.0 specification;
2. [`IMPLEMENTATION-GUIDE.md`](IMPLEMENTATION-GUIDE.md);
3. [`DESIGN.md`](DESIGN.md).

Outside protocol behaviour, [`API-STABILITY.md`](API-STABILITY.md) governs the
public surface and [`RELEASING.md`](RELEASING.md) governs publication. A report
never outranks a contract, whatever its date.

## Adding a document

Ask which kind you are writing.

A **contract** joins this directory and the table above, and it must be kept
true: if later work contradicts it, the document is updated in the same change.

A **report** joins [`reports/`](reports/) and its index. State the commit or
version it describes at the top, and leave it alone afterwards — superseding a
report means writing a new one, not editing the record. Measurements themselves
stay build artefacts: quote figures in the report, never commit generated result
files.
