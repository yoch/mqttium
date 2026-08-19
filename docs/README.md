# MQTTium documentation

Start with the task you are trying to complete. User guides explain how to use
MQTTium; contracts define behaviour that the implementation must preserve;
reports record dated evidence and decisions.

All active documentation is written in English and is readable directly on
GitHub or from the source distribution. MQTTium does not require a separate
documentation site or generator.

## Start here

| Document | Use it when |
| --- | --- |
| [`../README.md`](../README.md) | You are evaluating MQTTium or want the shortest path to a working client. |
| [`GETTING-STARTED.md`](GETTING-STARTED.md) | You are installing the library, connecting to a broker, publishing, subscribing or choosing a delivery style. |
| [`SESSIONS-AND-PERSISTENCE.md`](SESSIONS-AND-PERSISTENCE.md) | You need reconnects, broker sessions, SQLite-backed inflight state or restart recovery. |
| [`OPERATIONS.md`](OPERATIONS.md) | You need queue sizing, backpressure, statistics, timeouts, shutdown or failure diagnosis. |

Runnable counterparts live in [`../examples/`](../examples/): native pub/sub,
durable sessions, Paho VERSION2 migration and runtime statistics.

## User guides

| Document | Scope |
| --- | --- |
| [`MIGRATION.md`](MIGRATION.md) | A staged move from Paho VERSION2 or gmqtt to the native async API, plus one-shot helpers. |
| [`COMPAT.md`](COMPAT.md) | Exact Paho VERSION2 compatibility matrix, intentional differences and unsupported behaviour. |
| [`LOGGING.md`](LOGGING.md) | Application-owned logging and instrumentation using receipts, callbacks and snapshots. |
| [`BETA-REPORTING.md`](BETA-REPORTING.md) | Reproducible pre-release issue reports and the diagnostic data maintainers need. |

The Paho facade is a migration surface, not a second native API. New async code
should use `mqttium.api.AsyncClient`; existing synchronous VERSION2 code can use
`mqttium.compat.paho.Client` while migrating incrementally.

## Contracts and reference

These documents describe current behaviour. A code change that contradicts one
must update the contract in the same change.

| Document | Authority |
| --- | --- |
| [`API-STABILITY.md`](API-STABILITY.md) | Stable, Provisional and Internal tiers; canonical imports; defaults and deprecation policy. |
| [`IMPLEMENTATION-GUIDE.md`](IMPLEMENTATION-GUIDE.md) | MQTT properties, negotiation, keepalive, reconnect, QoS, backpressure, delivery and persistence contracts. |
| [`DESIGN.md`](DESIGN.md) | Architecture, ownership boundaries, receipts, persistence, observability and performance rules. |
| [`CONFORMANCE.md`](CONFORMANCE.md) | What is verified against the numbered MQTT statements, how, and what is not. |
| [`spec/`](spec/README.md) | The numbered statements themselves, extracted from reproducible OASIS archives with provenance. |

For protocol behaviour, conflicts resolve in this order:

1. the MQTT 3.1.1 or MQTT 5 specification — quoted per statement in
   [`spec/`](spec/README.md), which is the copy to cite from;
2. [`IMPLEMENTATION-GUIDE.md`](IMPLEMENTATION-GUIDE.md);
3. [`DESIGN.md`](DESIGN.md).

[`CONFORMANCE.md`](CONFORMANCE.md) records which statements are actually
verified; it reports coverage and does not itself grant authority.

Outside protocol behaviour, [`API-STABILITY.md`](API-STABILITY.md) governs the
public surface and [`RELEASING.md`](RELEASING.md) governs publication.

## Maintainer evidence and historical reports

Verification contracts explain how evidence is produced:

| Document | Scope |
| --- | --- |
| [`BENCHMARKING.md`](BENCHMARKING.md) | Paired A/B validity, harness overhead, latency semantics and acceptance thresholds. |
| [`MEMORY-BENCHMARK.md`](MEMORY-BENCHMARK.md) | Process-isolated memory scenarios, logical accounting and versioned thresholds. |
| [`FUZZING.md`](FUZZING.md) | Deterministic and Hypothesis campaigns with reproducible seeds and retained failures. |
| [`STABILITY.md`](STABILITY.md) | Reconnect, resource, soak and multi-broker interoperability gates. |
| [`RELEASING.md`](RELEASING.md) | Local candidate validation, publication and failure handling. |
| [`ROADMAP.md`](ROADMAP.md) | Remaining product and release work. |

Dated audits, experiments and release evidence live in
[`reports/`](reports/). Reports explain why a decision was made at a particular
commit; they are historical records, not current API contracts. Start with the
[report index](reports/README.md), the
[1.0.0rc1 release report](reports/RELEASE-CANDIDATE-1.0.0rc1.md) and the
[RC performance report](reports/PERFORMANCE-1.0.0rc1.md). Open scheduler
experiment contracts on this branch live in [`experiments/`](experiments/).

Repository automation notes for maintainers live in
[`CHATGPT-USAGE.md`](CHATGPT-USAGE.md).

## Documentation roadmap

Status labels describe documentation work, not product support. Planned entries
are intentionally plain text so the index never contains links to files that do
not exist.

### Available now

- **Getting started** — installation, broker connection, pub/sub and lifecycle.
- **Sessions and persistence** — MQTT 3.1.1/5 sessions, reconnect and SQLite.
- **Operations** — observability, sizing, backpressure, timeouts and shutdown.
- **Paho migration and compatibility** — staged adoption and exact VERSION2 scope.
- **Architecture, protocol and stability contracts** — maintained alongside code.

### Planned after the first RC

- **Transport and security recipes** — TLS, WebSocket and Unix-domain sockets.
- **MQTT 5 guide** — properties, enhanced authentication, Will messages, topic
  aliases and broker-negotiated limits.
- **Configuration and sizing guide** — workload-based queue, byte and inflight
  budget selection.
- **Troubleshooting and FAQ** — driven by reproducible RC feedback rather than
  speculative failure lists.

### Planned before 1.0.0

- **Stable API reference** — every Stable type, method, default and exception in
  a user-oriented reference.
- **Cookbook** — complete service, gateway, request/reply and graceful-shutdown
  patterns.
- **Broker compatibility matrix** — maintained broker, protocol, transport and
  Python coverage.
- **User-facing benchmarks** — reproducible environments, cross-client results,
  memory/latency guardrails and explicit interpretation limits.

## Maintaining this documentation

A user guide or contract describes current behaviour and changes with the code.
A report records one dated experiment or audit and is never rewritten to match
later behaviour. Superseding a report means writing a new report and updating
the report index.

Keep generated benchmark output outside the repository. Documentation may quote
reviewed results, but must identify the version, environment and methodology
that produced them.
