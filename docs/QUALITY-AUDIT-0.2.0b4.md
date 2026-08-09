# Quality baseline for 0.2.0b4

This document freezes the quality baseline used for the `0.2.0b4` work. It is
an inventory, not a claim that every item below must be redesigned. In
particular, line count and McCabe complexity are warning signals: a change is
useful only when it makes an invariant clearer or removes measured work.

## Reference and reproduction

- Reference commit: `dc13866cad99c55696da7d7622a99e81dd7d6f37`
- Reference date: 2026-08-10
- Last complete reference CI: green on Python 3.11 through 3.14, including
  Ruff, mypy, Bandit, fuzzing and packaging
- Unit coverage reported by that reference run: 86.87%

The physical-line snapshot can be reproduced without installing a tool:

```console
find src/mqttium -name '*.py' -print0 | xargs -0 wc -l
find tests -name '*.py' -print0 | xargs -0 wc -l
find benchmarks -name '*.py' -print0 | xargs -0 wc -l
```

Complexity is now checked by the normal CI command, `ruff check src tests
benchmarks`. To inspect only the ratchet use `ruff check src tests benchmarks
--select C901`.

## Size and ownership snapshot

| Area | Python files | Physical lines | Non-blank lines | Functions | Async functions | Classes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Runtime (`src/mqttium`) | 53 | 11,389 | 10,103 | 610 | 75 | 97 |
| Tests | 88 | 13,477 | 10,863 | 881 | 214 | 53 |
| Benchmarks | 19 | 5,609 | 4,908 | 238 | 34 | 22 |

The largest runtime modules are:

| Module | Physical lines | Primary responsibility |
| --- | ---: | --- |
| `api/async_client.py` | 2,148 | Public client plus runtime orchestration |
| `protocol/outbound.py` | 990 | Outbound MQTT state machine |
| `persistence/sqlite.py` | 934 | Durable session store |
| `compat/paho.py` | 780 | Threaded Paho-compatible facade |
| `protocol/inbound.py` | 736 | Inbound MQTT state machine |
| `protocol/engine.py` | 663 | Protocol state and session coordination |
| `transport/websocket.py` | 462 | WebSocket transport and framing |

`AsyncClient` is the principal concentration risk. Its size is partly
legitimate ownership of event-loop resources, but additions should first be
placed in the component that owns the invariant (engine, effect pump, writer or
transport). Splitting it solely to reduce this table is not a goal.

## Complexity ratchet

McCabe complexity is limited to 15. The reference contains ten runtime
exceptions, recorded locally on their definitions instead of exempting whole
files:

| Function | Reference complexity |
| --- | ---: |
| `AsyncClient._apply_effect` | 44 |
| `compat.paho.Client._drain_publish_requests` | 24 |
| `AsyncClient.__init__` | 23 |
| `AsyncClient._read_loop` | 21 |
| `transport.websocket._parse_frame` | 20 |
| `EffectPump._run_scheduled` | 19 |
| `ConnectPacket.encode` | 17 |
| `InboundSession.on_pubrel` | 17 |
| `codec.properties._encode_value` | 16 |
| `InboundSession.ack` | 16 |

Four non-runtime exceptions also predate the ratchet:
`paired_regression._worker` (41), `fuzz_engine` (30), `fuzz_websocket` (16),
and the concurrent Paho integration test (18).

The remaining inline `# noqa: C901` comments are the complete current
allowlist; the table preserves eliminated entries as part of the reference
baseline. New exceptions are not permitted. When an exempt function changes,
its complexity must fall; the exemption is removed as soon as it reaches 15.
A mechanical helper that only moves branches without clarifying ownership does
not satisfy this rule.

## Coupling and allocation-sensitive boundaries

Counting distinct internal modules imported directly gives the widest static
fan-out to `api.async_client` (19), `protocol.engine` (16),
`protocol.outbound` (16), `protocol.inbound` (13), and `packets.publish` (9).
This is a directional signal rather than a layering violation by itself.

Two private seams deserve characterization before optimization:

- the Paho facade calls eleven private `AsyncClient` operations for
  loop-confined admission, finalization, callbacks and reconfiguration;
- inbound/outbound sessions deliberately call the engine's private `_send`,
  `_emit` and size checks, while outbound transactional rollback also truncates
  the engine effect list.

These seams are internal and may be simplified, but no Paho-specific state or
method should enter the native public client. Replacing direct calls with a
generic abstraction is not automatically an improvement: the proposed seam
must reduce ownership ambiguity, branches or measured allocations.

Allocation-sensitive ownership is already split between bounded outbound
writes, effect queues, inbound persistence and delivery reservations. The
performance work must measure copies, tasks and wakeups on these boundaries;
per-message diagnostics must not be added to the production hot path solely to
make measurement easier.

## API and test protection

The stable surface is defined in `API-STABILITY.md` and executable in
`tests/unit/test_public_api_surface.py`. It snapshots root/API exports, all
constructor keywords and defaults, and parameter names for stable client
methods. This audit changes neither stable nor provisional runtime APIs and adds
no runtime dependency.

Existing characterization already protects the first refactoring targets:

- effect ordering, stale-epoch discard, cancellation and scheduled wakeups;
- QoS 0 direct emission and fallback when `on_publish` is installed;
- Paho mixed-QoS ordering, bounded batches, producer races and completion;
- inbound QoS 1/2, manual acknowledgement, replay and byte budgets;
- WebSocket fragmentation, masking, control frames and malformed input;
- MQTT property limits and caller-facing `ProtocolError` normalization.

Add a test before a refactor only when its intended invariant is absent. Tests
that merely mirror branches increase maintenance cost without strengthening the
contract.

## Pull-request evidence

Every quality or performance PR based on this baseline records:

1. production and test lines added/deleted/net;
2. McCabe values before and after for each touched hotspot;
3. stable/provisional/internal API impact;
4. focused correctness results and full CI result;
5. paired performance and memory evidence, or `not applicable` with a reason.

Issue #39 remains the long-lived ledger. Refer to it with `Refs #39`; do not use
a closing keyword.
