# ADR 0044 — Reactor runtime, two evidence tiers and frozen receptions

Status: **Accepted** (20 November 2026, decided with the repository owner
after the audit of `bf9f702`). Applied by the runtime lots that follow it.

Amended on 21 November 2026 after the independent audit of `8e4303d`: two
statements of the paragraph "Runtime" below describe an intention the
executable does not meet. MQTT decoding and encoding run on the core thread;
the I/O threads own sockets, TLS and framing. Three bounded 100 ms rescans
remain (pending work, Wills, message expiry); the first is needed until a
mark per session replaces it, because a full ordinary lane makes the runtime
forget which session has work. No loop sleeps between two passes any more.

## Context

The audit of `bf9f702` compared the product executable with the profile of
chapter 1, section 3. The pure core, the store contract and their evidence
are sound. The executable around them is a bounded prototype:

- `runtime.rs` accepts 16 connections. One reactor thread polls every
  connection and sleeps one millisecond per turn. Plain TCP uses two
  threads per connection and TLS one.
- Every store command is one `synchronous=FULL` transaction. There is no
  bounded batch and no Volatile backend, which ARC-020 both provide for.
- Report 0024 measured about 850 QoS 0 and 800 QoS 1 messages per second
  against about 120,000 and 47,000 for Mosquitto 2.0.21 on the same host.
- The oracle campaigns exercise the core and the stores through the lab's
  own codec. The reactor, `codec.rs`, TLS and the writer thread are outside
  that qualification, and no model describes their threads and queues.
- Each increment of the product carried a sealed reception. About twenty
  reception jobs assert the quotas and the event stream of the bounded
  preview, so they fail as soon as a quota or the runtime changes.

## Decision

**Target.** The work is directed at the initial Mosquitto replacement
profile of chapter 1, section 3, released as version 0.1 when a declared
compatibility matrix against Mosquitto passes at comparable throughput.

**Runtime.** The network runtime is rebuilt on readiness-driven I/O
reactors using `mio`, without `async`. I/O threads own sockets, TLS,
framing, decoding and encoding. One core thread owns the single `Owner` and
its ordered inputs. One thread owns the store. Threads exchange values
through bounded lanes and wake each other explicitly; no loop polls on a
timer for work. This departs from ARC-021, which proposes a multithreaded
Tokio runtime. The requirements of ARC-021 that do not name Tokio stand:
bounded tasks and queues, one effective writer per connection, a dedicated
storage thread, reserved control capacity and no wait on a full remote
queue. A hand-written loop is chosen because its scheduling can be modelled
exactly and replayed deterministically.

The single logical owner stays for version 0.1. Partitioned owners of
ARC-001 remain a later decision; nothing here forecloses them.

**Evidence tiers.**

| Tier | Paths | Regime |
| - | - | - |
| Core | `contracts/`, `crates/mqttiumd-contract/`, `crates/mqttiumd-core/`, `crates/mqttiumd-store-api/`, `crates/mqttiumd-store-sqlite/`, the deterministic store, `tests/oracle/`, `tests/scenarios/`, `proofs/` | Unchanged: decision record for a contract change, oracle, kept mutants, proofs, defect fixed test first |
| Runtime | `crates/mqttiumd/`, `tests/compat/`, runtime models under `models/`, load tools | Test first. Differential scenarios against a pinned Mosquitto, TLA+ models of the thread and queue protocol with mutant configurations, property tests and fuzzing of the codec, load measurements with recorded commands. No sealed reception per increment |

A runtime change that alters a public type, a durable encoding or an
injection point of the core tier is a core change and follows that regime.
A behaviour change still never ships together with the weakening of the
test that covers it.

**Frozen receptions.** The system receptions, preview packages and pilot,
site, split, forward and cutover kits are historical evidence of the
qualified executable built from `51be4079f7ee42f46500e993315eddc971f0a8d4`.
They stay in the repository and run on demand only. They are not
maintained as the runtime and its limits change. Report 0026 lists them.

**Build profile.** `[profile.release]` stays as the run manifests record
it. The distributed executable is built with a separate `dist` profile.

## Consequences

- `mqttiumd/AGENTS.md` states the two tiers.
- The required CI of the broker is `contracts`, `models`, `pilot proof` and
  `runtime`. The frozen jobs move to `mqttiumd-frozen-receptions.yml` and
  the four preview workflows to manual dispatch.
- A frozen reception that fails after a runtime change is not a detection
  and is not repaired. A regression it would have caught is covered by a
  runtime test or a compatibility scenario before the behaviour changes.
- The decision records 0010 to 0043 keep their status; their acceptance is
  a separate decision of the owner.
- Open: the measured targets of version 0.1 are fixed with the first load
  report on the new runtime (VAL-006, ARC-025).
