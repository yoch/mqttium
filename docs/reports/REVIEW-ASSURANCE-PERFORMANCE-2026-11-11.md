# Assurance and performance review — 2026-11-11

Review of MQTTium 1.2.1 at `af7004b`, oriented towards what the project can
*certify* to a user and towards performance. It ends with a work plan.

"Certify" is taken in the sense the maintainer asked for: guarantees about the
real behaviour of the implementation that are useful to an application —
correct concurrency, no possible hang, bounded waits, crash safety — and not
only clause-by-clause conformance to the OASIS text. No external scheme
(CRA, IEC 62443, a third-party test lab) is targeted; the review states what
evidence exists and what is missing for the project's own claims.

This report is a dated observation. It changes no code and no maintained
contract.

## 1. Scope and method

| Item | Value |
| --- | --- |
| Commit | `af7004b` (1.2.1), branch `claude/peaceful-galileo-uywfs4` |
| Host | Hosted Linux container, 4 vCPU, CPython 3.13.18, OpenJDK 21, no Mosquitto |
| Not available | Python 3.14/3.15, a broker, the ARM64 reference runner |

Executed here, all green:

- `ruff format --check` and `ruff check` over `src tests benchmarks tools`;
  `mypy src/mqttium`; `bandit -q -ll -r src`.
- `pytest tests/unit tests/project` with branch coverage: **2591 passed**,
  **93.85 %** (gate 89 %).
- `python tools/formal/run_tlc.py`: **61 of 61** configurations report their
  declared outcome (63 s).
- Micro-benchmarks that need no broker (section 6).
- Nine reproduction scripts for the behavioural findings (section 4). They
  drive the real `AsyncClient` over loopback TCP against a scripted peer, or
  over `tests/support.py::ScriptedBrokerTransport`. They are not committed;
  each finding gives the recipe, and the work plan turns each into a
  regression test, which is their proper home.

Not executed: Mosquitto integration, resilience and soak suites, runtime
fuzz campaigns beyond CI defaults, every network performance gate.

Each finding is marked **Reproduced** (script run during this review),
**Read** (code path read end to end), or **Suspected** (plausible, untested).

## 2. Existing assurance work

The prior "certification" work is a self-assessed conformance programme plus
release qualification evidence. It is solid in method and explicit about its
limits.

| Asset | What it gives | Its own stated limit |
| --- | --- | --- |
| `docs/conformance.md` | Audit method, 6 violations found and fixed, 1 deliberate deviation (`[MQTT-3.2.2-4]`), table of executable checks | "It is **not** a certification"; the rest of the statements "are not individually audited"; "The discovery rate is not yet zero" |
| `docs/spec/*.json`, `tools/extract_spec_statements.py` | 139 (3.1.1) + 251 (5.0) numbered statements extracted from hashed OASIS archives, with a `--check` drift mode | — |
| `tests/unit/test_conformance_statements.py` | Tests whose docstring quotes a statement; quotes are compared with the vendored text | "a sample, not a proof of full conformance" |
| `formal/models/` (19 TLA+ models, 61 configs) | Released-behaviour counterexample kept, repaired configuration passes | "never a proof of the Python code" |
| `tests/fuzz/runtime_*_fuzzer.py` | Schedule fuzzing of the real client with liveness oracles: checkpoint within 250 loop turns, whole-schedule watchdog, zero waiters and zero pending receipts at quiescence | Schedules listed in section 4.3 |
| `tests/project/` | AST checks: no `await` under `_engine_lock`, cancellation discipline, API surface, formal sidecars | Lock check parses `api/async_client.py` only |
| `docs/reports/` | Release qualification, audits, per-decision performance evidence | 1.0.0 shipped with an open performance reservation (section 6.1) |

Open items inherited from those documents:

- `conformance.md`: a broker that reuses an identifier before receiving the
  acknowledgement is not detected after the handoff (#541).
- `FINAL-1.0.0-PUBLICATION-CHECKPOINT-2026-09-28`: open-loop gate without
  valid closing evidence (#493), collector cost (#592), bounded-overload gate
  not implemented. No later report clears it.
- `RC17-AUDIT-CLOSURE-2026-09-28`: G02 (performance) open; G03, migration of
  a real downstream application, "not certified".
- `RELEASE-1.1.0`: fixes from "three outside audits" whose reports are not in
  the repository.
- `SIMPLIFICATION-AUDIT-2026-08-16`: two open questions left undecided
  (MQTT 5 properties silently dropped on 3.1.1; unused Stable enum members).

## 3. Protocol conformance

### 3.1 Measured traceability

A throwaway script crossed every `[MQTT-x.y.z-n]` label found in `tests/`,
`src/` and `docs/conformance.md` with the two statement indexes.

| Measure | Value |
| --- | --- |
| Distinct labels cited in tests | 41 (all under `tests/unit`) |
| MQTT 3.1.1 statements with a citing test | 19 of 139 |
| MQTT 5.0 statements with a citing test | 36 of 251 |
| Labels cited in `src/` | 24, of which 11 have no citing test |
| Labels in the `conformance.md` table | 29 |

Sections with no citing test at all, both versions: PUBREL/PUBCOMP (3.6,
3.7), SUBACK (3.9), UNSUBSCRIBE (3.10), UNSUBACK (3.11), PINGREQ (3.12),
ordering (4.6 has one), and for 3.1.1 the whole of 2.3 (packet identifiers)
and 4.3 (QoS flows). Many of these behaviours *are* exercised by unit tests;
they simply carry no statement label, so nothing can be claimed from them.

### 3.2 Findings

| ID | Sev. | Finding | Status |
| --- | --- | --- | --- |
| C1 | High | No generated statement-to-test matrix. The only matrix is the hand table in `conformance.md`, which no test checks. It omits 15 labels that tests cite (for example `MQTT-3.3.2-14`, `MQTT-4.3.2-5`, `MQTT-4.3.3-4`) and lists 3 labels no test cites (`MQTT-3.1.2-30`, `MQTT-3.1.3-8`, `MQTT-3.2.2-4`). | Measured |
| C2 | High | A label does not identify a statement: **119 labels exist in both specifications**, with different text. Coverage counted by label over-counts unless each citing test names the protocol version it runs. | Measured |
| C3 | Med. | The quote check reads only its own file (`test_conformance_statements.py:799`). Nine labels are cited only in other test files, and the 24 labels in `src/` are never checked against the vendored text. | Read |
| C4 | Med. | `extract_spec_statements.py --check` runs in no workflow, so index drift is not gated. | Read |
| C5 | Med. | No client-applicability classification. `conformance.md` estimates "roughly 290" statements within reach of a client from a "crude attribution pass"; without a per-statement class (client MUST, server-only, not testable) no bounded claim can be worded. | Read |
| C6 | Low | No labelled evidence in the integration, resilience or fuzz suites, although a real broker is the natural oracle for the QoS flow statements. | Measured |

## 4. Behavioural guarantees

This is the axis that matters most to an application. The question asked of
each public awaitable is: *what bounds this wait, and what does the user
observe if the bound is not met?*

### 4.1 What holds

- **No lock-order deadlock inside the library.** `_engine_lock` bodies never
  suspend, so the lock is never observed held; `EffectPump.lock` has a single
  acquirer; `_lifecycle_lock` is taken only by `connect`, `disconnect`, the
  reconnect loop and (as a barrier) the lifecycle worker. Task joins are
  acyclic: the writer, keepalive, flusher and auth tasks cancel the reader
  and never join it. (Read.)
- **No user code under a lock.** Message callbacks run on the reader outside
  locks; lifecycle hooks run in their own task; receipt callbacks use
  `call_soon`. (Read.)
- **Connection death is propagated** to parked publishers, writer waiters,
  pending SUBACK futures and receipts on terminal teardown — with the
  exception B5 below. (Read; covered by the runtime fuzzers' quiescence
  oracle.)
- **Cancellation of `publish`, `publish_many`, `subscribe`, `receipt.wait`,
  `connect`** leaves a consistent state, with a named regression test for
  each. (Read.)

### 4.2 Findings

| ID | Sev. | Finding | Status |
| --- | --- | --- | --- |
| B1 | High | **Application-level deadlock with `messages()`.** A consumer that awaits a QoS 1 receipt (or anything only the reader can deliver) inside `async for m in client.messages()` deadlocks permanently once the iterator queue is full: the reader is parked on queue space (`api/_delivery.py:234-281`, `iterator_admission_timeout` defaults to `None`), so it never reads the PUBACK. Keepalive does not rescue it, because the PINGRESP deadline is deferred while a delivery lot is active (`api/async_client.py:2287-2293`). Observed for 12 s with `keepalive=2`, `max_iterator_messages=4`: `state=CONNECTED`, `delivery.waiters=1`, `receipts.publish=1`, one PINGREQ sent, consumer never progresses. With `subscribe()` instead of a receipt it ends in `MQTTTimeoutError` after 30 s. No documentation warns about it. | Reproduced |
| B2 | High | **`keepalive=0` and a half-open peer hang everything with state `CONNECTED`.** Against a peer that sends CONNACK and then neither reads nor writes: QoS 0 `publish()` blocked after 19 calls, `subscribe(timeout=1.0)` still pending after 12 s, `receipt.wait()` pending. No TCP keepalive is set (`transport/tcp.py` sets only `TCP_NODELAY`). With `keepalive=1` the loss is detected after about 7 s. | Reproduced |
| B3 | High | **`publish()` and `receipt.wait()` take no timeout.** Because there is no in-session retransmission (invariant 7), a live broker that never acknowledges holds the receipt and its flow slot for the life of the connection. The bound is therefore the caller's own `asyncio.timeout`, which is nowhere stated as a requirement. | Read |
| B4 | Med. | **`subscribe(timeout=)` bounds only the SUBACK wait**, not the handoff to the writer (`api/async_client.py:1660, 1719`). Measured: `timeout=1.0` returned after 5.0 s with keepalive, never without. `docs/operations.md:195-199` documents it; the parameter name still promises more than it delivers. | Reproduced |
| B5 | Med. | **`disconnect()` racing reader teardown can leave a receipt pending forever and drop `on_disconnect`.** With a reconnect policy and a transport whose `close()` takes 0.3 s, a `disconnect()` that lands while the reader is at `api/async_client.py:2094` cancels it before `_fail_pending`: `receipt.is_done()` stays `False` 5 s after `disconnect()` returned, and `on_disconnect` is never called. On stock sockets the window is one or two loop turns. The runtime fuzzer does not reach it: its transport gates only the first `close()`. | Reproduced (injected slow close) |
| B6 | Med. | **Caller cancellation is swallowed inside `disconnect()`/`connect()` joins** (`api/async_client.py:1215-1218, 2805-2809, 2984-2987`). A task cancelled while `disconnect()` joins the reconnect task keeps running past it (`task.cancelling() == 1`, code after `disconnect()` executed). This breaks `asyncio.timeout` and `TaskGroup` semantics. `test_cancellation_discipline.py` allow-lists these handlers. | Reproduced |
| B7 | Med. | **SQLite contention blocks the event loop and can fail the client permanently.** Every store call is synchronous on the loop with a 5 s busy timeout. With another connection holding the write lock: one QoS 1 `publish()` raised `OperationalError` after 10.0 s with a 10.0 s loop stall; during inbound QoS 2 a 5 s stall ended in a terminal local failure — state `DISCONNECTED` despite a reconnect policy, later `connect()` refused. The persistence guide documents synchronous I/O, not these two outcomes. | Reproduced |
| B8 | Med. | **`connect(timeout=)` is per attempt.** With a `ReconnectPolicy` whose `max_retries` is `None` (the default), `connect(timeout=0.5)` to a refusing port was still pending after 5 s and 36 attempts. | Reproduced |
| B9 | Low | **Outbound replay and store hydration are O(N) without yielding.** Loop stall at CONNACK replay: 67 ms for 10 000 persisted records (the default budget), 394 ms for 60 000 on SQLite; the constructor's synchronous hydrate costs about the same. Inbound replay is batched (64 messages); outbound is not. | Reproduced |
| B10 | Low | **Loop confinement is enforced only in `publish_nowait` and `connect`.** `publish`, `subscribe`, `disconnect`, `ack`, `stats` called from another loop or thread are not detected. | Reproduced (no detection); consequences suspected |
| B11 | Low | **The keepalive loop catches only `CancelledError`** (`api/async_client.py:2278-2336`). Any other exception would end dead-peer detection silently. No raising path was found. | Suspected |
| B12 | Low | **Reader teardown is not hardened against a second canceller** (effect-flush failure, auth failure). A possible outcome is `RECONNECTING` with no reconnect task. | Suspected |
| B13 | Low | `MQTTTimeoutError` subclasses builtin `TimeoutError`, so `asyncio.wait_for(receipt.wait(), t)` cannot distinguish the caller's timeout from a receipt failed by a PINGRESP timeout. | Read |

### 4.3 Evidence gaps

| ID | Gap |
| --- | --- |
| G1 | **No liveness property is model-checked.** No `.cfg` declares a `PROPERTY` and no module states a fairness condition; only invariants are checked. Deadlock checking gives stuck-state freedom of the abstract model, not "every wait eventually ends". |
| G2 | **Models are small and unlinked to code.** Median state space is 6 distinct states (range 2 to 818 215). The link from a model action to the Python predicate is by review only; the sidecar names tests, but nothing checks that those tests replay the counterexample. |
| G3 | **TLC is not in the required gate.** `formal.yml` runs only for changes under `formal/` or `tools/formal/`, and is not among the `needs` of "CI required". A `src/` change that invalidates a model's predicates is not rechecked. |
| G4 | **The engine-lock AST test covers one file.** `api/_auth.py:105` also takes `_engine_lock`. More fundamentally, since the lock is never contended, safety rests on "no await inside a multi-step engine mutation", which the test only approximates. |
| G5 | **Fuzz schedules omit** application-consumer backpressure against receipts (B1), `keepalive=0` half-open (B2), a second `close()` gate (B5), SQLite lock contention (B7) and cross-loop calls (B10). |
| G6 | **No documented guarantee table.** A user cannot find, in one place, what bounds each awaitable and under which configuration. Section 4.4 proposes its shape. |

### 4.4 The guarantee register

The useful form of "certification" for this project is a short, numbered
register of user-facing guarantees, each with its precondition and its
machine-checked evidence, published next to `conformance.md`. Proposed
initial entries, with today's status:

| # | Guarantee | Today |
| --- | --- | --- |
| U1 | Every public awaitable ends within a stated bound once the connection is lost | Holds only with `keepalive > 0`; violated by B2, B5 |
| U2 | No wait cycle exists that the library can create on its own | Holds (read); not model-checked |
| U3 | Application backpressure cannot stall protocol liveness silently | Violated by B1 |
| U4 | A timeout argument bounds the whole call | Violated by B4, B8 |
| U5 | Cancelling any public awaitable is honoured and leaves consistent state | Violated by B6 |
| U6 | Every receipt settles: acknowledged, failed, or replayed after reconnect | Violated by B5; unbounded by B3 |
| U7 | The event loop is never blocked longer than a stated bound by the library | Violated by B7, B9 |
| U8 | A background-task failure always surfaces as a disconnect cause | Not shown (B11, B12) |
| U9 | Persisted QoS 1/2 state survives a process crash at any point | Covered by restart tests; no kill-point campaign found |
| U10 | Memory is bounded by configuration under any peer behaviour | Thresholds exist; MQTT 3.1.1 default accepts packets up to 256 MiB (A4) |

## 5. Release and supply-chain evidence

| ID | Sev. | Finding |
| --- | --- | --- |
| A1 | Med. | No SBOM, no build-provenance attestation, no OpenSSF Scorecard, no CodeQL. Trusted publishing, SHA-pinned actions, `zizmor`, `actionlint` and Dependabot are in place, so the remaining step is small. |
| A2 | Med. | No product threat model. `SECURITY.md` gives boundaries; the only threat model in the repository concerns the self-hosted runner. |
| A3 | Low | `mypy` runs without `disallow_untyped_defs`. |
| A4 | Med. | With MQTT 3.1.1 and no explicit `maximum_packet_size`, the decoder ceiling is `PROTOCOL_MAX_PACKET_SIZE` (268 435 460 bytes) (`api/async_client.py:407-423`). The choice is deliberate and commented; it is still a memory-exhaustion exposure to a hostile or faulty broker and deserves a line in the security boundaries. |
| A5 | Low | The reports of the three outside audits behind 1.1.0 are not archived. |
| A6 | Low | `docs/reports/README.md` says every report appears once; seven do not (`FORMAL-INBOUND-SESSION-OWNERSHIP-2026-09-24` and six `MISSION-*`). No test checks it. |
| A7 | Low | Stale wording: "pre-v1" in `AGENTS.md` and in the `llmstxt` description of `mkdocs.yml` ("published as 1.0.0rc17"); `persistence/sqlite.py:309` says "this experiment requires schema 5"; `docs/reference/helpers.md` is published but in no nav; an orphan Paho comment remains in `pyproject.toml`. |
| A8 | Low | `AGENTS.md` and `CONTRIBUTING.md` lint `src tests benchmarks`; CI also lints `tools`. |

## 6. Performance

### 6.1 State of the evidence

- The measurement contract (`docs/benchmarking.md`) is rigorous: fresh-process
  ABBA pairs, A/A neutrality within 2 %, baseline CV at most 5 %.
- **P1 (High).** No performance check runs on pull requests. Strict gates are
  manual on the ARM64 runner; the only scheduled pass/fail check is the weekly
  memory threshold.
- **P2 (High).** The 1.0.0 performance reservation is still open (#493, #592,
  bounded-overload gate). 1.1.0 and 1.2.0 qualify against the ARM64 network
  gate and paired regression, not against the reserved open-loop method.
- **P3 (Med.).** `RELEASE-1.2.0` leaves about 13 µs per received QoS 1
  message against gmqtt unexplained beyond "path depth" (W1c/W2b not pursued).

### 6.2 Measurements from this review

All figures are **indicative**: hosted container, CPython 3.13.

**The container cannot rank small optimisations.** `paired_regression.py`
run with base and candidate on the *same* tree, 3 pairs:

| Scenario | A/A median ratio | Base CV |
| --- | --- | --- |
| `qos1_cycle_memory` | 0.959 | 2.9 % |
| `qos1_cycle_sqlite` | 0.980 | 4.2 % |
| `qos2_cycle_sqlite` | 1.061 | 5.7 % |
| `native_publish_nowait_qos0` | 0.938 | 6.9 % |
| `ingress_publish_qos1` | 0.924 | 13.4 % |
| `delivery_callback` | 0.785 | 22.3 % |

A/A error of 4 to 22 % against a 2 % acceptance threshold: timing claims
below roughly 20 % must come from the reference runner. Only deterministic
counts and large ratios are reported here.

**Persistence dominates durable QoS.** Engine-level QoS 1 cycle, 4 KiB
payload: about 65 000 cycles/s on the memory store against about 11 000 on
SQLite — roughly 75 µs of the 90 µs is the store.

**SQLite statements per cycle** (counted with `set_trace_callback`,
deterministic):

| Cycle | Unbatched | Inside one `batch()` |
| --- | --- | --- |
| QoS 1 publish → PUBACK | 2 `BEGIN`, 2 `COMMIT`, 1 `INSERT`, 1 `SELECT`, 1 `DELETE` | 0 `BEGIN`/`COMMIT` per cycle, same 3 statements |
| QoS 2 publish → PUBCOMP | 3 `BEGIN`, 3 `COMMIT`, 1 `INSERT`, 2 `SELECT`, 1 `UPDATE`, 1 `DELETE` | same 5 statements |

Throughput with tracing on, same host: QoS 1 4 337 → 6 872 cycles/s and
QoS 2 3 599 → 7 154 cycles/s when 256 cycles share a transaction. In the
runtime, the acknowledgement side already shares the reader lot's
transaction; `put_out` from `publish()`, `mark_in_delivered` and `ack()`
each commit alone.

### 6.3 Candidates, ranked by expected value

Each is a hypothesis until measured under the benchmarking contract.

| ID | Candidate | Basis |
| --- | --- | --- |
| P4 | Group the publish-side `put_out` commits of one loop turn (or of one `publish_many`) in a single transaction, keeping "persisted before wire" | Measured: 1 of 2 commits per QoS 1 cycle; +50 to +100 % engine-level |
| P5 | Replace `SELECT` + conditional `UPDATE`/`DELETE` by one statement with `RETURNING` (SQLite 3.35+) in `complete_out`, `transition_*`, `complete_in` | Measured: 1 of 3 statements per QoS 1 cycle |
| P6 | `__slots__` on `AsyncClient` (62 instance attributes, above CPython's 30-entry inline layout — the same cliff that cost `WritePump` about 18 % in 1.2.0). `EffectPump` (23), `ProtocolEngine` (23), `ApplicationDelivery` (22) are below it and unlikely to gain. | Counted by AST; gain unmeasured |
| P7 | Skip the persisted-row probe (`in_meta`) per inbound QoS 1/2 PUBLISH when no row can match | Read: `protocol/inbound.py:315-319, 457-466` |
| P8 | One coalesced acknowledgement write per reader lot instead of one queue item per PUBACK | Read: `api/_writer.py`; related to the declined W2b |
| P9 | Inline the 1–2 byte length fast path in `IncrementalDecoder.next_packet`, as `peek_packet_bounds` already does | Read: `codec/buffer.py:289-331, 412-478` |
| P10 | Avoid the second pass in `Properties.__post_init__` for decoder-built tables; cache encoded topics on the publish path; drop `QoS(qos)` enum calls from admission | Read |
| P11 | Extra copy per received chunk on TLS, Unix, WebSocket and third-party loops (pull path through `StreamReader`) | Read: `transport/tcp.py:32-41` |

`benchmarks/micro_baseline.py` builds a decoder per iteration and so measures
setup as much as decode; `HOTPATH-RECON-2026-09-06` and
`FLOORS-NOT-CEILINGS-2026-08-16` cite reproduction scripts that no longer
exist.

## 7. Work plan

Lots are ordered by value to a user. Size: S under a day, M a few days, L a
week or more. Every behavioural fix starts with the failing regression test
built from the reproduction recipe in section 4.

### Lot 1 — Behavioural guarantees (highest priority)

| Step | Work | Acceptance | Size |
| --- | --- | --- | --- |
| 1.1 | Commit one failing test per reproduced finding B1, B2, B4, B5, B6, B7, B8 (marked `xfail(strict=True)` until fixed) | Tests fail for the stated reason on `af7004b` | M |
| 1.2 | **B1**: decide and implement — keep the PINGRESP deadline running while only the *application* holds the lot, or give `iterator_admission_timeout` a finite default, or fail the connection with a named cause; document the pattern "do not await the reader from inside `messages()`" and the safe alternative | B1 test ends within a stated bound with a named error | M |
| 1.3 | **B2**: enable `SO_KEEPALIVE` on TCP, and state in the contract that liveness bounds require `keepalive > 0`; consider warning on `keepalive=0` | B2 test bounded, or documented as an explicit opt-out | S |
| 1.4 | **B3/B4/B8**: one deadline per call — add `timeout=` to `publish()` and `receipt.wait()`, move the `subscribe` handoff under its timeout, make `connect(timeout=)` cover the retry loop or rename the semantics | Each timeout bounds its whole call; changelog and migration note | M |
| 1.5 | **B5**: settle receipts and schedule `on_disconnect` from `_force_close_transport` when the reader is cancelled mid-teardown; add a second-`close()` gate to the fuzz transport | Fuzzer's `receipts.publish == 0` oracle covers it | M |
| 1.6 | **B6**: re-raise the caller's cancellation after each join; shrink the allow-list in `test_cancellation_discipline.py` | E7 scenario: task ends cancelled | S |
| 1.7 | **B7**: decide whether `SQLITE_BUSY` is transient; bound the loop stall (shorter busy timeout, or retry outside the reader lot); document the outcome | Two-connection test fixes the behaviour | M |
| 1.8 | **B11/B12**: generic handler in the keepalive loop; fuzz mutation cancelling the reader at each await of its teardown | New mutation passes | S |
| 1.9 | **B9/B10**: batch outbound replay with a yield; reuse the owner-loop check in awaitable entry points | Loop-lag assertion at 10 000 records | M |

### Lot 2 — Make the guarantees checkable

| Step | Work | Acceptance | Size |
| --- | --- | --- | --- |
| 2.1 | Publish the guarantee register (section 4.4) as a maintained page; each entry names its precondition and its tests, fuzz oracle or model | A project test fails if an entry names a missing test | M |
| 2.2 | Add liveness to the models that own a wait: `PROPERTY` with weak fairness for "every receipt eventually settles", "every parked publisher eventually wakes or fails", "reader teardown eventually completes" | TLC checks at least three temporal properties | M |
| 2.3 | New models for the unmodelled waits: consumer backpressure against receipts (B1), cancel during reader teardown (B5), keepalive under delivery pressure | Released configuration reproduces B1 and B5 | M |
| 2.4 | Put TLC in "CI required" for changes under `src/mqttium/api` and `src/mqttium/protocol` (63 s measured) | `formal` is a `needs` of the required job | S |
| 2.5 | Extend the engine-lock AST test to every module under `api/`; add a check that sidecar-named tests exist and are collected | Covers `_auth.py` | S |
| 2.6 | Extend the runtime fuzzers with the schedules of G5 | Mutation switches confirm each new oracle fires | L |
| 2.7 | Crash-safety campaign for the SQLite store: kill at every statement boundary, reopen, check invariants | Supports U9 | M |

### Lot 3 — Conformance traceability

| Step | Work | Acceptance | Size |
| --- | --- | --- | --- |
| 3.1 | Generate the statement-to-test matrix from the tree, keyed by (version, label); fail when `conformance.md` and the tests disagree | Closes C1, C2 | M |
| 3.2 | Extend the quote check to all of `tests/` and to labels in `src/`; run `extract_spec_statements.py --check` in CI | Closes C3, C4 | S |
| 3.3 | Classify every statement: client obligation, client-observable server obligation, server-only, not testable | Closes C5; gives the denominator | M |
| 3.4 | Label existing tests section by section, starting with QoS flows (4.3), packet identifiers (2.3), SUBACK/UNSUBACK, then add missing checks; use Mosquitto as oracle where relevant | Stated coverage of client obligations, per version | L |
| 3.5 | Close #541 or record it as a bounded deviation | — | S |

### Lot 4 — Release evidence and hygiene

| Step | Work | Size |
| --- | --- | --- |
| 4.1 | SBOM and build-provenance attestation in `publish.yml`; Scorecard and CodeQL workflows | S |
| 4.2 | Product threat model (hostile broker, hostile network, local store), linked from `SECURITY.md`; include A4 | M |
| 4.3 | Archive or summarise the three outside audits of 1.1.0 | S |
| 4.4 | Index the seven missing reports and test the "appears exactly once" claim | S |
| 4.5 | Fix the stale wording of A7 and the lint-scope mismatch of A8 | S |

### Lot 5 — Performance

| Step | Work | Acceptance | Size |
| --- | --- | --- | --- |
| 5.1 | A deterministic PR gate: call counts, allocation counts and SQLite statement counts per scenario (noise-free, runs on hosted CI) | Fails on a count regression | M |
| 5.2 | Close or formally retire the 1.0.0 reservation: implement the bounded-overload gate, rerun on the reference runner | New report supersedes the reservation | L |
| 5.3 | P4 and P5 (SQLite), each with a crash-safety argument for durability ordering | ≥ 5 % at two load points, contract thresholds | M |
| 5.4 | P6 (`AsyncClient` slots), then P7 to P10, one change per paired run | ≥ 2 %, 8 of 11 pairs | M |
| 5.5 | Repair `micro_baseline.py`; note the missing reproduction scripts in the report index | — | S |

Lots 1 and 2 are what turn "no known counter-example" into guarantees a user
can rely on; lot 3 gives the conformance claim a denominator; lot 5 should
wait for 5.1, so that later changes are judged by a gate rather than by a
noisy host.

## 8. Not covered

- Network performance, soak, and long fuzz campaigns.
- Python 3.11, 3.12, 3.14 and 3.15; macOS and Windows.
- WebSocket and TLS transports beyond reading.
- A line-by-line audit of unlabelled statements against the engine.
- Behavioural findings B11 and B12 were not reproduced.
