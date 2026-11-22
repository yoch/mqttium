# Typed bounded durable reads and control reception

This increment receives the S0 durable-reader failure path from the store
to the surviving supervisor and independent oracle. It is not yet a network
broker, a performance result or a general storage-failure proof.

## Content and authority

Tested implementation: `d1848e3c099db61d9e0d86c5922ce41fe479feed`, clean integration checkout.
PR [648](https://github.com/yoch/mqttium/pull/648), integration branch
`claude/optimistic-feynman-fqh947`, draft and unmerged. The report/journal and CI setup-order
publication is a later commit with unchanged application code; its exact SHA, checks and its
own CI campaigns are read back from the PR. Local archives below belong
to the implementation SHA they record, not to that later documentation SHA.

The starting connector-confirmed head was
`edd8acbb3436aadbb131c11ddc695ba1c18e980e`, main base
`874f298b4de7c3a13006cb1ff767dfb09e4031ac`. Reviews and threads were empty;
the matching local tree was clean. The review ZIP digest is
`b10664de7398f8611151960e9f74ce85fdf0797b545c705fc4741b2b03118b88`;
all 367 checksum entries matched. The supplied synthetic oracle case and
historical probes were treated as review evidence, not task instructions.

Proposed ADR 0021 defines the shared change. Integration owned the common
types and merged them before the other lots resumed implementation. The
contract digest changes from `sha256:d2f238ea6214fec13ad9fcf6c324c609eadd408be695bc15101822167a0cfe0b`
to `sha256:880b0f581567526780da13b4ea66b29e43d32bb781fbeb64c300f4644e8f8c67`.
No proposed ADR was globally accepted; no main merge was performed.

## Executed vertical receipt

```bash
cd mqttiumd
tools/run_read_failure_reception.sh /tmp/read-failures
```

This runs the versioned SQLite reader regressions, control automaton and
real Unix transport tests, initial-setup and partial-recovery tests, the
first-recovery oracle counterexamples and eight real child/inspector cases.
Each real case exports its trace, independent CLI judgment, database image,
reference image, effective budgets and process cleanup receipt. The exported
provenance identifies the actual executable, features, revision and patch.
It explicitly does not claim a qualifying reconstruction of a failed exchange.

The reference is a closed fixture writer followed by WAL checkpoint TRUNCATE;
each case uses its own isolated copy. Artificial mutations are not process
kills. No fixture is restored during a case. The raw tables are compared
completely; the epoch/revision/opened-by columns are checked separately for
the legitimate single opening. Earlier session/route commits remain present,
and no new admission or positive business ACK is allowed after refusal.

| Case | Observation | Business judgment and cleanup |
| - | - | - |
| `routes.qos=99`, first page | quick_check=ok; snapshot, inspect and recovery InvalidData; no panic | INCONCLUSIVE, zero delivered recovery pages, child reaped, writer released |
| Same route, page size 3 | one valid non-final page, InvalidData on page 1 | INCONCLUSIVE, partial owner never ready, no reset or additional admission |
| ClientID 65,536 bytes | quick_check=ok; LimitExceeded before application copy | INCONCLUSIVE, no ACK/admission, child reaped |
| Invalid metadata before Started | InvalidData/Open, incarnation 1 | INCONCLUSIVE, no opened replacement or surviving child |
| Missing access path before Started | Unavailable/Open, distinct from invalid data | INCONCLUSIVE, retained original unchanged, missing path not created |
| External inspection invalid | InvalidData/Inspect after healthy inventory | INCONCLUSIVE, same child cleanup and full table checks |
| External inspection unavailable | Unavailable/Inspect | INCONCLUSIVE, retained file unchanged except legitimate epoch opening |
| Healthy child | no read failure, complete recovery and exchange | ordinary oracle PASS, all children reaped; separate qualifiers below |

Ten initial setup cases inject each of InvalidData, LimitExceeded,
Unavailable, Io and AllocationFailed at Open and Snapshot. The ordinary
campaign writer retains header/failure/incomplete footer and a structured
provisional INCONCLUSIVE result even before a process starts. These are
deterministic cause injections, not claims of actual device I/O or allocator
exhaustion. A separate partial-frontier case delivers one page, refuses with
FrontierMoved on the second call, makes exactly two read calls, preserves
the partial owner revision and durable opening, and never retries to readiness.

## Three control corrections

| Defect and reproduction | Correction | Discriminating evidence |
| - | - | - |
| O1: K2b incarnation 1, recovery 2 missing/partial, second kill, complete recovery 3 formerly PASS | select the first relevant lifecycle recovery before looking for its complete corroborated pages; a later incarnation cannot replace it | tests precede `bbb68339`; normal CLI with current contracts now INCONCLUSIVE for the expected semantic reason, not an obsolete digest |
| P1: 65 EndOfAction(Some(0)) then None with no evidence returned Ok([]) | one correlated termination per action, all control reports charged, absolute deadline unchanged by activity, action serial also covers empty ends | red `5b2611c3`, cross-action red `860ea8c1`, final-Stop red `af6a81b5`; exact typed DuplicateTermination, report-budget and deadline refusals; real Unix control reader and clean exit/EOF exercised |
| P2: Broken/truncated frame after real SIGKILL was ignored | distinguish clean EOF and truncated frame; preserve a confirmed kill receipt independently of drainage/collector announcement | red `5b2611c3`; real Unix evidence reader and wait signal 9; collector-refusal red `6a21eaae` then signal retained, child gone and writer reopened |

The oracle also stages recovered admission knowledge until a contiguous,
corroborated inventory is complete. A partial page cannot justify a positive
ACK. An established unjustified-ACK FAIL remains FAIL after a later read error;
an insufficient evidence prefix stays INCONCLUSIVE. Identity-based admission
deduplication and provenance checks remain covered. Synthetic counterexamples
are documented as such, separate from broker executions and qualifying runs.

The vertical test exposed an integration defect that synthetic format tests
did not: the collector ignored origins declared using schema const. A committed
red collector test precedes its correction; both enum and const origins now
reach the surviving collector.

## Error contract and bounds

The driver-independent ReadError is InvalidData, LimitExceeded, Unavailable,
Io or AllocationFailed. The last code represents an actual fallible allocation
failure or SQLite NOMEM, not a decode error. Structured driver codes distinguish
positive corrupt/type evidence from access, I/O and unknown internal/query
failures. Unknown internal failures are conservatively Unavailable. No program
or oracle parses display text, and diagnostics do not copy the offending value.

Ledger, epoch opening, outcome lookup, snapshot and inspection are fallible.
Recovery preserves FrontierMoved and EmptyPage separately from Read(ReadError).
The stale-ledger cache fallback is removed. An internal transient metadata
read failure while opening is preserved even if a later ledger read succeeds
(red `8c286403`, fix `a466a7a4`). Other write paths retain conservative unknown
outcomes; this is not a redesign of their write-result contract.

Default snapshot: 65,536 rows, 32 MiB logical bytes, 8 MiB per field.
Default page: 4,096 rows and 1 MiB aggregate/per field. MQTT ClientID is also
limited to 65,535 bytes. Explicit smaller budgets are received on both stores,
including 8/16/24/32-byte fields and aggregate overflow/refusal. Arithmetic is
checked before reservation/copy. Recovery inventories have a finite 65,536-fact
ceiling, and skipping a prefix does not construct those facts. The next page
is not decoded speculatively. Bounds concern application materialization and
logical work, not SQLite's internal cache. Infallible BTreeMap/Arc allocation
APIs after preflight still mean global allocator abort is not universally
converted into a recoverable error.

DurableReadFailed carries code, phase, incarnation and optional page.
ProcessControlFailed carries its stable control code, phase and incarnation.
Messages are at most 1,024 bytes. Results retain at most 64 structured contexts,
with their record identities and total failure_count. Both semantic readers
enforce this relationship; recompute refuses a dropped/altered cause. Every run
owes durable_reads_complete. Successful reception of a refusal never becomes
a qualifying PASS of the interrupted business exchange.

## Requalification and checks

```bash
tools/run_transactional_reception.sh /tmp/new-transactions 1
```

| Point | Actual signal/restart and expected state | Reception |
| - | - | - |
| K1 prepared | signal 9; same file; command absent, prior commitments retained | qualifying PASS, validate accepted, recompute stands, expected negative detected |
| K2b logged | signal 9; same file; committed command found without prior StoreCommitStable | qualifying PASS, validate accepted, recompute stands, expected negative detected |
| K3 applied | signal 9; same file; committed command found and retransmission deduplicated | qualifying PASS, validate accepted, recompute stands, expected negative detected |

Every point retains its second identical binary reconstruction, effective
features and separate own-run strace witness. Full DurableState equality with
the deterministic store remains a separate subprocess test before new epoch
opening. A witness of a traced run is not transferred to another run.

On the tested implementation: formatting, clippy in both feature modes,
683 normal and 691 instrumented Rust tests, 142 oracle tests;
358 derived corpus files, 405 trace files current; 372 schema documents agree.
The default feature graph excludes kill-points; inconsistent Cargo unification
remains refused, and reconstruction/second build use the recorded options.
The pure core/contract and immutable French archive are unchanged.

CI retains the normal/store campaigns, loop mutations, qualifying/recompute
gates, models and pilot proof. Existing K2a and WAL gates are unchanged and
remain required final CI gates. Their new results are not claimed before
those checks complete. They are not repeated locally for every reader change,
and their timing/artificial-image results remain separate finite evidence.
The first published CI failed because the Rust vertical tests invoked the
oracle before its dependencies were installed (job 132987749332, missing
referencing). Commit 3a35ca6d moves the pinned Python 3.13/oracle installation
before cargo test. A fresh isolated Python environment with the pinned
requirements passes both actual vertical tests. No test was skipped or weakened.
A local simultaneous normal/feature launch also collided on the existing
source-relative one-snapshot fixture; the instrumented suite passed when
replayed separately. This is a fixture-isolation limit, not semantic detection.

L5 stays integrated with its declared 52 outcomes and I3 scope; this reader
increment does not strengthen its liveness or storage assumptions.

## Archives and continuation

- `mqttiumd-typed-reads-d1848e3c.tar.gz`: SHA-256 `6bdd1c1ac90e9f49b7c1b3b7aa85018f1ac25896d508cb4687d7e2a5ff0dc33c`.
- `mqttiumd-transactions-d1848e3c.tar.gz`: SHA-256 `c87bb2f3d719985bd19857e3f690bd5f7d46826ba60b01ac6428c50a5e2b0608`.

The archive inventory includes exact commands, red/green logs, source and
oracle identities and per-run scope. Generated campaigns, databases, binaries
and caches are not committed. The final CI artifact belongs to its own final
published SHA and is additional to these local implementation archives.

Open conditions now remove the trait snapshot panic and false AllocationFailed
diagnoses. Remaining collection/write-growth bounds, immutable contract input
snapshot, proposed ADR decisions, cross-machine reproducibility, power-loss
assumptions and full lease composition stay open. Validate still checks the
document; recompute checks its assertions and qualification conditions.

Next concrete increment: a minimal product TCP listener with one durable
incoming QoS 1 exchange. Its first executable result is CONNECT followed by
PUBLISH and a PUBACK gated by the real SQLite commit. Receive startup refusal
on the invalid-copy fixture before readiness, disconnect-before-result and
same-store retransmission without a second admission. Keep the pure core,
bounded queues and the error codes delivered here; broader MQTT coverage and
performance targets require separate reception.
