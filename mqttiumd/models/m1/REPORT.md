# Campaign report — M1

One report per campaign. A result belongs to the exact content it was run
on; when a module or a configuration changes, the campaign is run again
and this file is rewritten.

## Campaign of 15 November 2026

| | |
| - | - |
| Models | `M1a.tla` (sidecar `M1a.json`, 18 configurations), `M1b.tla` (sidecar `M1b.json`, 19 configurations) |
| Base | Integration branch at `b4c80fb`, runner with `--models-dir` at `785c5fc` |
| Contract digest | `sha256:7a869371e38d0bd344e49bf8928905ef0d59cebe932f7df3ce0cb2530bfb47cb` |
| TLC | 2.19 of 08 August 2024 (rev 5a47802), `tla2tools.jar` 1.7.4, digest pinned by `tools/formal/run_tlc.py` |
| Java | OpenJDK 21.0.12, 64-bit server VM |
| Command | `python tools/formal/run_tlc.py --models-dir mqttiumd/models/m1` |
| Search | Breadth-first, one worker, no symmetry reduction, no state constraint |
| Machine | 4 cores, 3 GB heap (TLC default for the host) |
| Result | 37 of 37 configurations gave the outcome their sidecar declares; exit 0; 9 min 36 s in all, on a machine that was also running the extended run below |

States and depth are those TLC printed; for a violation they are the
states explored when the counterexample was found. Durations are
wall-clock, per configuration, measured by calling the runner's own
`run_check` in a separate pass; they vary with the load of the machine.
Every exploration of the campaign ended by itself: none was interrupted,
so none is inconclusive. TLC prints no depth for a temporal
counterexample.

### M1a

`Dests = {B, C}` and `NumOps = 2` in every configuration.

| Configuration | Specification | Pubs, crashes, losses | Mutation | Declared | Outcome | States generated | Distinct states | Depth | Time |
| - | - | - | - | - | - | - | - | - | - |
| `M1a-safety.cfg` | Spec | {a, b}, crash 1, loss 1 | none | pass | pass | 33429959 | 3292717 | 47 | 217.5s |
| `M1a-reach-commit.cfg` | Spec | {a, b}, crash 1, loss 1 | none | violation NoCommit | violation NoCommit | 47 | 36 | 6 | 1.0s |
| `M1a-reach-lost-result.cfg` | Spec | {a, b}, crash 1, loss 1 | none | violation NoLostResult | violation NoLostResult | 99 | 69 | 7 | 1.0s |
| `M1a-reach-unknown.cfg` | Spec | {a, b}, crash 1, loss 1 | none | violation NoUnknown | violation NoUnknown | 45 | 34 | 6 | 1.0s |
| `M1a-reach-rejection.cfg` | Spec | {a, b}, crash 1, loss 1 | none | violation NoRejection | violation NoRejection | 1361 | 590 | 10 | 1.2s |
| `M1a-reach-crash-after-commit.cfg` | Spec | {a, b}, crash 1, loss 1 | none | violation NoCrashAfterCommit | violation NoCrashAfterCommit | 100 | 70 | 7 | 0.9s |
| `M1a-reach-recovery.cfg` | Spec | {a, b}, crash 1, loss 1 | none | violation NoRecovery | violation NoRecovery | 11582 | 3689 | 13 | 1.3s |
| `M1a-reach-live-path.cfg` | Spec | {a, b}, crash 0, loss 1 | none | violation NoLivePath | violation NoLivePath | 31287 | 8546 | 15 | 1.6s |
| `M1a-reach-exhaustion.cfg` | Spec | {a, b}, crash 1, loss 1 | none | violation NoExhaustion | violation NoExhaustion | 22274 | 6603 | 14 | 1.7s |
| `M1a-mut-ack-before-commit.cfg` | Spec | {a, b}, crash 1, loss 1 | AckBeforeCommit | violation AnswerOnKnowledge | violation AnswerOnKnowledge | 44 | 34 | 6 | 0.9s |
| `M1a-mut-unknown-as-rejection.cfg` | Spec | {a, b}, crash 1, loss 1 | UnknownAsRejection | violation FenceForbidsExecution | violation FenceForbidsExecution | 86 | 64 | 7 | 1.1s |
| `M1a-mut-recipe-change.cfg` | Spec | {a, b}, crash 1, loss 1 | RecipeChange | violation OneRecipePerIdentity | violation OneRecipePerIdentity | 84 | 62 | 7 | 1.1s |
| `M1a-mut-second-admission.cfg` | Spec | {a, b}, crash 1, loss 1 | SecondAdmissionAtReplay | violation OneAdmissionPerExchange | violation OneAdmissionPerExchange | 22189 | 6554 | 14 | 1.8s |
| `M1a-mut-late-unknown.cfg` | Spec | {a, b}, crash 1, loss 1 | LateUnknownRegresses | violation TerminalKnowledgeStable | violation TerminalKnowledgeStable | 1272 | 567 | 10 | 1.2s |
| `M1a-live-path.cfg` | LiveSpec | {a}, crash 0, loss 2 | none | pass LivePathResolves | pass | 9810 | 1903 | 27 | 2.1s |
| `M1a-live-path-no-rearm.cfg` | LiveSpec | {a}, crash 0, loss 2 | NoRearm | liveness LivePathResolves | liveness | 6128 | 1383 | - | 1.8s |
| `M1a-recovery.cfg` | RecoverySpec | {a}, crash 1, loss 2 | none | pass RecoveryCompletes | pass | 80690 | 15436 | 38 | 6.0s |
| `M1a-recovery-silent-client.cfg` | LiveSpec | {a}, crash 1, loss 2 | none | liveness RecoveryCompletes | liveness | 52182 | 11333 | - | 4.0s |

### M1b

`Dests = {B, C}`, `NumOps = 2` and `Capacity = 1` unless stated; each
fault named is allowed once, unless a number follows.

| Configuration | Specification | Faults allowed | Mutation | Declared | Outcome | States generated | Distinct states | Depth | Time |
| - | - | - | - | - | - | - | - | - | - |
| `M1b-safety-epoch.cfg` | Spec | crash, connection reset, loss | none | pass | pass | 506720 | 100249 | 40 | 4.9s |
| `M1b-safety-source.cfg` | Spec | crash, source reset, loss | none | pass | pass | 3604848 | 473287 | 41 | 23.5s |
| `M1b-safety-connection.cfg` | Spec | connection reset, source reset, loss | none | pass | pass | 16325931 | 1979145 | 43 | 100.2s |
| `M1b-safety-destination.cfg` | Spec | source reset, destination reset, settle | none | pass | pass | 13420360 | 1754718 | 39 | 113.5s |
| `M1b-safety-capacity.cfg` | Spec | source reset, settle, loss | none | pass | pass | 4438834 | 522976 | 39 | 27.2s |
| `M1b-reach-replay-while-unknown.cfg` | Spec | connection reset | none | violation NoReplayWhileUnknown | violation NoReplayWhileUnknown | 3471 | 1233 | 15 | 1.2s |
| `M1b-reach-commit-before-fence.cfg` | Spec | loss | none | violation NoCommitBeforeFence | violation NoCommitBeforeFence | 699 | 269 | 13 | 1.0s |
| `M1b-reach-fence-before-command.cfg` | Spec | no fault | none | violation NoFenceBeforeCommand | violation NoFenceBeforeCommand | 388 | 169 | 13 | 1.0s |
| `M1b-reach-capacity-refusal.cfg` | Spec | source reset | none | violation NoCapacityRefusal | violation NoCapacityRefusal | 7258 | 2330 | 16 | 1.1s |
| `M1b-reach-stale-target-refusal.cfg` | Spec | destination reset | none | violation NoStaleTargetRefusal | violation NoStaleTargetRefusal | 567 | 287 | 11 | 1.3s |
| `M1b-reach-epoch-closed-refusal.cfg` | Spec | crash | none | violation NoEpochClosedRefusal | violation NoEpochClosedRefusal | 2822 | 1029 | 18 | 1.0s |
| `M1b-reach-second-exchange.cfg` | Spec | source reset; capacity 2 | none | violation NoSecondExchange | violation NoSecondExchange | 7270 | 2340 | 16 | 1.0s |
| `M1b-mut-fence-on-stale-read.cfg` | Spec | no fault | FenceOnStaleRead | violation DecisionExclusive | violation DecisionExclusive | 40 | 27 | 9 | 0.8s |
| `M1b-mut-old-epoch-executes.cfg` | Spec | crash | OldEpochExecutes | violation NoExecutionInClosedEpoch | violation NoExecutionInClosedEpoch | 52 | 37 | 8 | 0.9s |
| `M1b-mut-stale-target.cfg` | Spec | destination reset | StaleTargetAccepted | violation NoStaleTarget | violation NoStaleTarget | 36 | 27 | 7 | 0.9s |
| `M1b-mut-refusal-not-durable.cfg` | Spec | source reset, settle 2 | RefusalNotDurable | violation RefusalNeverExecutes | violation RefusalNeverExecutes | 41985 | 10783 | 18 | 1.5s |
| `M1b-mut-source-reset-drops.cfg` | Spec | source reset | SourceResetDropsObligations | violation ObligationsConserved | violation ObligationsConserved | 29 | 22 | 7 | 0.8s |
| `M1b-mut-reset-keeps-attachment.cfg` | Spec | source reset | ResetKeepsAttachment | violation AckedIsAdmitted | violation AckedIsAdmitted | 813 | 378 | 11 | 1.0s |
| `M1b-mut-replay-readmits.cfg` | Spec | connection reset | ReplayReadmits | violation RefusedIsNotAdmitted | violation RefusedIsNotAdmitted | 8236 | 2683 | 17 | 1.2s |

### Counterexamples

Each `violation` and `liveness` row is a counterexample that the sidecar
expects. The traces are not stored; they are reproduced by running the
configuration, for instance:

```bash
cd mqttiumd/models/m1
java -cp ~/.cache/mqttium/formal/tla2tools-1.7.4.jar tlc2.TLC -workers 1 \
    -config M1a-mut-second-admission.cfg M1a.tla
```

These traces were read, to check that they fail for the declared reason:

- `M1a-live-path-no-rearm.cfg`: publish, receive, prepare, submit; the
  deadline fires once and is not armed again; the resolution answers
  unknown and that answer is lost; the command then commits and its result
  is lost too. The behaviour stutters with the commit done and the outcome
  unknown for ever: no deadline is left to ask again.
- `M1a-recovery-silent-client.cfg`: the client publishes; the broker
  crashes before it received the frame, recovers, and the client
  reconnects. The behaviour then stutters because the client never sends
  its PUBLISH again: the broker has nothing to act on.
- `M1b-mut-refusal-not-durable.cfg`: the first exchange commits; the
  source is reset and one obligation of the first exchange is settled;
  the second exchange is refused for the capacity of the other
  destination, without a fence; that obligation is settled too, the same
  command is presented again and commits, and the core then receives the
  earlier refusal.
- `M1b-mut-reset-keeps-attachment.cfg`: the admission commits, the
  producer's session is reset, the commit result arrives and PUBREC is
  sent on the new connection, for an exchange that was never admitted.
- `M1b-mut-replay-readmits.cfg`: the first command commits; after a
  connection reset the replayed PUBLISH prepares a second command, which
  the store refuses; the refusal is sent for an admitted exchange.
- `M1b-reach-commit-before-fence.cfg`: timeout, absent read, the command
  commits, the fence step returns the commit, PUBREC is observed.
- `M1b-reach-epoch-closed-refusal.cfg`: submit, crash, recovery, replay
  and a second command in the new epoch; the old command is refused, the
  new one commits, PUBREC is observed.

### Extended runs, outside the campaign

Run by hand with the same jar and one worker, on the same `M1a.tla`
(SHA-256 `be34e011…`), because they exceed the runner's limit of 900 s per
configuration.

| Run | Domain | Outcome | States generated | Distinct states | Depth | Time |
| - | - | - | - | - | - | - |
| `LivePathResolves` under `LiveSpec` | `Pubs = {a, b}`, no crash, one loss | pass | 15537563 | 1575585 | 44 | 40 min 6 s |
| `RecoveryCompletes` under `RecoverySpec` | `Pubs = {a, b}`, one crash, one loss | Not finished when this report was written: inconclusive | — | — | — | — |
| M1b, every fault kind at once | One of each fault | Interrupted at 12.5 million distinct states: inconclusive | — | — | — | — |

### Not run

- No domain larger than those above, no symmetry, no simulation mode.
- No liveness property on M1b.
- M2 to M6 are other lots.

### Bounds and limits

See `README.md`, "Bounds and limits". In short: small finite domains; the
atomicity of the commit is assumed; M1a erases the internal transport at
a crash and M1b keeps the store side of it; M1b combines its fault kinds
in groups; PUBREL and PUBCOMP belong to M2; no test replays a
counterexample yet.
