# Campaign report — M1

One report per campaign. A result belongs to the exact content it was run
on; when a module or a configuration changes, the campaign is run again
and this file is rewritten.

## Campaign of 15 November 2026

| | |
| - | - |
| Models | `M1a.tla` (sidecar `M1a.json`, 19 configurations), `M1b.tla` (sidecar `M1b.json`, 19 configurations) |
| Base | Integration branch at `b4c80fb`, runner chain `8978453`, `785c5fc`, `f88da6e`, `f84db36` |
| Contract digest | `sha256:7a869371e38d0bd344e49bf8928905ef0d59cebe932f7df3ce0cb2530bfb47cb` |
| TLC | 2.19 of 08 August 2024 (rev 5a47802), `tla2tools.jar` 1.7.4, digest pinned by `tools/formal/run_tlc.py` |
| Java | OpenJDK 21.0.12, 64-bit server VM |
| Command | `python tools/formal/run_tlc.py --models-dir mqttiumd/models/m1` |
| Search | Breadth-first, one worker, no symmetry reduction, no state constraint |
| Machine | 4 cores, 3 GB heap (TLC default for the host) |
| Result | 38 of 38 configurations gave the outcome their sidecar declares; exit 0; 5 min 42 s in all |

The models follow the corrected reading of the fence (see `README.md`,
"Bounds and limits"): a resolution applied to an absent identity always
fences it; the outcome stays unknown only when the write of the fence is
refused, a fault counted by `MaxFenceFail`. Record 0001 says otherwise in
step 3 of P1-02b; the record that corrects it is proposed and has no
number yet.

States and depth are those TLC printed; for a violation they are the
states explored when the counterexample was found. Durations are
wall-clock, per configuration, measured by calling the runner's own
`run_check` in a separate pass; they vary with the load of the machine.
Every exploration of the campaign ended by itself: none was interrupted,
so none is inconclusive. TLC prints no depth for a temporal
counterexample.

### M1a

`Dests = {B, C}` and `NumOps = 2` in every configuration. "trace" means
that the verifier also records the events a witness names (`Trace =
TRUE`); it changes no behaviour.

| Configuration | Specification | Pubs and faults | Mutation | Declared | Outcome | States generated | Distinct states | Depth | Time |
| - | - | - | - | - | - | - | - | - | - |
| `M1a-safety.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | none | pass | pass | 30921975 | 3334251 | 45 | 200.2s |
| `M1a-reach-commit.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | none | violation NoCommit | violation NoCommit | 47 | 36 | 6 | 0.9s |
| `M1a-reach-lost-result.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | none | violation NoLostResult | violation NoLostResult | 99 | 69 | 7 | 0.9s |
| `M1a-reach-unknown.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | none | violation NoUnknown | violation NoUnknown | 45 | 34 | 6 | 1.0s |
| `M1a-reach-rejection.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | none | violation NoRejection | violation NoRejection | 1392 | 639 | 10 | 1.1s |
| `M1a-reach-crash-after-commit.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | none | violation NoCrashAfterCommit | violation NoCrashAfterCommit | 100 | 70 | 7 | 0.9s |
| `M1a-reach-recovery.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1, trace | none | violation NoRecovery | violation NoRecovery | 40468 | 13430 | 14 | 1.4s |
| `M1a-reach-live-path.cfg` | Spec | {a, b}, crash 0, loss 1, refused fence 1, trace | none | violation NoLivePath | violation NoLivePath | 58450 | 18039 | 15 | 1.6s |
| `M1a-reach-exhaustion.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | none | violation NoExhaustion | violation NoExhaustion | 25036 | 7905 | 14 | 1.3s |
| `M1a-reach-fence-while-held.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1, trace | none | violation NoFenceWhileHeld | violation NoFenceWhileHeld | 610 | 354 | 9 | 1.0s |
| `M1a-mut-ack-before-commit.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | AckBeforeCommit | violation AnswerOnKnowledge | violation AnswerOnKnowledge | 44 | 34 | 6 | 0.9s |
| `M1a-mut-unknown-as-rejection.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | UnknownAsRejection | violation FenceForbidsExecution | violation FenceForbidsExecution | 86 | 64 | 7 | 0.9s |
| `M1a-mut-recipe-change.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | RecipeChange | violation OneRecipePerIdentity | violation OneRecipePerIdentity | 84 | 62 | 7 | 0.9s |
| `M1a-mut-second-admission.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | SecondAdmissionAtReplay | violation OneAdmissionPerExchange | violation OneAdmissionPerExchange | 24951 | 7856 | 14 | 1.3s |
| `M1a-mut-late-unknown.cfg` | Spec | {a, b}, crash 1, loss 1, refused fence 1 | LateUnknownRegresses | violation TerminalKnowledgeStable | violation TerminalKnowledgeStable | 1278 | 601 | 10 | 1.0s |
| `M1a-live-path.cfg` | LiveSpec | {a}, crash 0, loss 2, refused fence 1 | none | pass LivePathResolves | pass | 13373 | 2772 | 27 | 1.7s |
| `M1a-live-path-no-rearm.cfg` | LiveSpec | {a}, crash 0, loss 2, refused fence 1 | NoRearm | liveness LivePathResolves | liveness | 7676 | 1823 | - | 1.4s |
| `M1a-recovery.cfg` | RecoverySpec | {a}, crash 1, loss 2, refused fence 1 | none | pass RecoveryCompletes | pass | 111994 | 22837 | 38 | 6.4s |
| `M1a-recovery-silent-client.cfg` | LiveSpec | {a}, crash 1, loss 2, refused fence 1 | none | liveness RecoveryCompletes | liveness | 67639 | 15929 | - | 4.0s |

### M1b

`Dests = {B, C}`, `NumOps = 2` and `Capacity = 1` unless stated; each
fault named is allowed once, unless a number follows.

| Configuration | Specification | Faults allowed | Mutation | Declared | Outcome | States generated | Distinct states | Depth | Time |
| - | - | - | - | - | - | - | - | - | - |
| `M1b-safety-epoch.cfg` | Spec | crash, connection reset, loss, refused fence | none | pass | pass | 716141 | 154898 | 40 | 6.3s |
| `M1b-safety-source.cfg` | Spec | crash, source reset, loss | none | pass | pass | 1008177 | 191434 | 38 | 8.3s |
| `M1b-safety-connection.cfg` | Spec | connection reset, source reset, loss | none | pass | pass | 3919894 | 648015 | 37 | 26.9s |
| `M1b-safety-destination.cfg` | Spec | source reset, destination reset, settle | none | pass | pass | 3915381 | 675186 | 33 | 26.4s |
| `M1b-safety-capacity.cfg` | Spec | source reset, settle, loss, refused fence | none | pass | pass | 4883284 | 683377 | 36 | 30.8s |
| `M1b-reach-replay-while-unknown.cfg` | Spec | connection reset, refused fence | none | violation NoReplayWhileUnknown | violation NoReplayWhileUnknown | 3846 | 1430 | 15 | 1.0s |
| `M1b-reach-commit-before-fence.cfg` | Spec | loss | none | violation NoCommitBeforeFence | violation NoCommitBeforeFence | 465 | 198 | 13 | 0.9s |
| `M1b-reach-fence-before-command.cfg` | Spec | no fault | none | violation NoFenceBeforeCommand | violation NoFenceBeforeCommand | 257 | 123 | 13 | 0.9s |
| `M1b-reach-capacity-refusal.cfg` | Spec | source reset | none | violation NoCapacityRefusal | violation NoCapacityRefusal | 4997 | 1743 | 16 | 1.0s |
| `M1b-reach-stale-target-refusal.cfg` | Spec | destination reset | none | violation NoStaleTargetRefusal | violation NoStaleTargetRefusal | 471 | 257 | 11 | 0.8s |
| `M1b-reach-epoch-closed-refusal.cfg` | Spec | crash | none | violation NoEpochClosedRefusal | violation NoEpochClosedRefusal | 1736 | 761 | 18 | 0.9s |
| `M1b-reach-second-exchange.cfg` | Spec | source reset; capacity 2 | none | violation NoSecondExchange | violation NoSecondExchange | 4996 | 1743 | 16 | 0.9s |
| `M1b-mut-fence-on-stale-read.cfg` | Spec | no fault | FenceOnStaleRead | violation DecisionExclusive | violation DecisionExclusive | 34 | 23 | 9 | 0.8s |
| `M1b-mut-old-epoch-executes.cfg` | Spec | crash | OldEpochExecutes | violation NoExecutionInClosedEpoch | violation NoExecutionInClosedEpoch | 51 | 36 | 8 | 0.8s |
| `M1b-mut-stale-target.cfg` | Spec | destination reset | StaleTargetAccepted | violation NoStaleTarget | violation NoStaleTarget | 36 | 27 | 7 | 0.8s |
| `M1b-mut-refusal-not-durable.cfg` | Spec | source reset, settle 2 | RefusalNotDurable | violation RefusalNeverExecutes | violation RefusalNeverExecutes | 27668 | 7578 | 18 | 1.2s |
| `M1b-mut-source-reset-drops.cfg` | Spec | source reset | SourceResetDropsObligations | violation ObligationsConserved | violation ObligationsConserved | 29 | 22 | 7 | 0.8s |
| `M1b-mut-reset-keeps-attachment.cfg` | Spec | source reset | ResetKeepsAttachment | violation AckedIsAdmitted | violation AckedIsAdmitted | 686 | 337 | 11 | 0.8s |
| `M1b-mut-replay-readmits.cfg` | Spec | connection reset | ReplayReadmits | violation RefusedIsNotAdmitted | violation RefusedIsNotAdmitted | 5198 | 1846 | 17 | 1.0s |

### Counterexamples

Each `violation` and `liveness` row is a counterexample that the sidecar
expects. The traces are not stored; they are reproduced by running the
configuration, for instance:

```bash
cd mqttiumd/models/m1
java -cp ~/.cache/mqttium/formal/tla2tools-1.7.4.jar tlc2.TLC -workers 1 \
    -config M1a-mut-second-admission.cfg M1a.tla
```

These traces were read, to check that each reaches its target by the path
its name claims:

- `M1a-reach-live-path.cfg` (P1-02b): publish, receive, prepare, submit;
  the deadline fires and the write of the fence is refused, so the
  resolution answers unknown; the deadline fires again; the command
  commits and its result is lost; the second resolution returns the
  commit, the core authorises PUBREC and the client observes it on the
  first connection, with the first unknown answer still undelivered. No
  crash, no second presentation.
- `M1a-reach-recovery.cfg` (P1-02): the command commits and its result is
  lost; crash, recovery, reconnect, the client sends its PUBLISH again and
  PUBREC is observed on the second connection. Nothing was authorised on
  the first, and one identity was used.
- `M1a-reach-fence-while-held.cfg` (QABS-B): the command is held; the
  deadline fires; the resolution fences the absent identity; the core
  receives the certain rejection while the command is still held.
- `M1a-live-path-no-rearm.cfg`: the deadline fires once and is not armed
  again; the command commits and its result is lost; the one resolution
  returns the commit and that result is lost too. The behaviour stutters
  with the commit done and the outcome unknown for ever: no deadline is
  left to ask again.
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

| Run | Domain | Outcome |
| - | - | - |
| `LivePathResolves`, two publications, no crash, one loss | Run on the module before the corrected fence rule: pass, 1,575,585 distinct states, 40 min 6 s | Superseded: it says nothing of the present module |
| `RecoveryCompletes`, two publications, one crash, one loss | Interrupted | Inconclusive |
| M1b, every fault kind at once | Interrupted at 14.1 million distinct states after 850 s | Inconclusive |

The bound claimed for the two liveness properties is the one of the
campaign: one publication.

### Not run

- No liveness on two publications on the present module; no domain larger
  than those above; no symmetry; no simulation mode.
- No liveness property on M1b.
- M2 to M6 are other lots.

### Bounds and limits

See `README.md`, "Bounds and limits". In short: small finite domains; the
atomicity of the commit is assumed; M1a erases the internal transport at
a crash and M1b keeps the store side of it; M1b combines its fault kinds
in groups; PUBREL and PUBCOMP belong to M2; no test replays a
counterexample yet.
