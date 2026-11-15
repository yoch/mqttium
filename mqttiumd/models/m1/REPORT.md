# Campaign report — M1

One report per campaign. A result belongs to the exact content it was run
on; when the module or a configuration changes, the campaign is run again
and this file is rewritten.

## Campaign of 15 November 2026

| | |
| - | - |
| Model | `M1a.tla`, sidecar `M1a.json`, 18 configurations |
| Base | Integration branch at `b4c80fb`, runner with `--models-dir` at `785c5fc` |
| Contract digest | `sha256:7a869371e38d0bd344e49bf8928905ef0d59cebe932f7df3ce0cb2530bfb47cb` |
| TLC | 2.19 of 08 August 2024 (rev 5a47802), `tla2tools.jar` 1.7.4, digest pinned by `tools/formal/run_tlc.py` |
| Java | OpenJDK 21.0.12, 64-bit server VM |
| Command | `python tools/formal/run_tlc.py --models-dir mqttiumd/models/m1` |
| Search | Breadth-first, one worker, no symmetry reduction, no state constraint |
| Machine | 4 cores, 3 GB heap (TLC default for the host) |
| Result | 18 of 18 configurations gave the outcome their sidecar declares; exit 0; 4 min 4 s in all |

`Dests = {B, C}` and `NumOps = 2` in every configuration. States and
depth are those TLC printed; for a violation they are the states explored
when the counterexample was found. Durations are wall-clock, per
configuration, measured by calling the runner's own `run_check`.

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

Every exploration ended by itself: none was interrupted, so none is
inconclusive. TLC prints no depth for a temporal counterexample.

### Counterexamples

Each `violation` and `liveness` row is a counterexample that the sidecar
expects. The traces are not stored; they are reproduced by running the
configuration, for instance:

```bash
cd mqttiumd/models/m1
java -cp ~/.cache/mqttium/formal/tla2tools-1.7.4.jar tlc2.TLC -workers 1 \
    -config M1a-mut-second-admission.cfg M1a.tla
```

The two temporal counterexamples were read, to check that they fail for
the declared reason:

- `M1a-live-path-no-rearm.cfg`: publish, receive, prepare, submit; the
  deadline fires once and is not armed again; the resolution answers
  unknown and that answer is lost; the command then commits and its result
  is lost too. The behaviour stutters with the commit done and the outcome
  unknown for ever: no deadline is left to ask again.
- `M1a-recovery-silent-client.cfg`: the client publishes; the broker
  crashes before it received the frame, recovers, and the client
  reconnects. The behaviour then stutters because the client never sends
  its PUBLISH again: the broker has nothing to act on.

### Not run in this campaign

- The two liveness properties on the domain of the safety configuration
  (two publications). They do not fit the runner's limit of 900 s per
  configuration: about 60 000 distinct states a minute with the fairness
  conditions, for 3.3 million states with one crash. They are run by hand
  outside the campaign; until one ends it is inconclusive and nothing is
  claimed from it.
- No domain larger than the one above, no symmetry, no simulation mode.
- M1b does not exist yet.

### Bounds and limits

See `README.md`, "Bounds and limits". In short: small finite domains; the
atomicity of the commit is assumed; a crash erases the internal transport;
PUBREL and PUBCOMP belong to M2; no test replays a counterexample yet.
