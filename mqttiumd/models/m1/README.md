# M1 — admission and authorisation of PUBREC

TLA+ models of lot L5 (DEM-020, V3-018, ARC-027; decision records 0001 and
0005). `M1a` is here. `M1b` (resets, session generations, capacity
refusals, fences and the change of epoch) is not written yet.

A model is evidence for a design. It is not a proof of the Rust code, of
SQLite or of the hardware.

## Running

From the repository root, with Java 11 or newer. The runner pins and
hash-checks the TLC jar; `mqttiumd/tools/install-tools.sh tlc` fetches it
through the same code.

```bash
python tools/formal/run_tlc.py --models-dir mqttiumd/models/m1 --list
python tools/formal/run_tlc.py --models-dir mqttiumd/models/m1
```

`M1a.json` is the sidecar: it declares the outcome TLC must give for every
configuration. A configuration that fails for another reason than the one
declared is a failure of the campaign, not a detection. Its status is
`specification`: the actions encode the predicates of the specification
and of the decision records, and no implementation exists yet to refine.

`python models/check_models.py`, from `mqttiumd/`, checks without Java
that the files, the sidecar and the correspondence table below agree.

## M1a

One producer, two destinations, two publications, two admission
identities, at most one crash. No collection, expiry, Will, PUBREL or
PUBCOMP.

### Domains

| Domain | Variables | Crash |
| - | - | - |
| Durable | `dDecision`, `dPub`, `dSlot`, `dObl`, `dAcct`, `dEpoch`, `dNextOp` | Kept; the only input of `Recover` |
| Volatile | `vSlot`, `vOp`, `vKnow`, `vPrep`, `vDeadline`, `vFault` | Erased |
| Internal transport | `tCmd`, `tResolve`, `tResult` | Erased |
| Connections | `cGen`, `cOpen` | Closed; the generation is never reused |
| External transport | `xIn`, `xAuth`, `xWire` | `xIn` and `xAuth` erased; `xWire` kept |
| Client | `kPhase`, `kSentGen`, `kGen` | Kept: another failure domain |
| Fault environment | `proc`, `eCrash`, `eLoss` | Bounds of the bench |
| History of the verifier | `hAdmit`, `hPresented`, `hKnown`, `hRefused`, `hFenced`, `hAuth` | Kept |

The history is written by actions and read by invariants and properties
only. No guard reads it, and `Recover` computes the volatile domain from
durable variables alone.

### Actions

| Group | Actions |
| - | - |
| Client | `ClientPublish`, `ClientReconnect`, `ClientResend`, `ClientObserve` |
| Reception and preparation | `Receive`, `Prepare` |
| Submission | `Submit`, `Represent` |
| Delayed command | `HoldCommand`, `ReleaseCommand` |
| Commit and rejection | `ApplyCommand`, `RejectCommand` |
| Result | `DeliverResult`, `LoseResult` |
| Timeout and resolution | `Timeout`, `Resolve` |
| Authorisation and observation | `Authorize`, `Refuse`, `SubmitFrame`, `ClientObserve` |
| Crash and recovery | `Crash`, `Recover` |

### Invariants

Checked by `M1a-safety.cfg` on the full domain, with no fairness.

| Invariant | Statement |
| - | - |
| `OneAdmissionPerExchange` | At most one committed admission per exchange as the client and the verifier identify it |
| `AtomicObligations` | A committed identity has its publication, its slot and both obligations; any other has none |
| `AnswerOnKnowledge` | PUBREC was authorised only after the core learnt the commit, by a result or by recovery |
| `OneRecipePerIdentity` | An identity was presented with one recipe only |
| `FenceForbidsExecution` | A fenced identity, and one the core treated as rejected, never executes |
| `AccountsMatch` | The account of a destination equals its obligations |
| `KnowledgeSound` | Committed knowledge agrees with the store; no contradiction was reported |
| `TerminalKnowledgeStable` | Terminal knowledge never regresses; recovery finds it again |
| `DelayedCommandKept` | An undecided identity the core waits for keeps an executable command |

### Liveness

Two properties, each with its own assumptions. Weak fairness only.

| Property | Configuration | Assumptions |
| - | - | - |
| `LivePathResolves` | `M1a-live-path.cfg`, `LiveSpec` | No crash. `FairBroker`: the store applies commands and resolutions, results are delivered, the deadline fires and stays armed, the core authorises, frames are submitted, the peer reads its socket, the bench releases a held command. At most `MaxLoss` results are lost. **No fairness on any action by which the client sends a packet.** |
| `RecoveryCompletes` | `M1a-recovery.cfg`, `RecoverySpec` | The above, no crash after the last one, and `FairClientResume`: the client reconnects and sends again the PUBLISH it has no answer for |

`LivePathResolves` goes from the unknown outcome to an observed answer:
PUBREC, or the refusal when the resolution established a fence.
`RecoveryCompletes` allows a creation suspended by the exhaustion of the
identities; with one publication `NoSuspension` shows that this escape is
not reachable in the checked configuration.

Each property has a configuration that must violate it:

- `M1a-live-path-no-rearm.cfg`: the deadline is not armed again after the
  first resolution. The commit result is lost after an unknown answer and
  nothing resolves the outcome.
- `M1a-recovery-silent-client.cfg`: the same broker fairness, without
  `FairClientResume`. The client reconnects and never sends its PUBLISH
  again. This shows the assumption is used, and that the live path does not
  rest on it.

### Non-vacuity

Each target is the negation of an invariant that TLC must report as
violated; the counterexample is the witness.

| Configuration | Witness |
| - | - |
| `M1a-reach-commit.cfg` | A commit |
| `M1a-reach-lost-result.cfg` | The result of a commit lost, the core not knowing |
| `M1a-reach-unknown.cfg` | An unknown outcome |
| `M1a-reach-rejection.cfg` | A certain rejection observed by the peer |
| `M1a-reach-crash-after-commit.cfg` | A crash after a commit the core never knew nor answered |
| `M1a-reach-recovery.cfg` | The shape of P1-02: recovered admission, PUBREC on the new connection, no second identity |
| `M1a-reach-live-path.cfg` | The shape of P1-02b: no crash, result lost, PUBREC observed while an old unknown answer is still held |
| `M1a-reach-exhaustion.cfg` | Both identities used, a creation suspended, a delayed command still held |

### Mutations

`Mutation` replaces one predicate. Each configuration checks `TypeOK` and
the property the mutation must break.

| Configuration | Mutation | Counterexample for |
| - | - | - |
| `M1a-mut-ack-before-commit.cfg` | PUBREC authorised while the admission is submitted or unknown | `AnswerOnKnowledge` |
| `M1a-mut-unknown-as-rejection.cfg` | A deadline is taken as a certain rejection | `FenceForbidsExecution` |
| `M1a-mut-recipe-change.cfg` | The targets change under the same identity | `OneRecipePerIdentity` |
| `M1a-mut-second-admission.cfg` | A replay is admitted again behind a new exchange generation (DEM-MU-04) | `OneAdmissionPerExchange` |
| `M1a-mut-late-unknown.cfg` | A late unknown replaces a commit (DEM-MU-05) | `TerminalKnowledgeStable` |
| `M1a-live-path-no-rearm.cfg` | The deadline is not armed again | `LivePathResolves` |

## Bounds and limits

- Domain of the safety campaign: `Pubs = {a, b}`, `Dests = {B, C}`,
  `NumOps = 2`, `MaxCrash = 1`, `MaxLoss = 1`. No symmetry reduction.
- Domain of the liveness campaign: one publication, `MaxLoss = 2`;
  `MaxCrash = 0` for the live path and `1` for recovery. The two-publication
  liveness runs do not fit the runner's time limit; see the report.
- Identities are consumed in order and never reused; `dNextOp` stands for
  an identity made of the run epoch and a serial. Using up both suspends
  `Prepare` and removes nothing.
- A crash erases the internal transport: no command of an old epoch
  survives. The race between a fence, an application and the change of
  epoch belongs to M1b.
- A reconnect happens only after a crash. A replayed PUBLISH while the
  outcome is unknown (record 0001) needs a reset without a crash: the
  branch is written in `Receive` and is not reachable before M1b.
- The store may fence an absent identity or fail to; the model does not
  say why a fence cannot be established.
- `RejectCommand` stands for every refusal of the store; capacities are
  detailed by M1b.
- The atomicity of `ApplyCommand` is an assumption. The SQLite tests must
  discharge it.
- PUBREL and PUBCOMP belong to M2. M1a does not cover the whole S0 path.
- The refusal sent to the peer (`NACK`) is abstract: the model does not
  choose its reason code.
- Small domains are not a proof for every number of clients.

## Correspondence (draft, completed with L4)

Model actions against the rules of `contracts/v1/p1.transitions.json` and
the steps of the scenarios. A dash means that nothing exists yet on that
side. The last column names the deterministic test that replays the action
on the real core, or the lot that owes it: no replay exists yet, and the
sidecar lists no test until one does.

| Model action | Registry rule | Scenario step | Replay |
| - | - | - | - |
| `ClientPublish` | — | `ClientPublish` (P1-02; P1-02b step 1) | Not replayed yet; L4 |
| `Receive`, slot `Absent` | `inbound_qos2.absent.publish.admit` (first half) | Driven by `DriveUntil` | Not replayed yet; L4 |
| `Prepare` | `inbound_qos2.absent.publish.admit` (second half) | Driven | Not replayed yet; L4 |
| `Prepare` disabled, no identity left | `inbound_qos2.absent.publish.defer` speaks of effect capacity; the exhaustion of identities has no rule yet | — | Not replayed yet; L4, after a rule by L3 |
| `Submit` | `command.prepared.submit`; `ArmDeadline` after `Persist` (record 0001) | Driven; P1-02b step 1 | Not replayed yet; L4 |
| `Represent` | — (the `Replay` cells of the `command` machine are not covered) | — | Not replayed yet; L4, after a rule by L3 |
| `HoldCommand` | — | P1-02b step 2; QABS-A, B, C (vocabulary to come, L3) | Not replayed yet; L4 |
| `ReleaseCommand` | — | P1-02b step 4 (vocabulary to come, L3) | Not replayed yet; L4 |
| `ApplyCommand`, commit | Store side; not a registry rule | Checkpoint `after_commit_before_crash` | Not replayed yet; L4 (L7 on SQLite) |
| `ApplyCommand` on a fenced identity; `RejectCommand` | Store side | QABS-B | Not replayed yet; L4 |
| `LoseResult` | — | Fault `lost_admission_result`: `drop_result` at `store.result.ready` | Not replayed yet; L4 |
| `DeliverResult`, committed | `command.submitted.result.committed`, `command.indeterminate.result.committed` | Driven | Not replayed yet; L4 |
| `DeliverResult`, rejected | `command.submitted.result.rejected`, `command.indeterminate.result.rejected` | QABS-B | Not replayed yet; L4 |
| `DeliverResult`, unknown | `command.submitted.result.unknown`, `command.indeterminate.result.unknown` | P1-02b step 3 | Not replayed yet; L4 |
| `DeliverResult`, late unknown | `command.committed.result.unknown`, `command.rejected.result.unknown`, `inbound_qos2.await_pubrel.late_unknown.ignore` | P1-02b step 6 | Not replayed yet; L4 |
| `DeliverResult`, repeated commit | `command.committed.result.duplicate` | — | Not replayed yet; L4 |
| `DeliverResult`, contradiction | `command.committed.result.contradiction`, `command.rejected.result.contradiction` | — | Not replayed yet; L4 |
| `Timeout` | `command.submitted.timeout`; a second deadline has no rule yet | P1-02b steps 3 and 5 (vocabulary to come, L3) | Not replayed yet; L4 |
| `Resolve` | Store side: `ResolveAndFence` | P1-02b steps 3 and 5; QABS-A, B, C | Not replayed yet; L4 |
| `Authorize` | `inbound_qos2.admission_pending.committed.authorize` | `DriveUntil PeerPacketObserved PUBREC` | Not replayed yet; L4 |
| `Refuse` | — (ARC-011, certain rejection; no rule yet) | — | Not replayed yet; L4, after a rule by L3 |
| `Receive`, slot `AwaitPubrel` | `inbound_qos2.await_pubrel.publish.replay` | `ClientResumeExchange` (P1-02) | Not replayed yet; L4 |
| `Receive`, slot `Received` or `AdmissionPending` | — (rows of record 0001, to be written by L3) | — | Not replayed yet; L4, after M1b reaches it |
| `SubmitFrame` | — | `DriveUntil PeerPacketObserved` | Not replayed yet; L4 |
| `ClientObserve` | — | `DriveUntil PeerPacketObserved` | Not replayed yet; L4 |
| `Crash` | — | `CrashBroker` | Not replayed yet; L4 |
| `Recover` | — | `RestartBroker` | Not replayed yet; L4 |
| `ClientReconnect` | — | `ClientResumeSession` | Not replayed yet; L4 |
| `ClientResend` | — | `ClientResumeExchange` | Not replayed yet; L4 |
| `Quiescent` | — | End of the run | Not an action of the product |

Not in the model: the `defer` rules on effect capacity, PUBREL and the
`release` rules.

## Witnesses and scenarios

The scenarios expected to replay each witness, once L4 runs them:

| Configuration | Scenario |
| - | - |
| `M1a-reach-recovery.cfg`, `M1a-reach-crash-after-commit.cfg`, `M1a-reach-lost-result.cfg` | P1-02 |
| `M1a-reach-live-path.cfg`, `M1a-reach-unknown.cfg`, `M1a-live-path.cfg` | P1-02b |
| `M1a-reach-rejection.cfg`, `M1a-mut-unknown-as-rejection.cfg` | QABS-B, QABS-C, DEM-MU-01 |
| `M1a-mut-second-admission.cfg` | DEM-MU-04 |
| `M1a-mut-late-unknown.cfg` | P1-02b step 6, DEM-MU-05 |
| `M1a-mut-ack-before-commit.cfg` | Assertion `no_positive_ack_before_confirmed_commit` |
| `M1a-mut-recipe-change.cfg` | Assertion `same_recipe_for_op_A` |
