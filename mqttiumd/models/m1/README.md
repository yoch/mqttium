# M1 — admission and authorisation of PUBREC

TLA+ models of lot L5 (DEM-020, V3-018, ARC-027; decision records 0001 and
0005). `M1a` is the admission and the authorisation of PUBREC through a
lost result, an unknown outcome and a crash. `M1b` adds resets, session
generations, capacity refusals, the race between a fence and an
application, and the change of epoch.

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

`M1a.json` and `M1b.json` are the sidecars: each declares the outcome TLC
must give for every configuration of its module. A configuration that fails for another reason than the one
declared is a failure of the campaign, not a detection. Their status is
`specification`: the actions encode the predicates of the specification
and of the decision records, and no implementation exists yet to refine.

`python models/check_models.py`, from `mqttiumd/`, checks without Java
that the files, the sidecars and the correspondence table below agree. A
passing entry of a sidecar lists the `invariants` its configuration
checks, and the script compares the two; the runner ignores that member.
`python -m unittest discover -s models -p "test_*.py"` runs the tests of
the script.

A witness whose predicate reads recorded events reads those of the
identity it is about, so that another publication cannot supply them.

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
| `LivePathResolves` | `M1a-live-path.cfg`, `LiveSpec` | No crash. `FairBroker`, action by action: the store applies a command that is not held (`ApplyCommand`) and a resolution (`Resolve`); the deadline fires and stays armed (`Timeout`); results are delivered to the core (`DeliverResult`); the core receives a frame that arrived, prepares, submits, authorises and refuses (`Receive`, `Prepare`, `Submit`, `Authorize`, `Refuse`); the transport adapter accepts an authorised frame (`SubmitFrame`); `Recover`. Faults are bounded: `MaxLoss` lost results, `MaxFenceFail` refused fences. **No fairness on any action the client, the peer or the bench owns**: `ClientPublish`, `ClientResend`, `ClientReconnect`, `ClientObserve`, `HoldCommand`, `ReleaseCommand`, `RejectCommand`, `LoseResult`, `Crash` |
| `RecoveryCompletes` | `M1a-recovery.cfg`, `RecoverySpec` | The above, no crash after the last one, and `FairClientResume`: the client reconnects (`ClientReconnect`) and sends again the PUBLISH it has no answer for (`ClientResend`) |

The goal of both is `Answered`: the answer is on the wire of the
connection the client reads, or the client has it. Whether the peer
reads its socket is not assumed.

`LivePathResolves` goes from the unknown outcome to that answer: PUBREC,
or the refusal when the resolution established a fence. It does not need
a held command to be released: the resolution fences an absent identity
whether its command is held or not. `RecoveryCompletes` allows a creation
suspended by the exhaustion of the identities; with one publication
`NoSuspension` shows that this escape is not reachable in the checked
configuration.

Each property has a configuration that must violate it:

- `M1a-live-path-no-rearm.cfg`: the deadline is not armed again after the
  first resolution. The commit result and the answer of that one
  resolution are both lost, and nothing asks again.
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
| `M1a-reach-recovery.cfg` | The shape of P1-02: result lost, nothing authorised before the crash, recovered admission, PUBREC on the new connection, no second identity |
| `M1a-reach-live-path.cfg` | The shape of P1-02b: no crash, a first resolution whose fence is refused, the commit result lost, PUBREC observed on the first connection while the old unknown answer is still held |
| `M1a-reach-fence-while-held.cfg` | QABS-B: the fence established while the command is held; the core knows the rejection |
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

## M1b

One producer with one inbound slot, two destinations, two admission
identities. The producer's session and each destination session have
generations; an exchange is identified by the generation of the
producer's session that opened it. M1b leaves the explicit hold of a
command and the liveness properties to M1a.

### What it adds

| Addition | Actions | Predicates of the store or the core |
| - | - | - |
| Connection reset without a crash; replay of a PUBLISH while the admission is unresolved | `ConnectionReset`, `ClientReconnect`, `ClientResend`, `Receive` | A replay creates no command and no acknowledgement; PUBREC goes to the connection that is current when it is authorised |
| Reset of the producer's session | `SourceSessionReset` | The inbound slot is removed, committed obligations stay, the old identity is detached from the slot and still reconciled; a command of the old exchange is refused |
| Reset of a destination session; settlement of an obligation | `DestSessionReset`, `Settle` | A command whose target generation is no longer current is refused |
| Durable capacity per destination | `ApplyCommand` | A full capacity is a certain rejection, durable: it stays after the capacity is free again |
| Commands that outlive a crash of the core; the change of epoch | `Crash`, `Recover`, `ApplyCommand` | The store keeps the commands it accepted and may apply one while the core is down; after recovery a command of a closed epoch is refused. Closing an epoch teaches the core nothing about an identity that left no durable fact |
| `ResolveAndFence` in two steps | `ResolveRead`, `FenceEstablish`, `FenceAbandon` | The fence is established only if the identity is still absent when the writer orders it: a command applied in between wins (QABS-A), otherwise the fence wins (QABS-B). Only a refused write of the fence, a counted fault, leaves the outcome unknown (QABS-C) |

The store-side transport `tCmd` is not erased by a crash, unlike in M1a:
this is what makes the change of epoch observable.

### Invariants

| Invariant | Statement |
| - | - |
| `OneAdmissionPerExchange` | At most one committed admission per exchange |
| `AtomicObligations` | A committed identity has its publication and an obligation for every target that still exists; any other has none |
| `DecisionExclusive` | An identity is never both committed and fenced |
| `RefusalNeverExecutes` | An identity the core treated as rejected never executes, also after capacity is free again |
| `AckedIsAdmitted` | The client holds an acknowledgement only for an admitted exchange |
| `RefusedIsNotAdmitted` | The client holds a refusal only for an exchange that is not admitted |
| `NoExecutionInClosedEpoch` | A command commits only in the epoch it was prepared in |
| `NoStaleTarget` | No obligation exists for a destination session that is gone |
| `ObligationsConserved` | An obligation for a session that still exists is removed only by its settlement |
| `AccountsMatch`, `WithinCapacity` | The account of a destination equals its obligations and stays within its capacity |
| `KnowledgeSound` | Committed knowledge agrees with the store; no contradiction was reported |

### Configurations

The fault kinds are checked in five groups, not all at once: the domain
with every fault kind together was interrupted at 14.1 million distinct
states and is inconclusive. `Dests = {B, C}`, `NumOps = 2` and
`Capacity = 1` unless stated.

| Configuration | Faults allowed (at most one of each) |
| - | - |
| `M1b-safety-epoch.cfg` | Crash, connection reset, lost result, refused fence |
| `M1b-safety-source.cfg` | Crash, reset of the producer's session, lost result |
| `M1b-safety-connection.cfg` | Connection reset, reset of the producer's session, lost result |
| `M1b-safety-destination.cfg` | Reset of a destination session, settlement, reset of the producer's session |
| `M1b-safety-capacity.cfg` | Settlement, reset of the producer's session, lost result, refused fence |

| Configuration | Witness |
| - | - |
| `M1b-reach-replay-while-unknown.cfg` | A PUBLISH replayed after a connection reset while its outcome is unknown, acknowledged on the new connection by the one admission |
| `M1b-reach-commit-before-fence.cfg` | QABS-A, then PUBREC observed |
| `M1b-reach-fence-before-command.cfg` | QABS-B, then the refusal observed |
| `M1b-reach-capacity-refusal.cfg` | A second exchange refused for capacity and for no other reason, observed by the peer |
| `M1b-reach-stale-target-refusal.cfg` | A command refused because a target session was reset, and for no other reason |
| `M1b-reach-epoch-closed-refusal.cfg` | A command of the first epoch refused in the second, for that reason only, while a new admission of the same exchange commits |
| `M1b-reach-second-exchange.cfg` | After a reset of the producer's session, a new exchange admitted on the same slot, the first obligations kept (`Capacity = 2`) |

| Configuration | Mutation | Counterexample for |
| - | - | - |
| `M1b-mut-fence-on-stale-read.cfg` | The fence is established on the absence read earlier | `DecisionExclusive` |
| `M1b-mut-old-epoch-executes.cfg` | A command of a closed epoch is applied | `NoExecutionInClosedEpoch` |
| `M1b-mut-stale-target.cfg` | Target generations are not checked at the commit | `NoStaleTarget` |
| `M1b-mut-refusal-not-durable.cfg` | A refusal leaves no fence; the same command commits once capacity is free | `RefusalNeverExecutes` |
| `M1b-mut-source-reset-drops.cfg` | The reset of the source removes its committed obligations | `ObligationsConserved` |
| `M1b-mut-reset-keeps-attachment.cfg` | The old identity stays attached to the slot after the reset of the session | `AckedIsAdmitted` |
| `M1b-mut-replay-readmits.cfg` | A replay while the admission is unresolved prepares a second command | `RefusedIsNotAdmitted` |

## Bounds and limits

- Domain of the safety campaign of M1a: `Pubs = {a, b}`, `Dests = {B, C}`,
  `NumOps = 2`, `MaxCrash = 1`, `MaxLoss = 1`, `MaxFenceFail = 1`. No
  symmetry reduction.
- Domain of the liveness campaign: one publication, `MaxLoss = 2`,
  `MaxFenceFail = 1`; `MaxCrash = 0` for the live path and `1` for
  recovery. This is the bound claimed for liveness. Runs on two
  publications do not fit the runner's time limit; see the report.
- Identities are consumed in order and never reused; `dNextOp` stands for
  an identity made of the run epoch and a serial. Using up both suspends
  `Prepare` and removes nothing.
- In M1a a crash erases the internal transport: no command of an old
  epoch survives, and a reconnect happens only after a crash. The replay
  of a PUBLISH while the outcome is unknown (record 0001) is written in
  `Receive` of M1a and reached in M1b only.
- M1b: one slot, so one exchange at a time; its fault kinds are combined
  in groups; no liveness property; the delivery and acknowledgement of an
  obligation are reduced to `Settle`; a reset of a session is one atomic
  durable step.
- The fence follows record 0015 (proposed), which corrects step 3 of
  P1-02b in record 0001 and agrees with DEM-018 and the store contract: a
  resolution applied to an absent identity always fences it, and the
  outcome stays unknown only when the write of the fence is refused, a
  counted fault (`MaxFenceFail`), which record 0015 injects as
  `refuse_fence` at `store.fence.transit`. A held command plays no part.
  Record 0015 is proposed, not accepted: if its decision changes, the
  fence of M1a and M1b changes with it.
- In M1a `RejectCommand` stands for every refusal of the store; M1b
  details the refusals for capacity, stale source, stale target and
  closed epoch.
- The atomicity of `ApplyCommand` is an assumption. The SQLite tests must
  discharge it.
- PUBREL and PUBCOMP belong to M2. M1a does not cover the whole S0 path.
- The refusal sent to the peer (`NACK`) is abstract: the model does not
  choose its reason code.
- Small domains are not a proof for every number of clients.

## Correspondence (draft, completed with L4)

Model actions against the rules of `contracts/v1/p1.transitions.json`
(status `complete`, 92 rules, at the integration head `88c92a0`) and the
steps of the scenarios. A dash means that nothing exists yet on that
side. The last column names the deterministic test that replays the action
on the real core, or the lot that owes it: no replay exists yet, and the
sidecar lists no test until one does.

| Model action | Registry rule | Scenario step | Replay |
| - | - | - | - |
| `ClientPublish` | — | `ClientPublish` (P1-02; P1-02b step 1) | Not replayed yet; L4 |
| `Receive`, slot `Absent` | `inbound_qos2.absent.publish.admit` (first half) | Driven by `DriveUntil` | Not replayed yet; L4 |
| `Prepare` | `inbound_qos2.absent.publish.admit` (second half) | Driven | Not replayed yet; L4 |
| `Prepare` disabled, no identity left | No rule: `inbound_qos2.absent.publish.defer` covers a lack of effect capacity, not an exhausted identity counter | — | Not replayed yet; L4, after a rule exists (L1 and L3) |
| `Submit` | `command.prepared.submit`; `ArmDeadline` after `Persist` (record 0001) | Driven; P1-02b step 1 | Not replayed yet; L4 |
| `Represent` | `command.indeterminate.replay.same` (and `command.submitted.replay.same`); a changed recipe is `command.*.replay.mismatch`, a fault, which is what `M1a-mut-recipe-change.cfg` violates | — | Not replayed yet; L4 |
| `HoldCommand` | — | Fault action `hold_command` at `store.command.transit` (QABS-A, B, C; P1-02b step 2, record 0015) | Not replayed yet; L4 |
| `ReleaseCommand` | — | Step `ReleaseHeldCommand` (record 0015; P1-02b step 4) | Not replayed yet; L4 |
| `ApplyCommand`, commit | Store side; not a registry rule | Checkpoint `after_commit_before_crash` | Not replayed yet; L4 (L7 on SQLite) |
| `ApplyCommand` on a fenced identity; `RejectCommand` | Store side | QABS-B | Not replayed yet; L4 |
| `LoseResult` | — | Fault `lost_admission_result`: `drop_result` at `store.result.ready` | Not replayed yet; L4 |
| `DeliverResult`, committed | `command.submitted.result.committed`, `command.indeterminate.result.committed` | Driven | Not replayed yet; L4 |
| `DeliverResult`, rejected | `command.submitted.result.rejected`, `command.indeterminate.result.rejected` | QABS-B | Not replayed yet; L4 |
| `DeliverResult`, unknown | `command.submitted.result.unknown`, `command.indeterminate.result.unknown` | P1-02b step 3 | Not replayed yet; L4 |
| `DeliverResult`, late unknown | `command.committed.result.unknown`, `command.rejected.result.unknown`, `inbound_qos2.await_pubrel.late_unknown.ignore` | P1-02b step 6 | Not replayed yet; L4 |
| `DeliverResult`, repeated commit | `command.committed.result.duplicate` | — | Not replayed yet; L4 |
| `DeliverResult`, contradiction | `command.committed.result.contradiction`, `command.rejected.result.contradiction` | — | Not replayed yet; L4 |
| `Timeout` | `command.submitted.timeout`; the second deadline, while the outcome is unknown, `command.indeterminate.timeout`; a deadline after a terminal result is ignored, `command.committed.timeout.stale`, `command.rejected.timeout.stale` | Step `FireDeadline` (record 0015; P1-02b steps 3 and 5) | Not replayed yet; L4 |
| `Resolve` | Store side: `ResolveAndFence`; its refused fence is the injection `refuse_fence` at `store.fence.transit` (record 0015) | P1-02b steps 3 and 5; QABS-A, B, C | Not replayed yet; L4 |
| `Authorize` | `inbound_qos2.admission_pending.committed.authorize` | `DriveUntil PeerPacketObserved PUBREC` | Not replayed yet; L4 |
| `Refuse` | `inbound_qos2.admission_pending.rejected.refuse`. **Differs**: the rule's next state is `Absent`; the model keeps a volatile `Refused` slot and answers a replay with the refusal again. See "Disagreements with the registry" below | QABS-B | Not replayed yet; L4 |
| `Receive`, slot `AwaitPubrel` | `inbound_qos2.await_pubrel.publish.replay` | `ClientResumeExchange` (P1-02) | Not replayed yet; L4 |
| `Receive`, slot `Received` or `AdmissionPending` | `inbound_qos2.admission_pending.publish.replay_after_reconnect` (M1b, after `ConnectionReset`), `inbound_qos2.admission_pending.publish.repeat_on_live_connection` (not reachable: the client never repeats on a live connection) | — | Not replayed yet; L4 |
| `SubmitFrame` | — | `DriveUntil PeerPacketObserved` | Not replayed yet; L4 |
| `ClientObserve` | — | `DriveUntil PeerPacketObserved` | Not replayed yet; L4 |
| `Crash` | — | `CrashBroker` | Not replayed yet; L4 |
| `Recover` | — | `RestartBroker` | Not replayed yet; L4 |
| `ClientReconnect` | — | `ClientResumeSession` | Not replayed yet; L4 |
| `ClientResend` | — | `ClientResumeExchange` | Not replayed yet; L4 |
| `ConnectionReset` (M1b) | — | — (no step closes a connection without a crash yet) | Not replayed yet; L4, after vocabulary by L3 |
| `SourceSessionReset` (M1b) | No rule: the registry has no session machine. A late result for the detached identity is `inbound_qos2.absent.admission_result.stale` | — | Not replayed yet; L4, after a rule exists |
| `DestSessionReset` (M1b) | No rule: the registry has no session machine | — (the fixture keeps the destinations without reset) | Not replayed yet; L4, after a rule exists |
| `Settle` (M1b) | — (outbound delivery, outside M1) | — | Not an admission step; owed by the lot of outbound delivery |
| `ResolveRead` (M1b) | Store side: first half of `ResolveAndFence` | QABS-A, B, C: the absent read | Not replayed yet; L4 (L2 for the store alone) |
| `FenceEstablish` (M1b) | Store side: second half of `ResolveAndFence` | QABS-A (commit returned), QABS-B (fence established) | Not replayed yet; L4 (L2 for the store alone) |
| `FenceAbandon` (M1b) | Store side | QABS-C | Not replayed yet; L4 (L2 for the store alone) |
| `Quiescent` | — | End of the run | Not an action of the product |

Not in the model: the `defer` rules on effect capacity, PUBREL and the
`release` rules.

## Disagreements with the registry

A disagreement between a model and a registry rule is a decision to take,
not an edit on either side (DEM-025). One is open:

- After a certain rejection, `inbound_qos2.admission_pending.rejected.refuse`
  takes the slot to `Absent`; `Refuse` of M1a and M1b takes it to a
  volatile `Refused` slot, which answers a replayed PUBLISH of the same
  exchange with the refusal again. The two differ only when the client
  sends that PUBLISH again before it observed the refusal, which M1b
  reaches after a connection reset: under the rule the replay would open
  a new admission with a new identity. Which one is intended is not
  decided here; the model keeps its predicate until it is.

## Witnesses and scenarios

The scenarios expected to replay each witness, once L4 runs them:

| Configuration | Scenario |
| - | - |
| `M1a-reach-recovery.cfg`, `M1a-reach-crash-after-commit.cfg`, `M1a-reach-lost-result.cfg` | P1-02 |
| `M1a-reach-live-path.cfg`, `M1a-reach-unknown.cfg`, `M1a-live-path.cfg` | P1-02b |
| `M1a-reach-rejection.cfg`, `M1a-reach-fence-while-held.cfg`, `M1a-mut-unknown-as-rejection.cfg` | QABS-B, QABS-C, DEM-MU-01 |
| `M1a-mut-second-admission.cfg` | DEM-MU-04 |
| `M1a-mut-late-unknown.cfg` | P1-02b step 6, DEM-MU-05 |
| `M1a-mut-ack-before-commit.cfg` | Assertion `no_positive_ack_before_confirmed_commit` |
| `M1a-mut-recipe-change.cfg` | Assertion `same_recipe_for_op_A` |
| `M1b-reach-commit-before-fence.cfg` | QABS-A (`tests/scenarios/qabs/QABS-A.json`) |
| `M1b-reach-fence-before-command.cfg`, `M1b-mut-fence-on-stale-read.cfg` | QABS-B (`tests/scenarios/qabs/QABS-B.json`) |
| `M1b-reach-replay-while-unknown.cfg`, `M1b-mut-replay-readmits.cfg` | Rule `inbound_qos2.admission_pending.publish.replay_after_reconnect`; no scenario yet |
| `M1b-reach-epoch-closed-refusal.cfg`, `M1b-mut-old-epoch-executes.cfg` | No scenario yet; DEM-012 |
| The other M1b configurations | No scenario yet |
