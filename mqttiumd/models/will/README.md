# Finite durable Will occurrence model

`MWill` is a finite specification model for integration PR
[#648](https://github.com/yoch/mqttium/pull/648). It checks the chosen durable
intention family: accepted Open installs an armed Will; an exact normal End
cancels it; an abnormal End, takeover or startup makes it pending; one atomic
Will admission consumes that intention and creates ordinary destination
obligations. This is not a Rust refinement, an implementation trace replay,
an MQTT conformance proof, a real crash experiment or independent-oracle
qualification. The sidecar therefore says `specification` and `tests: []`.

From the repository root, with the pinned tool environment:

```bash
python tools/formal/run_tlc.py --models-dir mqttiumd/models/will --list
python tools/formal/run_tlc.py --models-dir mqttiumd/models/will
python mqttiumd/models/check_models.py
python -m unittest discover -s mqttiumd/models -p 'test_*.py'
```

## Normative boundary

The source is the
[OASIS MQTT 5.0 OS specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
The paragraph numbers below refer to the main body, rather than its
non-normative mandatory-statement appendix.

* [Section 3.1.2.5](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901040),
  MQTT-3.1.2-7/-8/-10, associates the Will with the accepted session,
  requires publication following connection close unless normal DISCONNECT
  cancelled it, and removes the stored Will after publication or cancellation.
  Its examples include I/O failure, Keep Alive timeout and a server-initiated
  close without normal DISCONNECT. Shutdown or failure may defer publication
  until a subsequent restart; this model does not promise publication before
  an unobservable process loss.
* [Section 3.1.4](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901073),
  MQTT-3.1.4-3/-4/-5, orders takeover of the existing transport and its Will
  handling before Clean Start processing and successful CONNACK. Its explicit
  non-normative clarification says that Will Delay zero sends the old Will
  at close even when Clean Start zero resumes the same session.
  A rejected CONNECT is not an installed Will. Conversely, failure to observe
  the positive CONNACK does not prove that the server rejected an Open whose
  acceptance already committed.
* [Section 3.1.3.2.2](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901062)
  makes absent delay zero. MQTT-3.1.3-9 cancels the delayed publication only
  for a new connection before the delay elapses; a zero delay gives no such
  reconnection grace period.
* [Section 3.14.4](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901216),
  MQTT-3.14.4-3, requires discarding the current connection's Will on normal
  DISCONNECT zero. Reason 0x04 explicitly requests publication and follows
  the abnormal/publication branch here. An expiry-zero session ends at
  transport close; that cleanup does not cancel other destinations' newly
  admitted Will obligations.

The product profile is non-retained Will QoS1, absent/zero Will Delay and no
other Will properties. Will Message Expiry is explicitly refused rather than
starting a message lifetime at CONNECT. Nonzero delay, retained Will and
Will QoS0/2 are outside the model and this delivery.

## Identities, rows and authority

There are at most two non-reused attachment occurrences of one reusable
ClientID. An occurrence denotes the immutable installing Open OperationId,
its exact SessionId/attachment guard and one separately identified Will
admission operation. The occurrence numbers are finite labels, not recycled
Packet Identifiers, transport descriptors or proposed production counters.
The paired Open and publication operations have different identities; repeated
submission/resolution of either one keeps its own identity and recipe.

`d.will[o]` is an actual durable row only when `installed` or `pending`.
Its `absent` cells cost no row; cancelled/published Wills leave no terminal
Will tombstone. A pending row owns its payload reference independently of the
source session. `CleanupSource` may consequently remove that session before
admission without losing the pending Will or rebinding it to a later ClientID
incarnation. `d.current` identifies the latest attachment, not the target of
an older event. `OldClose` carries a saved, already attributed occurrence 1
through the real guard abstraction after occurrence 2 is installed; it must
not resolve occurrence 2 by current ClientID.

`v.open` distinguishes submitted, unknown, known and applied Open results.
Only a committed installation can reach the known result and positive CONNACK.
`ObserveClose` may run while the result is still unknown or known but unapplied.
`CommitClose` uses the original occurrence and the remembered normal/abnormal
intent, so early normal DISCONNECT is not lost when Open later completes.
A previously observed normal close is processed before a later takeover can
replace its decision with generic transport loss.

Only **durably committed cancellation** survives a process crash. If the
process dies after parsing normal DISCONNECT but before recording its
cancellation, recovery cannot infer those lost bytes from an armed durable
row. Such a surviving row becomes pending. This unavoidable information
boundary is different from the supported normal-close path, and is not
advertised as proof that every client-side `disconnect()` caused cancellation.

`d.openReceipt` and `d.admissionReceipt` abstract the existing retained-result
or compact retired-identity guard, rather than introduce permanent per-Will
history. Detailed receipt collection is checked independently by
[`MReceiptClosure`](../gc/README.md). The fixed arrays and all `h` fields are
finite verifier notation, not tables with one production row per old client.
`h` records test history for invariants and witnesses; normal guards do not
consult it. Once a publication result was applied and retired, replaying the
old operation still cannot execute it or recreate its obligations.

## Admission, source death and capacity

Will admission atomically consumes the exact pending row and creates frozen
target obligations. The two possible targets are the source session itself
and an independent persistent session. If the source has already ended,
only the independent target remains eligible. The No Local, wildcard,
per-contribution QoS and canonical routing decisions that produce this set
remain obligations of their existing model and implementation tests.
Selection is abstracted at the atomic admission decision. If the concrete
recipe freezes routes earlier, its unchanged bytes during uncertainty and
its stale-target refusal/retry ordering require separate implementation tests;
the model does not refine that queue interval.

Two Will rows and three reference credits are the finite configured budgets.
An initial unrelated reference is a pressure fixture. The atomic transfer
releases the registered reference and charges each new target reference;
admission waits when its net cost does not fit. No timeout or object absence
turns that wait into a certain refusal or deletes the pending intention.
`ReleasePressure` and target completion make capacity available. The product's
actual bounded intent/payload and control reservations require their own
tests; the numbers here deliberately remain small finite workloads.

`CleanupSource` returns only its own target reference, if one has already been
admitted. It cannot cancel the other target. `FinishTarget` stands for that
destination's ordinary durable Finish, not a local write receipt or successful
source cleanup. The Will is non-retained and never changes a retained slot.
`references` is updated independently by each ownership delta, while
`ReferencesExact` recomputes the owners. `PayloadLifetimeExact` retains each
payload exactly while its installed/pending Will row or a destination owns it.
Each source/target cleanup is conditional and returns its credit once.

The product maps these decisions to `SessionAction::OpenWillAt` and
`EndWill` (new action subtags 5/6 under recipe kind 8, preserving historical
subtags 0–4), plus the separately versioned `WillPublication` recipe kind 17.
WillPublication consumes the pending intention in the same transaction that
admits its publication, responsibilities and correlated result. Routing and
admission never require the source session to remain alive.

## Process loss and limitations

The safety configuration explores two attachment occurrences without a
process cut. The recovery configuration explores one occurrence with at most
one process cut. `Crash` loses transports and volatile requests; committed
rows, payload references, obligations and terminal receipts survive.
`Recover` converts surviving armed rows to pending. It does not reinstall
an absent Will, reinterpret absent business objects as an uncommitted result,
or create a fresh admission identity. An Open never committed before the cut
is rejected as an abandoned request; an accepted Open can produce its Will
even if no CONNACK reached the client. A pending or admitted occurrence can
survive source cleanup and result collection.

These are exhaustive checks of the named finite state spaces, with no
fairness or liveness claim. Atomic store transactions, committed-state
survival, authentic result correlations, durable receipt retirement,
non-reused causal counters, bounded reader validation and ordered lifecycle
submission are assumptions. The model does not check the socket decoder,
Keep Alive arithmetic, process signals, real writer queues, payload-byte
limits, all combinations of two attachments and a crash, SQLite/fsync,
power loss or universal MQTT correctness. Product mutations and system-client
runs are separate evidence, and neither can be replaced by these configs.

## Expected outcomes

| Configuration | Expected result |
| - | - |
| `safety` | All thirteen invariants pass with two occurrences, normal/abnormal early closes, takeover, pressure, cleanup and receipt collection. |
| `recovery` | The same invariants pass for one occurrence with one possible process loss and startup discovery. |
| `mut-early-connack` | `ConnackAfterDurableInstall` fails when an uncommitted Open can authorize CONNACK. |
| `mut-publish-normal` | `NoAdmissionAfterCancellation` fails when a normal End incorrectly queues publication. |
| `mut-forget-on-crash` | `AcceptedWillNotLost` fails when recovery deletes an installed/pending intention. |
| `mut-readmit-lost-result` | `AtMostOneAdmission` fails when retrying a committed occurrence admits it again. |
| `mut-rebind-old-close` | `NoStaleClose` fails when an attributed old Close targets the new current attachment. |
| `reach-recovery` | Intentionally violate `NoRecoveryWitness` after a committed Will is discovered pending at startup. |
| `reach-retired-live` | Intentionally violate `NoRetiredLiveWitness` after same-operation resolution, receipt retirement and source cleanup, with the independent obligation still owning the payload. |
| `reach-independent-intent` | Intentionally violate `NoIndependentIntentWitness` with a pending Will and its reference surviving removal of its source before admission. |
| `reach-takeover` | Intentionally violate `NoTakeoverWitness` with the old source cleaned up, its independent obligation live, the new Will installed and the saved old Close ignored. |

A negative configuration must violate its exact declared invariant. Parse
errors, tool failures, timeouts and unvisited injections are not detections.
The mutants change the finite specification and do not claim to mutate the
compiled broker.

## Correspondence

The table states implementation obligations, not completed Rust trace replays.

| Model action | Implementation predicate | Replay |
| - | - | - |
| `SubmitOpen`, `OpenUnknown` | An OpenWillAt request retains its identity, description and original transport guard while outcome is unknown. | Lifecycle writer/result tests; `safety`, `recovery`. |
| `CommitOpen` | Atomically install the exact armed Will; takeover first decides the old occurrence, without inheritance by the new attachment. | Both-store OpenWillAt/takeover tests; `reach-takeover`. |
| `RejectOpen` | A refused or abandoned uncommitted CONNECT cannot install or publish a Will. | Codec/negative-CONNACK and precommit crash tests; `NoUnacceptedAdmission`. |
| `ResolveOpen`, `ApplyOpen` | Authentic committed installation is applied before a still-live request receives successful CONNACK; a dead request does not regain readiness. | Runtime early-close/result permutations; `ConnackAfterDurableInstall`. |
| `PrematureConnack` | Negative control authorizes readiness without durable installation. | `mut-early-connack`, exact `ConnackAfterDurableInstall` violation. |
| `ObserveClose` | Pin normal/abnormal intent to the original attachment, including before Open's result applies. | Runtime before/during/after-Open-close tests; public normal DISCONNECT versus real transport loss. |
| `CommitClose` | EndWill cancels on normal zero; loss, takeover or 0x04 leaves an independently owned pending intention. | Both-store EndWill and early DISCONNECT tests; `mut-publish-normal`. |
| `SubmitAdmission`, `AdmissionUnknown` | Submit the stable WillPublication operation and exact canonical bytes; temporary incapacity retains the pending row. | Capacity/lost-result tests and transaction barriers; `safety`. |
| `CommitAdmission` | Atomically consume the exact pending Will while admitting its frozen publication and obligations. | Both-store WillPublication and actual crash boundaries; `AtMostOneAdmission`. |
| `ResolveAdmission`, `ApplyAdmission` | Resolve the same operation; result application does not repeat admission or require a live source. | Delayed/lost-result runtime tests; `reach-retired-live`. |
| `CollectReceipt`, `RepeatAdmission` | Result consumption/retirement forbids reexecution without retaining Will terminal rows. | Existing receipt gates plus Will replay tests; `mut-readmit-lost-result`. |
| `CleanupSource` | Session termination preserves an independent pending Will and other targets' admitted references. | Expiry-zero/explicit-source-end fanout tests; `IndependentResponsibility`. |
| `FinishTarget` | Only that destination's durable Finish returns its reference. | Existing QoS1 Finish gates and Will offline-destination completion. |
| `ReleasePressure` | Independent completion restores capacity; maintenance cannot silently discard a pending Will. | Bounded saturation/capacity-recovery tests; `CapacityBounded`. |
| `OldClose` | An already attributed old attachment fact cannot be looked up against the current ClientID binding. | Actual old-generation runtime input tests; `mut-rebind-old-close`. |
| `Crash`, `Recover` | Lose volatile transports, rediscover exact pending intentions and preserve admitted obligations on the same file. | Separate process/signal witnesses; `mut-forget-on-crash`, `reach-recovery`. On the executable: `a_kill_before_the_commit_publishes_the_will_a_normal_disconnect_cancelled` and `a_kill_after_the_commit_keeps_the_will_cancelled` (`crates/mqttiumd/tests/disconnect_durability.rs`). |
| `Idle` | Safety does not assume a fair scheduler or a bounded publication delay. | All configurations; no liveness declaration. |
