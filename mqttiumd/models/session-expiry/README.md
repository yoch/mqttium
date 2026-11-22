# Finite session expiry and temporal continuity

`MSessionExpiry` is a finite specification model of the new session-expiry
mechanism. It extends the decision coverage of
[`MSessionLifecycle`](../session-lifecycle/README.md) without changing that
model's eight expected results or the earlier receipt/PI models. It is not a
Rust refinement, an MQTT conformance proof, or evidence about the host clock,
SQLite commit latency or a real process crash.

```bash
# From the repository root, with the pinned tool environment:
python tools/formal/run_tlc.py --models-dir mqttiumd/models/session-expiry --list
python tools/formal/run_tlc.py --models-dir mqttiumd/models/session-expiry
cd mqttiumd
python models/check_models.py
python -m unittest discover -s models -p 'test_*.py'
```

## State and abstraction of time

The workload has one reusable session name, a finite number of distinct
incarnation/operation identities and a finite BOOTTIME tick domain. The model
uses a TTL of two integer ticks. A tick is an abstract unit: the product uses
checked nanoseconds and also tests maximum wire intervals and arithmetic
failures. The model does not establish numeric overflow, precision or procfs
parsing correctness.

`session.attached` is durable attachment state; `active` says whether its
transport is still live. An observed close sets `active` false and records
one immutable request with its original incarnation, attachment token and
observed logical instant. Commit can occur after additional ticks. Its
fixed deadline is `request.observed + TTL`, never commit time plus TTL.
A later open installs a fresh attachment token; a new incarnation also gets
a fresh session identity. Identity exhaustion suspends this finite workload;
it does not permit a production causal counter to wrap.

The global durable anchor contains a recognized domain, the tick at its
engagement and the logical progress already accounted for. In a continuous
domain the admissible time is:

```
anchor.logical + (current_tick - anchor.tick)
```

`Tick` can occur while the process is stopped. Restart in that same domain
therefore retains elapsed progress, including after repeated crashes.
`Checkpoint` coalesces that progress into the one anchor. It adds no per-tick
row or receipt history. This model has no real-time scheduling/fairness
assumption and does not establish the product's checkpoint-age bound.

A domain change resets the physical tick origin but retains committed logical
progress. No expiry/open decision runs before `ReanchorAndRestart` commits
that new anchor. Unknown downtime is not subtracted, and existing deadlines
are not reset. The model's two domains mean proven different clock origins.
Production recognition additionally requires a boot identity and an effective
time-namespace BOOTTIME origin; unreadable or unrecognized origin information
uses the conservative changed-domain path. That identification is a separate
provider test obligation. UTC has no modeled authority and no action reads it.

If a process dies while a finite session is still durably attached, recovery
uses its first reliable observation to establish detachment. A session already
durably detached preserves its deadline instead. `Crash` may discard an
uncommitted close request, but not a committed detachment or the clock anchor.
This distinction is intentional: the model cannot infer an unrecorded exact
transport-loss instant.

## Ordering, old facts and scope

`Open` and `ApplyTimer` are alternative atomic steps in one durable order.
An open at or after the current detached deadline creates a fresh incarnation
and does not depend on whether `QueueTimer` has run. A winning earlier resume
clears the detached state and advances the attachment token. A saved timer
carries its original session, attachment token, detachment identity and
deadline. Applying it rechecks all of those values, offline state and time.

Only one saved timer exists. It represents one duplicate of an already
associated internal fact, not a historical timer heap or a promise about
unobserved socket bytes. It may survive an original expiry and a new open.
It cannot acquire authority over that new incarnation or attachment.

The model collapses submission/result consumption for Open and Expire into
an atomic authoritative cut. `CommitDetach` separates observed close time
from engagement, but does not duplicate the complete receipted command
machine. Genuine held results, same-OperationId resolution, ownership of
results, bounded Outbox reservations and collection remain covered by their
existing models and concrete tests. The prior lifecycle model retains its
shared-payload cancellation, target-only cleanup, zero/infinite retention and
receipt-hole properties. This model does not replace those obligations.

`h` variables are verifier histories used for invariants and one reachability
witness. Normal enabling predicates do not use them. No fairness is assumed;
`Idle` permits no progress. Passing a finite configuration does not prove
universal liveness, indefinite service, a monotonic physical clock on every
Linux environment or a bound during storage unavailability.

## Expected results

| Configuration | Expected result and question |
| - | - |
| `safety` | All six invariants pass with no crash: delayed detach, active immunity, exact deadline boundaries, reopening and copied timers. |
| `continuity` | All invariants pass with up to two process losses in one recognized domain, including clock progress while stopped. |
| `domain-change` | All invariants pass across one process loss and an optional changed origin; only committed progress crosses that change. |
| `mut-deadline-at-commit` | `FixedDetachDeadline` fails when delayed COMMIT rearms from its later time. |
| `mut-stale-timer` | `NoStaleTimer` fails when a saved old timer can delete the current occupant without checking its immutable occurrence. |
| `mut-wait-for-timer` | `NoExpiredResume` fails when Open resumes an expired session because the timer has not applied. |
| `mut-lose-progress` | `ContinuousDomainProgress` fails when same-domain restart discards elapsed ticks by reanchoring without accounting for them. |
| `reach-boundary` | `NoBoundaryWitness` is intentionally violated after a delayed detachment, restart of that already-detached session and a queued due timer. This guards against vacuous timing/crash exploration. |

The sidecar requires the exact named invariant for every negative result.
A syntax error, timeout, unexpected invariant or incomplete exploration is
not a successful mutation detection. The specification mutations are distinct
from compiled product mutations and their system reception.

## Correspondence

Concrete predicates live in the shared `SessionLifecycle` actions, the
owner's serialized lifecycle flight, the stores' lifecycle staging and the
product `session_time` provider/runtime. The table names the separate
implementation obligations rather than claiming that this model-only lot
executed their tests. The sidecar remains `specification`, with `tests: []`:
no TLC-to-Rust trace refinement is registered.

| Model action | Implementation predicate | Replay |
| - | - | - |
| `Tick` | Safe external BOOTTIME reading and checked logical projection from the committed anchor. | Provider deterministic clock tests; `safety`, `continuity` and `domain-change` configurations. |
| `ObserveClose` | One closure observation retained with original session/attachment while Open or Detach is pending. | Real writer-result DISCONNECT0 drainage regression and finite-close runtime regressions; `safety` and boundary witness. |
| `CommitDetach` | Canonical Detach preserves captured close time despite delayed commit or result. | Store deterministic delayed-detach checks and product prepared/committed barriers; `mut-deadline-at-commit`. |
| `QueueTimer` | At most one bounded current wake per detached finite session; periodic maintenance is a fallback. | Runtime timer-cardinality and rearming tests; all positive configurations. |
| `ApplyTimer` | Expire checks current session, attachment, detachment identity, deadline, offline state and admissible time in the same order as Open. | Store/core stale-occurrence and deadline-boundary tests; `mut-stale-timer`. |
| `Open` | OpenAt decides due-before-resume without relying on prior timer execution and installs a new attachment token. | Before/exactly-at/after deadline and held-timer CONNECT tests; `mut-wait-for-timer`. |
| `Checkpoint` | One CAS-protected clock anchor advances monotonically without generating receipt history. | Store clock-checkpoint CAS/repetition tests; continuity/domain-change configurations. |
| `Crash` | A process loss destroys transports and volatile close requests, retaining committed session/clock state. | Each product crash reception has its own SIGKILL/same-file witness; this action is only an abstract store cut. |
| `RestartSameDomain` | A proven continuous clock origin contributes process downtime; already-detached deadlines remain fixed. | Injected-clock repeated-restart tests; `continuity` and `mut-lose-progress`. |
| `ChangeDomain` | Unproven continuity authorizes no invented downtime; retain only committed progress. | Injected provider/domain-change tests; `domain-change`. |
| `ReanchorAndRestart` | The conservative replacement anchor commits before temporal decisions or readiness; attached finite sessions get one first-recovery detachment. | Store startup/recovery and provider tests; `domain-change`. |
| `Idle` | No implicit fairness, time or progress guarantee. | Every configuration; real capacity and public-client receptions remain separate system evidence. |
