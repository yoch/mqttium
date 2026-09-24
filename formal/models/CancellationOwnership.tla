---- MODULE CancellationOwnership ----
EXTENDS Naturals

(***************************************************************************
Cancellation ownership at MQTTium runtime task boundaries.

asyncio.CancelledError does not identify who cancelled. A boundary task is
either cancelled by its owner (Task.cancel(), observable as
Task.cancelling() > 0), or receives a CancelledError raised by a dependency
it awaits (transport read/write/close, transport factory, publication
source, keepalive PINGREQ) while nobody asked it to stop.

Each boundary maps to one handler in src/mqttium/api. Reraise(b) encodes the
handler's actual predicate for "propagate as cancellation":

  Variant = "rc15"   the released predicates:
    writer       cancelling() or latency_failure is None   (_writer.py _run)
    all others   except CancelledError: raise              (type only)
  Variant = "fixed"  _cancel.owner_cancelled(): cancelling() > 0

When the handler does not re-raise, the boundary reports a failure through
its ordinary Exception path. When it re-raises without an owner request, the
task ends silently: the writer leaves a dead generation, the reconnect
supervisor disappears with an open stream, the pump strands drain() targets,
the caller of connect()/publish_many() sees a cancellation it never asked
for (issues #509 #510 #522 #525 #529 #538).

The boundaries are independent, so one is chosen at Init and checked alone;
this keeps the state space linear in the number of boundaries.
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"rc15", "fixed"}

Boundaries == {
  "writer", "pump", "reader", "keepalive",
  "reconnect", "publish_many", "connect"
}

VARIABLES boundary, pc, cancelRequested, latched, raisedWithRequest

vars == <<boundary, pc, cancelRequested, latched, raisedWithRequest>>

States == {
  "running",        \* between awaits
  "awaiting",       \* suspended in a dependency
  "stoppedByOwner", \* propagated a cancellation its owner requested
  "failed",         \* reported a failure through the ordinary path
  "deadSilently"    \* propagated a cancellation nobody requested
}

TypeOK ==
  /\ boundary \in Boundaries
  /\ pc \in States
  /\ cancelRequested \in BOOLEAN
  /\ latched \in BOOLEAN
  /\ raisedWithRequest \in BOOLEAN

Reraise ==
  IF Variant = "fixed"
  THEN cancelRequested
  ELSE IF boundary = "writer"
       THEN cancelRequested \/ ~latched
       ELSE TRUE

Init ==
  /\ boundary \in Boundaries
  /\ pc = "running"
  /\ cancelRequested = FALSE
  /\ latched = FALSE
  /\ raisedWithRequest = FALSE

Await ==
  /\ pc = "running"
  /\ pc' = "awaiting"
  /\ UNCHANGED <<boundary, cancelRequested, latched, raisedWithRequest>>

Resume ==
  /\ pc = "awaiting"
  /\ ~cancelRequested
  /\ pc' = "running"
  /\ UNCHANGED <<boundary, cancelRequested, latched, raisedWithRequest>>

\* The owner calls Task.cancel(): disconnect(), reconnect takeover, stop().
RequestCancel ==
  /\ pc \in {"running", "awaiting"}
  /\ ~cancelRequested
  /\ cancelRequested' = TRUE
  /\ UNCHANGED <<boundary, pc, latched, raisedWithRequest>>

\* Only the writer has a latched latency-batch failure.
LatchLatencyFailure ==
  /\ boundary = "writer"
  /\ pc \in {"running", "awaiting"}
  /\ ~latched
  /\ latched' = TRUE
  /\ UNCHANGED <<boundary, pc, cancelRequested, raisedWithRequest>>

\* A CancelledError reaches the handler: delivered by the owner's request, or
\* raised by the awaited dependency (with or without a concurrent request).
CancelledErrorArrives ==
  /\ pc = "awaiting"
  /\ raisedWithRequest' = cancelRequested
  /\ pc' = IF Reraise
           THEN IF cancelRequested THEN "stoppedByOwner" ELSE "deadSilently"
           ELSE "failed"
  /\ UNCHANGED <<boundary, cancelRequested, latched>>

Terminal == pc \in {"stoppedByOwner", "failed", "deadSilently"}

Quiescent == Terminal /\ UNCHANGED vars

Next ==
  \/ Await
  \/ Resume
  \/ RequestCancel
  \/ LatchLatencyFailure
  \/ CancelledErrorArrives
  \/ Quiescent

Spec == Init /\ [][Next]_vars

\* A dependency-raised CancelledError is always reported as a failure.
NoSilentDependencyDeath == pc # "deadSilently"

\* An owner request is never converted into a failure report.
OwnerCancellationPropagates == pc = "failed" => ~raisedWithRequest

=============================================================================
