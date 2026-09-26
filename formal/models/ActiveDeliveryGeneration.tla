---- MODULE ActiveDeliveryGeneration ----
EXTENDS Naturals

(***************************************************************************
Iterator admissions waiting for capacity (#500).

Refines ApplicationDelivery._accept_waiting_unaccounted / _accepted in
api/_delivery.py. A reader-owned delivery that finds the iterator queue full
captures the admission generation, then loops:

    while generation == _admission_generation and queue full:
        space.clear(); waiters += 1; await space.wait(); waiters -= 1
    if generation != _admission_generation: return False   # dropped
    enqueue; return True                                     # committed

_wake_waiters() sets `space` only while a waiter exists; it runs when the
application consumes (release), on invalidate_waiting_admissions() (a new
connection epoch) and on reset_stream() (a new application stream, whose
queue is emptied). The queue holds one message.

Variant = "rc15": the loop waits for capacity only and always commits; a new
  connection epoch does not wake it (reset_stream() did).
Variant = "fixed": the implementation above.

Invariant: a waiting delivery never commits into a connection epoch or an
application stream generation other than the one it was admitted under.
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"rc15", "fixed"}

VARIABLES
  epoch,            \* connection epoch
  stream,           \* application stream generation
  admission,        \* _admission_generation
  queued,           \* messages in the iterator queue (capacity 1)
  space,            \* the asyncio.Event
  waiter,           \* "none", "check", "blocked"
  waiterAdmission,  \* generation captured by the waiter
  waiterOwner,      \* <<epoch, stream>> when the waiter was admitted
  arrivals,         \* deliveries started (bound)
  stale             \* a waiter committed under another owner

vars == <<epoch, stream, admission, queued, space, waiter, waiterAdmission,
          waiterOwner, arrivals, stale>>

Init ==
  /\ epoch = 0
  /\ stream = 0
  /\ admission = 0
  /\ queued = 1
  /\ space = FALSE
  /\ waiter = "none"
  /\ waiterAdmission = 0
  /\ waiterOwner = <<0, 0>>
  /\ arrivals = 0
  /\ stale = FALSE

Wake(s) == IF waiter = "blocked" THEN TRUE ELSE s

\* The reader hands one delivery to a full queue: the slow path.
Arrive ==
  /\ waiter = "none"
  /\ queued = 1
  /\ arrivals < 2
  /\ waiter' = "check"
  /\ waiterAdmission' = admission
  /\ waiterOwner' = <<epoch, stream>>
  /\ arrivals' = arrivals + 1
  /\ UNCHANGED <<epoch, stream, admission, queued, space, stale>>

Current == waiterAdmission = admission

\* One evaluation of the loop condition, and the exit when it is false.
Check ==
  /\ waiter = "check"
  /\ IF (Variant = "rc15" \/ Current) /\ queued = 1
     THEN /\ waiter' = "blocked"
          /\ space' = FALSE
          /\ UNCHANGED <<queued, stale>>
     ELSE /\ waiter' = "none"
          /\ IF Variant = "fixed" /\ ~Current
             THEN UNCHANGED <<queued, stale, space>>
             ELSE /\ queued' = 1
                  /\ stale' = (stale \/ waiterOwner # <<epoch, stream>>)
                  /\ UNCHANGED space
  /\ UNCHANGED <<epoch, stream, admission, waiterAdmission, waiterOwner, arrivals>>

Resume ==
  /\ waiter = "blocked"
  /\ space
  /\ waiter' = "check"
  /\ UNCHANGED <<epoch, stream, admission, queued, space, waiterAdmission,
                 waiterOwner, arrivals, stale>>

\* The application takes the queued message: release() wakes the waiter.
Consume ==
  /\ queued = 1
  /\ queued' = 0
  /\ space' = Wake(space)
  /\ UNCHANGED <<epoch, stream, admission, waiter, waiterAdmission,
                 waiterOwner, arrivals, stale>>

\* A new connection epoch: invalidate_waiting_admissions().
NewEpoch ==
  /\ epoch < 2
  /\ epoch' = epoch + 1
  /\ IF Variant = "fixed"
     THEN /\ admission' = admission + 1
          /\ space' = Wake(space)
     ELSE UNCHANGED <<admission, space>>
  /\ UNCHANGED <<stream, queued, waiter, waiterAdmission, waiterOwner,
                 arrivals, stale>>

\* A new application stream: reset_stream() empties the queue.
NewStream ==
  /\ stream < 2
  /\ stream' = stream + 1
  /\ queued' = 0
  /\ space' = Wake(space)
  /\ admission' = IF Variant = "fixed" THEN admission + 1 ELSE admission
  /\ UNCHANGED <<epoch, waiter, waiterAdmission, waiterOwner, arrivals, stale>>

\* The application reads the next stream's queue until it fills again.
Refill ==
  /\ queued = 0
  /\ waiter = "none"
  /\ queued' = 1
  /\ UNCHANGED <<epoch, stream, admission, space, waiter, waiterAdmission,
                 waiterOwner, arrivals, stale>>

Terminal == waiter = "none" /\ arrivals = 2

Quiescent == Terminal /\ UNCHANGED vars

Next ==
  \/ Arrive \/ Check \/ Resume \/ Consume \/ NewEpoch \/ NewStream \/ Refill
  \/ Quiescent

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ epoch \in 0..2
  /\ stream \in 0..2
  /\ queued \in 0..1
  /\ space \in BOOLEAN
  /\ waiter \in {"none", "check", "blocked"}

NoStaleCommit == ~stale

=============================================================================
