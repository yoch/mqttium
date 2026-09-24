---- MODULE ActiveDeliveryGeneration ----
EXTENDS Naturals

(***************************************************************************
Formal boundary for mqttium issue #500.

A delivery has three materially different ownership states:

  pending  - still in DeliveryLane and epoch-tagged there;
  waiting  - active reader-owned work, but not yet committed to messages_queue;
  committed - application-visible and therefore governed by the stream contract.

The bug is in the middle state. rc15 loses owner-generation identity after the
slow ApplicationDelivery waiter starts. Replacing either the Network Connection
epoch or the application stream generation can therefore wake old waiting work
and let it commit into the replacement generation.

GuardAdmissions=TRUE models the candidate: a private admission generation is
captured when the slow wait is created and is invalidated by either owner
replacement. GuardAdmissions=FALSE models rc15.
***************************************************************************)

CONSTANT GuardAdmissions
ASSUME GuardAdmissions \in BOOLEAN

Idle == "idle"
Waiting == "waiting"

VARIABLES
  connectionEpoch,
  streamGeneration,
  admissionGeneration,
  waiter,
  waiterGeneration,
  capacityFull,
  committed,
  staleCommitted

vars == <<
  connectionEpoch,
  streamGeneration,
  admissionGeneration,
  waiter,
  waiterGeneration,
  capacityFull,
  committed,
  staleCommitted
>>

Init ==
  /\ connectionEpoch = 0
  /\ streamGeneration = 0
  /\ admissionGeneration = 0
  /\ waiter = Idle
  /\ waiterGeneration = 0
  /\ capacityFull = TRUE
  /\ committed = 0
  /\ staleCommitted = FALSE

TypeOK ==
  /\ connectionEpoch \in Nat
  /\ streamGeneration \in Nat
  /\ admissionGeneration \in Nat
  /\ waiter \in {Idle, Waiting}
  /\ waiterGeneration \in Nat
  /\ capacityFull \in BOOLEAN
  /\ committed \in Nat
  /\ staleCommitted \in BOOLEAN

NoStaleCommit == ~staleCommitted

StartWait ==
  /\ waiter = Idle
  /\ capacityFull
  /\ waiter' = Waiting
  /\ waiterGeneration' = admissionGeneration
  /\ UNCHANGED <<
       connectionEpoch, streamGeneration, admissionGeneration,
       capacityFull, committed, staleCommitted
     >>

InvalidateConnection ==
  /\ connectionEpoch' = connectionEpoch + 1
  /\ admissionGeneration' = admissionGeneration + 1
  /\ UNCHANGED <<
       streamGeneration, waiter, waiterGeneration,
       capacityFull, committed, staleCommitted
     >>

ResetStream ==
  /\ streamGeneration' = streamGeneration + 1
  /\ admissionGeneration' = admissionGeneration + 1
  /\ UNCHANGED <<
       connectionEpoch, waiter, waiterGeneration,
       capacityFull, committed, staleCommitted
     >>

FreeCapacity ==
  /\ capacityFull
  /\ capacityFull' = FALSE
  /\ UNCHANGED <<
       connectionEpoch, streamGeneration, admissionGeneration,
       waiter, waiterGeneration, committed, staleCommitted
     >>

RefillCapacity ==
  /\ ~capacityFull
  /\ capacityFull' = TRUE
  /\ UNCHANGED <<
       connectionEpoch, streamGeneration, admissionGeneration,
       waiter, waiterGeneration, committed, staleCommitted
     >>

ResumeWaitCurrent ==
  /\ waiter = Waiting
  /\ ~capacityFull
  /\ waiterGeneration = admissionGeneration
  /\ waiter' = Idle
  /\ committed' = committed + 1
  /\ UNCHANGED <<
       connectionEpoch, streamGeneration, admissionGeneration,
       waiterGeneration, capacityFull, staleCommitted
     >>

ResumeWaitStaleRc15 ==
  /\ ~GuardAdmissions
  /\ waiter = Waiting
  /\ ~capacityFull
  /\ waiterGeneration # admissionGeneration
  /\ waiter' = Idle
  /\ committed' = committed + 1
  /\ staleCommitted' = TRUE
  /\ UNCHANGED <<
       connectionEpoch, streamGeneration, admissionGeneration,
       waiterGeneration, capacityFull
     >>

DropWaitStaleCandidate ==
  /\ GuardAdmissions
  /\ waiter = Waiting
  /\ waiterGeneration # admissionGeneration
  /\ waiter' = Idle
  /\ UNCHANGED <<
       connectionEpoch, streamGeneration, admissionGeneration,
       waiterGeneration, capacityFull, committed, staleCommitted
     >>

Next ==
  \/ StartWait
  \/ InvalidateConnection
  \/ ResetStream
  \/ FreeCapacity
  \/ RefillCapacity
  \/ ResumeWaitCurrent
  \/ ResumeWaitStaleRc15
  \/ DropWaitStaleCandidate

Spec == Init /\ [][Next]_vars

(* The counters are monotonic; two owner replacements and two commits already
   cover every interleaving of wait, invalidation and resume. *)
StateBound ==
  /\ connectionEpoch <= 2
  /\ streamGeneration <= 2
  /\ committed <= 2

=============================================================================
