---- MODULE WritePumpCancellation ----
EXTENDS Naturals

(***************************************************************************
WritePump cancellation ownership model for mqttium issue #509.

A Python CancelledError has two materially different owners:

1. lifecycle cancellation requested on the writer task;
2. a dependency (transport write) raising CancelledError while the writer task
   itself has no cancellation request.

Only (1) may terminate the writer without reporting a transport failure.
ClassifyDependencyCancel=TRUE models the candidate implementation.
FALSE models rc15.
***************************************************************************)

CONSTANT ClassifyDependencyCancel
ASSUME ClassifyDependencyCancel \in BOOLEAN

VARIABLES
  writerAlive,
  writing,
  cancelRequested,
  generationValid,
  failureReported,
  receiptPending,
  connectionLive,
  latencyFailure

vars == <<
  writerAlive,
  writing,
  cancelRequested,
  generationValid,
  failureReported,
  receiptPending,
  connectionLive,
  latencyFailure
>>

Init ==
  /\ writerAlive = TRUE
  /\ writing = FALSE
  /\ cancelRequested = FALSE
  /\ generationValid = TRUE
  /\ failureReported = FALSE
  /\ receiptPending = FALSE
  /\ connectionLive = TRUE
  /\ latencyFailure = FALSE

TypeOK ==
  /\ writerAlive \in BOOLEAN
  /\ writing \in BOOLEAN
  /\ cancelRequested \in BOOLEAN
  /\ generationValid \in BOOLEAN
  /\ failureReported \in BOOLEAN
  /\ receiptPending \in BOOLEAN
  /\ connectionLive \in BOOLEAN
  /\ latencyFailure \in BOOLEAN

CommitQoS ==
  /\ connectionLive
  /\ writerAlive
  /\ generationValid
  /\ ~receiptPending
  /\ receiptPending' = TRUE
  /\ UNCHANGED <<
       writerAlive, writing, cancelRequested, generationValid,
       failureReported, connectionLive, latencyFailure
     >>

WriterTake ==
  /\ writerAlive
  /\ ~writing
  /\ receiptPending
  /\ writing' = TRUE
  /\ UNCHANGED <<
       writerAlive, cancelRequested, generationValid, failureReported,
       receiptPending, connectionLive, latencyFailure
     >>

RequestLifecycleStop ==
  /\ writerAlive
  /\ cancelRequested' = TRUE
  /\ UNCHANGED <<
       writerAlive, writing, generationValid, failureReported,
       receiptPending, connectionLive, latencyFailure
     >>

OwnerInvalidate ==
  /\ connectionLive
  /\ connectionLive' = FALSE
  /\ generationValid' = FALSE
  /\ UNCHANGED <<
       writerAlive, writing, cancelRequested, failureReported,
       receiptPending, latencyFailure
     >>

TransportCancelledRc15 ==
  /\ ~ClassifyDependencyCancel
  /\ writerAlive
  /\ writing
  /\ ~cancelRequested
  /\ ~latencyFailure
  /\ writerAlive' = FALSE
  /\ writing' = FALSE
  /\ UNCHANGED <<
       cancelRequested, generationValid, failureReported,
       receiptPending, connectionLive, latencyFailure
     >>

TransportCancelledCandidate ==
  /\ ClassifyDependencyCancel
  /\ writerAlive
  /\ writing
  /\ ~cancelRequested
  /\ ~latencyFailure
  /\ writerAlive' = FALSE
  /\ writing' = FALSE
  /\ generationValid' = FALSE
  /\ failureReported' = TRUE
  /\ UNCHANGED <<
       cancelRequested, receiptPending, connectionLive, latencyFailure
     >>

LatencyCancelled ==
  /\ writerAlive
  /\ writing
  /\ ~cancelRequested
  /\ writerAlive' = FALSE
  /\ writing' = FALSE
  /\ latencyFailure' = TRUE
  /\ generationValid' = FALSE
  /\ failureReported' = TRUE
  /\ UNCHANGED <<cancelRequested, receiptPending, connectionLive>>

SettleFailure ==
  /\ failureReported
  /\ connectionLive' = FALSE
  /\ receiptPending' = FALSE
  /\ UNCHANGED <<
       writerAlive, writing, cancelRequested, generationValid,
       failureReported, latencyFailure
     >>

Next ==
  \/ CommitQoS
  \/ WriterTake
  \/ RequestLifecycleStop
  \/ OwnerInvalidate
  \/ TransportCancelledRc15
  \/ TransportCancelledCandidate
  \/ LatencyCancelled
  \/ SettleFailure

Spec == Init /\ [][Next]_vars

(* A dead writer that was not cancelled by its owner cannot leave a live,
   admissible writer generation behind. *)
DeadDependencyWriterIsRetired ==
  ~(~writerAlive /\ ~cancelRequested /\ generationValid /\ connectionLive)

=============================================================================
