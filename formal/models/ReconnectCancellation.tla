---- MODULE ReconnectCancellation ----
EXTENDS Naturals

(***************************************************************************
Automatic reconnect cancellation ownership model for mqttium issue #510.

CancelledError can originate from:
  1. a cancellation request on the reconnect task itself;
  2. a dependency used by one retry attempt (transport factory/connect path).

Only (1) may terminate the supervisor immediately. Dependency-originated
cancellation must follow ordinary retry/terminal policy, otherwise the
reconnect-preserved application stream can remain open without a supervisor.
***************************************************************************)

CONSTANT ClassifyDependencyCancel, MaxRetries
ASSUME ClassifyDependencyCancel \in BOOLEAN
ASSUME MaxRetries \in Nat

VARIABLES
  taskAlive,
  cancelRequested,
  connected,
  streamOpen,
  attempt,
  trying,
  lastFailure,
  terminalized

vars == <<
  taskAlive,
  cancelRequested,
  connected,
  streamOpen,
  attempt,
  trying,
  lastFailure,
  terminalized
>>

Init ==
  /\ taskAlive = TRUE
  /\ cancelRequested = FALSE
  /\ connected = FALSE
  /\ streamOpen = TRUE
  /\ attempt = 0
  /\ trying = FALSE
  /\ lastFailure = FALSE
  /\ terminalized = FALSE

TypeOK ==
  /\ taskAlive \in BOOLEAN
  /\ cancelRequested \in BOOLEAN
  /\ connected \in BOOLEAN
  /\ streamOpen \in BOOLEAN
  /\ attempt \in Nat
  /\ trying \in BOOLEAN
  /\ lastFailure \in BOOLEAN
  /\ terminalized \in BOOLEAN

StartAttempt ==
  /\ taskAlive
  /\ ~connected
  /\ streamOpen
  /\ ~trying
  /\ attempt < MaxRetries
  /\ trying' = TRUE
  /\ attempt' = attempt + 1
  /\ UNCHANGED <<
       taskAlive, cancelRequested, connected, streamOpen,
       lastFailure, terminalized
     >>

DependencyCancelRc15 ==
  /\ ~ClassifyDependencyCancel
  /\ trying
  /\ ~cancelRequested
  /\ taskAlive' = FALSE
  /\ trying' = FALSE
  /\ lastFailure' = TRUE
  /\ UNCHANGED <<
       cancelRequested, connected, streamOpen, attempt, terminalized
     >>

DependencyCancelCandidate ==
  /\ ClassifyDependencyCancel
  /\ trying
  /\ ~cancelRequested
  /\ trying' = FALSE
  /\ lastFailure' = TRUE
  /\ UNCHANGED <<
       taskAlive, cancelRequested, connected, streamOpen,
       attempt, terminalized
     >>

RetryExhausted ==
  /\ taskAlive
  /\ ~connected
  /\ streamOpen
  /\ ~trying
  /\ attempt >= MaxRetries
  /\ lastFailure
  /\ streamOpen' = FALSE
  /\ terminalized' = TRUE
  /\ taskAlive' = FALSE
  /\ UNCHANGED <<cancelRequested, connected, attempt, trying, lastFailure>>

AttemptSucceeds ==
  /\ trying
  /\ taskAlive
  /\ connected' = TRUE
  /\ trying' = FALSE
  /\ taskAlive' = FALSE
  /\ UNCHANGED <<
       cancelRequested, streamOpen, attempt, lastFailure, terminalized
     >>

RequestOwnerCancel ==
  /\ taskAlive
  /\ cancelRequested' = TRUE
  /\ UNCHANGED <<
       taskAlive, connected, streamOpen, attempt, trying,
       lastFailure, terminalized
     >>

OwnerCancel ==
  /\ taskAlive
  /\ cancelRequested
  /\ taskAlive' = FALSE
  /\ trying' = FALSE
  /\ streamOpen' = FALSE
  /\ terminalized' = TRUE
  /\ UNCHANGED <<cancelRequested, connected, attempt, lastFailure>>

(* Completed runs are explicit, so TLC still reports any other deadlock. *)
Terminal == ~taskAlive /\ ~trying

Quiescent == Terminal /\ UNCHANGED vars

Next ==
  \/ StartAttempt
  \/ DependencyCancelRc15
  \/ DependencyCancelCandidate
  \/ RetryExhausted
  \/ AttemptSucceeds
  \/ RequestOwnerCancel
  \/ OwnerCancel
  \/ Quiescent

Spec == Init /\ [][Next]_vars

SupervisorDoesNotDisappearWithOpenDeadStream ==
  ~(~taskAlive /\ ~connected /\ streamOpen /\ ~terminalized)

=============================================================================
