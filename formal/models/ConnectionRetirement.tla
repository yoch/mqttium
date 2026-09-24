---- MODULE ConnectionRetirement ----
EXTENDS Naturals

(***************************************************************************
Reader teardown ownership order and terminal-cause precedence.

Retirement publishes two facts: the new connection epoch (writer and delivery
guards) and the retired protocol engine (no longer CONNECTED). A producer is
admitted into the transport when the engine is CONNECTED and the writer
epoch matches the client epoch.

Variant = "rc15":
  - teardown advances both epochs, then awaits (another task may run), and
    only then retires the engine;
  - the broker DISCONNECT verdict is recorded but becomes the public cause
    only at finalization, if nothing else was written first; a writer failure
    caused by closing the transport overwrites the cause.
Variant = "fixed":
  - epochs and engine are retired in one synchronous step;
  - the broker verdict is latched when observed; later real causes lose
    (first wins).

Invariants (issues):
  NoAdmissionAfterLoss  #544  no publication is admitted into a connection
                              whose loss the reader already observed
  BrokerReasonKept      #543  a broker verdict observed before the writer
                              failure is the connection's public cause
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"rc15", "fixed"}

VARIABLES
  phase,          \* "live" | "halfRetired" | "retired" | "finalized"
  epoch, writerEpoch,
  engineConnected,
  lossObserved,
  admittedAfterLoss,
  brokerSeen,     \* broker DISCONNECT applied
  writerFailed,
  cause           \* "none" | "broker" | "writer" | "synthetic"

vars == <<phase, epoch, writerEpoch, engineConnected, lossObserved,
          admittedAfterLoss, brokerSeen, writerFailed, cause>>

TypeOK ==
  /\ phase \in {"live", "halfRetired", "retired", "finalized"}
  /\ epoch \in 0..1 /\ writerEpoch \in 0..1
  /\ engineConnected \in BOOLEAN
  /\ lossObserved \in BOOLEAN
  /\ admittedAfterLoss \in BOOLEAN
  /\ brokerSeen \in BOOLEAN
  /\ writerFailed \in BOOLEAN
  /\ cause \in {"none", "broker", "writer", "synthetic"}

Init ==
  /\ phase = "live"
  /\ epoch = 0 /\ writerEpoch = 0
  /\ engineConnected = TRUE
  /\ lossObserved = FALSE
  /\ admittedAfterLoss = FALSE
  /\ brokerSeen = FALSE
  /\ writerFailed = FALSE
  /\ cause = "none"

\* A producer (publish_nowait QoS 0) passes the direct-admission guards.
Admit ==
  /\ engineConnected
  /\ writerEpoch = epoch
  /\ admittedAfterLoss' = (admittedAfterLoss \/ lossObserved)
  /\ UNCHANGED <<phase, epoch, writerEpoch, engineConnected, lossObserved,
                 brokerSeen, writerFailed, cause>>

\* The broker's DISCONNECT is applied (the engine is already DISCONNECTED).
BrokerDisconnect ==
  /\ phase = "live"
  /\ ~brokerSeen
  /\ brokerSeen' = TRUE
  /\ engineConnected' = FALSE
  /\ cause' = IF Variant = "fixed" /\ cause = "none" THEN "broker" ELSE cause
  /\ UNCHANGED <<phase, epoch, writerEpoch, lossObserved, admittedAfterLoss, writerFailed>>

\* Closing the transport makes an in-flight write fail.
WriterFails ==
  /\ ~writerFailed
  /\ brokerSeen
  /\ phase # "finalized"
  /\ writerFailed' = TRUE
  /\ cause' = IF Variant = "rc15" \/ cause = "none" THEN "writer" ELSE cause
  /\ UNCHANGED <<phase, epoch, writerEpoch, engineConnected, lossObserved,
                 admittedAfterLoss, brokerSeen>>

\* The reader observes EOF and starts teardown.
ObserveLoss ==
  /\ phase = "live"
  /\ ~lossObserved
  /\ lossObserved' = TRUE
  /\ IF Variant = "fixed"
     THEN /\ phase' = "retired"
          /\ epoch' = 1 /\ writerEpoch' = 1
          /\ engineConnected' = FALSE
     ELSE /\ phase' = "halfRetired"
          /\ epoch' = 1 /\ writerEpoch' = 1
          /\ UNCHANGED engineConnected
  /\ UNCHANGED <<admittedAfterLoss, brokerSeen, writerFailed, cause>>

\* rc15 only: teardown resumes after its await and retires the engine.
ResumeRetirement ==
  /\ phase = "halfRetired"
  /\ phase' = "retired"
  /\ engineConnected' = FALSE
  /\ UNCHANGED <<epoch, writerEpoch, lossObserved, admittedAfterLoss,
                 brokerSeen, writerFailed, cause>>

Finalize ==
  /\ phase = "retired"
  /\ phase' = "finalized"
  /\ cause' = IF cause # "none" THEN cause
              ELSE IF brokerSeen THEN "broker" ELSE "synthetic"
  /\ UNCHANGED <<epoch, writerEpoch, engineConnected, lossObserved,
                 admittedAfterLoss, brokerSeen, writerFailed>>

Quiescent == phase = "finalized" /\ UNCHANGED vars

Next ==
  \/ Admit \/ BrokerDisconnect \/ WriterFails
  \/ ObserveLoss \/ ResumeRetirement \/ Finalize
  \/ Quiescent

Spec == Init /\ [][Next]_vars

NoAdmissionAfterLoss == ~admittedAfterLoss

BrokerReasonKept == (phase = "finalized" /\ brokerSeen) => cause = "broker"

=============================================================================
