---- MODULE WritePumpEagerFailure ----
EXTENDS Naturals

(***************************************************************************
WritePump producer-side eager failure ownership model for mqttium issue #504.

write_nowait() is allowed to be ambiguous: it may raise after exposing any
prefix of the supplied bytes. Therefore the failed frame may be retained only
as an ownership/accounting record. It must never be retried on the wire.

FenceEagerFailure=FALSE models rc15: the exception returns to the producer while
the writer generation and eager transport binding remain usable.

FenceEagerFailure=TRUE models the candidate:
- latch failure,
- drop eager transport binding,
- invalidate writer generation,
- retain one ownership-only marker for the existing writer task,
- let the writer report failure without attempting the marker on the wire.
***************************************************************************)

CONSTANT FenceEagerFailure
ASSUME FenceEagerFailure \in BOOLEAN

VARIABLES
  protocolCommitted,
  producerFailed,
  generationValid,
  eagerBound,
  markerOwned,
  writerAlive,
  failureReported,
  connectionLive,
  receiptRegistered,
  wireRetry

vars == <<
  protocolCommitted,
  producerFailed,
  generationValid,
  eagerBound,
  markerOwned,
  writerAlive,
  failureReported,
  connectionLive,
  receiptRegistered,
  wireRetry
>>

Init ==
  /\ protocolCommitted = FALSE
  /\ producerFailed = FALSE
  /\ generationValid = TRUE
  /\ eagerBound = TRUE
  /\ markerOwned = FALSE
  /\ writerAlive = TRUE
  /\ failureReported = FALSE
  /\ connectionLive = TRUE
  /\ receiptRegistered = FALSE
  /\ wireRetry = FALSE

TypeOK ==
  /\ protocolCommitted \in BOOLEAN
  /\ producerFailed \in BOOLEAN
  /\ generationValid \in BOOLEAN
  /\ eagerBound \in BOOLEAN
  /\ markerOwned \in BOOLEAN
  /\ writerAlive \in BOOLEAN
  /\ failureReported \in BOOLEAN
  /\ connectionLive \in BOOLEAN
  /\ receiptRegistered \in BOOLEAN
  /\ wireRetry \in BOOLEAN

CommitQoS ==
  /\ connectionLive
  /\ ~protocolCommitted
  /\ protocolCommitted' = TRUE
  /\ receiptRegistered' = TRUE
  /\ UNCHANGED <<
       producerFailed, generationValid, eagerBound, markerOwned,
       writerAlive, failureReported, connectionLive, wireRetry
     >>

EagerFailRc15 ==
  /\ ~FenceEagerFailure
  /\ connectionLive
  /\ eagerBound
  /\ ~producerFailed
  /\ producerFailed' = TRUE
  /\ UNCHANGED <<
       protocolCommitted, generationValid, eagerBound, markerOwned,
       writerAlive, failureReported, connectionLive, receiptRegistered,
       wireRetry
     >>

EagerFailCandidate ==
  /\ FenceEagerFailure
  /\ connectionLive
  /\ eagerBound
  /\ writerAlive
  /\ ~producerFailed
  /\ producerFailed' = TRUE
  /\ generationValid' = FALSE
  /\ eagerBound' = FALSE
  /\ markerOwned' = TRUE
  /\ UNCHANGED <<
       protocolCommitted, writerAlive, failureReported, connectionLive,
       receiptRegistered, wireRetry
     >>

AdmitAfterFailedEager ==
  /\ producerFailed
  /\ generationValid
  /\ eagerBound
  /\ connectionLive
  /\ wireRetry' = TRUE
  /\ UNCHANGED <<
       protocolCommitted, producerFailed, generationValid, eagerBound,
       markerOwned, writerAlive, failureReported, connectionLive,
       receiptRegistered
     >>

WriterRetireMarker ==
  /\ markerOwned
  /\ writerAlive
  /\ markerOwned' = FALSE
  /\ writerAlive' = FALSE
  /\ failureReported' = TRUE
  /\ UNCHANGED <<
       protocolCommitted, producerFailed, generationValid, eagerBound,
       connectionLive, receiptRegistered, wireRetry
     >>

SettleFailure ==
  /\ failureReported
  /\ connectionLive' = FALSE
  /\ receiptRegistered' = FALSE
  /\ UNCHANGED <<
       protocolCommitted, producerFailed, generationValid, eagerBound,
       markerOwned, writerAlive, failureReported, wireRetry
     >>

Next ==
  \/ CommitQoS
  \/ EagerFailRc15
  \/ EagerFailCandidate
  \/ AdmitAfterFailedEager
  \/ WriterRetireMarker
  \/ SettleFailure

Spec == Init /\ [][Next]_vars

EagerFailureRetiresGeneration ==
  ~(producerFailed /\ (generationValid \/ eagerBound \/ wireRetry))

OwnershipMarkerNeverRetries ==
  ~(markerOwned /\ wireRetry)

=============================================================================
