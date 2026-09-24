---- MODULE OutboundQoS2Phases ----
EXTENDS Naturals

(***************************************************************************
QoS 2 sender phases under successful PUBREC (#503, #497).

One outbound QoS 2 exchange, resumed in WAIT_PUBREC. Parked = TRUE when the
replay found no send-quota slot and left it in OutboundSession._queued. The
peer may send successful PUBRECs (at most MaxPubrec), and another exchange's
PUBACK can later free a slot, letting drain() process the queue head.

Every successful PUBREC requires a PUBREL [MQTT-4.3.3-4]. A PUBREL must never
be produced by a stale queue entry for an exchange that already answered it.

Variant = "rc15": OutboundSession.on_pubrec sends PUBREL only when the
conditional WAIT_PUBREC -> WAIT_PUBCOMP transition succeeds, and leaves a
parked entry in the queue.
Variant = "fixed": a PUBREC in WAIT_PUBCOMP resends PUBREL; the transition
unparks the entry.
***************************************************************************)

CONSTANTS Variant, Parked, MaxPubrec
ASSUME Variant \in {"rc15", "fixed"}
ASSUME Parked \in BOOLEAN
ASSUME MaxPubrec \in {1, 2, 3}

VARIABLES phase, queued, pubrecs, unanswered, stalePubrel, slotSpent

vars == <<phase, queued, pubrecs, unanswered, stalePubrel, slotSpent>>

TypeOK ==
  /\ phase \in {"WAIT_PUBREC", "WAIT_PUBCOMP"}
  /\ queued \in BOOLEAN
  /\ pubrecs \in 0..MaxPubrec
  /\ unanswered \in 0..MaxPubrec
  /\ stalePubrel \in BOOLEAN
  /\ slotSpent \in BOOLEAN

Init ==
  /\ phase = "WAIT_PUBREC"
  /\ queued = Parked
  /\ pubrecs = 0
  /\ unanswered = 0
  /\ stalePubrel = FALSE
  /\ slotSpent = FALSE

\* Successful PUBREC from the peer.
Pubrec ==
  /\ pubrecs < MaxPubrec
  /\ pubrecs' = pubrecs + 1
  /\ IF phase = "WAIT_PUBREC"
     THEN /\ phase' = "WAIT_PUBCOMP"                  \* PUBREL sent
          /\ queued' = (queued /\ Variant = "rc15")   \* fixed: _unpark
          /\ UNCHANGED unanswered
     ELSE /\ UNCHANGED <<phase, queued>>
          /\ unanswered' = IF Variant = "rc15" THEN unanswered + 1 ELSE unanswered
  /\ UNCHANGED <<stalePubrel, slotSpent>>

\* drain() takes a freed slot and retransmits the queue head as stored.
Drain ==
  /\ queued
  /\ queued' = FALSE
  /\ slotSpent' = TRUE
  /\ stalePubrel' = (phase = "WAIT_PUBCOMP")          \* _retransmit sends PUBREL
  /\ UNCHANGED <<phase, pubrecs, unanswered>>

Terminal == pubrecs = MaxPubrec /\ ~queued

Quiescent == Terminal /\ UNCHANGED vars

Next == Pubrec \/ Drain \/ Quiescent

Spec == Init /\ [][Next]_vars

EveryPubrecAnswered == unanswered = 0

NoStalePubrel == ~stalePubrel

=============================================================================
