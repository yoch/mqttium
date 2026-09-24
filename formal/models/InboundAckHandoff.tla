---- MODULE InboundAckHandoff ----

(***************************************************************************
Inbound QoS 2 completion versus the PUBCOMP handoff (#537, #541).

Receive Maximum = 1. Exchange 1 completes (PUBREL processed, PUBCOMP emitted
into the engine batch). Before that batch is taken (take_effects, the only
point from which the PUBCOMP can reach the broker), the peer's next PUBLISH is
decoded: with a new packet identifier (Reuse = FALSE, #537) or the same one
(Reuse = TRUE, #541). A peer that sent it before receiving PUBCOMP violates
[MQTT-3.3.4-9] / [MQTT-2.2.1-4]; the client must refuse it, and must admit it
once the handoff has happened.

Variant = "rc15": InboundSession._complete_pubrel releases the slot and forgets
the identifier immediately.
Variant = "fixed": _hold_until_pubcomp_handoff keeps both until take_effects().
***************************************************************************)

CONSTANTS Variant, Reuse
ASSUME Variant \in {"rc15", "fixed"}
ASSUME Reuse \in BOOLEAN

VARIABLES slotHeld, midHeld, handedOff, next, decodedAfter

vars == <<slotHeld, midHeld, handedOff, next, decodedAfter>>

TypeOK ==
  /\ slotHeld \in BOOLEAN
  /\ midHeld \in BOOLEAN
  /\ handedOff \in BOOLEAN
  /\ next \in {"none", "accepted", "refused"}
  /\ decodedAfter \in BOOLEAN

\* Exchange 1 has just completed: PUBCOMP(1) is in the engine batch.
Init ==
  /\ slotHeld = (Variant = "fixed")
  /\ midHeld = (Variant = "fixed")
  /\ handedOff = FALSE
  /\ next = "none"
  /\ decodedAfter = FALSE

TakeEffects ==
  /\ ~handedOff
  /\ handedOff' = TRUE
  /\ slotHeld' = FALSE
  /\ midHeld' = FALSE
  /\ UNCHANGED <<next, decodedAfter>>

\* The next PUBLISH is decoded; refusal ends the connection.
NextPublish ==
  /\ next = "none"
  /\ next' = IF (Reuse /\ midHeld) \/ (~Reuse /\ slotHeld) THEN "refused" ELSE "accepted"
  /\ decodedAfter' = handedOff
  /\ UNCHANGED <<slotHeld, midHeld, handedOff>>

Terminal == next # "none" /\ (handedOff \/ next = "refused")

Quiescent == Terminal /\ UNCHANGED vars

Next == TakeEffects \/ NextPublish \/ Quiescent

Spec == Init /\ [][Next]_vars

\* A PUBLISH decoded before PUBCOMP could leave is never admitted...
NoAdmissionBeforeHandoff == next = "accepted" => decodedAfter

\* ...and one decoded after the handoff always is.
AdmittedAfterHandoff == next = "refused" => ~decodedAfter

=============================================================================
