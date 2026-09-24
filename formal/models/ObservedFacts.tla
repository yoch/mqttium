---- MODULE ObservedFacts ----
EXTENDS Sequences

(***************************************************************************
Observed facts versus output blocked on writer capacity.

One reader lot yields a SEND (an automatic PUBACK) and an observed fact
(a PUBACK completion, CONNACK, SUBACK, PINGRESP or broker DISCONNECT). The
writer admits nothing more, for as long as the model runs: the peer stopped
reading, or a slow transport never drains.

Variant = "rc15": the fact is queued behind the SEND in one FIFO lane, and
the lane only advances when the writer admits the SEND. The waiter for the
fact (receipt, connect(), on_disconnect) can never finish: TLC reports the
stuck state as a deadlock (#531 #532 #536 #540 #524 #526).
Variant = "fixed": facts are applied when collected; a broker DISCONNECT also
seals the writer, which fails the parked SEND instead of leaving it waiting.
***************************************************************************)

CONSTANTS Variant, Fact
ASSUME Variant \in {"rc15", "fixed"}
ASSUME Fact \in {"completion", "brokerDisconnect"}

VARIABLES lane, collected, applied, sealed, waiterDone

vars == <<lane, collected, applied, sealed, waiterDone>>

TypeOK ==
  /\ lane \in Seq({"send", "fact"})
  /\ collected \in BOOLEAN
  /\ applied \in BOOLEAN
  /\ sealed \in BOOLEAN
  /\ waiterDone \in BOOLEAN

Init ==
  /\ lane = <<>>
  /\ collected = FALSE
  /\ applied = FALSE
  /\ sealed = FALSE
  /\ waiterDone = FALSE

Collect ==
  /\ ~collected
  /\ collected' = TRUE
  /\ IF Variant = "rc15"
     THEN /\ lane' = <<"send", "fact">>  \* SEND-first partition
          /\ UNCHANGED <<applied, sealed>>
     ELSE /\ lane' = <<"send">>
          /\ applied' = TRUE
          /\ sealed' = (Fact = "brokerDisconnect")
  /\ UNCHANGED waiterDone

\* The lane advances only past a SEND the writer admits (never: no capacity)
\* or rejects (sealed writer); a queued fact at its head is applied.
PumpFact ==
  /\ lane # <<>>
  /\ Head(lane) = "fact"
  /\ lane' = Tail(lane)
  /\ applied' = TRUE
  /\ UNCHANGED <<collected, sealed, waiterDone>>

PumpRejectSealed ==
  /\ lane # <<>>
  /\ Head(lane) = "send"
  /\ sealed
  /\ lane' = Tail(lane)
  /\ UNCHANGED <<collected, applied, sealed, waiterDone>>

WaiterFinishes ==
  /\ applied
  /\ ~waiterDone
  /\ waiterDone' = TRUE
  /\ UNCHANGED <<lane, collected, applied, sealed>>

Terminal == waiterDone

Quiescent == Terminal /\ UNCHANGED vars

Next == Collect \/ PumpFact \/ PumpRejectSealed \/ WaiterFinishes \/ Quiescent

Spec == Init /\ [][Next]_vars

=============================================================================
