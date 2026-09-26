---- MODULE SendQuotaOwnership ----
EXTENDS Naturals, FiniteSets

(***************************************************************************
Send-quota ownership after a resumed session (#545).

Receive Maximum = 1. Four exchanges were unacknowledged on the previous
connection: 1 WAIT_PUBACK, 2 WAIT_PUBCOMP, 3 and 4 WAIT_PUBREC. Replay
(OutboundSession._replay_message, in store order) resends exchange 1's
PUBLISH, which takes the only slot; resends exchange 2's PUBREL, which needs
no slot; and parks 3 and 4. Then the broker may answer any live exchange in
any order:
- PUBACK, PUBCOMP or a negative PUBREC ends it;
- a successful PUBREC advances it to WAIT_PUBCOMP and unparks it.
drain() resends the head parked PUBLISH whenever FlowControl admits one.
A final teardown seals the failed publications (#521) and ends the
connection.

Variant = "rc15": a slot is released on every terminal ACK.
Variant = "rc16": a counter. _settle() skips the release for a sealed,
  parked or `_slotless` exchange; a PUBREC that unparks an exchange marks it
  `_slotless`; seal() releases unless the exchange is parked or slotless.
Variant = "owner": FlowControl records each slot's owning packet
  identifier; every release names its owner and is a no-op for a non-owner.

Invariants:
- PUBLISHes sent on this connection and not yet acknowledged never exceed
  Receive Maximum [MQTT-3.3.4-7].
- The slots in use are exactly those of the exchanges that sent their
  PUBLISH on this connection and are neither finished nor sealed. For the
  owner variant the owners are that exact set, which the engine fuzz oracle
  also checks (tests/fuzz/test_stateful_invariants.py).
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"rc15", "rc16", "owner"}

Mids == {1, 2, 3, 4}
Limit == 1

VARIABLES phase, sentHere, parked, sealed, closed, inflight, slotless, holders

vars == <<phase, sentHere, parked, sealed, closed, inflight, slotless, holders>>

Phases == {"ack", "rec", "comp", "done"}

TypeOK ==
  /\ phase \in [Mids -> Phases]
  /\ sentHere \subseteq Mids
  /\ parked \subseteq Mids
  /\ sealed \subseteq Mids
  /\ closed \in BOOLEAN
  /\ inflight \in 0..4
  /\ slotless \subseteq Mids
  /\ holders \subseteq Mids

Init ==
  /\ phase = [m \in Mids |-> CASE m = 1 -> "ack"
                               [] m = 2 -> "comp"
                               [] OTHER -> "rec"]
  /\ sentHere = {1}
  /\ parked = {3, 4}
  /\ sealed = {}
  /\ closed = FALSE
  /\ inflight = 1
  /\ slotless = IF Variant = "rc16" THEN {2} ELSE {}
  /\ holders = IF Variant = "owner" THEN {1} ELSE {}

Done == {m \in Mids : phase[m] = "done"}

Used == IF Variant = "owner" THEN Cardinality(holders) ELSE inflight

\* Counter release, clamped at zero as FlowControl.release() was.
Dec(n) == IF n > 0 THEN n - 1 ELSE n

\* The terminal acknowledgement of m: OutboundSession._finish / _settle.
Finish(m) ==
  /\ phase' = [phase EXCEPT ![m] = "done"]
  /\ parked' = parked \ {m}
  /\ sealed' = sealed \ {m}
  /\ slotless' = slotless \ {m}
  /\ inflight' =
       CASE Variant = "rc15" -> Dec(inflight)
         [] Variant = "rc16" ->
              IF m \in sealed \/ m \in parked \/ m \in slotless
              THEN inflight ELSE Dec(inflight)
         [] OTHER -> inflight
  /\ holders' = holders \ {m}
  /\ UNCHANGED <<sentHere, closed>>

PubAck(m) ==
  /\ ~closed
  /\ phase[m] = "ack"
  /\ Finish(m)

PubRecFailure(m) ==
  /\ ~closed
  /\ phase[m] = "rec"
  /\ Finish(m)

PubComp(m) ==
  /\ ~closed
  /\ phase[m] = "comp"
  /\ Finish(m)

PubRecSuccess(m) ==
  /\ ~closed
  /\ phase[m] = "rec"
  /\ phase' = [phase EXCEPT ![m] = "comp"]
  /\ parked' = parked \ {m}
  /\ slotless' = IF Variant = "rc16" /\ m \in parked THEN slotless \cup {m} ELSE slotless
  /\ UNCHANGED <<sentHere, sealed, closed, inflight, holders>>

Head(S) == CHOOSE m \in S : \A n \in S : m <= n

Drain ==
  /\ ~closed
  /\ parked # {}
  /\ Used < Limit
  /\ LET m == Head(parked) IN
       /\ parked' = parked \ {m}
       /\ sentHere' = sentHere \cup {m}
       /\ inflight' = inflight + 1
       /\ holders' = IF Variant = "owner" THEN holders \cup {m} ELSE holders
  /\ UNCHANGED <<phase, sealed, closed, slotless>>

\* Final teardown fails the receipts of `s` and seals them (#521); the
\* connection is gone. RC15 had no sealing.
Seal(s) ==
  /\ Variant # "rc15"
  /\ ~closed
  /\ s # {}
  /\ s \subseteq {m \in Mids : phase[m] # "done"}
  /\ sealed' = s
  /\ parked' = parked \ s
  /\ slotless' = slotless \ s
  /\ inflight' =
       IF Variant = "rc16"
       THEN inflight - Cardinality({m \in s : m \notin parked /\ m \notin slotless})
       ELSE inflight
  /\ holders' = holders \ s
  /\ closed' = TRUE
  /\ UNCHANGED <<phase, sentHere>>

Terminal == closed \/ Done = Mids

Quiescent == Terminal /\ UNCHANGED vars

Next ==
  \/ \E m \in Mids : PubAck(m) \/ PubRecFailure(m) \/ PubRecSuccess(m) \/ PubComp(m)
  \/ Drain
  \/ \E s \in SUBSET Mids : Seal(s)
  \/ Quiescent

Spec == Init /\ [][Next]_vars

WithinReceiveMaximum == Cardinality(sentHere \ Done) <= Limit

Owed == sentHere \ (Done \cup sealed)

SlotsMatchOwners ==
  /\ Used = Cardinality(Owed)
  /\ Variant = "owner" => holders = Owed

=============================================================================
