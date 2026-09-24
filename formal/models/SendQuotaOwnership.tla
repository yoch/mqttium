---- MODULE SendQuotaOwnership ----
EXTENDS Naturals, FiniteSets

(***************************************************************************
Send-quota ownership after a resumed session (#545).

Receive Maximum = 1. Exchanges 1, 2, 3 were unacknowledged on the previous
connection. Replay resends exchange 1's PUBLISH (it takes the only slot) and
parks 2 and 3. The broker may settle any live exchange; drain() then sends
the next parked PUBLISH while FlowControl admits one.

Variant = "rc15": OutboundSession releases a slot on every terminal ACK.
Variant = "fixed": _settle() reports whether the exchange held a slot on
this connection; only those release one.
Invariant: PUBLISHes sent on this connection and not yet acknowledged never
exceed Receive Maximum [MQTT-3.3.4-7].
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"rc15", "fixed"}

Mids == {1, 2, 3}

VARIABLES sentHere, parked, settled, inflight

vars == <<sentHere, parked, settled, inflight>>

TypeOK ==
  /\ sentHere \subseteq Mids
  /\ parked \subseteq Mids
  /\ settled \subseteq Mids
  /\ inflight \in 0..3

Init ==
  /\ sentHere = {1}
  /\ parked = {2, 3}
  /\ settled = {}
  /\ inflight = 1

Settle(m) ==
  /\ m \notin settled
  /\ settled' = settled \cup {m}
  /\ parked' = parked \ {m}
  /\ inflight' = IF (Variant = "rc15" \/ m \in sentHere) /\ inflight > 0
                 THEN inflight - 1 ELSE inflight
  /\ UNCHANGED sentHere

Drain ==
  /\ parked # {}
  /\ inflight < 1
  /\ \E m \in parked:
       /\ parked' = parked \ {m}
       /\ sentHere' = sentHere \cup {m}
  /\ inflight' = inflight + 1
  /\ UNCHANGED settled

Terminal == settled = Mids

Quiescent == Terminal /\ UNCHANGED vars

Next == (\E m \in Mids: Settle(m)) \/ Drain \/ Quiescent

Spec == Init /\ [][Next]_vars

WithinReceiveMaximum == Cardinality(sentHere \ settled) <= 1

=============================================================================
