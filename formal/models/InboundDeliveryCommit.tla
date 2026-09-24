---- MODULE InboundDeliveryCommit ----
EXTENDS Naturals, FiniteSets

(***************************************************************************
Inbound QoS 2 delivery commit and exchange identity.

An exchange is a (packet identifier, incarnation) pair: the broker may reuse
an identifier once the previous exchange completed. The engine emits a
MESSAGE effect; the runtime's reader-owned lane later commits it to the
application (callback run, or iterator queue accepted). A connection loss
discards the lane; the durable row survives and is replayed if it is not
marked delivered.

Variant = "rc15":
  - PUBREL completes the exchange (PUBCOMP, row deleted) immediately;
  - the delivery mark is applied later, keyed by identifier only, and is
    skipped when the connection epoch changed in between.
Variant = "fixed":
  - PUBREL of an undelivered row waits; the mark completes it;
  - the mark is applied atomically with the commit and carries the exchange
    token, so it is ignored once the exchange is gone.

Invariants (issues):
  CompletedOnlyAfterOwnership  #520 #519  PUBCOMP never precedes ownership
  MarkBelongsToItsExchange     #534       a mark never flags a later exchange
  OwnedMessagesStayMarked      #517       a committed delivery keeps its mark
***************************************************************************)

CONSTANTS Mids, MaxIncarnation, Variant
ASSUME Variant \in {"rc15", "fixed"}

Exchanges == Mids \X (1..MaxIncarnation)

VARIABLES
  row,        \* [Mids -> {"none", "waitRel", "released"}]
  inc,        \* current incarnation of each identifier
  delivered,  \* durable "delivered" flag of the current row
  lane,       \* emitted, not yet committed deliveries
  marks,      \* committed deliveries whose mark is not applied yet (rc15)
  owned,      \* exchanges the application owns
  completed   \* exchanges whose PUBCOMP was sent

vars == <<row, inc, delivered, lane, marks, owned, completed>>

TypeOK ==
  /\ row \in [Mids -> {"none", "waitRel", "released"}]
  /\ inc \in [Mids -> 0..MaxIncarnation]
  /\ delivered \in [Mids -> BOOLEAN]
  /\ lane \subseteq Exchanges
  /\ marks \subseteq Exchanges
  /\ owned \subseteq Exchanges
  /\ completed \subseteq Exchanges

Init ==
  /\ row = [m \in Mids |-> "none"]
  /\ inc = [m \in Mids |-> 0]
  /\ delivered = [m \in Mids |-> FALSE]
  /\ lane = {}
  /\ marks = {}
  /\ owned = {}
  /\ completed = {}

Current(m) == <<m, inc[m]>>

\* PUBCOMP, row deletion and release of the identifier.
Complete(m) ==
  /\ row' = [row EXCEPT ![m] = "none"]
  /\ completed' = completed \cup {Current(m)}

Publish(m) ==
  /\ row[m] = "none"
  /\ inc[m] < MaxIncarnation
  /\ inc' = [inc EXCEPT ![m] = inc[m] + 1]
  /\ row' = [row EXCEPT ![m] = "waitRel"]
  /\ delivered' = [delivered EXCEPT ![m] = FALSE]
  /\ lane' = lane \cup {<<m, inc[m] + 1>>}
  /\ UNCHANGED <<marks, owned, completed>>

Pubrel(m) ==
  /\ row[m] = "waitRel"
  /\ IF Variant = "rc15" \/ delivered[m]
     THEN Complete(m)
     ELSE /\ row' = [row EXCEPT ![m] = "released"]
          /\ UNCHANGED completed
  /\ UNCHANGED <<inc, delivered, lane, marks, owned>>

\* Mark application. rc15 keys it by identifier; fixed checks the exchange.
ApplyMark(e, r, c) ==
  LET m == e[1] IN
  IF row[m] = "none" \/ (Variant = "fixed" /\ inc[m] # e[2])
  THEN /\ delivered' = delivered /\ row' = r /\ completed' = c
  ELSE /\ delivered' = [delivered EXCEPT ![m] = TRUE]
       /\ IF Variant = "fixed" /\ row[m] = "released"
          THEN Complete(m)
          ELSE row' = r /\ completed' = c

Commit(e) ==
  /\ e \in lane
  /\ lane' = lane \ {e}
  /\ owned' = owned \cup {e}
  /\ IF Variant = "fixed"
     THEN /\ ApplyMark(e, row, completed) /\ UNCHANGED marks
     ELSE /\ marks' = marks \cup {e} /\ UNCHANGED <<row, delivered, completed>>
  /\ UNCHANGED inc

LateMark(e) ==
  /\ Variant = "rc15"
  /\ e \in marks
  /\ marks' = marks \ {e}
  /\ ApplyMark(e, row, completed)
  /\ UNCHANGED <<inc, lane, owned>>

\* Connection loss: the lane is discarded, rc15 drops marks fenced by the
\* old epoch, and a waiting completion returns to WAIT_PUBREL (the broker
\* resends PUBREL on the next connection).
Drop ==
  /\ lane' = {}
  /\ marks' = IF Variant = "rc15" THEN {} ELSE marks
  /\ row' = [m \in Mids |-> IF row[m] = "released" THEN "waitRel" ELSE row[m]]
  /\ UNCHANGED <<inc, delivered, owned, completed>>

\* Session resume replays an undelivered row.
Replay(m) ==
  /\ row[m] # "none"
  /\ ~delivered[m]
  /\ Current(m) \notin lane
  /\ lane' = lane \cup {Current(m)}
  /\ UNCHANGED <<row, inc, delivered, marks, owned, completed>>

Terminal ==
  /\ \A m \in Mids: row[m] = "none" /\ inc[m] = MaxIncarnation
  /\ lane = {} /\ marks = {}

Quiescent == Terminal /\ UNCHANGED vars

Next ==
  \/ \E m \in Mids: Publish(m) \/ Pubrel(m) \/ Replay(m)
  \/ \E e \in Exchanges: Commit(e) \/ LateMark(e)
  \/ Drop
  \/ Quiescent

Spec == Init /\ [][Next]_vars

CompletedOnlyAfterOwnership == completed \subseteq owned

MarkBelongsToItsExchange ==
  \A m \in Mids: (row[m] # "none" /\ delivered[m]) => Current(m) \in owned

OwnedMessagesStayMarked ==
  \A m \in Mids:
    (row[m] # "none" /\ Current(m) \in owned /\ Current(m) \notin marks) => delivered[m]

=============================================================================
