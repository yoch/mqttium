---- MODULE PacketIdOwnership ----

(***************************************************************************
One packet identifier pool is shared by publications and SUBSCRIBE /
UNSUBSCRIBE requests. A publication is live (its row is unacknowledged),
sealed (its receipt failed, #521: the row stays for recovery and keeps its
identifier, but no longer counts as unacknowledged), or settled (its row is
deleted and its identifier released).

When the transport closes, the engine releases the identifiers of pending
SUBSCRIBE/UNSUBSCRIBE requests (ProtocolEngine._release_pending_subscription
_requests). As a constant-time fast path it resets the whole pool when no
publication owns an identifier.

Variant = "rc16": the fast path tests OutboundSession.unacknowledged_messages
= 0, which ignores sealed rows, so it frees their identifiers too.
Variant = "fixed": the fast path tests OutboundSession.holds_packet_ids,
which also counts sealed rows.
***************************************************************************)

EXTENDS Naturals, FiniteSets

CONSTANTS Variant, Ids
ASSUME Variant \in {"rc16", "fixed"}

VARIABLES owner, row

vars == <<owner, row>>

Owners == {"free", "live", "sealed", "sub"}

TypeOK ==
  /\ owner \in [Ids -> Owners]
  /\ row \in [Ids -> BOOLEAN]

Init ==
  /\ owner = [i \in Ids |-> "free"]
  /\ row = [i \in Ids |-> FALSE]

Live == {i \in Ids : owner[i] = "live"}
Sealed == {i \in Ids : owner[i] = "sealed"}

\* PacketIdPool.allocate() for a publication; the store keeps its row under
\* this identifier (put_out upserts, so a reused identifier overwrites).
Publish(i) ==
  /\ owner[i] = "free"
  /\ owner' = [owner EXCEPT ![i] = "live"]
  /\ row' = [row EXCEPT ![i] = TRUE]

Subscribe(i) ==
  /\ owner[i] = "free"
  /\ owner' = [owner EXCEPT ![i] = "sub"]
  /\ UNCHANGED row

\* OutboundSession.seal(): the reservation is released, the row and its
\* identifier are kept.
Seal(i) ==
  /\ owner[i] = "live"
  /\ owner' = [owner EXCEPT ![i] = "sealed"]
  /\ UNCHANGED row

\* PUBACK (for a live or a sealed row): row deleted, identifier released.
Settle(i) ==
  /\ owner[i] \in {"live", "sealed"}
  /\ owner' = [owner EXCEPT ![i] = "free"]
  /\ row' = [row EXCEPT ![i] = FALSE]

SubAck(i) ==
  /\ owner[i] = "sub"
  /\ owner' = [owner EXCEPT ![i] = "free"]
  /\ UNCHANGED row

HoldsIds ==
  IF Variant = "rc16" THEN Live # {} ELSE Live \cup Sealed # {}

TransportClosed ==
  /\ \E i \in Ids : owner[i] = "sub"
  /\ owner' =
       IF HoldsIds
         THEN [i \in Ids |-> IF owner[i] = "sub" THEN "free" ELSE owner[i]]
         ELSE [i \in Ids |-> "free"]
  /\ UNCHANGED row

Next ==
  \/ \E i \in Ids : Publish(i) \/ Subscribe(i) \/ Seal(i) \/ Settle(i) \/ SubAck(i)
  \/ TransportClosed

Spec == Init /\ [][Next]_vars

\* An identifier whose row is still in the store is never free for reuse:
\* reusing it would overwrite the row that another process may recover.
NoFreeIdWithRow == \A i \in Ids : row[i] => owner[i] # "free"

=============================================================================
