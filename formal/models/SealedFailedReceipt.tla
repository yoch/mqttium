---- MODULE SealedFailedReceipt ----

(***************************************************************************
A client never sends a publication whose receipt it already failed (#521).

One QoS 1 publication, QUEUED offline or already sent (WAIT_PUBACK). The
client ends its connection terminally (refused CONNACK, final loss,
disconnect()): AsyncClient._fail_pending fails the receipt. The same client
may connect again, with or without the broker session. Another client
(a restart) may recover the durable row from the store.

Variant = "rc15": the row stays replayable by the same client.
Variant = "fixed": OutboundSession.seal() keeps the row for recovery but the
failing client never launches or replays it, and keeps its identifier.
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"rc15", "fixed"}

VARIABLES receipt, row, sentAfterFail, recovered

vars == <<receipt, row, sentAfterFail, recovered>>

TypeOK ==
  /\ receipt \in {"live", "failed"}
  /\ row \in {"queued", "sent", "none"}
  /\ sentAfterFail \in BOOLEAN
  /\ recovered \in BOOLEAN

Init ==
  /\ receipt = "live"
  /\ row \in {"queued", "sent"}
  /\ sentAfterFail = FALSE
  /\ recovered = FALSE

TerminalFailure ==
  /\ receipt = "live"
  /\ receipt' = "failed"
  /\ UNCHANGED <<row, sentAfterFail, recovered>>

\* The same client connects again and drains or replays its store.
SameClientReconnects ==
  /\ receipt = "failed"
  /\ row # "none"
  /\ ~sentAfterFail
  /\ sentAfterFail' = (Variant = "rc15")
  /\ UNCHANGED <<receipt, row, recovered>>

\* A new client or process recovers the durable row.
AnotherClientRecovers ==
  /\ receipt = "failed"
  /\ row # "none"
  /\ ~recovered
  /\ recovered' = TRUE
  /\ UNCHANGED <<receipt, row, sentAfterFail>>

Terminal == receipt = "failed" /\ recovered

Quiescent == Terminal /\ UNCHANGED vars

Next == TerminalFailure \/ SameClientReconnects \/ AnotherClientRecovers \/ Quiescent

Spec == Init /\ [][Next]_vars

\* The client that reported the failure never sends the publication.
NoSendAfterFailure == ~sentAfterFail

\* The durable row is still there for recovery (deletion would break it).
RecoverableAfterFailure == receipt = "failed" => row # "none"

=============================================================================
