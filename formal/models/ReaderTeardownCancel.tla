---- MODULE ReaderTeardownCancel ----

(***************************************************************************
A cancellation that reaches the reader while it releases a lost connection.

After a loss the reconnect policy will retry, the reader records the decision
(_reconnect_pending), keeps the receipts for replay, and then awaits the
release of the writer and the transport. Only after that release does it
start the reconnect task. Three parties can cancel it during the release:

  internal    a failing writer, keepalive, effect or auth task
              (_close_transport_after_connection_failure);
  disconnect  a terminal disconnect(), which first clears the reconnect
              decision and marks the shutdown intentional;
  external    code outside the client, at event-loop shutdown.

Variant = "v121": the cancellation ends the reader at the awaited release.
Nothing after it runs: no reconnect task, no receipt settlement.
Variant = "fixed": the release reports the interruption and the reader still
completes its decision (AsyncClient._finish_reader_retirement).
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"v121", "fixed"}

VARIABLES reader, pending, task, receipts, intentional, cancelled

vars == <<reader, pending, task, receipts, intentional, cancelled>>

TypeOK ==
  /\ reader \in {"releasing", "finished"}
  /\ pending \in BOOLEAN
  /\ task \in BOOLEAN
  /\ receipts \in {"kept", "failed"}
  /\ intentional \in BOOLEAN
  /\ cancelled \in {"none", "internal", "disconnect", "external"}

\* The reader has decided to reconnect and is about to release its resources.
Init ==
  /\ reader = "releasing"
  /\ pending = TRUE
  /\ task = FALSE
  /\ receipts = "kept"
  /\ intentional = FALSE
  /\ cancelled = "none"

InternalFailure ==
  /\ reader = "releasing" /\ cancelled = "none"
  /\ cancelled' = "internal"
  /\ UNCHANGED <<reader, pending, task, receipts, intentional>>

Disconnect ==
  /\ reader = "releasing" /\ cancelled = "none"
  /\ cancelled' = "disconnect"
  /\ intentional' = TRUE
  /\ pending' = FALSE
  /\ UNCHANGED <<reader, task, receipts>>

ExternalCancel ==
  /\ reader = "releasing" /\ cancelled = "none"
  /\ cancelled' = "external"
  /\ UNCHANGED <<reader, pending, task, receipts, intentional>>

\* What the client still wants once the release has ended.
StillReconnects == ~intentional /\ cancelled # "external"

Complete ==
  /\ reader = "releasing"
  /\ reader' = "finished"
  /\ IF Variant = "v121" /\ cancelled # "none"
       THEN UNCHANGED <<pending, task, receipts>>
       ELSE IF StillReconnects
         THEN /\ task' = TRUE
              /\ UNCHANGED <<pending, receipts>>
         ELSE /\ pending' = FALSE
              /\ receipts' = "failed"
              /\ UNCHANGED task
  /\ UNCHANGED <<intentional, cancelled>>

Terminal == reader = "finished"

Quiescent == Terminal /\ UNCHANGED vars

Next == InternalFailure \/ Disconnect \/ ExternalCancel \/ Complete \/ Quiescent

Spec == Init /\ [][Next]_vars

\* RECONNECTING always has something reconnecting it.
ReconnectingHasOwner == (reader = "finished" /\ pending) => task

\* A client that will not reconnect leaves no receipt waiting for a replay.
TerminalSettlesReceipts == (reader = "finished" /\ ~pending) => receipts = "failed"

=============================================================================
