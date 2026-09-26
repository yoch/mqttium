---- MODULE AdmissionRollback ----

(***************************************************************************
A QoS 1/2 admission writes the durable row, then may still fail (the
OutboundSession.queue_publish rollback path). The rollback deletes the row
and releases the packet identifier, then the caller sees the failure. The
delete itself can fail when the store is failing.

Variant = "rc16": a failed delete is swallowed and the identifier released.
The row stays stored, unsealed, with a free identifier: this client replays
it on its next resumed session although its caller saw the failure, and a
later publication can reuse the identifier and overwrite the row.
Variant = "fixed": a row that cannot be deleted is sealed (#521). It stays
for recovery by another process, keeps its identifier, and this client never
sends it.
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"rc16", "fixed"}

VARIABLES phase, row, id, sealed, sentAfterFailure

vars == <<phase, row, id, sealed, sentAfterFailure>>

TypeOK ==
  /\ phase \in {"admitting", "written", "failed"}
  /\ row \in BOOLEAN
  /\ id \in {"held", "free"}
  /\ sealed \in BOOLEAN
  /\ sentAfterFailure \in BOOLEAN

Init ==
  /\ phase = "admitting"
  /\ row = FALSE
  /\ id = "held"
  /\ sealed = FALSE
  /\ sentAfterFailure = FALSE

WriteRow ==
  /\ phase = "admitting"
  /\ phase' = "written"
  /\ row' = TRUE
  /\ UNCHANGED <<id, sealed, sentAfterFailure>>

\* A later admission step raises; rollback deletes the row successfully.
FailDeleteSucceeds ==
  /\ phase = "written"
  /\ phase' = "failed"
  /\ row' = FALSE
  /\ id' = "free"
  /\ UNCHANGED <<sealed, sentAfterFailure>>

\* A later admission step raises and the rollback delete fails too.
FailDeleteFails ==
  /\ phase = "written"
  /\ phase' = "failed"
  /\ UNCHANGED <<row, sentAfterFailure>>
  /\ IF Variant = "rc16"
       THEN /\ id' = "free" /\ UNCHANGED sealed
       ELSE /\ id' = "held" /\ sealed' = TRUE

\* The same client resumes its session and replays every stored, unsealed row.
Replay ==
  /\ phase = "failed"
  /\ row
  /\ ~sealed
  /\ ~sentAfterFailure
  /\ sentAfterFailure' = TRUE
  /\ UNCHANGED <<phase, row, id, sealed>>

Next == WriteRow \/ FailDeleteSucceeds \/ FailDeleteFails \/ Replay

Spec == Init /\ [][Next]_vars

\* The caller saw the admission fail: this client never sends the publication.
NoSendAfterFailure == ~sentAfterFailure

\* A stored row keeps its identifier, so no later publication overwrites it.
NoFreeIdWithRow == row => id = "held"

=============================================================================
