---- MODULE WritePumpEagerFailure ----
EXTENDS Naturals, Sequences

(***************************************************************************
A producer-side eager write that raises (#504).

Refines WritePump in api/_writer.py:
- try_enqueue / try_enqueue_ack admit only when no failure is latched and
  the caller's epoch is current (else StaleConnectionEffect);
- the eager path (_try_write_data_eager / _try_write_ack_eager) writes
  straight to the transport only when armed, idle, with no waiter and an
  empty queue; a success disarms it until the next turn;
- write_nowait() may raise after exposing any prefix of the frame, so the
  frame can never be written again. _retain_failed_eager() latches the
  failure, drops the eager binding, advances the epoch and queues the frame
  as an ownership-only marker;
- the writer task (_run) re-arms the eager path at its idle wait, takes the
  queued batch, and raises the latched failure before writing any of it.

Variant = "rc15": the exception returns to the producer; nothing is latched,
  the binding stays armed and the epoch stays current.
Variant = "fixed": _retain_failed_eager as above.

Invariants:
- NothingAdmittedAfterFailure: after the eager failure no frame is admitted
  to that connection, eagerly or through the queue.
- MarkerNeverWritten: the ambiguous frame never reaches the wire again.
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"rc15", "fixed"}

VARIABLES
  epoch,       \* WritePump.epoch
  latched,     \* _latency_failure is not None
  armed,       \* _eager_armed
  queue,       \* queued items: "frame" or "marker"
  writer,      \* "idle", "writing", "dead"
  batch,       \* the writer's current batch
  failed,      \* an eager write raised on this connection
  produced,    \* producer attempts (bound)
  admittedAfter,
  markerWritten

vars == <<epoch, latched, armed, queue, writer, batch, failed, produced,
          admittedAfter, markerWritten>>

Init ==
  /\ epoch = 0
  /\ latched = FALSE
  /\ armed = TRUE
  /\ queue = <<>>
  /\ writer = "idle"
  /\ batch = <<>>
  /\ failed = FALSE
  /\ produced = 0
  /\ admittedAfter = FALSE
  /\ markerWritten = FALSE

\* The producer captured epoch 0 when its effect was produced.
Admissible == ~latched /\ epoch = 0

EagerPossible == armed /\ writer = "idle" /\ queue = <<>>

EagerSuccess ==
  /\ produced < 3
  /\ Admissible
  /\ EagerPossible
  /\ produced' = produced + 1
  /\ armed' = FALSE
  /\ admittedAfter' = (admittedAfter \/ failed)
  /\ UNCHANGED <<epoch, latched, queue, writer, batch, failed, markerWritten>>

EagerRaises ==
  /\ produced < 3
  /\ ~failed
  /\ Admissible
  /\ EagerPossible
  /\ produced' = produced + 1
  /\ failed' = TRUE
  /\ IF Variant = "fixed"
     THEN /\ latched' = TRUE
          /\ armed' = FALSE
          /\ epoch' = epoch + 1
          /\ queue' = Append(queue, "marker")
     ELSE UNCHANGED <<latched, armed, epoch, queue>>
  /\ UNCHANGED <<writer, batch, admittedAfter, markerWritten>>

Enqueue ==
  /\ produced < 3
  /\ Admissible
  /\ ~EagerPossible
  /\ produced' = produced + 1
  /\ queue' = Append(queue, "frame")
  /\ admittedAfter' = (admittedAfter \/ failed)
  /\ UNCHANGED <<epoch, latched, armed, writer, batch, failed, markerWritten>>

\* _run's idle wait re-arms the eager path (the binding is still present).
WriterRearm ==
  /\ writer = "idle"
  /\ queue = <<>>
  /\ ~armed
  /\ armed' = TRUE
  /\ UNCHANGED <<epoch, latched, queue, writer, batch, failed, produced,
                 admittedAfter, markerWritten>>

WriterTake ==
  /\ writer = "idle"
  /\ queue # <<>>
  /\ writer' = "writing"
  /\ batch' = queue
  /\ queue' = <<>>
  /\ armed' = FALSE
  /\ UNCHANGED <<epoch, latched, failed, produced, admittedAfter, markerWritten>>

Contains(s, x) == \E i \in 1..Len(s) : s[i] = x

WriterWrite ==
  /\ writer = "writing"
  /\ IF latched
     THEN /\ writer' = "dead"
          /\ UNCHANGED markerWritten
     ELSE /\ writer' = "idle"
          /\ markerWritten' = (markerWritten \/ Contains(batch, "marker"))
  /\ batch' = <<>>
  /\ UNCHANGED <<epoch, latched, armed, queue, failed, produced, admittedAfter>>

Terminal == produced = 3 \/ writer = "dead" \/ (latched /\ writer = "idle" /\ queue = <<>>)

Quiescent == Terminal /\ UNCHANGED vars

Next ==
  \/ EagerSuccess \/ EagerRaises \/ Enqueue
  \/ WriterRearm \/ WriterTake \/ WriterWrite
  \/ Quiescent

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ epoch \in 0..1
  /\ writer \in {"idle", "writing", "dead"}
  /\ produced \in 0..3

NothingAdmittedAfterFailure == ~admittedAfter

MarkerNeverWritten == ~markerWritten

=============================================================================
