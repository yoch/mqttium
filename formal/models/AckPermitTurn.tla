---- MODULE AckPermitTurn ----
EXTENDS Naturals

(***************************************************************************
Who restores the success-ACK eager permit, and when (W2a, benchmark report
2026-10-04).

Refines WritePump in api/_writer.py and the reader in api/async_client.py:
- _try_write_ack_eager writes one ACK straight through only when the permit
  is armed, the writer is idle and its queue is empty; it then disarms the
  permit. Otherwise the ACK is queued for the coalescing writer task;
- "callback": the eager write schedules _rearm_eager_if_idle with call_soon.
  The loop runs it on the next turn, which re-arms when the writer is idle and
  the queue empty. When nothing else is ready that turn is an extra loop
  iteration;
- "owner": the reader is the only ACK producer (callback delivery, automatic
  acknowledgement, push transport). Its eager write records a deferred
  re-arm, and rearm_deferred_ack() applies the same condition just before the
  reader really suspends: before_wait in PushStreamTransport.receive(), the
  sleep(0) between ingress batches and the callback fairness yield;
- "naive": the owner design, but re-arming before every await of the reader,
  including one that does not suspend (an uncontended lock, or a receive()
  that finds bytes already waiting);
- the writer task re-arms at its idle wait in every variant.

A loop turn runs every handle that was ready when it began. The reader runs
at most one step per turn; a step handles one or two lots of one or two ACKs,
separated by an await that suspends only if Suspends.

Invariants:
- OneEagerAckPerTurn: at most one ACK eager write per loop turn, the rule
  that lets the writer coalesce ACK bursts (#254, #420).
- NoExtraTurn: no loop turn runs only the re-arm callback.
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"callback", "owner", "naive"}

VARIABLES
  turn,          \* loop iterations so far
  phase,         \* "start", "reader", "writer", "end" within a turn
  permit,        \* _ack_eager_armed
  deferred,      \* _ack_rearm_deferred
  callback,      \* a _rearm_eager_if_idle handle runs at the next turn
  runCallback,   \* that handle is ready in the current turn
  queued,        \* ACKs waiting for the writer task
  writing,       \* the writer task owns a batch this turn
  data,          \* bytes are ready for the reader this turn
  eager,         \* ACK eager writes in the current turn
  maxEager,      \* the most eager writes seen in one turn
  extraTurn      \* a turn ran only the re-arm callback

vars == <<turn, phase, permit, deferred, callback, runCallback, queued,
          writing, data, eager, maxEager, extraTurn>>

MaxTurn == 4

Init ==
  /\ turn = 0
  /\ phase = "start"
  /\ permit = TRUE
  /\ deferred = FALSE
  /\ callback = FALSE
  /\ runCallback = FALSE
  /\ queued = 0
  /\ writing = FALSE
  /\ data \in BOOLEAN
  /\ eager = 0
  /\ maxEager = 0
  /\ extraTurn = FALSE

CanEager == permit /\ ~writing /\ queued = 0

\* One ACK produced by the reader.
Ack(p, d, c, q, e) ==
  IF p /\ ~writing /\ q = 0
  THEN [permit |-> FALSE,
        deferred |-> IF Variant = "callback" THEN d ELSE TRUE,
        callback |-> IF Variant = "callback" THEN TRUE ELSE c,
        queued |-> q, eager |-> e + 1]
  ELSE [permit |-> p, deferred |-> d, callback |-> c, queued |-> q + 1, eager |-> e]

Rearm(s) ==
  IF s.deferred /\ ~writing /\ s.queued = 0
  THEN [s EXCEPT !.permit = TRUE, !.deferred = FALSE]
  ELSE [s EXCEPT !.deferred = FALSE]

\* The next-turn callback runs first among this turn's handles.
StartTurn ==
  /\ phase = "start"
  /\ turn < MaxTurn
  /\ IF runCallback /\ ~writing /\ queued = 0
     THEN permit' = TRUE
     ELSE UNCHANGED permit
  /\ extraTurn' = (extraTurn \/ (runCallback /\ ~data /\ queued = 0 /\ ~writing))
  /\ runCallback' = FALSE
  /\ phase' = "reader"
  /\ eager' = 0
  /\ UNCHANGED <<turn, deferred, callback, queued, writing, data, maxEager>>

\* One reader step: lot 1, an await, maybe lot 2, then a real suspension.
ReaderStep ==
  /\ phase = "reader"
  /\ \E n1 \in 1..2, n2 \in 0..2, suspends \in BOOLEAN :
       LET s0 == [permit |-> permit, deferred |-> deferred, callback |-> callback,
                  queued |-> queued, eager |-> eager]
           a1 == Ack(s0.permit, s0.deferred, s0.callback, s0.queued, s0.eager)
           b1 == IF n1 = 2 THEN Ack(a1.permit, a1.deferred, a1.callback, a1.queued, a1.eager)
                 ELSE a1
           \* The await between lots. Only the naive design re-arms before an
           \* await that may not suspend; a suspending await ends the step.
           m == IF Variant = "naive" THEN Rearm(b1) ELSE b1
           c2 == IF n2 >= 1 /\ ~suspends
                 THEN Ack(m.permit, m.deferred, m.callback, m.queued, m.eager) ELSE m
           d2 == IF n2 = 2 /\ ~suspends
                 THEN Ack(c2.permit, c2.deferred, c2.callback, c2.queued, c2.eager) ELSE c2
           \* The step ends with a real suspension.
           f == IF Variant = "callback" THEN d2 ELSE Rearm(d2)
       IN /\ data
          /\ permit' = f.permit
          /\ deferred' = f.deferred
          /\ callback' = f.callback
          /\ queued' = f.queued
          /\ eager' = f.eager
  /\ phase' = "writer"
  /\ UNCHANGED <<turn, runCallback, writing, data, maxEager, extraTurn>>

ReaderIdle ==
  /\ phase = "reader"
  /\ ~data
  /\ phase' = "writer"
  /\ UNCHANGED <<turn, permit, deferred, callback, runCallback, queued, writing,
                 data, eager, maxEager, extraTurn>>

\* The writer finishes its batch, or takes the queue, or re-arms when idle.
WriterStep ==
  /\ phase = "writer"
  /\ IF writing
     THEN /\ writing' = FALSE
          /\ UNCHANGED <<queued, permit>>
     ELSE IF queued > 0
          THEN /\ writing' = TRUE
               /\ queued' = 0
               /\ UNCHANGED permit
          ELSE /\ permit' = TRUE
               /\ UNCHANGED <<writing, queued>>
  /\ phase' = "end"
  /\ UNCHANGED <<turn, deferred, callback, runCallback, data, eager, maxEager,
                 extraTurn>>

EndTurn ==
  /\ phase = "end"
  /\ turn' = turn + 1
  /\ maxEager' = IF eager > maxEager THEN eager ELSE maxEager
  /\ runCallback' = callback
  /\ callback' = FALSE
  /\ data' \in BOOLEAN
  /\ phase' = "start"
  /\ UNCHANGED <<permit, deferred, queued, writing, eager, extraTurn>>

Terminal == phase = "start" /\ turn = MaxTurn

Quiescent == Terminal /\ UNCHANGED vars

Next == StartTurn \/ ReaderStep \/ ReaderIdle \/ WriterStep \/ EndTurn \/ Quiescent

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ turn \in 0..MaxTurn
  /\ phase \in {"start", "reader", "writer", "end"}
  /\ queued \in 0..8
  /\ eager \in 0..4

OneEagerAckPerTurn == eager <= 1 /\ maxEager <= 1

NoExtraTurn == ~extraTurn

=============================================================================
