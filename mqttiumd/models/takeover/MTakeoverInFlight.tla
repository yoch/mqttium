------------------------- MODULE MTakeoverInFlight -------------------------
(***************************************************************************)
(* MTakeoverInFlight: a client connects again while frames of its session  *)
(* are in flight on its previous connection, as written in                 *)
(* crates/mqttiumd/src/runtime.rs (`connect`, `start_takeover`,            *)
(* `pump_will_connects`, `close`, `finish_close`, `collect_written`),      *)
(* net.rs (`Control::stop`, `Connection::close`, `Lanes::finish`) and      *)
(* crates/mqttiumd-core/src/owner.rs (`plan_frame_result`).                *)
(*                                                                         *)
(* Generation 1 is the connection the session is bound to, generation 2    *)
(* the one that takes it over. A frame belongs to the generation it was    *)
(* submitted on: its receipt and its result carry that ConnectionKey.      *)
(*                                                                         *)
(*   - `start_takeover` marks the old peer `closing`: it gets no new       *)
(*     frame. Its close follows (at once for MQTT 3.1.1, after the         *)
(*     DISCONNECT or the 300 ms bound for MQTT 5).                         *)
(*   - `Connection::close` is `Control::stop` on the core thread, three    *)
(*     steps the task may interleave: the terminal reason is recorded,     *)
(*     `cancelled` is set, the socket is shut down.                        *)
(*   - The task checks `cancelled` and then writes; a write on a socket    *)
(*     that is shut down fails. A task that ends runs `Lanes::finish`.     *)
(*   - `finish_close` gives Failed to every frame the old generation       *)
(*     still has in `Runtime::frames`, queues ConnectionClosed and         *)
(*     removes the peer; its lanes are not read again.                     *)
(*   - `pump_will_connects` starts the open of the new generation only     *)
(*     once the old peer is removed.                                       *)
(*   - The owner ends a connection on a Failed result only when the        *)
(*     result names the generation the session is bound to.                *)
(*                                                                         *)
(* The owner takes its inputs from several lanes whose relative order the  *)
(* model does not fix: `inbox` is a set. `Lanes::reports`, Started and     *)
(* NotStarted are in MTransportCredit; the DISCONNECT of MQTT 5, the Will  *)
(* barrier and the store are not here. The history of the verifier         *)
(* (prefix h) is read by no guard.                                         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    NumOld,     \* frames the core has for generation 1
    NumNew,     \* frames the core has for generation 2
    OutSlots,   \* capacity of a frame lane
    WrittenCap, \* capacity of a receipt lane
    Preserve,   \* the core waits for the task's facts before it closes
    Mutation    \* "none" or the name of a model mutation

Mutations == {"none", "NoGenerationCheck", "LeaveFramesAtClose",
              "OpenWithoutWaiting", "CancelWithoutShutdown"}

ASSUME /\ NumOld \in Nat /\ NumNew \in Nat
       /\ OutSlots \in Nat \ {0} /\ WrittenCap \in Nat \ {0}
       /\ Preserve \in BOOLEAN
       /\ Mutation \in Mutations

Old == 1
New == 2
Gens == {Old, New}
Unbound == 0
Frames == 1..(NumOld + NumNew)
NoFrame == 0
GenOf(f) == IF f <= NumOld THEN Old ELSE New
Terminals == {"Complete", "Failed"}
Receipts == [frame : Frames, kind : Terminals]
FrameInput(f, k) == [type |-> "frame", frame |-> f, gen |-> GenOf(f), kind |-> k]
ClosedInput(g) == [type |-> "closed", frame |-> NoFrame, gen |-> g, kind |-> "none"]
OpenInput == [type |-> "open", frame |-> NoFrame, gen |-> New, kind |-> "none"]
Inputs == {FrameInput(f, k) : f \in Frames, k \in Terminals}
          \cup {ClosedInput(g) : g \in Gens} \cup {OpenInput}

VARIABLES
    peers,      \* generations in Runtime::peers
    closing,    \* per generation: Peer::closing
    retire,     \* per generation: Peer::retire_code
    opening,    \* the connect of generation 2: "none", "waiting", "queued", "done"
    submitted,  \* frames the core submitted
    pendingF,   \* Runtime::frames
    inbox,      \* inputs queued for the owner
    binding,    \* Owner::connection_of(session)
    result,     \* per frame: the terminal result the owner applied
    stopPc,     \* the core inside Control::stop: "idle", "cancel", "shut"
    stopGen,    \* the generation it is stopping
    recorded,   \* per generation: Control::terminal is set
    cancelled,  \* per generation: Control::cancelled
    shut,       \* per generation: the socket is shut down
    out,        \* per generation: the frame lane
    written,    \* per generation: the receipt lane
    tpc,        \* per generation: the task, "run", "fin" or "done"
    active,     \* per generation: the frame in progress, or NoFrame
    phase,      \* per generation, of that frame: "new", "checked", "written"
    finished,   \* per generation: Link::finished
    hForeignEnd,      \* a result ended a generation it does not name
    hWriteAfterBound, \* bytes left on the old socket once the new one was bound
    hWriteAfterRecord \* bytes left on a socket after its terminal reason

coreVars == <<peers, closing, retire, opening, submitted, pendingF>>
ownerVars == <<inbox, binding, result>>
stopVars == <<stopPc, stopGen>>
ctlVars == <<recorded, cancelled, shut>>
taskVars == <<tpc, active, phase, finished>>
histVars == <<hForeignEnd, hWriteAfterBound, hWriteAfterRecord>>
vars == <<coreVars, ownerVars, stopVars, ctlVars, out, written, taskVars, histVars>>

Room(g) == Len(written[g]) < WrittenCap
\* The receipts of the lane before its first Failed have been read.
FactsRead(g) ==
    IF written[g] = <<>> THEN TRUE ELSE Head(written[g]).kind = "Failed"
Of(g) == {f \in Frames : GenOf(f) = g}

Init ==
    /\ peers = {Old}
    /\ closing = [g \in Gens |-> FALSE] /\ retire = [g \in Gens |-> FALSE]
    /\ opening = "none"
    /\ submitted = {} /\ pendingF = {}
    /\ inbox = {} /\ binding = Old
    /\ result = [f \in Frames |-> "none"]
    /\ stopPc = "idle" /\ stopGen = Old
    /\ recorded = [g \in Gens |-> FALSE]
    /\ cancelled = [g \in Gens |-> FALSE]
    /\ shut = [g \in Gens |-> FALSE]
    /\ out = [g \in Gens |-> <<>>] /\ written = [g \in Gens |-> <<>>]
    /\ tpc = [g \in Gens |-> "run"]
    /\ active = [g \in Gens |-> NoFrame]
    /\ phase = [g \in Gens |-> "new"]
    /\ finished = [g \in Gens |-> FALSE]
    /\ hForeignEnd = FALSE /\ hWriteAfterBound = FALSE /\ hWriteAfterRecord = FALSE

(***************************************************************************)
(* The core thread. It does one thing at a time: no step below happens     *)
(* while it is inside Control::stop.                                       *)
(***************************************************************************)

\* Sink::try_submit: a frame goes to the peer of its generation only while
\* that peer is not closing and the session is bound to it.
CoreSubmit(f) ==
    LET g == GenOf(f) IN
    /\ stopPc = "idle"
    /\ f \notin submitted /\ \A e \in Of(g) : e < f => e \in submitted
    /\ g \in peers /\ ~closing[g] /\ binding = g
    /\ Len(out[g]) < OutSlots /\ tpc[g] # "done"
    /\ out' = [out EXCEPT ![g] = Append(@, f)]
    /\ submitted' = submitted \cup {f}
    /\ pendingF' = pendingF \cup {f}
    /\ UNCHANGED <<peers, closing, retire, opening, ownerVars, stopVars, ctlVars,
                   written, taskVars, histVars>>

\* collect_written, one receipt of a peer that is still in the table. The
\* receipt carries the ConnectionKey of its frame; the result keeps it.
CoreCollect(g) ==
    /\ stopPc = "idle" /\ g \in peers /\ written[g] # <<>>
    /\ LET r == Head(written[g]) IN
          /\ written' = [written EXCEPT ![g] = Tail(@)]
          /\ pendingF' = pendingF \ {r.frame}
          /\ inbox' = IF r.frame \in pendingF \/ r.kind = "Failed"
                      THEN inbox \cup {FrameInput(r.frame, r.kind)}
                      ELSE inbox
    /\ UNCHANGED <<peers, closing, retire, opening, submitted, binding, result,
                   stopVars, ctlVars, out, taskVars, histVars>>

\* The second connection of the client arrives. `connect` finds the
\* generation its session is bound to and `start_takeover` marks it.
NewConnect ==
    /\ stopPc = "idle" /\ opening = "none"
    /\ peers' = peers \cup {New}
    /\ opening' = "waiting"
    /\ closing' = [closing EXCEPT ![Old] = @ \/ Old \in peers]
    /\ UNCHANGED <<retire, submitted, pendingF, ownerVars, stopVars, ctlVars, out,
                   written, taskVars, histVars>>

\* Runtime::close begins: the retire code is set and Connection::close
\* records the terminal reason, the first step of Control::stop.
BeginClose(g) ==
    /\ stopPc = "idle" /\ g \in peers /\ ~retire[g]
    /\ retire' = [retire EXCEPT ![g] = TRUE]
    /\ closing' = [closing EXCEPT ![g] = TRUE]
    /\ recorded' = [recorded EXCEPT ![g] = TRUE]
    /\ stopPc' = "cancel" /\ stopGen' = g
    /\ UNCHANGED <<peers, opening, submitted, pendingF, ownerVars, cancelled, shut,
                   out, written, taskVars, histVars>>

\* A close that is due: the peer was taken over, or its task ended.
CoreCloseDue(g) == (closing[g] \/ finished[g]) /\ BeginClose(g)

\* A close for any other reason of the core: a deadline, a protocol error.
CoreCloseOther(g) == BeginClose(g)

\* `cancelled.store(true)`.
CoreStopCancel ==
    /\ stopPc = "cancel"
    /\ cancelled' = [cancelled EXCEPT ![stopGen] = TRUE]
    /\ stopPc' = "shut"
    /\ UNCHANGED <<coreVars, ownerVars, stopGen, recorded, shut, out, written,
                   taskVars, histVars>>

\* `stream.shutdown(Shutdown::Both)`. CancelWithoutShutdown leaves the
\* socket open and relies on the flag.
CoreStopShut ==
    /\ stopPc = "shut"
    /\ shut' = IF Mutation = "CancelWithoutShutdown" THEN shut
               ELSE [shut EXCEPT ![stopGen] = TRUE]
    /\ stopPc' = "idle"
    /\ UNCHANGED <<coreVars, ownerVars, stopGen, recorded, cancelled, out, written,
                   taskVars, histVars>>

\* finish_close: Failed for every frame the generation still has in the
\* table, ConnectionClosed, and the peer leaves the table. With Preserve
\* the core first waits for `is_finished` and reads the lane up to its
\* first Failed. LeaveFramesAtClose removes the peer and leaves its
\* frames.
CoreFinishClose(g) ==
    /\ stopPc = "idle" /\ g \in peers /\ retire[g]
    /\ Preserve => finished[g] /\ FactsRead(g)
    /\ peers' = peers \ {g}
    /\ LET left == pendingF \cap Of(g) IN
          IF Mutation = "LeaveFramesAtClose"
          THEN /\ inbox' = inbox \cup {ClosedInput(g)}
               /\ UNCHANGED pendingF
          ELSE /\ inbox' = inbox \cup {ClosedInput(g)}
                              \cup {FrameInput(f, "Failed") : f \in left}
               /\ pendingF' = pendingF \ left
    /\ UNCHANGED <<closing, retire, opening, submitted, binding, result, stopVars,
                   ctlVars, out, written, taskVars, histVars>>

\* pump_will_connects: the open of the new generation starts only once
\* the previous peer is gone from the table. OpenWithoutWaiting starts it
\* at once.
CoreOpenNew ==
    /\ stopPc = "idle" /\ opening = "waiting"
    /\ New \in peers /\ ~closing[New]
    /\ Old \notin peers \/ Mutation = "OpenWithoutWaiting"
    /\ opening' = "queued"
    /\ inbox' = inbox \cup {OpenInput}
    /\ UNCHANGED <<peers, closing, retire, submitted, pendingF, binding, result,
                   stopVars, ctlVars, out, written, taskVars, histVars>>

(***************************************************************************)
(* The owner applies one queued input.                                     *)
(***************************************************************************)

\* plan_frame_result: the result is recorded for its frame. Failed ends
\* the connection only if the session is bound to the generation the
\* result names. NoGenerationCheck ends whatever the session is bound to.
OwnerFrameResult(i) ==
    /\ i \in inbox /\ i.type = "frame"
    /\ inbox' = inbox \ {i}
    /\ result' = [result EXCEPT ![i.frame] = IF @ = "none" THEN i.kind ELSE "twice"]
    /\ LET ends == /\ i.kind = "Failed" /\ binding # Unbound
                   /\ binding = i.gen \/ Mutation = "NoGenerationCheck"
       IN /\ binding' = IF ends THEN Unbound ELSE binding
          /\ hForeignEnd' = (hForeignEnd \/ (ends /\ binding # i.gen))
    /\ UNCHANGED <<coreVars, stopVars, ctlVars, out, written, taskVars,
                   hWriteAfterBound, hWriteAfterRecord>>

\* ConnectionClosed detaches the session only from the generation it
\* names. pop_closing_input holds it back while an older ordinary input
\* of its generation is queued.
OwnerClosed(i) ==
    /\ i \in inbox /\ i.type = "closed"
    /\ ~(i.gen = New /\ OpenInput \in inbox)
    /\ inbox' = inbox \ {i}
    /\ binding' = IF binding = i.gen THEN Unbound ELSE binding
    /\ UNCHANGED <<coreVars, result, stopVars, ctlVars, out, written, taskVars, histVars>>

\* The session is bound to the new generation.
OwnerOpen ==
    /\ OpenInput \in inbox
    /\ inbox' = inbox \ {OpenInput}
    /\ binding' = New
    /\ opening' = "done"
    /\ UNCHANGED <<peers, closing, retire, submitted, pendingF, result, stopVars,
                   ctlVars, out, written, taskVars, histVars>>

(***************************************************************************)
(* The transport task of a generation, on its reactor thread.              *)
(***************************************************************************)

\* The task ends: `Driven::finish` runs Control::stop on its own thread,
\* where no write of this task can interleave, then Lanes::finish.
End(g) ==
    /\ tpc' = [tpc EXCEPT ![g] = "fin"]
    /\ recorded' = [recorded EXCEPT ![g] = TRUE]
    /\ cancelled' = [cancelled EXCEPT ![g] = TRUE]
    /\ shut' = [shut EXCEPT ![g] = TRUE]

TaskTake(g) ==
    /\ tpc[g] = "run" /\ active[g] = NoFrame /\ out[g] # <<>>
    /\ active' = [active EXCEPT ![g] = Head(out[g])]
    /\ phase' = [phase EXCEPT ![g] = "new"]
    /\ out' = [out EXCEPT ![g] = Tail(@)]
    /\ UNCHANGED <<coreVars, ownerVars, stopVars, ctlVars, written, tpc, finished, histVars>>

\* `control.is_cancelled()` before the write of write_attempt.
TaskCheck(g) ==
    /\ tpc[g] = "run" /\ active[g] # NoFrame /\ phase[g] = "new"
    /\ IF cancelled[g]
       THEN End(g) /\ UNCHANGED phase
       ELSE phase' = [phase EXCEPT ![g] = "checked"] /\ UNCHANGED <<tpc, ctlVars>>
    /\ UNCHANGED <<coreVars, ownerVars, stopVars, out, written, active, finished, histVars>>

\* `stream.write`: the bytes of the frame leave, unless the socket was
\* shut down since the check, in which case the write fails and the task
\* ends.
TaskWrite(g) ==
    /\ tpc[g] = "run" /\ active[g] # NoFrame /\ phase[g] = "checked"
    /\ IF shut[g]
       THEN End(g) /\ UNCHANGED <<phase, histVars>>
       ELSE /\ phase' = [phase EXCEPT ![g] = "written"]
            /\ hWriteAfterBound' = (hWriteAfterBound \/ (g = Old /\ binding = New))
            /\ hWriteAfterRecord' = (hWriteAfterRecord \/ recorded[g])
            /\ UNCHANGED <<tpc, ctlVars, hForeignEnd>>
    /\ UNCHANGED <<coreVars, ownerVars, stopVars, out, written, active, finished>>

\* Complete is published and the frame is no longer in progress.
TaskReport(g) ==
    /\ tpc[g] = "run" /\ active[g] # NoFrame /\ phase[g] = "written" /\ Room(g)
    /\ written' = [written EXCEPT ![g] = Append(@, [frame |-> active[g], kind |-> "Complete"])]
    /\ active' = [active EXCEPT ![g] = NoFrame]
    /\ UNCHANGED <<coreVars, ownerVars, stopVars, ctlVars, out, tpc, phase, finished, histVars>>

\* `control.is_cancelled()` at the start of a turn.
TaskCancelled(g) ==
    /\ tpc[g] = "run" /\ cancelled[g]
    /\ End(g)
    /\ UNCHANGED <<coreVars, ownerVars, stopVars, out, written, active, phase, finished, histVars>>

\* End of file or an error of the transport: at any time.
TaskError(g) ==
    /\ tpc[g] = "run"
    /\ End(g)
    /\ UNCHANGED <<coreVars, ownerVars, stopVars, out, written, active, phase, finished, histVars>>

\* Lanes::finish, one receipt at a time: the receipt of the frame in
\* progress (Complete if its bytes left, Failed otherwise), then Failed
\* for each frame still in the lane. Each is offered once; the first
\* refusal stops the loop. Then `retire` sets `Link::finished`.
TaskFinish(g) ==
    /\ tpc[g] = "fin"
    /\ LET f == IF active[g] # NoFrame THEN active[g]
                ELSE IF out[g] # <<>> THEN Head(out[g]) ELSE NoFrame
           kind == IF active[g] # NoFrame /\ phase[g] = "written" THEN "Complete" ELSE "Failed"
       IN IF f = NoFrame \/ ~Room(g)
          THEN /\ tpc' = [tpc EXCEPT ![g] = "done"]
               /\ finished' = [finished EXCEPT ![g] = TRUE]
               /\ active' = [active EXCEPT ![g] = NoFrame]
               /\ out' = IF f # NoFrame /\ active[g] = NoFrame
                         THEN [out EXCEPT ![g] = Tail(@)] ELSE out
               /\ UNCHANGED written
          ELSE /\ written' = [written EXCEPT ![g] = Append(@, [frame |-> f, kind |-> kind])]
               /\ active' = [active EXCEPT ![g] = NoFrame]
               /\ out' = IF active[g] = NoFrame THEN [out EXCEPT ![g] = Tail(@)] ELSE out
               /\ UNCHANGED <<tpc, finished>>
    /\ UNCHANGED <<coreVars, ownerVars, stopVars, ctlVars, phase, histVars>>

\* The end of a run: the second connection arrived, both peers are gone
\* from the table, both tasks ended and the owner applied every input.
\* A peer that stays in the table always has a step left: the core may
\* close it.
Terminal ==
    /\ peers = {} /\ opening # "none" /\ stopPc = "idle"
    /\ \A g \in Gens : tpc[g] = "done"
    /\ inbox = {}

Quiescent == Terminal /\ UNCHANGED vars

Next ==
    \/ \E f \in Frames : CoreSubmit(f)
    \/ \E g \in Gens : CoreCollect(g)
    \/ NewConnect
    \/ \E g \in Gens : CoreCloseDue(g)
    \/ \E g \in Gens : CoreCloseOther(g)
    \/ CoreStopCancel
    \/ CoreStopShut
    \/ \E g \in Gens : CoreFinishClose(g)
    \/ CoreOpenNew
    \/ \E i \in Inputs : OwnerFrameResult(i)
    \/ \E i \in Inputs : OwnerClosed(i)
    \/ OwnerOpen
    \/ \E g \in Gens : TaskTake(g)
    \/ \E g \in Gens : TaskCheck(g)
    \/ \E g \in Gens : TaskWrite(g)
    \/ \E g \in Gens : TaskReport(g)
    \/ \E g \in Gens : TaskCancelled(g)
    \/ \E g \in Gens : TaskError(g)
    \/ \E g \in Gens : TaskFinish(g)
    \/ Quiescent

Spec == Init /\ [][Next]_vars

\* The core finishes what it began and reads its lanes, the owner applies
\* its inputs and the reactors run their tasks. Nothing is assumed about
\* CoreSubmit, NewConnect, CoreCloseOther and TaskError: the core may have
\* nothing to send or no reason to close, and the clients own their
\* sockets.
FairSpec ==
    /\ Spec
    /\ WF_vars(CoreStopCancel) /\ WF_vars(CoreStopShut) /\ WF_vars(CoreOpenNew)
    /\ WF_vars(OwnerOpen)
    /\ \A i \in Inputs : WF_vars(OwnerFrameResult(i)) /\ WF_vars(OwnerClosed(i))
    /\ \A g \in Gens :
          /\ WF_vars(CoreCollect(g)) /\ WF_vars(CoreCloseDue(g))
          /\ WF_vars(CoreFinishClose(g))
          /\ WF_vars(TaskTake(g)) /\ WF_vars(TaskCheck(g)) /\ WF_vars(TaskWrite(g))
          /\ WF_vars(TaskReport(g)) /\ WF_vars(TaskCancelled(g)) /\ WF_vars(TaskFinish(g))

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

TypeOK ==
    /\ peers \subseteq Gens
    /\ closing \in [Gens -> BOOLEAN] /\ retire \in [Gens -> BOOLEAN]
    /\ opening \in {"none", "waiting", "queued", "done"}
    /\ submitted \subseteq Frames /\ pendingF \subseteq submitted
    /\ inbox \subseteq Inputs
    /\ binding \in Gens \cup {Unbound}
    /\ result \in [Frames -> Terminals \cup {"none", "twice"}]
    /\ stopPc \in {"idle", "cancel", "shut"} /\ stopGen \in Gens
    /\ recorded \in [Gens -> BOOLEAN] /\ cancelled \in [Gens -> BOOLEAN]
    /\ shut \in [Gens -> BOOLEAN] /\ finished \in [Gens -> BOOLEAN]
    /\ out \in [Gens -> Seq(Frames)] /\ written \in [Gens -> Seq(Receipts)]
    /\ tpc \in [Gens -> {"run", "fin", "done"}]
    /\ active \in [Gens -> Frames \cup {NoFrame}]
    /\ phase \in [Gens -> {"new", "checked", "written"}]
    /\ hForeignEnd \in BOOLEAN /\ hWriteAfterBound \in BOOLEAN
    /\ hWriteAfterRecord \in BOOLEAN

LanesFit ==
    \A g \in Gens : Len(out[g]) <= OutSlots /\ Len(written[g]) <= WrittenCap

Queued(f) == Cardinality({i \in inbox : i.type = "frame" /\ i.frame = f})

\* A submitted frame has exactly one terminal result in its life: it is
\* in the core's table, or its one result is queued for the owner, or the
\* owner applied it.
OneResultPerFrame ==
    \A f \in Frames :
        /\ result[f] # "twice"
        /\ f \in submitted =>
              (IF f \in pendingF THEN 1 ELSE 0) + Queued(f)
              + (IF result[f] # "none" THEN 1 ELSE 0) = 1

\* Once the peer of a generation is removed, none of its frames is left
\* in the table: each has its terminal result, queued or applied.
RetiredFramesResolved ==
    \A f \in submitted :
        GenOf(f) \notin peers =>
            /\ f \notin pendingF
            /\ Queued(f) + (IF result[f] # "none" THEN 1 ELSE 0) = 1

\* A result ends only the generation it names: nothing of the old
\* generation is applied to the new one.
NoForeignEnd == ~hForeignEnd

\* The session is bound to the new generation only after the old peer was
\* closed and removed: its socket is shut down.
OldClosedBeforeNewBound ==
    binding = New => Old \notin peers /\ shut[Old]

\* Nothing is written to the old socket once the session is bound to the
\* new generation.
OldSilentOnceNewBound == ~hWriteAfterBound

\* A peer that is closed has its socket shut down before the core does
\* anything else.
ClosedIsShut ==
    \A g \in Gens : (retire[g] /\ stopPc = "idle") => shut[g] /\ cancelled[g]

(***************************************************************************)
(* Progress, under FairSpec.                                               *)
(***************************************************************************)

\* Every accepted frame gets its terminal result at the owner.
EveryFrameResolved ==
    \A f \in Frames : (f \in pendingF) ~> (result[f] \in Terminals)

\* A takeover ends: the session is bound to the new generation, or the
\* new connection was closed first.
TakeoverCompletes == (opening = "waiting") ~> (binding = New \/ retire[New])

(***************************************************************************)
(* Non-vacuity, each the negation of an invariant TLC must report.         *)
(***************************************************************************)

\* Bytes leave on a socket after the core recorded its terminal reason and
\* before it shut the socket down: the task had checked `cancelled` before
\* the core set it. The implementation allows this window.
NoWriteAfterRecord == ~hWriteAfterRecord

\* A Failed result of the old generation is applied while the session is
\* bound to the new one, and the generation check absorbs it.
NoLateOldResultWitness ==
    ~\E i \in inbox : i.type = "frame" /\ i.gen = Old /\ i.kind = "Failed" /\ binding = New

\* The close of the old generation fails a frame that was in flight.
NoFrameFailedByCloseWitness ==
    ~\E f \in Frames : GenOf(f) = Old /\ result[f] = "Failed" /\ binding = New

\* A frame of the old generation was written and one failed.
NoMixedOutcomeWitness ==
    ~\E f, e \in Frames : result[f] = "Complete" /\ result[e] = "Failed"

=============================================================================
