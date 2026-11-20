-------------------------- MODULE MTransportCredit --------------------------
(***************************************************************************)
(* MTransportCredit: the frames the core hands to one transport task and   *)
(* the receipts that come back, as written in crates/mqttiumd/src/net.rs   *)
(* (`Lanes`, `Task`, `Lanes::finish`), net/reactor.rs (`retire`) and       *)
(* runtime.rs (`collect_written`, `close`, `finish_close`).                *)
(*                                                                         *)
(* There is no credit counter in the runtime. What bounds the frames in    *)
(* flight is the frame lane itself (`sync_channel(OUTPUT_SLOTS)`):         *)
(* `Outbound::try_send` refuses when it is full and the effect stays in    *)
(* the core's outbox. What bounds the receipts is the task:                *)
(*                                                                         *)
(*   - a receipt is queued in `Lanes::reports`, in publication order, and  *)
(*     published without blocking (`Lanes::flush`);                        *)
(*   - `Lanes::next_frame` takes no frame while a receipt is pending;      *)
(*   - a receipt refused for RESULT_WAIT ends the task                     *)
(*     ("write_result_queue_full"), as a write that exceeds WRITE_TIMEOUT  *)
(*     does.                                                               *)
(*                                                                         *)
(* A frame gets Started at its first byte when it carries an expiry, and   *)
(* one terminal receipt: NotStarted (expired with no byte sent), Complete  *)
(* or Failed. A task that ends runs `Lanes::finish`: the pending receipts, *)
(* then Failed for the frame in progress and for every frame still in the  *)
(* lane, each offered once with `try_send`; the first refusal stops it.    *)
(* `retire` sets `Link::finished` after that.                              *)
(*                                                                         *)
(* The core removes a frame from `Runtime::frames` when it reads its       *)
(* terminal receipt. `finish_close` fails every frame the connection       *)
(* still has there and removes the peer, whose lanes are never read        *)
(* again: it covers a receipt the full lane refused at the end and a       *)
(* frame submitted while the task was ending.                              *)
(*                                                                         *)
(* The two transports differ in one step, TaskSealFails: see               *)
(* TerminalWhenRoom. WireProgress, the input lane and the scheduling of    *)
(* the task are not here: a task step may happen at any time. The history  *)
(* of the verifier (prefix h) is read by no guard.                         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    NumFrames,  \* frames the core has for this connection
    OutSlots,   \* capacity of the frame lane (OUTPUT_SLOTS)
    WrittenCap, \* capacity of the receipt lane (WRITTEN_SLOTS)
    Writable,   \* the socket: "always", "never", or "any" (it changes)
    Transport,  \* "plain" (net.rs Task) or "tls" (net/tls.rs Task)
    Preserve,   \* the core waits for the task's facts before it closes
    Mutation    \* "none" or the name of a model mutation

Mutations == {"none", "DoubleReceipt", "LostReceiptOnClose", "NoCloseSweep",
              "FinishedFirst", "TakeWhilePending", "FlushNewestFirst"}

ASSUME /\ NumFrames \in Nat \ {0} /\ OutSlots \in Nat \ {0}
       /\ WrittenCap \in Nat \ {0}
       /\ Writable \in {"always", "never", "any"}
       /\ Transport \in {"plain", "tls"}
       /\ Preserve \in BOOLEAN
       /\ Mutation \in Mutations

Frames == 1..NumFrames
NoFrame == 0
Terminals == {"NotStarted", "Complete", "Failed"}
Receipts == [frame : Frames, kind : Terminals \cup {"Started"}]

VARIABLES
    next,       \* first frame the core has not submitted
    pendingF,   \* Runtime::frames: accepted frames without a terminal result
    result,     \* per frame: the terminal result the core queued, or "none"
    peer,       \* the connection is in Runtime::peers
    retire,     \* Peer::retire_code is set
    fault,      \* the core read a terminal receipt it cannot correlate
    cancelled,  \* Control::cancelled
    out,        \* the frame lane
    written,    \* the receipt lane
    tpc,        \* the task: "run", "finReports", "finFrames", "finFlag", "done"
    active,     \* the frame in progress, or NoFrame
    phase,      \* of the frame in progress: "new", "started", "written"
    reports,    \* Lanes::reports
    finished,   \* Link::finished
    writable,   \* the socket accepts bytes
    hProduced,  \* per frame: terminal receipts the task produced
    hFact,      \* per frame: the terminal receipt published into the lane
    hLateStart, \* a Started was published after the terminal of its frame
    hAfterEnd,  \* a receipt was published after `finished` was set
    hRefused,   \* the full lane refused a receipt of the ending task
    hLate       \* frames accepted after the ending task last looked at the lane

coreVars == <<next, pendingF, result, peer, retire, fault>>
taskVars == <<tpc, active, phase, reports, finished>>
histVars == <<hProduced, hFact, hLateStart, hAfterEnd, hRefused, hLate>>
vars == <<coreVars, cancelled, out, written, taskVars, writable, histVars>>

Accepted == 1..(next - 1)
\* The receipts of the lane before its first Failed have been read.
FactsRead == IF written = <<>> THEN TRUE ELSE Head(written).kind = "Failed"
Room == Len(written) < WrittenCap
Receipt(f, k) == [frame |-> f, kind |-> k]

Init ==
    /\ next = 1 /\ pendingF = {} /\ result = [f \in Frames |-> "none"]
    /\ peer = TRUE /\ retire = FALSE /\ fault = FALSE
    /\ cancelled = FALSE
    /\ out = <<>> /\ written = <<>>
    /\ tpc = "run" /\ active = NoFrame /\ phase = "new" /\ reports = <<>>
    /\ finished = FALSE
    /\ writable = (Writable # "never")
    /\ hProduced = [f \in Frames |-> 0] /\ hFact = [f \in Frames |-> "none"]
    /\ hLateStart = FALSE /\ hAfterEnd = FALSE /\ hRefused = FALSE /\ hLate = {}

\* The task publishes one receipt into the lane.
Publish(r) ==
    /\ written' = Append(written, r)
    /\ hFact' = IF r.kind \in Terminals THEN [hFact EXCEPT ![r.frame] = r.kind] ELSE hFact
    /\ hLateStart' = (hLateStart \/ (r.kind = "Started" /\ hFact[r.frame] # "none"))
    /\ hAfterEnd' = (hAfterEnd \/ finished)

NoPublish == UNCHANGED <<written, hFact, hLateStart, hAfterEnd>>

Produced(f) == hProduced' = [hProduced EXCEPT ![f] = @ + 1]

(***************************************************************************)
(* The core.                                                               *)
(***************************************************************************)

\* Sink::try_submit: `outgoing.try_send`, then `frames.insert`. A full
\* lane refuses and the effect stays in the outbox: the absence of this
\* step. A peer that is closing gets no frame, and the lane of a task that
\* is gone answers Disconnected.
CoreSubmit ==
    /\ peer /\ ~retire /\ next <= NumFrames
    /\ Len(out) < OutSlots /\ tpc # "done"
    /\ out' = Append(out, next)
    /\ pendingF' = pendingF \cup {next}
    /\ next' = next + 1
    /\ hLate' = IF tpc = "finFlag" THEN hLate \cup {next} ELSE hLate
    /\ UNCHANGED <<result, peer, retire, fault, cancelled, written, taskVars, writable,
                   hProduced, hFact, hLateStart, hAfterEnd, hRefused>>

\* collect_written, one receipt. A terminal receipt removes its frame
\* from the table and queues its result; Failed is queued whether or not
\* the frame is still there, and closes the connection. Complete for a
\* frame that is not in the table of a live peer is the fault
\* "uncorrelated_frame_result".
CoreCollect ==
    /\ peer /\ written # <<>>
    /\ LET r == Head(written)
           f == r.frame
           known == f \in pendingF
       IN /\ written' = Tail(written)
          /\ IF r.kind = "Started"
             THEN UNCHANGED <<pendingF, result, retire, fault, cancelled>>
             ELSE /\ pendingF' = pendingF \ {f}
                  /\ result' = IF known \/ r.kind = "Failed"
                               THEN [result EXCEPT ![f] = IF @ = "none" THEN r.kind ELSE "twice"]
                               ELSE result
                  /\ fault' = (fault \/ (r.kind = "Complete" /\ ~known))
                  /\ retire' = (retire \/ r.kind = "Failed")
                  /\ cancelled' = (cancelled \/ r.kind = "Failed")
    /\ UNCHANGED <<next, peer, out, taskVars, writable, histVars>>

\* Runtime::close for a reason of the core's own: a deadline, a protocol
\* error, a takeover. `Connection::close` cancels the task.
CoreClose ==
    /\ peer /\ ~retire
    /\ retire' = TRUE /\ cancelled' = TRUE
    /\ UNCHANGED <<next, pendingF, result, peer, fault, out, written, taskVars,
                   writable, histVars>>

\* The core learns that the task ended from its input lane (Closed, or
\* Disconnected once the task is gone).
CoreObserveEnd ==
    /\ peer /\ ~retire /\ finished
    /\ retire' = TRUE /\ cancelled' = TRUE
    /\ UNCHANGED <<next, pendingF, result, peer, fault, out, written, taskVars,
                   writable, histVars>>

\* finish_close: every frame the connection still has in the table gets
\* Failed and the peer is removed; its lanes are not read again. With
\* Preserve (a QoS 1 PUBLISH is pending: `must_preserve_write_facts`) the
\* core first waits for `is_finished` and reads the lane up to its first
\* Failed. NoCloseSweep removes the peer and leaves the frames.
CoreFinishClose ==
    /\ peer /\ retire
    /\ Preserve => finished /\ FactsRead
    /\ peer' = FALSE
    /\ IF Mutation = "NoCloseSweep"
       THEN UNCHANGED <<pendingF, result>>
       ELSE /\ pendingF' = {}
            /\ result' = [f \in Frames |->
                             IF f \in pendingF
                             THEN (IF result[f] = "none" THEN "Failed" ELSE "twice")
                             ELSE result[f]]
    /\ UNCHANGED <<next, retire, fault, cancelled, out, written, taskVars, writable,
                   histVars>>

(***************************************************************************)
(* The task, while it runs.                                                *)
(***************************************************************************)

\* Lanes::next_frame: no frame is taken while a receipt is pending.
TaskTake ==
    /\ tpc = "run" /\ active = NoFrame /\ out # <<>>
    /\ reports = <<>> \/ Mutation = "TakeWhilePending"
    /\ active' = Head(out) /\ phase' = "new"
    /\ out' = Tail(out)
    /\ UNCHANGED <<coreVars, cancelled, written, tpc, reports, finished, writable, histVars>>

\* prepare_expiry before the first byte: the frame is disposed of with
\* NotStarted and the connection goes on.
TaskNotStarted ==
    /\ tpc = "run" /\ active # NoFrame /\ phase = "new"
    /\ reports' = Append(reports, Receipt(active, "NotStarted"))
    /\ Produced(active)
    /\ active' = NoFrame
    /\ UNCHANGED <<coreVars, cancelled, out, written, tpc, phase, finished, writable,
                   hFact, hLateStart, hAfterEnd, hRefused, hLate>>

\* The first bytes leave. A correlated frame with an expiry reports
\* Started.
TaskStart ==
    /\ tpc = "run" /\ active # NoFrame /\ phase = "new" /\ writable
    /\ phase' = "started"
    /\ \E timed \in BOOLEAN :
          reports' = IF timed THEN Append(reports, Receipt(active, "Started")) ELSE reports
    /\ UNCHANGED <<coreVars, cancelled, out, written, tpc, active, finished, writable,
                   histVars>>

\* The last byte left: Complete is queued and the frame is no longer in
\* progress. DoubleReceipt queues Complete and keeps the frame in place
\* until TaskRelease.
TaskComplete ==
    /\ tpc = "run" /\ active # NoFrame /\ phase = "started" /\ writable
    /\ reports' = Append(reports, Receipt(active, "Complete"))
    /\ Produced(active)
    /\ IF Mutation = "DoubleReceipt"
       THEN phase' = "written" /\ UNCHANGED active
       ELSE active' = NoFrame /\ UNCHANGED phase
    /\ UNCHANGED <<coreVars, cancelled, out, written, tpc, finished, writable,
                   hFact, hLateStart, hAfterEnd, hRefused, hLate>>

\* Only DoubleReceipt reaches "written".
TaskRelease ==
    /\ tpc = "run" /\ active # NoFrame /\ phase = "written"
    /\ active' = NoFrame
    /\ UNCHANGED <<coreVars, cancelled, out, written, tpc, phase, reports, finished,
                   writable, histVars>>

\* Lanes::flush: the oldest pending receipt, if the lane has room.
\* FlushNewestFirst publishes the newest one.
TaskFlush ==
    /\ tpc = "run" /\ reports # <<>> /\ Room
    /\ IF Mutation = "FlushNewestFirst"
       THEN /\ Publish(reports[Len(reports)])
            /\ reports' = SubSeq(reports, 1, Len(reports) - 1)
       ELSE /\ Publish(Head(reports))
            /\ reports' = Tail(reports)
    /\ UNCHANGED <<coreVars, cancelled, out, tpc, active, phase, finished, writable,
                   hProduced, hRefused, hLate>>

\* The task ends: `turn` returned an error and the reactor retires it.
\* FinishedFirst sets `Link::finished` before the terminal facts.
End ==
    /\ tpc' = "finReports"
    /\ finished' = (Mutation = "FinishedFirst")
    /\ UNCHANGED <<coreVars, cancelled, out, written, active, phase, reports, writable,
                   histVars>>

\* `control.is_cancelled()` at the start of a turn or before a write.
TaskCancelled == tpc = "run" /\ cancelled /\ End

\* A deadline the task holds: WRITE_TIMEOUT of the frame in progress, or
\* RESULT_WAIT of a receipt the full lane refuses.
TaskDeadline ==
    /\ tpc = "run"
    /\ active # NoFrame \/ (reports # <<>> /\ ~Room)
    /\ End

\* End of file, a read or write error, a malformed packet: at any time.
TaskTransportError == tpc = "run" /\ End

\* tls::Task::next_write only. The frame taken from the lane is held in a
\* local variable while it is sealed; when `prepare_expiry` or
\* `frame_ciphertext` fails, the error ends the task and the frame is
\* dropped: it is neither the frame in progress nor in the lane when
\* `Lanes::finish` runs. The plain task keeps such a frame in place.
TaskSealFails ==
    /\ Transport = "tls"
    /\ tpc = "run" /\ active # NoFrame /\ phase = "new"
    /\ active' = NoFrame
    /\ tpc' = "finReports"
    /\ finished' = (Mutation = "FinishedFirst")
    /\ UNCHANGED <<coreVars, cancelled, out, written, phase, reports, writable,
                   histVars>>

(***************************************************************************)
(* Lanes::finish and retire.                                               *)
(***************************************************************************)

\* The pending receipts, in order, each offered once. The first refusal
\* drops the rest.
FinishReports ==
    /\ tpc = "finReports"
    /\ IF reports = <<>>
       THEN tpc' = "finFrames" /\ NoPublish /\ UNCHANGED <<reports, hRefused>>
       ELSE IF Room
            THEN /\ Publish(Head(reports)) /\ reports' = Tail(reports)
                 /\ UNCHANGED <<tpc, hRefused>>
            ELSE /\ reports' = <<>> /\ hRefused' = TRUE /\ tpc' = "finFrames"
                 /\ NoPublish
    /\ UNCHANGED <<coreVars, cancelled, out, active, phase, finished, writable,
                   hProduced, hLate>>

\* Failed for the frame in progress, then for each frame still in the
\* lane, each offered once. The first refusal stops the loop: that frame
\* is dropped and the others stay in the lane. LostReceiptOnClose leaves
\* the frames of the lane without a receipt.
FinishFrames ==
    /\ tpc = "finFrames"
    /\ LET f == IF active # NoFrame THEN active
                ELSE IF out # <<>> /\ Mutation # "LostReceiptOnClose" THEN Head(out)
                ELSE NoFrame
       IN IF f = NoFrame
          THEN /\ tpc' = "finFlag" /\ NoPublish
               /\ UNCHANGED <<out, active, hProduced, hRefused>>
          ELSE /\ active' = NoFrame
               /\ out' = IF active # NoFrame THEN out ELSE Tail(out)
               /\ IF Room
                  THEN /\ Publish(Receipt(f, "Failed")) /\ Produced(f)
                       /\ UNCHANGED <<tpc, hRefused>>
                  ELSE /\ hRefused' = TRUE /\ tpc' = "finFlag"
                       /\ NoPublish /\ UNCHANGED hProduced
    /\ UNCHANGED <<coreVars, cancelled, phase, reports, finished, writable, hLate>>

\* retire: `finished.store(true)` once `finish` returned. The lanes of the
\* task are dropped with it.
FinishFlag ==
    /\ tpc = "finFlag"
    /\ finished' = TRUE
    /\ tpc' = "done"
    /\ UNCHANGED <<coreVars, cancelled, out, written, active, phase, reports, writable,
                   histVars>>

\* The peer starts or stops reading.
SocketChanges ==
    /\ Writable = "any"
    /\ writable' = ~writable
    /\ UNCHANGED <<coreVars, cancelled, out, written, taskVars, histVars>>

\* The end of a run: the task is gone and the core removed the peer. A
\* connection that stays open always has a step left: its transport may
\* fail at any time.
Terminal == tpc = "done" /\ ~peer

Quiescent == Terminal /\ UNCHANGED vars

Next ==
    \/ CoreSubmit
    \/ CoreCollect
    \/ CoreClose
    \/ CoreObserveEnd
    \/ CoreFinishClose
    \/ TaskTake
    \/ TaskNotStarted
    \/ TaskStart
    \/ TaskComplete
    \/ TaskRelease
    \/ TaskFlush
    \/ TaskCancelled
    \/ TaskDeadline
    \/ TaskTransportError
    \/ TaskSealFails
    \/ FinishReports
    \/ FinishFrames
    \/ FinishFlag
    \/ SocketChanges
    \/ Quiescent

Spec == Init /\ [][Next]_vars

\* The reactor runs the task, the deadlines of the task fire, and the
\* core reads its lanes and finishes a close it began. Nothing is assumed
\* about CoreSubmit, CoreClose, TaskNotStarted, TaskTransportError and
\* SocketChanges: the core may have nothing to send or no reason to
\* close, and the peer owns what its socket does.
FairSpec ==
    /\ Spec
    /\ WF_vars(TaskTake) /\ WF_vars(TaskStart) /\ WF_vars(TaskComplete)
    /\ WF_vars(TaskRelease) /\ WF_vars(TaskFlush)
    /\ WF_vars(TaskCancelled) /\ WF_vars(TaskDeadline)
    /\ WF_vars(FinishReports) /\ WF_vars(FinishFrames) /\ WF_vars(FinishFlag)
    /\ WF_vars(CoreCollect) /\ WF_vars(CoreObserveEnd) /\ WF_vars(CoreFinishClose)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

TypeOK ==
    /\ next \in 1..(NumFrames + 1)
    /\ pendingF \subseteq Frames
    /\ result \in [Frames -> Terminals \cup {"none", "twice"}]
    /\ peer \in BOOLEAN /\ retire \in BOOLEAN /\ fault \in BOOLEAN
    /\ cancelled \in BOOLEAN /\ finished \in BOOLEAN /\ writable \in BOOLEAN
    /\ out \in Seq(Frames) /\ written \in Seq(Receipts) /\ reports \in Seq(Receipts)
    /\ tpc \in {"run", "finReports", "finFrames", "finFlag", "done"}
    /\ active \in Frames \cup {NoFrame}
    /\ phase \in {"new", "started", "written"}
    /\ hProduced \in [Frames -> 0..2]
    /\ hFact \in [Frames -> Terminals \cup {"none"}]
    /\ hLateStart \in BOOLEAN /\ hAfterEnd \in BOOLEAN /\ hRefused \in BOOLEAN
    /\ hLate \subseteq Frames

\* The lanes never hold more than their capacity, and the task holds at
\* most the two receipts of one frame.
LanesFit == Len(out) <= OutSlots /\ Len(written) <= WrittenCap
ReportsBounded == Len(reports) <= 2

\* The task produces at most one terminal receipt for a frame.
AtMostOneTerminal == \A f \in Frames : hProduced[f] <= 1

\* Started is published only before the terminal receipt of its frame.
StartedBeforeTerminal == ~hLateStart

\* Nothing is published once `Link::finished` reads true: a core that
\* sees it has every terminal fact of the task in its lanes.
NothingAfterFinished == ~hAfterEnd

\* A task that ended with room in the receipt lane left a terminal
\* receipt for every accepted frame, but for one accepted after it last
\* looked at the frame lane. This holds for the plain task. The TLS task
\* breaks it at TaskSealFails; RetiredFramesResolved holds for both.
TerminalWhenRoom ==
    (finished /\ ~hRefused) => \A f \in Accepted : hProduced[f] = 1 \/ f \in hLate

\* The core's table and its results agree: an accepted frame is in the
\* table until its one terminal result is queued, and no result is queued
\* twice or for a receipt that correlates with nothing.
OneResultPerFrame ==
    /\ ~fault
    /\ \A f \in Frames : result[f] # "twice"
    /\ peer => \A f \in Accepted : (f \in pendingF) <=> (result[f] = "none")

\* Once the peer is removed every accepted frame has its terminal result,
\* whatever the task managed to publish.
RetiredFramesResolved ==
    ~peer => pendingF = {} /\ \A f \in Accepted : result[f] \in Terminals

\* With Preserve, a terminal fact the task published is the result of its
\* frame: the close does not replace it with Failed.
FactsPreserved ==
    \A f \in Frames :
        (hFact[f] # "none" /\ result[f] # "none") => result[f] = hFact[f]

(***************************************************************************)
(* Progress, under FairSpec: every accepted frame gets its terminal        *)
(* result.                                                                 *)
(***************************************************************************)
EveryFrameResolved ==
    \A f \in Frames : (f \in pendingF) ~> (result[f] \in Terminals)

(***************************************************************************)
(* Non-vacuity, each the negation of an invariant TLC must report.         *)
(***************************************************************************)

\* The frame lane is full while the core has another frame.
NoBackpressureWitness == ~(peer /\ ~retire /\ next <= NumFrames /\ Len(out) = OutSlots)

\* The receipt lane refused a receipt of the ending task.
NoRefusedAtEndWitness == ~hRefused

\* A frame was accepted after the ending task last looked at the lane.
NoLateSubmitWitness == hLate = {}

\* One frame was written and another failed.
NoMixedOutcomeWitness ==
    ~\E f, g \in Frames : result[f] = "Complete" /\ result[g] = "Failed"

=============================================================================
