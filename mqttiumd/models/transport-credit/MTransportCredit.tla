-------------------------- MODULE MTransportCredit --------------------------
(***************************************************************************)
(* MTransportCredit: the correlated frames the core hands to the transport *)
(* task of one connection and the receipts that come back, as written in   *)
(* crates/mqttiumd/src/net.rs (`ConnCore`, `OutgoingState`, `ToCoreState`, *)
(* `StalledState`, `Task`), net/tls.rs (`Task`, `Machine`),                *)
(* net/reactor.rs (`retire`), runtime/sink.rs (`Sink::try_submit`) and     *)
(* runtime.rs (`collect_written`, `close`, `finish_close`).                *)
(*                                                                         *)
(* Scope: one connection of the product run with the single owner          *)
(* (`--delivery-shards 0`). The coordinator is the only producer of        *)
(* frames and the only consumer of receipts. With `--delivery-shards K`,   *)
(* K >= 1, a second bound applies to the writes a shard owns               *)
(* (`ShardIoRoute::reserve_write`), their receipts go to a `ShardInbox`    *)
(* and `finish_close` waits for the shard: none of that is here.           *)
(*                                                                         *)
(* There is no credit counter. What bounds the frames in flight is the     *)
(* output queue (`OutgoingState::queue`, OUTPUT_SLOTS):                    *)
(* `ConnCore::try_send_outgoing` refuses with Full and the effect stays in *)
(* the core's outbox. What bounds the receipts is the task:                *)
(*                                                                         *)
(*   - a turn writes frames and collects their receipts, then offers the   *)
(*     receipts to the lane in order, under one lock, without blocking     *)
(*     (`ConnCore::flush_reports`); a turn offers them at the latest when  *)
(*     BURST of them wait;                                                 *)
(*   - the receipts the lane refused stay in `StalledState::reports`, and  *)
(*     no frame is taken while one is there; the frames already in         *)
(*     progress go on;                                                     *)
(*   - one write may carry several frames that have no expiry              *)
(*     (`ConnCore::pop_outgoing_batch`): they complete or fail together;   *)
(*   - a receipt refused for RESULT_WAIT ends the task                     *)
(*     ("write_result_queue_full"), as a write that exceeds WRITE_TIMEOUT  *)
(*     does.                                                               *)
(*                                                                         *)
(* A frame gets Started at its first byte when it carries an expiry, and   *)
(* one terminal receipt: NotStarted (expired with no byte sent), Complete  *)
(* or Failed. A task that ends runs `ConnCore::finish_task`, in two        *)
(* critical sections. Under the output lock the queue is sealed            *)
(* (`accepting_frames`) and emptied: no frame is accepted afterwards.      *)
(* Under the lock of the lanes towards the core, the refused receipts are  *)
(* offered once, in order, then Failed for the frames in progress and for  *)
(* the frames that were in the queue; what the lane has no room for is     *)
(* dropped. A frame the task took and could not start (a bad expiry        *)
(* offset, a TLS frame that cannot be sealed) is a frame in progress.      *)
(* `retire` sets `Link::finished` after that.                              *)
(*                                                                         *)
(* The core removes a frame from `Runtime::frames` when it reads its       *)
(* terminal receipt. `finish_close` fails every frame the connection       *)
(* still has there and removes the peer, whose lanes are never read        *)
(* again: it covers a receipt the full lane refused at the end.            *)
(*                                                                         *)
(* WireProgress, the input lane, uncorrelated frames and the scheduling of *)
(* the task are not here: a task step may happen at any time. A full input *)
(* lane is backpressure and ends no task (record 0046, model MLaneOwed).   *)
(* The history of the verifier (prefix h) is read by no guard.             *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    NumFrames,  \* correlated frames the core has for this connection
    OutSlots,   \* capacity of the output queue (OUTPUT_SLOTS)
    WrittenCap, \* capacity of the receipt lane (WRITTEN_SLOTS)
    Burst,      \* receipts of a turn that force an offer (BURST)
    Coalesce,   \* frames one write may carry (MAX_COALESCE_FRAMES)
    Writable,   \* the socket: "always", "never", or "any" (it changes)
    Preserve,   \* the core waits for the task's facts before it closes
    Mutation    \* "none" or the name of a model mutation

Mutations == {"none", "DoubleReceipt", "LostReceiptOnClose", "NoCloseSweep",
              "FinishedFirst", "TakeWhilePending", "FlushNewestFirst",
              "DropUnsealedFrame", "NoSealAtFinish"}

ASSUME /\ NumFrames \in Nat \ {0} /\ OutSlots \in Nat \ {0}
       /\ WrittenCap \in Nat \ {0}
       /\ Burst \in Nat \ {0} /\ Coalesce \in Nat \ {0}
       /\ Writable \in {"always", "never", "any"}
       /\ Preserve \in BOOLEAN
       /\ Mutation \in Mutations

Frames == 1..NumFrames
Terminals == {"NotStarted", "Complete", "Failed"}
Receipts == [frame : Frames, kind : Terminals \cup {"Started"}]

VARIABLES
    next,       \* first frame the core has not submitted
    pendingF,   \* Runtime::frames: accepted frames without a terminal result
    result,     \* per frame: the terminal result the core queued, or "none"
    peer,       \* the connection is in Runtime::peers
    retire,     \* Peer::retire_code is set
    fault,      \* the core read a terminal receipt it cannot correlate
    cancelled,  \* ConnCore::cancelled, set by the core
    accepting,  \* OutgoingState::accepting_frames
    out,        \* OutgoingState::queue
    written,    \* ToCoreState::written
    tpc,        \* the task: "run", "finSeal", "finPublish", "finFlag", "done"
    active,     \* the frames in progress: one write, in queue order
    phase,      \* of the frames in progress: "new", "started", "written"
    reports,    \* receipts not in the lane yet, oldest first
    stalledN,   \* how many of them the lane refused: StalledState::reports
    finished,   \* Link::finished
    writable,   \* the socket accepts bytes
    hProduced,  \* per frame: terminal receipts the task produced
    hFact,      \* per frame: the terminal receipt published into the lane
    hLateStart, \* a Started was published after the terminal of its frame
    hAfterEnd,  \* a receipt was published after `finished` was set
    hRefused    \* the full lane refused a receipt of the ending task

coreVars == <<next, pendingF, result, peer, retire, fault>>
taskVars == <<tpc, active, phase, reports, stalledN, finished>>
histVars == <<hProduced, hFact, hLateStart, hAfterEnd, hRefused>>
vars == <<coreVars, cancelled, accepting, out, written, taskVars, writable, histVars>>

Accepted == 1..(next - 1)
Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
\* The receipts of the lane before its first Failed have been read.
FactsRead == IF written = <<>> THEN TRUE ELSE Head(written).kind = "Failed"
Free == WrittenCap - Len(written)
Receipt(f, k) == [frame |-> f, kind |-> k]
ReceiptsFor(s, k) == [i \in 1..Len(s) |-> Receipt(s[i], k)]

Init ==
    /\ next = 1 /\ pendingF = {} /\ result = [f \in Frames |-> "none"]
    /\ peer = TRUE /\ retire = FALSE /\ fault = FALSE
    /\ cancelled = FALSE /\ accepting = TRUE
    /\ out = <<>> /\ written = <<>>
    /\ tpc = "run" /\ active = <<>> /\ phase = "new"
    /\ reports = <<>> /\ stalledN = 0
    /\ finished = FALSE
    /\ writable = (Writable # "never")
    /\ hProduced = [f \in Frames |-> 0] /\ hFact = [f \in Frames |-> "none"]
    /\ hLateStart = FALSE /\ hAfterEnd = FALSE /\ hRefused = FALSE

\* The task puts the receipts `pub` into the lane, in order, under one
\* lock: nothing of the core happens between two of them.
Publish(pub) ==
    /\ written' = written \o pub
    /\ hFact' = [f \in Frames |->
                    LET at == {i \in 1..Len(pub) :
                                  pub[i].frame = f /\ pub[i].kind \in Terminals}
                    IN IF at = {} THEN hFact[f]
                       ELSE pub[CHOOSE i \in at : \A j \in at : j <= i].kind]
    /\ hLateStart' =
          (hLateStart \/ \E i \in 1..Len(pub) :
              /\ pub[i].kind = "Started"
              /\ \/ hFact[pub[i].frame] # "none"
                 \/ \E j \in 1..(i - 1) :
                       pub[j].frame = pub[i].frame /\ pub[j].kind \in Terminals)
    /\ hAfterEnd' = (hAfterEnd \/ (finished /\ Len(pub) > 0))

\* The task produced a terminal receipt for each frame of `s`.
Produced(s) ==
    hProduced' = [f \in Frames |->
                     IF \E i \in 1..Len(s) : s[i] = f THEN hProduced[f] + 1
                     ELSE hProduced[f]]

(***************************************************************************)
(* The core.                                                               *)
(***************************************************************************)

\* Sink::try_submit: `try_send_outgoing`, then `frames.insert`. A full
\* queue refuses with Full and the effect stays in the outbox: the absence
\* of this step. A peer that is closing gets no frame. A sealed queue
\* answers Disconnected: the core then closes the connection, which is
\* CoreClose.
CoreSubmit ==
    /\ peer /\ ~retire /\ next <= NumFrames
    /\ accepting /\ Len(out) < OutSlots
    /\ out' = Append(out, next)
    /\ pendingF' = pendingF \cup {next}
    /\ next' = next + 1
    /\ UNCHANGED <<result, peer, retire, fault, cancelled, accepting, written,
                   taskVars, writable, histVars>>

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
    /\ UNCHANGED <<next, peer, accepting, out, taskVars, writable, histVars>>

\* Runtime::close for a reason of the core's own: a deadline, a protocol
\* error, a takeover, a submission the sealed queue refused.
\* `Connection::close` cancels the task.
CoreClose ==
    /\ peer /\ ~retire
    /\ retire' = TRUE /\ cancelled' = TRUE
    /\ UNCHANGED <<next, pendingF, result, peer, fault, accepting, out, written,
                   taskVars, writable, histVars>>

\* The core learns that the task ended from its input lane: `finish_task`
\* left `Incoming::Closed` there, or the lane answers Disconnected.
CoreObserveEnd ==
    /\ peer /\ ~retire /\ tpc \in {"finFlag", "done"}
    /\ retire' = TRUE /\ cancelled' = TRUE
    /\ UNCHANGED <<next, pendingF, result, peer, fault, accepting, out, written,
                   taskVars, writable, histVars>>

\* finish_close: every frame the connection still has in the table gets
\* Failed and the peer is removed; its lanes are not read again. With
\* Preserve (a correlated PUBLISH above QoS 0 is pending:
\* `must_preserve_write_facts`) the core closes at once only if reading
\* the lane left it no frame; otherwise it waits for `is_finished` and
\* reads the lane up to its first Failed. NoCloseSweep removes the peer
\* and leaves the frames.
CoreFinishClose ==
    /\ peer /\ retire
    /\ Preserve => pendingF = {} \/ (finished /\ FactsRead)
    /\ peer' = FALSE
    /\ IF Mutation = "NoCloseSweep"
       THEN UNCHANGED <<pendingF, result>>
       ELSE /\ pendingF' = {}
            /\ result' = [f \in Frames |->
                             IF f \in pendingF
                             THEN (IF result[f] = "none" THEN "Failed" ELSE "twice")
                             ELSE result[f]]
    /\ UNCHANGED <<next, retire, fault, cancelled, accepting, out, written, taskVars,
                   writable, histVars>>

(***************************************************************************)
(* The task, while it runs.                                                *)
(***************************************************************************)

\* pop_outgoing_batch: one frame, or the first frames of the queue for one
\* write. No frame is taken while a receipt the lane refused is pending,
\* and a turn that holds Burst receipts offers them first.
\* TakeWhilePending ignores the refused receipts.
TaskTake ==
    /\ tpc = "run" /\ active = <<>> /\ out # <<>>
    /\ stalledN = 0 \/ Mutation = "TakeWhilePending"
    /\ Len(reports) - stalledN < Burst
    /\ \E n \in 1..Min(Coalesce, Len(out)) :
          /\ active' = SubSeq(out, 1, n)
          /\ out' = SubSeq(out, n + 1, Len(out))
    /\ phase' = "new"
    /\ UNCHANGED <<coreVars, cancelled, accepting, written, tpc, reports, stalledN,
                   finished, writable, histVars>>

\* prepare_expiry before the first byte: the frame is disposed of with
\* NotStarted and the connection goes on. A frame with an expiry is
\* written alone.
TaskNotStarted ==
    /\ tpc = "run" /\ Len(active) = 1 /\ phase = "new"
    /\ reports' = Append(reports, Receipt(active[1], "NotStarted"))
    /\ Produced(active)
    /\ active' = <<>>
    /\ UNCHANGED <<coreVars, cancelled, accepting, out, written, tpc, phase, stalledN,
                   finished, writable, hFact, hLateStart, hAfterEnd, hRefused>>

\* The first bytes leave. A correlated frame with an expiry reports
\* Started.
TaskStart ==
    /\ tpc = "run" /\ active # <<>> /\ phase = "new" /\ writable
    /\ phase' = "started"
    /\ \E timed \in BOOLEAN :
          /\ timed => Len(active) = 1
          /\ reports' = IF timed THEN Append(reports, Receipt(active[1], "Started"))
                        ELSE reports
    /\ UNCHANGED <<coreVars, cancelled, accepting, out, written, tpc, active, stalledN,
                   finished, writable, histVars>>

\* The last byte left: Complete is queued for each frame of the write and
\* none is in progress any more. DoubleReceipt queues Complete and keeps
\* the frames in place until TaskRelease.
TaskComplete ==
    /\ tpc = "run" /\ active # <<>> /\ phase = "started" /\ writable
    /\ reports' = reports \o ReceiptsFor(active, "Complete")
    /\ Produced(active)
    /\ IF Mutation = "DoubleReceipt"
       THEN phase' = "written" /\ UNCHANGED active
       ELSE active' = <<>> /\ UNCHANGED phase
    /\ UNCHANGED <<coreVars, cancelled, accepting, out, written, tpc, stalledN, finished,
                   writable, hFact, hLateStart, hAfterEnd, hRefused>>

\* Only DoubleReceipt reaches "written".
TaskRelease ==
    /\ tpc = "run" /\ active # <<>> /\ phase = "written"
    /\ active' = <<>>
    /\ UNCHANGED <<coreVars, cancelled, accepting, out, written, tpc, phase, reports,
                   stalledN, finished, writable, histVars>>

\* flush_reports: the pending receipts are offered oldest first, under one
\* lock. Those the lane has room for are published; the others are now
\* receipts the lane refused. FlushNewestFirst offers the newest first.
TaskFlush ==
    /\ tpc = "run" /\ reports # <<>>
    /\ LET n == Len(reports)
           k == Min(Free, n)
       IN /\ k > 0 \/ stalledN < n
          /\ IF Mutation = "FlushNewestFirst"
             THEN /\ Publish([i \in 1..k |-> reports[n + 1 - i]])
                  /\ reports' = SubSeq(reports, 1, n - k)
             ELSE /\ Publish(SubSeq(reports, 1, k))
                  /\ reports' = SubSeq(reports, k + 1, n)
          /\ stalledN' = n - k
    /\ UNCHANGED <<coreVars, cancelled, accepting, out, tpc, active, phase, finished,
                   writable, hProduced, hRefused>>

\* The task ends: `turn` returned an error and the reactor retires it.
\* FinishedFirst sets `Link::finished` before the terminal facts.
End ==
    /\ tpc' = "finSeal"
    /\ finished' = (Mutation = "FinishedFirst")
    /\ UNCHANGED <<coreVars, cancelled, accepting, out, written, active, phase, reports,
                   stalledN, writable, histVars>>

\* `is_cancelled()` at the start of a turn or before a write.
TaskCancelled == tpc = "run" /\ cancelled /\ End

\* A deadline the task holds: WRITE_TIMEOUT of the write in progress, or
\* RESULT_WAIT of a receipt the lane refused.
TaskDeadline ==
    /\ tpc = "run"
    /\ active # <<>> \/ stalledN > 0
    /\ End

\* End of file, a read or write error, a malformed packet: at any time.
TaskTransportError == tpc = "run" /\ End

\* The frames the task took cannot be started: `prepare_expiry` returns
\* Failed in `write_attempt`, or `next_write` of the TLS task cannot seal
\* them. The error ends the task and the frames stay in progress
\* (`Task::active`, `Machine::unsealed`), which `finish_task` fails.
\* DropUnsealedFrame forgets them, as the TLS task did when it held the
\* frame in a local variable.
TaskSealFails ==
    /\ tpc = "run" /\ active # <<>> /\ phase = "new"
    /\ active' = IF Mutation = "DropUnsealedFrame" THEN <<>> ELSE active
    /\ tpc' = "finSeal"
    /\ finished' = (Mutation = "FinishedFirst")
    /\ UNCHANGED <<coreVars, cancelled, accepting, out, written, phase, reports,
                   stalledN, writable, histVars>>

(***************************************************************************)
(* ConnCore::finish_task and retire.                                       *)
(***************************************************************************)

\* Under the output lock: the queue accepts nothing more and its frames
\* join the frames in progress, after them. NoSealAtFinish leaves the
\* queue open, as the channel of the earlier transport was until its
\* receiver was dropped. LostReceiptOnClose empties the queue and forgets
\* its frames.
FinishSeal ==
    /\ tpc = "finSeal"
    /\ accepting' = (Mutation = "NoSealAtFinish")
    /\ active' = IF Mutation = "LostReceiptOnClose" THEN active ELSE active \o out
    /\ out' = <<>>
    /\ tpc' = "finPublish"
    /\ UNCHANGED <<coreVars, cancelled, written, phase, reports, stalledN, finished,
                   writable, histVars>>

\* Under the lock of the lanes: the pending receipts, in order, then
\* Failed for each frame in progress or taken from the queue, each offered
\* once. What the lane has no room for is dropped.
FinishPublish ==
    /\ tpc = "finPublish"
    /\ LET kept == Min(Free, Len(reports))
           failed == SubSeq(active, 1, Min(Free - kept, Len(active)))
       IN /\ Publish(SubSeq(reports, 1, kept) \o ReceiptsFor(failed, "Failed"))
          /\ Produced(failed)
          /\ hRefused' = (hRefused \/ kept < Len(reports) \/ Len(failed) < Len(active))
    /\ reports' = <<>> /\ stalledN' = 0 /\ active' = <<>>
    /\ tpc' = "finFlag"
    /\ UNCHANGED <<coreVars, cancelled, accepting, out, phase, finished, writable>>

\* retire: `finished.store(true)` once `finish` returned.
FinishFlag ==
    /\ tpc = "finFlag"
    /\ finished' = TRUE
    /\ tpc' = "done"
    /\ UNCHANGED <<coreVars, cancelled, accepting, out, written, active, phase, reports,
                   stalledN, writable, histVars>>

\* The peer starts or stops reading.
SocketChanges ==
    /\ Writable = "any"
    /\ writable' = ~writable
    /\ UNCHANGED <<coreVars, cancelled, accepting, out, written, taskVars, histVars>>

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
    \/ FinishSeal
    \/ FinishPublish
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
    /\ WF_vars(FinishSeal) /\ WF_vars(FinishPublish) /\ WF_vars(FinishFlag)
    /\ WF_vars(CoreCollect) /\ WF_vars(CoreObserveEnd) /\ WF_vars(CoreFinishClose)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

TypeOK ==
    /\ next \in 1..(NumFrames + 1)
    /\ pendingF \subseteq Frames
    /\ result \in [Frames -> Terminals \cup {"none", "twice"}]
    /\ peer \in BOOLEAN /\ retire \in BOOLEAN /\ fault \in BOOLEAN
    /\ cancelled \in BOOLEAN /\ accepting \in BOOLEAN
    /\ finished \in BOOLEAN /\ writable \in BOOLEAN
    /\ out \in Seq(Frames) /\ active \in Seq(Frames)
    /\ written \in Seq(Receipts) /\ reports \in Seq(Receipts)
    /\ stalledN \in 0..Len(reports)
    /\ tpc \in {"run", "finSeal", "finPublish", "finFlag", "done"}
    /\ phase \in {"new", "started", "written"}
    /\ hProduced \in [Frames -> 0..2]
    /\ hFact \in [Frames -> Terminals \cup {"none"}]
    /\ hLateStart \in BOOLEAN /\ hAfterEnd \in BOOLEAN /\ hRefused \in BOOLEAN

\* The queue and the lane never hold more than their capacity.
LanesFit == Len(out) <= OutSlots /\ Len(written) <= WrittenCap

\* What the task holds outside the lane is bounded: the receipts of a
\* turn below Burst, and those of one more write.
ReportsBounded == Len(reports) <= (Burst - 1) + Max(Coalesce, 2)

\* The task produces at most one terminal receipt for a frame.
AtMostOneTerminal == \A f \in Frames : hProduced[f] <= 1

\* Started is published only before the terminal receipt of its frame.
StartedBeforeTerminal == ~hLateStart

\* Nothing is published once `Link::finished` reads true: a core that
\* sees it has every terminal fact of the task in its lanes.
NothingAfterFinished == ~hAfterEnd

\* A task that ended with room in the receipt lane left a terminal
\* receipt for every accepted frame.
TerminalWhenRoom ==
    (finished /\ ~hRefused) => \A f \in Accepted : hProduced[f] = 1

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

\* The output queue is full while the core has another frame.
NoBackpressureWitness ==
    ~(peer /\ ~retire /\ accepting /\ next <= NumFrames /\ Len(out) = OutSlots)

\* The queue is sealed while the core has another frame for a peer it has
\* not closed: the submission is refused, not accepted and lost.
NoSealedRefusalWitness == ~(peer /\ ~retire /\ ~accepting /\ next <= NumFrames)

\* One write carries two frames.
NoCoalescedWriteWitness == Len(active) <= 1

\* The receipts of two frames wait together outside the lane.
OneFrameOutsideLaneWitness ==
    \A i, j \in 1..Len(reports) : reports[i].frame = reports[j].frame

\* The receipt lane refused a receipt of the ending task.
NoRefusedAtEndWitness == ~hRefused

\* One frame was written and another failed.
NoMixedOutcomeWitness ==
    ~\E f, g \in Frames : result[f] = "Complete" /\ result[g] = "Failed"

=============================================================================
