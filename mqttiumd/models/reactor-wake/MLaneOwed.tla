----------------------------- MODULE MLaneOwed -----------------------------
(***************************************************************************)
(* MLaneOwed: a transport task that finds a lane towards the core full,    *)
(* as written in crates/mqttiumd/src/net.rs (for packets,                  *)
(* `ConnCore::push_incoming_batch`, `ConnCore::flush_held_incoming` and    *)
(* `ConnCore::push_incoming_one`; for receipts, `ConnCore::flush_reports`) *)
(* and net/reactor.rs (`Link::owe`, `Link::consumed`). The task never      *)
(* blocks. Each of those functions is the same loop of two attempts        *)
(* around a mark:                                                          *)
(*                                                                         *)
(*   lock; push what the lane has room for   TaskTry1                      *)
(*   link.owe()          owed = true         TaskOwe                       *)
(*   lock; push what the lane has room for   TaskTry2                      *)
(*                                                                         *)
(* The functions move batches; one value here stands for what a batch      *)
(* still holds after an attempt. What was refused twice is held (in        *)
(* `StalledState`). The two kinds of value differ in what                  *)
(* follows (decision record 0046):                                         *)
(*                                                                         *)
(*   - a packet for the input lane is held with no bound. The task reads   *)
(*     nothing more from its socket; the peer is slowed by its transport.  *)
(*     Every deadline of the connection stays with the runtime, which      *)
(*     cancels the task when one fires;                                    *)
(*   - a receipt is held with the instant of its first refusal; held for   *)
(*     RESULT_WAIT it ends the task ("write_result_queue_full",            *)
(*     "wire_progress_unavailable").                                       *)
(*                                                                         *)
(* The consumer that makes room in a lane runs the task if the mark is     *)
(* set. ConsumerTake is the step that makes the room visible to the task:  *)
(* for packets, the end of the batch the core borrowed                     *)
(* (`IncomingBatch::finish`, `incoming_borrowed.store(0)`: a borrowed      *)
(* packet keeps its place until then); for receipts, the pop under the     *)
(* lock of `ConnCore::drain_written` and `ConnCore::drain_wire_progress`.  *)
(* `Link::consumed` follows it:                                            *)
(*                                                                         *)
(*   room appears in the lane           ConsumerTake                       *)
(*   if link.owed.swap(false) {         ConsumerConsumed                   *)
(*       link.notify() }                                                   *)
(*                                                                         *)
(* A task that holds a value reports `waiting` and the reactor runs it     *)
(* again at every TICK. `Connection::close` cancels the task and notifies  *)
(* its link. `scheduled` stands for a notification on its way to the       *)
(* task; MReactorWake shows that it runs it. Socket readiness is the       *)
(* arrival of a new value. One lane stands for the three that share the    *)
(* mark: a run that still finds a lane full marks it again. A lane is the  *)
(* count sent - recv. No guard reads a history variable: there is none.    *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    NumValues, \* values the task has to publish in a run
    LaneCap,   \* capacity of the lane (INPUT_SLOTS, WRITTEN_SLOTS)
    Bounded,   \* the values are receipts, with a wait bound; FALSE: packets
    Tick,      \* the reactor runs a task that holds a value at every TICK
    Mutation   \* "none" or the name of a model mutation

Mutations == {"none", "SingleAttempt", "InputDeadline"}

ASSUME /\ NumValues \in Nat \ {0} /\ LaneCap \in Nat \ {0}
       /\ Bounded \in BOOLEAN /\ Tick \in BOOLEAN
       /\ Mutation \in Mutations

VARIABLES
    made,      \* values the task produced so far
    sent,      \* values published into the lane
    recv,      \* values the consumer took
    hand,      \* the value in the task's hands: "none", "new", "held"
    expired,   \* the wait bound of the value in hand has elapsed
    owed,      \* Link::owed
    scheduled, \* a notification is on its way to the task
    cancelled, \* ConnCore::cancelled
    ended,     \* how the task ended: "none", "queue_full", "cancelled"
    tpc,       \* where the task is in its turn
    cpc        \* where the consumer is

vars == <<made, sent, recv, hand, expired, owed, scheduled, cancelled, ended, tpc, cpc>>

LaneLen == sent - recv
Full == LaneLen = LaneCap
\* The value in hand has a wait bound. InputDeadline gives one to a
\* packet, as the task did before record 0046.
HasBound == Bounded \/ Mutation = "InputDeadline"

Init ==
    /\ made = 0 /\ sent = 0 /\ recv = 0
    /\ hand = "none" /\ expired = FALSE
    /\ owed = FALSE /\ scheduled = FALSE
    /\ cancelled = FALSE /\ ended = "none"
    /\ tpc = "idle" /\ cpc = "idle"

(***************************************************************************)
(* The task.                                                               *)
(***************************************************************************)

\* The lane took the value.
Published ==
    /\ sent' = sent + 1
    /\ hand' = "none"
    /\ tpc' = "idle"
    /\ UNCHANGED ended

\* The value came back twice. A receipt whose bound elapsed ends the task
\* with its queue-full code; any other value is held and the turn ends.
Refused ==
    /\ IF expired
       THEN ended' = "queue_full" /\ hand' = "none"
       ELSE hand' = "held" /\ UNCHANGED ended
    /\ tpc' = "idle"
    /\ UNCHANGED sent

\* Socket readiness, or the end of a write: the task has a new value and
\* offers it in the same turn. A task that holds a packet takes no other.
TaskProduce ==
    /\ tpc = "idle" /\ hand = "none" /\ ended = "none" /\ ~cancelled
    /\ made < NumValues
    /\ made' = made + 1
    /\ hand' = "new" /\ expired' = FALSE
    /\ tpc' = "try1"
    /\ UNCHANGED <<sent, recv, owed, scheduled, cancelled, ended, cpc>>

\* The reactor runs the task: a notification arrived, or the task holds a
\* value and the tick is due. A cancelled task ends; a run with nothing
\* held does nothing.
TaskRun ==
    /\ tpc = "idle"
    /\ scheduled \/ (Tick /\ hand = "held" /\ ended = "none")
    /\ scheduled' = FALSE
    /\ IF ended = "none" /\ cancelled
       THEN ended' = "cancelled" /\ hand' = "none" /\ UNCHANGED tpc
       ELSE /\ tpc' = IF ended = "none" /\ hand = "held" THEN "try1" ELSE "idle"
            /\ UNCHANGED <<hand, ended>>
    /\ UNCHANGED <<made, sent, recv, expired, owed, cancelled, cpc>>

\* The first attempt of the loop.
TaskTry1 ==
    /\ tpc = "try1"
    /\ IF ~Full
       THEN Published
       ELSE tpc' = "owe" /\ UNCHANGED <<sent, hand, ended>>
    /\ UNCHANGED <<made, recv, expired, owed, scheduled, cancelled, cpc>>

\* `link.owe()`. SingleAttempt gives up here, without the second attempt.
TaskOwe ==
    /\ tpc = "owe"
    /\ owed' = TRUE
    /\ IF Mutation = "SingleAttempt"
       THEN Refused
       ELSE tpc' = "try2" /\ UNCHANGED <<sent, hand, ended>>
    /\ UNCHANGED <<made, recv, expired, scheduled, cancelled, cpc>>

\* The second attempt covers a consumer that took a value just before
\* the mark and so did not see it.
TaskTry2 ==
    /\ tpc = "try2"
    /\ IF ~Full THEN Published ELSE Refused
    /\ UNCHANGED <<made, recv, expired, owed, scheduled, cancelled, cpc>>

\* Time passes: the wait bound of a refused receipt elapses. A packet has
\* none.
DeadlineElapses ==
    /\ HasBound /\ hand # "none" /\ ~expired
    /\ expired' = TRUE
    /\ UNCHANGED <<made, sent, recv, hand, owed, scheduled, cancelled, ended, tpc, cpc>>

(***************************************************************************)
(* The core.                                                               *)
(***************************************************************************)

\* Room appears in the lane: `IncomingBatch::finish`, or the pop of
\* `ConnCore::drain_written` and `ConnCore::drain_wire_progress`.
ConsumerTake ==
    /\ cpc = "idle" /\ LaneLen > 0
    /\ recv' = recv + 1
    /\ cpc' = "took"
    /\ UNCHANGED <<made, sent, hand, expired, owed, scheduled, cancelled, ended, tpc>>

\* `link.consumed()`: `owed.swap(false)`, and `notify()` if it was set.
ConsumerConsumed ==
    /\ cpc = "took"
    /\ owed' = FALSE
    /\ scheduled' = (scheduled \/ owed)
    /\ cpc' = "idle"
    /\ UNCHANGED <<made, sent, recv, hand, expired, cancelled, ended, tpc>>

\* `Connection::close`, for a deadline the runtime holds (CONNECT, Keep
\* Alive) or any other reason: the task is cancelled and its link
\* notified.
CoreCancel ==
    /\ ~cancelled /\ ended = "none"
    /\ cancelled' = TRUE
    /\ scheduled' = TRUE
    /\ UNCHANGED <<made, sent, recv, hand, expired, owed, ended, tpc, cpc>>

\* The end of a run: the task published every value or ended, the lane is
\* empty and nothing is on its way. Any other state without a step is a
\* deadlock that TLC reports.
Terminal ==
    /\ tpc = "idle" /\ cpc = "idle" /\ hand = "none"
    /\ made = NumValues \/ ended # "none"
    /\ LaneLen = 0 /\ ~scheduled

Quiescent == Terminal /\ UNCHANGED vars

Next ==
    \/ TaskProduce
    \/ TaskRun
    \/ TaskTry1
    \/ TaskOwe
    \/ TaskTry2
    \/ DeadlineElapses
    \/ ConsumerTake
    \/ ConsumerConsumed
    \/ CoreCancel
    \/ Quiescent

Spec == Init /\ [][Next]_vars

TaskStep == TaskRun \/ TaskTry1 \/ TaskOwe \/ TaskTry2
ConsumerStep == ConsumerTake \/ ConsumerConsumed

\* The reactor runs the task when it must and time passes. Nothing is
\* assumed about the core: it may take nothing and cancel nothing.
FairSpec ==
    /\ Spec
    /\ WF_vars(TaskStep)
    /\ WF_vars(DeadlineElapses)

\* The core takes what the lane holds. Nothing is assumed about time: a
\* wait bound may never elapse.
FairConsumerSpec ==
    /\ Spec
    /\ WF_vars(TaskStep)
    /\ WF_vars(ConsumerStep)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

TypeOK ==
    /\ made \in 0..NumValues /\ sent \in 0..NumValues /\ recv \in 0..NumValues
    /\ hand \in {"none", "new", "held"}
    /\ expired \in BOOLEAN /\ owed \in BOOLEAN
    /\ scheduled \in BOOLEAN /\ cancelled \in BOOLEAN
    /\ ended \in {"none", "queue_full", "cancelled"}
    /\ tpc \in {"idle", "try1", "owe", "try2"}
    /\ cpc \in {"idle", "took"}

LaneBounded == recv <= sent /\ sent <= made /\ LaneLen <= LaneCap

\* A task that rests on a held value has its next run assured without the
\* tick: a notification is on its way, or the mark is set and either the
\* lane is still full, so that the next value taken raises it, or the
\* consumer took a value and has not looked at the mark yet.
NoLostOwed ==
    (hand = "held" /\ tpc = "idle") =>
        \/ scheduled
        \/ owed /\ Full
        \/ owed /\ cpc = "took"

\* The task ends with its queue-full code only once the wait bound of the
\* refused value has elapsed.
EndedOnlyAfterDeadline == ended = "queue_full" => expired

\* Input pressure never ends a task (record 0046): a full input lane is
\* backpressure, and only a receipt has a wait bound.
PacketNeverEndsTask == ~Bounded => ended # "queue_full"

\* A cancelled task has its run assured.
CancelRunsTask == (cancelled /\ ended = "none") => scheduled \/ tpc # "idle"

(***************************************************************************)
(* Progress.                                                               *)
(***************************************************************************)

\* A refused value does not stay held. For a receipt this holds under
\* FairSpec through the tick, whatever the core does: it is published or
\* the task ends with its queue-full code. For a packet it holds under
\* FairConsumerSpec, with no tick and no deadline, through `owe` and
\* `consumed` alone: it is delivered once the core takes values, or its
\* task is cancelled.
HeldEndsOrPublishes == (hand = "held") ~> (hand = "none")

\* A task the core cancelled ends, with no tick and whatever it holds.
CancelledEnds == cancelled ~> (ended # "none")

(***************************************************************************)
(* Non-vacuity, each the negation of an invariant TLC must report.         *)
(***************************************************************************)

\* The consumer took a value before the mark was set and raised nothing:
\* only the second attempt publishes the value.
NoSecondAttemptSavesWitness ==
    ~(tpc = "try2" /\ ~Full /\ owed /\ ~scheduled /\ cpc = "idle")

\* A consumed value raised a notification for a task that holds a value.
NoConsumedRunsTaskWitness == ~(hand = "held" /\ tpc = "idle" /\ scheduled)

\* The task ended with its queue-full code.
NoQueueFullEndWitness == ended # "queue_full"

\* A held packet is about to be delivered: the core took a value.
NoHeldPacketDeliveredWitness == ~(~Bounded /\ hand = "held" /\ tpc = "try1" /\ ~Full)

=============================================================================
