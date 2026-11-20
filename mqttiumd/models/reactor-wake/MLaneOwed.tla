----------------------------- MODULE MLaneOwed -----------------------------
(***************************************************************************)
(* MLaneOwed: a transport task that finds a lane towards the core full,    *)
(* as written in crates/mqttiumd/src/net.rs (`publish`, `Lanes::deliver`,  *)
(* `Lanes::flush`) and net/reactor.rs (`Link::owe`, `Link::consumed`).     *)
(* The task never blocks. It offers its value twice around a mark:         *)
(*                                                                         *)
(*   lane.try_send(value)               TaskTry1                           *)
(*   link.owe()          owed = true    TaskOwe                            *)
(*   lane.try_send(value)               TaskTry2                           *)
(*                                                                         *)
(* A value refused twice is held with the instant of its first refusal;    *)
(* held longer than INPUT_WAIT (RESULT_WAIT for a receipt) it ends the     *)
(* task with its queue-full code. The consumer that takes a value runs     *)
(* the task if the mark is set (`Inbound::try_recv`):                      *)
(*                                                                         *)
(*   receiver.try_recv()                ConsumerTake                       *)
(*   if link.owed.swap(false) {         ConsumerConsumed                   *)
(*       link.notify() }                                                   *)
(*                                                                         *)
(* A task that holds a value reports `waiting` and the reactor runs it     *)
(* again at every TICK, so that its deadline is checked without a          *)
(* consumer. `scheduled` stands for a notification on its way to the       *)
(* task; MReactorWake shows that it runs it. Socket readiness is the       *)
(* arrival of a new value. One lane stands for the three that share the    *)
(* mark: a run that still finds a lane full marks it again. A lane is the  *)
(* count sent - recv. No guard reads a history variable: there is none.    *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    NumValues, \* values the task has to publish in a run
    LaneCap,   \* capacity of the lane (INPUT_SLOTS, WRITTEN_SLOTS)
    Tick,      \* the reactor runs a task that holds a value at every TICK
    Mutation   \* "none" or the name of a model mutation

Mutations == {"none", "SingleAttempt"}

ASSUME /\ NumValues \in Nat \ {0} /\ LaneCap \in Nat \ {0}
       /\ Tick \in BOOLEAN
       /\ Mutation \in Mutations

VARIABLES
    made,      \* values the task produced so far
    sent,      \* values published into the lane
    recv,      \* values the consumer took
    hand,      \* the value in the task's hands: "none", "new", "held"
    expired,   \* the wait bound of the value in hand has elapsed
    owed,      \* Link::owed
    scheduled, \* a notification is on its way to the task
    ended,     \* the task ended with its queue-full code
    tpc,       \* where the task is in its turn
    cpc        \* where the consumer is

vars == <<made, sent, recv, hand, expired, owed, scheduled, ended, tpc, cpc>>

LaneLen == sent - recv
Full == LaneLen = LaneCap

Init ==
    /\ made = 0 /\ sent = 0 /\ recv = 0
    /\ hand = "none" /\ expired = FALSE
    /\ owed = FALSE /\ scheduled = FALSE /\ ended = FALSE
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

\* The value came back twice. `since.elapsed() >= INPUT_WAIT` ends the
\* task with "input_queue_full" ("write_result_queue_full" for a receipt);
\* before that the value is held and the turn ends.
Refused ==
    /\ IF expired
       THEN ended' = TRUE /\ hand' = "none"
       ELSE hand' = "held" /\ UNCHANGED ended
    /\ tpc' = "idle"
    /\ UNCHANGED sent

\* Socket readiness: the task parses a packet, holds it (`Lanes::hold`
\* records the instant) and offers it in the same turn.
TaskProduce ==
    /\ tpc = "idle" /\ hand = "none" /\ ~ended /\ made < NumValues
    /\ made' = made + 1
    /\ hand' = "new" /\ expired' = FALSE
    /\ tpc' = "try1"
    /\ UNCHANGED <<sent, recv, owed, scheduled, ended, cpc>>

\* The reactor runs the task: a notification arrived, or the task holds a
\* value and the tick is due. A run with nothing held does nothing.
TaskRun ==
    /\ tpc = "idle"
    /\ scheduled \/ (Tick /\ hand = "held")
    /\ scheduled' = FALSE
    /\ tpc' = IF hand = "held" THEN "try1" ELSE "idle"
    /\ UNCHANGED <<made, sent, recv, hand, expired, owed, ended, cpc>>

\* The first attempt of `publish`.
TaskTry1 ==
    /\ tpc = "try1"
    /\ IF ~Full
       THEN Published
       ELSE tpc' = "owe" /\ UNCHANGED <<sent, hand, ended>>
    /\ UNCHANGED <<made, recv, expired, owed, scheduled, cpc>>

\* `link.owe()`. SingleAttempt gives up here, without the second attempt.
TaskOwe ==
    /\ tpc = "owe"
    /\ owed' = TRUE
    /\ IF Mutation = "SingleAttempt"
       THEN Refused
       ELSE tpc' = "try2" /\ UNCHANGED <<sent, hand, ended>>
    /\ UNCHANGED <<made, recv, expired, scheduled, cpc>>

\* The second attempt covers a consumer that took a value just before
\* the mark and so did not see it.
TaskTry2 ==
    /\ tpc = "try2"
    /\ IF ~Full THEN Published ELSE Refused
    /\ UNCHANGED <<made, recv, expired, owed, scheduled, cpc>>

\* Time passes: the wait bound of the value in hand elapses.
DeadlineElapses ==
    /\ hand # "none" /\ ~expired
    /\ expired' = TRUE
    /\ UNCHANGED <<made, sent, recv, hand, owed, scheduled, ended, tpc, cpc>>

(***************************************************************************)
(* The consumer: the core, `Inbound::try_recv`.                            *)
(***************************************************************************)

ConsumerTake ==
    /\ cpc = "idle" /\ LaneLen > 0
    /\ recv' = recv + 1
    /\ cpc' = "took"
    /\ UNCHANGED <<made, sent, hand, expired, owed, scheduled, ended, tpc>>

\* `link.consumed()`: `owed.swap(false)`, and `notify()` if it was set.
ConsumerConsumed ==
    /\ cpc = "took"
    /\ owed' = FALSE
    /\ scheduled' = (scheduled \/ owed)
    /\ cpc' = "idle"
    /\ UNCHANGED <<made, sent, recv, hand, expired, ended, tpc>>

\* The end of a run: the task published every value or ended, the lane is
\* empty and nothing is on its way. Any other state without a step is a
\* deadlock that TLC reports.
Terminal ==
    /\ tpc = "idle" /\ cpc = "idle" /\ hand = "none"
    /\ made = NumValues \/ ended
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
    \/ Quiescent

Spec == Init /\ [][Next]_vars

TaskStep == TaskRun \/ TaskTry1 \/ TaskOwe \/ TaskTry2
ConsumerStep == ConsumerTake \/ ConsumerConsumed

\* The reactor runs the task when it must and time passes. Nothing is
\* assumed about the consumer: the core may be stalled.
FairSpec ==
    /\ Spec
    /\ WF_vars(TaskStep)
    /\ WF_vars(DeadlineElapses)

\* The consumer takes what the lane holds. Nothing is assumed about time:
\* the wait bound may never elapse.
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
    /\ scheduled \in BOOLEAN /\ ended \in BOOLEAN
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
EndedOnlyAfterDeadline == ended => expired

(***************************************************************************)
(* Progress: a value the lane refused is published, or the task ends with  *)
(* its queue-full code. Under FairSpec this holds through the tick with a  *)
(* stalled consumer; under FairConsumerSpec it holds without tick or       *)
(* deadline, through `owe` and `consumed` alone.                           *)
(***************************************************************************)
HeldEndsOrPublishes == (hand = "held") ~> (hand = "none")

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
NoQueueFullEndWitness == ~ended

=============================================================================
