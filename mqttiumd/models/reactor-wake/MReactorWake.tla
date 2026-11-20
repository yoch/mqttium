---------------------------- MODULE MReactorWake ----------------------------
(***************************************************************************)
(* MReactorWake: how the core thread gets a reactor thread to run a        *)
(* transport task, as written in crates/mqttiumd/src/net/reactor.rs        *)
(* (ADR 0044). The core publishes a frame into the task's bounded lane     *)
(* and then notifies its link (`Outbound::try_send`, net.rs):              *)
(*                                                                         *)
(*   sender.try_send(frame)                 ProducerPublish                *)
(*   if !link.dirty.swap(true) {            ProducerSwap                   *)
(*       queue.lock().push(Run(link))       ProducerPush                   *)
(*       if sleeping.load() {               ProducerLoad                   *)
(*           waker.wake() } }               ProducerWake                   *)
(*                                                                         *)
(* The reactor thread loops (`run`):                                       *)
(*                                                                         *)
(*   messages = take(queue)                 ReactorTake                    *)
(*   for Run(link) in messages {            ReactorHandle                  *)
(*       link.dirty.store(false)                                           *)
(*       if !entry.queued { entry.queued = true; ready.push(slot) } }      *)
(*   while let Some(slot) = ready.pop() {   ReactorRun                     *)
(*       entry.queued = false; entry.task.turn() }                         *)
(*   sleeping.store(true)                   ReactorSleep                   *)
(*   pending = !queue.is_empty()            ReactorRecheck                 *)
(*   poll(if pending { 0 } else { timeout })   ReactorPoll                 *)
(*   sleeping.store(false)                  ReactorAwake                   *)
(*                                                                         *)
(* `waker` is the mio waker: a wake makes the poll in progress, or the     *)
(* next one, return, and that poll consumes it.                            *)
(*                                                                         *)
(* A turn of the task takes every frame its lane holds. Socket readiness,  *)
(* the tick of a waiting task and the timeout of the poll are absent: the  *)
(* model shows that a published frame depends on none of them. A lane is   *)
(* the count published - taken. No guard reads a history variable: there   *)
(* is none.                                                                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    NumLinks,     \* transport tasks of one reactor thread, each with a lane
    NumProducers, \* threads that publish and notify
    LaneCap,      \* capacity of a lane (OUTPUT_SLOTS)
    MaxPublish,   \* frames one producer publishes in a run
    Mutation      \* "none" or the name of a model mutation

Mutations == {"none", "CheckBeforePush", "PollWithoutRecheck", "ClearDirtyAfterRun"}

ASSUME /\ NumLinks \in Nat \ {0} /\ NumProducers \in Nat \ {0}
       /\ LaneCap \in Nat \ {0} /\ MaxPublish \in Nat \ {0}
       /\ Mutation \in Mutations

Links == 1..NumLinks
Producers == 1..NumProducers
NoLink == 0
MaxItems == NumProducers * MaxPublish

VARIABLES
    published,   \* per link: frames published into its lane
    taken,       \* per link: frames its task took
    dirty,       \* per link: Link::dirty
    queue,       \* per link: Run messages in Shared::queue
    batch,       \* per link: Run messages the reactor took and has not handled
    queued,      \* per link: Entry::queued, the task is in `ready`
    sleeping,    \* Shared::sleeping
    waker,       \* a wake the poll has not returned yet
    pendingSeen, \* the reactor's `pending`, read after it set `sleeping`
    rpc,         \* where the reactor is in its loop
    ppc,         \* per producer: where it is in its steps
    ptarget,     \* per producer: the link of the frame in hand
    sent         \* per producer: frames published so far

vars == <<published, taken, dirty, queue, batch, queued, sleeping, waker,
          pendingSeen, rpc, ppc, ptarget, sent>>

LaneLen(l) == published[l] - taken[l]
QueueEmpty == \A l \in Links : queue[l] = 0
None == [l \in Links |-> 0]

\* The reactor is inside a poll that nothing ends but a wake.
Blocked == rpc = "poll" /\ ~pendingSeen /\ ~waker

Init ==
    /\ published = None /\ taken = None
    /\ dirty = [l \in Links |-> FALSE]
    /\ queue = None /\ batch = None
    /\ queued = [l \in Links |-> FALSE]
    /\ sleeping = FALSE /\ waker = FALSE /\ pendingSeen = FALSE
    /\ rpc = "take"
    /\ ppc = [p \in Producers |-> "idle"]
    /\ ptarget = [p \in Producers |-> NoLink]
    /\ sent = [p \in Producers |-> 0]

Done(p) ==
    /\ ppc' = [ppc EXCEPT ![p] = "idle"]
    /\ ptarget' = [ptarget EXCEPT ![p] = NoLink]

(***************************************************************************)
(* Producers: `Outbound::try_send`, then `Link::notify` and                *)
(* `Shared::send`.                                                         *)
(***************************************************************************)

\* try_send: a full lane refuses and raises no notification; the refusal
\* is the absence of this step.
ProducerPublish(p, l) ==
    /\ ppc[p] = "idle" /\ sent[p] < MaxPublish /\ LaneLen(l) < LaneCap
    /\ published' = [published EXCEPT ![l] = @ + 1]
    /\ sent' = [sent EXCEPT ![p] = @ + 1]
    /\ ppc' = [ppc EXCEPT ![p] = "notify"]
    /\ ptarget' = [ptarget EXCEPT ![p] = l]
    /\ UNCHANGED <<taken, dirty, queue, batch, queued, sleeping, waker, pendingSeen, rpc>>

\* `dirty.swap(true)`: a link already marked sends nothing.
\* CheckBeforePush goes on to read `sleeping` before it pushes.
ProducerSwap(p) ==
    LET l == ptarget[p] IN
    /\ ppc[p] = "notify"
    /\ IF dirty[l]
       THEN Done(p) /\ UNCHANGED dirty
       ELSE /\ dirty' = [dirty EXCEPT ![l] = TRUE]
            /\ ppc' = [ppc EXCEPT ![p] =
                          IF Mutation = "CheckBeforePush" THEN "preload" ELSE "push"]
            /\ UNCHANGED ptarget
    /\ UNCHANGED <<published, taken, queue, batch, queued, sleeping, waker,
                   pendingSeen, rpc, sent>>

\* Only CheckBeforePush reaches "preload": it remembers `sleeping` as it
\* was before the message is in the queue.
ProducerPreload(p) ==
    /\ ppc[p] = "preload"
    /\ ppc' = [ppc EXCEPT ![p] = IF sleeping THEN "pushw" ELSE "pushn"]
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   pendingSeen, rpc, ptarget, sent>>

\* `queue.lock().push(Run(link))`.
ProducerPush(p) ==
    /\ ppc[p] \in {"push", "pushw", "pushn"}
    /\ queue' = [queue EXCEPT ![ptarget[p]] = @ + 1]
    /\ IF ppc[p] = "pushn"
       THEN Done(p)
       ELSE /\ ppc' = [ppc EXCEPT ![p] = IF ppc[p] = "push" THEN "load" ELSE "wake"]
            /\ UNCHANGED ptarget
    /\ UNCHANGED <<published, taken, dirty, batch, queued, sleeping, waker,
                   pendingSeen, rpc, sent>>

\* `sleeping.load()`, after the push.
ProducerLoad(p) ==
    /\ ppc[p] = "load"
    /\ IF sleeping
       THEN ppc' = [ppc EXCEPT ![p] = "wake"] /\ UNCHANGED ptarget
       ELSE Done(p)
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   pendingSeen, rpc, sent>>

\* `waker.wake()`.
ProducerWake(p) ==
    /\ ppc[p] = "wake"
    /\ waker' = TRUE
    /\ Done(p)
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping,
                   pendingSeen, rpc, sent>>

(***************************************************************************)
(* The reactor thread.                                                     *)
(***************************************************************************)

\* `std::mem::take(&mut *queue)`.
ReactorTake ==
    /\ rpc = "take"
    /\ batch' = queue /\ queue' = None
    /\ rpc' = "handle"
    /\ UNCHANGED <<published, taken, dirty, queued, sleeping, waker, pendingSeen,
                   ppc, ptarget, sent>>

\* Message::Run: the mark is cleared before the task is queued, so a
\* notification made while the task runs sends a new message.
\* ClearDirtyAfterRun keeps the mark until the tasks have run.
ReactorHandle ==
    /\ rpc = "handle"
    /\ queued' = [l \in Links |-> queued[l] \/ batch[l] > 0]
    /\ IF Mutation = "ClearDirtyAfterRun"
       THEN UNCHANGED <<dirty, batch>>
       ELSE /\ dirty' = [l \in Links |-> dirty[l] /\ batch[l] = 0]
            /\ batch' = None
    /\ rpc' = "run"
    /\ UNCHANGED <<published, taken, queue, sleeping, waker, pendingSeen,
                   ppc, ptarget, sent>>

\* One task of `ready` takes its turn: it takes every frame of its lane.
\* With `ready` empty the reactor goes on to sleep.
ReactorRun ==
    /\ rpc = "run"
    /\ IF \E l \in Links : queued[l]
       THEN /\ \E l \in Links :
                 /\ queued[l]
                 /\ queued' = [queued EXCEPT ![l] = FALSE]
                 /\ taken' = [taken EXCEPT ![l] = published[l]]
            /\ UNCHANGED rpc
       ELSE /\ rpc' = IF Mutation = "ClearDirtyAfterRun" THEN "lateclear" ELSE "sleep"
            /\ UNCHANGED <<queued, taken>>
    /\ UNCHANGED <<published, dirty, queue, batch, sleeping, waker, pendingSeen,
                   ppc, ptarget, sent>>

\* Only ClearDirtyAfterRun reaches "lateclear".
ReactorLateClear ==
    /\ rpc = "lateclear"
    /\ dirty' = [l \in Links |-> dirty[l] /\ batch[l] = 0]
    /\ batch' = None
    /\ rpc' = "sleep"
    /\ UNCHANGED <<published, taken, queue, queued, sleeping, waker, pendingSeen,
                   ppc, ptarget, sent>>

\* `sleeping.store(true)`. PollWithoutRecheck polls without looking at
\* the queue again.
ReactorSleep ==
    /\ rpc = "sleep"
    /\ sleeping' = TRUE
    /\ pendingSeen' = FALSE
    /\ rpc' = IF Mutation = "PollWithoutRecheck" THEN "poll" ELSE "recheck"
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, waker,
                   ppc, ptarget, sent>>

\* `pending = !queue.is_empty()`, read after `sleeping` is set.
ReactorRecheck ==
    /\ rpc = "recheck"
    /\ pendingSeen' = ~QueueEmpty
    /\ rpc' = "poll"
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   ppc, ptarget, sent>>

\* `poll`: it returns at once with a zero timeout or a wake already
\* delivered, and otherwise when a wake arrives. It consumes the wake.
ReactorPoll ==
    /\ rpc = "poll" /\ (pendingSeen \/ waker)
    /\ waker' = FALSE
    /\ rpc' = "awake"
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping,
                   pendingSeen, ppc, ptarget, sent>>

\* `sleeping.store(false)`.
ReactorAwake ==
    /\ rpc = "awake"
    /\ sleeping' = FALSE
    /\ rpc' = "take"
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, waker,
                   pendingSeen, ppc, ptarget, sent>>

\* The end of a run: every producer published its frames and finished its
\* steps, every lane is empty and the reactor is blocked in its poll with
\* an empty queue. Any other state without a step is a deadlock that TLC
\* reports.
Terminal ==
    /\ \A p \in Producers : ppc[p] = "idle" /\ sent[p] = MaxPublish
    /\ \A l \in Links : LaneLen(l) = 0 /\ ~queued[l] /\ batch[l] = 0
    /\ QueueEmpty /\ Blocked

Quiescent == Terminal /\ UNCHANGED vars

Next ==
    \/ \E p \in Producers, l \in Links : ProducerPublish(p, l)
    \/ \E p \in Producers : ProducerSwap(p)
    \/ \E p \in Producers : ProducerPreload(p)
    \/ \E p \in Producers : ProducerPush(p)
    \/ \E p \in Producers : ProducerLoad(p)
    \/ \E p \in Producers : ProducerWake(p)
    \/ ReactorTake
    \/ ReactorHandle
    \/ ReactorRun
    \/ ReactorLateClear
    \/ ReactorSleep
    \/ ReactorRecheck
    \/ ReactorPoll
    \/ ReactorAwake
    \/ Quiescent

Spec == Init /\ [][Next]_vars

\* A thread that has a step takes it. Nothing is assumed about
\* ProducerPublish: the core may never have another frame.
ReactorStep ==
    \/ ReactorTake \/ ReactorHandle \/ ReactorRun \/ ReactorLateClear
    \/ ReactorSleep \/ ReactorRecheck \/ ReactorPoll \/ ReactorAwake
ProducerFinish(p) ==
    \/ ProducerSwap(p) \/ ProducerPreload(p) \/ ProducerPush(p)
    \/ ProducerLoad(p) \/ ProducerWake(p)

FairSpec ==
    /\ Spec
    /\ WF_vars(ReactorStep)
    /\ \A p \in Producers : WF_vars(ProducerFinish(p))

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

TypeOK ==
    /\ published \in [Links -> 0..MaxItems] /\ taken \in [Links -> 0..MaxItems]
    /\ dirty \in [Links -> BOOLEAN] /\ queued \in [Links -> BOOLEAN]
    /\ queue \in [Links -> 0..MaxItems] /\ batch \in [Links -> 0..MaxItems]
    /\ sleeping \in BOOLEAN /\ waker \in BOOLEAN /\ pendingSeen \in BOOLEAN
    /\ rpc \in {"take", "handle", "run", "lateclear", "sleep", "recheck", "poll", "awake"}
    /\ ppc \in [Producers -> {"idle", "notify", "preload", "push", "pushw", "pushn",
                              "load", "wake"}]
    /\ ptarget \in [Producers -> Links \cup {NoLink}]
    /\ sent \in [Producers -> 0..MaxPublish]

LaneBounded == \A l \in Links : taken[l] <= published[l] /\ LaneLen(l) <= LaneCap

Pushing(p, l) == ptarget[p] = l /\ ppc[p] \in {"preload", "push", "pushw", "pushn"}

\* `notify` is idempotent until the task runs: a link has at most one Run
\* message between its mark and the reactor, so the queue is bounded by
\* the number of links.
RunOnce ==
    \A l \in Links :
        queue[l] + batch[l] + Cardinality({p \in Producers : Pushing(p, l)}) <= 1

\* A marked link has its message on the way.
DirtyHasRun ==
    \A l \in Links : dirty[l] =>
        queue[l] + batch[l] + Cardinality({p \in Producers : Pushing(p, l)}) = 1

\* A frame in a lane has something that will run its task: a Run message
\* in the queue or in the reactor's hands, the task already in `ready`, or
\* a producer that has not finished notifying.
NoStrandedFrame ==
    \A l \in Links : LaneLen(l) > 0 =>
        \/ queue[l] > 0 \/ batch[l] > 0 \/ queued[l]
        \/ \E p \in Producers : ptarget[p] = l /\ ppc[p] = "notify"
        \/ \E p \in Producers : Pushing(p, l)

\* A reactor blocked in its poll with a message in its queue has a wake
\* on its way: the producer of that message has not read `sleeping` yet,
\* or has read it set. Otherwise the message waits for the timeout.
NoLostWake ==
    (Blocked /\ ~QueueEmpty) => \E p \in Producers : ppc[p] \in {"load", "wake"}

(***************************************************************************)
(* Progress, under FairSpec: every published frame is taken by its task,   *)
(* with no readiness event, tick or timeout in the model.                  *)
(***************************************************************************)
EveryFrameTaken ==
    \A l \in Links : \A n \in 1..MaxItems :
        (published[l] >= n) ~> (taken[l] >= n)

(***************************************************************************)
(* Non-vacuity, each the negation of an invariant TLC must report.         *)
(***************************************************************************)

\* A producer wakes a reactor that is blocked in its poll.
NoWakeOfBlockedWitness == ~(Blocked /\ \E p \in Producers : ppc[p] = "wake")

\* A producer read `sleeping` clear and left; the re-check of the queue,
\* not a wake, keeps the reactor from blocking.
NoRecheckSavesWitness ==
    ~(rpc = "poll" /\ pendingSeen /\ ~waker /\ ~QueueEmpty
      /\ \A p \in Producers : ppc[p] \notin {"load", "wake"})

\* A producer finds the link already marked and sends no message.
NoCoalescedNotifyWitness ==
    ~\E p \in Producers : ppc[p] = "notify" /\ dirty[ptarget[p]]

\* A frame published while the tasks run sends a second Run message.
NoNotifyDuringRunWitness == ~(rpc = "run" /\ ~QueueEmpty)

=============================================================================
