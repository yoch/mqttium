---------------------------- MODULE MReactorWake ----------------------------
(***************************************************************************)
(* MReactorWake: how the core thread gets a reactor thread to run a        *)
(* transport task, as written in crates/mqttiumd/src/net/reactor.rs        *)
(* (ADR 0044). The core publishes a frame into the task's bounded lane     *)
(* and then notifies its link (`ConnCore::try_send_outgoing`, net.rs;      *)
(* `Link::notify`, `Shared::send` and `Shared::kick`, net/reactor.rs):     *)
(*                                                                         *)
(*   outgoing.lock().enqueue(frame)         ProducerPublish                *)
(*   if !link.dirty.swap(true) {            ProducerSwap                   *)
(*       queue.lock().push(Run(link))       ProducerPush                   *)
(*       owing.store(true)                  ProducerOwe                    *)
(*       if !held.load() {                  ProducerReadHeld               *)
(*           kick() } }                                                    *)
(*                                                                         *)
(*   kick: if owing.swap(false)             ProducerKick                   *)
(*            && sleeping.load() {          ProducerLoad                   *)
(*             waker.wake() }               ProducerWake                   *)
(*                                                                         *)
(* A producer that serves many lanes in one pass holds the wake-ups        *)
(* (`Reactors::hold`, taken by `Runtime::turn` through `net::hold_wakes`)  *)
(* and pays one when its guard is dropped (`Shared::hold`,                 *)
(* `Shared::release`):                                                     *)
(*                                                                         *)
(*   held.store(true)                       ProducerHold                   *)
(*   ...                                                                   *)
(*   held.store(false); kick()              ProducerRelease                *)
(*                                                                         *)
(* The debt is recorded before the hold is read, and the release clears    *)
(* the hold before it reads the debt: one of the two pays it. `held` is    *)
(* one flag for every holder, here as in the code: the release of one      *)
(* holder ends the hold of all.                                            *)
(*                                                                         *)
(* FairSpec releases every hold: in `Runtime::turn` the guard is dropped   *)
(* at the end of the pass, before the thread waits, and so it is in        *)
(* `DeliveryShardWorker::run_loop` with `--delivery-shards K >= 1`. At     *)
(* `9dfd568` a shard kept its guard while it waited, and its release came  *)
(* as late as the end of that wait, which a timer bounds and this model    *)
(* does not contain. FairSpecHoldKept drops the fairness of the release    *)
(* to show what then depended on it.                                       *)
(*                                                                         *)
(* The reactor thread loops (`run`, `Shared::take`, `Shared::sleep`):      *)
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
(* model shows that a published frame depends on none of them once every   *)
(* hold is released. A lane is the count published - taken. No guard       *)
(* reads a history variable: there is none.                                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    NumLinks,     \* transport tasks of one reactor thread, each with a lane
    NumProducers, \* threads that publish and notify
    Holders,      \* producers that hold the wake-ups around their publications
    LaneCap,      \* capacity of a lane (OUTPUT_SLOTS)
    MaxPublish,   \* frames one producer publishes in a run
    Mutation      \* "none" or the name of a model mutation

Mutations == {"none", "CheckBeforePush", "PollWithoutRecheck", "ClearDirtyAfterRun",
              "DebtReadBeforeRelease", "HoldReadBeforeDebt"}

ASSUME /\ NumLinks \in Nat \ {0} /\ NumProducers \in Nat \ {0}
       /\ LaneCap \in Nat \ {0} /\ MaxPublish \in Nat \ {0}
       /\ Holders \subseteq 1..NumProducers
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
    held,        \* Shared::held
    owing,       \* Shared::owing
    holding,     \* per producer: it has a live Held guard
    pseen,       \* per producer: a flag it read ahead of its turn (mutants)
    pendingSeen, \* the reactor's `pending`, read after it set `sleeping`
    rpc,         \* where the reactor is in its loop
    ppc,         \* per producer: where it is in its steps
    ptarget,     \* per producer: the link of the frame in hand
    sent         \* per producer: frames published so far

vars == <<published, taken, dirty, queue, batch, queued, sleeping, waker,
          held, owing, holding, pseen, pendingSeen, rpc, ppc, ptarget, sent>>

\* What the reactor does not write and what a producer does not write.
debtVars == <<held, owing, holding, pseen>>

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
    /\ held = FALSE /\ owing = FALSE
    /\ holding = [p \in Producers |-> FALSE]
    /\ pseen = [p \in Producers |-> FALSE]
    /\ rpc = "take"
    /\ ppc = [p \in Producers |-> "idle"]
    /\ ptarget = [p \in Producers |-> NoLink]
    /\ sent = [p \in Producers |-> 0]

Done(p) ==
    /\ ppc' = [ppc EXCEPT ![p] = "idle"]
    /\ ptarget' = [ptarget EXCEPT ![p] = NoLink]

Releasing(p) == ppc[p] \in {"rkick", "rload", "rwake"}

\* The end of a kick. The kick of a send ends the notification; the kick
\* of a release ends the release, and DebtReadBeforeRelease clears the
\* hold only now.
Kicked(p) ==
    IF Releasing(p)
    THEN /\ ppc' = [ppc EXCEPT ![p] =
                       IF Mutation = "DebtReadBeforeRelease" THEN "unhold" ELSE "idle"]
         /\ UNCHANGED ptarget
    ELSE Done(p)

(***************************************************************************)
(* Producers: `ConnCore::try_send_outgoing`, then `Link::notify`,          *)
(* `Shared::send` and `Shared::kick`; `Reactors::hold` and the drop of     *)
(* its guard.                                                              *)
(***************************************************************************)

\* `Reactors::hold`: `held.store(true)`.
ProducerHold(p) ==
    /\ p \in Holders /\ ppc[p] = "idle" /\ ~holding[p] /\ sent[p] < MaxPublish
    /\ held' = TRUE
    /\ holding' = [holding EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   owing, pseen, pendingSeen, rpc, ppc, ptarget, sent>>

\* try_send: a full lane refuses and raises no notification; the refusal
\* is the absence of this step.
ProducerPublish(p, l) ==
    /\ ppc[p] = "idle" /\ sent[p] < MaxPublish /\ LaneLen(l) < LaneCap
    /\ published' = [published EXCEPT ![l] = @ + 1]
    /\ sent' = [sent EXCEPT ![p] = @ + 1]
    /\ ppc' = [ppc EXCEPT ![p] = "notify"]
    /\ ptarget' = [ptarget EXCEPT ![p] = l]
    /\ UNCHANGED <<taken, dirty, queue, batch, queued, sleeping, waker, debtVars,
                   pendingSeen, rpc>>

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
    /\ UNCHANGED <<published, taken, queue, batch, queued, sleeping, waker, debtVars,
                   pendingSeen, rpc, sent>>

\* Only CheckBeforePush reaches "preload": it remembers `sleeping` as it
\* was before the message is in the queue.
ProducerPreload(p) ==
    /\ ppc[p] = "preload"
    /\ pseen' = [pseen EXCEPT ![p] = sleeping]
    /\ ppc' = [ppc EXCEPT ![p] = "push"]
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   held, owing, holding, pendingSeen, rpc, ptarget, sent>>

\* `queue.lock().push(Run(link))`. HoldReadBeforeDebt reads the hold
\* next, before it records the debt.
ProducerPush(p) ==
    /\ ppc[p] = "push"
    /\ queue' = [queue EXCEPT ![ptarget[p]] = @ + 1]
    /\ ppc' = [ppc EXCEPT ![p] =
                  IF Mutation = "HoldReadBeforeDebt" THEN "readheld" ELSE "owe"]
    /\ UNCHANGED <<published, taken, dirty, batch, queued, sleeping, waker, debtVars,
                   pendingSeen, rpc, ptarget, sent>>

\* `owing.store(true)`: the debt is recorded before the hold is read.
ProducerOwe(p) ==
    /\ ppc[p] = "owe"
    /\ owing' = TRUE
    /\ IF Mutation = "HoldReadBeforeDebt"
       THEN IF pseen[p]
            THEN Done(p)
            ELSE ppc' = [ppc EXCEPT ![p] = "kick"] /\ UNCHANGED ptarget
       ELSE ppc' = [ppc EXCEPT ![p] = "readheld"] /\ UNCHANGED ptarget
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   held, holding, pseen, pendingSeen, rpc, sent>>

\* `held.load()`: under a hold the wake-up is left to the release.
ProducerReadHeld(p) ==
    /\ ppc[p] = "readheld"
    /\ IF Mutation = "HoldReadBeforeDebt"
       THEN /\ pseen' = [pseen EXCEPT ![p] = held]
            /\ ppc' = [ppc EXCEPT ![p] = "owe"]
            /\ UNCHANGED ptarget
       ELSE /\ UNCHANGED pseen
            /\ IF held
               THEN Done(p)
               ELSE ppc' = [ppc EXCEPT ![p] = "kick"] /\ UNCHANGED ptarget
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   held, owing, holding, pendingSeen, rpc, sent>>

\* The drop of the guard: `held.store(false)`, then `kick()`.
\* DebtReadBeforeRelease kicks first and clears the hold at
\* ProducerUnhold.
ProducerRelease(p) ==
    /\ ppc[p] = "idle" /\ holding[p]
    /\ ppc' = [ppc EXCEPT ![p] = "rkick"]
    /\ IF Mutation = "DebtReadBeforeRelease"
       THEN UNCHANGED <<held, holding>>
       ELSE /\ held' = FALSE
            /\ holding' = [holding EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   owing, pseen, pendingSeen, rpc, ptarget, sent>>

\* `owing.swap(false)` in `kick`: whoever takes the debt gives the
\* wake-up.
ProducerKick(p) ==
    /\ ppc[p] \in {"kick", "rkick"}
    /\ owing' = FALSE
    /\ IF owing
       THEN /\ ppc' = [ppc EXCEPT ![p] = IF ppc[p] = "kick" THEN "load" ELSE "rload"]
            /\ UNCHANGED ptarget
       ELSE Kicked(p)
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   held, holding, pseen, pendingSeen, rpc, sent>>

\* `sleeping.load()`, after the push. CheckBeforePush uses what it read
\* before the push.
ProducerLoad(p) ==
    /\ ppc[p] \in {"load", "rload"}
    /\ IF (IF Mutation = "CheckBeforePush" /\ ppc[p] = "load" THEN pseen[p] ELSE sleeping)
       THEN /\ ppc' = [ppc EXCEPT ![p] = IF ppc[p] = "load" THEN "wake" ELSE "rwake"]
            /\ UNCHANGED ptarget
       ELSE Kicked(p)
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   debtVars, pendingSeen, rpc, sent>>

\* `waker.wake()`.
ProducerWake(p) ==
    /\ ppc[p] \in {"wake", "rwake"}
    /\ waker' = TRUE
    /\ Kicked(p)
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping,
                   debtVars, pendingSeen, rpc, sent>>

\* Only DebtReadBeforeRelease reaches "unhold".
ProducerUnhold(p) ==
    /\ ppc[p] = "unhold"
    /\ held' = FALSE
    /\ holding' = [holding EXCEPT ![p] = FALSE]
    /\ ppc' = [ppc EXCEPT ![p] = "idle"]
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   owing, pseen, pendingSeen, rpc, ptarget, sent>>

(***************************************************************************)
(* The reactor thread.                                                     *)
(***************************************************************************)

\* `std::mem::take(&mut *queue)`.
ReactorTake ==
    /\ rpc = "take"
    /\ batch' = queue /\ queue' = None
    /\ rpc' = "handle"
    /\ UNCHANGED <<published, taken, dirty, queued, sleeping, waker, pendingSeen,
                   ppc, ptarget, sent, debtVars>>

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
                   ppc, ptarget, sent, debtVars>>

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
                   ppc, ptarget, sent, debtVars>>

\* Only ClearDirtyAfterRun reaches "lateclear".
ReactorLateClear ==
    /\ rpc = "lateclear"
    /\ dirty' = [l \in Links |-> dirty[l] /\ batch[l] = 0]
    /\ batch' = None
    /\ rpc' = "sleep"
    /\ UNCHANGED <<published, taken, queue, queued, sleeping, waker, pendingSeen,
                   ppc, ptarget, sent, debtVars>>

\* `sleeping.store(true)`. PollWithoutRecheck polls without looking at
\* the queue again.
ReactorSleep ==
    /\ rpc = "sleep"
    /\ sleeping' = TRUE
    /\ pendingSeen' = FALSE
    /\ rpc' = IF Mutation = "PollWithoutRecheck" THEN "poll" ELSE "recheck"
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, waker,
                   ppc, ptarget, sent, debtVars>>

\* `pending = !queue.is_empty()`, read after `sleeping` is set.
ReactorRecheck ==
    /\ rpc = "recheck"
    /\ pendingSeen' = ~QueueEmpty
    /\ rpc' = "poll"
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping, waker,
                   ppc, ptarget, sent, debtVars>>

\* `poll`: it returns at once with a zero timeout or a wake already
\* delivered, and otherwise when a wake arrives. It consumes the wake.
ReactorPoll ==
    /\ rpc = "poll" /\ (pendingSeen \/ waker)
    /\ waker' = FALSE
    /\ rpc' = "awake"
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, sleeping,
                   pendingSeen, ppc, ptarget, sent, debtVars>>

\* `sleeping.store(false)`.
ReactorAwake ==
    /\ rpc = "awake"
    /\ sleeping' = FALSE
    /\ rpc' = "take"
    /\ UNCHANGED <<published, taken, dirty, queue, batch, queued, waker,
                   pendingSeen, ppc, ptarget, sent, debtVars>>

\* The end of a run: every producer published its frames, released its
\* hold and finished its steps, no debt is left, every lane is empty and
\* the reactor is blocked in its poll with an empty queue. Any other state
\* without a step is a deadlock that TLC reports.
Terminal ==
    /\ \A p \in Producers : ppc[p] = "idle" /\ sent[p] = MaxPublish /\ ~holding[p]
    /\ ~held /\ ~owing
    /\ \A l \in Links : LaneLen(l) = 0 /\ ~queued[l] /\ batch[l] = 0
    /\ QueueEmpty /\ Blocked

Quiescent == Terminal /\ UNCHANGED vars

Next ==
    \/ \E p \in Producers : ProducerHold(p)
    \/ \E p \in Producers, l \in Links : ProducerPublish(p, l)
    \/ \E p \in Producers : ProducerSwap(p)
    \/ \E p \in Producers : ProducerPreload(p)
    \/ \E p \in Producers : ProducerPush(p)
    \/ \E p \in Producers : ProducerOwe(p)
    \/ \E p \in Producers : ProducerReadHeld(p)
    \/ \E p \in Producers : ProducerRelease(p)
    \/ \E p \in Producers : ProducerKick(p)
    \/ \E p \in Producers : ProducerLoad(p)
    \/ \E p \in Producers : ProducerWake(p)
    \/ \E p \in Producers : ProducerUnhold(p)
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

\* A thread that has a step takes it, and a hold is released: it lasts
\* one pass of its producer. Nothing is assumed about ProducerPublish and
\* ProducerHold: the core may never have another frame.
ReactorStep ==
    \/ ReactorTake \/ ReactorHandle \/ ReactorRun \/ ReactorLateClear
    \/ ReactorSleep \/ ReactorRecheck \/ ReactorPoll \/ ReactorAwake
ProducerFinish(p) ==
    \/ ProducerSwap(p) \/ ProducerPreload(p) \/ ProducerPush(p)
    \/ ProducerOwe(p) \/ ProducerReadHeld(p) \/ ProducerRelease(p)
    \/ ProducerKick(p) \/ ProducerLoad(p) \/ ProducerWake(p) \/ ProducerUnhold(p)

FairSpec ==
    /\ Spec
    /\ WF_vars(ReactorStep)
    /\ \A p \in Producers : WF_vars(ProducerFinish(p))

\* The same without the release: a producer that took a hold may keep it,
\* as a thread does that waits with its guard alive. Every other step a
\* thread has is still taken.
ProducerFinishKeepingHold(p) ==
    \/ ProducerSwap(p) \/ ProducerPreload(p) \/ ProducerPush(p)
    \/ ProducerOwe(p) \/ ProducerReadHeld(p)
    \/ ProducerKick(p) \/ ProducerLoad(p) \/ ProducerWake(p) \/ ProducerUnhold(p)

FairSpecHoldKept ==
    /\ Spec
    /\ WF_vars(ReactorStep)
    /\ \A p \in Producers : WF_vars(ProducerFinishKeepingHold(p))

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

TypeOK ==
    /\ published \in [Links -> 0..MaxItems] /\ taken \in [Links -> 0..MaxItems]
    /\ dirty \in [Links -> BOOLEAN] /\ queued \in [Links -> BOOLEAN]
    /\ queue \in [Links -> 0..MaxItems] /\ batch \in [Links -> 0..MaxItems]
    /\ sleeping \in BOOLEAN /\ waker \in BOOLEAN /\ pendingSeen \in BOOLEAN
    /\ held \in BOOLEAN /\ owing \in BOOLEAN
    /\ holding \in [Producers -> BOOLEAN] /\ pseen \in [Producers -> BOOLEAN]
    /\ rpc \in {"take", "handle", "run", "lateclear", "sleep", "recheck", "poll", "awake"}
    /\ ppc \in [Producers -> {"idle", "notify", "preload", "push", "owe", "readheld",
                              "kick", "load", "wake", "rkick", "rload", "rwake",
                              "unhold"}]
    /\ ptarget \in [Producers -> Links \cup {NoLink}]
    /\ sent \in [Producers -> 0..MaxPublish]

LaneBounded == \A l \in Links : taken[l] <= published[l] /\ LaneLen(l) <= LaneCap

Pushing(p, l) == ptarget[p] = l /\ ppc[p] \in {"preload", "push"}

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

\* A recorded debt has someone who will pay it: a producer between its
\* record and its look at the hold, a kick that has not taken the debt
\* yet, or a live hold, whose release kicks.
DebtHasPayer ==
    owing => \/ \E p \in Producers : ppc[p] \in {"readheld", "kick", "rkick"}
             \/ \E p \in Producers : holding[p]

\* A reactor blocked in its poll with a message in its queue has a wake
\* on its way: a kick took the debt and has not read `sleeping` yet or
\* read it set, or a producer pushed and has not settled its debt yet, or
\* the debt is recorded and has a payer. Otherwise the message waits for
\* the timeout.
NoLostWake ==
    (Blocked /\ ~QueueEmpty) =>
        \/ \E p \in Producers : ppc[p] \in {"load", "wake", "rload", "rwake"}
        \/ \E p \in Producers : ppc[p] \in {"owe", "readheld"}
        \/ owing /\ DebtHasPayer

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
    ~(rpc = "poll" /\ pendingSeen /\ ~waker /\ ~QueueEmpty /\ ~owing
      /\ \A p \in Producers : ppc[p] = "idle")

\* A message queued under a hold waits in the queue of a blocked reactor
\* until the release pays its wake-up.
NoDebtPaidAtReleaseWitness ==
    ~(Blocked /\ ~QueueEmpty /\ \E p \in Producers : ppc[p] = "rwake")

\* A producer finds the link already marked and sends no message.
NoCoalescedNotifyWitness ==
    ~\E p \in Producers : ppc[p] = "notify" /\ dirty[ptarget[p]]

\* A frame published while the tasks run sends a second Run message.
NoNotifyDuringRunWitness == ~(rpc = "run" /\ ~QueueEmpty)

=============================================================================
