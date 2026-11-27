------------------------------- MODULE MWake -------------------------------
(***************************************************************************)
(* MWake: the wake-up signal of a thread that waits for work, as written   *)
(* in crates/mqttiumd/src/wake.rs. Two loops of the default runtime wait   *)
(* on one: the core thread (runtime.rs, `runtime.wake`) and the store      *)
(* worker (writer.rs, `requests`). With delivery shards each shard worker  *)
(* waits on one more (delivery_shard.rs, `store.wake`), with the same loop *)
(* and other producers; that loop was read and is not replayed.            *)
(*                                                                         *)
(* A producer publishes its value, then signals:                           *)
(*                                                                         *)
(*   lane.try_send(value)                 ProducerPublish                  *)
(*   if !pending.swap(true) {             ProducerSwap                     *)
(*       let guard = lock.lock();         ProducerNotify                   *)
(*       ready.notify_one();                                               *)
(*       drop(guard);                                                      *)
(*   }                                                                     *)
(*                                                                         *)
(* The consumer makes passes over its sources and waits only after a pass  *)
(* that made no progress:                                                  *)
(*                                                                         *)
(*   loop {                                                                *)
(*       before = progress; turn();       ConsumerPass                     *)
(*       if progress == before {                                           *)
(*           guard = lock.lock();         ConsumerLock                     *)
(*           loop {                                                        *)
(*               if pending.swap(false)   ConsumerCheck                    *)
(*                   { return }                                            *)
(*               guard = ready.wait(guard)   (blocked) / ConsumerRelock    *)
(*           }                                                             *)
(*       }                                                                 *)
(*   }                                                                     *)
(*                                                                         *)
(* The timeout of `Wake::wait` is absent: the model shows that no          *)
(* published value depends on it. A lane is the count published -          *)
(* consumed; values are not told apart. No guard reads a history           *)
(* variable: there is none.                                                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    NumProducers, \* threads that publish towards the consumer
    MaxPublish,   \* values one producer publishes in a run
    LaneCap,      \* capacity of the lane the consumer drains
    Mutation      \* "none" or the name of a model mutation

Mutations == {"none", "WakeBeforePublish", "CheckOutsideLock", "ClearAfterPass"}

ASSUME /\ NumProducers \in Nat \ {0} /\ MaxPublish \in Nat \ {0}
       /\ LaneCap \in Nat \ {0}
       /\ Mutation \in Mutations

Producers == 1..NumProducers
MaxItems == NumProducers * MaxPublish
Free == 0
ConsumerId == NumProducers + 1

VARIABLES
    published, \* values published into the lane
    consumed,  \* values the consumer took out of it
    pending,   \* Inner::pending
    lock,      \* holder of Inner::lock: Free, a producer or ConsumerId
    ppc,       \* per producer: where it is in its steps
    sent,      \* per producer: publications started so far
    cpc        \* where the consumer is in its loop

vars == <<published, consumed, pending, lock, ppc, sent, cpc>>

LaneLen == published - consumed
Early == Mutation = "WakeBeforePublish"

Init ==
    /\ published = 0 /\ consumed = 0
    /\ pending = FALSE /\ lock = Free
    /\ ppc = [p \in Producers |-> "idle"]
    /\ sent = [p \in Producers |-> 0]
    /\ cpc = "pass"

(***************************************************************************)
(* Producers. `Signalled::try_send` and every other producer of the        *)
(* runtime publish first and call `Wake::wake` after.                      *)
(***************************************************************************)

\* A producer starts one publication. WakeBeforePublish signals first and
\* publishes at ProducerLate.
ProducerPublish(p) ==
    /\ ppc[p] = "idle" /\ sent[p] < MaxPublish
    /\ sent' = [sent EXCEPT ![p] = @ + 1]
    /\ IF Early
       THEN UNCHANGED published
       ELSE /\ LaneLen < LaneCap
            /\ published' = published + 1
    /\ ppc' = [ppc EXCEPT ![p] = "swap"]
    /\ UNCHANGED <<consumed, pending, lock, cpc>>

\* `pending.swap(true)`: only the transition from clear to pending goes on
\* to the lock.
ProducerSwap(p) ==
    /\ ppc[p] = "swap"
    /\ pending' = TRUE
    /\ ppc' = [ppc EXCEPT ![p] =
                  IF ~pending THEN "notify" ELSE IF Early THEN "late" ELSE "idle"]
    /\ UNCHANGED <<published, consumed, lock, sent, cpc>>

\* lock; notify_one; unlock. The three are one step: nothing else can use
\* the lock in between. A consumer inside `Condvar::wait` is woken; one
\* that is not waiting is left alone.
ProducerNotify(p) ==
    /\ ppc[p] = "notify" /\ lock = Free
    /\ cpc' = IF cpc = "waiting" THEN "woken" ELSE cpc
    /\ ppc' = [ppc EXCEPT ![p] = IF Early THEN "late" ELSE "idle"]
    /\ UNCHANGED <<published, consumed, pending, lock, sent>>

\* Only WakeBeforePublish reaches "late": the value is published after
\* its signal.
ProducerLate(p) ==
    /\ ppc[p] = "late" /\ LaneLen < LaneCap
    /\ published' = published + 1
    /\ ppc' = [ppc EXCEPT ![p] = "idle"]
    /\ UNCHANGED <<consumed, pending, lock, sent, cpc>>

(***************************************************************************)
(* The consumer.                                                           *)
(***************************************************************************)

\* One pass: it takes what is published, or finds nothing and goes to
\* wait. ClearAfterPass consumes the signal here (`Wake::take`) instead of
\* inside `wait`; CheckOutsideLock tests `pending` before it locks.
ConsumerPass ==
    /\ cpc = "pass"
    /\ IF LaneLen > 0
       THEN consumed' = published /\ UNCHANGED cpc
       ELSE /\ cpc' = CASE Mutation = "ClearAfterPass" -> "clear"
                        [] Mutation = "CheckOutsideLock" -> "check"
                        [] OTHER -> "lock"
            /\ UNCHANGED consumed
    /\ UNCHANGED <<published, pending, lock, ppc, sent>>

\* Only ClearAfterPass reaches "clear".
ConsumerClear ==
    /\ cpc = "clear"
    /\ pending' = FALSE
    /\ cpc' = "lock"
    /\ UNCHANGED <<published, consumed, lock, ppc, sent>>

\* `lock.lock()` at the entry of `wait`. CheckOutsideLock already tested
\* `pending`: it locks and waits at once, without a second look.
ConsumerLock ==
    /\ cpc = "lock" /\ lock = Free
    /\ IF Mutation = "CheckOutsideLock"
       THEN cpc' = "waiting" /\ UNCHANGED lock
       ELSE cpc' = "check" /\ lock' = ConsumerId
    /\ UNCHANGED <<published, consumed, pending, ppc, sent>>

\* `pending.swap(false)` under the lock: a signal raised since the last
\* wait ends this one at once; otherwise `Condvar::wait` releases the lock
\* and blocks in one step.
ConsumerCheck ==
    /\ cpc = "check"
    /\ Mutation # "CheckOutsideLock" => lock = ConsumerId
    /\ IF pending
       THEN /\ pending' = FALSE /\ cpc' = "pass" /\ lock' = Free
       ELSE /\ UNCHANGED pending
            /\ IF Mutation = "CheckOutsideLock"
               THEN cpc' = "lock" /\ UNCHANGED lock
               ELSE cpc' = "waiting" /\ lock' = Free
    /\ UNCHANGED <<published, consumed, ppc, sent>>

\* A notified consumer takes the lock again and loops to the check.
ConsumerRelock ==
    /\ cpc = "woken" /\ lock = Free
    /\ IF Mutation = "CheckOutsideLock"
       THEN UNCHANGED lock
       ELSE lock' = ConsumerId
    /\ cpc' = "check"
    /\ UNCHANGED <<published, consumed, pending, ppc, sent>>

\* The end of a run: every producer published its values and finished its
\* steps, the lane is empty and the consumer waits with no signal left.
\* Any other state without a step is a deadlock that TLC reports.
Terminal ==
    /\ \A p \in Producers : ppc[p] = "idle" /\ sent[p] = MaxPublish
    /\ LaneLen = 0
    /\ cpc = "waiting" /\ ~pending /\ lock = Free

Quiescent == Terminal /\ UNCHANGED vars

Next ==
    \/ \E p \in Producers : ProducerPublish(p)
    \/ \E p \in Producers : ProducerSwap(p)
    \/ \E p \in Producers : ProducerNotify(p)
    \/ \E p \in Producers : ProducerLate(p)
    \/ ConsumerPass
    \/ ConsumerClear
    \/ ConsumerLock
    \/ ConsumerCheck
    \/ ConsumerRelock
    \/ Quiescent

Spec == Init /\ [][Next]_vars

\* A thread that has a step takes it. Nothing is assumed about
\* ProducerPublish: a producer may never have another value.
ConsumerStep ==
    ConsumerPass \/ ConsumerClear \/ ConsumerLock \/ ConsumerCheck \/ ConsumerRelock
ProducerFinish(p) == ProducerSwap(p) \/ ProducerNotify(p) \/ ProducerLate(p)

FairSpec ==
    /\ Spec
    /\ WF_vars(ConsumerStep)
    /\ \A p \in Producers : WF_vars(ProducerFinish(p))

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

TypeOK ==
    /\ published \in 0..MaxItems /\ consumed \in 0..MaxItems
    /\ pending \in BOOLEAN
    /\ lock \in {Free, ConsumerId} \cup Producers
    /\ ppc \in [Producers -> {"idle", "swap", "notify", "late"}]
    /\ sent \in [Producers -> 0..MaxPublish]
    /\ cpc \in {"pass", "clear", "lock", "check", "waiting", "woken"}

\* The lane never holds more than its capacity, and nothing is taken out
\* that was not put in.
LaneBounded == consumed <= published /\ LaneLen <= LaneCap

\* A consumer blocked in `Condvar::wait` while a value is published has a
\* notification on its way: a producer that set `pending` and has not
\* notified yet, or one that is about to set it. Otherwise the value waits
\* for the timeout.
NoLostWakeup ==
    (cpc = "waiting" /\ LaneLen > 0) =>
        \/ \E p \in Producers : ppc[p] = "notify"
        \/ ~pending /\ \E p \in Producers : ppc[p] = "swap"

\* A blocked consumer never sits on a signal it has not consumed, unless
\* the producer that raised it is still on its way to the lock.
NoSleepOnSignal ==
    (cpc = "waiting" /\ pending) => \E p \in Producers : ppc[p] = "notify"

(***************************************************************************)
(* Progress, under FairSpec: every published value is consumed, with no    *)
(* timeout in the model.                                                   *)
(***************************************************************************)
EveryValueConsumed ==
    \A n \in 1..MaxItems : (published >= n) ~> (consumed >= n)

(***************************************************************************)
(* Non-vacuity, each the negation of an invariant TLC must report.         *)
(***************************************************************************)

\* A signal raised during a pass that saw nothing is found by the check
\* under the lock: `wait` returns without blocking.
NoSignalBeforeWaitWitness ==
    ~(cpc = "check" /\ lock = ConsumerId /\ pending /\ LaneLen > 0)

\* A producer notifies a consumer that is blocked in `Condvar::wait`.
NoNotifyWhileWaitingWitness == ~(cpc = "woken")

\* A producer finds `pending` already set and does not take the lock.
NoCoalescedSignalWitness ==
    ~\E p \in Producers : ppc[p] = "swap" /\ pending

=============================================================================
