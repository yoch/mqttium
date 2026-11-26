---------------------------- MODULE MInlineLease ----------------------------
(***************************************************************************)
(* The inline-claim lease of one session with --delivery-shards K >= 1.    *)
(*                                                                         *)
(* The coordinator admits the publications of one publisher to one         *)
(* subscriber, in order. An admission carries an inline claim when the    *)
(* lease the shard published allows it (InlineClaimLease::try_reserve);   *)
(* otherwise it is stored and the lease is marked dirty                   *)
(* (InlineClaimOracle::mark_ineligible). The store applies admissions in  *)
(* order; its answer may be unknown, and is then learnt by a resolution.  *)
(* A committed inline claim reaches the shard as InlineCommitted; a       *)
(* stored publication reaches it by a read of the store (a scan) that a   *)
(* dirty message asks for. The shard sends its rows in order, publishes   *)
(* the lease again once its scans are done, and frees an identifier at    *)
(* the acknowledgement. A subscription replay (row 0) is committed by a   *)
(* command of its own at any time.                                        *)
(*                                                                         *)
(* The flags select the behaviour. 9dfd568 had none; 1ccddfb has         *)
(* ForeignIds; the repair of report 0046 is HoldBehind, ScanGate and      *)
(* NotifyAfterStep, without ForeignIds.                                   *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    N,                \* publications of the publisher, admitted in order 1..N
    Ids,              \* packet identifiers
    Cap,              \* the lease's room (preclaim_cap)
    Unknown,          \* whether an answer of the store may be unknown
    Replay,           \* whether a subscription replay commits a row 0
    ForeignIds,       \* report 0045: a shard claim avoids the lease's bits, and the
                      \* lease is not published while a committed claim waits
    HoldBehind,       \* a committed claim waits while an inline admission of the
                      \* session has an unknown outcome
    ScanGate,         \* no read of the store while an inline claim may be on its way
    NotifyAfterStep,  \* the coordinator notifies a stored target after its step
    Mutation          \* "none", or a change to the repair that a configuration refutes

Pubs == 1..N
Rows == 0..N
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max2(a, b) == IF a > b THEN a ELSE b
Msg(t, s, p) == [t |-> t, s |-> s, p |-> p]

VARIABLES
    \* coordinator (ShardPool, Owner)
    next, applied, kind, pid, adm, replies, store, replayed,
    dirty, acked, elig, inflight, used,
    count, nonInline, barrier, held, chan,
    \* delivery shard (DeliveryShardWorker, its Owner)
    seen, scanned, published, fenced, coreDirty, pendingInl,
    rows, rowPi, foreign, sent, emitted

coord == << next, applied, kind, pid, adm, replies, store, replayed,
            dirty, acked, elig, inflight, used, count, nonInline, barrier, held, chan >>
shard == << seen, scanned, published, fenced, coreDirty, pendingInl,
            rows, rowPi, foreign, sent, emitted >>
vars == << coord, shard >>

Init ==
    /\ next = 1 /\ applied = 0
    /\ kind = [p \in Rows |-> IF p = 0 THEN "stored" ELSE "none"]
    /\ pid = [p \in Rows |-> 0]
    /\ adm = [p \in Rows |-> "none"]
    /\ replies = << >>
    /\ store = [p \in Rows |-> "none"]
    /\ replayed = FALSE
    \* ShardPool::attach_session marks the lease and sends Attach.
    /\ dirty = 1 /\ acked = 0 /\ elig = FALSE /\ inflight = 0 /\ used = {}
    /\ count = 0 /\ nonInline = 0 /\ barrier = 0 /\ held = << >>
    /\ chan = << Msg("Attach", 1, 0) >>
    /\ seen = 0 /\ scanned = 0 /\ published = FALSE /\ fenced = FALSE
    /\ coreDirty = FALSE /\ pendingInl = << >>
    /\ rows = {} /\ rowPi = [p \in Rows |-> 0] /\ foreign = {}
    /\ sent = {} /\ emitted = << >>

-----------------------------------------------------------------------------
(* The coordinator's state as a record, so that one step can apply its     *)
(* operations in the order of the code.                                    *)
C == [dirty |-> dirty, elig |-> elig, barrier |-> barrier, chan |-> chan,
      count |-> count, nonInline |-> nonInline, held |-> held,
      used |-> used, inflight |-> inflight, adm |-> adm]

SetC(c) ==
    /\ dirty' = c.dirty /\ elig' = c.elig /\ barrier' = c.barrier /\ chan' = c.chan
    /\ count' = c.count /\ nonInline' = c.nonInline /\ held' = c.held
    /\ used' = c.used /\ inflight' = c.inflight /\ adm' = c.adm

\* InlineClaimLease::mark_dirty
Mark(c) == [c EXCEPT !.dirty = c.dirty + 1, !.elig = FALSE]

\* ShardPool::notify_dirty: the mark is sent, or kept as a barrier while
\* inline claims are pending.
Notify(c) ==
    LET m == Mark(c) IN
    IF m.count = 0
    THEN [m EXCEPT !.barrier = 0, !.chan = Append(m.chan, Msg("NotifyDirty", m.dirty, 0))]
    ELSE [m EXCEPT !.barrier = m.dirty, !.chan = Append(m.chan, Msg("NotifyDirty", 0, 0))]

\* InlineClaimOracle::mark_ineligible: a barrier recorded, nothing sent.
MarkIneligible(c) == LET m == Mark(c) IN [m EXCEPT !.barrier = m.dirty]

\* ShardPool::flush_dirty_barrier_if_pending, when no inline claim is pending.
FlushIfIdle(c) ==
    IF c.count = 0 /\ c.barrier # 0
    THEN [c EXCEPT !.chan = Append(c.chan, Msg("DirtyBarrier", c.barrier, 0)), !.barrier = 0]
    ELSE c

SendInline(c, p) == [c EXCEPT !.chan = Append(c.chan, Msg("InlineCommitted", 0, p))]

\* An inline admission of the session whose outcome is unknown.
Unresolved(c) == \E q \in Pubs : kind[q] = "inline" /\ c.adm[q] = "unknown"

\* The held claims stay in the order of their admission.
InsertSorted(s, p) ==
    LET k == Cardinality({i \in 1..Len(s) : s[i] < p}) IN
    SubSeq(s, 1, k) \o << p >> \o SubSeq(s, k + 1, Len(s))

RECURSIVE SendAll(_, _)
SendAll(c, s) == IF s = << >> THEN c ELSE SendAll(SendInline(c, Head(s)), Tail(s))

\* Once no outcome of the session is unknown, the held claims are sent, in
\* order, before any barrier.
ReleaseHeld(c) ==
    IF ~Unresolved(c) THEN [SendAll(c, c.held) EXCEPT !.held = << >>] ELSE c

\* What the coordinator does when it learns the outcome of p: the step of
\* its store result (Owner::apply_draft) and what the runtime does after it.
Learn(c, p, out) ==
    IF kind[p] = "inline" THEN
        IF out = "committed" THEN
            LET c1 == [c EXCEPT !.adm[p] = "committed", !.count = c.count - 1]
                c2 == IF HoldBehind /\ (Unresolved(c1) \/ c1.held # << >>)
                      THEN [c1 EXCEPT !.held = InsertSorted(c1.held, p)]
                      ELSE SendInline(c1, p)
                c3 == IF HoldBehind THEN ReleaseHeld(c2) ELSE c2
            IN IF Mutation = "barrier-before-release" THEN ReleaseHeld(FlushIfIdle(c2))
               ELSE FlushIfIdle(c3)
        ELSE
            \* InlineClaimOracle::rollback, InlineClaimLease::release_pi
            LET c1 == [c EXCEPT !.adm[p] = "rejected", !.count = c.count - 1,
                                !.used = c.used \ {pid[p]},
                                !.inflight = IF pid[p] \in c.used THEN c.inflight - 1
                                             ELSE c.inflight]
                c2 == IF HoldBehind THEN ReleaseHeld(c1) ELSE c1
            IN IF Mutation = "barrier-before-release" THEN ReleaseHeld(FlushIfIdle(c1))
               ELSE FlushIfIdle(c2)
    ELSE
        IF out = "committed" THEN
            LET c1 == MarkIneligible([c EXCEPT !.adm[p] = "committed",
                                               !.nonInline = c.nonInline - 1])
            IN IF NotifyAfterStep THEN Notify(c1) ELSE c1
        ELSE FlushIfIdle([c EXCEPT !.adm[p] = "rejected", !.nonInline = c.nonInline - 1])

-----------------------------------------------------------------------------
Reservable ==
    dirty # 0 /\ acked = dirty /\ elig /\ inflight < Cap /\ Ids \ used # {}

\* Owner::plan_admission, delegated branch, and Owner::apply_change.
Admit ==
    /\ next <= N
    /\ LET p == next IN
       IF nonInline = 0 /\ Reservable THEN
           LET id == Min(Ids \ used) IN
           /\ kind' = [kind EXCEPT ![p] = "inline"]
           /\ pid' = [pid EXCEPT ![p] = id]
           /\ used' = used \cup {id}
           /\ inflight' = inflight + 1
           /\ count' = count + 1
           /\ UNCHANGED << dirty, elig, nonInline, barrier >>
       ELSE
           /\ kind' = [kind EXCEPT ![p] = "stored"]
           /\ dirty' = dirty + 1 /\ elig' = FALSE /\ barrier' = dirty + 1
           /\ nonInline' = nonInline + 1
           /\ UNCHANGED << pid, used, inflight, count >>
    /\ adm' = [adm EXCEPT ![next] = "submitted"]
    /\ next' = next + 1
    /\ UNCHANGED << applied, replies, store, replayed, acked, held, chan, shard >>

\* The store applies the admissions in order; its answer may be unknown.
StoreApply ==
    /\ applied < next - 1
    /\ LET p == applied + 1 IN
       \E out \in {"committed", "rejected"}, known \in (IF Unknown THEN BOOLEAN ELSE {TRUE}) :
           /\ store' = [store EXCEPT ![p] = out]
           /\ replies' = Append(replies, [p |-> p, known |-> known])
    /\ applied' = applied + 1
    /\ UNCHANGED << next, kind, pid, adm, replayed, dirty, acked, elig, inflight, used,
                    count, nonInline, barrier, held, chan, shard >>

\* A subscription replay commits a row of the session; the coordinator
\* notifies the shard once the SUBACK is queued.
ReplayCommit ==
    /\ Replay /\ ~replayed
    /\ replayed' = TRUE
    /\ store' = [store EXCEPT ![0] = "committed"]
    /\ SetC(Notify(C))
    /\ UNCHANGED << next, applied, kind, pid, replies, acked, shard >>

\* The coordinator reads the store's answer (Runtime::writer_results) and
\* steps it. Without NotifyAfterStep the targets of a committed admission
\* are notified when the answer arrives, before the step.
Reply ==
    /\ replies # << >>
    /\ LET r == Head(replies)
           p == r.p
           c1 == IF r.known /\ store[p] = "committed" /\ kind[p] = "stored" /\ ~NotifyAfterStep
                 THEN Notify(C) ELSE C
           c2 == IF r.known THEN Learn(c1, p, store[p])
                 ELSE [c1 EXCEPT !.adm[p] = "unknown"]
       IN SetC(c2)
    /\ replies' = Tail(replies)
    /\ UNCHANGED << next, applied, kind, pid, store, replayed, acked, shard >>

\* The resolution of an unknown answer (Effect::ResolveAndFence) names no
\* target.
Resolve ==
    /\ \E p \in Pubs :
        /\ adm[p] = "unknown"
        /\ SetC(Learn(C, p, store[p]))
    /\ UNCHANGED << next, applied, kind, pid, replies, store, replayed, acked, shard >>

-----------------------------------------------------------------------------
\* DeliveryShardWorker::drain_coord_messages
Recv ==
    /\ chan # << >>
    /\ LET m == Head(chan) IN
       /\ chan' = Tail(chan)
       /\ CASE m.t \in {"Attach", "NotifyDirty"} ->
                /\ published' = FALSE
                /\ seen' = IF m.s > 0 THEN Max2(seen, m.s) ELSE 0
                /\ fenced' = (m.s = 0)
                /\ coreDirty' = TRUE
                /\ foreign' = IF ForeignIds THEN used ELSE {}
                /\ UNCHANGED pendingInl
            [] m.t = "DirtyBarrier" ->
                /\ published' = FALSE
                /\ seen' = Max2(seen, m.s)
                /\ fenced' = FALSE
                /\ coreDirty' = TRUE
                /\ foreign' = IF ForeignIds THEN used ELSE {}
                /\ UNCHANGED pendingInl
            [] m.t = "InlineCommitted" ->
                /\ pendingInl' = Append(pendingInl, m.p)
                /\ UNCHANGED << published, seen, fenced, coreDirty, foreign >>
    /\ UNCHANGED << next, applied, kind, pid, adm, replies, store, replayed,
                    dirty, acked, elig, inflight, used, count, nonInline, barrier, held,
                    scanned, rows, rowPi, sent, emitted >>

\* DeliveryShardWorker::drain_pending_inline, Owner::step_shard_inline_committed:
\* a delivery the core holds is left as it is; any other is inserted.
ApplyInline ==
    /\ pendingInl # << >>
    /\ LET p == Head(pendingInl) IN
       /\ pendingInl' = Tail(pendingInl)
       /\ IF p \in rows
          THEN UNCHANGED << rows, rowPi, sent >>
          ELSE /\ rows' = rows \cup {p}
               /\ rowPi' = [rowPi EXCEPT ![p] = pid[p]]
               /\ sent' = sent \ {p}
    /\ UNCHANGED << coord, seen, scanned, published, fenced, coreDirty, foreign, emitted >>

\* A read of the session's committed rows, in order. A stored row is claimed
\* with an identifier beside the rows (Deliveries::free_packet_id); a row the
\* coordinator claimed is taken with its identifier.
RECURSIVE LoadFrom(_, _)
LoadFrom(st, k) ==
    IF k > N THEN st
    ELSE IF store[k] # "committed" \/ k \in st.rows THEN LoadFrom(st, k + 1)
    ELSE IF kind[k] = "inline"
         THEN LoadFrom([st EXCEPT !.rows = st.rows \cup {k}, !.pi[k] = pid[k]], k + 1)
    ELSE LET free == Ids \ ({st.pi[r] : r \in st.rows} \cup foreign) IN
         IF free = {} THEN [st EXCEPT !.caught = FALSE]
         ELSE LoadFrom([st EXCEPT !.rows = st.rows \cup {k}, !.pi[k] = Min(free)], k + 1)

ScanAllowed ==
    /\ coreDirty
    /\ ScanGate => ~fenced
    /\ (ScanGate /\ Mutation # "gate-ignores-waiting-claims") => pendingInl = << >>

Scan ==
    /\ ScanAllowed
    /\ LET st == LoadFrom([rows |-> rows, pi |-> rowPi, caught |-> TRUE], 0) IN
       /\ rows' = st.rows
       /\ rowPi' = st.pi
       /\ scanned' = IF st.caught THEN seen ELSE scanned
       /\ coreDirty' = ~st.caught
    /\ UNCHANGED << coord, seen, published, fenced, pendingInl, foreign, sent, emitted >>

\* The shard sends its oldest unsent row.
Emit ==
    /\ rows \ sent # {}
    /\ LET p == Min(rows \ sent) IN
       /\ sent' = sent \cup {p}
       /\ emitted' = Append(emitted, p)
    /\ UNCHANGED << coord, seen, scanned, published, fenced, coreDirty, pendingInl,
                    rows, rowPi, foreign >>

\* The subscriber acknowledges; the delivery is finished and its identifier
\* given back to a published lease (InlineClaimLease::release_pi).
Ack ==
    /\ \E p \in rows \cap sent :
        /\ rows' = rows \ {p}
        /\ store' = [store EXCEPT ![p] = IF @ = "committed" THEN "finished" ELSE @]
        /\ IF published /\ rowPi[p] \in used
           THEN /\ used' = used \ {rowPi[p]}
                /\ inflight' = inflight - 1
           ELSE UNCHANGED << used, inflight >>
    /\ UNCHANGED << next, applied, kind, pid, adm, replies, replayed, dirty, acked, elig,
                    count, nonInline, barrier, held, chan,
                    seen, scanned, published, fenced, coreDirty, pendingInl, rowPi,
                    foreign, sent, emitted >>

\* DeliveryShardWorker::try_publish_lease, InlineClaimLease::publish_eligible
TryPublish ==
    /\ ~published /\ seen # 0 /\ scanned >= seen /\ ~coreDirty
    /\ ForeignIds => pendingInl = << >>
    /\ used' = {rowPi[r] : r \in rows}
    /\ inflight' = Cardinality(rows)
    /\ elig' = TRUE
    /\ acked' = seen
    /\ published' = TRUE
    /\ foreign' = {}
    /\ UNCHANGED << next, applied, kind, pid, adm, replies, store, replayed, dirty,
                    count, nonInline, barrier, held, chan,
                    seen, scanned, fenced, coreDirty, pendingInl, rows, rowPi, sent, emitted >>

Terminal ==
    /\ next > N
    /\ ~ENABLED (StoreApply \/ ReplayCommit \/ Reply \/ Resolve \/ Recv \/ ApplyInline
                 \/ Scan \/ Emit \/ Ack \/ TryPublish)

\* Nothing more happens. Deadlock checking stays on: a state where no other
\* action is enabled and that is not Terminal is a deadlock.
Done == Terminal /\ UNCHANGED vars

Next ==
    \/ Admit \/ StoreApply \/ ReplayCommit \/ Reply \/ Resolve
    \/ Recv \/ ApplyInline \/ Scan \/ Emit \/ Ack \/ TryPublish \/ Done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
    /\ next \in 1..(N + 1) /\ applied \in 0..N
    /\ kind \in [Rows -> {"none", "inline", "stored"}]
    /\ adm \in [Rows -> {"none", "submitted", "unknown", "committed", "rejected"}]
    /\ store \in [Rows -> {"none", "committed", "rejected", "finished"}]
    /\ used \subseteq Ids /\ rows \subseteq Rows /\ sent \subseteq Rows
    /\ count \in Nat /\ nonInline \in Nat /\ inflight \in Nat

\* No two deliveries the shard holds share an identifier: a second would
\* fail the shard on its outgoing binding.
UniqueIds == \A p, q \in rows : p # q => rowPi[p] # rowPi[q]

Emitted(p) == \E i \in 1..Len(emitted) : emitted[i] = p
First(p) == CHOOSE i \in 1..Len(emitted) :
                emitted[i] = p /\ \A k \in 1..(i - 1) : emitted[k] # p

\* The subscriber receives the publications in the order of their admission.
InOrder == \A p, q \in Pubs : (p < q /\ Emitted(p) /\ Emitted(q)) => First(p) < First(q)

\* Nothing is sent twice on one connection.
NoDuplicate == \A i, j \in 1..Len(emitted) : i # j => emitted[i] # emitted[j]

\* When nothing more happens, every committed row was delivered...
TerminalDelivered == Terminal => \A p \in Rows : store[p] # "committed"

\* ... and the lease allows inline claims again.
TerminalEligible == Terminal => (acked = dirty /\ elig)
=============================================================================
