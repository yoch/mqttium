---------------------------- MODULE MGroupCommit ----------------------------
(***************************************************************************)
(* MGroupCommit: several store commands in one transaction of the engine,  *)
(* as written in crates/mqttiumd-store-sqlite/src/group.rs                 *)
(* (`SqliteStore::apply_group`, `member`, `savepoint`; decision record     *)
(* 0045) and delivered by the store worker of                              *)
(* crates/mqttiumd/src/writer.rs (`group_responses`).                      *)
(*                                                                         *)
(*   BEGIN IMMEDIATE                              Begin                    *)
(*   for each command, in order:                                           *)
(*     recognise an operation already decided     MemberStage              *)
(*     SAVEPOINT member; guards and rows                                   *)
(*     accepted: RELEASE member                                            *)
(*     refused:  ROLLBACK TO member; RELEASE      MemberUndo               *)
(*               SAVEPOINT member; the rejection                           *)
(*               receipt; RELEASE                 MemberReject             *)
(*     fault:    ROLLBACK TO member; RELEASE      MemberUndoFault          *)
(*   nothing staged: return the answers           Decide                   *)
(*   K1 store.commit.prepared; COMMIT             Commit                   *)
(*   return the answers                           Release                  *)
(*   the caller, for a member it knows nothing certain about:              *)
(*     resolve_and_fence by its identity          Resolve                  *)
(*     the same command presented again           Represent                *)
(*                                                                         *)
(* An answer leaves the store only after COMMIT returned. A group that     *)
(* staged a write and was not committed answers PendingOrUnknown for       *)
(* every member. A process killed before the COMMIT is logged leaves no    *)
(* member; killed after it, every member.                                  *)
(*                                                                         *)
(* COMMIT has three results. It returns and the group is durable. It       *)
(* fails and nothing is durable. Or the group is durable and the call      *)
(* returns an error: the engine reports the result of its write-ahead-log  *)
(* callback, which runs after the commit was logged. The store cannot      *)
(* tell the last two apart and answers PendingOrUnknown for every member   *)
(* in both. The caller then reconciles each member by its identity; that   *)
(* never gives an operation a second effect.                               *)
(*                                                                         *)
(* The model takes from the engine that COMMIT is atomic, that ROLLBACK    *)
(* TO restores the state at its SAVEPOINT, and that a transaction which    *)
(* is not committed leaves nothing. An operation has one of four durable   *)
(* states; rows, quotas and the meta revision are folded into it. The      *)
(* store returns the answers together; the worker publishes them one       *)
(* member at a time, in the order of the requests. How the worker forms    *)
(* a group from its request lane is not here. The history of the verifier  *)
(* (prefix h, and `effects`) is read by no guard of the store or of the    *)
(* worker; the mutation UncertainIsNotCommitted alone reads hCommit, to    *)
(* model a caller that believes the error of the commit call.              *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    NumMembers, \* commands of the group
    Repeats,    \* members that present the operation of member 1 again
    Settled,    \* operations already committed before the group
    Faults,     \* reads, writes and the commit may fail, the engine may drop
    Mutation    \* "none" or the name of a model mutation

Mutations == {"none", "ReleaseBeforeCommit", "RefusalAbortsGroup", "ReleaseAtSavepoint",
              "UncertainIsNotCommitted"}

ASSUME /\ NumMembers \in Nat \ {0}
       /\ Repeats \subseteq 2..NumMembers /\ Settled \subseteq 1..NumMembers
       /\ Faults \in BOOLEAN
       /\ Mutation \in Mutations

Members == 1..NumMembers
\* The OperationId of each member: its own, or that of member 1.
OpOf == [m \in Members |-> IF m \in Repeats THEN 1 ELSE m]
Ops == Members

Answers == {"Committed", "Rejected", "Unknown"}
InitDurable == [o \in Ops |-> IF o \in Settled THEN "applied" ELSE "absent"]
AllUnknown == [m \in Members |-> "Unknown"]

VARIABLES
    durable,   \* per operation: "absent", "applied", "rejected" or "fenced", on disk
    staged,    \* per operation: what the open transaction holds
    sp,        \* `staged` at the SAVEPOINT of the member in progress
    pc,        \* the store worker
    cur,       \* the member in progress
    reply,     \* per member: the answer computed, or "none"
    wrote,     \* per member: it staged something (Applied::wrote)
    released,  \* per member: the answer handed to the caller, or "none"
    known,     \* per member: what the caller learnt by reconciling, or "none"
    effects,   \* per operation: how many times its effect was made durable
    hVerdict,  \* per member: what staging decided
    hCommit,   \* "none", "ok", "failed" or "uncertain"
    hCrashAt   \* where the process was killed, or "none"

vars == <<durable, staged, sp, pc, cur, reply, wrote, released, known, effects,
          hVerdict, hCommit, hCrashAt>>

InTransaction == {"member", "undo", "reject", "undoFault", "decide", "prepared"}
Decided(o) == staged[o] \in {"applied", "rejected"}
AnswerOf(o) == IF staged[o] = "applied" THEN "Committed" ELSE "Rejected"

Init ==
    /\ durable = InitDurable /\ staged = InitDurable /\ sp = InitDurable
    /\ pc = "idle" /\ cur = 1
    /\ reply = [m \in Members |-> "none"]
    /\ wrote = [m \in Members |-> FALSE]
    /\ released = [m \in Members |-> "none"]
    /\ known = [m \in Members |-> "none"]
    /\ effects = [o \in Ops |-> IF o \in Settled THEN 1 ELSE 0]
    /\ hVerdict = [m \in Members |-> "none"]
    /\ hCommit = "none" /\ hCrashAt = "none"

\* The member in progress is answered and the next one begins.
Answer(a, w) ==
    /\ reply' = [reply EXCEPT ![cur] = a]
    /\ wrote' = [wrote EXCEPT ![cur] = w]
    /\ cur' = cur + 1
    /\ pc' = IF cur = NumMembers THEN "decide" ELSE "member"

\* BEGIN IMMEDIATE.
Begin ==
    /\ pc = "idle"
    /\ pc' = "member"
    /\ UNCHANGED <<durable, staged, sp, cur, reply, wrote, released, known, effects,
                   hVerdict, hCommit, hCrashAt>>

\* One member: the recognition reads see the members before it, so an
\* operation presented twice is decided once. Otherwise a savepoint is
\* opened and the command staged: accepted and released into the group,
\* refused by a guard, or stopped by a fault after some rows were
\* written. ReleaseAtSavepoint hands the answer over as soon as the
\* savepoint is released.
MemberStage ==
    /\ pc = "member"
    /\ LET o == OpOf[cur] IN
       IF Decided(o)
       THEN /\ Answer(AnswerOf(o), FALSE)
            /\ hVerdict' = [hVerdict EXCEPT ![cur] = "recognised"]
            /\ UNCHANGED <<staged, sp, released>>
       ELSE \E v \in (IF Faults THEN {"accept", "refuse", "fault"} ELSE {"accept", "refuse"}) :
            /\ sp' = staged
            /\ hVerdict' = [hVerdict EXCEPT ![cur] = v]
            /\ IF v = "accept"
               THEN /\ staged' = [staged EXCEPT ![o] = "applied"]
                    /\ Answer("Committed", TRUE)
                    /\ released' = IF Mutation = "ReleaseAtSavepoint"
                                   THEN [released EXCEPT ![cur] = "Committed"]
                                   ELSE released
               ELSE /\ staged' = [staged EXCEPT ![o] = "partial"]
                    /\ pc' = IF v = "refuse" THEN "undo" ELSE "undoFault"
                    /\ UNCHANGED <<cur, reply, wrote, released>>
    /\ UNCHANGED <<durable, known, effects, hCommit, hCrashAt>>

\* ROLLBACK TO member: the refused member is undone alone.
\* RefusalAbortsGroup rolls the whole transaction back and goes on.
MemberUndo ==
    /\ pc = "undo"
    /\ staged' = IF Mutation = "RefusalAbortsGroup" THEN durable ELSE sp
    /\ pc' = "reject"
    /\ UNCHANGED <<durable, sp, cur, reply, wrote, released, known, effects,
                   hVerdict, hCommit, hCrashAt>>

\* The rejection is recorded on a receipt, in a savepoint of its own.
\* Without a receipt slot nothing is staged and nothing certain is said.
MemberReject ==
    /\ pc = "reject"
    /\ \E slot \in (IF Faults THEN BOOLEAN ELSE {TRUE}) :
          IF slot
          THEN /\ staged' = [staged EXCEPT ![OpOf[cur]] = "rejected"]
               /\ Answer("Rejected", TRUE)
          ELSE /\ Answer("Unknown", FALSE)
               /\ UNCHANGED staged
    /\ UNCHANGED <<durable, sp, released, known, effects, hVerdict, hCommit, hCrashAt>>

\* A member that could not be read or written is rolled back to its
\* savepoint and answered PendingOrUnknown; the group goes on.
MemberUndoFault ==
    /\ pc = "undoFault"
    /\ staged' = sp
    /\ Answer("Unknown", FALSE)
    /\ UNCHANGED <<durable, sp, released, known, effects, hVerdict, hCommit, hCrashAt>>

\* The engine dropped the transaction on an error, or a savepoint could
\* not be closed: the group is lost and every member is unknown.
EngineDrops ==
    /\ Faults /\ pc \in {"member", "undo", "reject", "undoFault"}
    /\ staged' = durable
    /\ reply' = AllUnknown
    /\ pc' = "reply"
    /\ UNCHANGED <<durable, sp, cur, wrote, released, known, effects, hVerdict, hCommit, hCrashAt>>

\* Every member ran. A group that staged nothing has nothing to commit:
\* its answers were read from what was already durable.
Decide ==
    /\ pc = "decide"
    /\ IF \E m \in Members : wrote[m]
       THEN pc' = "prepared" /\ UNCHANGED staged
       ELSE pc' = "reply" /\ staged' = durable
    /\ UNCHANGED <<durable, sp, cur, reply, wrote, released, known, effects,
                   hVerdict, hCommit, hCrashAt>>

\* COMMIT, after the point K1 `store.commit.prepared`. It makes every
\* staged member durable at once ("ok"), or fails (the medium is failing,
\* the engine refuses): then nothing is durable and every member is
\* unknown ("failed"). Or it makes every staged member durable and its call
\* returns an error ("uncertain"): the store sees a commit that failed and
\* answers every member unknown, with the group on disk.
\* ReleaseBeforeCommit calls it once the answers have left.
Commit ==
    /\ pc = "prepared"
    /\ Mutation = "ReleaseBeforeCommit" => \A m \in Members : released[m] # "none"
    /\ \E result \in (IF Faults THEN {"ok", "failed", "uncertain"} ELSE {"ok"}) :
          /\ hCommit' = result
          /\ IF result = "failed"
             THEN /\ staged' = durable
                  /\ UNCHANGED <<durable, effects>>
             ELSE /\ durable' = staged
                  /\ effects' = [o \in Ops |->
                                    IF staged[o] = "applied" /\ durable[o] # "applied"
                                    THEN effects[o] + 1 ELSE effects[o]]
                  /\ UNCHANGED staged
          /\ reply' = IF result = "ok" THEN reply ELSE AllUnknown
    /\ pc' = "reply"
    /\ UNCHANGED <<sp, cur, wrote, released, known, hVerdict, hCrashAt>>

\* One answer is published to the core, in the order of the requests
\* (`rtx.try_send` in the worker loop). This happens only once
\* `apply_group` returned, so after COMMIT; ReleaseBeforeCommit does it
\* with the group staged and COMMIT not called.
Release(m) ==
    /\ pc = "reply" \/ (Mutation = "ReleaseBeforeCommit" /\ pc = "prepared")
    /\ released[m] = "none" /\ reply[m] # "none"
    /\ \A k \in Members : k < m => released[k] # "none"
    /\ released' = [released EXCEPT ![m] = reply[m]]
    /\ UNCHANGED <<durable, staged, sp, pc, cur, reply, wrote, known, effects,
                   hVerdict, hCommit, hCrashAt>>

\* Every answer was handed over.
Finish ==
    /\ pc = "reply" /\ \A m \in Members : released[m] # "none"
    /\ pc' = "done"
    /\ UNCHANGED <<durable, staged, sp, cur, reply, wrote, released, known, effects,
                   hVerdict, hCommit, hCrashAt>>

\* The process is killed. The open transaction is gone; what is on disk
\* stays; the answers not yet handed over never are.
Crash ==
    /\ pc \in InTransaction \cup {"reply"}
    /\ hCrashAt' = pc
    /\ pc' = "crashed"
    /\ staged' = durable
    /\ UNCHANGED <<durable, sp, cur, reply, wrote, released, known, effects, hVerdict, hCommit>>

Terminal == pc \in {"done", "crashed"}

\* The call of the group is over, by its last answer or by a kill, and the
\* caller holds nothing certain about member m: its answer was unknown or
\* never arrived, and it has not been reconciled yet.
Unsettled(m) == Terminal /\ released[m] \in {"none", "Unknown"} /\ known[m] = "none"

\* What a durable state tells the caller.
Knowledge(state) ==
    CASE state = "applied" -> "Committed"
      [] state = "rejected" -> "Rejected"
      [] state = "fenced" -> "Fenced"

\* `resolve_and_fence` on the identity of a member: a durable decision is
\* returned as it is and nothing is written; an absence is fenced, so the
\* operation never executes.
Resolve(m) ==
    /\ Unsettled(m)
    /\ LET o == OpOf[m]
           found == IF durable[o] = "absent" THEN "fenced" ELSE durable[o]
       IN /\ durable' = [durable EXCEPT ![o] = found]
          /\ known' = [known EXCEPT ![m] = Knowledge(found)]
    /\ UNCHANGED <<staged, sp, pc, cur, reply, wrote, released, effects,
                   hVerdict, hCommit, hCrashAt>>

\* The same command presented again, alone or in a later group: the
\* recognition reads answer a decided operation from its receipt and write
\* nothing; an operation that is absent is decided as a first submission.
\* UncertainIsNotCommitted believes the error of the commit call: what the
\* group left is taken for absent and applied again.
Represent(m) ==
    /\ Unsettled(m)
    /\ LET o == OpOf[m]
           seen == IF Mutation = "UncertainIsNotCommitted" /\ hCommit = "uncertain"
                   THEN "absent" ELSE durable[o]
       IN IF seen = "absent"
          THEN /\ durable' = [durable EXCEPT ![o] = "applied"]
               /\ effects' = [effects EXCEPT ![o] = @ + 1]
               /\ known' = [known EXCEPT ![m] = "Committed"]
          ELSE /\ known' = [known EXCEPT ![m] = Knowledge(seen)]
               /\ UNCHANGED <<durable, effects>>
    /\ UNCHANGED <<staged, sp, pc, cur, reply, wrote, released, hVerdict, hCommit, hCrashAt>>

Quiescent == Terminal /\ UNCHANGED vars

Next ==
    \/ Begin
    \/ MemberStage
    \/ MemberUndo
    \/ MemberReject
    \/ MemberUndoFault
    \/ EngineDrops
    \/ Decide
    \/ Commit
    \/ \E m \in Members : Release(m)
    \/ Finish
    \/ Crash
    \/ \E m \in Members : Resolve(m)
    \/ \E m \in Members : Represent(m)
    \/ Quiescent

Spec == Init /\ [][Next]_vars

\* The worker takes the steps it has. Nothing is assumed about faults and
\* kills.
WorkerStep ==
    \/ Begin \/ MemberStage \/ MemberUndo \/ MemberReject \/ MemberUndoFault
    \/ Decide \/ Commit \/ Finish \/ \E m \in Members : Release(m)

FairSpec == Spec /\ WF_vars(WorkerStep)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

States == {"absent", "partial", "applied", "rejected"}

TypeOK ==
    /\ durable \in [Ops -> (States \ {"partial"}) \cup {"fenced"}]
    /\ staged \in [Ops -> States] /\ sp \in [Ops -> States]
    /\ pc \in InTransaction \cup {"idle", "reply", "done", "crashed"}
    /\ cur \in 1..(NumMembers + 1)
    /\ reply \in [Members -> Answers \cup {"none"}]
    /\ wrote \in [Members -> BOOLEAN]
    /\ released \in [Members -> Answers \cup {"none"}]
    /\ known \in [Members -> {"none", "Committed", "Rejected", "Fenced"}]
    /\ effects \in [Ops -> 0..(NumMembers + 1)]
    /\ hVerdict \in [Members -> {"none", "accept", "refuse", "fault", "recognised"}]
    /\ hCommit \in {"none", "ok", "failed", "uncertain"}
    /\ hCrashAt \in InTransaction \cup {"none", "reply"}

\* No acknowledgement before commit: an answer that says committed or
\* rejected is handed over only for a fact that is on disk.
AckAfterCommit ==
    \A m \in Members :
        /\ released[m] = "Committed" => durable[OpOf[m]] = "applied"
        /\ released[m] = "Rejected" => durable[OpOf[m]] = "rejected"

\* While the transaction of the group is open no answer has left.
NothingReleasedInTransaction ==
    pc \in InTransaction => \A m \in Members : released[m] = "none"

\* Until the caller reconciles a member, the disk holds what the group
\* left; a reconciliation may then fence or decide an absent operation.
Unreconciled == \A m \in Members : known[m] = "none"

\* The group is atomic: what is on disk is what was there before the
\* group, or, once it committed, that and every staged member.
GroupAtomic ==
    /\ (hCommit \notin {"ok", "uncertain"} /\ Unreconciled) => durable = InitDurable
    /\ \A o \in Ops : InitDurable[o] # "absent" => durable[o] = InitDurable[o]

\* A kill before the COMMIT loses the whole group, and no answer was
\* handed over for it.
CrashBeforeCommitLosesGroup ==
    hCrashAt \in InTransaction =>
        /\ Unreconciled => durable = InitDurable
        /\ \A m \in Members : released[m] = "none"

\* A commit that failed makes every member unknown, never positive, and
\* leaves nothing.
CommitFailureSaysNothingCertain ==
    hCommit = "failed" =>
        /\ Unreconciled => durable = InitDurable
        /\ \A m \in Members : released[m] \in {"none", "Unknown"}

\* A commit that is durable and whose call returned an error says nothing
\* certain either: every answer handed over is unknown, for members that
\* are all on disk. The caller learns their outcome by reconciling only.
UncertainCommitSaysNothingCertain ==
    hCommit = "uncertain" =>
        /\ \A m \in Members : released[m] \in {"none", "Unknown"}
        /\ \A m \in Members : hVerdict[m] = "accept" => durable[OpOf[m]] = "applied"

\* What a reconciliation tells the caller is what the disk holds, and it
\* is told only about a member whose answer said nothing certain.
ReconciledFromDisk ==
    \A m \in Members :
        /\ known[m] = "Committed" => durable[OpOf[m]] = "applied"
        /\ known[m] = "Rejected" => durable[OpOf[m]] = "rejected"
        /\ known[m] = "Fenced" => durable[OpOf[m]] = "fenced"
        /\ known[m] # "none" => released[m] \in {"none", "Unknown"}

\* No operation takes effect twice: not through the group, and not through
\* the reconciliation of an answer that was unknown or lost. A fence closes
\* only an operation that never took effect.
NoSecondEffect ==
    \A o \in Ops :
        /\ effects[o] <= 1
        /\ durable[o] = "fenced" => effects[o] = 0

\* A refused or faulted member leaves none of its rows in the group.
NoPartialMemberCommitted ==
    pc \in {"decide", "prepared", "reply", "done"} =>
        \A o \in Ops : staged[o] # "partial"

\* A refused member is undone alone: in a committed group every accepted
\* member is applied, every recorded rejection is on disk, and every
\* computed answer agrees with the disk, as after the same commands
\* applied one after the other.
RefusalIsolated ==
    hCommit = "ok" =>
        \A m \in Members :
            /\ hVerdict[m] = "accept" => durable[OpOf[m]] = "applied"
            /\ reply[m] = "Committed" => durable[OpOf[m]] = "applied"
            /\ reply[m] = "Rejected" => durable[OpOf[m]] = "rejected"

(***************************************************************************)
(* Progress, under FairSpec: unless the process is killed, every member    *)
(* is answered.                                                            *)
(***************************************************************************)
EveryMemberAnswered ==
    (pc = "member") ~> (pc = "crashed" \/ \A m \in Members : released[m] # "none")

(***************************************************************************)
(* Non-vacuity, each the negation of an invariant TLC must report.         *)
(***************************************************************************)

\* A group commits with a refused member recorded between two applied
\* ones.
NoMixedGroupWitness ==
    ~(hCommit = "ok" /\ \E m, k \in Members : reply[m] = "Rejected" /\ reply[k] = "Committed")

\* The process is killed after the commit with some answers handed over
\* and some not: every member is on disk, the caller resolves the rest.
NoCrashAfterPartialReleaseWitness ==
    ~(pc = "crashed" /\ hCommit = "ok"
      /\ \E m, k \in Members : released[m] # "none" /\ released[k] = "none")

\* A member is recognised from an earlier member of the same group.
NoRecognisedInGroupWitness ==
    ~\E m \in Members : hVerdict[m] = "recognised" /\ OpOf[m] \notin Settled

\* A group that staged nothing returns its answers without a commit.
NoNothingToCommitWitness ==
    ~(pc = "done" /\ hCommit = "none" /\ \E m \in Members : released[m] = "Committed")

\* The commit of a group fails.
NoCommitFailureWitness == hCommit # "failed"

\* The commit of a group is durable and its call returns an error; the
\* caller, told nothing certain, finds every member committed.
NoUncertainCommitReconciledWitness ==
    ~(hCommit = "uncertain" /\ \A m \in Members : known[m] = "Committed")

\* The commit of a group fails and the caller fences a member of it.
NoFenceAfterFailureWitness ==
    ~(hCommit = "failed" /\ \E m \in Members : known[m] = "Fenced")

=============================================================================
