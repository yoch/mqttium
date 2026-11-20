---------------------------- MODULE MGroupCommit ----------------------------
(***************************************************************************)
(* MGroupCommit: several store commands in one transaction of the engine,  *)
(* as decision record 0045 (proposed) and                                  *)
(* crates/mqttiumd-store-sqlite/src/group.rs of its lot describe           *)
(* `SqliteStore::apply_group`. Specification: neither is on this branch.   *)
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
(*                                                                         *)
(* An answer leaves the store only after COMMIT returned. A group that     *)
(* staged a write and was not committed answers PendingOrUnknown for       *)
(* every member. A process killed before the COMMIT is logged leaves no    *)
(* member; killed after it, every member.                                  *)
(*                                                                         *)
(* The model takes from the engine that COMMIT is atomic, that ROLLBACK    *)
(* TO restores the state at its SAVEPOINT, and that a transaction which    *)
(* is not committed leaves nothing. An operation has one of four durable   *)
(* states; rows, quotas and the meta revision are folded into it. The      *)
(* answers are handed over one member at a time: the store returns them    *)
(* together, the writer that will deliver them is another lot. The         *)
(* history of the verifier (prefix h) is read by no guard.                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    NumMembers, \* commands of the group
    Repeats,    \* members that present the operation of member 1 again
    Settled,    \* operations already committed before the group
    Faults,     \* reads, writes and the commit may fail, the engine may drop
    Mutation    \* "none" or the name of a model mutation

Mutations == {"none", "ReleaseBeforeCommit", "RefusalAbortsGroup", "ReleaseAtSavepoint"}

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
    durable,   \* per operation: "absent", "applied" or "rejected", on disk
    staged,    \* per operation: what the open transaction holds
    sp,        \* `staged` at the SAVEPOINT of the member in progress
    pc,        \* the store worker
    cur,       \* the member in progress
    reply,     \* per member: the answer computed, or "none"
    wrote,     \* per member: it staged something (Applied::wrote)
    released,  \* per member: the answer handed to the caller, or "none"
    hVerdict,  \* per member: what staging decided
    hCommit,   \* "none", "ok" or "failed"
    hCrashAt   \* where the process was killed, or "none"

vars == <<durable, staged, sp, pc, cur, reply, wrote, released, hVerdict, hCommit, hCrashAt>>

InTransaction == {"member", "undo", "reject", "undoFault", "decide", "prepared"}
Decided(o) == staged[o] \in {"applied", "rejected"}
AnswerOf(o) == IF staged[o] = "applied" THEN "Committed" ELSE "Rejected"

Init ==
    /\ durable = InitDurable /\ staged = InitDurable /\ sp = InitDurable
    /\ pc = "idle" /\ cur = 1
    /\ reply = [m \in Members |-> "none"]
    /\ wrote = [m \in Members |-> FALSE]
    /\ released = [m \in Members |-> "none"]
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
    /\ UNCHANGED <<durable, staged, sp, cur, reply, wrote, released, hVerdict, hCommit, hCrashAt>>

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
    /\ UNCHANGED <<durable, hCommit, hCrashAt>>

\* ROLLBACK TO member: the refused member is undone alone.
\* RefusalAbortsGroup rolls the whole transaction back and goes on.
MemberUndo ==
    /\ pc = "undo"
    /\ staged' = IF Mutation = "RefusalAbortsGroup" THEN durable ELSE sp
    /\ pc' = "reject"
    /\ UNCHANGED <<durable, sp, cur, reply, wrote, released, hVerdict, hCommit, hCrashAt>>

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
    /\ UNCHANGED <<durable, sp, released, hVerdict, hCommit, hCrashAt>>

\* A member that could not be read or written is rolled back to its
\* savepoint and answered PendingOrUnknown; the group goes on.
MemberUndoFault ==
    /\ pc = "undoFault"
    /\ staged' = sp
    /\ Answer("Unknown", FALSE)
    /\ UNCHANGED <<durable, sp, released, hVerdict, hCommit, hCrashAt>>

\* The engine dropped the transaction on an error, or a savepoint could
\* not be closed: the group is lost and every member is unknown.
EngineDrops ==
    /\ Faults /\ pc \in {"member", "undo", "reject", "undoFault"}
    /\ staged' = durable
    /\ reply' = AllUnknown
    /\ pc' = "reply"
    /\ UNCHANGED <<durable, sp, cur, wrote, released, hVerdict, hCommit, hCrashAt>>

\* Every member ran. A group that staged nothing has nothing to commit:
\* its answers were read from what was already durable.
Decide ==
    /\ pc = "decide"
    /\ IF \E m \in Members : wrote[m]
       THEN pc' = "prepared" /\ UNCHANGED staged
       ELSE pc' = "reply" /\ staged' = durable
    /\ UNCHANGED <<durable, sp, cur, reply, wrote, released, hVerdict, hCommit, hCrashAt>>

\* COMMIT, after the point K1 `store.commit.prepared`. It makes every
\* staged member durable at once, or fails (the medium is failing, the
\* engine refuses): then nothing is durable and every member is unknown.
\* ReleaseBeforeCommit calls it once the answers have left.
Commit ==
    /\ pc = "prepared"
    /\ Mutation = "ReleaseBeforeCommit" => \A m \in Members : released[m] # "none"
    /\ \E ok \in (IF Faults THEN BOOLEAN ELSE {TRUE}) :
          IF ok
          THEN /\ durable' = staged /\ hCommit' = "ok"
               /\ UNCHANGED <<staged, reply>>
          ELSE /\ staged' = durable /\ hCommit' = "failed"
               /\ reply' = AllUnknown
               /\ UNCHANGED durable
    /\ pc' = "reply"
    /\ UNCHANGED <<sp, cur, wrote, released, hVerdict, hCrashAt>>

\* One answer is handed to the caller. In the decision this happens only
\* once COMMIT returned; ReleaseBeforeCommit does it with the group
\* staged and COMMIT not called.
Release(m) ==
    /\ pc = "reply" \/ (Mutation = "ReleaseBeforeCommit" /\ pc = "prepared")
    /\ released[m] = "none" /\ reply[m] # "none"
    /\ \A k \in Members : k < m => released[k] # "none"
    /\ released' = [released EXCEPT ![m] = reply[m]]
    /\ UNCHANGED <<durable, staged, sp, pc, cur, reply, wrote, hVerdict, hCommit, hCrashAt>>

\* Every answer was handed over.
Finish ==
    /\ pc = "reply" /\ \A m \in Members : released[m] # "none"
    /\ pc' = "done"
    /\ UNCHANGED <<durable, staged, sp, cur, reply, wrote, released, hVerdict, hCommit, hCrashAt>>

\* The process is killed. The open transaction is gone; what is on disk
\* stays; the answers not yet handed over never are.
Crash ==
    /\ pc \in InTransaction \cup {"reply"}
    /\ hCrashAt' = pc
    /\ pc' = "crashed"
    /\ staged' = durable
    /\ UNCHANGED <<durable, sp, cur, reply, wrote, released, hVerdict, hCommit>>

Terminal == pc \in {"done", "crashed"}

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
    /\ durable \in [Ops -> States \ {"partial"}]
    /\ staged \in [Ops -> States] /\ sp \in [Ops -> States]
    /\ pc \in InTransaction \cup {"idle", "reply", "done", "crashed"}
    /\ cur \in 1..(NumMembers + 1)
    /\ reply \in [Members -> Answers \cup {"none"}]
    /\ wrote \in [Members -> BOOLEAN]
    /\ released \in [Members -> Answers \cup {"none"}]
    /\ hVerdict \in [Members -> {"none", "accept", "refuse", "fault", "recognised"}]
    /\ hCommit \in {"none", "ok", "failed"}
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

\* The group is atomic: what is on disk is what was there before the
\* group, or, once it committed, that and every staged member.
GroupAtomic ==
    /\ hCommit # "ok" => durable = InitDurable
    /\ \A o \in Ops : InitDurable[o] # "absent" => durable[o] = InitDurable[o]

\* A kill before the COMMIT loses the whole group, and no answer was
\* handed over for it.
CrashBeforeCommitLosesGroup ==
    hCrashAt \in InTransaction =>
        durable = InitDurable /\ \A m \in Members : released[m] = "none"

\* A commit that failed makes every member unknown, never positive, and
\* leaves nothing.
CommitFailureSaysNothingCertain ==
    hCommit = "failed" =>
        /\ durable = InitDurable
        /\ \A m \in Members : released[m] \in {"none", "Unknown"}

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

=============================================================================
