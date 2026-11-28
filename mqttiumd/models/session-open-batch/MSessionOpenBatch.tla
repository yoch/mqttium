------------------------- MODULE MSessionOpenBatch -------------------------
EXTENDS Integers, FiniteSets

(*
 * Bounded concurrent distinct-client_id session openings batched into one
 * Writer::apply_group transaction (reserve R4 of docs/missions/L11-optimisation.md,
 * amending ADR 0048 LIFECYCLE_IN_FLIGHT = 1).
 *
 * Distinct fresh / non-destructive-resume Open flights run concurrently up to
 * MaxConcurrent while exclusive lifecycle transitions (takeover, destructive
 * CleanStart, Close, Detach, Expire) run strictly serialized when flights = {}.
 *
 * Mutations:
 * - "none"                 : repaired concurrent-open policy
 * - "AllowSameClient"      : omits distinct client_id check
 * - "NoCapacityGuard"      : omits pending_fresh + sessions < Capacity guard
 * - "AllowExclusiveOverlap": permits Close alongside in-flight BatchOpen
 *)

CONSTANTS
    MaxClients,
    Capacity,
    MaxConcurrent,
    Mutation

VARIABLES
    store_sessions,
    owner_sessions,
    flights,
    next_op,
    batch_committed,
    unexpected_refusal

vars == <<
    store_sessions, owner_sessions, flights, next_op,
    batch_committed, unexpected_refusal
>>

Clients == 1..MaxClients
Ops == 1..6

FlightRecord == [
    op : Ops,
    client : Clients,
    kind : {"BatchOpen", "ExclusiveOpen", "ExclusiveClose"},
    creates : BOOLEAN,
    stage : {"Submitted", "Committed", "Refused", "Indeterminate"}
]

Init ==
    /\ store_sessions = {}
    /\ owner_sessions = {}
    /\ flights = {}
    /\ next_op = 1
    /\ batch_committed = FALSE
    /\ unexpected_refusal = FALSE

TypeOK ==
    /\ store_sessions \subseteq Clients
    /\ owner_sessions \subseteq Clients
    /\ flights \subseteq FlightRecord
    /\ next_op \in 1..7
    /\ batch_committed \in BOOLEAN
    /\ unexpected_refusal \in BOOLEAN

PendingFresh == Cardinality({ f \in flights : f.creates })
AllBatchSafe == \A f \in flights : f.kind = "BatchOpen"
DistinctClient(c) == \A f \in flights : f.client # c

SubmitBatchOpen ==
    /\ next_op \in Ops
    /\ Cardinality(flights) < MaxConcurrent
    /\ AllBatchSafe
    /\ \E c \in Clients :
        LET creates == c \notin owner_sessions IN
        /\ (Mutation = "AllowSameClient" \/ DistinctClient(c))
        /\ (Mutation = "NoCapacityGuard"
            \/ ~creates
            \/ Cardinality(owner_sessions) + PendingFresh < Capacity)
        /\ flights' = flights \cup {[
                op |-> next_op,
                client |-> c,
                kind |-> "BatchOpen",
                creates |-> creates,
                stage |-> "Submitted"
           ]}
        /\ next_op' = next_op + 1
        /\ UNCHANGED << store_sessions, owner_sessions, batch_committed, unexpected_refusal >>

\* Exclusive Open at capacity boundary (tests refusal) or Exclusive Close.
SubmitExclusiveOpen ==
    /\ next_op \in Ops
    /\ flights = {}
    /\ \E c \in (Clients \ owner_sessions) :
        /\ Cardinality(owner_sessions) = Capacity
        /\ flights' = {[
                op |-> next_op,
                client |-> c,
                kind |-> "ExclusiveOpen",
                creates |-> TRUE,
                stage |-> "Submitted"
           ]}
        /\ next_op' = next_op + 1
        /\ UNCHANGED << store_sessions, owner_sessions, batch_committed, unexpected_refusal >>

SubmitExclusiveClose ==
    /\ next_op \in Ops
    /\ (Mutation = "AllowExclusiveOverlap" \/ flights = {})
    /\ \E c \in owner_sessions :
        /\ (Mutation = "AllowExclusiveOverlap" \/ DistinctClient(c))
        /\ flights' = flights \cup {[
                op |-> next_op,
                client |-> c,
                kind |-> "ExclusiveClose",
                creates |-> FALSE,
                stage |-> "Submitted"
           ]}
        /\ next_op' = next_op + 1
        /\ UNCHANGED << store_sessions, owner_sessions, batch_committed, unexpected_refusal >>

\* Atomic group commit of 1..K submitted BatchOpen flights (Writer::apply_group).
GroupCommitBatch ==
    LET sub == { f \in flights : f.kind = "BatchOpen" /\ f.stage = "Submitted" } IN
    /\ sub # {}
    /\ \E batch \in (SUBSET sub \ {{}}) :
        \* Commit members sequentially inside one atomic transaction.
        \* In the finite model, either all distinct new clients fit in Capacity,
        \* or overflow members are rejected for SessionCapacityExceeded.
        LET new_clients == { f.client : f \in batch } \ store_sessions
            room == Capacity - Cardinality(store_sessions)
            all_fit == Cardinality(new_clients) <= room
            accepted_clients == IF all_fit THEN new_clients ELSE {}
            updated == {
                [f EXCEPT !.stage =
                    IF f.client \in store_sessions \/ f.client \in accepted_clients
                    THEN "Committed" ELSE "Refused"]
                : f \in batch
            }
        IN
            /\ store_sessions' = store_sessions \cup accepted_clients
            /\ flights' = (flights \ batch) \cup updated
            /\ batch_committed' = (batch_committed \/ Cardinality(batch) >= 2)
            /\ unexpected_refusal' = (unexpected_refusal \/ ~all_fit)
            /\ UNCHANGED << owner_sessions, next_op >>

CommitExclusive ==
    \E f \in flights :
        /\ f.kind # "BatchOpen"
        /\ f.stage = "Submitted"
        /\ IF f.kind = "ExclusiveClose" THEN
                /\ store_sessions' = store_sessions \ {f.client}
                /\ flights' = (flights \ {f}) \cup {[f EXCEPT !.stage = "Committed"]}
           ELSE \* ExclusiveOpen (at capacity -> refused cleanly)
                LET ok == (f.client \in store_sessions) \/ (Cardinality(store_sessions) < Capacity) IN
                /\ store_sessions' = IF ok THEN store_sessions \cup {f.client} ELSE store_sessions
                /\ flights' = (flights \ {f}) \cup {[f EXCEPT !.stage = IF ok THEN "Committed" ELSE "Refused"]}
        /\ UNCHANGED << owner_sessions, next_op, batch_committed, unexpected_refusal >>

TimeoutAndResolve ==
    \E f \in flights :
        /\ f.stage = "Submitted"
        /\ flights' = (flights \ {f}) \cup {[f EXCEPT !.stage = "Indeterminate"]}
        /\ UNCHANGED << store_sessions, owner_sessions, next_op, batch_committed, unexpected_refusal >>

ResolveIndeterminate ==
    \E f \in flights :
        /\ f.stage = "Indeterminate"
        /\ flights' = (flights \ {f}) \cup {[f EXCEPT !.stage = "Refused"]}
        /\ UNCHANGED << store_sessions, owner_sessions, next_op, batch_committed, unexpected_refusal >>

\* Owner may receive and apply completed flight results in ANY order.
ApplyResult ==
    \E f \in flights :
        /\ f.stage \in {"Committed", "Refused"}
        /\ flights' = flights \ {f}
        /\ owner_sessions' =
            IF f.stage = "Committed" THEN
                IF f.kind = "ExclusiveClose" THEN owner_sessions \ {f.client}
                ELSE owner_sessions \cup {f.client}
            ELSE owner_sessions
        /\ UNCHANGED << store_sessions, next_op, batch_committed, unexpected_refusal >>

Done ==
    /\ next_op = 7
    /\ flights = {}
    /\ UNCHANGED vars

Next ==
    \/ SubmitBatchOpen
    \/ SubmitExclusiveOpen
    \/ SubmitExclusiveClose
    \/ GroupCommitBatch
    \/ CommitExclusive
    \/ TimeoutAndResolve
    \/ ResolveIndeterminate
    \/ ApplyResult
    \/ Done

Spec == Init /\ [][Next]_vars

CapacityRespected ==
    /\ Cardinality(store_sessions) <= Capacity
    /\ Cardinality(owner_sessions) <= Capacity

AtMostOneFlightPerClient ==
    \A f1, f2 \in flights : (f1 # f2) => (f1.client # f2.client)

NoExclusiveOverlap ==
    (\E f \in flights : f.kind # "BatchOpen") => (Cardinality(flights) = 1)

OwnerStoreConsistentWhenIdle ==
    (flights = {}) => (owner_sessions = store_sessions)

NoUnexpectedBatchRefusal ==
    ~unexpected_refusal

NeverBatched ==
    ~batch_committed

=============================================================================
