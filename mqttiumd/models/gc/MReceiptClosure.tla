--------------------------- MODULE MReceiptClosure ---------------------------
(***************************************************************************)
(* Finite ARC-019/021 receipt-retirement model (proposed ADR 0027).          *)
(* A fixed origin abstracts (incarnation, epoch, logical owner).            *)
(* Data identities have their own issuance counters, not OperationSeq.      *)
(* h-prefixed values are verifier history: no transition guard reads them.  *)
(* Atomic store steps assume the SQLite transaction contract.               *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Origins, NumOps, Window, DataWindow, MaxAdvance, Mutation

Mutations == {"none", "CrossHole", "DeleteUnclosed", "OldPublication",
              "OldPayload", "DoubleCredit"}
ASSUME /\ Origins # {} /\ IsFiniteSet(Origins)
       /\ NumOps \in Nat /\ NumOps >= 2
       /\ Window \in Nat /\ DataWindow \in Nat
       /\ 0 < DataWindow /\ DataWindow < Window
       /\ MaxAdvance \in Nat /\ MaxAdvance > 0
       /\ Mutation \in Mutations

Seqs == 1..NumOps
Ids == 1..(NumOps \div 2)
Ops == [origin : Origins, seq : Seqs]
Identities == [origin : Origins, serial : Ids]
Role(n) == IF n % 2 = 0 THEN "Admission"
           ELSE IF n % 3 = 0 THEN "Control" ELSE "Business"
IssuedData(n) == Cardinality({s \in 1..n : Role(s) = "Admission"})
Op(o, n) == [origin |-> o, seq |-> n]
Identity(o, n) == [origin |-> o, serial |-> n]
NoRecipe == [kind |-> "None", publication |-> 0, payload |-> 0]
Recipes == {NoRecipe}
           \cup [kind : {"Business", "Control"}, publication : {0}, payload : {0}]
           \cup [kind : {"Admission"}, publication : Ids, payload : Ids]
Closure(o, n) == [origin |-> o, through |-> n,
                  publication |-> IssuedData(n), payload |-> IssuedData(n)]
Closures == {Closure(o, n) : o \in Origins, n \in 0..NumOps}
LiveObjects == [op : Ops, publication : Ids, payload : Ids]
Capacity == Cardinality(Origins) * Window
Max(a, b) == IF a >= b THEN a ELSE b

VARIABLES
    issued,          \* external never-reused issuance authority, per origin
    commands,        \* bounded accepted recipes waiting for the writer
    rows, canonical, \* retained receipt/fence and exact recipe; absent cells cost zero
    live,            \* business records, independently of retained outcomes
    floor, publicationFloor, payloadFloor, \* compact durable nonexecution guards
    knownFloor,      \* absolute durable closure acknowledged by each origin
    consumed,        \* bounded results applied/consumed beyond knownFloor
    results,         \* authentic terminal replies; may be lost/duplicated
    requests, replies, lastRequest, \* bounded maintenance transport, no OperationId
    used,            \* physical receipt/fence account
    hExecuted, hPublicationUses, hPayloadUses, hConsumed, hDeleted

vars == <<issued, commands, rows, canonical, live, floor,
          publicationFloor, payloadFloor, knownFloor, consumed, results,
          requests, replies, lastRequest, used, hExecuted,
          hPublicationUses, hPayloadUses, hConsumed, hDeleted>>

Init ==
    /\ issued = [o \in Origins |-> 0]
    /\ commands = [op \in Ops |-> NoRecipe]
    /\ rows = [op \in Ops |-> "Absent"]
    /\ canonical = [op \in Ops |-> NoRecipe]
    /\ live = {}
    /\ floor = [o \in Origins |-> 0]
    /\ publicationFloor = [o \in Origins |-> 0]
    /\ payloadFloor = [o \in Origins |-> 0]
    /\ knownFloor = [o \in Origins |-> 0]
    /\ consumed = {} /\ results = {} /\ requests = {} /\ replies = {}
    /\ lastRequest = [o \in Origins |-> Closure(o, 0)]
    /\ used = 0
    /\ hExecuted = [op \in Ops |-> 0]
    /\ hPublicationUses = [id \in Identities |-> 0]
    /\ hPayloadUses = [id \in Identities |-> 0]
    /\ hConsumed = {} /\ hDeleted = {}

Issue(o, p, b) ==
    LET n == issued[o] + 1
        limit == IF Role(n) = "Control" THEN Window ELSE DataWindow
        recipe == [kind |-> Role(n), publication |-> p, payload |-> b]
    IN /\ n \in Seqs
       /\ issued[o] - knownFloor[o] < limit
       /\ IF Role(n) = "Admission"
              THEN /\ p \in 1..IssuedData(n) /\ b \in 1..IssuedData(n)
              ELSE /\ p = 0 /\ b = 0
       /\ issued' = [issued EXCEPT ![o] = n]
       /\ commands' = [commands EXCEPT ![Op(o, n)] = recipe]
       /\ UNCHANGED <<rows, canonical, live, floor, publicationFloor,
                      payloadFloor, knownFloor, consumed, results,
                      requests, replies, lastRequest, used, hExecuted,
                      hPublicationUses, hPayloadUses, hConsumed, hDeleted>>

NamesPublication(o, p) ==
    \/ \E object \in live : object.op.origin = o /\ object.publication = p
    \/ \E op \in Ops : /\ op.origin = o /\ rows[op] = "Committed"
                       /\ canonical[op].kind = "Admission"
                       /\ canonical[op].publication = p
NamesPayload(o, b) ==
    \/ \E object \in live : object.op.origin = o /\ object.payload = b
    \/ \E op \in Ops : /\ op.origin = o /\ rows[op] = "Committed"
                       /\ canonical[op].kind = "Admission"
                       /\ canonical[op].payload = b
FreshPublication(o, p) ==
    /\ ~NamesPublication(o, p)
    /\ (Mutation = "OldPublication" \/ p > publicationFloor[o])
FreshPayload(o, b) ==
    /\ ~NamesPayload(o, b)
    /\ (Mutation = "OldPayload" \/ b > payloadFloor[o])

ApplyCommand(op) ==
    LET recipe == commands[op]
        admitted == recipe.kind = "Admission"
        fresh == ~admitted \/ (FreshPublication(op.origin, recipe.publication)
                               /\ FreshPayload(op.origin, recipe.payload))
        decision == IF fresh THEN "Committed" ELSE "Rejected"
    IN /\ recipe # NoRecipe
       /\ op.seq > floor[op.origin]
       /\ rows[op] = "Absent"
       /\ used < Capacity
       /\ rows' = [rows EXCEPT ![op] = decision]
       /\ canonical' = [canonical EXCEPT ![op] = recipe]
       /\ commands' = [commands EXCEPT ![op] = NoRecipe]
       /\ results' = results \cup {op}
       /\ used' = used + 1
       /\ live' = IF admitted /\ fresh
                     THEN live \cup {[op |-> op, publication |-> recipe.publication,
                                      payload |-> recipe.payload]}
                     ELSE live
       /\ hExecuted' = [hExecuted EXCEPT ![op] = @ + IF fresh THEN 1 ELSE 0]
       /\ hPublicationUses' = IF admitted /\ fresh
                THEN [hPublicationUses EXCEPT
                       ![Identity(op.origin, recipe.publication)] = @ + 1]
                ELSE hPublicationUses
       /\ hPayloadUses' = IF admitted /\ fresh
                THEN [hPayloadUses EXCEPT
                       ![Identity(op.origin, recipe.payload)] = @ + 1]
                ELSE hPayloadUses
       /\ UNCHANGED <<issued, floor, publicationFloor, payloadFloor,
                      knownFloor, consumed, requests, replies, lastRequest,
                      hConsumed, hDeleted>>

Fence(op) ==
    /\ op.seq <= issued[op.origin] /\ op.seq > floor[op.origin]
    /\ rows[op] = "Absent" /\ used < Capacity
    /\ rows' = [rows EXCEPT ![op] = "Fenced"]
    /\ canonical' = [canonical EXCEPT ![op] = NoRecipe]
    /\ commands' = [commands EXCEPT ![op] = NoRecipe]
    /\ results' = results \cup {op} /\ used' = used + 1
    /\ UNCHANGED <<issued, live, floor, publicationFloor, payloadFloor,
                   knownFloor, consumed, requests, replies, lastRequest,
                   hExecuted, hPublicationUses, hPayloadUses, hConsumed, hDeleted>>

Resolve(op) ==
    /\ op.seq > floor[op.origin] /\ rows[op] # "Absent"
    /\ results' = results \cup {op}
    /\ UNCHANGED <<issued, commands, rows, canonical, live, floor,
                   publicationFloor, payloadFloor, knownFloor, consumed,
                   requests, replies, lastRequest, used, hExecuted,
                   hPublicationUses, hPayloadUses, hConsumed, hDeleted>>

LoseResult(op) ==
    /\ op \in results /\ results' = results \ {op}
    /\ UNCHANGED <<issued, commands, rows, canonical, live, floor,
                   publicationFloor, payloadFloor, knownFloor, consumed,
                   requests, replies, lastRequest, used, hExecuted,
                   hPublicationUses, hPayloadUses, hConsumed, hDeleted>>

ConsumeResult(op) ==
    /\ op \in results
    /\ results' = results \ {op}
    /\ consumed' = IF op.seq > knownFloor[op.origin]
                    THEN consumed \cup {op} ELSE consumed
    /\ hConsumed' = hConsumed \cup {op}
    /\ UNCHANGED <<issued, commands, rows, canonical, live, floor,
                   publicationFloor, payloadFloor, knownFloor,
                   requests, replies, lastRequest, used, hExecuted,
                   hPublicationUses, hPayloadUses, hDeleted>>

RequestCollection(o, n) ==
    LET prefix == {Op(o, s) : s \in (knownFloor[o] + 1)..n}
        proof == IF Mutation = "CrossHole"
                    THEN Op(o, n) \in consumed
                    ELSE prefix \subseteq consumed
    IN /\ n \in (knownFloor[o] + 1)..issued[o]
       /\ proof
       /\ lastRequest[o].through <= knownFloor[o]
       /\ ~\E request \in requests : request.origin = o
       /\ ~\E reply \in replies : reply.origin = o
       /\ Cardinality(requests) < Cardinality(Origins) * 2
       /\ requests' = requests \cup {Closure(o, n)}
       /\ lastRequest' = [lastRequest EXCEPT ![o] = Closure(o, n)]
       /\ UNCHANGED <<issued, commands, rows, canonical, live, floor,
                      publicationFloor, payloadFloor, knownFloor, consumed,
                      results, replies, used, hExecuted,
                      hPublicationUses, hPayloadUses, hConsumed, hDeleted>>

RetryCollection(o, n) ==
    /\ n > 0 /\ (n = lastRequest[o].through \/ n <= knownFloor[o])
    /\ ~\E request \in requests : request.origin = o
    /\ Cardinality(requests) < Cardinality(Origins) * 2
    /\ requests' = requests \cup {Closure(o, n)}
    /\ UNCHANGED <<issued, commands, rows, canonical, live, floor,
                   publicationFloor, payloadFloor, knownFloor, consumed,
                   results, replies, lastRequest, used, hExecuted,
                   hPublicationUses, hPayloadUses, hConsumed, hDeleted>>

ApplyCollection(request) ==
    LET o == request.origin
        n == request.through
        newlyClosed == {Op(o, s) : s \in (floor[o] + 1)..n}
        eligible == {op \in Ops : /\ op.origin = o /\ op.seq <= n
                                   /\ rows[op] # "Absent"}
        removed == IF eligible = {} THEN {}
                     ELSE {CHOOSE op \in eligible : TRUE}
        advance == Mutation # "DeleteUnclosed"
        nextFloor == IF advance THEN Max(floor[o], n) ELSE floor[o]
        nextPublication == IF advance
                             THEN Max(publicationFloor[o], request.publication)
                             ELSE publicationFloor[o]
        nextPayload == IF advance THEN Max(payloadFloor[o], request.payload)
                           ELSE payloadFloor[o]
        answer == [origin |-> o, through |-> nextFloor,
                   publication |-> nextPublication, payload |-> nextPayload]
        count == IF Mutation = "DoubleCredit" /\ used >= 3
                    /\ Cardinality(removed) = 1
                    THEN 2 ELSE Cardinality(removed)
    IN /\ request \in requests
       /\ n - floor[o] <= MaxAdvance
       /\ \A op \in newlyClosed : rows[op] # "Absent"
       /\ \A reply \in replies : reply.origin # o \/ reply = answer
       /\ Cardinality(replies) < Cardinality(Origins) * 2
       /\ floor' = [floor EXCEPT ![o] = nextFloor]
       /\ publicationFloor' = [publicationFloor EXCEPT ![o] = nextPublication]
       /\ payloadFloor' = [payloadFloor EXCEPT ![o] = nextPayload]
       /\ rows' = [op \in Ops |-> IF op \in removed THEN "Absent" ELSE rows[op]]
       /\ canonical' = [op \in Ops |-> IF op \in removed THEN NoRecipe ELSE canonical[op]]
       /\ used' = used - count
       /\ requests' = requests \ {request}
       /\ replies' = replies \cup {answer}
       /\ hDeleted' = hDeleted \cup removed
       /\ UNCHANGED <<issued, commands, live, knownFloor, consumed, results,
                      lastRequest, hExecuted, hPublicationUses,
                      hPayloadUses, hConsumed>>

ObserveCollection(reply) ==
    LET o == reply.origin
        learned == Max(knownFloor[o], reply.through)
    IN /\ reply \in replies
       /\ knownFloor' = [knownFloor EXCEPT ![o] = learned]
       /\ consumed' = {op \in consumed : op.origin # o \/ op.seq > learned}
       /\ replies' = replies \ {reply}
       /\ UNCHANGED <<issued, commands, rows, canonical, live, floor,
                      publicationFloor, payloadFloor, results, requests,
                      lastRequest, used, hExecuted, hPublicationUses,
                      hPayloadUses, hConsumed, hDeleted>>

LoseCollectionReply(reply) ==
    /\ reply \in replies /\ replies' = replies \ {reply}
    /\ UNCHANGED <<issued, commands, rows, canonical, live, floor,
                   publicationFloor, payloadFloor, knownFloor, consumed,
                   results, requests, lastRequest, used, hExecuted,
                   hPublicationUses, hPayloadUses, hConsumed, hDeleted>>

Settle(object) ==
    /\ object \in live /\ live' = live \ {object}
    /\ UNCHANGED <<issued, commands, rows, canonical, floor,
                   publicationFloor, payloadFloor, knownFloor, consumed,
                   results, requests, replies, lastRequest, used, hExecuted,
                   hPublicationUses, hPayloadUses, hConsumed, hDeleted>>

InjectLateReply(o, n) ==
    /\ n \in 1..knownFloor[o]
    /\ ~\E reply \in replies : reply.origin = o
    /\ replies' = replies \cup {Closure(o, n)}
    /\ UNCHANGED <<issued, commands, rows, canonical, live, floor,
                   publicationFloor, payloadFloor, knownFloor, consumed,
                   results, requests, lastRequest, used, hExecuted,
                   hPublicationUses, hPayloadUses, hConsumed, hDeleted>>

ReplayOld(op) ==
    \* Logical closure is tested BEFORE looking at a retained physical row.
    /\ op.seq <= issued[op.origin]
    /\ (op.seq <= floor[op.origin] \/ rows[op] # "Absent")
    \* Below floor: retired, not a certain rejection. Above: exact canonical
    \* match replays its outcome; any other recipe is a mismatch. None executes.
    /\ UNCHANGED vars

Idle == UNCHANGED vars
Next ==
    \/ \E o \in Origins, p \in 0..(NumOps \div 2), b \in 0..(NumOps \div 2) : Issue(o, p, b)
    \/ \E op \in Ops : ApplyCommand(op)
    \/ \E op \in Ops : Fence(op)
    \/ \E op \in Ops : Resolve(op)
    \/ \E op \in Ops : LoseResult(op)
    \/ \E op \in Ops : ConsumeResult(op)
    \/ \E o \in Origins, n \in Seqs : RequestCollection(o, n)
    \/ \E o \in Origins, n \in Seqs : RetryCollection(o, n)
    \/ \E request \in requests : ApplyCollection(request)
    \/ \E reply \in replies : ObserveCollection(reply)
    \/ \E reply \in replies : LoseCollectionReply(reply)
    \/ \E o \in Origins, n \in Seqs : InjectLateReply(o, n)
    \/ \E object \in live : Settle(object)
    \/ \E op \in Ops : ReplayOld(op)
    \/ Idle
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ issued \in [Origins -> 0..NumOps]
    /\ commands \in [Ops -> Recipes] /\ canonical \in [Ops -> Recipes]
    /\ rows \in [Ops -> {"Absent", "Committed", "Rejected", "Fenced"}]
    /\ live \subseteq LiveObjects
    /\ floor \in [Origins -> 0..NumOps]
    /\ publicationFloor \in [Origins -> 0..(NumOps \div 2)]
    /\ payloadFloor \in [Origins -> 0..(NumOps \div 2)]
    /\ knownFloor \in [Origins -> 0..NumOps]
    /\ consumed \subseteq Ops /\ results \subseteq Ops
    /\ requests \subseteq Closures /\ replies \subseteq Closures
    /\ Cardinality(requests) <= Cardinality(Origins) * 2
    /\ Cardinality(replies) <= Cardinality(Origins) * 2
    /\ lastRequest \in [Origins -> Closures]
    /\ used \in (-Capacity)..Capacity
    /\ hExecuted \in [Ops -> 0..2]
    /\ hPublicationUses \in [Identities -> 0..2]
    /\ hPayloadUses \in [Identities -> 0..2]
    /\ hConsumed \subseteq Ops /\ hDeleted \subseteq Ops
ClosedResultsConsumed ==
    \A o \in Origins : {Op(o, n) : n \in 1..floor[o]} \subseteq hConsumed
DeletedOnlyAfterClosure == \A op \in hDeleted : op.seq <= floor[op.origin]
CreditsExact == used = Cardinality({op \in Ops : rows[op] # "Absent"})
WithinWindow == \A o \in Origins : issued[o] - knownFloor[o] <= Window
OriginMetadataBounded ==
    \A o \in Origins : Cardinality({op \in consumed : op.origin = o}) <= Window
ClosureKnowledgeSound == \A o \in Origins : knownFloor[o] <= floor[o]
AdmissionIdentityUnique == \A op \in Ops : hExecuted[op] <= 1
PublicationIdentityUnique == \A id \in Identities : hPublicationUses[id] <= 1
PayloadIdentityUnique == \A id \in Identities : hPayloadUses[id] <= 1
NoUnknownGap ==
    ~\E o \in Origins : /\ rows[Op(o, 1)] # "Absent"
                        /\ Op(o, 1) \notin hConsumed /\ Op(o, 2) \in consumed
NoWindowReuse == \A o \in Origins : issued[o] <= Window
NoClosedLiveObject == \A object \in live : object.op.seq > floor[object.op.origin]
=============================================================================
