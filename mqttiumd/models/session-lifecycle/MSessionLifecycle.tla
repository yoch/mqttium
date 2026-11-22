------------------------- MODULE MSessionLifecycle -------------------------
(***************************************************************************)
(* Proposed ADR 0029: finite session incarnation, attachment and cleanup.   *)
(* Store steps assume atomic SQLite transactions and authentic replies.    *)
(* h-prefixed fields are verifier histories, never authority for an action. *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS MaxSessionId, MaxOps, Window, Scenario, AllowCrash, Mutation

Mutations == {"none", "KeepTerminal", "OldToken", "DeleteOther",
              "ResumeZero", "DoubleCredit"}
ASSUME /\ MaxSessionId \in Nat /\ MaxSessionId >= 3
       /\ MaxOps \in Nat /\ MaxOps >= 3
       /\ Window \in Nat /\ Window >= 2
       /\ Scenario \in {"any", "cleanup"}
       /\ AllowCrash \in BOOLEAN /\ Mutation \in Mutations

Ids == 1..MaxSessionId
Ops == 1..MaxOps
Policies == {"Zero", "Never"}
Capacity == 2
Other == 1
Source == 2
Active(a) == {Other} \cup IF a = 0 THEN {} ELSE {a}
NoRecipe == [kind |-> "None", sid |-> 0, token |-> 0,
             clean |-> FALSE, expiry |-> "Never"]
Recipes == {NoRecipe}
           \cup [kind : {"Open", "Close"}, sid : 0..MaxSessionId,
                  token : 0..MaxOps, clean : BOOLEAN, expiry : Policies]
NoOutcome == [kind |-> "None", sid |-> 0, retired |-> 0,
              token |-> 0, present |-> FALSE]
Outcomes == {NoOutcome}
            \cup [kind : {"Opened", "Closed", "Refused", "Admission"},
                   sid : 0..MaxSessionId, retired : 0..MaxSessionId,
                   token : 0..MaxOps, present : BOOLEAN]
NoCopy == [op |-> 0, sid |-> 0, token |-> 0]
Copies == [op : 0..MaxOps, sid : 0..MaxSessionId, token : 0..MaxOps]

VARIABLES
    current, rows, policy, token, revision, nextSession,
    obligations, inbound, claims, references, exchangeSlots,
    transport, ownerSession, ownerToken, ownerRows,
    issued, recipes, receipts, flight, stage, requestAlive,
    consumed, floor, replyCopy,
    epoch, running,
    hCreated, hClosed, hExpired, hExecuted, hConsumed, hWrongClose,
    hOtherFinished, hFlags

vars == <<current, rows, policy, token, revision, nextSession,
          obligations, inbound, claims, references, exchangeSlots,
          transport, ownerSession, ownerToken, ownerRows,
          issued, recipes, receipts, flight, stage, requestAlive,
          consumed, floor, replyCopy, epoch, running,
          hCreated, hClosed, hExpired, hExecuted, hConsumed, hWrongClose,
          hOtherFinished, hFlags>>

Init ==
    /\ current = Source /\ rows = {Other, Source}
    /\ policy = [s \in Ids |-> IF s \in rows THEN "Never" ELSE "Absent"]
    /\ token = [s \in Ids |-> 0]
    /\ revision = [s \in Ids |-> IF s \in rows THEN 1 ELSE 0]
    /\ nextSession = 3
    \* One shared payload was admitted by Source, with its result still lost.
    /\ obligations = {Other, Source} /\ inbound = {Source} /\ claims = {Source}
    /\ references = 2 /\ exchangeSlots = 2
    /\ transport = TRUE /\ ownerSession = Source /\ ownerToken = 0
    /\ ownerRows = rows
    /\ issued = 1
    /\ recipes = [o \in Ops |-> NoRecipe]
    /\ receipts = [o \in Ops |-> IF o = 1
            THEN [kind |-> "Admission", sid |-> Source, retired |-> 0,
                  token |-> 0, present |-> FALSE] ELSE NoOutcome]
    /\ flight = 0 /\ stage = "Idle" /\ requestAlive = FALSE
    /\ consumed = {} /\ floor = 0 /\ replyCopy = NoCopy
    /\ epoch = 1 /\ running = TRUE
    /\ hCreated = {Other, Source} /\ hClosed = {} /\ hExpired = {}
    /\ hExecuted = [o \in Ops |-> IF o = 1 THEN 1 ELSE 0]
    /\ hConsumed = {} /\ hWrongClose = FALSE /\ hOtherFinished = FALSE
    /\ hFlags = {}

CanIssue == /\ running /\ flight = 0 /\ issued < MaxOps
            /\ issued - floor < Window
OpenChoice(clean, expiry) ==
    \/ Scenario = "any"
    \/ /\ Scenario = "cleanup"
       /\ IF issued = 1 THEN /\ ~clean /\ expiry = "Zero"
             ELSE IF issued = 2 THEN /\ ~clean /\ expiry = "Never"
             ELSE /\ clean /\ expiry = "Never"

SubmitOpen(clean, expiry) ==
    LET o == issued + 1
        t == IF current = 0 THEN 0 ELSE token[current]
    IN /\ CanIssue /\ OpenChoice(clean, expiry)
       /\ issued' = o /\ flight' = o /\ stage' = "Submitted"
       /\ recipes' = [recipes EXCEPT ![o] =
            [kind |-> "Open", sid |-> current, token |-> t,
             clean |-> clean, expiry |-> expiry]]
       \* Actual transport takeover precedes the store's Clean Start cut.
       /\ transport' = FALSE /\ ownerSession' = 0 /\ ownerToken' = 0
       /\ requestAlive' = TRUE
       /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                      obligations, inbound, claims, references, exchangeSlots, ownerRows,
                      receipts, consumed, floor, replyCopy, epoch, running,
                      hCreated, hClosed, hExpired, hExecuted, hConsumed,
                      hWrongClose, hOtherFinished, hFlags>>

SubmitClose(saved) ==
    LET o == issued + 1
        s == IF saved THEN Source ELSE current
        t == IF saved \/ current = 0 THEN 0 ELSE token[current]
        closeCurrent == ~saved \/ (current = Source /\ token[Source] = 0)
    IN /\ Scenario = "any" /\ CanIssue
       /\ saved \/ current # 0
       /\ issued' = o /\ flight' = o /\ stage' = "Submitted"
       /\ recipes' = [recipes EXCEPT ![o] =
            [kind |-> "Close", sid |-> s, token |-> t,
             clean |-> FALSE, expiry |-> "Zero"]]
       \* A retained old Close does not close the replacement transport.
       /\ transport' = IF closeCurrent THEN FALSE ELSE transport
       /\ ownerSession' = IF closeCurrent THEN 0 ELSE ownerSession
       /\ ownerToken' = IF closeCurrent THEN 0 ELSE ownerToken
       /\ requestAlive' = FALSE
       /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                      obligations, inbound, claims, references, exchangeSlots, ownerRows,
                      receipts, consumed, floor, replyCopy, epoch, running,
                      hCreated, hClosed, hExpired, hExecuted, hConsumed,
                      hWrongClose, hOtherFinished, hFlags>>

GuardMatches(r) ==
    /\ r.sid = current
    /\ IF current = 0 THEN r.token = 0 ELSE r.token = token[current]
RemovedRefs(victims) == Cardinality(obligations \cap victims)
RemovedSlots(victims) == Cardinality(inbound \cap victims)
                         + Cardinality(claims \cap victims)
CleanupRows(victims) == IF Mutation = "KeepTerminal" THEN rows ELSE rows \ victims
CleanupTargets(victims) ==
    IF Mutation = "DeleteOther" /\ victims # {} THEN Active(current) ELSE victims
CleanupCount(victims) ==
    LET count == RemovedRefs(victims)
    IN IF Mutation = "DoubleCredit" /\ count > 0 THEN count + 1 ELSE count

CommitOpen ==
    LET o == flight
        r == recipes[o]
        present == current # 0 /\ ~r.clean /\ policy[current] = "Never"
        fresh == ~present
        removed == IF fresh /\ current # 0 THEN {current} ELSE {}
        victims == CleanupTargets(removed)
        s == IF fresh THEN nextSession ELSE current
        liveAfter == (Active(current) \ removed) \cup {s}
    IN /\ running /\ stage = "Submitted" /\ r.kind = "Open"
       /\ GuardMatches(r)
       /\ ~fresh \/ nextSession \in Ids
       /\ Cardinality(liveAfter) <= Capacity
       /\ receipts[o] = NoOutcome /\ o > floor
       /\ current' = s
       /\ rows' = CleanupRows(removed) \cup {s}
       /\ policy' = [i \in Ids |-> IF i = s THEN r.expiry
                             ELSE IF i \in removed THEN "Absent" ELSE policy[i]]
       /\ token' = [i \in Ids |-> IF i = s THEN o
                             ELSE IF i \in removed THEN 0 ELSE token[i]]
       /\ revision' = [i \in Ids |-> IF i = s THEN 1
                                ELSE IF i \in removed THEN 0 ELSE revision[i]]
       /\ nextSession' = IF fresh THEN nextSession + 1 ELSE nextSession
       /\ obligations' = obligations \ victims /\ inbound' = inbound \ victims
       /\ claims' = claims \ victims /\ references' = references - CleanupCount(victims)
       /\ exchangeSlots' = exchangeSlots - RemovedSlots(victims)
       /\ receipts' = [receipts EXCEPT ![o] =
            [kind |-> "Opened", sid |-> s,
             retired |-> IF removed = {} THEN 0 ELSE current,
             token |-> o, present |-> present]]
       /\ stage' = "Unknown"
       /\ hCreated' = hCreated \cup {s} /\ hClosed' = hClosed \cup removed
       /\ hExecuted' = [hExecuted EXCEPT ![o] = @ + 1]
       /\ hFlags' = hFlags
             \cup (IF present /\ r.expiry = "Zero" THEN {"ResumedToZero"} ELSE {})
             \cup (IF current # 0 /\ policy[current] = "Zero" /\ ~r.clean
                       THEN {"ZeroTakeoverFresh"} ELSE {})
             \cup (IF Source \in removed /\ 1 \notin hConsumed
                       THEN {"UnknownSurvivedClosure"} ELSE {})
       /\ UNCHANGED <<transport, ownerSession, ownerToken, ownerRows, issued,
                      recipes, flight, requestAlive, consumed, floor, replyCopy,
                      epoch, running, hExpired, hConsumed, hWrongClose,
                      hOtherFinished>>

CommitClose ==
    LET o == flight
        r == recipes[o]
        correct == current # 0 /\ GuardMatches(r)
        accept == current # 0 /\ (correct \/ Mutation = "OldToken")
        removed == IF accept THEN {current} ELSE {}
        victims == CleanupTargets(removed)
    IN /\ running /\ stage = "Submitted" /\ r.kind = "Close"
       /\ receipts[o] = NoOutcome /\ o > floor
       /\ current' = IF accept THEN 0 ELSE current
       /\ rows' = CleanupRows(removed)
       /\ policy' = [i \in Ids |-> IF i \in removed THEN "Absent" ELSE policy[i]]
       /\ token' = [i \in Ids |-> IF i \in removed THEN 0 ELSE token[i]]
       /\ revision' = [i \in Ids |-> IF i \in removed THEN 0 ELSE revision[i]]
       /\ obligations' = obligations \ victims /\ inbound' = inbound \ victims
       /\ claims' = claims \ victims /\ references' = references - CleanupCount(victims)
       /\ exchangeSlots' = exchangeSlots - RemovedSlots(victims)
       /\ receipts' = [receipts EXCEPT ![o] =
            [kind |-> IF accept THEN "Closed" ELSE "Refused",
             sid |-> r.sid, retired |-> IF accept THEN current ELSE 0,
             token |-> r.token, present |-> FALSE]]
       /\ stage' = "Unknown" /\ hClosed' = hClosed \cup removed
       /\ hExecuted' = [hExecuted EXCEPT ![o] = @ + 1]
       /\ hWrongClose' = (hWrongClose \/ (accept /\ ~correct))
       /\ hFlags' = hFlags
             \cup (IF Source \in removed /\ 1 \notin hConsumed
                       THEN {"UnknownSurvivedClosure"} ELSE {})
             \cup (IF ~correct /\ ~accept THEN {"OldCloseRejected"} ELSE {})
       /\ UNCHANGED <<nextSession, transport, ownerSession, ownerToken,
                      ownerRows, issued, recipes, flight, requestAlive,
                      consumed, floor, replyCopy, epoch, running,
                      hCreated, hExpired, hConsumed, hOtherFinished>>

ResolveResult ==
    /\ running /\ stage = "Unknown" /\ receipts[flight] # NoOutcome
    /\ stage' = "Known"
    /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                   obligations, inbound, claims, references, exchangeSlots,
                   transport, ownerSession, ownerToken, ownerRows,
                   issued, recipes, receipts, flight, requestAlive,
                   consumed, floor, replyCopy, epoch, running,
                   hCreated, hClosed, hExpired, hExecuted, hConsumed,
                   hWrongClose, hOtherFinished, hFlags>>

ApplyResult ==
    LET o == flight
        outcome == receipts[o]
        attach == outcome.kind = "Opened" /\ requestAlive
                  /\ outcome.sid = current /\ current # 0
                  /\ outcome.token = token[current]
    IN /\ running /\ stage = "Known"
       /\ transport' = (transport \/ attach)
       /\ ownerSession' = IF attach THEN current
             ELSE IF ownerSession \in Active(current) THEN ownerSession ELSE 0
       /\ ownerToken' = IF attach THEN token[current]
             ELSE IF ownerSession \in Active(current) THEN ownerToken ELSE 0
       /\ ownerRows' = Active(current)
       /\ consumed' = consumed \cup {o} /\ hConsumed' = hConsumed \cup {o}
       /\ flight' = 0 /\ stage' = "Idle" /\ requestAlive' = FALSE
       /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                      obligations, inbound, claims, references, exchangeSlots, issued,
                      recipes, receipts, floor, replyCopy, epoch, running,
                      hCreated, hClosed, hExpired, hExecuted,
                      hWrongClose, hOtherFinished, hFlags>>

EndPendingTransport ==
    /\ Scenario = "any" /\ requestAlive
    /\ requestAlive' = FALSE
    /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                   obligations, inbound, claims, references, exchangeSlots,
                   transport, ownerSession, ownerToken, ownerRows,
                   issued, recipes, receipts, flight, stage,
                   consumed, floor, replyCopy, epoch, running,
                   hCreated, hClosed, hExpired, hExecuted, hConsumed,
                   hWrongClose, hOtherFinished, hFlags>>

SaveReply ==
    /\ Scenario = "any" /\ stage = "Known" /\ replyCopy = NoCopy
    /\ replyCopy' = [op |-> flight, sid |-> receipts[flight].sid,
                     token |-> receipts[flight].token]
    /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                   obligations, inbound, claims, references, exchangeSlots,
                   transport, ownerSession, ownerToken, ownerRows,
                   issued, recipes, receipts, flight, stage, requestAlive,
                   consumed, floor, epoch, running,
                   hCreated, hClosed, hExpired, hExecuted, hConsumed,
                   hWrongClose, hOtherFinished, hFlags>>

ReplayReply ==
    /\ Scenario = "any" /\ replyCopy # NoCopy
    \* Frozen old replies cannot acquire the new flight's correlation.
    /\ replyCopy.op # flight
    /\ hFlags' = hFlags \cup {"OldReplyIgnored"}
    /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                   obligations, inbound, claims, references, exchangeSlots,
                   transport, ownerSession, ownerToken, ownerRows,
                   issued, recipes, receipts, flight, stage, requestAlive,
                   consumed, floor, replyCopy, epoch, running,
                   hCreated, hClosed, hExpired, hExecuted, hConsumed,
                   hWrongClose, hOtherFinished>>

ResolveAdmission ==
    /\ running /\ floor = 0 /\ 1 \notin consumed
    /\ receipts[1].kind = "Admission"
    \* Source termination does not turn this unknown commit into rejection.
    /\ consumed' = consumed \cup {1} /\ hConsumed' = hConsumed \cup {1}
    /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                   obligations, inbound, claims, references, exchangeSlots,
                   transport, ownerSession, ownerToken, ownerRows,
                   issued, recipes, receipts, flight, stage, requestAlive,
                   floor, replyCopy, epoch, running,
                   hCreated, hClosed, hExpired, hExecuted,
                   hWrongClose, hOtherFinished, hFlags>>

CollectPrefix ==
    LET o == floor + 1
    IN /\ o <= issued /\ o \in consumed
       /\ floor' = o /\ consumed' = consumed \ {o}
       /\ receipts' = [receipts EXCEPT ![o] = NoOutcome]
       /\ recipes' = [recipes EXCEPT ![o] = NoRecipe]
       /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                      obligations, inbound, claims, references, exchangeSlots,
                      transport, ownerSession, ownerToken, ownerRows,
                      issued, flight, stage, requestAlive, replyCopy, epoch,
                      running, hCreated, hClosed, hExpired, hExecuted,
                      hConsumed, hWrongClose, hOtherFinished, hFlags>>

ReplayCommand(o) ==
    /\ o <= issued
    /\ o <= floor \/ receipts[o] # NoOutcome
    \* A compact retired floor is nonexecution, not certain rejection.
    /\ UNCHANGED vars

FinishOther ==
    /\ Scenario = "any" /\ running /\ Other \in obligations
    /\ obligations' = obligations \ {Other} /\ references' = references - 1
    /\ hOtherFinished' = TRUE
    /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                   inbound, claims, exchangeSlots, transport, ownerSession, ownerToken,
                   ownerRows, issued, recipes, receipts, flight, stage,
                   requestAlive, consumed, floor, replyCopy, epoch, running,
                   hCreated, hClosed, hExpired, hExecuted, hConsumed,
                   hWrongClose, hFlags>>

Crash ==
    /\ AllowCrash /\ Scenario = "any" /\ running /\ epoch = 1
    \* The two commit-boundary cases are an idle flight or a committed flight.
    /\ stage \in {"Idle", "Unknown", "Known"}
    /\ running' = FALSE /\ transport' = FALSE
    /\ ownerSession' = 0 /\ ownerToken' = 0 /\ ownerRows' = {}
    /\ requestAlive' = FALSE
    /\ stage' = IF stage = "Known" THEN "Unknown" ELSE stage
    /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                   obligations, inbound, claims, references, exchangeSlots,
                   issued, recipes, receipts, flight, consumed, floor,
                   replyCopy, epoch, hCreated, hClosed, hExpired, hExecuted,
                   hConsumed, hWrongClose, hOtherFinished, hFlags>>

OpenEpoch ==
    LET expected == {s \in Active(current) : policy[s] = "Zero"}
        removed == IF Mutation = "ResumeZero" THEN {} ELSE expected
        victims == CleanupTargets(removed)
        remaining == Active(current) \ removed
    IN /\ ~running /\ epoch = 1
       /\ running' = TRUE /\ epoch' = 2
       /\ current' = IF current \in removed THEN 0 ELSE current
       /\ rows' = CleanupRows(removed)
       /\ policy' = [i \in Ids |-> IF i \in removed THEN "Absent" ELSE policy[i]]
       /\ token' = [i \in Ids |-> IF i \in removed THEN 0 ELSE token[i]]
       /\ revision' = [i \in Ids |-> IF i \in removed THEN 0 ELSE revision[i]]
       /\ obligations' = obligations \ victims /\ inbound' = inbound \ victims
       /\ claims' = claims \ victims /\ references' = references - CleanupCount(victims)
       /\ exchangeSlots' = exchangeSlots - RemovedSlots(victims)
       /\ ownerRows' = remaining
       /\ hClosed' = hClosed \cup removed /\ hExpired' = hExpired \cup expected
       /\ hFlags' = hFlags
             \cup IF Source \in removed /\ 1 \notin hConsumed
                       THEN {"UnknownSurvivedClosure"} ELSE {}
       /\ UNCHANGED <<nextSession, transport, ownerSession, ownerToken,
                      issued, recipes, receipts, flight, stage, requestAlive,
                      consumed, floor, replyCopy, hCreated, hExecuted,
                      hConsumed, hWrongClose, hOtherFinished>>

DuplicateOpenEpoch ==
    /\ AllowCrash /\ running /\ epoch = 2
    \* Already-opened epoch result replay grants no second startup cleanup.
    /\ hFlags' = hFlags \cup
            IF current # 0 /\ policy[current] = "Zero"
               THEN {"DuplicateOpeningPreservedZero"} ELSE {}
    /\ UNCHANGED <<current, rows, policy, token, revision, nextSession,
                   obligations, inbound, claims, references, exchangeSlots,
                   transport, ownerSession, ownerToken, ownerRows,
                   issued, recipes, receipts, flight, stage, requestAlive,
                   consumed, floor, replyCopy, epoch, running,
                   hCreated, hClosed, hExpired, hExecuted, hConsumed,
                   hWrongClose, hOtherFinished>>

Idle == UNCHANGED vars
Next ==
    \/ \E clean \in BOOLEAN, expiry \in Policies : SubmitOpen(clean, expiry)
    \/ \E saved \in BOOLEAN : SubmitClose(saved)
    \/ CommitOpen
    \/ CommitClose
    \/ ResolveResult
    \/ ApplyResult
    \/ EndPendingTransport
    \/ SaveReply
    \/ ReplayReply
    \/ ResolveAdmission
    \/ CollectPrefix
    \/ \E o \in Ops : ReplayCommand(o)
    \/ FinishOther
    \/ Crash
    \/ OpenEpoch
    \/ DuplicateOpenEpoch
    \/ Idle
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ current \in 0..MaxSessionId /\ rows \subseteq Ids
    /\ policy \in [Ids -> Policies \cup {"Absent"}]
    /\ token \in [Ids -> 0..MaxOps] /\ revision \in [Ids -> 0..1]
    /\ nextSession \in 3..(MaxSessionId + 1)
    /\ obligations \subseteq Ids /\ inbound \subseteq Ids /\ claims \subseteq Ids
    /\ references \in (-3)..3 /\ exchangeSlots \in 0..2
    /\ transport \in BOOLEAN /\ ownerSession \in 0..MaxSessionId
    /\ ownerToken \in 0..MaxOps /\ ownerRows \subseteq Ids
    /\ issued \in 1..MaxOps /\ recipes \in [Ops -> Recipes]
    /\ receipts \in [Ops -> Outcomes] /\ flight \in 0..MaxOps
    /\ stage \in {"Idle", "Submitted", "Unknown", "Known"}
    /\ requestAlive \in BOOLEAN /\ consumed \subseteq Ops /\ floor \in 0..MaxOps
    /\ replyCopy \in Copies /\ epoch \in 1..2 /\ running \in BOOLEAN
    /\ hCreated \subseteq Ids /\ hClosed \subseteq Ids /\ hExpired \subseteq Ids
    /\ hExecuted \in [Ops -> 0..2] /\ hConsumed \subseteq Ops
    /\ hWrongClose \in BOOLEAN /\ hOtherFinished \in BOOLEAN
    /\ hFlags \subseteq {"ResumedToZero", "ZeroTakeoverFresh", "UnknownSurvivedClosure",
                         "OldCloseRejected", "OldReplyIgnored", "DuplicateOpeningPreservedZero"}
SessionRowsBounded == /\ Cardinality(rows) <= Capacity
                      /\ Cardinality(ownerRows) <= Capacity
SessionIdentitiesMonotone == /\ hCreated = 1..(nextSession - 1)
                             /\ Active(current) \subseteq hCreated
                             /\ Active(current) \cap hClosed = {}
BusinessRevisionStable == \A s \in Active(current) : revision[s] = 1
ActiveMetadata == /\ Active(current) \subseteq rows
                  /\ \A s \in Active(current) : policy[s] \in Policies
                  /\ claims \subseteq obligations
                  /\ obligations \cup inbound \subseteq Active(current)
CreditsExact == /\ references = Cardinality(obligations)
                /\ exchangeSlots = Cardinality(inbound) + Cardinality(claims)
OtherResponsibilityPreserved == ~hOtherFinished => Other \in obligations
NoStaleClose == ~hWrongClose
NoZeroAfterRestart == rows \cap hExpired = {}
OwnerReadyAuthentic == ownerSession # 0 =>
    /\ running /\ transport /\ ownerSession = current
    /\ ownerSession \in Active(current) /\ ownerToken = token[current]
UnknownResultsRetained ==
    /\ \A o \in 1..issued : o \notin hConsumed =>
            /\ o > floor
            /\ ((o = flight /\ stage = "Submitted") \/ receipts[o] # NoOutcome)
    /\ 1 \notin hConsumed => receipts[1].kind = "Admission"
ClosedPrefixConsumed == 1..floor \subseteq hConsumed
NoReexecution == \A o \in Ops : hExecuted[o] <= 1
ReceiptWindowBounded == /\ issued - floor <= Window
                        /\ Cardinality(consumed) <= Window
                        /\ Cardinality({o \in Ops : receipts[o] # NoOutcome}) <= Window
NoCleanupWitness ==
    ~ /\ nextSession = MaxSessionId + 1
      /\ Cardinality(hCreated) > Capacity
      /\ rows = Active(current) /\ floor >= 3
      /\ {"ResumedToZero", "ZeroTakeoverFresh", "UnknownSurvivedClosure"} \subseteq hFlags
      /\ obligations = {Other} /\ inbound = {} /\ references = 1 /\ exchangeSlots = 0
=============================================================================
