------------------------- MODULE MWildcardRouting -------------------------
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS MaxChanges, Mutation

VARIABLES routes, revision, phase, publication, snapshot, cut, remaining,
          copies, offered, decision, hExpected, hFrozen, hStaleCut

vars == <<routes, revision, phase, publication, snapshot, cut, remaining,
          copies, offered, decision, hExpected, hFrozen, hStaleCut>>

Sessions == 1..2
Entries == 1..6
Topics == {"leaf", "empty-child", "parent", "system-parent", "system-leaf",
           "nested-dollar", "leading-empty", "deep"}
EntrySession == <<1, 1, 2, 2, 2, 1>>
EntryFilter == <<"exact", "hash", "plus", "all", "two-level", "system">>

\* This is an independently enumerated example relation, not a copy of the
\* production matcher. The README gives each symbolic topic/filter's bytes.
MatchPairs == {
    <<"exact", "leaf">>,
    <<"plus", "leaf">>, <<"plus", "empty-child">>,
    <<"plus", "nested-dollar">>,
    <<"hash", "leaf">>, <<"hash", "empty-child">>,
    <<"hash", "parent">>, <<"hash", "nested-dollar">>, <<"hash", "deep">>,
    <<"all", "leaf">>, <<"all", "empty-child">>, <<"all", "parent">>,
    <<"all", "nested-dollar">>, <<"all", "leading-empty">>, <<"all", "deep">>,
    <<"two-level", "leaf">>, <<"two-level", "leading-empty">>,
    <<"system", "system-parent">>, <<"system", "system-leaf">>}

Option == [present : BOOLEAN, grant : 0..1, noLocal : BOOLEAN]
Absent == [present |-> FALSE, grant |-> 0, noLocal |-> FALSE]
Choices == {Absent} \cup
           {[present |-> TRUE, grant |-> q, noLocal |-> n] :
                q \in 0..1, n \in BOOLEAN}
Zero == [s \in Sessions |-> 0]
EmptyDecision == [present |-> FALSE, copies |-> Zero, qos |-> Zero]

Matches(e, p) == <<EntryFilter[e], p.topic>> \in MatchPairs
Eligible(rs, p, s) == {e \in Entries : rs[e].present /\ EntrySession[e] = s
                         /\ Matches(e, p)
                         /\ ~(rs[e].noLocal /\ p.source = s)}
ExpectedCopies(rs, p) == [s \in Sessions |->
                            IF Eligible(rs, p, s) = {} THEN 0 ELSE 1]
ExpectedQoS(rs, p) == [s \in Sessions |->
    IF p.qos = 0 \/ Eligible(rs, p, s) = {} THEN 0
    ELSE IF \E e \in Eligible(rs, p, s) : rs[e].grant = 1 THEN 1 ELSE 0]
Expected(rs, p) == [present |-> TRUE,
                    copies |-> ExpectedCopies(rs, p), qos |-> ExpectedQoS(rs, p)]

Init ==
    /\ routes = <<[present |-> TRUE, grant |-> 1, noLocal |-> TRUE],
                  [present |-> TRUE, grant |-> 0, noLocal |-> FALSE],
                  [present |-> TRUE, grant |-> 0, noLocal |-> FALSE],
                  [present |-> TRUE, grant |-> 1, noLocal |-> FALSE],
                  [present |-> TRUE, grant |-> 1, noLocal |-> FALSE], Absent>>
    /\ revision = 0 /\ phase = "idle"
    /\ publication = [source |-> 1, topic |-> "leaf", qos |-> 1]
    /\ snapshot = routes /\ cut = 0 /\ remaining = {}
    /\ copies = Zero /\ offered = Zero
    /\ decision = EmptyDecision /\ hExpected = EmptyDecision
    /\ hFrozen = EmptyDecision /\ hStaleCut = FALSE

Begin(source, topic, qos) ==
    /\ phase = "idle"
    /\ publication' = [source |-> source, topic |-> topic, qos |-> qos]
    /\ snapshot' = routes /\ cut' = revision /\ remaining' = Entries
    /\ copies' = Zero /\ offered' = Zero /\ phase' = "selecting"
    /\ hExpected' = Expected(routes, publication')
    /\ UNCHANGED <<routes, revision, decision, hFrozen, hStaleCut>>

\* Every possible entry visitation order is explored. No Local is applied
\* to an individual match before combining that destination's offered QoS.
Scan(e) ==
    /\ phase = "selecting" /\ e \in remaining
    /\ remaining' = remaining \ {e}
    /\ LET s == EntrySession[e]
           usable == snapshot[e].present /\ Matches(e, publication)
                     /\ (Mutation = "local-after-max"
                         \/ ~(snapshot[e].noLocal /\ publication.source = s))
           effective == IF publication.qos = 0 THEN 0 ELSE snapshot[e].grant
       IN /\ copies' = IF usable
                       THEN [copies EXCEPT ![s] =
                           IF Mutation = "duplicate-target" THEN @ + 1 ELSE 1]
                       ELSE copies
          /\ offered' = IF usable
                        THEN [offered EXCEPT ![s] =
                            IF Mutation = "last-qos-wins" THEN effective
                            ELSE IF @ = 1 \/ effective = 1 THEN 1 ELSE 0]
                        ELSE offered
    /\ UNCHANGED <<routes, revision, phase, publication, snapshot, cut,
                   decision, hExpected, hFrozen, hStaleCut>>

Prepare == /\ phase = "selecting" /\ remaining = {}
           /\ phase' = "prepared"
           /\ UNCHANGED <<routes, revision, publication, snapshot, cut,
                          remaining, copies, offered, decision, hExpected,
                          hFrozen, hStaleCut>>

Commit ==
    /\ phase = "prepared"
    /\ LET accepts == cut = revision \/ Mutation = "ignore-cut"
           selected == [present |-> TRUE, copies |-> copies, qos |-> offered]
       IN /\ phase' = IF accepts THEN "committed" ELSE "rejected"
          /\ decision' = IF accepts THEN selected ELSE EmptyDecision
          /\ hFrozen' = IF accepts THEN selected ELSE EmptyDecision
          /\ hStaleCut' = (hStaleCut \/ (accepts /\ cut # revision))
    /\ UNCHANGED <<routes, revision, publication, snapshot, cut, remaining,
                   copies, offered, hExpected>>

\* An upsert or unsubscribe is one atomic row/projection change. A committed
\* publication's obligations are never rerouted by a later subscription cut.
Update(e, option) ==
    /\ revision < MaxChanges /\ e \in {1, 2, 6} /\ option \in Choices
    /\ routes[e] # option
    /\ routes' = [routes EXCEPT ![e] = option]
    /\ revision' = revision + 1
    /\ decision' = IF Mutation = "retroactive" /\ decision.present
                    THEN Expected(routes', publication) ELSE decision
    /\ UNCHANGED <<phase, publication, snapshot, cut, remaining, copies,
                   offered, hExpected, hFrozen, hStaleCut>>

Idle == UNCHANGED vars
Next == (\E s \in Sessions, t \in Topics, q \in 0..2 : Begin(s, t, q))
        \/ (\E e \in Entries : Scan(e)) \/ Prepare \/ Commit
        \/ (\E e \in {1, 2, 6}, o \in Choices : Update(e, o)) \/ Idle
Spec == Init /\ [][Next]_vars

Selection == [present : BOOLEAN, copies : [Sessions -> 0..6],
               qos : [Sessions -> 0..1]]
TypeOK == /\ routes \in [Entries -> Option] /\ snapshot \in [Entries -> Option]
          /\ revision \in 0..MaxChanges /\ cut \in 0..MaxChanges
          /\ phase \in {"idle", "selecting", "prepared", "committed", "rejected"}
          /\ publication \in [source : Sessions, topic : Topics, qos : 0..2]
          /\ remaining \subseteq Entries /\ copies \in [Sessions -> 0..6]
          /\ offered \in [Sessions -> 0..1]
          /\ decision \in Selection /\ hExpected \in Selection
          /\ hFrozen \in Selection /\ hStaleCut \in BOOLEAN
OnePerDestination == decision.present => \A s \in Sessions : decision.copies[s] <= 1
EligibleRecipients == decision.present => decision.copies = hExpected.copies
EligibleMaximumQoS == decision.present =>
    \A s \in Sessions : decision.copies[s] > 0 /\ hExpected.copies[s] > 0
                         => decision.qos[s] = hExpected.qos[s]
CurrentRoutingCut == ~hStaleCut
FrozenResponsibilities == decision.present => decision = hFrozen
NoOverlapWitness == ~(decision.present /\ publication.source = 1
                       /\ publication.topic = "leaf" /\ publication.qos = 2
                       /\ decision.copies = [s \in Sessions |-> 1]
                       /\ decision.qos = [s \in Sessions |-> IF s = 1 THEN 0 ELSE 1])
=============================================================================
