--------------------------- MODULE MReadBehind ---------------------------
EXTENDS Naturals, Sequences, FiniteSets
CONSTANT Mutation
Pub(i, n) == [kind |-> "P", id |-> i, bytes |-> n]
Rel(i) == [kind |-> "R", id |-> i, bytes |-> 1]
Streams == { <<Pub(4,1), Pub(5,1), Rel(1)>>,
             <<Pub(4,2), Pub(5,2), Rel(1)>>,
             <<Pub(4,1), Pub(5,1), Pub(6,1), Rel(1)>>,
             <<Rel(3), Pub(4,1), Rel(4), Rel(1)>>,
             <<Rel(1), Pub(4,2), Pub(5,2), Rel(2)>> }
VARIABLES stream, cursor, behind, slots, state, crossed
vars == <<stream, cursor, behind, slots, state, crossed>>
Init == /\ stream \in Streams /\ cursor = 1 /\ behind = <<>>
        /\ slots \in { [i \in 1..2 |-> "RecQueued"],
                       [i \in 1..2 |-> IF i = 1 THEN "OwnAck" ELSE "RecQueued"] }
        /\ state = "Waiting" /\ crossed = FALSE
RECURSIVE ByteCount(_)
ByteCount(q) == IF q = <<>> THEN 0 ELSE Head(q).bytes + ByteCount(Tail(q))
CanKeep(p) == Len(behind) < 2 /\ ByteCount(behind) + p.bytes <= 2
Independent(p) == p.kind = "R" /\ p.id # 3 /\
                  \A i \in 1..Len(behind) : behind[i].kind # "P" \/ behind[i].id # p.id
AllOwed == \A i \in 1..2 : slots[i] = "RecWritten"
LocalRoom == \E i \in 1..2 : slots[i] = "Free"
WriteRec == /\ state = "Waiting"
            /\ \E i \in 1..2 : /\ slots[i] = "RecQueued"
                               /\ slots' = [slots EXCEPT ![i] = "RecWritten"]
            /\ UNCHANGED <<stream, cursor, behind, state, crossed>>
FinishRel == /\ state = "Waiting"
             /\ \E i \in 1..2 : /\ slots[i] \in {"Releasing", "OwnAck"}
                                /\ slots' = [slots EXCEPT ![i] = "Free"]
             /\ UNCHANGED <<stream, cursor, behind, state, crossed>>
Read == /\ state = "Waiting" /\ ~LocalRoom /\ cursor <= Len(stream)
        /\ LET p == stream[cursor]
               bypass == Independent(p) \/ (Mutation = "AnyRel" /\ p.kind = "R")
               releasing == p.id \in 1..2 /\ slots[p.id] \in {"RecWritten","RecQueued"}
           IN IF bypass THEN
                   /\ slots' = IF releasing THEN [slots EXCEPT ![p.id] = "Releasing"] ELSE slots
                   /\ cursor' = cursor + 1 /\ crossed' = (crossed \/ ~Independent(p))
                   /\ UNCHANGED <<stream, behind, state>>
              ELSE IF CanKeep(p) THEN
                   /\ behind' = Append(behind, p) /\ cursor' = cursor + 1
                   /\ UNCHANGED <<stream, slots, state, crossed>>
              ELSE /\ Mutation # "WaitForever" /\ (AllOwed \/ Mutation = "CloseEarly")
                   /\ state' = "Closed" /\ UNCHANGED <<stream, cursor, behind, slots, crossed>>
Resume == /\ state = "Waiting" /\ LocalRoom /\ state' = "Admitted"
          /\ UNCHANGED <<stream, cursor, behind, slots, crossed>>
Terminal == /\ state # "Waiting" /\ UNCHANGED vars
Next == WriteRec \/ Read \/ FinishRel \/ Resume \/ Terminal
FairSpec == Init /\ [][Next]_vars /\ WF_vars(WriteRec) /\ WF_vars(Read)
                 /\ WF_vars(FinishRel) /\ WF_vars(Resume)
Bounded == Len(behind) <= 2 /\ ByteCount(behind) <= 2
IndependentOnly == ~crossed
CloseOnlyWhenPeerOwes == state = "Closed" => AllOwed
Terminates == <>(state # "Waiting")
NoPacketClose == ~(state = "Closed" /\ Len(behind) = 2 /\ stream[cursor] = Pub(6,1))
NoByteClose == ~(state = "Closed" /\ Len(behind) = 1 /\ behind[1] = Pub(4,2))
NoReleasedSlot == ~(state = "Admitted" /\ Len(behind) = 2)
============================================================================
