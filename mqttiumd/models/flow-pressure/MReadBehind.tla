--------------------------- MODULE MReadBehind ---------------------------
(***************************************************************************)
(* What the runtime reads behind an MQTT 3.1.1 PUBLISH that waits for a    *)
(* slot of the receive window (record 0060), as written in                 *)
(* crates/mqttiumd/src/runtime.rs and receiving.rs:                        *)
(*                                                                         *)
(*   Peer::overtakes_deferred    Independent: a PUBREL whose identifier    *)
(*                               is neither the waiting PUBLISH's nor      *)
(*                               that of a PUBLISH kept behind             *)
(*   PeerCold::room_behind       CanKeep: packets and bytes                *)
(*   Peer::keep_behind           the packet joins `behind`                 *)
(*   Peer::lookahead_blocked     the head neither passes nor fits          *)
(*   Window::all_wait_for_remote_pubrel   AllOwed                          *)
(*                                                                         *)
(* The waiting PUBLISH has the identifier WaitingId; slots 1 and 2 are     *)
(* the full window. `passed` is history: the positions of the stream       *)
(* handed to the core ahead of the waiting PUBLISH. No guard reads it.     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets
CONSTANT Mutation
Pub(i, n) == [kind |-> "P", id |-> i, bytes |-> n]
Rel(i) == [kind |-> "R", id |-> i, bytes |-> 1]
WaitingId == 3
Streams == { <<Pub(4,1), Pub(5,1), Rel(1)>>,
             <<Pub(4,2), Pub(5,2), Rel(1)>>,
             <<Pub(4,1), Pub(5,1), Pub(6,1), Rel(1)>>,
             <<Rel(3), Pub(4,1), Rel(4), Rel(1)>>,
             <<Rel(1), Pub(4,2), Pub(5,2), Rel(2)>> }
VARIABLES stream, cursor, behind, slots, state, passed
vars == <<stream, cursor, behind, slots, state, passed>>
Init == /\ stream \in Streams /\ cursor = 1 /\ behind = <<>>
        /\ slots \in { [i \in 1..2 |-> "RecQueued"],
                       [i \in 1..2 |-> IF i = 1 THEN "OwnAck" ELSE "RecQueued"] }
        /\ state = "Waiting" /\ passed = {}
RECURSIVE ByteCount(_)
ByteCount(q) == IF q = <<>> THEN 0 ELSE Head(q).bytes + ByteCount(Tail(q))
CanKeep(p) == Len(behind) < 2 /\ ByteCount(behind) + p.bytes <= 2
Independent(p) == p.kind = "R" /\ p.id # WaitingId /\
                  \A i \in 1..Len(behind) : behind[i].kind # "P" \/ behind[i].id # p.id
AllOwed == \A i \in 1..2 : slots[i] = "RecWritten"
LocalRoom == \E i \in 1..2 : slots[i] = "Free"
WriteRec == /\ state = "Waiting"
            /\ \E i \in 1..2 : /\ slots[i] = "RecQueued"
                               /\ slots' = [slots EXCEPT ![i] = "RecWritten"]
            /\ UNCHANGED <<stream, cursor, behind, state, passed>>
FinishRel == /\ state = "Waiting"
             /\ \E i \in 1..2 : /\ slots[i] \in {"Releasing", "OwnAck"}
                                /\ slots' = [slots EXCEPT ![i] = "Free"]
             /\ UNCHANGED <<stream, cursor, behind, state, passed>>
Read == /\ state = "Waiting" /\ ~LocalRoom /\ cursor <= Len(stream)
        /\ LET p == stream[cursor]
               bypass == Independent(p) \/ (Mutation = "AnyRel" /\ p.kind = "R")
               releasing == p.id \in 1..2 /\ slots[p.id] \in {"RecWritten","RecQueued"}
           IN IF bypass THEN
                   /\ slots' = IF releasing THEN [slots EXCEPT ![p.id] = "Releasing"] ELSE slots
                   /\ cursor' = cursor + 1 /\ passed' = passed \cup {cursor}
                   /\ UNCHANGED <<stream, behind, state>>
              ELSE IF CanKeep(p) THEN
                   /\ behind' = Append(behind, p) /\ cursor' = cursor + 1
                   /\ UNCHANGED <<stream, slots, state, passed>>
              ELSE /\ Mutation # "WaitForever" /\ (AllOwed \/ Mutation = "CloseEarly")
                   /\ state' = "Closed" /\ UNCHANGED <<stream, cursor, behind, slots, passed>>
Resume == /\ state = "Waiting" /\ LocalRoom /\ state' = "Admitted"
          /\ UNCHANGED <<stream, cursor, behind, slots, passed>>
Terminal == /\ state # "Waiting" /\ UNCHANGED vars
Next == WriteRec \/ Read \/ FinishRel \/ Resume \/ Terminal
FairSpec == Init /\ [][Next]_vars /\ WF_vars(WriteRec) /\ WF_vars(Read)
                 /\ WF_vars(FinishRel) /\ WF_vars(Resume)
Bounded == Len(behind) <= 2 /\ ByteCount(behind) <= 2
\* The order the core is given, read on the stream and on `passed` alone:
\* neither `behind` nor Independent is named. A packet that went ahead
\* reached the core before the waiting PUBLISH and before every earlier
\* packet of the stream that did not go ahead. None of those is a PUBLISH
\* of its own identifier: a PUBREL applied before the PUBLISH it answers
\* releases nothing, and the exchange that PUBLISH then opens has no
\* PUBREL left to come.
NoPublishOvertaken ==
    \A k \in passed :
        /\ stream[k].id # WaitingId
        /\ \A j \in 1..(k-1) :
              (j \notin passed /\ stream[j].kind = "P") => stream[j].id # stream[k].id
\* The guard of the closing branch of Read, written a second time on the
\* state that branch leaves, without AllOwed: nothing moves after a close,
\* so this says no more than that guard and fails only when the guard or
\* AllOwed is weakened (CloseEarly). It guards a regression of the guard;
\* that the guard is the right one is not shown here.
CloseOnlyWhenPeerOwes ==
    state = "Closed" => \A i \in 1..2 : slots[i] = "RecWritten"
Terminates == <>(state # "Waiting")
NoPacketClose == ~(state = "Closed" /\ Len(behind) = 2 /\ stream[cursor] = Pub(6,1))
NoByteClose == ~(state = "Closed" /\ Len(behind) = 1 /\ behind[1] = Pub(4,2))
NoReleasedSlot == ~(state = "Admitted" /\ Len(behind) = 2)
============================================================================
