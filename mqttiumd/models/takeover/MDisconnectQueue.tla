-------------------------- MODULE MDisconnectQueue --------------------------
EXTENDS Naturals, Sequences

CONSTANT SealOnDisconnect
ASSUME SealOnDisconnect \in BOOLEAN

Producers == {"Coordinator", "Shard"}
Disconnect == "DISCONNECT"
Frames == {Disconnect, "PUBLISH1", "PUBLISH2"}
Capacity == 2

VARIABLES pending, accepting, out, batch, wire
vars == <<pending, accepting, out, batch, wire>>

Init ==
    /\ pending = [p \in Producers |->
                     IF p = "Coordinator" THEN <<Disconnect>> ELSE <<"PUBLISH1", "PUBLISH2">>]
    /\ accepting = TRUE
    /\ out = <<>> /\ batch = <<>> /\ wire = <<>>

\* One successful call under the outgoing mutex. A full FIFO leaves the
\* frame with its producer, which can retry after TakeBatch frees space.
Admit(p) ==
    /\ pending[p] # <<>> /\ accepting /\ Len(out) < Capacity
    /\ LET frame == Head(pending[p]) IN
         /\ out' = Append(out, frame)
         /\ accepting' = ~(SealOnDisconnect /\ frame = Disconnect)
    /\ pending' = [pending EXCEPT ![p] = Tail(@)]
    /\ UNCHANGED <<batch, wire>>

RejectSealed(p) ==
    /\ pending[p] # <<>> /\ ~accepting
    /\ pending' = [pending EXCEPT ![p] = Tail(@)]
    /\ UNCHANGED <<accepting, out, batch, wire>>

\* The single reader may take one frame or a FIFO prefix of coalescable
\* frames. Its socket write can finish after another producer takes the lock.
TakeBatch ==
    /\ batch = <<>> /\ out # <<>>
    /\ \E n \in 1..Len(out) :
           /\ batch' = SubSeq(out, 1, n)
           /\ out' = SubSeq(out, n+1, Len(out))
    /\ UNCHANGED <<pending, accepting, wire>>

WriteOne ==
    /\ batch # <<>>
    /\ wire' = Append(wire, Head(batch))
    /\ batch' = Tail(batch)
    /\ UNCHANGED <<pending, accepting, out>>

Finished ==
    /\ \A p \in Producers : pending[p] = <<>>
    /\ out = <<>> /\ batch = <<>>
    /\ UNCHANGED vars

Next ==
    \/ \E p \in Producers : Admit(p) \/ RejectSealed(p)
    \/ TakeBatch \/ WriteOne \/ Finished

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ pending \in [Producers -> Seq(Frames)]
    /\ accepting \in BOOLEAN
    /\ out \in Seq(Frames) /\ Len(out) <= Capacity
    /\ batch \in Seq(Frames) /\ Len(batch) <= Capacity
    /\ wire \in Seq(Frames)

TerminalWire ==
    \A i \in 1..Len(wire) : wire[i] = Disconnect => i = Len(wire)

\* Negation of the schedule exercised by the deterministic socket test.
NoDrainedPrefixWitness ==
    ~(wire = <<"PUBLISH1", Disconnect>> /\ pending["Shard"] = <<>>)

=============================================================================
