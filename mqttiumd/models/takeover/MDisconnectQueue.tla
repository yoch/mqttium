-------------------------- MODULE MDisconnectQueue --------------------------
(***************************************************************************)
(* MDisconnectQueue: the order of the MQTT packets one connection writes   *)
(* once a DISCONNECT is queued for it (MQTT-3.14.4-1), as written in       *)
(* crates/mqttiumd/src/net.rs: `OutgoingState::enqueue`,                   *)
(* `ConnCore::try_send_outgoing`, `ConnCore::pop_outgoing_batch` and       *)
(* `ConnCore::finish_task`.                                                *)
(*                                                                         *)
(* Two producers submit to the one output queue of the connection: the     *)
(* coordinator, which queues the DISCONNECT (`start_takeover` and the      *)
(* other closes of runtime.rs), and one other producer with two PUBLISH    *)
(* packets in order. With `--delivery-shards K`, K >= 1, that producer is  *)
(* the delivery shard and its `OutgoingHandle`, on another thread. With    *)
(* the single owner (`--delivery-shards 0`) it stands for a later          *)
(* submission of the coordinator itself: both go through the same lock.    *)
(*                                                                         *)
(* Not here: the bound on the writes a shard owns                          *)
(* (`ShardIoRoute::reserve_write`), whose refusal is a submission that     *)
(* does not happen; what a producer does with a refusal; receipts; a       *)
(* second DISCONNECT; TCP fragmentation and TLS records (the model         *)
(* observes whole MQTT packets); the close of the connection and the open  *)
(* of the next generation (MTakeoverInFlight).                             *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANT SealOnDisconnect
ASSUME SealOnDisconnect \in BOOLEAN

Producers == {"Coordinator", "Shard"}
Disconnect == "DISCONNECT"
Frames == {Disconnect, "PUBLISH1", "PUBLISH2"}
Capacity == 2

VARIABLES
    pending,    \* per producer: the frames it has not submitted yet
    accepting,  \* OutgoingState::accepting_frames
    out,        \* OutgoingState::queue
    batch,      \* the frames the socket task took and has not written
    wire        \* the MQTT packets written to the socket, in order
vars == <<pending, accepting, out, batch, wire>>

Init ==
    /\ pending = [p \in Producers |->
                     IF p = "Coordinator" THEN <<Disconnect>> ELSE <<"PUBLISH1", "PUBLISH2">>]
    /\ accepting = TRUE
    /\ out = <<>> /\ batch = <<>> /\ wire = <<>>

\* One successful try_send_outgoing, under the output mutex: `enqueue`
\* places the frame and, for a DISCONNECT, closes admission. A full queue
\* answers Full and the frame stays with its producer: the absence of this
\* step. A producer may submit it again once TakeBatch made room; the
\* coordinator does not do so for a DISCONNECT (`start_takeover` closes
\* the connection instead), which is a run where it never submits.
Admit(p) ==
    /\ pending[p] # <<>> /\ accepting /\ Len(out) < Capacity
    /\ LET frame == Head(pending[p]) IN
         /\ out' = Append(out, frame)
         /\ accepting' = ~(SealOnDisconnect /\ frame = Disconnect)
    /\ pending' = [pending EXCEPT ![p] = Tail(@)]
    /\ UNCHANGED <<batch, wire>>

\* try_send_outgoing answers Disconnected: the frame is not queued and its
\* producer gives it up.
RejectSealed(p) ==
    /\ pending[p] # <<>> /\ ~accepting
    /\ pending' = [pending EXCEPT ![p] = Tail(@)]
    /\ UNCHANGED <<accepting, out, batch, wire>>

\* pop_outgoing_batch: the single reader takes one frame or the first
\* frames of the queue for one write. Its socket write can finish after
\* another producer takes the lock.
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

\* The socket task ends, at any time: its socket is shut, then
\* finish_task seals the queue and empties it under the output mutex.
\* Nothing more is written, queued or not.
ReaderEnds ==
    /\ accepting \/ out # <<>> \/ batch # <<>>
    /\ accepting' = FALSE
    /\ out' = <<>> /\ batch' = <<>>
    /\ UNCHANGED <<pending, wire>>

Finished ==
    /\ \A p \in Producers : pending[p] = <<>>
    /\ out = <<>> /\ batch = <<>>
    /\ UNCHANGED vars

Next ==
    \/ \E p \in Producers : Admit(p) \/ RejectSealed(p)
    \/ TakeBatch \/ WriteOne \/ ReaderEnds \/ Finished

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ pending \in [Producers -> Seq(Frames)]
    /\ accepting \in BOOLEAN
    /\ out \in Seq(Frames) /\ Len(out) <= Capacity
    /\ batch \in Seq(Frames) /\ Len(batch) <= Capacity
    /\ wire \in Seq(Frames)

\* DISCONNECT is the last packet of every wire prefix that contains it.
\* It says nothing of a DISCONNECT that never reaches the wire.
TerminalWire ==
    \A i \in 1..Len(wire) : wire[i] = Disconnect => i = Len(wire)

\* Negation of the schedule exercised by the deterministic socket test.
NoDrainedPrefixWitness ==
    ~(wire = <<"PUBLISH1", Disconnect>> /\ pending["Shard"] = <<>>)

\* Negation of a run that ends with no DISCONNECT on the wire: the task
\* ended before it wrote the one that was queued, or before one was.
NoLostDisconnectWitness ==
    ~(pending["Coordinator"] = <<>> /\ accepting = FALSE /\ out = <<>> /\ batch = <<>>
      /\ \A i \in 1..Len(wire) : wire[i] # Disconnect)

=============================================================================
