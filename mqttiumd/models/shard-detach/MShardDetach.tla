---------------------------- MODULE MShardDetach ----------------------------
(***************************************************************************)
(* Generation closure, closing-connection fence, and shard detach fence    *)
(* for --delivery-shards K >= 1 (runtime.rs::close / finish_close,         *)
(* delivery_shard.rs::finish_detaches with O(1) terminal_by_conn (D9),     *)
(* ConnCore::closing (D7), and Owner::shard_ack_progress).                 *)
(*                                                                         *)
(* Guarantees:                                                             *)
(* 1. Once the coordinator marks generation 1 closing, the delivery shard  *)
(*    cannot enqueue any new frame onto generation 1 (D7).                 *)
(* 2. The shard signals detach `completed` only after the I/O task has     *)
(*    finished, zero shard ACKs/written events remain in flight, the O(1)  *)
(*    per-connection terminal index `terminal_by_conn` is 0 (D9), and all  *)
(*    FinishDelivery/PreparePubrel store commands for gen 1 have settled.  *)
(* 3. Generation 2 cannot attach until generation 1's detach completes.    *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS
    Mutation \* "None", "WriteWhileClosing", "IgnoreTerminal", "IgnoreFinishInFlight"

VARIABLES
    coordClosing,
    ioFinished,
    detachSent,
    completed,
    gen2Attached,
    pendingIoAcks,
    pendingIoWritten,
    terminalQueue,
    terminalByConn,
    finishInFlight,
    wroteAfterClosing,
    shardGen

vars == << coordClosing, ioFinished, detachSent, completed, gen2Attached,
           pendingIoAcks, pendingIoWritten, terminalQueue, terminalByConn,
           finishInFlight, wroteAfterClosing, shardGen >>

Init ==
    /\ coordClosing = FALSE
    /\ ioFinished = FALSE
    /\ detachSent = FALSE
    /\ completed = FALSE
    /\ gen2Attached = FALSE
    /\ pendingIoAcks = 1
    /\ pendingIoWritten = 1
    /\ terminalQueue = 0
    /\ terminalByConn = 0
    /\ finishInFlight = 0
    /\ wroteAfterClosing = FALSE
    /\ shardGen = 1

\* Coordinator Peer::set_closing / Runtime::close sets ConnCore::closing.
CoordMarkClosing ==
    /\ ~coordClosing
    /\ coordClosing' = TRUE
    /\ UNCHANGED << ioFinished, detachSent, completed, gen2Attached,
                    pendingIoAcks, pendingIoWritten, terminalQueue,
                    terminalByConn, finishInFlight, wroteAfterClosing, shardGen >>

\* I/O task finishes after publishing final ACK and Written events.
IoFinish ==
    /\ coordClosing
    /\ ~ioFinished
    /\ ioFinished' = TRUE
    /\ UNCHANGED << coordClosing, detachSent, completed, gen2Attached,
                    pendingIoAcks, pendingIoWritten, terminalQueue,
                    terminalByConn, finishInFlight, wroteAfterClosing, shardGen >>

\* Runtime::finish_close sends CoordToShard::Detach once peer.io.is_finished().
CoordSendDetach ==
    /\ ioFinished
    /\ ~detachSent
    /\ detachSent' = TRUE
    /\ UNCHANGED << coordClosing, ioFinished, completed, gen2Attached,
                    pendingIoAcks, pendingIoWritten, terminalQueue,
                    terminalByConn, finishInFlight, wroteAfterClosing, shardGen >>

\* ShardSink::try_submit checks !outgoing.is_closing() (and ConnCore::try_send).
ShardTryWrite ==
    /\ shardGen = 1
    /\ ~ioFinished
    /\ IF Mutation = "WriteWhileClosing" THEN
           /\ coordClosing
           /\ ~wroteAfterClosing
           /\ wroteAfterClosing' = TRUE
       ELSE
           /\ ~coordClosing
           /\ UNCHANGED wroteAfterClosing
    /\ UNCHANGED << coordClosing, ioFinished, detachSent, completed, gen2Attached,
                    pendingIoAcks, pendingIoWritten, terminalQueue,
                    terminalByConn, finishInFlight, shardGen >>

\* DeliveryShardWorker::drain_io_events processes ACK -> emits FinishDelivery.
ShardDrainIoAck ==
    /\ shardGen = 1
    /\ pendingIoAcks > 0
    /\ pendingIoAcks' = pendingIoAcks - 1
    /\ finishInFlight' = finishInFlight + 1
    /\ UNCHANGED << coordClosing, ioFinished, detachSent, completed, gen2Attached,
                    pendingIoWritten, terminalQueue, terminalByConn,
                    wroteAfterClosing, shardGen >>

\* DeliveryShardWorker::handle_written enqueues terminal input and updates terminal_by_conn.
ShardDrainIoWritten ==
    /\ shardGen = 1
    /\ pendingIoWritten > 0
    /\ pendingIoWritten' = pendingIoWritten - 1
    /\ terminalQueue' = terminalQueue + 1
    /\ terminalByConn' = terminalByConn + 1
    /\ UNCHANGED << coordClosing, ioFinished, detachSent, completed, gen2Attached,
                    pendingIoAcks, finishInFlight, wroteAfterClosing, shardGen >>

\* DeliveryShardWorker::drain_inputs steps terminal input and decrements terminal_by_conn.
ShardStepTerminal ==
    /\ terminalQueue > 0
    /\ terminalQueue' = terminalQueue - 1
    /\ terminalByConn' = terminalByConn - 1
    /\ UNCHANGED << coordClosing, ioFinished, detachSent, completed, gen2Attached,
                    pendingIoAcks, pendingIoWritten, finishInFlight,
                    wroteAfterClosing, shardGen >>

\* Writer commits and shard applies FinishDelivery store result.
ShardFinishCommit ==
    /\ finishInFlight > 0
    /\ finishInFlight' = finishInFlight - 1
    /\ UNCHANGED << coordClosing, ioFinished, detachSent, completed, gen2Attached,
                    pendingIoAcks, pendingIoWritten, terminalQueue,
                    terminalByConn, wroteAfterClosing, shardGen >>

\* DeliveryShardWorker::finish_detaches checks I/O, O(1) terminal_by_conn, and shard_ack_progress.
ShardFinishDetach ==
    /\ detachSent
    /\ ~completed
    /\ pendingIoAcks = 0
    /\ pendingIoWritten = 0
    /\ (Mutation = "IgnoreTerminal" \/ terminalByConn = 0)
    /\ (Mutation = "IgnoreFinishInFlight" \/ finishInFlight = 0)
    /\ completed' = TRUE
    /\ shardGen' = 0
    /\ UNCHANGED << coordClosing, ioFinished, detachSent, gen2Attached,
                    pendingIoAcks, pendingIoWritten, terminalQueue,
                    terminalByConn, finishInFlight, wroteAfterClosing >>

\* Coordinator attaches replacement generation 2 after gen 1 detach completes.
CoordAttachGen2 ==
    /\ completed
    /\ ~gen2Attached
    /\ gen2Attached' = TRUE
    /\ shardGen' = 2
    /\ UNCHANGED << coordClosing, ioFinished, detachSent, completed,
                    pendingIoAcks, pendingIoWritten, terminalQueue,
                    terminalByConn, finishInFlight, wroteAfterClosing >>

Terminal ==
    /\ completed
    /\ gen2Attached
    /\ pendingIoAcks = 0
    /\ pendingIoWritten = 0
    /\ terminalQueue = 0
    /\ finishInFlight = 0

Done == Terminal /\ UNCHANGED vars

Next ==
    \/ CoordMarkClosing
    \/ IoFinish
    \/ CoordSendDetach
    \/ ShardTryWrite
    \/ ShardDrainIoAck
    \/ ShardDrainIoWritten
    \/ ShardStepTerminal
    \/ ShardFinishCommit
    \/ ShardFinishDetach
    \/ CoordAttachGen2
    \/ Done

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ coordClosing \in BOOLEAN
    /\ ioFinished \in BOOLEAN
    /\ detachSent \in BOOLEAN
    /\ completed \in BOOLEAN
    /\ gen2Attached \in BOOLEAN
    /\ pendingIoAcks \in 0..1
    /\ pendingIoWritten \in 0..1
    /\ terminalQueue \in 0..1
    /\ terminalByConn \in 0..1
    /\ finishInFlight \in 0..1
    /\ wroteAfterClosing \in BOOLEAN
    /\ shardGen \in {0, 1, 2}

NoWriteAfterClosing == ~wroteAfterClosing

TerminalIndexExact == terminalByConn = terminalQueue

NoDetachWithPendingWork ==
    completed =>
        /\ ioFinished
        /\ pendingIoAcks = 0
        /\ pendingIoWritten = 0
        /\ terminalQueue = 0
        /\ finishInFlight = 0

NoOverlapWithNextGen == gen2Attached => completed

TerminalConsistent == Terminal => shardGen = 2

=============================================================================
