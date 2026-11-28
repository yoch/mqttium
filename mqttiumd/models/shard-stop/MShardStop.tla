----------------------------- MODULE MShardStop -----------------------------
(***************************************************************************)
(* Ordered delivery-shard shutdown and panic reporting                     *)
(* (delivery_shard.rs: ShardPool::shutdown, atomic ShardClient::stop,      *)
(* DeliveryShardWorker::drain_coord_messages / run_loop, catch_unwind ->   *)
(* Notices::publish(ShardToCoord::Fatal), and Writer::Stop ordering).      *)
(*                                                                         *)
(* Guarantees:                                                             *)
(* 1. Shutdown uses an atomic stop flag outside the bounded CoordToShard   *)
(*    lane, so a saturated lane never loses the stop request.              *)
(* 2. On clean stop, the shard drains every message queued before `stop`   *)
(*    and finishes every in-flight store command before exiting.           *)
(* 3. A shard panic is caught and published to the dedicated Notices fatal *)
(*    slot even if the close lane is saturated.                            *)
(* 4. The coordinator sends Writer::Stop only after every shard exits.     *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS
    LaneCap,    \* Bounded CoordToShard channel capacity (e.g. 2)
    MaxSent,    \* Coordinator messages sent before shutdown (e.g. 2)
    AllowPanic, \* Whether shard may panic mid-run
    Mutation    \* "None", "LaneStop", "DropInflight", "SilentPanic"

VARIABLES
    sent,
    laneLen,
    stopRequested,
    stopFlag,
    stopLost,
    shardStopping,
    inflight,
    shardState,
    fatalPublished,
    writerStopped

vars == << sent, laneLen, stopRequested, stopFlag, stopLost,
           shardStopping, inflight, shardState, fatalPublished,
           writerStopped >>

Init ==
    /\ sent = 0
    /\ laneLen = 0
    /\ stopRequested = FALSE
    /\ stopFlag = FALSE
    /\ stopLost = FALSE
    /\ shardStopping = FALSE
    /\ inflight = 0
    /\ shardState = "running"
    /\ fatalPublished = FALSE
    /\ writerStopped = FALSE

\* Coordinator sends a CoordToShard message into the bounded lane.
CoordSend ==
    /\ ~stopRequested
    /\ sent < MaxSent
    /\ laneLen < LaneCap
    /\ sent' = sent + 1
    /\ laneLen' = laneLen + 1
    /\ UNCHANGED << stopRequested, stopFlag, stopLost, shardStopping,
                    inflight, shardState, fatalPublished, writerStopped >>

\* ShardPool::shutdown sets atomic `stop` outside the bounded lane.
CoordRequestStop ==
    /\ ~stopRequested
    /\ stopRequested' = TRUE
    /\ IF Mutation = "LaneStop" THEN
           IF laneLen < LaneCap THEN
               /\ stopFlag' = TRUE
               /\ UNCHANGED stopLost
           ELSE
               /\ stopLost' = TRUE
               /\ UNCHANGED stopFlag
       ELSE
           /\ stopFlag' = TRUE
           /\ UNCHANGED stopLost
    /\ UNCHANGED << sent, laneLen, shardStopping, inflight, shardState,
                    fatalPublished, writerStopped >>

\* DeliveryShardWorker::drain_coord_messages consumes a queued message.
ShardRecvMsg ==
    /\ shardState = "running"
    /\ laneLen > 0
    /\ laneLen' = laneLen - 1
    /\ inflight' = inflight + 1
    /\ UNCHANGED << sent, stopRequested, stopFlag, stopLost, shardStopping,
                    shardState, fatalPublished, writerStopped >>

\* drain_coord_messages sees TryRecvError::Empty and reads atomic `stop`.
ShardObserveStop ==
    /\ shardState = "running"
    /\ ~shardStopping
    /\ stopFlag
    /\ (laneLen = 0 \/ Mutation = "DropInflight")
    /\ shardStopping' = TRUE
    /\ UNCHANGED << sent, laneLen, stopRequested, stopFlag, stopLost,
                    inflight, shardState, fatalPublished, writerStopped >>

\* Writer finishes one in-flight store command for the shard.
StoreFinishOne ==
    /\ shardState = "running"
    /\ inflight > 0
    /\ inflight' = inflight - 1
    /\ UNCHANGED << sent, laneLen, stopRequested, stopFlag, stopLost,
                    shardStopping, shardState, fatalPublished, writerStopped >>

\* run_loop exits cleanly once stopping and all store work drained.
ShardExitOk ==
    /\ shardState = "running"
    /\ shardStopping
    /\ ((laneLen = 0 /\ inflight = 0) \/ Mutation = "DropInflight")
    /\ shardState' = "exited_ok"
    /\ UNCHANGED << sent, laneLen, stopRequested, stopFlag, stopLost,
                    shardStopping, inflight, fatalPublished, writerStopped >>

\* catch_unwind catches a shard panic and publishes ShardToCoord::Fatal to Notices.
ShardPanic ==
    /\ AllowPanic
    /\ shardState = "running"
    /\ shardState' = "exited_err"
    /\ fatalPublished' = IF Mutation = "SilentPanic" THEN FALSE ELSE TRUE
    /\ UNCHANGED << sent, laneLen, stopRequested, stopFlag, stopLost,
                    shardStopping, inflight, writerStopped >>

\* Coordinator stops the Writer only after ShardPool::shutdown joins all shards.
CoordStopWriter ==
    /\ stopRequested
    /\ shardState \in {"exited_ok", "exited_err"}
    /\ ~writerStopped
    /\ writerStopped' = TRUE
    /\ UNCHANGED << sent, laneLen, stopRequested, stopFlag, stopLost,
                    shardStopping, inflight, shardState, fatalPublished >>

Done == writerStopped /\ UNCHANGED vars

Next ==
    \/ CoordSend
    \/ CoordRequestStop
    \/ ShardRecvMsg
    \/ ShardObserveStop
    \/ StoreFinishOne
    \/ ShardExitOk
    \/ ShardPanic
    \/ CoordStopWriter
    \/ Done

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ sent \in 0..MaxSent
    /\ laneLen \in 0..LaneCap
    /\ stopRequested \in BOOLEAN
    /\ stopFlag \in BOOLEAN
    /\ stopLost \in BOOLEAN
    /\ shardStopping \in BOOLEAN
    /\ inflight \in 0..MaxSent
    /\ shardState \in {"running", "exited_ok", "exited_err"}
    /\ fatalPublished \in BOOLEAN
    /\ writerStopped \in BOOLEAN

StopNeverLost == ~stopLost

CleanExitDrainedAll ==
    (shardState = "exited_ok") => (laneLen = 0 /\ inflight = 0 /\ stopFlag)

WriterStopsAfterShard ==
    writerStopped => shardState \in {"exited_ok", "exited_err"}

PanicAlwaysReported ==
    (shardState = "exited_err") => fatalPublished

=============================================================================
