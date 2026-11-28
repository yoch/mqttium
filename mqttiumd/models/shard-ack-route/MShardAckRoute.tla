--------------------------- MODULE MShardAckRoute ---------------------------
(***************************************************************************)
(* Direct I/O ACK and write-receipt routing for --delivery-shards K >= 1   *)
(* (crates/mqttiumd/src/net.rs: ShardAckBudget, ShardIoRoute,              *)
(* ConnCore::plan_read, ConnCore::park_unpaid_ack,                         *)
(* ConnCore::push_incoming_batch, ShardIoRoute::reserve_write /            *)
(* written_event).                                                         *)
(*                                                                         *)
(* Models a TCP or TLS socket reading an interleaved wire stream           *)
(* <<"ACK", "PING", "ACK", "DISC">> under bounded ShardAckBudget and       *)
(* correlated shard WriteSlot budgets:                                     *)
(* - Non-ACK packets (PING, PUBLISH) still progress at zero ACK credit;    *)
(* - An ACK read at zero credit is parked in StalledState.unpaid_ack, and  *)
(*   nothing behind it (e.g. DISCONNECT) is read until credit returns;     *)
(* - ShardAckCredit stays charged across ShardInbox and worker ack_inputs  *)
(*   until the worker consumes/drops it;                                   *)
(* - WriteSlot is charged on reserve_write, untouched by Written::Started, *)
(*   and released once when the terminal Written receipt is dropped.       *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    AckCap,     \* ShardAckBudget initial slots (e.g. 1)
    WriteCap,   \* ShardInbox write_slots initial slots (e.g. 1)
    MaxWrites,  \* Total correlated shard writes exercised (e.g. 2)
    Mutation    \* "None", "NoPark", "EarlyCredit", "DoubleWriteFree"

Wire == << "ACK", "PING", "ACK", "DISC" >>

VARIABLES
    wireIdx,
    availAck,
    pendingAck,
    unpaidAck,
    inbox,
    shardAcks,
    routedAcks,
    coordIn,
    readNonAckZeroCredit,
    writeSlots,
    writesAdmitted,
    activeWrites,
    startedSent,
    pendingStartWritten,
    pendingTermWritten

vars == << wireIdx, availAck, pendingAck, unpaidAck, inbox, shardAcks,
           routedAcks, coordIn, readNonAckZeroCredit, writeSlots,
           writesAdmitted, activeWrites, startedSent,
           pendingStartWritten, pendingTermWritten >>

Init ==
    /\ wireIdx = 1
    /\ availAck = AckCap
    /\ pendingAck = 0
    /\ unpaidAck = 0
    /\ inbox = << >>
    /\ shardAcks = << >>
    /\ routedAcks = {}
    /\ coordIn = << >>
    /\ readNonAckZeroCredit = FALSE
    /\ writeSlots = WriteCap
    /\ writesAdmitted = 0
    /\ activeWrites = 0
    /\ startedSent = 0
    /\ pendingStartWritten = 0
    /\ pendingTermWritten = 0

\* ConnCore::plan_read promotes a parked unpaid ACK once credit returns.
PromoteUnpaid ==
    /\ unpaidAck # 0
    /\ availAck > 0
    /\ availAck' = availAck - 1
    /\ pendingAck' = pendingAck + 1
    /\ inbox' = Append(inbox, unpaidAck)
    /\ routedAcks' = routedAcks \cup {unpaidAck}
    /\ unpaidAck' = 0
    /\ UNCHANGED << wireIdx, shardAcks, coordIn, readNonAckZeroCredit,
                    writeSlots, writesAdmitted, activeWrites, startedSent,
                    pendingStartWritten, pendingTermWritten >>

\* ConnCore::plan_read + push_incoming_batch when ACK credit > 0.
ReadWithCredit ==
    /\ unpaidAck = 0
    /\ wireIdx <= Len(Wire)
    /\ availAck > 0
    /\ IF Wire[wireIdx] = "ACK" THEN
           /\ availAck' = availAck - 1
           /\ pendingAck' = pendingAck + 1
           /\ inbox' = Append(inbox, wireIdx)
           /\ routedAcks' = routedAcks \cup {wireIdx}
           /\ UNCHANGED coordIn
       ELSE
           /\ coordIn' = Append(coordIn, wireIdx)
           /\ UNCHANGED << availAck, pendingAck, inbox, routedAcks >>
    /\ wireIdx' = wireIdx + 1
    /\ UNCHANGED << unpaidAck, shardAcks, readNonAckZeroCredit,
                    writeSlots, writesAdmitted, activeWrites, startedSent,
                    pendingStartWritten, pendingTermWritten >>

\* ConnCore::plan_read + park_unpaid_ack when ACK credit = 0.
ReadWithoutCredit ==
    /\ (unpaidAck = 0 \/ Mutation = "NoPark")
    /\ wireIdx <= Len(Wire)
    /\ availAck = 0
    /\ IF Wire[wireIdx] = "ACK" THEN
           /\ unpaidAck' = wireIdx
           /\ UNCHANGED << coordIn, readNonAckZeroCredit >>
       ELSE
           /\ coordIn' = Append(coordIn, wireIdx)
           /\ readNonAckZeroCredit' = TRUE
           /\ UNCHANGED unpaidAck
    /\ wireIdx' = wireIdx + 1
    /\ UNCHANGED << availAck, pendingAck, inbox, shardAcks, routedAcks,
                    writeSlots, writesAdmitted, activeWrites, startedSent,
                    pendingStartWritten, pendingTermWritten >>

\* DeliveryShardWorker::drain_io_events moves ACK from ShardInbox to ack_inputs.
ShardDrainInbox ==
    /\ inbox # << >>
    /\ shardAcks' = Append(shardAcks, Head(inbox))
    /\ inbox' = Tail(inbox)
    /\ IF Mutation = "EarlyCredit" THEN
           /\ availAck' = availAck + 1
           /\ pendingAck' = pendingAck - 1
       ELSE
           /\ UNCHANGED << availAck, pendingAck >>
    /\ UNCHANGED << wireIdx, unpaidAck, routedAcks, coordIn, readNonAckZeroCredit,
                    writeSlots, writesAdmitted, activeWrites, startedSent,
                    pendingStartWritten, pendingTermWritten >>

\* DeliveryShardWorker::drain_inputs steps ACK and drops ShardAckCredit.
ShardStepAck ==
    /\ shardAcks # << >>
    /\ shardAcks' = Tail(shardAcks)
    /\ IF Mutation = "EarlyCredit" THEN
           /\ UNCHANGED << availAck, pendingAck >>
       ELSE
           /\ availAck' = availAck + 1
           /\ pendingAck' = pendingAck - 1
    /\ UNCHANGED << wireIdx, unpaidAck, inbox, routedAcks, coordIn,
                    readNonAckZeroCredit, writeSlots, writesAdmitted,
                    activeWrites, startedSent, pendingStartWritten,
                    pendingTermWritten >>

\* ShardIoRoute::reserve_write on outgoing frame admission.
AdmitWrite ==
    /\ writesAdmitted < MaxWrites
    /\ writeSlots > 0
    /\ writeSlots' = writeSlots - 1
    /\ writesAdmitted' = writesAdmitted + 1
    /\ activeWrites' = activeWrites + 1
    /\ UNCHANGED << wireIdx, availAck, pendingAck, unpaidAck, inbox, shardAcks,
                    routedAcks, coordIn, readNonAckZeroCredit, startedSent,
                    pendingStartWritten, pendingTermWritten >>

\* ShardIoRoute::written_event for Written::Started (does not free WriteSlot).
EmitWriteStarted ==
    /\ activeWrites > startedSent
    /\ startedSent' = startedSent + 1
    /\ pendingStartWritten' = pendingStartWritten + 1
    /\ IF Mutation = "DoubleWriteFree" THEN
           writeSlots' = writeSlots + 1
       ELSE
           UNCHANGED writeSlots
    /\ UNCHANGED << wireIdx, availAck, pendingAck, unpaidAck, inbox, shardAcks,
                    routedAcks, coordIn, readNonAckZeroCredit, writesAdmitted,
                    activeWrites, pendingTermWritten >>

\* ShardIoRoute::written_event for terminal Written::Complete/Failed/NotStarted.
EmitWriteTerminal ==
    /\ activeWrites > 0
    /\ activeWrites' = activeWrites - 1
    /\ startedSent' = IF startedSent > activeWrites - 1 THEN activeWrites - 1 ELSE startedSent
    /\ pendingTermWritten' = pendingTermWritten + 1
    /\ UNCHANGED << wireIdx, availAck, pendingAck, unpaidAck, inbox, shardAcks,
                    routedAcks, coordIn, readNonAckZeroCredit, writeSlots,
                    writesAdmitted, pendingStartWritten >>

\* Shard consumes Written::Started (drops ShardWrittenCredit without WriteSlot).
ShardConsumeStarted ==
    /\ pendingStartWritten > 0
    /\ pendingStartWritten' = pendingStartWritten - 1
    /\ UNCHANGED << wireIdx, availAck, pendingAck, unpaidAck, inbox, shardAcks,
                    routedAcks, coordIn, readNonAckZeroCredit, writeSlots,
                    writesAdmitted, activeWrites, startedSent, pendingTermWritten >>

\* Shard consumes terminal Written (drops WriteSlot, returning 1 slot).
ShardConsumeTerminal ==
    /\ pendingTermWritten > 0
    /\ pendingTermWritten' = pendingTermWritten - 1
    /\ writeSlots' = writeSlots + 1
    /\ UNCHANGED << wireIdx, availAck, pendingAck, unpaidAck, inbox, shardAcks,
                    routedAcks, coordIn, readNonAckZeroCredit, writesAdmitted,
                    activeWrites, startedSent, pendingStartWritten >>

Terminal ==
    /\ wireIdx > Len(Wire)
    /\ unpaidAck = 0
    /\ inbox = << >>
    /\ shardAcks = << >>
    /\ writesAdmitted = MaxWrites
    /\ activeWrites = 0
    /\ pendingStartWritten = 0
    /\ pendingTermWritten = 0

Done == Terminal /\ UNCHANGED vars

Next ==
    \/ PromoteUnpaid
    \/ ReadWithCredit
    \/ ReadWithoutCredit
    \/ ShardDrainInbox
    \/ ShardStepAck
    \/ AdmitWrite
    \/ EmitWriteStarted
    \/ EmitWriteTerminal
    \/ ShardConsumeStarted
    \/ ShardConsumeTerminal
    \/ Done

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ wireIdx \in 1..(Len(Wire) + 1)
    /\ unpaidAck \in 0..Len(Wire)
    /\ routedAcks \subseteq 1..Len(Wire)
    /\ readNonAckZeroCredit \in BOOLEAN
    /\ writesAdmitted \in 0..MaxWrites
    /\ activeWrites \in 0..MaxWrites
    /\ startedSent \in 0..MaxWrites
    /\ pendingStartWritten \in 0..MaxWrites
    /\ pendingTermWritten \in 0..MaxWrites

BudgetBounded ==
    /\ availAck >= 0 /\ availAck <= AckCap
    /\ availAck + pendingAck = AckCap
    /\ pendingAck = Len(inbox) + Len(shardAcks)
    /\ writeSlots >= 0 /\ writeSlots <= WriteCap
    /\ writeSlots + activeWrites + pendingTermWritten = WriteCap

DisconnectNeverOvertakesAck ==
    (\E i \in 1..Len(coordIn) : coordIn[i] = 4) => ({1, 3} \subseteq routedAcks)

TerminalConsistent ==
    Terminal =>
        /\ coordIn = << 2, 4 >>
        /\ routedAcks = {1, 3}
        /\ availAck = AckCap
        /\ writeSlots = WriteCap

NeverReadNonAckAtZeroCredit == ~readNonAckZeroCredit

=============================================================================
