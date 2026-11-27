---------------------------- MODULE MQos0Window ----------------------------
(***************************************************************************)
(* The window of volatile QoS 0 frames the owner lets one connection have  *)
(* in flight (record 0063), as `Owner::plan_qos0_continue` serves it in    *)
(* crates/mqttiumd-core/src/owner/pubsub.rs:                               *)
(*                                                                         *)
(*   a lane with a waiting delivery and room    Serve: its oldest leaves   *)
(*     (VolatileQueue::front_ready,                                        *)
(*      VolatileQueue::serve_ready)                                        *)
(*   else the oldest pending delivery           Arrive                     *)
(*     its lane has deliveries waiting, or        park behind them, or     *)
(*     its window is full                         drop it past the budget  *)
(*     otherwise                                  emit it                  *)
(*   the write of a frame is reported           ApplyWritten               *)
(*     (VolatileQueue::remove_inflight)                                    *)
(*                                                                         *)
(* Window stands for QOS0_WINDOW, ParkSlots for QOS0_PARKED, OutSlots for  *)
(* OUTPUT_SLOTS of net.rs; identifier 0 in `outgoing` is a control frame.  *)
(* `sent` is the order of the frames on the wire; no guard reads it.       *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets
CONSTANT Mutation
Peers == {"slow","fast"}
Last(p) == IF p = "slow" THEN 4 ELSE 2
Window == 1
ParkSlots == 2
OutSlots == 2
VARIABLES next, parked, outgoing, receipts, sent, dropped, controlAdmitted
vars == <<next, parked, outgoing, receipts, sent, dropped, controlAdmitted>>
Zeros == [p \in Peers |-> 0]
Empties == [p \in Peers |-> <<>>]
Init == /\ next = [p \in Peers |-> 1] /\ parked = Empties /\ outgoing = Empties
        /\ receipts = Zeros /\ sent = Empties /\ dropped = [p \in Peers |-> {}]
        /\ controlAdmitted = FALSE
Data(q) == SelectSeq(q, LAMBDA id : id # 0)
InFlight(p) == Len(Data(outgoing[p])) + receipts[p]
Room(p) == InFlight(p) < Window
\* PassParked takes a pending delivery of p without regard to what waits
\* in the lane of p: neither the lane is served first nor the delivery
\* parked behind it.
Arrive(p) ==
    /\ \A q \in Peers : Len(parked[q]) = 0 \/ ~Room(q)
                        \/ (Mutation = "PassParked" /\ q = p)
    /\ Mutation # "WaitForSlow" \/ p # "fast" \/ Room("slow")
    /\ next[p] <= Last(p) /\ next' = [next EXCEPT ![p] = @ + 1]
    /\ IF (Room(p) \/ Mutation = "NoWindow") /\
          (Len(parked[p]) = 0 \/ Mutation = "PassParked") THEN
           IF Len(outgoing[p]) < OutSlots THEN
              /\ outgoing' = [outgoing EXCEPT ![p] = Append(@, next[p])]
              /\ UNCHANGED <<parked, dropped>>
           ELSE /\ dropped' = [dropped EXCEPT ![p] = @ \cup {next[p]}]
                /\ UNCHANGED <<outgoing, parked>>
       ELSE IF Len(parked[p]) < ParkSlots THEN
           /\ parked' = [parked EXCEPT ![p] = Append(@, next[p])]
           /\ UNCHANGED <<outgoing, dropped>>
       ELSE /\ dropped' = [dropped EXCEPT ![p] = @ \cup {next[p]}]
            /\ UNCHANGED <<outgoing, parked>>
    /\ UNCHANGED <<receipts, sent, controlAdmitted>>
Serve(p) ==
    /\ Len(parked[p]) > 0 /\ Room(p)
    /\ LET q == parked[p]
           i == IF Mutation = "NewestFirst" THEN Len(q) ELSE 1
       IN /\ outgoing' = [outgoing EXCEPT ![p] = Append(@, q[i])]
          /\ parked' = [parked EXCEPT ![p] = IF i = 1 THEN Tail(q) ELSE SubSeq(q, 1, i-1)]
    /\ UNCHANGED <<next, receipts, sent, dropped, controlAdmitted>>
Control == /\ Len(outgoing["slow"]) < OutSlots /\ ~controlAdmitted /\ controlAdmitted' = TRUE
           /\ outgoing' = [outgoing EXCEPT !["slow"] = Append(@, 0)]
           /\ UNCHANGED <<next, parked, receipts, sent, dropped>>
Write(p) ==
    /\ Len(outgoing[p]) > 0
    /\ LET id == Head(outgoing[p]) IN
       /\ sent' = IF id = 0 THEN sent ELSE [sent EXCEPT ![p] = Append(@, id)]
       /\ receipts' = IF id = 0 THEN receipts ELSE [receipts EXCEPT ![p] = @ + 1]
    /\ outgoing' = [outgoing EXCEPT ![p] = Tail(@)]
    /\ UNCHANGED <<next, parked, dropped, controlAdmitted>>
ApplyWritten(p) == /\ receipts[p] > 0 /\ receipts' = [receipts EXCEPT ![p] = @ - 1]
                   /\ UNCHANGED <<next, parked, outgoing, sent, dropped, controlAdmitted>>
Idle == UNCHANGED vars
Next == (\E p \in Peers : Arrive(p) \/ Serve(p) \/ Write(p) \/ ApplyWritten(p)) \/ Control \/ Idle
FairSpec == Init /\ [][Next]_vars /\ WF_vars(Arrive("fast")) /\ WF_vars(Serve("fast")) /\ WF_vars(Serve("slow"))
                 /\ WF_vars(Write("fast")) /\ WF_vars(ApplyWritten("fast"))
WithinWindow == \A p \in Peers : InFlight(p) <= Window /\ Len(parked[p]) <= ParkSlots /\ Len(outgoing[p]) <= OutSlots
ControlRoom == ~controlAdmitted => Len(outgoing["slow"]) < OutSlots
\* On the wire, the publications a connection is sent keep the order of
\* their arrival; a dropped one leaves a gap, never an inversion.
Ordered == \A p \in Peers : \A i \in 2..Len(sent[p]) : sent[p][i-1] < sent[p][i]
FastCompletes == <>(Len(sent["fast"]) = Last("fast"))
NoIndependentDrain == ~(Len(sent["fast"]) = 2 /\ sent["slow"] = <<>> /\
                        Len(parked["slow"]) = ParkSlots /\ dropped["slow"] # {})
============================================================================
