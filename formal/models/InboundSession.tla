---- MODULE InboundSession ----
EXTENDS Naturals, FiniteSets, Sequences

(***************************************************************************
Inbound Receive Maximum ownership across resumed sessions, with manual
acknowledgement (#498, #499).

A broker with a durable session sends QoS 1 and QoS 2 PUBLISHes within the
client's Receive Maximum [MQTT-3.3.4-9]: it counts a PUBLISH until it
receives its PUBACK or PUBCOMP, and its quota restarts with each Network
Connection [MQTT-4.9.0-1]. On reconnect it resends unacknowledged PUBLISHes
and PUBRELs by phase [MQTT-4.4.0-1]. Acknowledgements are lost with the
connection that carried them.

The client is InboundSession with manual_ack=True:
- rows: WAIT_PUBACK ("q1"), WAIT_PUBREL ("rel", with `user_acked`),
  WAIT_USER_ACK ("user");
- Receive Maximum owners: `_current_persisted_mids` (a PUBLISH observed on
  this connection) and the PUBCOMPs still in the engine batch that owned a
  slot (`_pending_pubcomp_slots`); the occupancy is their size;
- manual QoS 1 PUBACKs leave in PUBLISH arrival order (`_manual_qos1_order`,
  `_pending_manual_qos1_acks`) [MQTT-4.6.0-2];
- a QoS 2 exchange completes when both its PUBREL and the application
  acknowledgement arrived, in either order (`user_acked` before PUBREL,
  WAIT_USER_ACK after);
- take_effects() hands the batch to the wire and frees the PUBCOMP slots.

Delivery marks and automatic acknowledgement are covered by
InboundDeliveryCommit and InboundAckHandoff; every message counts as
delivered here.

Variant = "rc15": replay_session() pre-charges the replacement connection
with every durable row (`_inflight = persisted`).
Variant = "rc16": a row owns a slot only once its PUBLISH is observed on the
current connection, but the application's acknowledgement of a replayed
QoS 1 row sends its PUBACK at once, possibly before the broker's resend.
Variant = "fixed": a QoS 1 PUBACK leaves only for a PUBLISH observed on the
current connection (_drain_manual_qos1_acks stops at a row that is not yet).

Invariants:
- NoFalseRefusal: the client never refuses (DISCONNECT 0x93) a PUBLISH the
  broker sent within Receive Maximum.
- OwnersAreOutstanding: every slot owner is a PUBLISH the broker still counts
  on this connection, so occupancy never exceeds the broker's own count.
- NoSwallowedMessage: a new PUBLISH never lands on a stored row the broker
  considers finished (the row would answer it with its old payload).
- Q1OrderLive: the PUBACK order holds exactly the unacknowledged QoS 1 rows.
***************************************************************************)

CONSTANTS Variant, Mids, ReceiveMaximum
ASSUME Variant \in {"rc15", "rc16", "fixed"}

Kinds == {"puback", "pubrec", "pubcomp"}
Ack(k, m) == [k |-> k, m |-> m]

VARIABLES
  phase, userAcked, current, pubcompSlots, q1Order, q1Ready, batch,
  wire, bph, bout, relOwed, connected, refused, swallowed

vars == <<phase, userAcked, current, pubcompSlots, q1Order, q1Ready, batch,
          wire, bph, bout, relOwed, connected, refused, swallowed>>

clientVars == <<phase, userAcked, current, pubcompSlots, q1Order, q1Ready, batch>>

TypeOK ==
  /\ phase \in [Mids -> {"none", "q1", "rel", "user"}]
  /\ userAcked \in [Mids -> BOOLEAN]
  /\ current \subseteq Mids
  /\ pubcompSlots \subseteq Mids
  /\ q1Order \in Seq(Mids)
  /\ q1Ready \subseteq Mids
  /\ bph \in [Mids -> {"idle", "pub1", "pub2", "rel"}]
  /\ bout \subseteq Mids
  /\ relOwed \subseteq Mids
  /\ connected \in BOOLEAN
  /\ refused \in BOOLEAN
  /\ swallowed \in BOOLEAN

Init ==
  /\ phase = [m \in Mids |-> "none"]
  /\ userAcked = [m \in Mids |-> FALSE]
  /\ current = {}
  /\ pubcompSlots = {}
  /\ q1Order = <<>>
  /\ q1Ready = {}
  /\ batch = <<>>
  /\ wire = <<>>
  /\ bph = [m \in Mids |-> "idle"]
  /\ bout = {}
  /\ relOwed = {}
  /\ connected = TRUE
  /\ refused = FALSE
  /\ swallowed = FALSE

Used == Cardinality(current) + Cardinality(pubcompSlots)

\* The client refuses the PUBLISH and the connection ends (0x93).
Refuse ==
  /\ refused' = TRUE
  /\ connected' = FALSE
  /\ batch' = <<>>
  /\ wire' = <<>>
  /\ q1Ready' = {}
  /\ pubcompSlots' = {}
  /\ relOwed' = {}
  /\ UNCHANGED <<phase, userAcked, current, q1Order>>

\* Client handling of PUBLISH(m, qos): _on_qos1_manual / _on_qos2.
ClientPublish(m, qos) ==
  IF m \notin current /\ Used >= ReceiveMaximum
  THEN Refuse
  ELSE
    /\ current' = current \cup {m}
    /\ IF phase[m] = "none"
       THEN /\ phase' = [phase EXCEPT ![m] = IF qos = 1 THEN "q1" ELSE "rel"]
            /\ q1Order' = IF qos = 1 THEN Append(q1Order, m) ELSE q1Order
       ELSE UNCHANGED <<phase, q1Order>>
    /\ batch' = IF qos = 2 THEN Append(batch, Ack("pubrec", m)) ELSE batch
    /\ UNCHANGED <<userAcked, pubcompSlots, q1Ready, wire, relOwed, connected, refused>>

\* The broker sends a new PUBLISH within its quota.
BrokerNew(m, qos) ==
  /\ connected
  /\ bph[m] = "idle"
  /\ Cardinality(bout) < ReceiveMaximum
  /\ bph' = [bph EXCEPT ![m] = IF qos = 1 THEN "pub1" ELSE "pub2"]
  /\ bout' = bout \cup {m}
  \* A stored row under an identifier the broker reuses answers it as a
  \* duplicate: the new message is lost.
  /\ swallowed' = (swallowed \/ phase[m] # "none")
  /\ ClientPublish(m, qos)

\* After reconnect the broker resends an unacknowledged PUBLISH.
BrokerResend(m) ==
  /\ connected
  /\ bph[m] \in {"pub1", "pub2"}
  /\ m \notin bout
  /\ Cardinality(bout) < ReceiveMaximum
  /\ bout' = bout \cup {m}
  /\ UNCHANGED <<bph, swallowed>>
  /\ ClientPublish(m, IF bph[m] = "pub1" THEN 1 ELSE 2)

Complete2(m) ==
  /\ phase' = [phase EXCEPT ![m] = "none"]
  /\ userAcked' = [userAcked EXCEPT ![m] = FALSE]
  /\ current' = current \ {m}
  /\ pubcompSlots' = IF m \in current THEN pubcompSlots \cup {m} ELSE pubcompSlots
  /\ batch' = Append(batch, Ack("pubcomp", m))

\* The broker sends PUBREL; the client answers (on_pubrel, manual_ack).
BrokerPubrel(m) ==
  /\ connected
  /\ m \in relOwed
  /\ relOwed' = relOwed \ {m}
  /\ CASE phase[m] = "rel" /\ userAcked[m] ->
            /\ Complete2(m)
            /\ UNCHANGED <<q1Order, q1Ready>>
       [] phase[m] = "rel" ->
            /\ phase' = [phase EXCEPT ![m] = "user"]
            /\ UNCHANGED <<userAcked, current, pubcompSlots, q1Order, q1Ready, batch>>
       [] phase[m] = "none" ->
            /\ batch' = Append(batch, Ack("pubcomp", m))
            /\ UNCHANGED <<phase, userAcked, current, pubcompSlots, q1Order, q1Ready>>
       [] OTHER -> UNCHANGED clientVars
  /\ UNCHANGED <<wire, bph, bout, connected, refused, swallowed>>

\* The application acknowledges a delivered message (ack()).
AppAck(m) ==
  /\ connected
  /\ CASE phase[m] = "q1" /\ m \notin q1Ready ->
            /\ q1Ready' = q1Ready \cup {m}
            /\ UNCHANGED <<phase, userAcked, current, pubcompSlots, q1Order, batch>>
       [] phase[m] = "rel" /\ ~userAcked[m] ->
            /\ userAcked' = [userAcked EXCEPT ![m] = TRUE]
            /\ UNCHANGED <<phase, current, pubcompSlots, q1Order, q1Ready, batch>>
       [] phase[m] = "user" ->
            /\ Complete2(m)
            /\ UNCHANGED <<q1Order, q1Ready>>
       [] OTHER -> FALSE
  /\ UNCHANGED <<wire, bph, bout, relOwed, connected, refused, swallowed>>

\* _drain_manual_qos1_acks: the ready prefix leaves in arrival order.
DrainQ1 ==
  /\ connected
  /\ q1Order # <<>>
  /\ Head(q1Order) \in q1Ready
  /\ Variant = "fixed" => Head(q1Order) \in current
  /\ LET m == Head(q1Order) IN
       /\ phase' = [phase EXCEPT ![m] = "none"]
       /\ current' = current \ {m}
       /\ q1Order' = Tail(q1Order)
       /\ q1Ready' = q1Ready \ {m}
       /\ batch' = Append(batch, Ack("puback", m))
  /\ UNCHANGED <<userAcked, pubcompSlots, wire, bph, bout, relOwed, connected, refused,
                 swallowed>>

\* take_effects(): the batch reaches the wire; PUBCOMP slots are freed.
Handoff ==
  /\ connected
  /\ batch # <<>>
  /\ wire' = wire \o batch
  /\ batch' = <<>>
  /\ pubcompSlots' = {}
  /\ UNCHANGED <<phase, userAcked, current, q1Order, q1Ready, bph, bout, relOwed,
                 connected, refused, swallowed>>

\* The broker receives the next acknowledgement, in wire order.
BrokerReceive ==
  /\ connected
  /\ wire # <<>>
  /\ LET a == Head(wire) IN
       CASE a.k = "puback" /\ bph[a.m] = "pub1" ->
              /\ bph' = [bph EXCEPT ![a.m] = "idle"]
              /\ bout' = bout \ {a.m}
              /\ UNCHANGED relOwed
         [] a.k = "pubrec" /\ bph[a.m] = "pub2" ->
              /\ bph' = [bph EXCEPT ![a.m] = "rel"]
              /\ relOwed' = relOwed \cup {a.m}
              /\ UNCHANGED bout
         [] a.k = "pubcomp" /\ bph[a.m] = "rel" ->
              /\ bph' = [bph EXCEPT ![a.m] = "idle"]
              /\ bout' = bout \ {a.m}
              /\ UNCHANGED relOwed
         [] OTHER -> UNCHANGED <<bph, bout, relOwed>>
  /\ wire' = Tail(wire)
  /\ UNCHANGED <<clientVars, connected, refused, swallowed>>

\* The connection is lost, with every acknowledgement it still carried.
Disconnect ==
  /\ connected
  /\ connected' = FALSE
  /\ batch' = <<>>
  /\ wire' = <<>>
  /\ q1Ready' = {}
  /\ pubcompSlots' = {}
  /\ relOwed' = {}
  /\ UNCHANGED <<phase, userAcked, current, q1Order, bph, bout, refused, swallowed>>

\* Session resumed (Session Present = 1) on a new Network Connection.
Reconnect ==
  /\ ~connected
  /\ connected' = TRUE
  /\ bout' = {}
  /\ relOwed' = {m \in Mids : bph[m] = "rel"}
  /\ current' = IF Variant = "rc15" THEN {m \in Mids : phase[m] # "none"} ELSE {}
  /\ UNCHANGED <<phase, userAcked, pubcompSlots, q1Order, q1Ready, batch, wire, bph, refused,
                 swallowed>>

Next ==
  \/ \E m \in Mids : \E q \in {1, 2} : BrokerNew(m, q)
  \/ \E m \in Mids : BrokerResend(m) \/ BrokerPubrel(m) \/ AppAck(m)
  \/ DrainQ1 \/ Handoff \/ BrokerReceive \/ Disconnect \/ Reconnect

Spec == Init /\ [][Next]_vars

NoFalseRefusal == ~refused

OwnersAreOutstanding == connected => (current \cup pubcompSlots) \subseteq bout

NoSwallowedMessage == ~swallowed

Q1OrderLive ==
  /\ {q1Order[i] : i \in 1..Len(q1Order)} = {m \in Mids : phase[m] = "q1"}
  /\ Len(q1Order) = Cardinality({m \in Mids : phase[m] = "q1"})
  /\ q1Ready \subseteq {m \in Mids : phase[m] = "q1"}

=============================================================================
