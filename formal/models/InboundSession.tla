---- MODULE InboundSession ----
EXTENDS Naturals, FiniteSets, Sequences

(***************************************************************************
Inbound MQTT QoS/session ownership abstraction used for the MQTTium study.

Four notions are deliberately independent:
  1. durable protocol/application phase (\`phase\`),
  2. Receive-Maximum ownership for the current Network Connection (\`slots\`),
  3. manual QoS1 acknowledgement order/intent (\`q1Order\`, \`q1Ready\`),
  4. connection liveness (\`connected\`).

Normative hooks:
- MQTT-4.9.0-1/-2: send quota is initialized per Network Connection and a
  QoS>0 PUBLISH consumes it when that PUBLISH is sent.
- MQTT-4.3.3-6: a QoS2 sender MUST NOT re-send PUBLISH after sending PUBREL.
- MQTT-4.3.3-10: duplicate QoS2 PUBLISH is acknowledged again only until the
  receiver gets the corresponding PUBREL.
- MQTT-4.6.0-2: PUBACKs follow PUBLISH receive order.

\`DrainQ1\` is modeled as a separate internal action. MQTTium drains the ready
prefix synchronously inside ack(); splitting it here is a conservative
interleaving over-approximation for safety checking.
***************************************************************************)

CONSTANTS Mids, ReceiveMaximum

None          == "None"
Q1WaitAck     == "Q1WaitAck"
Q2WaitRel     == "Q2WaitRel"
Q2WaitRelAck  == "Q2WaitRelAck"
Q2WaitUser    == "Q2WaitUser"
Phases == {None, Q1WaitAck, Q2WaitRel, Q2WaitRelAck, Q2WaitUser}

VARIABLES phase, slots, q1Order, q1Ready, connected
vars == <<phase, slots, q1Order, q1Ready, connected>>

OrderSet == {q1Order[i] : i \in 1..Len(q1Order)}
OrderUnique ==
  \A i, j \in 1..Len(q1Order): i # j => q1Order[i] # q1Order[j]

TypeOK ==
  /\ phase \in [Mids -> Phases]
  /\ slots \subseteq Mids
  /\ q1Order \in Seq(Mids)
  /\ q1Ready \subseteq Mids
  /\ connected \in BOOLEAN

OwnershipOK ==
  /\ \A m \in slots: phase[m] # None
  /\ \A m \in OrderSet: phase[m] = Q1WaitAck
  /\ q1Ready \subseteq OrderSet
  /\ OrderUnique

Init ==
  /\ phase = [m \in Mids |-> None]
  /\ slots = {}
  /\ q1Order = <<>>
  /\ q1Ready = {}
  /\ connected = TRUE

Disconnect ==
  /\ connected' = FALSE
  /\ UNCHANGED <<phase, slots, q1Order, q1Ready>>

Reconnect ==
  /\ phase' = phase
  /\ slots' = {}
  /\ q1Order' = q1Order
  /\ q1Ready' = {}
  /\ connected' = TRUE

Q1New(m) ==
  /\ connected
  /\ phase[m] = None
  /\ Cardinality(slots) < ReceiveMaximum
  /\ phase' = [phase EXCEPT ![m] = Q1WaitAck]
  /\ slots' = slots \cup {m}
  /\ q1Order' = Append(q1Order, m)
  /\ UNCHANGED <<q1Ready, connected>>

Q1NewOverQuota(m) ==
  /\ connected
  /\ phase[m] = None
  /\ Cardinality(slots) >= ReceiveMaximum
  /\ Disconnect

Q1Retransmit(m) ==
  /\ connected
  /\ phase[m] = Q1WaitAck
  /\ m \notin slots
  /\ Cardinality(slots) < ReceiveMaximum
  /\ slots' = slots \cup {m}
  /\ UNCHANGED <<phase, q1Order, q1Ready, connected>>

Q1RetransmitSameConnection(m) ==
  /\ connected
  /\ phase[m] = Q1WaitAck
  /\ m \in slots
  /\ UNCHANGED vars

Q1RetransmitOverQuota(m) ==
  /\ connected
  /\ phase[m] = Q1WaitAck
  /\ m \notin slots
  /\ Cardinality(slots) >= ReceiveMaximum
  /\ Disconnect

Q1Collision(m) ==
  /\ connected
  /\ phase[m] \in {Q2WaitRel, Q2WaitRelAck, Q2WaitUser}
  /\ Disconnect

Q2New(m) ==
  /\ connected
  /\ phase[m] = None
  /\ Cardinality(slots) < ReceiveMaximum
  /\ phase' = [phase EXCEPT ![m] = Q2WaitRel]
  /\ slots' = slots \cup {m}
  /\ UNCHANGED <<q1Order, q1Ready, connected>>

Q2NewOverQuota(m) ==
  /\ connected
  /\ phase[m] = None
  /\ Cardinality(slots) >= ReceiveMaximum
  /\ Disconnect

Q2DuplicateBeforePubrel(m) ==
  /\ connected
  /\ phase[m] \in {Q2WaitRel, Q2WaitRelAck}
  /\ m \in slots
  /\ UNCHANGED vars

Q2RetransmitAfterReconnect(m) ==
  /\ connected
  /\ phase[m] \in {Q2WaitRel, Q2WaitRelAck}
  /\ m \notin slots
  /\ Cardinality(slots) < ReceiveMaximum
  /\ slots' = slots \cup {m}
  /\ UNCHANGED <<phase, q1Order, q1Ready, connected>>

Q2RetransmitOverQuota(m) ==
  /\ connected
  /\ phase[m] \in {Q2WaitRel, Q2WaitRelAck}
  /\ m \notin slots
  /\ Cardinality(slots) >= ReceiveMaximum
  /\ Disconnect

Q2PublishAfterPubrel(m) ==
  /\ connected
  /\ phase[m] = Q2WaitUser
  /\ Disconnect

Q2Collision(m) ==
  /\ connected
  /\ phase[m] = Q1WaitAck
  /\ Disconnect

Pubrel(m) ==
  /\ connected
  /\ phase[m] = Q2WaitRel
  /\ phase' = [phase EXCEPT ![m] = Q2WaitUser]
  /\ UNCHANGED <<slots, q1Order, q1Ready, connected>>

PubrelAfterEarlyAck(m) ==
  /\ connected
  /\ phase[m] = Q2WaitRelAck
  /\ phase' = [phase EXCEPT ![m] = None]
  /\ slots' = slots \ {m}
  /\ UNCHANGED <<q1Order, q1Ready, connected>>

DuplicatePubrel(m) ==
  /\ connected
  /\ phase[m] = Q2WaitUser
  /\ UNCHANGED vars

OrphanPubrel(m) ==
  /\ connected
  /\ phase[m] = None
  /\ UNCHANGED vars

BadPubrelOnQ1(m) ==
  /\ connected
  /\ phase[m] = Q1WaitAck
  /\ Disconnect

AckQ1Mark(m) ==
  /\ connected
  /\ phase[m] = Q1WaitAck
  /\ q1Ready' = q1Ready \cup {m}
  /\ UNCHANGED <<phase, slots, q1Order, connected>>

DrainQ1 ==
  /\ connected
  /\ Len(q1Order) > 0
  /\ Head(q1Order) \in q1Ready
  /\ LET m == Head(q1Order) IN
       /\ phase' = [phase EXCEPT ![m] = None]
       /\ slots' = slots \ {m}
       /\ q1Order' = Tail(q1Order)
       /\ q1Ready' = q1Ready \ {m}
       /\ UNCHANGED connected

EarlyAckQ2(m) ==
  /\ connected
  /\ phase[m] = Q2WaitRel
  /\ phase' = [phase EXCEPT ![m] = Q2WaitRelAck]
  /\ UNCHANGED <<slots, q1Order, q1Ready, connected>>

RepeatEarlyAckQ2(m) ==
  /\ connected
  /\ phase[m] = Q2WaitRelAck
  /\ UNCHANGED vars

AckQ2(m) ==
  /\ connected
  /\ phase[m] = Q2WaitUser
  /\ phase' = [phase EXCEPT ![m] = None]
  /\ slots' = slots \ {m}
  /\ UNCHANGED <<q1Order, q1Ready, connected>>

Q1Publish(m) ==
  Q1New(m) \/ Q1NewOverQuota(m) \/ Q1Retransmit(m) \/
  Q1RetransmitSameConnection(m) \/ Q1RetransmitOverQuota(m) \/ Q1Collision(m)

Q2Publish(m) ==
  Q2New(m) \/ Q2NewOverQuota(m) \/ Q2DuplicateBeforePubrel(m) \/
  Q2RetransmitAfterReconnect(m) \/ Q2RetransmitOverQuota(m) \/
  Q2PublishAfterPubrel(m) \/ Q2Collision(m)

RecvPubrel(m) ==
  Pubrel(m) \/ PubrelAfterEarlyAck(m) \/ DuplicatePubrel(m) \/
  OrphanPubrel(m) \/ BadPubrelOnQ1(m)

AppAck(m) == AckQ1Mark(m) \/ EarlyAckQ2(m) \/ RepeatEarlyAckQ2(m) \/ AckQ2(m)

Next ==
  Reconnect \/ DrainQ1 \/
  \E m \in Mids: Q1Publish(m) \/ Q2Publish(m) \/ RecvPubrel(m) \/ AppAck(m)

Spec == Init /\ [][Next]_vars

ReceiveMaximumInv == Cardinality(slots) <= ReceiveMaximum
SlotOwnsLiveExchange == \A m \in slots: phase[m] # None
Q1OrderLive == \A m \in OrderSet: phase[m] = Q1WaitAck
Q1ReadyOrdered == q1Ready \subseteq OrderSet

=============================================================================
