---- MODULE ResumedSessionReplay ----

(***************************************************************************
A resumed session whose mandatory replay the new CONNACK forbids (#539).

One QoS 1/2 PUBLISH was sent and the connection was lost before its
acknowledgement: the broker session still holds the exchange under its
packet id. The replacement CONNACK has Session Present = 1, so the client must
resend it with that packet id [MQTT-4.4.0-1]. Forbidden = TRUE when the new
Maximum QoS, Retain Available or Maximum Packet Size forbids that PUBLISH
[MQTT-3.2.2-11] (OutboundSession.validate_against_negotiated raises).

Variant = "rc15": OutboundSession._replay_message deletes the record, releases
its packet id, fails the receipt and keeps the resumed connection.
Variant = "fixed": check_session_replayable raises SessionReplayError before
the engine becomes CONNECTED; record and packet id are kept.
A later publication takes the lowest free packet id, i.e. this one once freed.
***************************************************************************)

CONSTANTS Variant, Forbidden
ASSUME Variant \in {"rc15", "fixed"}
ASSUME Forbidden \in BOOLEAN

VARIABLES conn, record, midOwned, replayed, brokerHolds, newUsesMid

vars == <<conn, record, midOwned, replayed, brokerHolds, newUsesMid>>

TypeOK ==
  /\ conn \in {"lost", "resumed", "closed"}
  /\ record \in {"wait", "none"}
  /\ midOwned \in BOOLEAN
  /\ replayed \in BOOLEAN
  /\ brokerHolds \in BOOLEAN
  /\ newUsesMid \in BOOLEAN

Init ==
  /\ conn = "lost"
  /\ record = "wait"
  /\ midOwned = TRUE
  /\ replayed = FALSE
  /\ brokerHolds = TRUE
  /\ newUsesMid = FALSE

ResumedConnack ==
  /\ conn = "lost"
  /\ IF ~Forbidden
     THEN /\ conn' = "resumed"
          /\ replayed' = TRUE
          /\ UNCHANGED <<record, midOwned>>
     ELSE IF Variant = "rc15"
     THEN /\ conn' = "resumed"               \* discard_record + release
          /\ record' = "none"
          /\ midOwned' = FALSE
          /\ UNCHANGED replayed
     ELSE /\ conn' = "closed"                \* SessionReplayError
          /\ UNCHANGED <<record, midOwned, replayed>>
  /\ UNCHANGED <<brokerHolds, newUsesMid>>

\* The broker completes the resent exchange; only then is the id free.
BrokerCompletes ==
  /\ conn = "resumed"
  /\ replayed
  /\ record = "wait"
  /\ record' = "none"
  /\ midOwned' = FALSE
  /\ brokerHolds' = FALSE
  /\ UNCHANGED <<conn, replayed, newUsesMid>>

NewPublication ==
  /\ conn = "resumed"
  /\ ~midOwned
  /\ ~newUsesMid
  /\ newUsesMid' = TRUE
  /\ midOwned' = TRUE
  /\ UNCHANGED <<conn, record, replayed, brokerHolds>>

Terminal == conn = "closed" \/ newUsesMid

Quiescent == Terminal /\ UNCHANGED vars

Next == ResumedConnack \/ BrokerCompletes \/ NewPublication \/ Quiescent

Spec == Init /\ [][Next]_vars

\* A packet id the broker session still holds is never given to new work.
NoLiveMidReuse == newUsesMid => ~brokerHolds

\* A resumed session never continues without the exchange it must resend.
ResumedKeepsExchange == (conn = "resumed" /\ brokerHolds) => record = "wait"

=============================================================================
