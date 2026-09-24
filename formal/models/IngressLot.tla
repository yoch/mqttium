---- MODULE IngressLot ----
EXTENDS Naturals, Sequences

(***************************************************************************
Peer errors inside an ingress lot, for every way of cutting the byte stream.

The peer sends, in order: a QoS 1 PUBLISH (auto-acknowledged), the PUBACK of
our outstanding publication, a fatal packet, and another QoS 1 PUBLISH. The
reader decodes lots of any length (reads coalesce arbitrarily), processes
them inside one store batch, applies protocol effects, then delivers the
lot's messages.

Variant = "rc15":
  - a malformed packet raises out of the batch: the lot's store writes roll
    back and pending receipts fail before the lot's effects are applied;
  - a protocol error is only an effect: later packets of the lot are still
    processed, and raising it at effect application drops the lot's
    deliveries.
Variant = "fixed":
  - the lot ends at the fatal packet; the prefix commits, its effects apply
    and its messages are delivered, then the error retires the connection.

Invariants (issues):
  ObservedOutcomeKept   #511  a PUBACK decoded before the fatal packet settles
                              its receipt successfully
  NothingAfterFatal     #513  no packet after the fatal one is acknowledged
  AckedIsDelivered      #513  an acknowledged message reaches the application
***************************************************************************)

CONSTANTS Variant, BadKind
ASSUME Variant \in {"rc15", "fixed"}
ASSUME BadKind \in {"malformed", "protocol"}

Stream == <<"pub1", "ack", "bad", "pub2">>
N == Len(Stream)
BadIndex == 3

VARIABLES
  next,        \* index of the next undecoded packet
  receipt,     \* "pending" | "ok" | "failed"
  acked,       \* PUBLISH packets whose PUBACK left
  delivered,   \* PUBLISH packets the application received
  done         \* the connection has been retired

vars == <<next, receipt, acked, delivered, done>>

TypeOK ==
  /\ next \in 1..(N + 1)
  /\ receipt \in {"pending", "ok", "failed"}
  /\ acked \subseteq {"pub1", "pub2"}
  /\ delivered \subseteq {"pub1", "pub2"}
  /\ done \in BOOLEAN

Init ==
  /\ next = 1
  /\ receipt = "pending"
  /\ acked = {}
  /\ delivered = {}
  /\ done = FALSE

Pubs(S) == {p \in S : p \in {"pub1", "pub2"}}
Items(lo, hi) == {Stream[i] : i \in lo..hi}

\* Process one lot ending at packet `last` (any cut of the stream).
Lot(last) ==
  /\ ~done
  /\ last \in next..N
  /\ LET containsBad == next <= BadIndex /\ BadIndex <= last
         prefix == IF containsBad THEN Items(next, BadIndex - 1) ELSE Items(next, last)
         rest == IF containsBad /\ last > BadIndex THEN Items(BadIndex + 1, last) ELSE {}
     IN
     IF ~containsBad
     THEN /\ acked' = acked \cup Pubs(prefix)
          /\ delivered' = delivered \cup Pubs(prefix)
          /\ receipt' = IF "ack" \in prefix THEN "ok" ELSE receipt
          /\ next' = last + 1
          /\ UNCHANGED done
     ELSE IF Variant = "fixed"
     THEN /\ acked' = acked \cup Pubs(prefix)
          /\ delivered' = delivered \cup Pubs(prefix)
          /\ receipt' = IF "ack" \in prefix \/ receipt = "ok" THEN "ok" ELSE "failed"
          /\ next' = N + 1
          /\ done' = TRUE
     ELSE IF BadKind = "malformed"
     THEN \* rollback; receipts fail before the lot's completion applies
          /\ acked' = acked
          /\ delivered' = delivered
          /\ receipt' = IF receipt = "ok" THEN "ok" ELSE "failed"
          /\ next' = N + 1
          /\ done' = TRUE
     ELSE \* protocol error: later packets processed, deliveries dropped
          /\ acked' = acked \cup Pubs(prefix) \cup Pubs(rest)
          /\ delivered' = delivered
          /\ receipt' = IF "ack" \in prefix \/ receipt = "ok" THEN "ok" ELSE "failed"
          /\ next' = N + 1
          /\ done' = TRUE

Quiescent == done /\ UNCHANGED vars

Next == (\E last \in 1..N: Lot(last)) \/ Quiescent

Spec == Init /\ [][Next]_vars

\* The PUBACK precedes the fatal packet in the stream, so it was observed.
ObservedOutcomeKept == done => receipt = "ok"

NothingAfterFatal == "pub2" \notin acked

AckedIsDelivered == done => acked \subseteq delivered

=============================================================================
