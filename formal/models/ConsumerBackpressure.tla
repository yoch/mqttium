---- MODULE ConsumerBackpressure ----

EXTENDS Naturals, Sequences

(***************************************************************************
A messages() consumer that waits on the reader, while the reader waits on it.

The reader admits each inbound PUBLISH into the bounded iterator queue before
it reads the next packet. With the queue full it parks
(the waiting admission of ApplicationDelivery) and reads nothing: an acknowledgement
that arrived behind the waiting message is never processed.

A consumer that takes one message, publishes a reply and awaits its receipt
before taking the next one frees no queue space while it waits. The reply's
PUBACK is in the stream behind messages the reader cannot admit.

Deadline = FALSE: iterator_admission_timeout=None, the released default. The
reader parks without bound.
Deadline = TRUE: the admission deadline ends the wait; the connection fails
with MessageDeliveryError and the consumer's receipt fails with it.
Awaits = FALSE: the consumer does not wait on the reader inside its loop
(publish_nowait with a done callback): no cycle exists either way.
***************************************************************************)

CONSTANTS Capacity, Backlog, Deadline, Awaits
ASSUME Capacity \in Nat \ {0}
ASSUME Backlog \in Nat
ASSUME Deadline \in BOOLEAN
ASSUME Awaits \in BOOLEAN

VARIABLES stream, queue, reader, consumer, conn

vars == <<stream, queue, reader, consumer, conn>>

TypeOK ==
  /\ stream \in Seq({"msg", "ack"})
  /\ queue \in 0..Capacity
  /\ reader \in {"reading", "parked", "stopped"}
  /\ consumer \in {"idle", "awaiting", "failed"}
  /\ conn \in {"up", "failed"}

Init ==
  /\ stream = [i \in 1..Backlog |-> "msg"]
  /\ queue = 0
  /\ reader = "reading"
  /\ consumer = "idle"
  /\ conn = "up"

\* The reader handles the packet at the head of the stream, in order.
ReadMessage ==
  /\ conn = "up" /\ reader = "reading"
  /\ stream # <<>> /\ Head(stream) = "msg"
  /\ IF queue < Capacity
       THEN /\ queue' = queue + 1
            /\ stream' = Tail(stream)
            /\ UNCHANGED reader
       ELSE /\ reader' = "parked"
            /\ UNCHANGED <<queue, stream>>
  /\ UNCHANGED <<consumer, conn>>

ReadAck ==
  /\ conn = "up" /\ reader = "reading"
  /\ stream # <<>> /\ Head(stream) = "ack"
  /\ stream' = Tail(stream)
  /\ consumer' = IF consumer = "awaiting" THEN "idle" ELSE consumer
  /\ UNCHANGED <<queue, reader, conn>>

SpaceFreed ==
  /\ conn = "up" /\ reader = "parked" /\ queue < Capacity
  /\ reader' = "reading"
  /\ UNCHANGED <<stream, queue, consumer, conn>>

\* The consumer takes one message and replies. The broker's acknowledgement
\* of the reply arrives behind everything already in the stream.
Take ==
  /\ consumer = "idle" /\ queue > 0
  /\ queue' = queue - 1
  /\ IF Awaits /\ conn = "up"
       THEN /\ consumer' = "awaiting"
            /\ stream' = Append(stream, "ack")
       ELSE UNCHANGED <<consumer, stream>>
  /\ UNCHANGED <<reader, conn>>

\* iterator_admission_timeout expires while the reader is parked.
AdmissionDeadline ==
  /\ Deadline
  /\ conn = "up" /\ reader = "parked"
  /\ conn' = "failed"
  /\ reader' = "stopped"
  /\ consumer' = IF consumer = "awaiting" THEN "failed" ELSE consumer
  /\ UNCHANGED <<stream, queue>>

Terminal ==
  \/ conn = "up" /\ stream = <<>> /\ queue = 0 /\ consumer = "idle"
  \/ conn = "failed" /\ (consumer = "failed" \/ queue = 0)

Quiescent == Terminal /\ UNCHANGED vars

Next == ReadMessage \/ ReadAck \/ SpaceFreed \/ Take \/ AdmissionDeadline \/ Quiescent

Spec == Init /\ [][Next]_vars

\* A consumer never waits on a reader that waits on it without a bound.
WaitCycleIsBounded ==
  (conn = "up" /\ reader = "parked" /\ queue = Capacity /\ consumer = "awaiting") => Deadline

=============================================================================
