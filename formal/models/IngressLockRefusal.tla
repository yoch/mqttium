---- MODULE IngressLockRefusal ----

EXTENDS Naturals

(***************************************************************************
An inbound packet that writes the store, while another connection may hold
the store's write lock.

The engine applies a packet in memory (slot and byte reservations, indexes)
and writes its row; the reader commits the lot. SQLite grants or refuses the
write lock at the first statement that needs it.

Variant = "v121": the lock is requested at the first write, after the engine
applied the packet. A refusal rolls the lot back while memory keeps what it
applied, so the client fail-stops for good.
Variant = "retry": the same order, with the refusal treated as an ordinary
connection loss. The client stays usable with memory ahead of the store.
Variant = "reserve": the reader reserves the lock before the engine sees the
packet (SqliteInflightStore.reserve_batch_write). A refusal drops a packet
that was never applied; the connection is retried and the broker resends it.
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"v121", "retry", "reserve"}

VARIABLES lock, phase, memory, rows, conn

vars == <<lock, phase, memory, rows, conn>>

TypeOK ==
  /\ lock \in {"free", "ours", "other"}
  /\ phase \in {"arrived", "reserved", "applied", "done"}
  /\ memory \in 0..1
  /\ rows \in 0..1
  /\ conn \in {"up", "lost", "failed"}

Init ==
  /\ lock = "free"
  /\ phase = "arrived"
  /\ memory = 0
  /\ rows = 0
  /\ conn = "up"

\* Another connection to the same file, breaking the one-client-per-file rule.
OtherTakes ==
  /\ lock = "free"
  /\ lock' = "other"
  /\ UNCHANGED <<phase, memory, rows, conn>>

OtherReleases ==
  /\ lock = "other"
  /\ lock' = "free"
  /\ UNCHANGED <<phase, memory, rows, conn>>

\* The reader asks for the lock before handing the packet to the engine.
Reserve ==
  /\ Variant = "reserve"
  /\ conn = "up" /\ phase = "arrived"
  /\ IF lock = "free"
       THEN /\ lock' = "ours"
            /\ phase' = "reserved"
            /\ UNCHANGED conn
       ELSE /\ conn' = "lost"
            /\ phase' = "done"
            /\ UNCHANGED lock
  /\ UNCHANGED <<memory, rows>>

\* The engine applies the packet in memory.
Apply ==
  /\ conn = "up"
  /\ phase = (IF Variant = "reserve" THEN "reserved" ELSE "arrived")
  /\ memory' = 1
  /\ phase' = "applied"
  /\ UNCHANGED <<lock, rows, conn>>

\* The row is written and the lot committed, or the lock is refused.
Write ==
  /\ conn = "up" /\ phase = "applied"
  /\ phase' = "done"
  /\ IF lock # "other"
       THEN /\ rows' = 1
            /\ lock' = "free"
            /\ UNCHANGED conn
       ELSE /\ conn' = (IF Variant = "v121" THEN "failed" ELSE "lost")
            /\ UNCHANGED <<rows, lock>>
  /\ UNCHANGED memory

Terminal == phase = "done"

Quiescent == Terminal /\ UNCHANGED vars

Next == OtherTakes \/ OtherReleases \/ Reserve \/ Apply \/ Write \/ Quiescent

Spec == Init /\ [][Next]_vars

\* Lock contention alone never ends the client for good.
ContentionIsNotTerminal == conn # "failed"

\* A client that goes on to reconnect holds nothing the store does not.
MemoryMatchesStore == (phase = "done" /\ conn # "failed") => memory = rows

=============================================================================
