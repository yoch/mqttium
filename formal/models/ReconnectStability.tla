---- MODULE ReconnectStability ----

(***************************************************************************
The reconnect supervisor after a successful attempt (#542).

The replacement connection may drop at any time; the stability timer fires
after stable_after. When the connection drops first, the next retry must be
runnable without waiting for the timer (the reader does not start a second
supervisor while this one lives).

Variant = "rc15": AsyncClient._reconnect_loop sleeps for stable_after.
Variant = "fixed": it waits for the replacement reader or the timer.
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"rc15", "fixed"}

VARIABLES conn, timer, supervisor

vars == <<conn, timer, supervisor>>

TypeOK ==
  /\ conn \in {"up", "lost"}
  /\ timer \in {"running", "fired"}
  /\ supervisor \in {"waiting", "retrying", "reset"}

Init ==
  /\ conn = "up"
  /\ timer = "running"
  /\ supervisor = "waiting"

Loss ==
  /\ conn = "up"
  /\ supervisor = "waiting"
  /\ conn' = "lost"
  /\ UNCHANGED <<timer, supervisor>>

TimerFires ==
  /\ timer = "running"
  /\ timer' = "fired"
  /\ UNCHANGED <<conn, supervisor>>

\* What the supervisor waits on: rc15 only the timer, fixed either event.
CanWake == timer = "fired" \/ (Variant = "fixed" /\ conn = "lost")

Wake ==
  /\ supervisor = "waiting"
  /\ CanWake
  /\ supervisor' = IF conn = "lost" THEN "retrying" ELSE "reset"
  /\ UNCHANGED <<conn, timer>>

Terminal == supervisor # "waiting"

Quiescent == Terminal /\ UNCHANGED vars

Next == Loss \/ TimerFires \/ Wake \/ Quiescent

Spec == Init /\ [][Next]_vars

\* The supervisor runs whenever it can. Nothing is assumed of the stability
\* timer: stable_after may be arbitrarily long, so a retry that needs the
\* timer to fire first is not guaranteed to happen.
FairSpec == Spec /\ WF_vars(Wake)

\* A lost replacement connection is always retried.
LossIsRetried == (conn = "lost") ~> (supervisor = "retrying")

\* A lost replacement never leaves the supervisor asleep behind the timer.
NoSleepThroughLoss == (conn = "lost" /\ supervisor = "waiting") => CanWake

=============================================================================
