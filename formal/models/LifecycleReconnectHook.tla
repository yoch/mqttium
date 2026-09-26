---- MODULE LifecycleReconnectHook ----
EXTENDS Naturals

(***************************************************************************
Automatic reconnect while an on_disconnect hook runs (#508).

Refines api/_lifecycle.py (LifecycleHooks) and the reconnect loop:
- reader teardown calls retiring(token) (pending cleared, readiness closed),
  then disconnected(error, token) (pending on_disconnect, readiness closed)
  and starts the reconnect task, which awaits wait_reconnect();
- the worker (_run) takes the pending notification, drops it if its token is
  stale, and runs it in a hook task (_invoke), which checks the token again;
- the reconnect task calls begin_operation(preserve_hook=True): the token
  advances, pending is cleared and readiness reopens; on CONNACK,
  connected(token) reopens readiness and queues on_connect behind the
  running hook, since one worker runs hooks one at a time.

The on_disconnect hook here awaits the receipt of a publication left
unacknowledged by the lost connection: only the replacement connection
settles it (session replay), or reconnect exhaustion fails it.

Variant = "rc15": readiness reopens only after the hook returns (the end of
  _run's iteration), so the hook and the reconnect wait for each other.
Variant = "fixed": _invoke reopens readiness once the disconnect hook owns
  the lifecycle (after its token check), before it runs user code.

TLC reports the rc15 wait cycle as a deadlock; the fixed configuration has
no deadlock and keeps on_connect behind on_disconnect.
***************************************************************************)

CONSTANT Variant
ASSUME Variant \in {"rc15", "fixed"}

VARIABLES
  token,        \* LifecycleHooks.token
  ready,        \* LifecycleHooks._reconnect_ready
  pending,      \* kind of LifecycleHooks.pending: "none", "disc", "conn"
  pendingToken,
  hook,         \* hook task: "none", "scheduled", "running", "waiting", "done"
  hookKind,     \* "disc" or "conn" while a hook task exists
  hookToken,
  phase,        \* connection: "connected", "retiring", "lost"
  reconnect,    \* "none", "waiting", "running", "connected", "terminal"
  receipt,      \* "none", "pending", "settled", "failed"
  discRan,      \* the on_disconnect callback started
  discReturned, \* the on_disconnect callback returned
  connRan       \* the on_connect callback started

vars == <<token, ready, pending, pendingToken, hook, hookKind, hookToken,
          phase, reconnect, receipt, discRan, discReturned, connRan>>

Init ==
  /\ token = 0
  /\ ready = TRUE
  /\ pending = "none"
  /\ pendingToken = 0
  /\ hook = "none"
  /\ hookKind = "none"
  /\ hookToken = 0
  /\ phase = "connected"
  /\ reconnect = "none"
  /\ receipt = "pending"
  /\ discRan = FALSE
  /\ discReturned = FALSE
  /\ connRan = FALSE

\* _read_loop teardown: retiring(token) before cleanup can suspend.
Retiring ==
  /\ phase = "connected"
  /\ phase' = "retiring"
  /\ pending' = "none"
  /\ ready' = FALSE
  /\ UNCHANGED <<token, pendingToken, hook, hookKind, hookToken, reconnect,
                 receipt, discRan, discReturned, connRan>>

\* After cleanup: disconnected(error, token), then the reconnect task.
Disconnected ==
  /\ phase = "retiring"
  /\ phase' = "lost"
  /\ ready' = FALSE
  /\ pending' = "disc"
  /\ pendingToken' = token
  /\ reconnect' = "waiting"
  /\ UNCHANGED <<token, hook, hookKind, hookToken, receipt, discRan, discReturned, connRan>>

\* _run: take the pending notification; a stale one is dropped.
WorkerTake ==
  /\ pending # "none"
  /\ hook = "none"
  /\ pending' = "none"
  /\ IF pendingToken = token
     THEN /\ hook' = "scheduled"
          /\ hookKind' = pending
          /\ hookToken' = pendingToken
     ELSE UNCHANGED <<hook, hookKind, hookToken>>
  /\ UNCHANGED <<token, ready, pendingToken, phase, reconnect, receipt,
                 discRan, discReturned, connRan>>

\* _invoke: token check, then (fixed) reopen readiness for a disconnect hook.
Invoke ==
  /\ hook = "scheduled"
  /\ IF hookToken # token
     THEN /\ hook' = "done"
          /\ UNCHANGED <<ready, discRan, connRan>>
     ELSE /\ hook' = "running"
          /\ ready' = IF Variant = "fixed" /\ hookKind = "disc" THEN TRUE ELSE ready
          /\ discRan' = (discRan \/ hookKind = "disc")
          /\ connRan' = (connRan \/ hookKind = "conn")
  /\ UNCHANGED <<token, pending, pendingToken, hookKind, hookToken, phase,
                 reconnect, receipt, discReturned>>

\* The disconnect hook awaits the receipt of the lost connection.
HookAwaitsReceipt ==
  /\ hook = "running"
  /\ hookKind = "disc"
  /\ hook' = "waiting"
  /\ UNCHANGED <<token, ready, pending, pendingToken, hookKind, hookToken,
                 phase, reconnect, receipt, discRan, discReturned, connRan>>

HookReturns ==
  /\ \/ hook = "running" /\ hookKind = "conn"
     \/ hook = "waiting" /\ receipt \in {"settled", "failed"}
  /\ hook' = "done"
  /\ discReturned' = (discReturned \/ hookKind = "disc")
  /\ UNCHANGED <<token, ready, pending, pendingToken, hookKind, hookToken,
                 phase, reconnect, receipt, discRan, connRan>>

\* The end of one _run iteration.
WorkerDone ==
  /\ hook = "done"
  /\ hook' = "none"
  /\ hookKind' = "none"
  /\ ready' = IF hookKind = "disc" /\ hookToken = token /\ pending = "none"
              THEN TRUE ELSE ready
  /\ UNCHANGED <<token, pending, pendingToken, hookToken, phase, reconnect,
                 receipt, discRan, discReturned, connRan>>

\* _reconnect_loop: wait_reconnect(), then begin_operation(preserve_hook=True).
ReconnectStart ==
  /\ reconnect = "waiting"
  /\ ready
  /\ reconnect' = "running"
  /\ token' = token + 1
  /\ pending' = "none"
  /\ ready' = TRUE
  /\ UNCHANGED <<pendingToken, hook, hookKind, hookToken, phase, receipt,
                 discRan, discReturned, connRan>>

\* CONNACK on the replacement: the receipt settles, connected(token).
ReconnectSuccess ==
  /\ reconnect = "running"
  /\ reconnect' = "connected"
  /\ receipt' = IF receipt = "pending" THEN "settled" ELSE receipt
  /\ ready' = TRUE
  /\ pending' = "conn"
  /\ pendingToken' = token
  /\ UNCHANGED <<token, hook, hookKind, hookToken, phase, discRan, discReturned, connRan>>

ReconnectExhausted ==
  /\ reconnect = "running"
  /\ reconnect' = "terminal"
  /\ receipt' = IF receipt = "pending" THEN "failed" ELSE receipt
  /\ UNCHANGED <<token, ready, pending, pendingToken, hook, hookKind, hookToken,
                 phase, discRan, discReturned, connRan>>

Terminal ==
  /\ reconnect \in {"connected", "terminal"}
  /\ pending = "none"
  /\ hook = "none"

Quiescent == Terminal /\ UNCHANGED vars

Next ==
  \/ Retiring \/ Disconnected \/ WorkerTake \/ Invoke \/ HookAwaitsReceipt
  \/ HookReturns \/ WorkerDone \/ ReconnectStart \/ ReconnectSuccess
  \/ ReconnectExhausted \/ Quiescent

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ token \in 0..1
  /\ ready \in BOOLEAN
  /\ pending \in {"none", "disc", "conn"}
  /\ hook \in {"none", "scheduled", "running", "waiting", "done"}
  /\ reconnect \in {"none", "waiting", "running", "connected", "terminal"}
  /\ receipt \in {"pending", "settled", "failed"}

\* on_connect never starts before the disconnect hook it follows returned.
OnConnectSerializedBehindDisconnect == connRan => discReturned

\* A successful reconnect runs on_connect, and the disconnect hook ran.
HooksRun == Terminal /\ reconnect = "connected" => (discRan /\ connRan)

=============================================================================
