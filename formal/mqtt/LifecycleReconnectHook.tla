---- MODULE LifecycleReconnectHook ----
EXTENDS Naturals

(***************************************************************************
LifecycleHooks / automatic reconnect ownership model for mqttium issue #508.

A disconnect notification is serialized by LifecycleHooks, but transport
recovery must not depend on completion of arbitrary user code when that code
may itself wait for MQTT work whose completion requires the replacement
connection.

AllowReconnectDuringHook=FALSE models rc15:
  _reconnect_ready stays clear until on_disconnect returns.

TRUE models the candidate:
  reconnect readiness opens only after the running disconnect notification has
  passed its token check. Automatic reconnect preserves that hook; a later
  on_connect notification remains pending until the disconnect hook finishes.
***************************************************************************)

CONSTANT AllowReconnectDuringHook
ASSUME AllowReconnectDuringHook \in BOOLEAN

Idle == "idle"
Running == "running"
WaitingReceipt == "waiting-receipt"
Done == "done"

ReconnectWaiting == "reconnect-waiting"
ReconnectRunning == "reconnect-running"
ReconnectConnected == "reconnect-connected"
ReconnectTerminal == "reconnect-terminal"

None == "none"
Pending == "pending"
Settled == "settled"
Failed == "failed"

VARIABLES
  hook,
  reconnectReady,
  reconnect,
  receipt,
  onConnect

vars == <<hook, reconnectReady, reconnect, receipt, onConnect>>

Init ==
  /\ hook = Idle
  /\ reconnectReady = TRUE
  /\ reconnect = Idle
  /\ receipt = None
  /\ onConnect = None

TypeOK ==
  /\ hook \in {Idle, Running, WaitingReceipt, Done}
  /\ reconnectReady \in BOOLEAN
  /\ reconnect \in {
       Idle, ReconnectWaiting, ReconnectRunning,
       ReconnectConnected, ReconnectTerminal
     }
  /\ receipt \in {None, Pending, Settled, Failed}
  /\ onConnect \in {None, Pending, Done}

Loss ==
  /\ hook = Idle
  /\ reconnect = Idle
  /\ hook' = Running
  /\ reconnectReady' = FALSE
  /\ reconnect' = ReconnectWaiting
  /\ UNCHANGED <<receipt, onConnect>>

HookWaitReceipt ==
  /\ hook = Running
  /\ hook' = WaitingReceipt
  /\ receipt' = Pending
  /\ reconnectReady' =
       IF AllowReconnectDuringHook THEN TRUE ELSE reconnectReady
  /\ UNCHANGED <<reconnect, onConnect>>

ReconnectStart ==
  /\ reconnect = ReconnectWaiting
  /\ reconnectReady
  /\ reconnect' = ReconnectRunning
  /\ UNCHANGED <<hook, reconnectReady, receipt, onConnect>>

ReconnectSuccess ==
  /\ reconnect = ReconnectRunning
  /\ reconnect' = ReconnectConnected
  /\ receipt' = Settled
  /\ onConnect' = Pending
  /\ UNCHANGED <<hook, reconnectReady>>

ReconnectExhausted ==
  /\ reconnect = ReconnectRunning
  /\ reconnect' = ReconnectTerminal
  /\ receipt' = Failed
  /\ UNCHANGED <<hook, reconnectReady, onConnect>>

HookFinish ==
  /\ hook = WaitingReceipt
  /\ receipt \in {Settled, Failed}
  /\ hook' = Done
  /\ UNCHANGED <<reconnectReady, reconnect, receipt, onConnect>>

RunOnConnect ==
  /\ onConnect = Pending
  /\ hook = Done
  /\ onConnect' = Done
  /\ UNCHANGED <<hook, reconnectReady, reconnect, receipt>>

Next ==
  \/ Loss
  \/ HookWaitReceipt
  \/ ReconnectStart
  \/ ReconnectSuccess
  \/ ReconnectExhausted
  \/ HookFinish
  \/ RunOnConnect

Spec == Init /\ [][Next]_vars

NoHookReceiptReconnectCycle ==
  ~(hook = WaitingReceipt /\ receipt = Pending /\
    reconnect = ReconnectWaiting /\ ~reconnectReady)

OnConnectSerializedBehindDisconnect ==
  ~(onConnect = Done /\ hook # Done)

=============================================================================
