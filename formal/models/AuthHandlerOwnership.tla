---- MODULE AuthHandlerOwnership ----
EXTENDS Naturals

(***************************************************************************
Who runs auth_handler, and when its answer may be sent.

The handler awaits one operation that needs a runtime owner:
  Need = "pump"    publish()/subscribe()/auth() drain the effect pump, or a
                   receipt whose completion is queued behind AUTH (#502 #527)
  Need = "reader"  a message from the same read, delivered by the reader
                   after its protocol target (#523)
  Need = "none"    nothing: isolates the stale-answer rule
Meanwhile the broker may end the exchange (AUTH Success, DISCONNECT) before
the handler answers (#528 #535).

Variant = "rc15": the handler runs inside the effect pump under its lock;
the reader's protocol target includes the AUTH effect; any returned
AuthPacket is queued.
Variant = "fixed": the handler runs in its own task; neither the pump nor
the reader waits for it; the answer is sent only if the exchange still waits
for this challenge.
***************************************************************************)

CONSTANTS Variant, Need
ASSUME Variant \in {"rc15", "fixed"}
ASSUME Need \in {"pump", "reader", "none"}

VARIABLES handler, exchangeOpen, staleAnswerSent

vars == <<handler, exchangeOpen, staleAnswerSent>>

TypeOK ==
  /\ handler \in {"idle", "awaiting", "answering", "done"}
  /\ exchangeOpen \in BOOLEAN
  /\ staleAnswerSent \in BOOLEAN

Init ==
  /\ handler = "idle"
  /\ exchangeOpen = TRUE
  /\ staleAnswerSent = FALSE

\* The pump is busy exactly while it runs the handler (rc15 only).
PumpBusy == Variant = "rc15" /\ handler \in {"awaiting", "answering"}

\* rc15: the reader waits for its lot's protocol target, which includes AUTH.
ReaderBlocked == Variant = "rc15" /\ handler \in {"awaiting", "answering"}

Challenge ==
  /\ handler = "idle"
  /\ handler' = "awaiting"
  /\ UNCHANGED <<exchangeOpen, staleAnswerSent>>

\* The awaited operation completes only when its owner can make progress.
OperationCompletes ==
  /\ handler = "awaiting"
  /\ CASE Need = "pump" -> ~PumpBusy
       [] Need = "reader" -> ~ReaderBlocked
       [] OTHER -> TRUE
  /\ handler' = "answering"
  /\ UNCHANGED <<exchangeOpen, staleAnswerSent>>

\* The broker ends the exchange before the answer (Success or DISCONNECT).
BrokerEndsExchange ==
  /\ exchangeOpen
  /\ handler \in {"awaiting", "answering"}
  /\ exchangeOpen' = FALSE
  /\ UNCHANGED <<handler, staleAnswerSent>>

Answer ==
  /\ handler = "answering"
  /\ handler' = "done"
  /\ staleAnswerSent' = (staleAnswerSent \/ (Variant = "rc15" /\ ~exchangeOpen))
  /\ UNCHANGED exchangeOpen

Terminal == handler = "done"

Quiescent == Terminal /\ UNCHANGED vars

Next == Challenge \/ OperationCompletes \/ BrokerEndsExchange \/ Answer \/ Quiescent

Spec == Init /\ [][Next]_vars

NoStaleAnswer == ~staleAnswerSent

=============================================================================
