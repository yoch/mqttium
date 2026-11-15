-------------------------------- MODULE M1a --------------------------------
(***************************************************************************)
(* M1a: admission of an inbound QoS 2 publication and authorisation of     *)
(* PUBREC (DEM-020, V3-018, ARC-011, decision records 0001 and 0005).      *)
(*                                                                         *)
(* One producer, the destinations Dests, the publications Pubs, NumOps     *)
(* admission identities, at most MaxCrash crashes and MaxLoss lost         *)
(* results. No collection, expiry, Will, PUBREL or PUBCOMP.                *)
(*                                                                         *)
(* Variables are grouped by domain. A product guard reads the durable,     *)
(* volatile and connection domains only. The history of the verifier,      *)
(* prefixed h, is written by actions and read by invariants and            *)
(* properties, never by a guard. The fault environment (proc, eCrash,      *)
(* eLoss) bounds the faults the bench injects; it is not product state     *)
(* either.                                                                 *)
(*                                                                         *)
(* Mutation selects one deliberately wrong predicate. "none" is the        *)
(* specified behaviour; every other value must give a counterexample for   *)
(* the property its configuration names.                                   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Pubs,       \* publications the producer may send; one inbound exchange each
    Dests,      \* destination sessions of every publication
    NumOps,     \* admission identities; consumed in order, never reused
    MaxCrash,   \* crashes of the broker process
    MaxLoss,    \* results lost between the store and the core
    Mutation    \* "none" or the name of a model mutation

Mutations == {"none", "AckBeforeCommit", "UnknownAsRejection", "RecipeChange",
              "SecondAdmissionAtReplay", "LateUnknownRegresses", "NoRearm"}

ASSUME /\ NumOps \in Nat /\ MaxCrash \in Nat /\ MaxLoss \in Nat
       /\ Mutation \in Mutations

Ops  == 1..NumOps
NoOp == 0
Gens == 1..(MaxCrash + 1)

\* A recipe names its source exchange (pub, xgen) and its targets. xgen is
\* the exchange generation of the inbound slot: 0 for the exchange the
\* client opened. Only a mutation mints 1.
Recipes    == [pub : Pubs, xgen : {0, 1}, targets : SUBSET Dests]
Outcomes   == {"Committed", "RejectedCertain", "PendingOrUnknown", "Mismatch"}
Commands   == [op : Ops, recipe : Recipes, hold : {"fresh", "held", "released"}]
Results    == [op : Ops, out : Outcomes]
Publishes  == [pub : Pubs, gen : Gens]
Answers    == [pub : Pubs, gen : Gens, kind : {"PUBREC", "NACK"}]

VARIABLES
    \* Durable: survives a crash; the only input of recovery.
    dDecision,  \* per identity: "none", "committed" or "fenced"
    dPub,       \* committed publications with their canonical recipe
    dSlot,      \* inbound QoS 2 slots in AwaitPubrel: [pub, xgen, op]
    dObl,       \* obligations: <<op, destination>>
    dAcct,      \* per destination: number of obligations charged
    dEpoch,     \* run epoch, advanced by every recovery
    dNextOp,    \* next identity; stands for (epoch, serial): never reused
    \* Volatile: erased by a crash.
    vSlot,      \* per exchange: Absent, Received, AdmissionPending,
                \*               AwaitPubrel, Refused
    vOp,        \* per exchange: the identity of its admission, or NoOp
    vKnow,      \* per identity: None, Prepared, Submitted, Unknown,
                \*               Committed, Rejected
    vPrep,      \* frozen recipes: [op, recipe]
    vDeadline,  \* per identity: a resolution deadline is armed
    vFault,     \* contradictory terminal results were reported
    \* Internal transport: erased by a crash.
    tCmd,       \* commands accepted by the store adapter, not applied yet
    tResolve,   \* identities with a ResolveAndFence waiting for the writer
    tResult,    \* results ready for the core
    \* Connections.
    cGen,       \* generation of the producer's connection
    cOpen,      \* that connection is open
    \* External transport.
    xIn,        \* PUBLISH frames sent by the client, not received yet
    xAuth,      \* answers authorised by the core, in its effect box
    xWire,      \* answers submitted to the socket, not observed yet
    \* Client (its own failure domain; survives a broker crash).
    kPhase,     \* per exchange: idle, sent, acked, refused
    kSentGen,   \* per exchange: connection generation of the last PUBLISH
    kGen,       \* connection generation the client reads from
    \* Fault environment of the bench.
    proc,       \* "up" or "down"
    eCrash,     \* crashes so far
    eLoss,      \* results lost so far
    \* History of the verifier.
    hAdmit,     \* admissions really committed: [pub, op]
    hPresented, \* every recipe presented to the store: <<op, recipe>>
    hKnown,     \* identities the core learnt as committed (result or recovery)
    hRefused,   \* identities the core treated as certainly rejected
    hFenced,    \* identities the store fenced
    hAuth       \* every answer the core authorised

durable  == <<dDecision, dPub, dSlot, dObl, dAcct, dEpoch, dNextOp>>
volatile == <<vSlot, vOp, vKnow, vPrep, vDeadline, vFault>>
internal == <<tCmd, tResolve, tResult>>
conn     == <<cGen, cOpen>>
external == <<xIn, xAuth, xWire>>
client   == <<kPhase, kSentGen, kGen>>
env      == <<proc, eCrash, eLoss>>
history  == <<hAdmit, hPresented, hKnown, hRefused, hFenced, hAuth>>
vars     == <<durable, volatile, internal, conn, external, client, env, history>>

RecipeOf(o) == (CHOOSE x \in vPrep : x.op = o).recipe

TypeOK ==
    /\ dDecision \in [Ops -> {"none", "committed", "fenced"}]
    /\ dPub \subseteq [op : Ops, recipe : Recipes]
    /\ dSlot \subseteq [pub : Pubs, xgen : {0, 1}, op : Ops]
    /\ dObl \subseteq Ops \X Dests
    /\ dAcct \in [Dests -> 0..NumOps]
    /\ dEpoch \in 1..(MaxCrash + 1)
    /\ dNextOp \in 1..(NumOps + 1)
    /\ vSlot \in [Pubs -> {"Absent", "Received", "AdmissionPending",
                           "AwaitPubrel", "Refused"}]
    /\ vOp \in [Pubs -> Ops \cup {NoOp}]
    /\ vKnow \in [Ops -> {"None", "Prepared", "Submitted", "Unknown",
                          "Committed", "Rejected"}]
    /\ vPrep \subseteq [op : Ops, recipe : Recipes]
    /\ vDeadline \in [Ops -> BOOLEAN]
    /\ vFault \in BOOLEAN
    /\ tCmd \subseteq Commands
    /\ tResolve \subseteq Ops
    /\ tResult \subseteq Results
    /\ cGen \in Gens
    /\ cOpen \in BOOLEAN
    /\ xIn \subseteq Publishes
    /\ xAuth \subseteq Answers
    /\ xWire \subseteq Answers
    /\ kPhase \in [Pubs -> {"idle", "sent", "acked", "refused"}]
    /\ kSentGen \in [Pubs -> 0..(MaxCrash + 1)]
    /\ kGen \in Gens
    /\ proc \in {"up", "down"}
    /\ eCrash \in 0..MaxCrash
    /\ eLoss \in 0..MaxLoss
    /\ hAdmit \subseteq [pub : Pubs, op : Ops]
    /\ hPresented \subseteq Ops \X Recipes
    /\ hKnown \subseteq Ops
    /\ hRefused \subseteq Ops
    /\ hFenced \subseteq Ops
    /\ hAuth \subseteq Answers

Init ==
    /\ dDecision = [o \in Ops |-> "none"]
    /\ dPub = {} /\ dSlot = {} /\ dObl = {}
    /\ dAcct = [d \in Dests |-> 0]
    /\ dEpoch = 1 /\ dNextOp = 1
    /\ vSlot = [p \in Pubs |-> "Absent"]
    /\ vOp = [p \in Pubs |-> NoOp]
    /\ vKnow = [o \in Ops |-> "None"]
    /\ vPrep = {}
    /\ vDeadline = [o \in Ops |-> FALSE]
    /\ vFault = FALSE
    /\ tCmd = {} /\ tResolve = {} /\ tResult = {}
    /\ cGen = 1 /\ cOpen = TRUE
    /\ xIn = {} /\ xAuth = {} /\ xWire = {}
    /\ kPhase = [p \in Pubs |-> "idle"]
    /\ kSentGen = [p \in Pubs |-> 0]
    /\ kGen = 1
    /\ proc = "up" /\ eCrash = 0 /\ eLoss = 0
    /\ hAdmit = {} /\ hPresented = {} /\ hKnown = {}
    /\ hRefused = {} /\ hFenced = {} /\ hAuth = {}

(***************************************************************************)
(* Client. MQTT 5 forbids resending a PUBLISH on a live connection: the    *)
(* client sends an exchange once per connection, and again only after a    *)
(* reconnect, while it has observed no answer.                             *)
(***************************************************************************)
ClientPublish(p) ==
    /\ kPhase[p] = "idle" /\ cOpen /\ kGen = cGen
    /\ xIn' = xIn \cup {[pub |-> p, gen |-> kGen]}
    /\ kPhase' = [kPhase EXCEPT ![p] = "sent"]
    /\ kSentGen' = [kSentGen EXCEPT ![p] = kGen]
    /\ UNCHANGED <<durable, volatile, internal, conn, xAuth, xWire, kGen,
                   env, history>>

ClientReconnect ==
    /\ proc = "up" /\ ~cOpen
    /\ cGen' = cGen + 1 /\ cOpen' = TRUE /\ kGen' = cGen + 1
    /\ UNCHANGED <<durable, volatile, internal, external, kPhase, kSentGen,
                   env, history>>

ClientResend(p) ==
    /\ kPhase[p] = "sent" /\ cOpen /\ kGen = cGen /\ kSentGen[p] < kGen
    /\ xIn' = xIn \cup {[pub |-> p, gen |-> kGen]}
    /\ kSentGen' = [kSentGen EXCEPT ![p] = kGen]
    /\ UNCHANGED <<durable, volatile, internal, conn, xAuth, xWire, kPhase,
                   kGen, env, history>>

\* Observation of an answer by the peer. A frame of an older connection is
\* never read once the client has reconnected.
ClientObserve(f) ==
    /\ f \in xWire /\ f.gen = kGen
    /\ xWire' = xWire \ {f}
    /\ kPhase' = [kPhase EXCEPT ![f.pub] =
                      IF f.kind = "PUBREC" THEN "acked" ELSE "refused"]
    /\ UNCHANGED <<durable, volatile, internal, conn, xIn, xAuth, kSentGen,
                   kGen, env, history>>

(***************************************************************************)
(* Core: reception and preparation (ARC-011).                              *)
(***************************************************************************)
Emit(f) == /\ xAuth' = xAuth \cup {f}
           /\ hAuth' = hAuth \cup {f}

\* The answer goes to the connection generation that is current when it is
\* authorised (record 0001); with no open connection nothing is emitted.
Answer(p, kind) ==
    IF cOpen THEN Emit([pub |-> p, gen |-> cGen, kind |-> kind])
             ELSE UNCHANGED <<xAuth, hAuth>>

Receive(p) ==
    LET f == [pub |-> p, gen |-> cGen] IN
    /\ proc = "up" /\ cOpen /\ f \in xIn
    /\ xIn' = xIn \ {f}
    /\ \/ /\ vSlot[p] = "Absent"
          /\ vSlot' = [vSlot EXCEPT ![p] = "Received"]
          /\ UNCHANGED <<xAuth, hAuth>>
       \* A replay while the admission is unresolved creates no command and
       \* no acknowledgement (record 0001). Not reachable without a reset.
       \/ /\ vSlot[p] \in {"Received", "AdmissionPending"}
          /\ UNCHANGED <<vSlot, xAuth, hAuth>>
       \* A replay of an admitted exchange is answered without a second
       \* admission.
       \/ /\ vSlot[p] = "AwaitPubrel" /\ Mutation # "SecondAdmissionAtReplay"
          /\ Answer(p, "PUBREC")
          /\ UNCHANGED vSlot
       \/ /\ vSlot[p] = "AwaitPubrel" /\ Mutation = "SecondAdmissionAtReplay"
          /\ vSlot' = [vSlot EXCEPT ![p] = "Received"]
          /\ UNCHANGED <<xAuth, hAuth>>
       \/ /\ vSlot[p] = "Refused"
          /\ Answer(p, "NACK")
          /\ UNCHANGED vSlot
    /\ UNCHANGED <<durable, vOp, vKnow, vPrep, vDeadline, vFault, internal,
                   conn, xWire, client, env, hAdmit, hPresented, hKnown,
                   hRefused, hFenced>>

\* Creation takes the next identity. With none left it is suspended: the
\* exchange stays Received and nothing else is touched.
Prepare(p) ==
    /\ proc = "up" /\ vSlot[p] = "Received" /\ dNextOp <= NumOps
    /\ LET o == dNextOp
           r == [pub |-> p,
                 \* The mutation hides the replay behind a new exchange
                 \* generation (DEM-MU-04).
                 xgen |-> IF vOp[p] # NoOp THEN 1 ELSE 0,
                 targets |-> Dests]
       IN /\ vSlot' = [vSlot EXCEPT ![p] = "AdmissionPending"]
          /\ vOp' = [vOp EXCEPT ![p] = o]
          /\ vKnow' = [vKnow EXCEPT ![o] = "Prepared"]
          /\ vPrep' = vPrep \cup {[op |-> o, recipe |-> r]}
          /\ dNextOp' = o + 1
    /\ UNCHANGED <<dDecision, dPub, dSlot, dObl, dAcct, dEpoch, vDeadline,
                   vFault, internal, conn, external, client, env, history>>

\* The store adapter accepts Persist, then the timer adapter accepts
\* ArmDeadline: the delay starts at this acceptance (record 0001).
Submit(o) ==
    /\ proc = "up" /\ vKnow[o] = "Prepared"
    /\ tCmd' = tCmd \cup {[op |-> o, recipe |-> RecipeOf(o), hold |-> "fresh"]}
    /\ hPresented' = hPresented \cup {<<o, RecipeOf(o)>>}
    /\ vKnow' = [vKnow EXCEPT ![o] = "Submitted"]
    /\ vDeadline' = [vDeadline EXCEPT ![o] = TRUE]
    /\ UNCHANGED <<durable, vSlot, vOp, vPrep, vFault, tResolve, tResult,
                   conn, external, client, env, hAdmit, hKnown, hRefused,
                   hFenced, hAuth>>

\* While the outcome is unknown the same identity may be presented again,
\* with the same frozen recipe only (DEM-009).
Represent(o) ==
    /\ proc = "up" /\ vKnow[o] = "Unknown"
    /\ LET r == IF Mutation = "RecipeChange"
                THEN [RecipeOf(o) EXCEPT !.targets = {CHOOSE d \in Dests : TRUE}]
                ELSE RecipeOf(o)
       IN /\ tCmd' = tCmd \cup {[op |-> o, recipe |-> r, hold |-> "fresh"]}
          /\ hPresented' = hPresented \cup {<<o, r>>}
    /\ UNCHANGED <<durable, volatile, tResolve, tResult, conn, external,
                   client, env, hAdmit, hKnown, hRefused, hFenced, hAuth>>

(***************************************************************************)
(* Internal transport and store. The bench may hold a command once before  *)
(* it enters the writer, and release it later.                             *)
(***************************************************************************)
HoldCommand(c) ==
    /\ c \in tCmd /\ c.hold = "fresh"
    /\ tCmd' = (tCmd \ {c}) \cup {[c EXCEPT !.hold = "held"]}
    /\ UNCHANGED <<durable, volatile, tResolve, tResult, conn, external,
                   client, env, history>>

ReleaseCommand(c) ==
    /\ c \in tCmd /\ c.hold = "held"
    /\ tCmd' = (tCmd \ {c}) \cup {[c EXCEPT !.hold = "released"]}
    /\ UNCHANGED <<durable, volatile, tResolve, tResult, conn, external,
                   client, env, history>>

Ready(o, out) == tResult' = tResult \cup {[op |-> o, out |-> out]}

SlotFree(r) == ~\E s \in dSlot : s.pub = r.pub /\ s.xgen = r.xgen

\* ApplyCommit: publication, obligations, inbound slot and accounts are
\* committed together. This atomicity is an assumption of the model.
Commit(o, r) ==
    /\ dDecision' = [dDecision EXCEPT ![o] = "committed"]
    /\ dPub' = dPub \cup {[op |-> o, recipe |-> r]}
    /\ dSlot' = dSlot \cup {[pub |-> r.pub, xgen |-> r.xgen, op |-> o]}
    /\ dObl' = dObl \cup {<<o, d>> : d \in r.targets}
    /\ dAcct' = [d \in Dests |-> IF d \in r.targets THEN dAcct[d] + 1
                                                    ELSE dAcct[d]]
    /\ hAdmit' = hAdmit \cup {[pub |-> r.pub, op |-> o]}
    /\ UNCHANGED hFenced

\* A certain rejection is durable: the identity can never execute.
Fence(o) ==
    /\ dDecision' = [dDecision EXCEPT ![o] = "fenced"]
    /\ hFenced' = hFenced \cup {o}

ApplyCommand(c) ==
    LET o == c.op IN
    /\ proc = "up" /\ c \in tCmd /\ c.hold # "held"
    /\ tCmd' = tCmd \ {c}
    /\ \/ /\ dDecision[o] = "committed"
          \* A repeated presentation: compared with the stored recipe, no
          \* second effect.
          /\ Ready(o, IF [op |-> o, recipe |-> c.recipe] \in dPub
                      THEN "Committed" ELSE "Mismatch")
          /\ UNCHANGED <<durable, hAdmit, hFenced>>
       \/ /\ dDecision[o] = "fenced"
          /\ Ready(o, "RejectedCertain")
          /\ UNCHANGED <<durable, hAdmit, hFenced>>
       \/ /\ dDecision[o] = "none" /\ SlotFree(c.recipe)
          /\ Commit(o, c.recipe)
          /\ Ready(o, "Committed")
          /\ UNCHANGED <<dEpoch, dNextOp>>
       \/ /\ dDecision[o] = "none" /\ ~SlotFree(c.recipe)
          /\ Fence(o)
          /\ Ready(o, "RejectedCertain")
          /\ UNCHANGED <<dPub, dSlot, dObl, dAcct, dEpoch, dNextOp, hAdmit>>
    /\ UNCHANGED <<volatile, tResolve, conn, external, client, env,
                   hPresented, hKnown, hRefused, hAuth>>

\* The store refuses a command for a reason of its own (a durable
\* capacity, detailed by M1b). Nothing is committed.
RejectCommand(c) ==
    LET o == c.op IN
    /\ proc = "up" /\ c \in tCmd /\ c.hold # "held" /\ dDecision[o] = "none"
    /\ tCmd' = tCmd \ {c}
    /\ Fence(o)
    /\ Ready(o, "RejectedCertain")
    /\ UNCHANGED <<dPub, dSlot, dObl, dAcct, dEpoch, dNextOp, volatile,
                   tResolve, conn, external, client, env, hAdmit, hPresented,
                   hKnown, hRefused, hAuth>>

\* ResolveAndFence, ordered with the application of commands. An absent
\* identity is either fenced, or the fence cannot be established and the
\* outcome stays unknown: absence alone is never a rejection (DEM-010).
Resolve(o) ==
    /\ proc = "up" /\ o \in tResolve
    /\ tResolve' = tResolve \ {o}
    /\ \/ /\ dDecision[o] = "committed"
          /\ Ready(o, "Committed")
          /\ UNCHANGED <<dDecision, hFenced>>
       \/ /\ dDecision[o] = "fenced"
          /\ Ready(o, "RejectedCertain")
          /\ UNCHANGED <<dDecision, hFenced>>
       \/ /\ dDecision[o] = "none"
          /\ Fence(o)
          /\ Ready(o, "RejectedCertain")
       \/ /\ dDecision[o] = "none"
          /\ Ready(o, "PendingOrUnknown")
          /\ UNCHANGED <<dDecision, hFenced>>
    /\ UNCHANGED <<dPub, dSlot, dObl, dAcct, dEpoch, dNextOp, volatile, tCmd,
                   conn, external, client, env, hAdmit, hPresented, hKnown,
                   hRefused, hAuth>>

LoseResult(r) ==
    /\ r \in tResult /\ eLoss < MaxLoss
    /\ tResult' = tResult \ {r}
    /\ eLoss' = eLoss + 1
    /\ UNCHANGED <<durable, volatile, tCmd, tResolve, conn, external, client,
                   proc, eCrash, history>>

(***************************************************************************)
(* Core: knowledge. It only grows; a late unknown never replaces a         *)
(* terminal result, and contradictory terminal results are a fault.        *)
(***************************************************************************)
DeliverResult(r) ==
    LET o == r.op
        k == vKnow[o]
    IN
    /\ proc = "up" /\ r \in tResult
    /\ tResult' = tResult \ {r}
    /\ \/ /\ k \in {"Submitted", "Unknown"} /\ r.out = "Committed"
          /\ vKnow' = [vKnow EXCEPT ![o] = "Committed"]
          /\ vDeadline' = [vDeadline EXCEPT ![o] = FALSE]
          /\ hKnown' = hKnown \cup {o}
          /\ UNCHANGED <<vFault, hRefused>>
       \/ /\ k \in {"Submitted", "Unknown"} /\ r.out = "RejectedCertain"
          /\ vKnow' = [vKnow EXCEPT ![o] = "Rejected"]
          /\ vDeadline' = [vDeadline EXCEPT ![o] = FALSE]
          /\ hRefused' = hRefused \cup {o}
          /\ UNCHANGED <<vFault, hKnown>>
       \/ /\ k \in {"Submitted", "Unknown"} /\ r.out = "PendingOrUnknown"
          /\ vKnow' = [vKnow EXCEPT ![o] = "Unknown"]
          /\ UNCHANGED <<vDeadline, vFault, hKnown, hRefused>>
       \* Late or repeated results that change nothing.
       \/ /\ \/ k \in {"None", "Prepared"}
             \/ k = "Committed" /\ r.out = "Committed"
             \/ k = "Committed" /\ r.out = "PendingOrUnknown"
                                /\ Mutation # "LateUnknownRegresses"
             \/ k = "Rejected" /\ r.out \in {"RejectedCertain",
                                             "PendingOrUnknown"}
          /\ UNCHANGED <<vKnow, vDeadline, vFault, hKnown, hRefused>>
       \/ /\ k = "Committed" /\ r.out = "PendingOrUnknown"
          /\ Mutation = "LateUnknownRegresses"
          /\ vKnow' = [vKnow EXCEPT ![o] = "Unknown"]
          /\ vDeadline' = [vDeadline EXCEPT ![o] = TRUE]
          /\ UNCHANGED <<vFault, hKnown, hRefused>>
       \/ /\ \/ k \in {"Submitted", "Unknown"} /\ r.out = "Mismatch"
             \/ k = "Committed" /\ r.out \in {"RejectedCertain", "Mismatch"}
             \/ k = "Rejected" /\ r.out \in {"Committed", "Mismatch"}
          /\ vFault' = TRUE
          /\ UNCHANGED <<vKnow, vDeadline, hKnown, hRefused>>
    /\ UNCHANGED <<durable, vSlot, vOp, vPrep, tCmd, tResolve, conn, external,
                   client, env, hAdmit, hPresented, hFenced, hAuth>>

\* A deadline is applied only for an identity that is still unresolved; it
\* asks for a resolution and stays armed until knowledge is terminal.
Timeout(o) ==
    /\ proc = "up" /\ vDeadline[o] /\ vKnow[o] \in {"Submitted", "Unknown"}
    /\ IF Mutation = "UnknownAsRejection"
       THEN /\ vKnow' = [vKnow EXCEPT ![o] = "Rejected"]
            /\ vDeadline' = [vDeadline EXCEPT ![o] = FALSE]
            /\ hRefused' = hRefused \cup {o}
            /\ UNCHANGED tResolve
       ELSE /\ vKnow' = [vKnow EXCEPT ![o] = "Unknown"]
            /\ tResolve' = tResolve \cup {o}
            /\ vDeadline' = IF Mutation = "NoRearm"
                            THEN [vDeadline EXCEPT ![o] = FALSE]
                            ELSE vDeadline
            /\ UNCHANGED hRefused
    /\ UNCHANGED <<durable, vSlot, vOp, vPrep, vFault, tCmd, tResult, conn,
                   external, client, env, hAdmit, hPresented, hKnown, hFenced,
                   hAuth>>

(***************************************************************************)
(* Core: authorisation. PUBREC needs the knowledge of the commit.          *)
(***************************************************************************)
Authorize(p) ==
    /\ proc = "up" /\ vSlot[p] = "AdmissionPending"
    /\ \/ vKnow[vOp[p]] = "Committed"
       \/ Mutation = "AckBeforeCommit" /\ vKnow[vOp[p]] \in {"Submitted", "Unknown"}
    /\ vSlot' = [vSlot EXCEPT ![p] = "AwaitPubrel"]
    /\ Answer(p, "PUBREC")
    /\ UNCHANGED <<durable, vOp, vKnow, vPrep, vDeadline, vFault, internal,
                   conn, xIn, xWire, client, env, hAdmit, hPresented, hKnown,
                   hRefused, hFenced>>

\* A certain rejection ends the exchange without distribution.
Refuse(p) ==
    /\ proc = "up" /\ vSlot[p] = "AdmissionPending"
    /\ vKnow[vOp[p]] = "Rejected"
    /\ vSlot' = [vSlot EXCEPT ![p] = "Refused"]
    /\ Answer(p, "NACK")
    /\ UNCHANGED <<durable, vOp, vKnow, vPrep, vDeadline, vFault, internal,
                   conn, xIn, xWire, client, env, hAdmit, hPresented, hKnown,
                   hRefused, hFenced>>

\* The transport adapter accepts an authorised frame. A frame for a
\* connection that is no longer current is dropped.
SubmitFrame(f) ==
    /\ proc = "up" /\ f \in xAuth
    /\ xAuth' = xAuth \ {f}
    /\ xWire' = IF cOpen /\ f.gen = cGen THEN xWire \cup {f} ELSE xWire
    /\ UNCHANGED <<durable, volatile, internal, conn, xIn, client, env,
                   history>>

(***************************************************************************)
(* Crash and recovery. A crash erases the volatile domain, the internal    *)
(* transport, the effect box and the connection. Recovery reads durable    *)
(* variables only.                                                         *)
(***************************************************************************)
Crash ==
    /\ proc = "up" /\ eCrash < MaxCrash
    /\ proc' = "down" /\ eCrash' = eCrash + 1
    /\ vSlot' = [p \in Pubs |-> "Absent"]
    /\ vOp' = [p \in Pubs |-> NoOp]
    /\ vKnow' = [o \in Ops |-> "None"]
    /\ vPrep' = {}
    /\ vDeadline' = [o \in Ops |-> FALSE]
    /\ vFault' = FALSE
    /\ tCmd' = {} /\ tResolve' = {} /\ tResult' = {}
    /\ xIn' = {} /\ xAuth' = {}
    /\ cOpen' = FALSE
    /\ UNCHANGED <<durable, cGen, xWire, client, eLoss, history>>

Recover ==
    /\ proc = "down"
    /\ proc' = "up"
    /\ dEpoch' = dEpoch + 1
    /\ vSlot' = [p \in Pubs |-> IF \E s \in dSlot : s.pub = p
                                THEN "AwaitPubrel" ELSE "Absent"]
    /\ vOp' = [p \in Pubs |-> IF \E s \in dSlot : s.pub = p
                              THEN (CHOOSE s \in dSlot : s.pub = p).op
                              ELSE NoOp]
    /\ vKnow' = [o \in Ops |-> IF dDecision[o] = "committed"
                               THEN "Committed" ELSE "None"]
    /\ vPrep' = dPub
    /\ hKnown' = hKnown \cup {o \in Ops : dDecision[o] = "committed"}
    /\ UNCHANGED <<dDecision, dPub, dSlot, dObl, dAcct, dNextOp, vDeadline,
                   vFault, internal, conn, external, client, eCrash, eLoss,
                   hAdmit, hPresented, hRefused, hFenced, hAuth>>

(***************************************************************************)
(* Legitimate ends: every exchange the client opened is answered, or its   *)
(* creation is suspended because the identities are used up.               *)
(***************************************************************************)
Suspended(p) == vSlot[p] = "Received" /\ dNextOp > NumOps

Terminal ==
    /\ proc = "up" /\ cOpen
    /\ tCmd = {} /\ tResolve = {} /\ tResult = {} /\ xAuth = {}
    /\ \A p \in Pubs : \/ kPhase[p] \in {"acked", "refused"}
                       \/ kPhase[p] = "sent" /\ Suspended(p)

Quiescent == Terminal /\ UNCHANGED vars

Next ==
    \/ \E p \in Pubs : \/ ClientPublish(p) \/ ClientResend(p)
                       \/ Receive(p) \/ Prepare(p)
                       \/ Authorize(p) \/ Refuse(p)
    \/ ClientReconnect
    \/ \E f \in xWire : ClientObserve(f)
    \/ \E f \in xAuth : SubmitFrame(f)
    \/ \E o \in Ops : Submit(o) \/ Represent(o) \/ Timeout(o) \/ Resolve(o)
    \/ \E c \in tCmd : \/ HoldCommand(c) \/ ReleaseCommand(c)
                       \/ ApplyCommand(c) \/ RejectCommand(c)
    \/ \E r \in tResult : DeliverResult(r) \/ LoseResult(r)
    \/ Crash
    \/ Recover
    \/ Quiescent

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Safety. No fairness is assumed.                                         *)
(***************************************************************************)

\* One admission per exchange as the client and the verifier identify it:
\* a new exchange generation does not reset the count.
OneAdmissionPerExchange ==
    \A p \in Pubs : Cardinality({a \in hAdmit : a.pub = p}) <= 1

\* A committed identity has its publication, its slot and an obligation
\* for every destination; any other identity has none of them.
AtomicObligations ==
    \A o \in Ops :
        IF dDecision[o] = "committed"
        THEN /\ \E x \in dPub : x.op = o
             /\ \E s \in dSlot : s.op = o
             /\ {d \in Dests : <<o, d>> \in dObl} = Dests
        ELSE /\ ~\E x \in dPub : x.op = o
             /\ ~\E s \in dSlot : s.op = o
             /\ ~\E d \in Dests : <<o, d>> \in dObl

\* A PUBREC was authorised only for an exchange whose commit the core had
\* learnt, by a result or by recovery. The hidden truth of the store is
\* not enough.
AnswerOnKnowledge ==
    \A f \in hAuth : f.kind = "PUBREC" =>
        \E a \in hAdmit : a.pub = f.pub /\ a.op \in hKnown

OneRecipePerIdentity ==
    \A o \in Ops : Cardinality({x \in hPresented : x[1] = o}) <= 1

\* A fence, and a rejection the core relied on, exclude any execution.
FenceForbidsExecution ==
    \A o \in hFenced \cup hRefused : dDecision[o] # "committed"

AccountsMatch ==
    \A d \in Dests : dAcct[d] = Cardinality({o \in Ops : <<o, d>> \in dObl})

\* Knowledge agrees with the store and no contradiction was reported.
KnowledgeSound ==
    /\ ~vFault
    /\ \A o \in Ops : vKnow[o] = "Committed" => dDecision[o] = "committed"

\* Terminal knowledge never regresses; recovery finds it again.
TerminalKnowledgeStable ==
    proc = "up" => \A o \in hKnown : vKnow[o] = "Committed"

\* An undecided identity the core still waits for keeps an executable
\* command: using up the identities removes no delayed command.
DelayedCommandKept ==
    \A o \in Ops :
        vKnow[o] \in {"Submitted", "Unknown"} /\ dDecision[o] = "none"
            => \E c \in tCmd : c.op = o

(***************************************************************************)
(* Non-vacuity. Each predicate is checked as an invariant that TLC must    *)
(* report as violated: the witness is the counterexample.                  *)
(***************************************************************************)
ReachCommit == \E o \in Ops : dDecision[o] = "committed"

\* The result of a commit was lost and the core does not know of it.
ReachLostResult ==
    /\ eLoss > 0
    /\ \E o \in Ops : /\ dDecision[o] = "committed"
                      /\ vKnow[o] \in {"Submitted", "Unknown"}
                      /\ ~\E r \in tResult : r.op = o

ReachUnknown == \E o \in Ops : vKnow[o] = "Unknown"

\* A certain rejection reached the peer.
ReachRejection == \E p \in Pubs : kPhase[p] = "refused"

\* A crash while a commit was never known to the core nor answered.
ReachCrashAfterCommit ==
    /\ proc = "down"
    /\ \E a \in hAdmit : a.op \notin hKnown /\ kPhase[a.pub] = "sent"

\* The shape of P1-02: an admission of the first epoch, never known before
\* the crash, recovered and acknowledged on the new connection without a
\* second identity.
ReachRecovery ==
    /\ eCrash > 0 /\ proc = "up" /\ dNextOp = 2
    /\ \E p \in Pubs : /\ kPhase[p] = "acked" /\ kSentGen[p] = 2
                       /\ \E s \in dSlot : s.pub = p /\ s.op = 1

\* The shape of P1-02b: no crash, the commit result lost, the outcome
\* resolved and PUBREC observed while an old unknown answer is still held.
ReachLivePath ==
    /\ eCrash = 0 /\ eLoss > 0
    /\ \E p \in Pubs : /\ kPhase[p] = "acked" /\ kSentGen[p] = 1
                       /\ vOp[p] # NoOp /\ vKnow[vOp[p]] = "Committed"
                       /\ [op |-> vOp[p], out |-> "PendingOrUnknown"] \in tResult

\* Both identities are used up, a creation is suspended and a delayed
\* command is still there.
ReachExhaustion ==
    /\ \E p \in Pubs : kPhase[p] = "sent" /\ Suspended(p)
    /\ \E c \in tCmd : c.hold = "held"

NoCommit           == ~ReachCommit
NoLostResult       == ~ReachLostResult
NoUnknown          == ~ReachUnknown
NoRejection        == ~ReachRejection
NoCrashAfterCommit == ~ReachCrashAfterCommit
NoRecovery         == ~ReachRecovery
NoLivePath         == ~ReachLivePath
NoExhaustion       == ~ReachExhaustion

\* With one publication and one crash the identities cannot run out: the
\* recovery property is then checked without its suspension escape.
NoSuspension == \A p \in Pubs : ~Suspended(p)

(***************************************************************************)
(* Liveness: two properties, each with its own assumptions.                *)
(***************************************************************************)
Answered(p) == kPhase[p] \in {"acked", "refused"}

\* What the broker, its store, its timer and its transport do by
\* themselves. Weak fairness only. Nothing is assumed of the client
\* sending a packet; the peer reads the frames that reach its socket. The
\* bench releases a command it holds, and loses at most MaxLoss results.
FairBroker ==
    /\ \A o \in Ops :
        /\ WF_vars(\E c \in tCmd : c.op = o /\ ReleaseCommand(c))
        /\ WF_vars(\E c \in tCmd : c.op = o /\ ApplyCommand(c))
        /\ WF_vars(Resolve(o))
        /\ WF_vars(Timeout(o))
        /\ \A out \in Outcomes : WF_vars(DeliverResult([op |-> o, out |-> out]))
    /\ \A p \in Pubs :
        /\ WF_vars(Authorize(p))
        /\ WF_vars(Refuse(p))
        /\ WF_vars(\E f \in xAuth : f.pub = p /\ SubmitFrame(f))
        /\ WF_vars(\E f \in xWire : f.pub = p /\ ClientObserve(f))
        \* Intake of a frame that did arrive; it assumes no arrival.
        /\ WF_vars(Receive(p))
        /\ WF_vars(Prepare(p))
    /\ \A o \in Ops : WF_vars(Submit(o))
    /\ WF_vars(Recover)

\* The live path of P1-02b (record 0001): from the unknown outcome to its
\* resolution and to the answer, with no new packet from the client.
LiveSpec == Spec /\ FairBroker

LivePathResolves ==
    \A p \in Pubs :
        (vSlot[p] = "AdmissionPending" /\ vKnow[vOp[p]] = "Unknown")
            ~> Answered(p)

\* After the last crash. The one assumption added to those of the live
\* path is about the client: it usefully reconnects and sends again the
\* PUBLISH it has no answer for.
FairClientResume ==
    /\ WF_vars(ClientReconnect)
    /\ \A p \in Pubs : WF_vars(ClientResend(p))

RecoverySpec == LiveSpec /\ FairClientResume

RecoveryCompletes ==
    \A p \in Pubs :
        (eCrash = MaxCrash /\ kPhase[p] = "sent")
            ~> (Answered(p) \/ Suspended(p))

=============================================================================
