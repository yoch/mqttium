-------------------------------- MODULE M1b --------------------------------
(***************************************************************************)
(* M1b: admission under resets, session generations, capacity refusals,    *)
(* fences and the change of epoch (DEM-020, V3-018, DEM-010, DEM-012,      *)
(* ARC-011, decision record 0001).                                         *)
(*                                                                         *)
(* One producer with one inbound slot (one Packet Identifier), the         *)
(* destinations Dests, NumOps admission identities. The producer's         *)
(* session and each destination session have generations: a session that  *)
(* is recreated is another session. An exchange is identified by the       *)
(* generation of the producer's session that opened it.                    *)
(*                                                                         *)
(* What M1b adds to M1a:                                                   *)
(*  - a connection reset without a crash, and the replay of a PUBLISH      *)
(*    while its admission is unresolved;                                   *)
(*  - the reset of the producer's session and of a destination session;    *)
(*  - a durable capacity per destination and its refusal;                  *)
(*  - commands that outlive a crash of the core, and the closing of the    *)
(*    epoch that forbids their execution;                                  *)
(*  - ResolveAndFence in two steps, so that an application can fall        *)
(*    between the observed absence and the fence.                          *)
(*                                                                         *)
(* It leaves to M1a: the explicit hold of a command and the liveness       *)
(* properties. The history of the verifier, prefixed h, is read by no      *)
(* guard. Mutation selects one deliberately wrong predicate.               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Dests,          \* destination sessions
    NumOps,         \* admission identities; consumed in order, never reused
    Capacity,       \* obligations a destination may hold
    MaxCrash,       \* crashes of the core
    MaxConnReset,   \* connection resets without a crash
    MaxSrcReset,    \* resets of the producer's session
    MaxDestReset,   \* resets of destination sessions, in all
    MaxSettle,      \* obligations settled by their destination
    MaxLoss,        \* results lost between the store and the core
    MaxFenceFail,   \* resolutions whose write of the fence is refused
    Mutation

Mutations == {"none", "FenceOnStaleRead", "OldEpochExecutes",
              "StaleTargetAccepted", "RefusalNotDurable",
              "SourceResetDropsObligations", "ResetKeepsAttachment",
              "ReplayReadmits"}

ASSUME /\ NumOps \in Nat /\ Capacity \in Nat /\ MaxSettle \in Nat
       /\ MaxCrash \in Nat /\ MaxConnReset \in Nat /\ MaxSrcReset \in Nat
       /\ MaxDestReset \in Nat /\ MaxLoss \in Nat /\ MaxFenceFail \in Nat
       /\ Mutation \in Mutations

Ops      == 1..NumOps
NoOp     == 0
Epochs   == 1..(MaxCrash + 1)
SrcGens  == 1..(MaxSrcReset + 1)
DestGens == 1..(MaxDestReset + 1)
ConnGens == 1..(MaxCrash + MaxConnReset + MaxSrcReset + 1)

Targets   == [dest : Dests, gen : DestGens]
\* A recipe names its source exchange, the epoch it was prepared in and its
\* targets with their generations.
Recipes   == [sgen : SrcGens, epoch : Epochs, targets : SUBSET Targets]
Outcomes  == {"Committed", "RejectedCertain", "PendingOrUnknown", "Mismatch"}
Commands  == [op : Ops, recipe : Recipes]
Results   == [op : Ops, out : Outcomes]
Answers   == [gen : ConnGens, kind : {"PUBREC", "NACK"}]
Obligations == [op : Ops, dest : Dests, gen : DestGens]

VARIABLES
    \* Durable.
    dEpoch,     \* run epoch; every older epoch is closed for execution
    dCommitted, \* identities whose admission is committed
    dFenced,    \* identities that can never execute
    dPub,       \* committed publications with their canonical recipe
    dSlot,      \* inbound slot rows in AwaitPubrel: [sgen, op]
    dObl,       \* obligations
    dUsed,      \* per destination: capacity in use
    dSrcGen,    \* generation of the producer's session
    dDestGen,   \* generation of each destination session
    dNextOp,    \* next identity; stands for (epoch, serial): never reused
    \* Volatile: erased by a crash of the core.
    vSlot,      \* Absent, Received, AdmissionPending, AwaitPubrel, Refused
    vOp,        \* the identity attached to the slot, or NoOp
    vKnow,      \* per identity: None, Prepared, Submitted, Unknown,
                \*               Committed, Rejected
    vPrep,      \* frozen recipes: [op, recipe]
    vDeadline,  \* per identity: a resolution deadline is armed
    vFault,     \* contradictory terminal results were reported
    \* Store side of the internal transport: outlives a crash of the core.
    tCmd,       \* commands accepted by the store adapter, not applied yet
    \* Core side of the internal transport: erased by a crash.
    tResolve,   \* identities with a ResolveAndFence waiting for the writer
    tFencing,   \* identities the resolver observed as absent, not fenced yet
    tResult,    \* results ready for the core
    \* Connections.
    cGen, cOpen,
    \* External transport.
    xIn,        \* a PUBLISH is in flight on the current connection
    xAuth,      \* answers authorised by the core, in its effect box
    xWire,      \* answers submitted to the socket, not observed yet
    \* Client.
    kPhase,     \* idle, sent, acked, refused
    kExch,      \* the exchange the client is in: its session generation
    kSentGen,   \* connection generation of its last PUBLISH
    kGen,       \* connection generation the client reads from
    \* Fault environment of the bench.
    proc, eCrash, eConnReset, eDestReset, eSettle, eLoss, eFence,
    \* History of the verifier.
    hAdmit,     \* admissions really committed: [sgen, op, epoch at commit]
    hObl,       \* every obligation committed and not settled
    hRefused,   \* identities the core treated as certainly rejected
    hKnown,     \* identities the core learnt as committed
    hRaces      \* per identity: how its fence and its application were
                \* ordered, and why the store refused its command

durable  == <<dEpoch, dCommitted, dFenced, dPub, dSlot, dObl, dUsed, dSrcGen,
              dDestGen, dNextOp>>
volatile == <<vSlot, vOp, vKnow, vPrep, vDeadline, vFault>>
internal == <<tCmd, tResolve, tFencing, tResult>>
conn     == <<cGen, cOpen>>
external == <<xIn, xAuth, xWire>>
client   == <<kPhase, kExch, kSentGen, kGen>>
env      == <<proc, eCrash, eConnReset, eDestReset, eSettle, eLoss, eFence>>
history  == <<hAdmit, hObl, hRefused, hKnown, hRaces>>
vars     == <<durable, volatile, internal, conn, external, client, env, history>>

RecipeOf(o) == (CHOOSE x \in vPrep : x.op = o).recipe

\* What the verifier recorded about one identity.
RacesOf(o) == {e[1] : e \in {x \in hRaces : x[2] = o}}

TypeOK ==
    /\ dEpoch \in Epochs
    /\ dCommitted \subseteq Ops /\ dFenced \subseteq Ops
    /\ dPub \subseteq [op : Ops, recipe : Recipes]
    /\ dSlot \subseteq [sgen : SrcGens, op : Ops]
    /\ dObl \subseteq Obligations
    /\ dUsed \in [Dests -> 0..NumOps]
    /\ dSrcGen \in SrcGens
    /\ dDestGen \in [Dests -> DestGens]
    /\ dNextOp \in 1..(NumOps + 1)
    /\ vSlot \in {"Absent", "Received", "AdmissionPending", "AwaitPubrel",
                  "Refused"}
    /\ vOp \in Ops \cup {NoOp}
    /\ vKnow \in [Ops -> {"None", "Prepared", "Submitted", "Unknown",
                          "Committed", "Rejected"}]
    /\ vPrep \subseteq [op : Ops, recipe : Recipes]
    /\ vDeadline \in [Ops -> BOOLEAN]
    /\ vFault \in BOOLEAN
    /\ tCmd \subseteq Commands
    /\ tResolve \subseteq Ops /\ tFencing \subseteq Ops
    /\ tResult \subseteq Results
    /\ cGen \in ConnGens /\ cOpen \in BOOLEAN
    /\ xIn \in BOOLEAN
    /\ xAuth \subseteq Answers /\ xWire \subseteq Answers
    /\ kPhase \in {"idle", "sent", "acked", "refused"}
    /\ kExch \in SrcGens
    /\ kSentGen \in 0..(MaxCrash + MaxConnReset + MaxSrcReset + 1)
    /\ kGen \in ConnGens
    /\ proc \in {"up", "down"}
    /\ eCrash \in 0..MaxCrash /\ eConnReset \in 0..MaxConnReset
    /\ eDestReset \in 0..MaxDestReset /\ eSettle \in 0..MaxSettle
    /\ eLoss \in 0..MaxLoss /\ eFence \in 0..MaxFenceFail
    /\ hAdmit \subseteq [sgen : SrcGens, op : Ops, epoch : Epochs]
    /\ hObl \subseteq Obligations
    /\ hRefused \subseteq Ops /\ hKnown \subseteq Ops
    /\ hRaces \subseteq {"CommitBeforeFence", "FenceBeforeCommand",
                         "EpochClosed", "StaleSource", "StaleTarget",
                         "NoCapacity"} \X Ops

Init ==
    /\ dEpoch = 1 /\ dCommitted = {} /\ dFenced = {} /\ dPub = {} /\ dSlot = {}
    /\ dObl = {}
    /\ dUsed = [d \in Dests |-> 0]
    /\ dSrcGen = 1 /\ dDestGen = [d \in Dests |-> 1] /\ dNextOp = 1
    /\ vSlot = "Absent" /\ vOp = NoOp
    /\ vKnow = [o \in Ops |-> "None"] /\ vPrep = {}
    /\ vDeadline = [o \in Ops |-> FALSE] /\ vFault = FALSE
    /\ tCmd = {} /\ tResolve = {} /\ tFencing = {} /\ tResult = {}
    /\ cGen = 1 /\ cOpen = TRUE
    /\ xIn = FALSE /\ xAuth = {} /\ xWire = {}
    /\ kPhase = "idle" /\ kExch = 1 /\ kSentGen = 0 /\ kGen = 1
    /\ proc = "up" /\ eCrash = 0 /\ eConnReset = 0 /\ eDestReset = 0
    /\ eSettle = 0 /\ eLoss = 0 /\ eFence = 0
    /\ hAdmit = {} /\ hObl = {} /\ hRefused = {} /\ hKnown = {} /\ hRaces = {}

(***************************************************************************)
(* Client.                                                                 *)
(***************************************************************************)
ClientPublish ==
    /\ kPhase = "idle" /\ cOpen /\ kGen = cGen
    /\ xIn' = TRUE /\ kPhase' = "sent" /\ kSentGen' = kGen
    /\ UNCHANGED <<durable, volatile, internal, conn, xAuth, xWire, kExch,
                   kGen, env, history>>

\* With its session present: after a crash or a connection reset.
ClientReconnect ==
    /\ proc = "up" /\ ~cOpen
    /\ cGen' = cGen + 1 /\ cOpen' = TRUE /\ kGen' = cGen + 1
    /\ UNCHANGED <<durable, volatile, internal, external, kPhase, kExch,
                   kSentGen, env, history>>

ClientResend ==
    /\ kPhase = "sent" /\ cOpen /\ kGen = cGen /\ kSentGen < kGen
    /\ xIn' = TRUE /\ kSentGen' = kGen
    /\ UNCHANGED <<durable, volatile, internal, conn, xAuth, xWire, kPhase,
                   kExch, kGen, env, history>>

ClientObserve(f) ==
    /\ f \in xWire /\ f.gen = kGen
    /\ xWire' = xWire \ {f}
    /\ kPhase' = IF f.kind = "PUBREC" THEN "acked" ELSE "refused"
    /\ UNCHANGED <<durable, volatile, internal, conn, xIn, xAuth, kExch,
                   kSentGen, kGen, env, history>>

\* The producer connects again with a clean start: its session is another
\* session, its inbound state is removed and the Packet Identifier may
\* serve a new exchange. Obligations already committed are kept. The core
\* detaches the old admission from the slot; its resolution continues.
SourceSessionReset ==
    /\ proc = "up" /\ dSrcGen <= MaxSrcReset
    /\ dSrcGen' = dSrcGen + 1
    /\ dSlot' = {}
    /\ IF Mutation = "SourceResetDropsObligations"
       THEN /\ dObl' = {x \in dObl : x.op \notin {s.op : s \in dSlot}}
            /\ dUsed' = [d \in Dests |->
                  dUsed[d] - Cardinality({x \in dObl : x.dest = d /\
                                            x.op \in {s.op : s \in dSlot}})]
       ELSE UNCHANGED <<dObl, dUsed>>
    /\ IF Mutation = "ResetKeepsAttachment"
       THEN UNCHANGED <<vSlot, vOp>>
       ELSE vSlot' = "Absent" /\ vOp' = NoOp
    /\ cGen' = cGen + 1 /\ cOpen' = TRUE /\ kGen' = cGen + 1
    /\ xIn' = FALSE
    /\ kPhase' = "idle" /\ kExch' = dSrcGen + 1 /\ kSentGen' = 0
    /\ UNCHANGED <<dEpoch, dCommitted, dFenced, dPub, dDestGen, dNextOp,
                   vKnow, vPrep, vDeadline, vFault, internal, xAuth, xWire,
                   env, history>>

\* A destination connects again with a clean start: its obligations go
\* with the old session and its capacity is free again.
DestSessionReset(d) ==
    /\ proc = "up" /\ eDestReset < MaxDestReset
    /\ eDestReset' = eDestReset + 1
    /\ dDestGen' = [dDestGen EXCEPT ![d] = dDestGen[d] + 1]
    /\ dObl' = {x \in dObl : x.dest # d}
    /\ dUsed' = [dUsed EXCEPT ![d] = 0]
    /\ UNCHANGED <<dEpoch, dCommitted, dFenced, dPub, dSlot, dSrcGen, dNextOp,
                   volatile, internal, conn, external, client, proc, eCrash,
                   eConnReset, eSettle, eLoss, eFence, history>>

\* A destination received and acknowledged a publication: the obligation
\* is settled and its capacity is free again. Delivery itself is outside
\* the model; the verifier stops expecting a settled obligation.
Settle(x) ==
    /\ x \in dObl /\ eSettle < MaxSettle
    /\ eSettle' = eSettle + 1
    /\ dObl' = dObl \ {x}
    /\ dUsed' = [dUsed EXCEPT ![x.dest] = dUsed[x.dest] - 1]
    /\ hObl' = hObl \ {x}
    /\ UNCHANGED <<dEpoch, dCommitted, dFenced, dPub, dSlot, dSrcGen, dDestGen,
                   dNextOp, volatile, internal, conn, external, client, proc,
                   eCrash, eConnReset, eDestReset, eLoss, eFence, hAdmit,
                   hRefused,
                   hKnown, hRaces>>

(***************************************************************************)
(* Core.                                                                   *)
(***************************************************************************)
Answer(kind) ==
    xAuth' = IF cOpen THEN xAuth \cup {[gen |-> cGen, kind |-> kind]} ELSE xAuth

Receive ==
    /\ proc = "up" /\ cOpen /\ xIn
    /\ xIn' = FALSE
    /\ \/ /\ vSlot = "Absent"
          /\ vSlot' = "Received"
          /\ UNCHANGED xAuth
       \* A replay while the admission is unresolved: no command and no
       \* acknowledgement; the pending resolution continues (record 0001).
       \/ /\ vSlot = "Received"
          /\ UNCHANGED <<vSlot, xAuth>>
       \/ /\ vSlot = "AdmissionPending" /\ Mutation # "ReplayReadmits"
          /\ UNCHANGED <<vSlot, xAuth>>
       \/ /\ vSlot = "AdmissionPending" /\ Mutation = "ReplayReadmits"
          /\ vSlot' = "Received"
          /\ UNCHANGED xAuth
       \/ /\ vSlot = "AwaitPubrel"
          /\ Answer("PUBREC")
          /\ UNCHANGED vSlot
       \/ /\ vSlot = "Refused"
          /\ Answer("NACK")
          /\ UNCHANGED vSlot
    /\ UNCHANGED <<durable, vOp, vKnow, vPrep, vDeadline, vFault, internal,
                   conn, xWire, client, env, history>>

\* The recipe freezes the source exchange, the epoch and the targets with
\* the generations the routes give at this moment.
Prepare ==
    /\ proc = "up" /\ vSlot = "Received" /\ dNextOp <= NumOps
    /\ LET o == dNextOp
           r == [sgen |-> dSrcGen, epoch |-> dEpoch,
                 targets |-> {[dest |-> d, gen |-> dDestGen[d]] : d \in Dests}]
       IN /\ vSlot' = "AdmissionPending"
          /\ vOp' = o
          /\ vKnow' = [vKnow EXCEPT ![o] = "Prepared"]
          /\ vPrep' = vPrep \cup {[op |-> o, recipe |-> r]}
          /\ dNextOp' = o + 1
    /\ UNCHANGED <<dEpoch, dCommitted, dFenced, dPub, dSlot, dObl, dUsed,
                   dSrcGen, dDestGen, vDeadline, vFault, internal, conn,
                   external, client, env, history>>

Submit(o) ==
    /\ proc = "up" /\ vKnow[o] = "Prepared"
    /\ tCmd' = tCmd \cup {[op |-> o, recipe |-> RecipeOf(o)]}
    /\ vKnow' = [vKnow EXCEPT ![o] = "Submitted"]
    /\ vDeadline' = [vDeadline EXCEPT ![o] = TRUE]
    /\ UNCHANGED <<durable, vSlot, vOp, vPrep, vFault, tResolve, tFencing,
                   tResult, conn, external, client, env, history>>

\* The same identity and the same frozen recipe, presented again.
Represent(o) ==
    /\ proc = "up" /\ vKnow[o] = "Unknown"
    /\ tCmd' = tCmd \cup {[op |-> o, recipe |-> RecipeOf(o)]}
    /\ UNCHANGED <<durable, volatile, tResolve, tFencing, tResult, conn,
                   external, client, env, history>>

(***************************************************************************)
(* Store. It runs whether the core is up or not: a command it accepted     *)
(* outlives a crash of the core.                                           *)
(***************************************************************************)
Ready(o, out) == tResult' = tResult \cup {[op |-> o, out |-> out]}

\* The result reaches the core only while the core that asked is alive.
Report(o, out) == IF proc = "up" THEN Ready(o, out) ELSE UNCHANGED tResult

EpochOpen(r) == r.epoch = dEpoch \/ Mutation = "OldEpochExecutes"

SourceCurrent(r) == r.sgen = dSrcGen /\ ~\E s \in dSlot : s.sgen = r.sgen

TargetsCurrent(r) ==
    \/ \A t \in r.targets : t.gen = dDestGen[t.dest]
    \/ Mutation = "StaleTargetAccepted"

HasCapacity(r) == \A t \in r.targets : dUsed[t.dest] < Capacity

\* The first reason, in the order the store checks them.
RefusalCause(r) ==
    IF ~EpochOpen(r) THEN "EpochClosed"
    ELSE IF ~SourceCurrent(r) THEN "StaleSource"
    ELSE IF ~TargetsCurrent(r) THEN "StaleTarget"
    ELSE "NoCapacity"

Commit(o, r) ==
    LET new == {[op |-> o, dest |-> t.dest, gen |-> t.gen] : t \in r.targets} IN
    /\ dCommitted' = dCommitted \cup {o}
    /\ dPub' = dPub \cup {[op |-> o, recipe |-> r]}
    /\ dSlot' = dSlot \cup {[sgen |-> r.sgen, op |-> o]}
    /\ dObl' = dObl \cup new
    /\ dUsed' = [d \in Dests |->
                    dUsed[d] + Cardinality({t \in r.targets : t.dest = d})]
    /\ hAdmit' = hAdmit \cup {[sgen |-> r.sgen, op |-> o, epoch |-> dEpoch]}
    /\ hObl' = hObl \cup new
    /\ UNCHANGED dFenced

ApplyCommand(c) ==
    LET o == c.op
        r == c.recipe
    IN
    /\ c \in tCmd
    /\ tCmd' = tCmd \ {c}
    /\ \/ /\ o \in dCommitted
          /\ Report(o, IF [op |-> o, recipe |-> r] \in dPub
                       THEN "Committed" ELSE "Mismatch")
          /\ UNCHANGED <<durable, hAdmit, hObl, hRaces>>
       \/ /\ o \notin dCommitted /\ o \in dFenced
          /\ Report(o, "RejectedCertain")
          /\ hRaces' = hRaces \cup {<<"FenceBeforeCommand", o>>}
          /\ UNCHANGED <<durable, hAdmit, hObl>>
       \/ /\ o \notin dCommitted /\ o \notin dFenced
          /\ EpochOpen(r) /\ SourceCurrent(r) /\ TargetsCurrent(r)
          /\ HasCapacity(r)
          /\ Commit(o, r)
          /\ Report(o, "Committed")
          /\ UNCHANGED <<dEpoch, dSrcGen, dDestGen, dNextOp, hRaces>>
       \* A closed epoch, a stale source or target, or a full capacity: a
       \* certain rejection, durable, that stays after quota is freed.
       \/ /\ o \notin dCommitted /\ o \notin dFenced
          /\ ~(EpochOpen(r) /\ SourceCurrent(r) /\ TargetsCurrent(r)
               /\ HasCapacity(r))
          /\ dFenced' = IF Mutation = "RefusalNotDurable" THEN dFenced
                        ELSE dFenced \cup {o}
          /\ Report(o, "RejectedCertain")
          /\ hRaces' = hRaces \cup {<<RefusalCause(r), o>>}
          /\ UNCHANGED <<dEpoch, dCommitted, dPub, dSlot, dObl, dUsed,
                         dSrcGen, dDestGen, dNextOp, hAdmit, hObl>>
    /\ UNCHANGED <<volatile, tResolve, tFencing, conn, external, client,
                   env, hRefused, hKnown>>

\* ResolveAndFence, first step: the resolver reads the identity. If it is
\* neither committed nor fenced, it has observed an absence and nothing
\* more.
ResolveRead(o) ==
    /\ proc = "up" /\ o \in tResolve
    /\ tResolve' = tResolve \ {o}
    /\ \/ /\ o \in dCommitted
          /\ Ready(o, "Committed")
          /\ UNCHANGED tFencing
       \/ /\ o \notin dCommitted /\ o \in dFenced
          /\ Ready(o, "RejectedCertain")
          /\ UNCHANGED tFencing
       \/ /\ o \notin dCommitted /\ o \notin dFenced
          /\ tFencing' = tFencing \cup {o}
          /\ UNCHANGED tResult
    /\ UNCHANGED <<durable, volatile, tCmd, conn, external, client, env,
                   history>>

\* Second step, ordered with the application of commands: the fence is
\* established only if the identity is still absent. A command applied in
\* between wins and its result is returned (QABS-A); otherwise the fence
\* wins and the command will be refused (QABS-B).
FenceEstablish(o) ==
    /\ proc = "up" /\ o \in tFencing
    /\ tFencing' = tFencing \ {o}
    /\ IF o \in dCommitted /\ Mutation # "FenceOnStaleRead"
       THEN /\ Ready(o, "Committed")
            /\ hRaces' = hRaces \cup {<<"CommitBeforeFence", o>>}
            /\ UNCHANGED dFenced
       ELSE /\ dFenced' = dFenced \cup {o}
            /\ Ready(o, "RejectedCertain")
            /\ UNCHANGED hRaces
    /\ UNCHANGED <<dEpoch, dCommitted, dPub, dSlot, dObl, dUsed, dSrcGen,
                   dDestGen, dNextOp, volatile, tCmd, tResolve, conn,
                   external, client, env, hAdmit, hObl, hRefused, hKnown>>

\* The write of the fence is refused, a fault of the bench: the outcome
\* stays unknown (QABS-C). Nothing else leaves an absence unresolved.
FenceAbandon(o) ==
    /\ proc = "up" /\ o \in tFencing /\ eFence < MaxFenceFail
    /\ eFence' = eFence + 1
    /\ tFencing' = tFencing \ {o}
    /\ Ready(o, "PendingOrUnknown")
    /\ UNCHANGED <<durable, volatile, tCmd, tResolve, conn, external, client,
                   proc, eCrash, eConnReset, eDestReset, eSettle, eLoss,
                   history>>

LoseResult(r) ==
    /\ r \in tResult /\ eLoss < MaxLoss
    /\ tResult' = tResult \ {r}
    /\ eLoss' = eLoss + 1
    /\ UNCHANGED <<durable, volatile, tCmd, tResolve, tFencing, conn,
                   external, client, proc, eCrash, eConnReset, eDestReset,
                   eSettle, eFence, history>>

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
       \/ /\ \/ k \in {"None", "Prepared"}
             \/ k = "Committed" /\ r.out \in {"Committed", "PendingOrUnknown"}
             \/ k = "Rejected" /\ r.out \in {"RejectedCertain",
                                             "PendingOrUnknown"}
          /\ UNCHANGED <<vKnow, vDeadline, vFault, hKnown, hRefused>>
       \/ /\ \/ k \in {"Submitted", "Unknown"} /\ r.out = "Mismatch"
             \/ k = "Committed" /\ r.out \in {"RejectedCertain", "Mismatch"}
             \/ k = "Rejected" /\ r.out \in {"Committed", "Mismatch"}
          /\ vFault' = TRUE
          /\ UNCHANGED <<vKnow, vDeadline, hKnown, hRefused>>
    /\ UNCHANGED <<durable, vSlot, vOp, vPrep, tCmd, tResolve, tFencing,
                   conn, external, client, env, hAdmit, hObl, hRaces>>

Timeout(o) ==
    /\ proc = "up" /\ vDeadline[o] /\ vKnow[o] \in {"Submitted", "Unknown"}
    /\ vKnow' = [vKnow EXCEPT ![o] = "Unknown"]
    /\ tResolve' = tResolve \cup {o}
    /\ UNCHANGED <<durable, vSlot, vOp, vPrep, vDeadline, vFault, tCmd,
                   tFencing, tResult, conn, external, client, env, history>>

\* PUBREC goes to the exchange the identity is attached to, on the
\* connection that is current now. An identity detached by a reset of the
\* session is reconciled and answers nothing.
Authorize ==
    /\ proc = "up" /\ vSlot = "AdmissionPending"
    /\ vKnow[vOp] = "Committed"
    /\ vSlot' = "AwaitPubrel"
    /\ Answer("PUBREC")
    /\ UNCHANGED <<durable, vOp, vKnow, vPrep, vDeadline, vFault, internal,
                   conn, xIn, xWire, client, env, history>>

Refuse ==
    /\ proc = "up" /\ vSlot = "AdmissionPending"
    /\ vKnow[vOp] = "Rejected"
    /\ vSlot' = "Refused"
    /\ Answer("NACK")
    /\ UNCHANGED <<durable, vOp, vKnow, vPrep, vDeadline, vFault, internal,
                   conn, xIn, xWire, client, env, history>>

SubmitFrame(f) ==
    /\ proc = "up" /\ f \in xAuth
    /\ xAuth' = xAuth \ {f}
    /\ xWire' = IF cOpen /\ f.gen = cGen THEN xWire \cup {f} ELSE xWire
    /\ UNCHANGED <<durable, volatile, internal, conn, xIn, client, env,
                   history>>

(***************************************************************************)
(* Faults.                                                                 *)
(***************************************************************************)
ConnectionReset ==
    /\ proc = "up" /\ cOpen /\ eConnReset < MaxConnReset
    /\ eConnReset' = eConnReset + 1
    /\ cOpen' = FALSE
    /\ xIn' = FALSE
    /\ UNCHANGED <<durable, volatile, internal, cGen, xAuth, xWire, client,
                   proc, eCrash, eDestReset, eSettle, eLoss, eFence, history>>

\* The core dies. The store keeps the commands it accepted.
Crash ==
    /\ proc = "up" /\ eCrash < MaxCrash
    /\ proc' = "down" /\ eCrash' = eCrash + 1
    /\ vSlot' = "Absent" /\ vOp' = NoOp
    /\ vKnow' = [o \in Ops |-> "None"]
    /\ vPrep' = {}
    /\ vDeadline' = [o \in Ops |-> FALSE]
    /\ vFault' = FALSE
    /\ tResolve' = {} /\ tFencing' = {} /\ tResult' = {}
    /\ xIn' = FALSE /\ xAuth' = {}
    /\ cOpen' = FALSE
    /\ UNCHANGED <<durable, tCmd, cGen, xWire, client, eConnReset,
                   eDestReset, eSettle, eLoss, eFence, history>>

\* Recovery closes the old epoch, then reads the durable data. Closing an
\* epoch forbids old executions; it does not turn an unknown outcome into
\* a rejection, and nothing is learnt about an identity that left no fact.
Recover ==
    /\ proc = "down"
    /\ proc' = "up"
    /\ dEpoch' = dEpoch + 1
    /\ vSlot' = IF \E s \in dSlot : s.sgen = dSrcGen
                THEN "AwaitPubrel" ELSE "Absent"
    /\ vOp' = IF \E s \in dSlot : s.sgen = dSrcGen
              THEN (CHOOSE s \in dSlot : s.sgen = dSrcGen).op ELSE NoOp
    /\ vKnow' = [o \in Ops |-> IF o \in dCommitted THEN "Committed" ELSE "None"]
    /\ vPrep' = dPub
    /\ hKnown' = hKnown \cup dCommitted
    /\ UNCHANGED <<dCommitted, dFenced, dPub, dSlot, dObl, dUsed, dSrcGen,
                   dDestGen, dNextOp, vDeadline, vFault, internal, conn,
                   external, client, eCrash, eConnReset, eDestReset, eSettle,
                   eLoss, eFence, hAdmit, hObl, hRefused, hRaces>>

Suspended == vSlot = "Received" /\ dNextOp > NumOps

Terminal ==
    /\ proc = "up" /\ cOpen
    /\ tCmd = {} /\ tResolve = {} /\ tFencing = {} /\ tResult = {}
    /\ xAuth = {}
    /\ \/ kPhase \in {"acked", "refused"}
       \/ kPhase = "sent" /\ Suspended

Quiescent == Terminal /\ UNCHANGED vars

Next ==
    \/ ClientPublish \/ ClientReconnect \/ ClientResend
    \/ SourceSessionReset
    \/ \E d \in Dests : DestSessionReset(d)
    \/ \E x \in dObl : Settle(x)
    \/ Receive \/ Prepare \/ Authorize \/ Refuse
    \/ \E f \in xWire : ClientObserve(f)
    \/ \E f \in xAuth : SubmitFrame(f)
    \/ \E o \in Ops : \/ Submit(o) \/ Represent(o) \/ Timeout(o)
                      \/ ResolveRead(o) \/ FenceEstablish(o) \/ FenceAbandon(o)
    \/ \E c \in tCmd : ApplyCommand(c)
    \/ \E r \in tResult : DeliverResult(r) \/ LoseResult(r)
    \/ ConnectionReset
    \/ Crash
    \/ Recover
    \/ Quiescent

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Safety. No fairness is assumed.                                         *)
(***************************************************************************)

\* One admission per exchange; a reset of the session opens another one.
OneAdmissionPerExchange ==
    \A g \in SrcGens : Cardinality({a \in hAdmit : a.sgen = g}) <= 1

\* A committed identity created an obligation for every target of its
\* recipe: each is still there unless its destination was reset or it was
\* settled. Any other identity created none.
AtomicObligations ==
    \A o \in Ops :
        IF o \in dCommitted
        THEN \E x \in dPub :
                /\ x.op = o
                /\ eSettle = 0 =>
                     \A t \in x.recipe.targets :
                        t.gen = dDestGen[t.dest] =>
                            [op |-> o, dest |-> t.dest, gen |-> t.gen] \in dObl
        ELSE /\ ~\E x \in dObl : x.op = o
             /\ ~\E x \in dPub : x.op = o

\* An identity is never both committed and fenced.
DecisionExclusive == dCommitted \cap dFenced = {}

\* A rejection the core relied on excludes any execution, also after the
\* capacity it lacked is free again.
RefusalNeverExecutes == hRefused \cap dCommitted = {}

\* The client holds an acknowledgement only for an exchange that is
\* admitted, and a refusal only for one that is not.
AckedIsAdmitted ==
    kPhase = "acked" => \E a \in hAdmit : a.sgen = kExch

RefusedIsNotAdmitted ==
    kPhase = "refused" => ~\E a \in hAdmit : a.sgen = kExch

\* No command executes in an epoch other than the one it was prepared in.
NoExecutionInClosedEpoch ==
    \A a \in hAdmit : \E x \in dPub : x.op = a.op /\ x.recipe.epoch = a.epoch

\* No obligation exists for a destination session that is gone.
NoStaleTarget == \A x \in dObl : x.gen = dDestGen[x.dest]

\* An obligation for a session that still exists is never removed: the
\* reset of the source does not take away what was committed to others.
ObligationsConserved ==
    \A x \in hObl : x.gen = dDestGen[x.dest] => x \in dObl

AccountsMatch ==
    \A d \in Dests : dUsed[d] = Cardinality({x \in dObl : x.dest = d})

WithinCapacity == \A d \in Dests : dUsed[d] <= Capacity

KnowledgeSound ==
    /\ ~vFault
    /\ \A o \in Ops : vKnow[o] = "Committed" => o \in dCommitted

(***************************************************************************)
(* Non-vacuity: each predicate is the negation of an invariant that TLC    *)
(* must report as violated.                                                *)
(***************************************************************************)

\* A PUBLISH replayed after a connection reset while its outcome was
\* unknown, answered on the new connection by the one admission.
ReachReplayWhileUnknown ==
    /\ eCrash = 0 /\ eConnReset > 0 /\ dNextOp = 2
    /\ kPhase = "acked" /\ kSentGen = 2 /\ kExch = 1
    /\ \E r \in tResult : r.op = 1 /\ r.out = "PendingOrUnknown"

\* QABS-A: the resolver observed an absence, the command committed, the
\* fence step returned the commit and PUBREC was observed.
ReachCommitBeforeFence ==
    /\ kPhase = "acked" /\ vOp # NoOp /\ vOp \in dCommitted
    /\ RacesOf(vOp) = {"CommitBeforeFence"}

\* QABS-B: the fence won, the delayed command was then refused without
\* effect, and the refusal was observed.
ReachFenceBeforeCommand ==
    /\ kPhase = "refused" /\ vOp # NoOp /\ vOp \in dFenced
    /\ RacesOf(vOp) = {"FenceBeforeCommand"} /\ dCommitted = {}

\* The second exchange finds the capacity used by the first and is refused
\* without any resolution being involved.
ReachCapacityRefusal ==
    /\ kPhase = "refused" /\ kExch = 2
    /\ \A d \in Dests : dUsed[d] = Capacity
    /\ 1 \in dCommitted /\ 2 \in dFenced /\ vOp = 2
    /\ RacesOf(2) = {"NoCapacity"}

\* A command prepared for a destination session that was then reset is
\* refused.
ReachStaleTargetRefusal ==
    /\ eDestReset > 0 /\ kPhase = "refused" /\ dNextOp = 2 /\ 1 \in dFenced
    /\ vOp = 1 /\ RacesOf(1) = {"StaleTarget"}

\* A command of the first epoch refused in the second, while a new
\* admission of the same exchange commits.
ReachEpochClosedRefusal ==
    /\ dEpoch = 2 /\ 1 \in dFenced /\ 2 \in dCommitted
    /\ kPhase = "acked" /\ kExch = 1
    /\ vOp = 2 /\ RacesOf(1) = {"EpochClosed"} /\ RacesOf(2) = {}

\* The source session was reset after a commit: the obligations stay and
\* a new exchange is admitted on the same Packet Identifier.
ReachSecondExchange ==
    /\ dSrcGen = 2 /\ kPhase = "acked" /\ kExch = 2
    /\ Cardinality(hAdmit) = 2
    /\ eSettle = 0 /\ eDestReset = 0
    /\ \A a \in hAdmit : \A d \in Dests : \E x \in dObl : x.op = a.op /\ x.dest = d

NoReplayWhileUnknown  == ~ReachReplayWhileUnknown
NoCommitBeforeFence   == ~ReachCommitBeforeFence
NoFenceBeforeCommand  == ~ReachFenceBeforeCommand
NoCapacityRefusal     == ~ReachCapacityRefusal
NoStaleTargetRefusal  == ~ReachStaleTargetRefusal
NoEpochClosedRefusal  == ~ReachEpochClosedRefusal
NoSecondExchange      == ~ReachSecondExchange

=============================================================================
