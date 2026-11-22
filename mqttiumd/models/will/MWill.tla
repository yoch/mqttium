---------------------------- MODULE MWill ----------------------------
EXTENDS Naturals, Integers, FiniteSets, TLC

CONSTANTS MaxOpens, MaxCrashes, Mutation
VARIABLES d, v, h
vars == <<d, v, h>>

Occurrences == 1..MaxOpens
Own == 1
Other == 2
Targets == {Own, Other}
Pairs == Occurrences \X Targets
ReferenceCapacity == 3
Bit(b) == IF b THEN 1 ELSE 0
Row(w) == w \in {"installed", "pending"}
Rows(ws) == {o \in Occurrences : Row(ws[o])}
Owed(os, o) == {p \in os : p[1] = o}
ExpectedRefs(ws, os) == [o \in Occurrences |->
    Bit(Row(ws[o])) + Cardinality(Owed(os, o))]
Payloads(rs) == {o \in Occurrences : rs[o] > 0}
TotalRefs(rs, pressure) ==
    (IF MaxOpens = 1 THEN rs[1] ELSE rs[1] + rs[2]) + Bit(pressure)

Init ==
    /\ d = [current |-> 0,
             will |-> [o \in Occurrences |-> "absent"],
             openReceipt |-> [o \in Occurrences |-> FALSE],
             admissionReceipt |-> [o \in Occurrences |-> "none"],
             sourceAlive |-> {}, obligations |-> {},
             references |-> [o \in Occurrences |-> 0], payloads |-> {},
             pressure |-> TRUE]
    /\ v = [running |-> TRUE, crashes |-> 0,
             open |-> [o \in Occurrences |-> "idle"],
             alive |-> [o \in Occurrences |-> FALSE],
             close |-> [o \in Occurrences |-> "none"],
             admission |-> [o \in Occurrences |-> "idle"],
             repeat |-> [o \in Occurrences |-> FALSE],
             oldCloseHandled |-> FALSE]
    /\ h = [accepted |-> {}, cancelled |-> {}, connack |-> {},
             admissions |-> [o \in Occurrences |-> 0],
             finished |-> {}, cancelledTargets |-> {},
             sourceClosed |-> {}, wrongClose |-> FALSE,
             recoveredPending |-> {}, resolved |-> {}, ignoredOld |-> FALSE]

SubmitOpen(o) ==
    /\ v.running /\ v.open[o] = "idle"
    /\ (o = 1 \/ d.current = o - 1)
    /\ v' = [v EXCEPT !.open[o] = "submitted", !.alive[o] = TRUE,
                 !.close[o] = "none"]
    /\ UNCHANGED <<d, h>>

OpenUnknown(o) ==
    /\ v.running /\ v.open[o] = "submitted"
    /\ v' = [v EXCEPT !.open[o] = "unknown"]
    /\ UNCHANGED <<d, h>>

\* One accepted Open has one immutable occurrence/operation identity. A
\* takeover marks only the previous attachment's installed Will pending.
CommitOpen(o) ==
    /\ v.running /\ v.open[o] \in {"submitted", "unknown"}
    /\ ~d.openReceipt[o]
    /\ \A i \in Occurrences :
         i = d.current /\ v.close[i] = "normal" => d.will[i] # "installed"
    /\ TotalRefs(d.references, d.pressure) < ReferenceCapacity
    /\ LET old == d.current
           ws == [i \in Occurrences |->
               IF i = o THEN "installed"
               ELSE IF i = old /\ d.will[i] = "installed"
                    THEN "pending" ELSE d.will[i]]
           rs == [d.references EXCEPT ![o] = @ + 1]
       IN /\ d' = [d EXCEPT !.current = o, !.will = ws,
                     !.openReceipt[o] = TRUE,
                     !.sourceAlive = @ \cup {o},
                     !.references = rs, !.payloads = Payloads(rs)]
          /\ v' = [v EXCEPT !.open[o] = "unknown",
                       !.alive = [i \in Occurrences |->
                           IF i = old /\ i # o THEN FALSE ELSE v.alive[i]],
                       !.close = [i \in Occurrences |->
                           IF i = old /\ i # o /\ v.close[i] = "none"
                           THEN "abnormal" ELSE v.close[i]]]
    /\ h' = [h EXCEPT !.accepted = @ \cup {o}]

RejectOpen(o) ==
    /\ v.running /\ v.open[o] \in {"submitted", "unknown"}
    /\ ~d.openReceipt[o]
    /\ v' = [v EXCEPT !.open[o] = "rejected", !.alive[o] = FALSE]
    /\ UNCHANGED <<d, h>>

ResolveOpen(o) ==
    /\ v.running /\ v.open[o] \in {"submitted", "unknown"}
    /\ d.openReceipt[o]
    /\ v' = [v EXCEPT !.open[o] = "known"]
    /\ UNCHANGED <<d, h>>

ApplyOpen(o) ==
    /\ v.running /\ v.open[o] = "known"
    /\ v' = [v EXCEPT !.open[o] = "applied"]
    /\ h' = [h EXCEPT !.connack =
                 IF v.alive[o] THEN @ \cup {o} ELSE @]
    /\ UNCHANGED d

PrematureConnack(o) ==
    /\ Mutation = "early-connack"
    /\ v.running /\ v.open[o] \in {"submitted", "unknown"}
    /\ ~d.openReceipt[o] /\ o \notin h.connack
    /\ h' = [h EXCEPT !.connack = @ \cup {o}]
    /\ UNCHANGED <<d, v>>

\* The observed close can precede Open's result, including result application.
\* Its immutable occurrence is retained; it is never looked up by ClientID.
ObserveClose(o, mode) ==
    /\ v.running /\ v.alive[o]
    /\ mode \in {"normal", "abnormal"}
    /\ v' = [v EXCEPT !.alive[o] = FALSE, !.close[o] = mode]
    /\ UNCHANGED <<d, h>>

CommitClose(o) ==
    /\ v.running /\ d.openReceipt[o] /\ ~v.alive[o]
    /\ v.close[o] \in {"normal", "abnormal"}
    /\ d.will[o] = "installed"
    /\ LET cancel == v.close[o] = "normal"
           remove == cancel /\ Mutation # "publish-normal"
           rs == [d.references EXCEPT ![o] = @ - Bit(remove)]
       IN d' = [d EXCEPT !.will[o] = IF remove THEN "absent" ELSE "pending",
                  !.references = rs, !.payloads = Payloads(rs)]
    /\ h' = [h EXCEPT !.cancelled =
                 IF v.close[o] = "normal" THEN @ \cup {o} ELSE @]
    /\ UNCHANGED v

SubmitAdmission(o) ==
    /\ v.running /\ d.will[o] = "pending"
    /\ d.admissionReceipt[o] = "none" /\ v.admission[o] = "idle"
    /\ v' = [v EXCEPT !.admission[o] = "submitted"]
    /\ UNCHANGED <<d, h>>

AdmissionUnknown(o) ==
    /\ v.running /\ v.admission[o] = "submitted"
    /\ v' = [v EXCEPT !.admission[o] = "unknown"]
    /\ UNCHANGED <<d, h>>

\* Atomic admission transfers the registered payload reference to the fixed
\* target obligations. Cleanup may already have removed the source's own
\* destination; the independent target remains eligible. A full budget
\* leaves the durable intention pending, even after source cleanup.
CommitAdmission(o) ==
    /\ v.running /\ d.will[o] = "pending"
    /\ d.admissionReceipt[o] = "none"
    /\ v.admission[o] \in {"submitted", "unknown"}
    /\ LET selected == IF o \in d.sourceAlive THEN Targets ELSE {Other}
           added == Cardinality(selected) - 1
           os == d.obligations \cup {<<o, t>> : t \in selected}
           rs == [d.references EXCEPT ![o] = @ + added]
       IN /\ TotalRefs(d.references, d.pressure) + added <= ReferenceCapacity
          /\ d' = [d EXCEPT !.will[o] = "absent", !.obligations = os,
                      !.references = rs, !.payloads = Payloads(rs),
                      !.admissionReceipt[o] = "committed"]
    /\ v' = [v EXCEPT !.admission[o] = "unknown"]
    /\ h' = [h EXCEPT !.admissions[o] = @ + 1]

ResolveAdmission(o) ==
    /\ v.running /\ v.admission[o] \in {"submitted", "unknown"}
    /\ d.admissionReceipt[o] = "committed"
    /\ v' = [v EXCEPT !.admission[o] = "known"]
    /\ h' = [h EXCEPT !.resolved = @ \cup {o}]
    /\ UNCHANGED d

ApplyAdmission(o) ==
    /\ v.running /\ v.admission[o] = "known"
    /\ v' = [v EXCEPT !.admission[o] = "applied"]
    /\ UNCHANGED <<d, h>>

CollectReceipt(o) ==
    /\ v.running /\ v.admission[o] = "applied"
    /\ d.admissionReceipt[o] = "committed"
    /\ d' = [d EXCEPT !.admissionReceipt[o] = "retired"]
    /\ UNCHANGED <<v, h>>

RepeatAdmission(o) ==
    /\ v.running /\ d.admissionReceipt[o] # "none" /\ ~v.repeat[o]
    /\ v' = [v EXCEPT !.repeat[o] = TRUE]
    /\ h' = [h EXCEPT !.admissions[o] =
                 @ + Bit(Mutation = "readmit-lost-result")]
    /\ UNCHANGED d

\* The source may disappear after a durable cancellation or pending decision,
\* before Will admission. Its separate intention retains the payload. This
\* returns only an already admitted own target reference, never another's.
CleanupSource(o) ==
    /\ v.running /\ o \in d.sourceAlive /\ ~v.alive[o]
    /\ d.will[o] # "installed"
    /\ LET removed == {<<o, Own>>} \cap d.obligations
           os == d.obligations \ removed
           rs == [d.references EXCEPT ![o] = @ - Cardinality(removed)]
       IN /\ d' = [d EXCEPT !.sourceAlive = @ \ {o}, !.obligations = os,
                      !.references = rs, !.payloads = Payloads(rs)]
          /\ h' = [h EXCEPT !.sourceClosed = @ \cup {o},
                      !.cancelledTargets = @ \cup removed]
    /\ UNCHANGED v

FinishTarget(o, t) ==
    /\ v.running /\ <<o, t>> \in d.obligations
    /\ LET rs == [d.references EXCEPT ![o] = @ - 1]
       IN d' = [d EXCEPT !.obligations = @ \ {<<o, t>>},
                  !.references = rs, !.payloads = Payloads(rs)]
    /\ h' = [h EXCEPT !.finished = @ \cup {<<o, t>>}]
    /\ UNCHANGED v

ReleasePressure ==
    /\ v.running /\ d.pressure
    /\ d' = [d EXCEPT !.pressure = FALSE]
    /\ UNCHANGED <<v, h>>

\* Duplicate a close already attributed to attachment 1, after attachment 2
\* replaced it. Correct processing is a no-op against the new occurrence.
OldClose ==
    /\ MaxOpens = 2 /\ v.running /\ d.current = 2
    /\ ~v.oldCloseHandled /\ d.will[2] = "installed"
    /\ v' = [v EXCEPT !.oldCloseHandled = TRUE]
    /\ d' = IF Mutation = "rebind-old-close"
             THEN [d EXCEPT !.will[2] = "pending"] ELSE d
    /\ h' = [h EXCEPT !.wrongClose = Mutation = "rebind-old-close",
                 !.ignoredOld = Mutation # "rebind-old-close"]

Crash ==
    /\ v.running /\ v.crashes < MaxCrashes
    /\ v' = [v EXCEPT !.running = FALSE, !.crashes = @ + 1,
                 !.alive = [o \in Occurrences |-> FALSE]]
    /\ UNCHANGED <<d, h>>

Recover ==
    /\ ~v.running
    /\ LET lost == Mutation = "forget-on-crash"
           ws == [o \in Occurrences |->
               IF lost THEN "absent"
               ELSE IF d.will[o] = "installed" THEN "pending" ELSE d.will[o]]
           rs == [o \in Occurrences |->
               d.references[o] - Bit(lost /\ Row(d.will[o]))]
       IN /\ d' = [d EXCEPT !.will = ws, !.references = rs,
                      !.payloads = Payloads(rs)]
          /\ v' = [v EXCEPT !.running = TRUE,
                      !.close = [o \in Occurrences |-> "abnormal"],
                      !.open = [o \in Occurrences |->
                          IF d.openReceipt[o] THEN "applied"
                          ELSE IF v.open[o] = "idle" THEN "idle" ELSE "rejected"],
                      !.admission = [o \in Occurrences |->
                          IF d.admissionReceipt[o] # "none" THEN "applied"
                          ELSE "idle"]]
    /\ h' = [h EXCEPT !.recoveredPending = @ \cup Rows(d.will)]

Idle == UNCHANGED vars
Next == (\E o \in Occurrences : SubmitOpen(o) \/ OpenUnknown(o) \/ CommitOpen(o)
           \/ RejectOpen(o) \/ ResolveOpen(o) \/ ApplyOpen(o) \/ PrematureConnack(o)
           \/ (\E mode \in {"normal", "abnormal"} : ObserveClose(o, mode))
           \/ CommitClose(o) \/ SubmitAdmission(o) \/ AdmissionUnknown(o)
           \/ CommitAdmission(o) \/ ResolveAdmission(o) \/ ApplyAdmission(o)
           \/ CollectReceipt(o) \/ RepeatAdmission(o) \/ CleanupSource(o)
           \/ (\E t \in Targets : FinishTarget(o, t)))
        \/ ReleasePressure \/ OldClose \/ Crash \/ Recover \/ Idle
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ d.current \in 0..MaxOpens
    /\ d.will \in [Occurrences -> {"absent", "installed", "pending"}]
    /\ d.openReceipt \in [Occurrences -> BOOLEAN]
    /\ d.admissionReceipt \in [Occurrences -> {"none", "committed", "retired"}]
    /\ d.sourceAlive \subseteq Occurrences /\ d.obligations \subseteq Pairs
    /\ d.references \in [Occurrences -> 0..3] /\ d.payloads \subseteq Occurrences
    /\ d.pressure \in BOOLEAN /\ v.running \in BOOLEAN
    /\ v.crashes \in 0..MaxCrashes
    /\ v.open \in [Occurrences ->
         {"idle", "submitted", "unknown", "known", "applied", "rejected"}]
    /\ v.alive \in [Occurrences -> BOOLEAN]
    /\ v.close \in [Occurrences -> {"none", "normal", "abnormal"}]
    /\ v.admission \in [Occurrences ->
         {"idle", "submitted", "unknown", "known", "applied"}]
    /\ v.repeat \in [Occurrences -> BOOLEAN] /\ v.oldCloseHandled \in BOOLEAN
    /\ h.accepted \subseteq Occurrences /\ h.cancelled \subseteq Occurrences
    /\ h.connack \subseteq Occurrences /\ h.admissions \in [Occurrences -> 0..2]
    /\ h.finished \subseteq Pairs /\ h.cancelledTargets \subseteq Pairs
    /\ h.sourceClosed \subseteq Occurrences /\ h.wrongClose \in BOOLEAN
    /\ h.recoveredPending \subseteq Occurrences /\ h.resolved \subseteq Occurrences
    /\ h.ignoredOld \in BOOLEAN
ConnackAfterDurableInstall == h.connack \subseteq h.accepted
NoUnacceptedAdmission == \A o \in Occurrences :
    h.admissions[o] > 0 => o \in h.accepted
NoAdmissionAfterCancellation == \A o \in h.cancelled : h.admissions[o] = 0
NoPublicationOnNormalLiveClose == \A o \in Occurrences :
    v.close[o] = "normal" => h.admissions[o] = 0
AtMostOneAdmission == \A o \in Occurrences : h.admissions[o] <= 1
AcceptedWillNotLost == \A o \in h.accepted :
    o \in h.cancelled \/ h.admissions[o] = 1 \/ Row(d.will[o])
NoStaleClose == ~h.wrongClose
IndependentResponsibility == \A o \in Occurrences :
    h.admissions[o] = 1 => <<o, Other>> \in d.obligations \cup h.finished
NoClosedDestination == \A o \in Occurrences :
    <<o, Own>> \in d.obligations => o \in d.sourceAlive
ReferencesExact == d.references = ExpectedRefs(d.will, d.obligations)
PayloadLifetimeExact == d.payloads = Payloads(d.references)
CapacityBounded ==
    /\ Cardinality(Rows(d.will)) <= MaxOpens
    /\ TotalRefs(d.references, d.pressure) <= ReferenceCapacity
NoRecoveryWitness == ~(h.recoveredPending # {} /\
    \E o \in h.recoveredPending : d.will[o] = "pending" /\ h.admissions[o] = 0)
NoRetiredLiveWitness == ~(\E o \in Occurrences :
    d.admissionReceipt[o] = "retired" /\ o \in h.resolved /\ o \in h.sourceClosed
    /\ <<o, Other>> \in d.obligations /\ d.references[o] = 1)
NoIndependentIntentWitness == ~(\E o \in h.sourceClosed :
    d.will[o] = "pending" /\ h.admissions[o] = 0 /\ d.references[o] = 1)
NoTakeoverWitness == ~(MaxOpens = 2 /\ d.current = 2 /\ h.ignoredOld
    /\ h.admissions[1] = 1 /\ 1 \in h.sourceClosed
    /\ <<1, Other>> \in d.obligations /\ d.will[2] = "installed")
=====================================================================
