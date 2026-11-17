----------------------- MODULE MRetainedOwnership -----------------------
EXTENDS Naturals, Integers, FiniteSets, TLC

CONSTANTS MaxCrashes, Mutation
VARIABLES retained, references, payloads, liveOther, replay, frozen, finished,
          replaced, cleared, producerAlive, lastWrite, phase, receipt, retired,
          repeated, executions, running, crashes
vars == <<retained, references, payloads, liveOther, replay, frozen, finished,
          replaced, cleared, producerAlive, lastWrite, phase, receipt, retired,
          repeated, executions, running, crashes>>

Versions == 1..2
ReplayIds == 1..4
SelectedIds == 1..2
UnusedIds == 3..4
Bit(b) == IF b THEN 1 ELSE 0
Alive(refs) == {v \in Versions : refs[v] > 0}
ReplayCount(rs, v) == Cardinality({i \in ReplayIds : rs[i] = v})
ExpectedReferences == [v \in Versions |-> Bit(retained = v)
                       + Bit(liveOther /\ v = 1) + ReplayCount(replay, v)]

Init == /\ retained = 1 /\ references = [v \in Versions |-> IF v = 1 THEN 2 ELSE 0]
        /\ payloads = {1} /\ liveOther = TRUE
        /\ replay = [i \in ReplayIds |-> 0]
        /\ frozen = [i \in ReplayIds |-> 0]
        /\ finished = [i \in ReplayIds |-> FALSE]
        /\ replaced = FALSE /\ cleared = FALSE /\ producerAlive = TRUE
        /\ lastWrite = 1 /\ phase = "idle" /\ receipt = FALSE /\ retired = FALSE
        /\ repeated = FALSE /\ executions = 0
        /\ running = TRUE /\ crashes = 0

SubmitReplay == /\ running /\ phase = "idle" /\ phase' = "submitted"
                /\ UNCHANGED <<retained, references, payloads, liveOther,
                               replay, frozen, finished, replaced, cleared,
                               producerAlive, lastWrite, receipt, retired,
                               repeated, executions, running, crashes>>

\* Selection freezes at this actual commit, not at submission. Two ordered
\* RH0 filter occurrences each select the current topic for subscriber A.
CommitReplay ==
    /\ running /\ phase = "submitted"
    /\ replay' = [i \in ReplayIds |-> IF i \in SelectedIds THEN retained ELSE 0]
    /\ frozen' = replay'
    /\ references' = [v \in Versions |-> references[v] + 2 * Bit(retained = v)]
    /\ payloads' = Alive(references')
    /\ phase' = "unknown" /\ receipt' = TRUE /\ executions' = executions + 1
    /\ UNCHANGED <<retained, liveOther, finished, replaced, cleared,
                   producerAlive, lastWrite, retired, repeated, running, crashes>>

ResolveSameOperation == /\ running /\ phase = "unknown" /\ receipt
                        /\ phase' = "known"
                        /\ UNCHANGED <<retained, references, payloads, liveOther,
                                       replay, frozen, finished, replaced, cleared,
                                       producerAlive, lastWrite, receipt, retired,
                                       repeated, executions, running, crashes>>
ConsumeResult == /\ running /\ phase = "known" /\ phase' = "consumed"
                 /\ UNCHANGED <<retained, references, payloads, liveOther,
                                replay, frozen, finished, replaced, cleared,
                                producerAlive, lastWrite, receipt, retired,
                                repeated, executions, running, crashes>>
CollectReceipt == /\ running /\ phase = "consumed" /\ receipt
                  /\ receipt' = FALSE /\ retired' = TRUE
                  /\ UNCHANGED <<retained, references, payloads, liveOther,
                                 replay, frozen, finished, replaced, cleared,
                                 producerAlive, lastWrite, phase, repeated,
                                 executions, running, crashes>>

\* The bounded reserved range includes unused positions 3/4. After collection,
\* the compact retired guard must forbid materializing them by an old command.
RepeatRetiredReplay ==
    /\ running /\ retired /\ ~repeated
    /\ repeated' = TRUE
    /\ LET broken == Mutation = "reexecute-retired"
       IN /\ replay' = IF broken
                       THEN [i \in ReplayIds |-> IF i \in UnusedIds THEN retained ELSE replay[i]]
                       ELSE replay
          /\ references' = [v \in Versions |-> references[v]
                               + IF broken THEN 2 * Bit(retained = v) ELSE 0]
          /\ payloads' = Alive(references')
          /\ executions' = executions + Bit(broken)
    /\ UNCHANGED <<retained, liveOther, frozen, finished, replaced, cleared,
                   producerAlive, lastWrite, phase, receipt, retired, running, crashes>>

Replace ==
    /\ running /\ producerAlive /\ ~replaced
    /\ retained' = 2 /\ replaced' = TRUE /\ lastWrite' = 2
    /\ replay' = IF Mutation = "retarget-replay"
                 THEN [i \in ReplayIds |-> IF replay[i] > 0 THEN 2 ELSE 0]
                 ELSE replay
    /\ LET ordinary == [v \in Versions |-> references[v]
                         + Bit(v = 2) - Bit(retained = v)
                         + ReplayCount(replay', v) - ReplayCount(replay, v)]
       IN references' = IF Mutation = "free-old-payload" /\ retained # 0 /\ retained # 2
                         THEN [ordinary EXCEPT ![retained] = 0] ELSE ordinary
    /\ payloads' = Alive(references')
    /\ UNCHANGED <<liveOther, frozen, finished, cleared, producerAlive,
                   phase, receipt, retired, repeated, executions, running, crashes>>

Clear ==
    /\ running /\ producerAlive /\ ~cleared
    /\ retained' = 0 /\ cleared' = TRUE /\ lastWrite' = 0
    /\ references' = [v \in Versions |-> references[v] - Bit(retained = v)]
    /\ payloads' = Alive(references')
    /\ UNCHANGED <<liveOther, replay, frozen, finished, replaced, producerAlive,
                   phase, receipt, retired, repeated, executions, running, crashes>>

ProducerDeath ==
    /\ running /\ producerAlive /\ producerAlive' = FALSE
    /\ LET broken == Mutation = "producer-owns-retained"
       IN /\ retained' = IF broken THEN 0 ELSE retained
          /\ references' = [v \in Versions |-> references[v]
                              - IF broken THEN Bit(retained = v) ELSE 0]
    /\ payloads' = Alive(references')
    /\ UNCHANGED <<liveOther, replay, frozen, finished, replaced, cleared,
                   lastWrite, phase, receipt, retired, repeated, executions,
                   running, crashes>>

FinishOther == /\ running /\ liveOther /\ liveOther' = FALSE
               /\ references' = [references EXCEPT ![1] = @ - 1]
               /\ payloads' = Alive(references')
               /\ UNCHANGED <<retained, replay, frozen, finished, replaced,
                              cleared, producerAlive, lastWrite, phase, receipt,
                              retired, repeated, executions, running, crashes>>
FinishReplay(i) ==
    /\ running /\ i \in ReplayIds /\ replay[i] > 0
    /\ references' = [references EXCEPT ![replay[i]] = @ - 1]
    /\ replay' = [replay EXCEPT ![i] = 0]
    /\ finished' = [finished EXCEPT ![i] = TRUE]
    /\ payloads' = Alive(references')
    /\ UNCHANGED <<retained, liveOther, frozen, replaced, cleared, producerAlive,
                   lastWrite, phase, receipt, retired, repeated, executions,
                   running, crashes>>

\* A process cut loses availability, not committed business/receipt state.
\* The concrete epoch/recovery protocol is an explicit assumption here.
Crash == /\ running /\ crashes < MaxCrashes
         /\ running' = FALSE /\ crashes' = crashes + 1
         /\ UNCHANGED <<retained, references, payloads, liveOther, replay, frozen,
                        finished, replaced, cleared, producerAlive, lastWrite,
                        phase, receipt, retired, repeated, executions>>
Recover == /\ ~running /\ running' = TRUE
           /\ UNCHANGED <<retained, references, payloads, liveOther, replay, frozen,
                          finished, replaced, cleared, producerAlive, lastWrite,
                          phase, receipt, retired, repeated, executions, crashes>>
Idle == UNCHANGED vars
Next == SubmitReplay \/ CommitReplay \/ ResolveSameOperation \/ ConsumeResult
        \/ CollectReceipt \/ RepeatRetiredReplay \/ Replace \/ Clear
        \/ ProducerDeath \/ FinishOther \/ (\E i \in ReplayIds : FinishReplay(i))
        \/ Crash \/ Recover \/ Idle
Spec == Init /\ [][Next]_vars

TypeOK == /\ retained \in 0..2 /\ references \in [Versions -> 0..6]
          /\ payloads \subseteq Versions /\ liveOther \in BOOLEAN
          /\ replay \in [ReplayIds -> 0..2] /\ frozen \in [ReplayIds -> 0..2]
          /\ finished \in [ReplayIds -> BOOLEAN]
          /\ replaced \in BOOLEAN /\ cleared \in BOOLEAN /\ producerAlive \in BOOLEAN
          /\ lastWrite \in 0..2
          /\ phase \in {"idle", "submitted", "unknown", "known", "consumed"}
          /\ receipt \in BOOLEAN /\ retired \in BOOLEAN /\ repeated \in BOOLEAN
          /\ executions \in 0..2 /\ running \in BOOLEAN /\ crashes \in 0..MaxCrashes
ReferencesExact == references = ExpectedReferences
PayloadLifetimeExact == payloads = Alive(ExpectedReferences)
FrozenReplay == \A i \in ReplayIds : replay[i] = IF finished[i] THEN 0 ELSE frozen[i]
RetainedIndependentOfProducer == retained = lastWrite
NoReexecution == executions <= 1
RetirementRequiresConsumption == retired => phase = "consumed"
NoOwnershipWitness == ~(retired /\ replaced /\ ~producerAlive /\ retained = 2
                         /\ replay[1] = 1 /\ replay[2] = 1 /\ liveOther
                         /\ payloads = Versions)
=============================================================================
