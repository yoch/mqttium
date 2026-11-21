----------------------------- MODULE MPacketReuse -----------------------------
(***************************************************************************)
(* Bounded outgoing QoS 1 PI resource reuse; specification, not refinement. *)
(* A PI is recyclable. Delivery, claim/finish operations, effects and       *)
(* connection generations are separate non-reused causal identities.        *)
(* The h-prefixed verifier histories are never consulted by guards.         *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS NumPackets, NumDeliveries, NumGenerations, MaxObservations,
          Start, ObservationKind, Mutation
ASSUME /\ NumPackets \in Nat /\ NumPackets > 0
       /\ NumDeliveries = 2 /\ NumGenerations \in 1..2
       /\ MaxObservations \in 1..2
       /\ Start \in {"live", "recovery"}
       /\ ObservationKind \in {"any", "target", "none"}
       /\ Mutation \in {"none", "EarlyFree", "Occupied", "Rebind", "OldCallback"}

Packets == 1..NumPackets
Deliveries == 1..NumDeliveries
Generations == 1..NumGenerations
MaxEffects == NumDeliveries * NumGenerations
Effects == 1..MaxEffects
ClaimOperation(d) == 2 * d - 1
FinishOperation(d) == 2 * d
NoTarget == [delivery |-> 0, claim |-> 0, frame |-> 0]
Target(d, effect) == [delivery |-> d, claim |-> ClaimOperation(d), frame |-> effect]
Targets == {NoTarget} \cup {Target(d, e) : d \in Deliveries, e \in Effects}
NoRaw == [packet |-> 0, generation |-> 0]
Raws == {NoRaw} \cup [packet : Packets, generation : Generations]
NoInput == [packet |-> 0, generation |-> 0, target |-> NoTarget]
Inputs == {NoInput} \cup [packet : Packets, generation : Generations, target : Targets]
NoFrame == [generation |-> 0, target |-> NoTarget]
Frames == {NoFrame} \cup [generation : Generations, target : Targets \ {NoTarget}]
NoFinish == [delivery |-> 0, packet |-> 0, claim |-> 0, operation |-> 0]
FinishCopy(d, p) == [delivery |-> d, packet |-> p,
                     claim |-> ClaimOperation(d), operation |-> FinishOperation(d)]
Finishes == {NoFinish} \cup {FinishCopy(d, p) : d \in Deliveries, p \in Packets}

VARIABLES phase, packet, reserved, credits, nextDelivery,
          storedClaims, finishOutcome, ready, payloadReady,
          generation, effectSequence, frame, frameGeneration,
          mapping, wirePublish, written, mayHaveStarted, dup,
          reader, input, savedInput, observationCount, firstFrame, oldFinish,
          hAssigned, hFinishApplied, hFreed, hWrongFinish,
          hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten
vars == <<phase, packet, reserved, credits, nextDelivery, storedClaims,
          finishOutcome, ready, payloadReady, generation, effectSequence,
          frame, frameGeneration, mapping, wirePublish, written, mayHaveStarted,
          dup, reader, input, savedInput, observationCount, firstFrame, oldFinish,
          hAssigned, hFinishApplied, hFreed, hWrongFinish,
          hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

Init ==
    /\ phase = [d \in Deliveries |-> IF Start = "recovery" /\ d = 1 THEN "Absent" ELSE "Queued"]
    /\ packet = [d \in Deliveries |-> 0]
    /\ reserved = {} /\ credits = NumDeliveries
    /\ nextDelivery = IF Start = "recovery" THEN 2 ELSE 1
    /\ storedClaims = [d \in Deliveries |-> IF Start = "recovery" /\ d = 1 THEN 1 ELSE 0]
    /\ finishOutcome = {}
    /\ ready = (Start = "live")
    /\ payloadReady = IF Start = "recovery" THEN Deliveries \ {1} ELSE Deliveries
    /\ generation = 1 /\ effectSequence = 0
    /\ frame = [d \in Deliveries |-> 0]
    /\ frameGeneration = [d \in Deliveries |-> 0]
    /\ mapping = [p \in Packets |-> NoTarget]
    /\ wirePublish = {} /\ written = {} /\ mayHaveStarted = {} /\ dup = {}
    /\ reader = NoRaw /\ input = NoInput /\ savedInput = NoInput
    /\ observationCount = 0 /\ firstFrame = NoFrame /\ oldFinish = NoFinish
    /\ hAssigned = [d \in Deliveries |-> 0]
    /\ hFinishApplied = {} /\ hFreed = {} /\ hWrongFinish = FALSE
    /\ hAckBeforeWritten = FALSE /\ hStaleIgnored = FALSE /\ hNoneIgnored = FALSE
    /\ hWrongWritten = FALSE

OccupiedForSelection == {d \in reserved : ~(Mutation = "EarlyFree" /\ phase[d] = "FinishPending")}
AvailablePackets == Packets \ {packet[d] : d \in OccupiedForSelection}

Reserve ==
    LET d == nextDelivery
        free == AvailablePackets
        choices == IF Mutation = "Occupied" THEN Packets ELSE free
        p == CHOOSE candidate \in choices : \A other \in choices : candidate <= other
    IN /\ ready /\ d \in Deliveries /\ phase[d] = "Queued"
       /\ credits > 0 /\ choices # {}
       /\ packet' = [packet EXCEPT ![d] = p]
       /\ phase' = [phase EXCEPT ![d] = "ClaimPending"]
       /\ reserved' = reserved \cup {d} /\ credits' = credits - 1
       /\ nextDelivery' = d + 1 /\ hAssigned' = [hAssigned EXCEPT ![d] = p]
       /\ UNCHANGED <<storedClaims, finishOutcome, ready, payloadReady, generation,
                      effectSequence, frame, frameGeneration, mapping, wirePublish,
                      written, mayHaveStarted, dup, reader, input, savedInput,
                      observationCount, firstFrame, oldFinish, hFinishApplied,
                      hFreed, hWrongFinish, hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

ClaimCommit(d) ==
    /\ phase[d] = "ClaimPending" /\ storedClaims[d] = 0
    /\ storedClaims' = [storedClaims EXCEPT ![d] = packet[d]]
    /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, finishOutcome,
                   ready, payloadReady, generation, effectSequence, frame,
                   frameGeneration, mapping, wirePublish, written, mayHaveStarted,
                   dup, reader, input, savedInput, observationCount, firstFrame,
                   oldFinish, hAssigned, hFinishApplied, hFreed, hWrongFinish,
                   hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

ClaimKnown(d) ==
    /\ phase[d] = "ClaimPending" /\ storedClaims[d] = packet[d]
    /\ phase' = [phase EXCEPT ![d] = "AwaitPuback"]
    /\ UNCHANGED <<packet, reserved, credits, nextDelivery, storedClaims, finishOutcome,
                   ready, payloadReady, generation, effectSequence, frame,
                   frameGeneration, mapping, wirePublish, written, mayHaveStarted,
                   dup, reader, input, savedInput, observationCount, firstFrame,
                   oldFinish, hAssigned, hFinishApplied, hFreed, hWrongFinish,
                   hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

RecoverClaim(d) ==
    /\ ~ready /\ storedClaims[d] # 0 /\ d \notin reserved
    /\ phase' = [phase EXCEPT ![d] = "AwaitPuback"]
    /\ packet' = [packet EXCEPT ![d] = storedClaims[d]]
    /\ reserved' = reserved \cup {d} /\ credits' = credits - 1
    /\ mayHaveStarted' = mayHaveStarted \cup {d}
    /\ hAssigned' = [hAssigned EXCEPT ![d] = storedClaims[d]]
    /\ UNCHANGED <<nextDelivery, storedClaims, finishOutcome, ready, payloadReady,
                   generation, effectSequence, frame, frameGeneration, mapping,
                   wirePublish, written, dup, reader, input, savedInput,
                   observationCount, firstFrame, oldFinish, hFinishApplied,
                   hFreed, hWrongFinish, hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

RecoveryComplete ==
    /\ ~ready /\ \A d \in Deliveries : storedClaims[d] # 0 => d \in reserved
    /\ ready' = TRUE
    /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, storedClaims,
                   finishOutcome, payloadReady, generation, effectSequence, frame,
                   frameGeneration, mapping, wirePublish, written, mayHaveStarted,
                   dup, reader, input, savedInput, observationCount, firstFrame,
                   oldFinish, hAssigned, hFinishApplied, hFreed, hWrongFinish,
                   hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

LoadPayload(d) ==
    /\ ready /\ phase[d] = "AwaitPuback" /\ d \notin payloadReady
    /\ payloadReady' = payloadReady \cup {d}
    /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, storedClaims,
                   finishOutcome, ready, generation, effectSequence, frame,
                   frameGeneration, mapping, wirePublish, written, mayHaveStarted,
                   dup, reader, input, savedInput, observationCount, firstFrame,
                   oldFinish, hAssigned, hFinishApplied, hFreed, hWrongFinish,
                   hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

AcceptPublish(d) ==
    LET e == effectSequence + 1
        target == Target(d, e)
    IN /\ ready /\ phase[d] = "AwaitPuback" /\ d \in payloadReady
       /\ frame[d] = 0 /\ e \in Effects
       /\ effectSequence' = e
       /\ frame' = [frame EXCEPT ![d] = e]
       /\ frameGeneration' = [frameGeneration EXCEPT ![d] = generation]
       /\ mapping' = [mapping EXCEPT ![packet[d]] = target]
       /\ dup' = IF d \in mayHaveStarted THEN dup \cup {d} ELSE dup \ {d}
       /\ mayHaveStarted' = mayHaveStarted \cup {d}
       /\ firstFrame' = IF firstFrame = NoFrame
                         THEN [generation |-> generation, target |-> target] ELSE firstFrame
       /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, storedClaims,
                      finishOutcome, ready, payloadReady, generation, wirePublish,
                      written, reader, input, savedInput, observationCount, oldFinish,
                      hAssigned, hFinishApplied, hFreed, hWrongFinish,
                      hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

WirePublish(d) ==
    /\ frame[d] # 0 /\ frameGeneration[d] = generation /\ d \notin wirePublish
    /\ mapping[packet[d]] = Target(d, frame[d])
    /\ wirePublish' = wirePublish \cup {d}
    /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, storedClaims,
                   finishOutcome, ready, payloadReady, generation, effectSequence,
                   frame, frameGeneration, mapping, written, mayHaveStarted, dup,
                   reader, input, savedInput, observationCount, firstFrame, oldFinish,
                   hAssigned, hFinishApplied, hFreed, hWrongFinish,
                   hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

WrittenCallback(callback) ==
    LET candidates == {d \in Deliveries : /\ callback.generation = generation
                            /\ frameGeneration[d] = generation /\ frame[d] # 0
                            /\ (frame[d] = callback.target.frame \/ Mutation = "OldCallback")}
        d == CHOOSE delivery \in candidates : TRUE
    IN /\ callback # NoFrame
       /\ written' = IF candidates = {} THEN written ELSE written \cup {d}
       /\ hWrongWritten' = (hWrongWritten \/ (candidates # {} /\ Target(d, frame[d]) # callback.target))
       /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, storedClaims,
                      finishOutcome, ready, payloadReady, generation, effectSequence,
                      frame, frameGeneration, mapping, wirePublish, mayHaveStarted,
                      dup, reader, input, savedInput, observationCount, firstFrame,
                      oldFinish, hAssigned, hFinishApplied, hFreed, hWrongFinish,
                      hAckBeforeWritten, hStaleIgnored, hNoneIgnored>>

CurrentWrittenCallback(d) ==
    /\ frame[d] # 0
    /\ WrittenCallback([generation |-> frameGeneration[d], target |-> Target(d, frame[d])])
OldWrittenCallback == firstFrame # NoFrame /\ WrittenCallback(firstFrame)

QueueRawAck(p, g) ==
    /\ ready /\ reader = NoRaw /\ g <= generation
    /\ (mapping[p] = NoTarget \/ mapping[p].delivery \in wirePublish)
    /\ reader' = [packet |-> p, generation |-> g]
    /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, storedClaims,
                   finishOutcome, ready, payloadReady, generation, effectSequence,
                   frame, frameGeneration, mapping, wirePublish, written,
                   mayHaveStarted, dup, input, savedInput, observationCount,
                   firstFrame, oldFinish, hAssigned, hFinishApplied, hFreed,
                   hWrongFinish, hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

DequeueAck ==
    LET target == IF reader.generation = generation THEN mapping[reader.packet] ELSE NoTarget
        observed == [packet |-> reader.packet, generation |-> reader.generation, target |-> target]
    IN /\ reader # NoRaw /\ input = NoInput /\ observationCount < MaxObservations
       /\ (ObservationKind = "any" \/ (ObservationKind = "target" /\ target # NoTarget)
                                     \/ (ObservationKind = "none" /\ target = NoTarget))
       /\ input' = observed /\ reader' = NoRaw
       /\ savedInput' = IF savedInput = NoInput THEN observed ELSE savedInput
       /\ observationCount' = observationCount + 1
       /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, storedClaims,
                      finishOutcome, ready, payloadReady, generation, effectSequence,
                      frame, frameGeneration, mapping, wirePublish, written,
                      mayHaveStarted, dup, firstFrame, oldFinish, hAssigned,
                      hFinishApplied, hFreed, hWrongFinish,
                      hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

ReplayObservedCopy ==
    /\ input = NoInput /\ savedInput # NoInput /\ input' = savedInput
    /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, storedClaims,
                   finishOutcome, ready, payloadReady, generation, effectSequence,
                   frame, frameGeneration, mapping, wirePublish, written,
                   mayHaveStarted, dup, reader, savedInput, observationCount,
                   firstFrame, oldFinish, hAssigned, hFinishApplied, hFreed,
                   hWrongFinish, hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

ProcessAck ==
    LET rebound == IF input.generation = generation THEN mapping[input.packet] ELSE NoTarget
        target == IF Mutation = "Rebind" THEN rebound ELSE input.target
        d == target.delivery
        valid == IF target = NoTarget THEN FALSE ELSE
            /\ input.generation = generation /\ phase[d] = "AwaitPuback"
            /\ d \in reserved /\ packet[d] = input.packet
            /\ target.claim = ClaimOperation(d) /\ target.frame = frame[d]
            /\ frameGeneration[d] = generation
    IN /\ input # NoInput
       /\ phase' = IF valid THEN [phase EXCEPT ![d] = "FinishPending"] ELSE phase
       /\ input' = NoInput
       /\ hWrongFinish' = (hWrongFinish \/ (valid /\ target # input.target))
       /\ hAckBeforeWritten' = (hAckBeforeWritten
             \/ (valid /\ d \in wirePublish /\ d \notin written /\ target = input.target))
       /\ hStaleIgnored' = (hStaleIgnored
             \/ (~valid /\ input.target # NoTarget /\ rebound # NoTarget
                         /\ rebound # input.target /\ input.generation = generation))
       /\ hNoneIgnored' = (hNoneIgnored
             \/ (~valid /\ input.target = NoTarget /\ rebound # NoTarget))
       /\ UNCHANGED <<packet, reserved, credits, nextDelivery, storedClaims,
                      finishOutcome, ready, payloadReady, generation, effectSequence,
                      frame, frameGeneration, mapping, wirePublish, written,
                      mayHaveStarted, dup, reader, savedInput, observationCount,
                      firstFrame, oldFinish, hAssigned, hFinishApplied, hFreed, hWrongWritten>>

FinishCommit(d) ==
    /\ phase[d] = "FinishPending" /\ storedClaims[d] = packet[d]
    /\ storedClaims' = [storedClaims EXCEPT ![d] = 0]
    /\ finishOutcome' = finishOutcome \cup {d}
    /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, ready,
                   payloadReady, generation, effectSequence, frame, frameGeneration,
                   mapping, wirePublish, written, mayHaveStarted, dup, reader,
                   input, savedInput, observationCount, firstFrame, oldFinish,
                   hAssigned, hFinishApplied, hFreed, hWrongFinish,
                   hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

FinishKnown(d) ==
    /\ phase[d] = "FinishPending" /\ d \in finishOutcome
    /\ phase' = [phase EXCEPT ![d] = "Absent"]
    /\ packet' = [packet EXCEPT ![d] = 0]
    /\ reserved' = reserved \ {d} /\ credits' = credits + 1
    /\ finishOutcome' = finishOutcome \ {d}
    /\ payloadReady' = payloadReady \ {d}
    /\ frame' = [frame EXCEPT ![d] = 0] /\ frameGeneration' = [frameGeneration EXCEPT ![d] = 0]
    /\ mapping' = [p \in Packets |-> IF mapping[p].delivery = d THEN NoTarget ELSE mapping[p]]
    /\ wirePublish' = wirePublish \ {d} /\ written' = written \ {d}
    /\ mayHaveStarted' = mayHaveStarted \ {d} /\ dup' = dup \ {d}
    /\ oldFinish' = IF oldFinish = NoFinish THEN FinishCopy(d, packet[d]) ELSE oldFinish
    /\ hFinishApplied' = hFinishApplied \cup {d} /\ hFreed' = hFreed \cup {d}
    /\ UNCHANGED <<nextDelivery, storedClaims, ready, generation, effectSequence,
                   reader, input, savedInput, observationCount, firstFrame,
                   hAssigned, hWrongFinish, hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

OldFinishCallback ==
    /\ oldFinish # NoFinish
    /\ UNCHANGED vars

Takeover ==
    /\ ready /\ generation < NumGenerations /\ generation' = generation + 1
    /\ frame' = [d \in Deliveries |-> 0] /\ frameGeneration' = [d \in Deliveries |-> 0]
    /\ mapping' = [p \in Packets |-> NoTarget]
    /\ wirePublish' = {} /\ written' = {} /\ dup' = {}
    /\ UNCHANGED <<phase, packet, reserved, credits, nextDelivery, storedClaims,
                   finishOutcome, ready, payloadReady, effectSequence, mayHaveStarted,
                   reader, input, savedInput, observationCount, firstFrame, oldFinish,
                   hAssigned, hFinishApplied, hFreed, hWrongFinish,
                   hAckBeforeWritten, hStaleIgnored, hNoneIgnored, hWrongWritten>>

Idle == UNCHANGED vars
Next ==
    \/ Reserve
    \/ \E d \in Deliveries : ClaimCommit(d)
    \/ \E d \in Deliveries : ClaimKnown(d)
    \/ \E d \in Deliveries : RecoverClaim(d)
    \/ RecoveryComplete
    \/ \E d \in Deliveries : LoadPayload(d)
    \/ \E d \in Deliveries : AcceptPublish(d)
    \/ \E d \in Deliveries : WirePublish(d)
    \/ \E d \in Deliveries : CurrentWrittenCallback(d)
    \/ OldWrittenCallback
    \/ \E p \in Packets, g \in Generations : QueueRawAck(p, g)
    \/ DequeueAck
    \/ ReplayObservedCopy
    \/ ProcessAck
    \/ \E d \in Deliveries : FinishCommit(d)
    \/ \E d \in Deliveries : FinishKnown(d)
    \/ OldFinishCallback
    \/ Takeover
    \/ Idle
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in [Deliveries -> {"Queued", "ClaimPending", "AwaitPuback", "FinishPending", "Absent"}]
    /\ packet \in [Deliveries -> 0..NumPackets] /\ reserved \subseteq Deliveries
    /\ credits \in 0..(NumDeliveries + 1) /\ nextDelivery \in 1..(NumDeliveries + 1)
    /\ storedClaims \in [Deliveries -> 0..NumPackets] /\ finishOutcome \subseteq Deliveries
    /\ ready \in BOOLEAN /\ payloadReady \subseteq Deliveries
    /\ generation \in Generations /\ effectSequence \in 0..MaxEffects
    /\ frame \in [Deliveries -> 0..MaxEffects] /\ frameGeneration \in [Deliveries -> 0..NumGenerations]
    /\ mapping \in [Packets -> Targets] /\ wirePublish \subseteq Deliveries
    /\ written \subseteq Deliveries /\ mayHaveStarted \subseteq Deliveries /\ dup \subseteq Deliveries
    /\ reader \in Raws /\ input \in Inputs /\ savedInput \in Inputs
    /\ observationCount \in 0..MaxObservations /\ firstFrame \in Frames /\ oldFinish \in Finishes
    /\ hAssigned \in [Deliveries -> 0..NumPackets]
    /\ hFinishApplied \subseteq Deliveries /\ hFreed \subseteq Deliveries
    /\ hWrongFinish \in BOOLEAN /\ hAckBeforeWritten \in BOOLEAN
    /\ hStaleIgnored \in BOOLEAN /\ hNoneIgnored \in BOOLEAN /\ hWrongWritten \in BOOLEAN
UniqueLivePacket == \A a, b \in reserved : a # b => packet[a] # packet[b]
ReservationHeld == \A d \in Deliveries : phase[d] \in {"ClaimPending", "AwaitPuback", "FinishPending"} => d \in reserved
ReleasedOnlyAfterKnownFinish ==
    /\ hFreed \subseteq hFinishApplied
    /\ \A d \in reserved : packet[d] \notin AvailablePackets
CreditsExact == credits + Cardinality(reserved) = NumDeliveries
RecoveredClaimsReserved == ready => (\A d \in Deliveries : storedClaims[d] # 0 => d \in reserved /\ packet[d] = storedClaims[d])
NoWrongFinish == ~hWrongFinish
NoWrongWritten == ~hWrongWritten
EmissionRegistered == \A d \in wirePublish : mapping[packet[d]] = Target(d, frame[d]) /\ frame[d] # 0
ActiveFrameGuard == \A d \in Deliveries : frame[d] # 0 => phase[d] \in {"AwaitPuback", "FinishPending"}
FreshFrameDupZero == \A d \in Deliveries : frame[d] # 0 /\ frameGeneration[d] = 1 /\ ~(Start = "recovery" /\ d = 1) => d \notin dup
RecoveredFrameDup == Start = "recovery" /\ frame[1] # 0 => 1 \in dup
NoReuseWitness == ~\E a, b \in Deliveries :
    /\ a < b /\ a \in hFinishApplied /\ b \in reserved /\ hAssigned[a] = hAssigned[b]
    /\ frame[b] # 0 /\ b \notin dup /\ ClaimOperation(a) # ClaimOperation(b)
    /\ frame[b] # firstFrame.target.frame
NoAckBeforeWrittenWitness == ~hAckBeforeWritten
NoStaleCopyWitness == ~hStaleIgnored
NoNoneWitness == ~hNoneIgnored
NoRecoveryBeforePayloadWitness == ~(Start = "recovery" /\ ready /\ 1 \in reserved /\ 1 \notin payloadReady)
=============================================================================
