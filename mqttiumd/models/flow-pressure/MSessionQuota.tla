-------------------------- MODULE MSessionQuota --------------------------
EXTENDS Naturals, FiniteSets
CONSTANT Mutation
Sessions == {"A","B"}
Keys == Sessions \X (1..2)
Quota == 1
VARIABLES generation, online, phase, origin, refusal
vars == <<generation, online, phase, origin, refusal>>
Held(s) == {k \in Keys : k[1] = s /\ phase[k] # "Free"}
Counted(s) == IF Mutation = "ResetAtConnect"
              THEN {k \in Held(s) : origin[k] = generation[s]} ELSE Held(s)
Owes(s) == \A k \in Held(s) : phase[k] = "AwaitRel"
Init == /\ generation = [s \in Sessions |-> 1] /\ online = [s \in Sessions |-> TRUE]
        /\ phase = [k \in Keys |-> "Free"] /\ origin = [k \in Keys |-> 0] /\ refusal = "None"
Publish == \E k \in Keys :
            LET s == k[1] IN
            /\ online[s] /\ phase[k] = "Free"
            /\ IF Cardinality(Counted(s)) < Quota THEN
                   /\ phase' = [phase EXCEPT ![k] = "Admitting"]
                   /\ origin' = [origin EXCEPT ![k] = generation[s]]
                   /\ UNCHANGED <<generation, online, refusal>>
               ELSE /\ Owes(s) \/ Mutation = "RefuseLocalWork"
                    /\ online' = [online EXCEPT ![s] = FALSE]
                    /\ refusal' = IF Owes(s) THEN "Owed" ELSE "Premature"
                    /\ UNCHANGED <<generation, phase, origin>>
CommitAdmission == \E k \in Keys :
                    /\ phase[k] = "Admitting" /\ phase' = [phase EXCEPT ![k] = "AwaitRel"]
                    /\ UNCHANGED <<generation, online, origin, refusal>>
Rel == \E k \in Keys : /\ online[k[1]] /\ phase[k] = "AwaitRel"
                       /\ phase' = [phase EXCEPT ![k] = "Releasing"]
                       /\ UNCHANGED <<generation, online, origin, refusal>>
CommitRelease == \E k \in Keys : /\ phase[k] = "Releasing" /\ phase' = [phase EXCEPT ![k] = "Free"]
                                 /\ origin' = [origin EXCEPT ![k] = 0]
                                 /\ UNCHANGED <<generation, online, refusal>>
Reconnect == \E s \in Sessions :
              /\ generation[s] = 1 /\ generation' = [generation EXCEPT ![s] = 2]
              /\ online' = [online EXCEPT ![s] = TRUE]
              /\ UNCHANGED <<phase, origin, refusal>>
Idle == UNCHANGED vars
Next == Publish \/ CommitAdmission \/ Rel \/ CommitRelease \/ Reconnect \/ Idle
Spec == Init /\ [][Next]_vars
PerSession == \A s \in Sessions : Cardinality(Held(s)) <= Quota
NoPrematureRefusal == refusal # "Premature"
NoPeerService == ~(\E a,b \in Keys : a[1] # b[1] /\ phase[a] = "AwaitRel" /\
                                     origin[a] < generation[a[1]] /\ phase[b] = "Releasing")
============================================================================
