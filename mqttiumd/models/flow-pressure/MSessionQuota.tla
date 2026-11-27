-------------------------- MODULE MSessionQuota --------------------------
(***************************************************************************)
(* The bound of inbound QoS 2 exchanges one session holds across its       *)
(* connections (record 0061), as `Owner::plan_admission` decides it in     *)
(* crates/mqttiumd-core/src/owner.rs:                                      *)
(*                                                                         *)
(*   exchanges = QoS 2 slots of slots_by_session[session]                  *)
(*   exchanges.count() < session_qos2_exchanges      admit                 *)
(*   every exchange is owed by the client            close the connection  *)
(*   otherwise                                       the publication waits *)
(*                                                                         *)
(* "Owed" is `SlotState::AwaitPubrel`, or an answer held again for a       *)
(* PUBLISH received again (`AnswerHeld { resent: true }`): "AwaitRel"      *)
(* here. An admission whose result is awaited and a first PUBREC held      *)
(* behind an earlier answer are "Admitting"; a release in flight is        *)
(* "Releasing". A wait is the absence of a step: the model has no waiting  *)
(* publication, and nothing about how long the runtime lets one wait.      *)
(* `origin` is the generation of the connection that opened an exchange:   *)
(* no guard of the unmutated model reads it, only the mutation             *)
(* ResetAtConnect and one witness.                                         *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets
CONSTANT Mutation
Sessions == {"A","B"}
Keys == Sessions \X (1..2)
Quota == 1
VARIABLES generation, online, phase, origin
vars == <<generation, online, phase, origin>>
Held(s) == {k \in Keys : k[1] = s /\ phase[k] # "Free"}
Counted(s) == IF Mutation = "ResetAtConnect"
              THEN {k \in Held(s) : origin[k] = generation[s]} ELSE Held(s)
Owes(s) == \A k \in Held(s) : phase[k] = "AwaitRel"
Init == /\ generation = [s \in Sessions |-> 1] /\ online = [s \in Sessions |-> TRUE]
        /\ phase = [k \in Keys |-> "Free"] /\ origin = [k \in Keys |-> 0]
Publish == \E k \in Keys :
            LET s == k[1] IN
            /\ online[s] /\ phase[k] = "Free"
            /\ IF Cardinality(Counted(s)) < Quota THEN
                   /\ phase' = [phase EXCEPT ![k] = "Admitting"]
                   /\ origin' = [origin EXCEPT ![k] = generation[s]]
                   /\ UNCHANGED <<generation, online>>
               ELSE /\ Owes(s) \/ Mutation = "RefuseLocalWork"
                    /\ online' = [online EXCEPT ![s] = FALSE]
                    /\ UNCHANGED <<generation, phase, origin>>
CommitAdmission == \E k \in Keys :
                    /\ phase[k] = "Admitting" /\ phase' = [phase EXCEPT ![k] = "AwaitRel"]
                    /\ UNCHANGED <<generation, online, origin>>
Rel == \E k \in Keys : /\ online[k[1]] /\ phase[k] = "AwaitRel"
                       /\ phase' = [phase EXCEPT ![k] = "Releasing"]
                       /\ UNCHANGED <<generation, online, origin>>
CommitRelease == \E k \in Keys : /\ phase[k] = "Releasing" /\ phase' = [phase EXCEPT ![k] = "Free"]
                                 /\ origin' = [origin EXCEPT ![k] = 0]
                                 /\ UNCHANGED <<generation, online>>
Reconnect == \E s \in Sessions :
              /\ generation[s] = 1 /\ generation' = [generation EXCEPT ![s] = 2]
              /\ online' = [online EXCEPT ![s] = TRUE]
              /\ UNCHANGED <<phase, origin>>
Idle == UNCHANGED vars
Next == Publish \/ CommitAdmission \/ Rel \/ CommitRelease \/ Reconnect \/ Idle
Spec == Init /\ [][Next]_vars
PerSession == \A s \in Sessions : Cardinality(Held(s)) <= Quota
\* What a refusal leaves, read on the connections and the exchanges alone.
\* `online` is cleared by the refusal of Publish and by nothing else. A
\* session whose connection was closed at its bound is at the bound and
\* every exchange it holds awaits the PUBREL of its client: in the state of
\* the refusal and in every state until the client is back. An owner step
\* on an exchange of such a session breaks it: the owner then still had
\* work the publication could have waited for.
RefusedSessionOnlyOwes ==
    \A s \in Sessions : ~online[s] =>
        /\ Cardinality(Held(s)) >= Quota
        /\ \A k \in Held(s) : phase[k] = "AwaitRel"
NoPeerService == ~(\E a,b \in Keys : a[1] # b[1] /\ phase[a] = "AwaitRel" /\
                                     origin[a] < generation[a[1]] /\ phase[b] = "Releasing")
============================================================================
