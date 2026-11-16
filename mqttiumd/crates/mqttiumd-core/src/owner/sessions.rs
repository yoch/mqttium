//! Receipted session incarnations; transport loss alone is not business completion.

#[cfg(test)]
mod tests {
    use super::super::*;
    use alloc::vec::Vec;
    use crate::contract::effects::{SubmitResult, OwnerAuthority};
    use crate::contract::opening::{EpochOpening, EpochOpened, OpeningOutcome, OpeningReply, StartupNonce};
    use crate::contract::session::{SessionChange, SessionExpiry, SessionCancellations};

    const TURN: TurnGrant = TurnGrant {work_limit: WorkUnits::new(u64::MAX)};
    struct Taking(Vec<OwnedEffect>);
    impl EffectSink for Taking {
        fn try_submit(&mut self, effect: OwnedEffect) -> SubmitResult { self.0.push(effect); SubmitResult::Accepted }
    }
    fn bound(n: u32) -> NonZeroU32 { NonZeroU32::new(n).unwrap() }
    fn bytes(value: &[u8]) -> SharedBytes { SharedBytes::copy_from(value, 128).unwrap() }
    fn drain(owner: &mut Owner) -> Vec<(EffectId, Effect)> {
        let mut sink = Taking(Vec::new());
        owner.dispatch(&mut sink);
        sink.0.into_iter().map(OwnedEffect::into_parts).collect()
    }
    fn apply(owner: &mut Owner, input: Input) -> Vec<(EffectId, Effect)> {
        let result = owner.step(input, TURN);
        assert!(matches!(result, StepOutcome::Applied { .. }), "session_lifecycle_not_applied: {result:?}");
        drain(owner)
    }
    fn world() -> Owner {
        let incarnation = StoreIncarnation::new(1);
        let epoch = RunEpoch::new(2);
        let mut opening = EpochOpening::new(incarnation, StartupNonce::new([7;16]));
        let command = opening.submit(RunEpoch::new(1)).unwrap();
        let _ = opening.observe(OpeningReply { command, outcome: OpeningOutcome::Committed(EpochOpened { incarnation, epoch }) }).unwrap();
        let mut authority = OwnerAuthority::from_grant(opening.hand_over().unwrap(), bound(1)).unwrap();
        let outbox = authority.create_outbox(LogicalOwnerId::new(4), NonZeroUsize::new(32).unwrap(), bound(8)).unwrap();
        let mut owner = Owner::new(OwnerConfig {
            max_sessions: bound(1), max_routes: bound(2), max_exchanges: bound(8), account: AccountId::new(1), payload_bytes: PayloadBytes::new(1024), resolution_delay: LogicalDuration::new(1), retained_operations: bound(16), remembered_connections: bound(4),
        },outbox).unwrap();
        let facts = [RecoveredFact::Epoch { incarnation, epoch }, RecoveredFact::Limits { max_targets_per_admission: 2, max_topic_bytes:128, max_payload_bytes:PayloadBytes::new(128) }, RecoveredFact::RouteTable {revision:Revision::new(0)}];
        assert!(apply(&mut owner,Input::Recovery {facts:BoundedVec::from_slice(&facts,facts.len()).unwrap(),last:true}).is_empty());
        owner
    }
    fn known(owner: &mut Owner, effects: &[(EffectId, Effect)], change: SessionChange) -> (OperationId, Vec<(EffectId, Effect)>) {
        let (effect, command) = effects.iter().find_map(|(effect,item)| match item {Effect::Persist {command}=>Some((*effect,command)),_=>None}).expect("a canonical session command");
        let operation=command.recipe.operation();
        (operation,apply(owner,Input::StoreResult {effect,operation,reply:StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(change)))}))
    }
    #[test]
    fn a_new_ephemeral_session_ends_and_releases_the_owner_slot() {
        let mut owner=world();
        let connection=ConnectionGeneration::new(1);
        let opened=apply(&mut owner,Input::OpenSession {connection, client_id:bytes(b"ephemeral"),clean_start:false,expiry:SessionExpiry::OnDisconnect});
        assert_eq!(owner.sessions.len(),0,"creation is not known at submission");
        let (_, frames)=known(&mut owner,&opened,SessionChange::Opened {session:SessionId::new(1),revision:Revision::new(1),session_present:false,expiry:SessionExpiry::OnDisconnect,retired:None,route_revision:Revision::new(0),cancelled:SessionCancellations::default()});
        assert!(frames.iter().any(|(_,effect)|matches!(effect,Effect::EmitFrame {frame:OutboundFrame::Connack {session_present:false},..})),"known durable creation must precede CONNACK");
        let close=apply(&mut owner,Input::ConnectionClosed {connection});
        assert_eq!(owner.sessions.len(),1,"closure is not known at submission");
        known(&mut owner,&close,SessionChange::Closed {session:SessionId::new(1),route_revision:Revision::new(0),cancelled:SessionCancellations::default()});
        assert!(owner.sessions.is_empty(),"terminal_session_owner_history_retained");
        assert!(owner.slots.is_empty());
    }
}
