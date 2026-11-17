//! Durable wildcard subscription options. These are implementation checks,
//! independent of the network/system reception and its public clients.
use std::path::{Path,PathBuf};
use std::sync::atomic::{AtomicU64,Ordering};
use mqttiumd_contract::capacity::{BoundedVec,PayloadBytes,References,ExchangeSlots,ReceiptSlots};
use mqttiumd_contract::command::{Command,FrozenRecipe,OpenEpoch,OpeningAttempt,OpeningTicket,Recipe,StartupNonce,UpdateSubscriptions};
use mqttiumd_contract::effects::RecoveredFact;
use mqttiumd_contract::ids::{LogicalOwnerId,OperationId,OperationSeq,Revision,RunEpoch,SessionId,StoreIncarnation};
use mqttiumd_contract::packet::{Qos,SharedBytes};
use mqttiumd_contract::pubsub::{GrantedSubscriptionChange,SubscriptionOptions};
use mqttiumd_store_api::{AdminCommand,AdminDone,DurableStore,Limits};
use mqttiumd_store_sqlite::SqliteStore;
static NEXT:AtomicU64=AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new()->Self {
        let path=std::env::temp_dir().join(format!("mqttiumd-wildcard-store-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        std::fs::create_dir(&path).unwrap();Self(path)
    }
    fn db(&self)->PathBuf {self.0.join("store.sqlite")}
}
impl Drop for Directory {fn drop(&mut self){let _=std::fs::remove_dir_all(&self.0);}}
fn limits()->Limits {
    Limits {payload_bytes:PayloadBytes::new(4096),references:References::new(16),exchange_slots:ExchangeSlots::new(4),receipt_slots:ReceiptSlots::new(8192),max_targets_per_admission:16,max_topic_bytes:256,max_payload_bytes:1024,max_sessions:8,max_route_targets:16}
}
fn setup(path:&Path)->(SqliteStore,SessionId) {
    let mut store=SqliteStore::create(path,StoreIncarnation::new(1),limits()).unwrap();
    let AdminDone::SessionCreated {session,..}=store.admin(AdminCommand::CreateSession{client_id:b"subscriber".to_vec()}).unwrap() else {panic!("session")};
    store.open_epoch(&OpenEpoch {attempt:OpeningAttempt{startup:StartupNonce::new([1;16]),ticket:OpeningTicket::new(1)},incarnation:StoreIncarnation::new(1),after:RunEpoch::new(0)}).unwrap();
    (store,session)
}
fn update(session:SessionId,sequence:u64,route_revision:Revision,filter:&[u8],granted:Option<Qos>,options:u8)->Command {
    Command {recipe:FrozenRecipe::freeze(Recipe::UpdateSubscriptions(UpdateSubscriptions {
        operation:OperationId::new(StoreIncarnation::new(1),RunEpoch::new(1),LogicalOwnerId::new(0),OperationSeq::new(sequence)),session,session_revision:Revision::new(1),route_revision,
        changes:BoundedVec::from_slice(&[GrantedSubscriptionChange {topic:SharedBytes::copy_from(filter,256).unwrap(),granted,options:SubscriptionOptions::from_bits(options).unwrap()}],8).unwrap(),
    })).unwrap(),payload:None}
}
#[test]
fn wildcard_options_are_committed_recovered_and_replaced_without_history() {
    let directory=Directory::new();
    let (mut store,session)=setup(&directory.db());
    let command=update(session,1,Revision::new(0),b"sensor/+",Some(Qos::AtLeastOnce),4);
    assert!(store.apply(&command).committed_now);
    let state=store.snapshot().unwrap();
    assert_eq!(state.routes[b"sensor/+".as_slice()][0].options.bits(),4,"subscription_options_were_not_durable");
    let recovered=store.recover(None,8).unwrap();
    assert!(recovered.facts.as_slice().iter().any(|fact|matches!(fact,RecoveredFact::Route {topic,options,..} if topic.as_slice()==b"sensor/+" && options.bits()==4)));
    drop(store);
    let mut store=SqliteStore::open(&directory.db()).unwrap();
    assert_eq!(store.snapshot().unwrap().routes[b"sensor/+".as_slice()][0].options.bits(),4);
    assert!(!store.apply(&command).committed_now,"same operation remains idempotent after reopen");
    for (sequence,bits,qos) in [(2,44,Qos::AtMostOnce),(3,0,Qos::AtLeastOnce),(4,20,Qos::AtLeastOnce)] {
        let revision=store.snapshot().unwrap().route_revision;
        assert!(store.apply(&update(session,sequence,revision,b"sensor/+",Some(qos),bits)).committed_now);
        let state=store.snapshot().unwrap();
        assert_eq!(state.routes.len(),1);
        assert_eq!(state.routes[b"sensor/+".as_slice()].len(),1);
        assert_eq!(state.routes[b"sensor/+".as_slice()][0].options.bits(),bits);
        assert_eq!(state.routes[b"sensor/+".as_slice()][0].qos,qos);
    }
}
