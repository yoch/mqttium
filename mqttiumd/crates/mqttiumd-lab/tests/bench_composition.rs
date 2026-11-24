//! Finding F4 through the runner's real start-up path (ADR 0009, 0011):
//! the opening of an epoch, its one grant, `OwnerAuthority::from_grant`,
//! one box per owner, `Owner::new` taking that box. No test here builds
//! an authority, a grant or a box another way.

#[path = "bench_support.rs"]
mod bench_support;

use std::fs;

use mqttiumd_contract::ids::{LogicalOwnerId, RunEpoch};
use mqttiumd_lab::bench::backend::Memory;
use mqttiumd_lab::bench::collector::{Collector, KindTable};
use mqttiumd_lab::bench::process::{Books, OWNER, Process};
use mqttiumd_lab::bench::setup::Setup;
use mqttiumd_lab::store::durable::MemoryStore;

use bench_support::root;

fn collector() -> Collector {
    let schema = mqttiumd_lab::strict::parse(
        &fs::read(root().join("contracts/schema/v1/trace-record.schema.json")).unwrap(),
    )
    .unwrap();
    Collector::new(
        "bench-f4",
        KindTable::from_schema(&schema).unwrap(),
        1 << 24,
    )
}

fn setup() -> Setup<MemoryStore> {
    let fixture = mqttiumd_lab::strict::parse(
        &fs::read(root().join("tests/fixtures/p1/three-persistent-sessions.json")).unwrap(),
    )
    .unwrap();
    Setup::from_fixture(&mut Memory, &fixture, "bench-f4", 1, 1).unwrap()
}

#[test]
fn the_real_start_up_builds_one_owner_and_no_second_of_its_identity() {
    let setup = setup();
    let (mut collector, mut books, mut choices) = (collector(), Books::new(0), Vec::new());
    let mut process = Process::start(
        setup.store,
        0,
        setup.settings,
        &[],
        &mut collector,
        &mut books,
        &mut choices,
    )
    .expect("the real start-up");
    assert_eq!(process.epoch(), RunEpoch::new(1));
    assert_eq!(process.active_owners(), 1);
    let second = process.create_owner(OWNER);
    assert_eq!(second.err().as_deref(), Some("OwnerAlreadyActive"));
    assert_eq!(
        process.active_owners(),
        1,
        "a refused creation changes nothing"
    );
    // S0 runs one owner: the authority of the runner has one entry, and
    // another identity is refused for that bound, not for its identity.
    assert_eq!(
        process
            .create_owner(LogicalOwnerId::new(1))
            .err()
            .as_deref(),
        Some("TableFull")
    );
    assert_eq!(process.active_owners(), 1);
}

#[test]
fn after_a_simulated_crash_the_new_process_opens_a_new_epoch_and_one_owner_again() {
    let setup = setup();
    let settings = setup.settings;
    let (mut collector, mut books, mut choices) = (collector(), Books::new(0), Vec::new());
    let mut first = Process::start(
        setup.store,
        0,
        settings,
        &[],
        &mut collector,
        &mut books,
        &mut choices,
    )
    .unwrap();
    first.recover(&mut collector, &mut books).unwrap();
    let (store, _) = first.crash(&mut collector, &mut books).unwrap();
    collector.restart_process();
    let mut second = Process::start(
        store,
        1,
        settings,
        &[],
        &mut collector,
        &mut books,
        &mut choices,
    )
    .unwrap();
    // The authority of the dead process is gone with it; the new one
    // belongs to the epoch the new opening committed, after the old one.
    assert_eq!(second.epoch(), RunEpoch::new(2));
    second.recover(&mut collector, &mut books).unwrap();
    assert_eq!(second.active_owners(), 1);
    assert_eq!(
        second.create_owner(OWNER).err().as_deref(),
        Some("OwnerAlreadyActive")
    );
}
