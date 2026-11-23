//! A process this one forks holds a copy of each of its descriptors until
//! it executes its program, the writer lock of an open SQLite store among
//! them (`flock` on `<database>.writer-lock`). An in-process SQLite
//! campaign that restarts its store meanwhile meets its own lock and
//! stops (`WriterAlive`). The tests keep the two apart; this one spawns
//! processes without pause while SQLite campaigns restart.

#[path = "bench_support.rs"]
mod bench_support;

use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bench_support::{run_scenario_on, spawning};

#[test]
fn a_spawned_process_does_not_stop_an_in_process_sqlite_restart() {
    let done = Arc::new(AtomicBool::new(false));
    let spawner = {
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let mut spawned = 0_u64;
            while !done.load(Ordering::Relaxed) {
                let _turn = spawning();
                Command::new("true").status().expect("true");
                spawned += 1;
            }
            spawned
        })
    };
    for round in 0..8 {
        let campaign = run_scenario_on(
            "tests/scenarios/p1/P1-02.json",
            1,
            &format!("fork-guard-{round}"),
            "sqlite",
        );
        assert_eq!(campaign.report.stopped, None, "round {round}");
    }
    done.store(true, Ordering::Relaxed);
    assert!(spawner.join().expect("the spawner") > 0);
}
