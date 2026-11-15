//! A campaign is generated: the runner refuses to write it inside the
//! work tree of the repository, where it would count as a modification of
//! the checkout it describes. Nothing is written.

#[path = "bench_support.rs"]
mod bench_support;

use std::path::PathBuf;

use mqttiumd_lab::bench::campaign::{Request, execute};

fn request(out: PathBuf) -> Request {
    Request {
        root: bench_support::root(),
        contracts: PathBuf::from("contracts/v1"),
        scenario: PathBuf::from("tests/scenarios/p1/P1-02.json"),
        out,
        seed: 1,
        run_id: Some("bench-test".to_owned()),
        backend: "sim-store".to_owned(),
        built_from: None,
        process: "in-process".to_owned(),
    }
}

#[test]
fn a_campaign_directory_inside_the_work_tree_is_refused_and_nothing_is_written() {
    let name = format!("campaign-in-tree-{}", std::process::id());
    for out in [
        bench_support::root().join("target").join(&name),
        bench_support::root().join("..").join(&name),
        bench_support::root().join(&name).join("deeper"),
    ] {
        let refused = execute(&request(out.clone())).expect_err("refused");
        assert!(refused.contains("inside the work tree"), "{refused}");
        assert!(!out.exists(), "{} was written", out.display());
    }
    assert!(!bench_support::root().join(&name).exists());
}

#[test]
fn a_campaign_directory_outside_the_work_tree_is_written() {
    let out = bench_support::fresh("out-of-tree");
    let report = execute(&request(out.clone())).expect("written");
    assert_eq!(report.stopped, None);
    let _ = std::fs::remove_dir_all(&out);
}
