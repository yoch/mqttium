//! `run --rebuild`: the binary that runs the campaign is built from the
//! source the campaign holds, and that source builds the same binary
//! again (`tools/check_campaign_binary.sh`).

#[path = "bench_support.rs"]
mod bench_support;

use std::process::Command;

#[test]
fn a_rebuilt_campaign_holds_the_source_of_its_binary_and_that_source_builds_it_again() {
    let root = bench_support::root();
    let out = bench_support::fresh("rebuild");
    let status = Command::new(env!("CARGO_BIN_EXE_mqttiumd-lab"))
        .args(["run", "--contracts", "contracts/v1"])
        .args(["--scenario", "tests/scenarios/p1/P1-02.json"])
        .arg("--out")
        .arg(&out)
        .arg("--rebuild")
        .current_dir(&root)
        .status()
        .expect("the lab runs");
    assert!(status.success(), "run --rebuild");
    let campaign: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("campaign.json")).unwrap()).unwrap();
    let tar = std::fs::read(out.join("source.tar")).unwrap();
    assert_eq!(
        campaign["built_from"]["source"]["sha256"],
        mqttiumd_lab::manifest::sha256_hex(&tar).as_str()
    );
    let check = Command::new(root.join("tools/check_campaign_binary.sh"))
        .arg(&out)
        .current_dir(&root)
        .output()
        .expect("the check runs");
    let _ = std::fs::remove_dir_all(&out);
    assert!(
        check.status.success(),
        "{}{}",
        String::from_utf8_lossy(&check.stdout),
        String::from_utf8_lossy(&check.stderr)
    );
}
