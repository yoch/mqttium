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
    // The manifest ties the binary to that source (decision 0017): the
    // archive as a file of the campaign, and the command that built it.
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("run-manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["artifact"]["built_from"],
        serde_json::json!({
            "source": {
                "path": "source.tar",
                "sha256": mqttiumd_lab::manifest::sha256_hex(&tar),
                "bytes": tar.len().to_string(),
            },
            "build": campaign["built_from"]["build"],
        })
    );
    let validated = Command::new(env!("CARGO_BIN_EXE_mqttiumd-lab"))
        .args(["validate", "--contracts", "contracts/v1"])
        .args(["--file", "run-manifest.json"])
        .arg("--root")
        .arg(&out)
        .output()
        .expect("validate runs");
    assert!(
        validated.status.success(),
        "{}{}",
        String::from_utf8_lossy(&validated.stdout),
        String::from_utf8_lossy(&validated.stderr)
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

#[test]
fn a_campaign_whose_binary_was_not_rebuilt_ties_it_to_no_source() {
    let campaign = bench_support::run_scenario("tests/scenarios/p1/P1-02.json", 1, "not-rebuilt");
    let manifest: serde_json::Value =
        serde_json::from_slice(&campaign.file("run-manifest.json")).unwrap();
    assert!(manifest["artifact"].is_object());
    assert!(
        manifest["artifact"].get("built_from").is_none(),
        "{}",
        manifest["artifact"]
    );
    assert!(!campaign.out.join("source.tar").exists());
}
