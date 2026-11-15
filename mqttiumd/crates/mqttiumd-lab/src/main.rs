//! Command line of the mqttiumd bench.
//!
//! Lot L0 implements `validate` and `manifest`; lot L4 implements `run`,
//! which plays a scenario and leaves a campaign directory for the oracle.
//! `replay` and `minimize` are not implemented yet and are refused as
//! such; a command that does not exist never reports a verdict, and `run`
//! reports none either.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use mqttiumd_lab::bench::campaign;
use mqttiumd_lab::diagnostic::Report;
use mqttiumd_lab::manifest;
use mqttiumd_lab::workspace::Workspace;

const USAGE: &str = "\
usage:
  mqttiumd-lab validate --contracts <dir> [--scenario <file>]... [--file <file>]... [--root <dir>]
  mqttiumd-lab manifest [--contracts <dir>] [--root <dir>]
  mqttiumd-lab run --contracts <dir> --scenario <file> --out <dir> [--seed <n>] [--run-id <slug>] [--root <dir>]

Paths are relative to --root, which defaults to the current directory.
Exit codes: 0 PASS, 1 FAIL, 2 INCONCLUSIVE, 3 OUT_OF_SCOPE (DEM-023).
validate exits 0 when every document is accepted and 2 otherwise.
run computes no verdict: it exits 0 when the campaign directory was
written, whatever the oracle will find in it, and 2 when no campaign
could be written. The oracle judges the directory (--root <out>).";

/// Invalid configuration prevents a conclusion (DEM-023).
const INCONCLUSIVE: u8 = 2;

struct Arguments {
    root: PathBuf,
    contracts: Option<PathBuf>,
    documents: Vec<PathBuf>,
    out: Option<PathBuf>,
    seed: Option<u64>,
    run_id: Option<String>,
}

fn parse(arguments: &[String]) -> Result<Arguments, String> {
    let mut parsed = Arguments {
        root: PathBuf::from("."),
        contracts: None,
        documents: Vec::new(),
        out: None,
        seed: None,
        run_id: None,
    };
    let mut rest = arguments.iter();
    while let Some(flag) = rest.next() {
        let value = rest.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--root" => parsed.root = PathBuf::from(value),
            "--contracts" => parsed.contracts = Some(PathBuf::from(value)),
            "--scenario" | "--file" => parsed.documents.push(PathBuf::from(value)),
            "--out" => parsed.out = Some(PathBuf::from(value)),
            "--seed" => {
                parsed.seed = Some(
                    mqttiumd_contract::decimal::parse_u64(value)
                        .map_err(|_| format!("--seed {value} is not a 64-bit decimal"))?,
                );
            }
            "--run-id" => parsed.run_id = Some(value.clone()),
            _ => return Err(format!("unknown option {flag}")),
        }
    }
    Ok(parsed)
}

fn show(path: &Path, report: &Report) -> bool {
    for note in &report.notes {
        eprintln!("note  {}: {note}", path.display());
    }
    if report.omitted_notes > 0 {
        eprintln!(
            "note  {}: {} more notes omitted",
            path.display(),
            report.omitted_notes
        );
    }
    for diagnostic in &report.diagnostics {
        eprintln!("error {}: {diagnostic}", path.display());
    }
    if report.omitted_diagnostics > 0 {
        eprintln!(
            "error {}: {} more diagnostics omitted",
            path.display(),
            report.omitted_diagnostics
        );
    }
    report.is_accepted()
}

fn validate(arguments: &Arguments) -> Result<bool, String> {
    let contracts = arguments
        .contracts
        .as_ref()
        .ok_or("validate needs --contracts")?;
    let workspace =
        Workspace::open(&arguments.root, contracts).map_err(|diagnostic| diagnostic.to_string())?;
    println!("contracts sha256:{}", workspace.contracts_digest());
    let mut accepted = true;
    let mut checked = 0usize;
    for (path, report) in workspace.check_contracts() {
        accepted &= show(&path, &report);
        checked += 1;
    }
    for path in &arguments.documents {
        accepted &= show(path, &workspace.check_file(path));
        checked += 1;
    }
    println!(
        "{}: {checked} documents checked",
        if accepted { "accepted" } else { "refused" }
    );
    Ok(accepted)
}

fn run(arguments: &Arguments) -> Result<bool, String> {
    let [scenario] = arguments.documents.as_slice() else {
        return Err("run needs exactly one --scenario".to_owned());
    };
    let request = campaign::Request {
        root: arguments.root.clone(),
        contracts: arguments.contracts.clone().ok_or("run needs --contracts")?,
        scenario: scenario.clone(),
        out: arguments.out.clone().ok_or("run needs --out")?,
        seed: arguments.seed.unwrap_or(1),
        run_id: arguments.run_id.clone(),
    };
    let report = campaign::execute(&request)?;
    println!("contracts sha256:{}", report.contracts_digest);
    println!(
        "campaign {} written to {}",
        report.run_id,
        report.out.display()
    );
    if let Some(stopped) = &report.stopped {
        println!("the run stopped: {stopped}");
    }
    for fault in &report.not_reached {
        println!("not reached: {fault}");
    }
    println!("no verdict: judge it with");
    println!("  {}", report.judge);
    Ok(true)
}

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let Some((command, rest)) = arguments.split_first() else {
        eprintln!("{USAGE}");
        return ExitCode::from(INCONCLUSIVE);
    };
    let outcome = match command.as_str() {
        "validate" => parse(rest).and_then(|arguments| validate(&arguments)),
        "manifest" => parse(rest)
            .and_then(|arguments| {
                let contracts = arguments
                    .contracts
                    .unwrap_or_else(|| PathBuf::from("contracts/v1"));
                manifest::collect(&arguments.root, &contracts)
            })
            .and_then(|manifest| {
                serde_json::to_string_pretty(&manifest).map_err(|error| error.to_string())
            })
            .map(|text| {
                println!("{text}");
                true
            }),
        "run" => parse(rest).and_then(|arguments| run(&arguments)),
        "replay" | "minimize" => Err(format!(
            "{command} is not implemented yet; it reports no verdict"
        )),
        _ => Err(format!("unknown command {command}\n{USAGE}")),
    };
    match outcome {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(INCONCLUSIVE),
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::from(INCONCLUSIVE)
        }
    }
}
