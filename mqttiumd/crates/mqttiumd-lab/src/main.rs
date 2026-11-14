//! Command line of the mqttiumd bench.
//!
//! Lot L0 implements `validate` and `manifest`. `run`, `replay` and
//! `minimize` are not implemented yet and are refused as such; a command
//! that does not exist never reports a verdict.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use mqttiumd_lab::diagnostic::Report;
use mqttiumd_lab::manifest;
use mqttiumd_lab::workspace::Workspace;

const USAGE: &str = "\
usage:
  mqttiumd-lab validate --contracts <dir> [--scenario <file>]... [--file <file>]... [--root <dir>]
  mqttiumd-lab manifest [--root <dir>]

Paths are relative to --root, which defaults to the current directory.
Exit codes: 0 PASS, 1 FAIL, 2 INCONCLUSIVE, 3 OUT_OF_SCOPE (DEM-023).
validate exits 0 when every document is accepted and 2 otherwise.";

/// Invalid configuration prevents a conclusion (DEM-023).
const INCONCLUSIVE: u8 = 2;

struct Arguments {
    root: PathBuf,
    contracts: Option<PathBuf>,
    documents: Vec<PathBuf>,
}

fn parse(arguments: &[String]) -> Result<Arguments, String> {
    let mut parsed = Arguments {
        root: PathBuf::from("."),
        contracts: None,
        documents: Vec::new(),
    };
    let mut rest = arguments.iter();
    while let Some(flag) = rest.next() {
        let value = rest.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--root" => parsed.root = PathBuf::from(value),
            "--contracts" => parsed.contracts = Some(PathBuf::from(value)),
            "--scenario" | "--file" => parsed.documents.push(PathBuf::from(value)),
            _ => return Err(format!("unknown option {flag}")),
        }
    }
    Ok(parsed)
}

fn show(path: &Path, report: &Report) -> bool {
    for note in &report.notes {
        eprintln!("note  {}: {note}", path.display());
    }
    for diagnostic in &report.diagnostics {
        eprintln!("error {}: {diagnostic}", path.display());
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

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let Some((command, rest)) = arguments.split_first() else {
        eprintln!("{USAGE}");
        return ExitCode::from(INCONCLUSIVE);
    };
    let outcome = match command.as_str() {
        "validate" => parse(rest).and_then(|arguments| validate(&arguments)),
        "manifest" => parse(rest)
            .and_then(|arguments| manifest::collect(&arguments.root))
            .and_then(|manifest| {
                serde_json::to_string_pretty(&manifest).map_err(|error| error.to_string())
            })
            .map(|text| {
                println!("{text}");
                true
            }),
        "run" | "replay" | "minimize" => Err(format!(
            "{command} is not implemented in lot L0; it reports no verdict"
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
