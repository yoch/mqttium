//! Bounded, exploratory K2a process kills. This is not an oracle campaign.
//!
//! A child announces one command and waits for its acknowledgement. The
//! parent releases it, waits a seeded delay, sends SIGKILL, requires wait
//! signal 9, and reopens the same database before comparing the entire
//! DurableState with the deterministic before/after references. A delay
//! cannot establish that the signal interrupted SQLite COMMIT.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mqttiumd_contract::capacity::{PayloadBytes, ReceiptSlots, References};
use mqttiumd_contract::command::Command as StoreCommand;
use mqttiumd_contract::packet::SharedBytes;
use mqttiumd_lab::manifest::sha256_hex;
use mqttiumd_lab::store::conformance::{
    World, accounts_match_what_is_held, altered, fixture_limits,
};
use mqttiumd_lab::store::durable::{DurableState, DurableStore, Limits, MemoryStore};
use mqttiumd_store_sqlite::SqliteStore;
use serde_json::{Value, json};

const PAYLOAD: usize = 65_536;
const PREFIX_PAIRS: u64 = 3;
const DEADLINE: Duration = Duration::from_secs(30);
const MAX_LINE: u64 = 65_536;
const CALIBRATION_DELAYS_US: [u64; 8] = [0, 25, 50, 100, 200, 400, 800, 1600];

#[derive(Clone, Copy)]
enum Target {
    Admit,
    Release,
}

impl Target {
    fn name(self) -> &'static str {
        match self {
            Self::Admit => "admission",
            Self::Release => "release",
        }
    }

    fn parse(text: &str) -> Result<Self, String> {
        match text {
            "admission" => Ok(Self::Admit),
            "release" => Ok(Self::Release),
            _ => Err(format!("unknown target: {text}")),
        }
    }
}

fn limits() -> Limits {
    Limits {
        payload_bytes: PayloadBytes::new(1 << 20),
        references: References::new(100),
        receipt_slots: ReceiptSlots::new(100),
        max_payload_bytes: PAYLOAD,
        ..fixture_limits()
    }
}

fn admission<S>(world: &World<S>, n: u64) -> StoreCommand {
    // The conformance fixture's byte builder is capped at 1024. Reuse its
    // identities and targets, then construct this explorer's larger recipe
    // and bytes with its own declared bound before anything is submitted.
    let mut command = world.admission(
        2 * n,
        u16::try_from(n).expect("bounded packet"),
        b"",
        &[world.b, world.c],
    );
    command = altered(&command, |recipe| {
        recipe.payload.length = PayloadBytes::new(PAYLOAD as u64);
        recipe.costs.payload_bytes = recipe.payload.length;
    });
    command.payload = Some(SharedBytes::copy_from(&vec![0x5a; PAYLOAD], PAYLOAD).expect("bounded"));
    command
}

fn prepare<S: DurableStore>(world: &mut World<S>, target: Target) -> StoreCommand {
    for n in 1..=PREFIX_PAIRS {
        let command = admission(world, n);
        assert!(world.store.apply(&command).committed_now);
        let release = world.release(2 * n + 1, u16::try_from(n).unwrap(), 2 * n);
        assert!(world.store.apply(&release).committed_now);
    }
    let n = PREFIX_PAIRS + 1;
    let command = admission(world, n);
    match target {
        Target::Admit => command,
        Target::Release => {
            assert!(world.store.apply(&command).committed_now);
            world.release(2 * n + 1, u16::try_from(n).unwrap(), 2 * n)
        }
    }
}

fn emit(value: Value) -> Result<(), String> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &value).map_err(|e| e.to_string())?;
    writeln!(stdout).map_err(|e| e.to_string())?;
    stdout.flush().map_err(|e| e.to_string())
}

fn child(path: &Path, target: Target) -> Result<(), String> {
    let mut world = World::with_limits(
        &|incarnation, limits| SqliteStore::create(path, incarnation, limits).expect("created"),
        limits(),
    );
    let pages = world
        .store
        .set_wal_autocheckpoint_for_measurement(16)
        .map_err(|e| format!("checkpoint setting: {e:?}"))?;
    if pages != 16 {
        return Err(format!("checkpoint threshold read back {pages}"));
    }
    let command = prepare(&mut world, target);
    let settings = world.store.effective_settings();
    emit(json!({
        "kind": "CommandSubmitted",
        "target": target.name(),
        "operation": format!("{:?}", command.recipe.operation()),
        "settings": {
            "sqlite_version": settings.sqlite_version,
            "compile_options": settings.compile_options,
            "journal_mode": settings.journal_mode,
            "synchronous": settings.synchronous,
            "page_size": settings.page_size,
            "wal_autocheckpoint": settings.wal_autocheckpoint,
            "kill_points": settings.kill_points,
            "wal_hook": "SQLite autocheckpoint; no custom hook installed",
        },
    }))?;
    let mut start = [0; 1];
    std::io::stdin()
        .read_exact(&mut start)
        .map_err(|e| e.to_string())?;
    if start != *b"G" {
        return Err("the parent did not acknowledge the command".to_owned());
    }
    let started = Instant::now();
    let applied = world.store.apply(&command);
    let elapsed = started.elapsed();
    if !applied.committed_now {
        return Err(format!("the target was not committed: {applied:?}"));
    }
    emit(json!({"kind": "CommitReturned", "apply_ns": elapsed.as_nanos().to_string()}))?;
    // Keep the writer and the same files alive after a fast commit. The
    // parent always kills it; EOF here is an error, not a simulated kill.
    std::io::stdin()
        .read_exact(&mut start)
        .map_err(|e| e.to_string())?;
    Err("unexpected command after the commit".to_owned())
}

struct Holder {
    child: Child,
    output: Receiver<Result<Value, String>>,
    reader: Option<JoinHandle<()>>,
}

impl Holder {
    fn spawn(path: &Path, target: Target) -> Result<Self, String> {
        let mut child = Command::new(std::env::current_exe().map_err(|e| e.to_string())?)
            .args(["--child", target.name()])
            .arg(path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| e.to_string())?;
        let stdout = child.stdout.take().expect("piped output");
        let (send, output) = mpsc::sync_channel(4);
        let reader = std::thread::spawn(move || {
            let mut input = BufReader::new(stdout);
            // Two frames at most: submitted and returned. The third would
            // be an explicit protocol error, so no unbounded stream/queue.
            for _ in 0..3 {
                let mut bytes = Vec::new();
                let result = (&mut input)
                    .take(MAX_LINE + 1)
                    .read_until(b'\n', &mut bytes);
                let frame = match result {
                    Ok(0) => break,
                    Ok(_) if bytes.len() as u64 > MAX_LINE || !bytes.ends_with(b"\n") => {
                        Err("child frame exceeds its bound or is incomplete".to_owned())
                    }
                    Ok(_) => serde_json::from_slice(&bytes).map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                };
                if send.send(frame).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            child,
            output,
            reader: Some(reader),
        })
    }

    fn submitted(&self) -> Result<Value, String> {
        let frame = self
            .output
            .recv_timeout(DEADLINE)
            .map_err(|e| format!("command deadline: {e}"))??;
        if frame["kind"] != "CommandSubmitted" {
            return Err(format!("expected CommandSubmitted: {frame}"));
        }
        Ok(frame)
    }

    fn release(&mut self) -> Result<(), String> {
        let stdin = self.child.stdin.as_mut().ok_or("closed child input")?;
        stdin.write_all(b"G").map_err(|e| e.to_string())?;
        stdin.flush().map_err(|e| e.to_string())
    }

    fn kill(&mut self) -> Result<Value, String> {
        self.child.kill().map_err(|e| format!("SIGKILL: {e}"))?;
        let deadline = Instant::now() + DEADLINE;
        let status = loop {
            if let Some(status) = self.child.try_wait().map_err(|e| e.to_string())? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err("wait status deadline expired".to_owned());
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        if status.signal() != Some(9) || status.code().is_some() {
            return Err(format!("expected signal 9 with no exit code: {status:?}"));
        }
        if let Some(reader) = self.reader.take() {
            reader.join().map_err(|_| "reader panicked")?;
        }
        Ok(json!({"signal_sent": "SIGKILL", "signal": status.signal(), "exit_code": status.code()}))
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn digest(state: &DurableState) -> String {
    // Debug includes every field, and the comparison below is direct Eq,
    // not equality of a projection or of these diagnostic hashes.
    sha256_hex(format!("{state:?}").as_bytes())
}

fn reference(target: Target) -> (DurableState, DurableState, String) {
    let mut world = World::with_limits(&MemoryStore::create, limits());
    let command = prepare(&mut world, target);
    let before = world.store.snapshot().expect("healthy snapshot");
    assert!(world.store.apply(&command).committed_now);
    let after = world.store.snapshot().expect("healthy snapshot");
    (before, after, format!("{:?}", command.recipe.operation()))
}

fn remove_store(path: &Path) -> Result<(), String> {
    for suffix in ["", "-wal", "-shm", ".writer-lock"] {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        match fs::remove_file(PathBuf::from(file)) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

fn trial(
    out: &Path,
    index: u64,
    delay_ns: u64,
    seed: u64,
    target: Target,
) -> Result<Value, String> {
    let (before, after, operation) = reference(target);
    let path = out.join(format!("trial-{index:04}.sqlite"));
    let mut holder = Holder::spawn(&path, target)?;
    let submitted = holder.submitted()?;
    if submitted["operation"] != operation || submitted["target"] != target.name() {
        return Err("the announced command is not the target".to_owned());
    }
    let released = Instant::now();
    holder.release()?;
    if delay_ns != 0 {
        std::thread::sleep(Duration::from_nanos(delay_ns));
    }
    let mut last = "CommandSubmitted";
    let mut apply_ns = None;
    for frame in holder.output.try_iter() {
        let frame = frame?;
        if frame["kind"] != "CommitReturned" || apply_ns.is_some() {
            return Err(format!("unexpected child report: {frame}"));
        }
        last = "CommitReturned";
        apply_ns = frame["apply_ns"].as_str().map(str::to_owned);
    }
    let elapsed_before_signal = released.elapsed().as_nanos();
    let status = holder.kill()?;
    // No open, copy, restoration or inspection of database/WAL/SHM occurs
    // between the actual signal, its wait status and this real reopen.
    let store = SqliteStore::open(&path).map_err(|e| format!("reopen after kill: {e:?}"))?;
    let recovered = store
        .try_snapshot()
        .map_err(|e| format!("snapshot: {e:?}"))?;
    accounts_match_what_is_held(&recovered).map_err(|e| format!("accounts: {e:?}"))?;
    let state = if recovered == before {
        "absent"
    } else if recovered == after {
        "found"
    } else {
        fs::write(
            out.join(format!("trial-{index:04}-mixed.debug")),
            format!("{recovered:#?}"),
        )
        .map_err(|e| e.to_string())?;
        "mixed"
    };
    let record = json!({
        "trial": index, "seed": seed.to_string(), "target": target.name(),
        "operation": operation, "last_observed_before_kill": last,
        "requested_delay_ns": delay_ns.to_string(),
        "elapsed_before_signal_ns": elapsed_before_signal.to_string(),
        "apply_ns_if_observed": apply_ns,
        "pid": holder.child.id(), "kill": status,
        "state": state, "full_durable_state_compared": true,
        "before_sha256": digest(&before), "after_sha256": digest(&after),
        "recovered_sha256": digest(&recovered),
        "quick_check": "ok (SqliteStore::open)",
        "settings": submitted["settings"],
        "established_position": if last == "CommitReturned" { "after COMMIT returned" } else { "released command, no commit position established" },
        "possible_position": if last == "CommitReturned" { "after COMMIT returned" } else { "before command execution, in command, inside COMMIT, or after COMMIT" },
        "error": Value::Null,
    });
    drop(store);
    if state != "mixed" {
        // Only after recovery, integrity check and full business equality.
        remove_store(&path)?;
    }
    Ok(record)
}

fn next(random: &mut u64) -> u64 {
    // SplitMix64: the generator's choices reproduce; OS scheduling does not.
    *random = random.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *random;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn uniform_delay(random: &mut u64, max_us: u64) -> Result<u64, String> {
    let width = max_us * 1000 + 1;
    let ceiling = u64::MAX - u64::MAX % width;
    // Reject the incomplete last bucket, so the requested delay has no
    // modulo bias. The retry budget is explicit, including generator use.
    for _ in 0..16 {
        let value = next(random);
        if value < ceiling {
            return Ok(value % width);
        }
    }
    Err("uniform-delay generator exhausted its retry budget".to_owned())
}

fn run(args: &[String]) -> Result<(), String> {
    if args.len() != 5 {
        return Err(
            "usage: kill_sweep <calibration|campaign> <out> <trials> <seed> <max-delay-us>"
                .to_owned(),
        );
    }
    let phase = &args[0];
    let out = PathBuf::from(&args[1]);
    let trials: u64 = args[2].parse().map_err(|_| "invalid trials")?;
    let seed: u64 = args[3].parse().map_err(|_| "invalid seed")?;
    let max_us: u64 = args[4].parse().map_err(|_| "invalid delay")?;
    if !matches!(phase.as_str(), "calibration" | "campaign")
        || trials == 0
        || trials > 1000
        || max_us > 100_000
    {
        return Err("invalid bounded campaign parameters".to_owned());
    }
    if phase == "calibration" && trials != 64 {
        return Err("calibration budget is exactly 64 trials".to_owned());
    }
    fs::create_dir_all(&out).map_err(|e| e.to_string())?;
    if fs::read_dir(&out)
        .map_err(|e| e.to_string())?
        .next()
        .is_some()
    {
        return Err("output directory must be empty".to_owned());
    }
    let parameters = json!({
        "format": "mqttiumd.k2a-exploration.v1", "phase": phase,
        "trials": trials, "seed": seed.to_string(), "max_delay_us": max_us,
        "distribution": if phase == "calibration" { "eight fixed grid delays, eight repetitions each" } else { "uniform integer nanoseconds in [0,max_delay_us*1000]" },
        "calibration_grid_us": CALIBRATION_DELAYS_US,
        "prefix_admission_release_pairs": PREFIX_PAIRS, "payload_bytes": PAYLOAD,
        "fanout": 2, "wal_autocheckpoint": 16, "command_deadline_seconds": DEADLINE.as_secs(),
        "positions": "last observed before signal; no claim of interruption inside COMMIT",
        "comparison": "complete DurableState Eq before opening a new epoch; includes ledger, revisions, identities, routes, receipts, payloads, publications, inbound slots, obligations, accounts and limits",
        "files": "same files retained without intervention from kill through reopen; removed only after comparison unless mixed",
        "limit": "process kill; kernel and page cache survive; not power loss or torn writes; no oracle verdict",
    });
    fs::write(
        out.join("parameters.json"),
        serde_json::to_vec_pretty(&parameters).unwrap(),
    )
    .map_err(|e| e.to_string())?;
    for target in [Target::Admit, Target::Release] {
        let (before, after, _) = reference(target);
        for (label, state) in [("before", before), ("after", after)] {
            fs::write(
                out.join(format!("reference-{}-{label}.debug", target.name())),
                format!("{state:#?}"),
            )
            .map_err(|e| e.to_string())?;
        }
    }
    let mut log = File::create(out.join("trials.jsonl")).map_err(|e| e.to_string())?;
    let mut random = seed;
    let mut found = 0;
    let mut absent = 0;
    let mut mixed = 0;
    let mut errors = 0;
    let mut after_return = 0;
    let mut found_without_return_observed = 0;
    for index in 0..trials {
        let target = if index % 2 == 0 {
            Target::Admit
        } else {
            Target::Release
        };
        let delay_ns = if phase == "calibration" {
            CALIBRATION_DELAYS_US[(index / 8) as usize] * 1000
        } else {
            uniform_delay(&mut random, max_us)?
        };
        let record = match trial(&out, index, delay_ns, seed, target) {
            Ok(record) => {
                match record["state"].as_str() {
                    Some("found") => found += 1,
                    Some("absent") => absent += 1,
                    _ => mixed += 1,
                }
                if record["last_observed_before_kill"] == "CommitReturned" {
                    after_return += 1;
                } else if record["state"] == "found" {
                    found_without_return_observed += 1;
                }
                record
            }
            Err(error) => {
                errors += 1;
                json!({"trial": index, "seed": seed.to_string(), "target": target.name(), "requested_delay_ns": delay_ns.to_string(), "error": error})
            }
        };
        serde_json::to_writer(&mut log, &record).map_err(|e| e.to_string())?;
        writeln!(log).map_err(|e| e.to_string())?;
        log.flush().map_err(|e| e.to_string())?;
        if (index + 1) % 100 == 0 {
            eprintln!(
                "{}: {}/{} found={} absent={} mixed={} errors={}",
                phase,
                index + 1,
                trials,
                found,
                absent,
                mixed,
                errors
            );
        }
    }
    let summary = json!({
        "phase": phase, "trials": trials, "found": found, "absent": absent,
        "mixed": mixed, "errors": errors, "usable": found + absent,
        "position_after_commit_return_established": after_return,
        "position_inside_commit_established": 0,
        "other_positions_only_possible": trials - after_return - errors,
        "found_without_commit_return_observed": found_without_return_observed,
        "all_outcome_diversity_quotas_met": found >= 100 && absent >= 100,
        "in_flight_observation_diversity_quotas_met": found_without_return_observed >= 100 && absent >= 100,
        "verdict": "exploratory data; not an oracle verdict or a universal atomicity proof",
    });
    fs::write(
        out.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .map_err(|e| e.to_string())?;
    println!("{summary}");
    if mixed != 0 || errors != 0 {
        return Err("exploration contains a mixed state or execution errors".to_owned());
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = if args.first().map(String::as_str) == Some("--child") && args.len() == 3 {
        Target::parse(&args[1]).and_then(|target| child(Path::new(&args[2]), target))
    } else {
        run(&args)
    };
    if let Err(error) = result {
        eprintln!("kill sweep: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sweep_command_uses_the_declared_large_payload_and_commits() {
        let mut world = World::with_limits(&MemoryStore::create, limits());
        let command = admission(&world, 1);
        assert_eq!(command.payload.as_ref().unwrap().len(), PAYLOAD);
        assert!(world.store.apply(&command).committed_now);
        assert_eq!(
            world
                .store
                .snapshot()
                .expect("healthy snapshot")
                .accounts
                .payload_bytes
                .get(),
            PAYLOAD as u64
        );
    }
}
