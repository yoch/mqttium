use super::*;
fn assert_control<T: std::fmt::Debug>(
    result: &Result<T, Stop>,
    code: ControlCode,
    phase: ControlPhase,
) {
    assert_eq!(
        result
            .as_ref()
            .expect_err("the malformed control must be refused")
            .failure,
        Some(super::super::failure::Failure::Control {
            code,
            phase,
            process_incarnation: 1
        })
    );
}
fn fixture() -> (Child, mpsc::SyncSender<Arrival>, Collector, Books) {
    let process = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    fixture_from_process(process)
}

fn fixture_from_process(
    mut process: std::process::Child,
) -> (Child, mpsc::SyncSender<Arrival>, Collector, Books) {
    let actions = process.stdin.take().unwrap();
    let pid = process.id();
    let (sender, received) = mpsc::sync_channel(ARRIVAL_CAPACITY);
    if let Some(output) = process.stdout.take() {
        reader(output, None, sender.clone());
    }
    let schema = crate::strict::parse(include_bytes!(
        "../../../../contracts/schema/v1/trace-record.schema.json"
    ))
    .unwrap();
    let collector = Collector::new("review", KindTable::from_schema(&schema).unwrap(), 1 << 20);
    let child = Child {
        process,
        pid,
        actions: Some(BufWriter::new(actions)),
        received,
        sequencer: Sequencer::new(),
        eligible: Vec::new(),
        state: State::default(),
        base_hits: Vec::new(),
        epoch: 1,
        rank: 0,
        settings: Value::Null,
        _sockets: SocketDirectory(std::env::temp_dir().join(format!("mqttiumd-review-io-{pid}"))),
        silence: Duration::from_secs(2),
        ended: true,
        parked: None,
        next_action: 0,
    };
    (child, sender, collector, Books::new(0))
}
#[test]
fn repeated_contradictory_ends_exceeding_report_budget_are_refused() {
    let (mut child, sender, mut collector, mut books) = fixture();
    for _ in 0..=REPORTS_PER_ACTION {
        sender
            .send(Arrival::Report(Report::EndOfAction {
                action_sequence: 0,
                last_emission: Some(0),
            }))
            .unwrap();
    }
    sender
        .send(Arrival::Report(Report::EndOfAction {
            action_sequence: 0,
            last_emission: None,
        }))
        .unwrap();
    drop(sender);
    let result = child.act(&Action::Settle, &mut collector, &mut books);
    assert_control(
        &result,
        ControlCode::DuplicateTermination,
        ControlPhase::Action,
    );
}
#[test]
fn broken_evidence_after_signal_is_not_silently_discarded() {
    let (child, sender, _, _) = fixture();
    let pid = child.pid();
    std::thread::spawn(move || {
        let start = Instant::now();
        while std::path::Path::new(&format!("/proc/{pid}")).exists() {
            assert!(start.elapsed() < Duration::from_secs(2));
            std::thread::sleep(Duration::from_millis(1));
        }
        let mut bytes = 64u32.to_be_bytes().to_vec();
        bytes.push(1);
        let refusal = read_frame(&mut bytes.as_slice()).unwrap_err();
        sender.send(Arrival::Broken(refusal)).unwrap();
    });
    let result = child.kill();
    assert_control(
        &result,
        ControlCode::TruncatedFrame,
        ControlPhase::KillDrain,
    );
}

#[test]
fn one_end_cannot_erase_the_evidence_promised_by_an_earlier_end() {
    let (mut child, sender, mut collector, mut books) = fixture();
    sender
        .send(Arrival::Report(Report::EndOfAction {
            action_sequence: 0,
            last_emission: Some(0),
        }))
        .unwrap();
    sender
        .send(Arrival::Report(Report::EndOfAction {
            action_sequence: 0,
            last_emission: None,
        }))
        .unwrap();
    drop(sender);
    assert_control(
        &child.act(&Action::Settle, &mut collector, &mut books),
        ControlCode::DuplicateTermination,
        ControlPhase::Action,
    );
}

#[test]
fn termination_reports_are_in_the_same_bounded_control_budget() {
    let (mut child, sender, mut collector, mut books) = fixture();
    sender
        .send(Arrival::Report(Report::EndOfAction {
            action_sequence: 0,
            last_emission: Some(0),
        }))
        .unwrap();
    for _ in 1..REPORTS_PER_ACTION {
        sender
            .send(Arrival::Report(Report::Eligible {
                choices: vec![],
                digest: choices_digest(&[]),
            }))
            .unwrap();
    }
    sender
        .send(Arrival::Report(Report::EndOfAction {
            action_sequence: 0,
            last_emission: None,
        }))
        .unwrap();
    drop(sender);
    assert_control(
        &child.act(&Action::Settle, &mut collector, &mut books),
        ControlCode::ReportBudgetExceeded,
        ControlPhase::Action,
    );
}

#[test]
fn reports_without_evidence_progress_do_not_reset_the_action_deadline() {
    let (mut child, sender, mut collector, mut books) = fixture();
    child.silence = Duration::from_millis(200);
    let producer = std::thread::spawn(move || {
        for _ in 0..6 {
            if sender
                .send(Arrival::Report(Report::Eligible {
                    choices: vec![],
                    digest: choices_digest(&[]),
                }))
                .is_err()
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = sender.send(Arrival::Report(Report::EndOfAction {
            action_sequence: 0,
            last_emission: None,
        }));
    });
    let result = child.act(&Action::Settle, &mut collector, &mut books);
    drop(child);
    producer.join().unwrap();
    assert_control(
        &result,
        ControlCode::ActionDeadlineExceeded,
        ControlPhase::Action,
    );
}

#[test]
fn an_actual_unix_reader_preserves_a_truncated_frame_after_real_kill() {
    use std::io::Write;
    let (child, sender, _, _) = fixture();
    let pid = child.pid();
    let (input, mut output) =
        UnixStream::pair().expect("Unix socket pair for the actual frame reader");
    reader(input, Some(Origin::BrokerCore), sender.clone());
    drop(sender);
    let producer = std::thread::spawn(move || {
        let start = Instant::now();
        while std::path::Path::new(&format!("/proc/{pid}")).exists() {
            assert!(start.elapsed() < Duration::from_secs(2));
            std::thread::sleep(Duration::from_millis(1));
        }
        output.write_all(&64_u32.to_be_bytes()).unwrap();
        output.write_all(&[1]).unwrap();
    });
    let result = child.kill();
    producer.join().unwrap();
    assert_control(
        &result,
        ControlCode::TruncatedFrame,
        ControlPhase::KillDrain,
    );
}

#[test]
fn a_drain_failure_keeps_the_confirmed_signal_nine_receipt() {
    let (child, sender, _, _) = fixture();
    let pid = child.pid();
    let producer = std::thread::spawn(move || {
        let start = Instant::now();
        while std::path::Path::new(&format!("/proc/{pid}")).exists() {
            assert!(start.elapsed() < Duration::from_secs(2));
            std::thread::sleep(Duration::from_millis(1));
        }
        let mut bytes = 64_u32.to_be_bytes().to_vec();
        bytes.push(1);
        let refusal = read_frame(&mut bytes.as_slice()).unwrap_err();
        sender.send(Arrival::Broken(refusal)).unwrap();
    });
    let attempt = child.kill_observed();
    producer.join().unwrap();
    assert_control(
        &attempt.result,
        ControlCode::TruncatedFrame,
        ControlPhase::KillDrain,
    );
    let observed = attempt
        .observed
        .expect("the real SIGKILL is not erased by collection failure");
    assert_eq!(observed.pid, pid);
    assert_eq!(observed.wait_signal, 9);
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}

#[test]
fn the_actual_unix_reader_accepts_a_clean_eof_after_real_kill() {
    let (child, sender, _, _) = fixture();
    let pid = child.pid();
    let (input, output) = UnixStream::pair().expect("Unix socket pair");
    reader(input, Some(Origin::BrokerCore), sender.clone());
    drop(sender);
    let producer = std::thread::spawn(move || {
        let start = Instant::now();
        while std::path::Path::new(&format!("/proc/{pid}")).exists() {
            assert!(start.elapsed() < Duration::from_secs(2));
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(output);
    });
    let result = child.kill();
    producer.join().unwrap();
    assert_eq!(
        result
            .expect("clean EOF does not invent protocol damage")
            .wait_signal,
        9
    );
}

#[test]
fn a_duplicate_empty_end_cannot_complete_a_second_action() {
    let (mut child, sender, mut collector, mut books) = fixture();
    sender
        .send(Arrival::Report(Report::EndOfAction {
            action_sequence: 0,
            last_emission: None,
        }))
        .unwrap();
    sender
        .send(Arrival::Report(Report::EndOfAction {
            action_sequence: 0,
            last_emission: None,
        }))
        .unwrap();
    drop(sender);
    let first = child.act(&Action::Settle, &mut collector, &mut books);
    let second = child.act(&Action::Settle, &mut collector, &mut books);
    assert!(first.is_ok(), "the first empty action is valid");
    assert_control(
        &second,
        ControlCode::DuplicateTermination,
        ControlPhase::Action,
    );
}

#[test]
fn successive_empty_actions_with_distinct_serials_are_valid() {
    let (mut child, sender, mut collector, mut books) = fixture();
    for action_sequence in 0..2 {
        sender
            .send(Arrival::Report(Report::EndOfAction {
                action_sequence,
                last_emission: None,
            }))
            .unwrap();
    }
    assert!(
        child
            .act(&Action::Settle, &mut collector, &mut books)
            .is_ok()
    );
    assert!(
        child
            .act(&Action::Settle, &mut collector, &mut books)
            .is_ok()
    );
}

#[test]
fn a_parked_report_cannot_reuse_the_serial_of_an_earlier_action() {
    let (mut child, sender, mut collector, mut books) = fixture();
    child.next_action = 1;
    sender
        .send(Arrival::Report(Report::Parked {
            action_sequence: 0,
            last_emission: 0,
            fault: 0,
            point: "store.commit.prepared".to_owned(),
            settings: "{}".to_owned(),
        }))
        .unwrap();
    assert_control(
        &child.act(&Action::Settle, &mut collector, &mut books),
        ControlCode::DuplicateTermination,
        ControlPhase::Action,
    );
}

#[test]
fn duplicate_terminations_cross_the_actual_unix_control_reader() {
    let (mut child, sender, mut collector, mut books) = fixture();
    let (input, mut output) = UnixStream::pair().expect("Unix control socket pair");
    reader(input, None, sender);
    for last_emission in [Some(0), None] {
        write_frame(
            &mut output,
            &Report::EndOfAction {
                action_sequence: 0,
                last_emission,
            }
            .encode(),
        )
        .unwrap();
    }
    drop(output);
    assert_control(
        &child.act(&Action::Settle, &mut collector, &mut books),
        ControlCode::DuplicateTermination,
        ControlPhase::Action,
    );
}

/// Reads a complete Stop frame, emits the specified terminations through the
/// real control descriptor, then exits 0. No timeout or broken write is needed.
fn stopping_fixture(terminations: u8) -> (Child, mpsc::SyncSender<Arrival>, Collector, Books) {
    let process = Command::new("python3")
        .arg("-c")
        .arg(
            r#"
import sys
length = int.from_bytes(sys.stdin.buffer.read(4), "big")
assert sys.stdin.buffer.read(length) == bytes([11]), "expected the Stop action"
body = bytes.fromhex(sys.argv[2])
sys.stdout.buffer.write((len(body).to_bytes(4, "big") + body) * int(sys.argv[1]))
sys.stdout.buffer.flush()
sys.exit(0)
"#,
        )
        .arg(terminations.to_string())
        .arg(
            Report::EndOfAction {
                action_sequence: 0,
                last_emission: None,
            }
            .encode()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    fixture_from_process(process)
}

#[test]
fn a_successful_final_stop_refuses_a_duplicate_termination_before_eof() {
    let (child, sender, mut collector, mut books) = stopping_fixture(2);
    let pid = child.pid();
    drop(sender);
    let result = child.stop(&mut collector, &mut books);
    assert_control(
        &result,
        ControlCode::DuplicateTermination,
        ControlPhase::Stop,
    );
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "the normal exit was reaped"
    );
}

#[test]
fn a_successful_final_stop_accepts_a_clean_control_eof() {
    let (child, sender, mut collector, mut books) = stopping_fixture(1);
    let pid = child.pid();
    drop(sender);
    child
        .stop(&mut collector, &mut books)
        .expect("one complete termination and a normal exit is healthy");
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "the normal exit was reaped"
    );
}
