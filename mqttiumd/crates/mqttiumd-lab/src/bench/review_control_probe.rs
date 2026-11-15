use super::*;
fn fixture() -> (Child, mpsc::SyncSender<Arrival>, Collector, Books) {
    let mut process = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let actions = process.stdin.take().unwrap();
    let pid = process.id();
    let (sender, received) = mpsc::sync_channel(ARRIVAL_CAPACITY);
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
    };
    (child, sender, collector, Books::new(0))
}
#[test]
fn repeated_contradictory_ends_exceeding_report_budget_are_refused() {
    let (mut child, sender, mut collector, mut books) = fixture();
    for _ in 0..=REPORTS_PER_ACTION {
        sender
            .send(Arrival::Report(Report::EndOfAction {
                last_emission: Some(0),
            }))
            .unwrap();
    }
    sender
        .send(Arrival::Report(Report::EndOfAction {
            last_emission: None,
        }))
        .unwrap();
    drop(sender);
    let result = child.act(&Action::Settle, &mut collector, &mut books);
    assert!(
        result.is_err(),
        "accepted 65 EndOfAction(Some(0)) followed by EndOfAction(None) with no evidence: {result:?}"
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
        sender
            .send(Arrival::Broken(format!("broker-core: {refusal}")))
            .unwrap();
    });
    let result = child.kill();
    assert!(
        result.is_err(),
        "accepted kill although its evidence reader reported a truncated frame: {result:?}"
    );
}

#[test]
fn one_end_cannot_erase_the_evidence_promised_by_an_earlier_end() {
    let (mut child, sender, mut collector, mut books) = fixture();
    sender
        .send(Arrival::Report(Report::EndOfAction {
            last_emission: Some(0),
        }))
        .unwrap();
    sender
        .send(Arrival::Report(Report::EndOfAction {
            last_emission: None,
        }))
        .unwrap();
    drop(sender);
    assert!(
        child
            .act(&Action::Settle, &mut collector, &mut books)
            .is_err()
    );
}

#[test]
fn termination_reports_are_in_the_same_bounded_control_budget() {
    let (mut child, sender, mut collector, mut books) = fixture();
    sender
        .send(Arrival::Report(Report::EndOfAction {
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
            last_emission: None,
        }))
        .unwrap();
    drop(sender);
    assert!(
        child
            .act(&Action::Settle, &mut collector, &mut books)
            .is_err()
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
            last_emission: None,
        }));
    });
    let result = child.act(&Action::Settle, &mut collector, &mut books);
    drop(child);
    producer.join().unwrap();
    assert!(
        result.is_err(),
        "reports without progress kept an action alive: {result:?}"
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
    assert!(
        result.is_err(),
        "the real Unix frame reader's truncation was dropped: {result:?}"
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
        sender
            .send(Arrival::Broken(format!("broker-core: {refusal}")))
            .unwrap();
    });
    let attempt = child.kill_observed();
    producer.join().unwrap();
    assert!(attempt.result.is_err());
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
    sender.send(Arrival::Report(Report::EndOfAction { last_emission: None })).unwrap();
    sender.send(Arrival::Report(Report::EndOfAction { last_emission: None })).unwrap();
    drop(sender);
    let first = child.act(&Action::Settle, &mut collector, &mut books);
    let second = child.act(&Action::Settle, &mut collector, &mut books);
    assert!(first.is_err() || second.is_err(), "two actions were accepted from one repeated empty terminator");
}
