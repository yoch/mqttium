//! The control protocol over real byte streams, with no process: a child
//! played by a thread emits more records than the window holds, on two
//! origin descriptors, waits for the acknowledgement of each decisive
//! record, and ends the action; the supervisor reads every descriptor,
//! acknowledges as it reads, and orders the batch. Neither side blocks on
//! a write the other is waiting to read.

use std::io::{BufReader, BufWriter, Write};
use std::os::unix::net::UnixStream;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use mqttiumd_lab::bench::protocol::{
    Action, Evidence, Report, Sequencer, WINDOW, Window, decisive, read_frame, write_frame,
};

/// How many records the action emits: well above the window, so that the
/// child must wait for acknowledgements more than once.
const RECORDS: u64 = 200;
const _: () = assert!(RECORDS > WINDOW);

/// The origin and kind of the record of emission `number`: a decisive
/// record now and then, rarely enough that the window fills between two.
fn record_of(number: u64) -> (&'static str, &'static str) {
    if number % 100 == 99 {
        ("store-evidence", "StoreCommitStable")
    } else {
        ("broker-core", "FrameEligible")
    }
}

/// The child: one action, `RECORDS` records, decisive ones waited for.
fn child(
    actions: UnixStream,
    core: UnixStream,
    store: UnixStream,
    control: UnixStream,
) -> Result<u64, String> {
    let mut actions = BufReader::new(actions);
    let mut core = BufWriter::new(core);
    let mut store = BufWriter::new(store);
    let mut control = BufWriter::new(control);
    let frame = read_frame(&mut actions).map_err(|error| error.to_string())?;
    let action = Action::decode(&frame.ok_or("no action")?).map_err(|error| error.to_string())?;
    if action != (Action::Start { rank: 0 }) {
        return Err(format!("unexpected {action:?}"));
    }
    let mut window = Window::new();
    let mut waits = 0;
    let mut read_ack = |window: &mut Window| -> Result<(), String> {
        let body = read_frame(&mut actions)
            .map_err(|error| error.to_string())?
            .ok_or("the supervisor went away")?;
        match Action::decode(&body).map_err(|error| error.to_string())? {
            Action::Acknowledge { through } => window.acknowledge(through),
            other => Err(format!("an action while one is outstanding: {other:?}")),
        }
    };
    for _ in 0..RECORDS {
        while !window.open() {
            waits += 1;
            read_ack(&mut window)?;
        }
        let number = window.emit().ok_or("the window is open")?;
        let (origin, kind) = record_of(number);
        let evidence = Evidence {
            emission: number,
            record: format!("{{\"kind\":\"{kind}\",\"n\":{number}}}").into_bytes(),
        };
        let stream: &mut BufWriter<UnixStream> = if origin == "store-evidence" {
            &mut store
        } else {
            &mut core
        };
        write_frame(stream, &evidence.encode()).map_err(|error| error.to_string())?;
        // Past a decisive record only once it is acknowledged.
        while !window.past(number, decisive(origin, kind)) {
            read_ack(&mut window)?;
        }
    }
    write_frame(
        &mut control,
        &Report::EndOfAction {
            action_sequence: 0,
            last_emission: Some(RECORDS - 1),
        }
        .encode(),
    )
    .map_err(|error| error.to_string())?;
    while !window.drained() {
        read_ack(&mut window)?;
    }
    Ok(waits)
}

enum Read {
    Evidence(&'static str, Evidence),
    Report(Report),
}

/// One reader per descriptor, so that a full descriptor never waits for
/// another to be read.
fn reader(stream: UnixStream, origin: Option<&'static str>, to: mpsc::Sender<Read>) {
    thread::spawn(move || {
        let mut stream = BufReader::new(stream);
        while let Ok(Some(body)) = read_frame(&mut stream) {
            let read = match origin {
                Some(origin) => Read::Evidence(origin, Evidence::decode(&body).unwrap()),
                None => Read::Report(Report::decode(&body).unwrap()),
            };
            if to.send(read).is_err() {
                return;
            }
        }
    });
}

#[test]
fn more_records_than_the_window_flow_without_a_deadlock_and_in_order() {
    let (supervisor_actions, child_actions) = UnixStream::pair().unwrap();
    let (supervisor_core, child_core) = UnixStream::pair().unwrap();
    let (supervisor_store, child_store) = UnixStream::pair().unwrap();
    let (supervisor_control, child_control) = UnixStream::pair().unwrap();
    for stream in [&supervisor_core, &supervisor_store, &supervisor_control] {
        stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
    }
    let child = thread::spawn(move || child(child_actions, child_core, child_store, child_control));

    let (sender, received) = mpsc::channel();
    reader(supervisor_core, Some("broker-core"), sender.clone());
    reader(supervisor_store, Some("store-evidence"), sender.clone());
    reader(supervisor_control, None, sender);
    let mut actions = supervisor_actions;
    write_frame(&mut actions, &Action::Start { rank: 0 }.encode()).unwrap();

    let mut sequencer = Sequencer::new();
    let mut seen = 0_u64;
    let mut end = None;
    // The supervisor acknowledges every frame as soon as it has read it
    // and every frame below it; it reads until the end of the action and
    // every frame the end names.
    let mut read_numbers = std::collections::BTreeSet::new();
    let mut acknowledged = None;
    while end.is_none() || seen < RECORDS {
        let read = received
            .recv_timeout(Duration::from_secs(20))
            .expect("the child stopped writing: a deadlock");
        match read {
            Read::Evidence(origin, evidence) => {
                read_numbers.insert(evidence.emission);
                sequencer.receive(origin, evidence);
                seen += 1;
                let contiguous = (0..).take_while(|n| read_numbers.contains(n)).last();
                if let Some(through) = contiguous
                    && contiguous != acknowledged
                {
                    acknowledged = contiguous;
                    write_frame(&mut actions, &Action::Acknowledge { through }.encode()).unwrap();
                }
            }
            Read::Report(Report::EndOfAction { last_emission, .. }) => end = Some(last_emission),
            Read::Report(other) => panic!("unexpected {other:?}"),
        }
    }
    let batch = sequencer.end_action(end.unwrap()).expect("in order");
    let waits = child.join().unwrap().expect("the child ran its action");
    assert!(waits > 0, "the window made the child wait");
    assert_eq!(batch.len() as u64, RECORDS);
    for (number, (origin, record)) in batch.iter().enumerate() {
        let number = number as u64;
        assert_eq!(*origin, record_of(number).0);
        assert!(
            String::from_utf8_lossy(record).contains(&format!("\"n\":{number}}}")),
            "{number}"
        );
    }
}

#[test]
fn a_frame_cut_by_the_end_of_the_stream_is_refused() {
    let (mut writer, mut reader) = UnixStream::pair().unwrap();
    writer.write_all(&[0, 0, 0, 8, 1, 2, 3]).unwrap();
    drop(writer);
    assert_eq!(
        read_frame(&mut reader),
        Err(mqttiumd_lab::bench::protocol::Refusal::Truncated)
    );
}
