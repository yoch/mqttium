use super::*;
fn fixture() -> (Child, mpsc::SyncSender<Arrival>, Collector, Books) {
    let mut process=Command::new("cat").stdin(Stdio::piped()).stdout(Stdio::null()).spawn().unwrap();
    let actions=process.stdin.take().unwrap();
    let pid=process.id();
    let (sender,received)=mpsc::sync_channel(ARRIVAL_CAPACITY);
    let schema=crate::strict::parse(include_bytes!("../../../../contracts/schema/v1/trace-record.schema.json")).unwrap();
    let collector=Collector::new("review",KindTable::from_schema(&schema).unwrap(),1<<20);
    let child=Child {
      process,pid,actions:Some(BufWriter::new(actions)),received,
      sequencer:Sequencer::new(),eligible:Vec::new(),state:State::default(),base_hits:Vec::new(),
      epoch:1,rank:0,settings:Value::Null,
      _sockets:SocketDirectory(std::env::temp_dir().join(format!("mqttiumd-review-io-{pid}"))),
      silence:Duration::from_secs(2),ended:true,parked:None,
    };
    (child,sender,collector,Books::new(0))
}
#[test]
fn repeated_contradictory_ends_exceeding_report_budget_are_refused() {
    let (mut child,sender,mut collector,mut books)=fixture();
    for _ in 0..=REPORTS_PER_ACTION {
      sender.send(Arrival::Report(Report::EndOfAction {last_emission:Some(0)})).unwrap();
    }
    sender.send(Arrival::Report(Report::EndOfAction {last_emission:None})).unwrap();
    drop(sender);
    let result=child.act(&Action::Settle,&mut collector,&mut books);
    assert!(result.is_err(),"accepted 65 EndOfAction(Some(0)) followed by EndOfAction(None) with no evidence: {result:?}");
}
#[test]
fn broken_evidence_after_signal_is_not_silently_discarded() {
    let (child,sender,_,_)=fixture();
    let pid=child.pid();
    std::thread::spawn(move || {
       let start=Instant::now();
       while std::path::Path::new(&format!("/proc/{pid}")).exists() {
          assert!(start.elapsed()<Duration::from_secs(2));
          std::thread::sleep(Duration::from_millis(1));
       }
       let mut bytes=64u32.to_be_bytes().to_vec();bytes.push(1);
       let refusal=read_frame(&mut bytes.as_slice()).unwrap_err();
       sender.send(Arrival::Broken(format!("broker-core: {refusal}"))).unwrap();
    });
    let result=child.kill();
    assert!(result.is_err(),"accepted kill although its evidence reader reported a truncated frame: {result:?}");
}

