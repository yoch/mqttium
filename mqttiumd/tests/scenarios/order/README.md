# Scenario of the order of acknowledgements

`ORDER-A.json` (`ORDER-A`) plays what record 0050 decides, on the bench:
the PUBREC of a publication waits for that of a publication the same
connection sent before it, when the result of the earlier admission is the
later one to reach the core.

One client opens two QoS 2 exchanges, Packet Identifiers 7 and 8. Each
admission is held in the transit of the store adapter
(`store.command.transit`, `hold_command`). The second is released first
and a barrier at `store.result.consume` marks the instant the core takes
its result; only then is the first admission released. The core therefore
learns of the second commit while the first is still to be made. It keeps
PUBREC 8, and makes it eligible in the transition that makes PUBREC 7
eligible. Both exchanges then run to their PUBCOMP, and the durable state
is inspected after the acknowledgements and at the end.

The scenario uses the existing vocabulary and the fixture
`three-persistent-sessions`, and runs on `sim-store` and `sqlite`
(`tools/run_campaign.sh tests/scenarios/order/ORDER-A.json <directory>`).

What judges it. The oracle judges the run with the assertions the
scenario lists, and passes it. It has no assertion on the order of two
acknowledgements: a core that answered 8 before 7 would pass it too. The
order is read on the records of the trace by
`crates/mqttiumd-lab/tests/bench_order.rs`, which requires the result of
operation 2, then that of operation 1, then PUBREC 7, then PUBREC 8, on
both stores and four seeds. On the core before record 0050 that test
reads "result 2, PUBREC 8, result 1, PUBREC 7".
