# Scenarios of group commit

Scenarios with two commands outstanding at once, for the grouped writer of
the bench (`--backend sqlite-group`, record 0045). They are played by the
runner of lot L4 (`tools/run_campaign.sh <scenario> <directory> 1
sqlite-group`) and judged by the oracle like any other. They use the
existing vocabulary and the fixture `three-persistent-sessions`, and they
also run, and pass, with one transaction for each command (`sqlite`,
`sim-store`).

No scenario of `p1/`, `qabs/` or `audit/` has two commands outstanding:
each has one client and one exchange, so a grouped writer commits them in
groups of one, which is `apply`, and their traces are the same bytes as on
`sqlite` (`crates/mqttiumd-lab/tests/bench_group.rs`). Playing them in
that mode shows nothing about a group.

Here one client opens two QoS 2 exchanges. Each admission is held in the
transit of the store adapter when it is submitted
(`store.command.transit`, `hold_command`), and both are released before
anything enters the writer: the next action of the writer finds them
waiting together and commits them as one group, whatever the seed. The
fixture allows two commands in flight, so a group here has two members.

| Scenario | What it plays | What the oracle judges |
| - | - | - |
| `GROUP-A.json` (`GROUP-A`) | The two admissions commit as one group; each result is delivered and each exchange gets its PUBREC; the client sends both PUBREL and gets both PUBCOMP. The durable state is inspected after the acknowledgements of the group and at the end | One admission for each exchange, no acknowledgement before the core knew of a commit, the same obligations at both checkpoints, provenance of every input |
| `GROUP-B.json` (`GROUP-B`) | P1-02 on a group: the two admissions commit as one group, the result of the first is lost and the broker crashes before any answer of the group reached the core. After the restart the client resumes both exchanges, is acknowledged from what recovery found, and completes both. The two releases commit as a second group | The same, and no PUBREC observed before the crash |
| `GROUP-ACK.json` (`GROUP-ACK`) | `GROUP-A` up to the acknowledgements of the group, with an inspection after each | The control of mutation `g1` of `tests/mutants/loop/`: a group that is rolled back and answered as committed is acknowledged for what the store does not hold, and the run ends before anything else is asked of the store, so the trace is complete and the rejection is a FAIL |

Limits. A group has two members: the fixture allows no more in flight. The
group is formed by the store adapter of the bench, from its transit, not
by the store worker of the product (`crates/mqttiumd/src/writer.rs`),
whose group formation the bench does not run. A crash is simulated in the
process of the bench; `--process child` is refused in this mode. The trace
has no record of a group: it shows one by the commits of its members
coming before any of their results, and the campaign states the count of
the adapter in `store_groups` of `campaign.json`.
