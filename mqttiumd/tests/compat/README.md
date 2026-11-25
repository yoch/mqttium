# Differential compatibility bench: mqttiumd against Mosquitto

Version 0.1 of `mqttiumd` is a drop-in replacement for Mosquitto on a
supported subset (ADR 0044). This bench says where the two brokers answer
the same packets differently, and what each difference means. It belongs to
the Runtime evidence tier of `mqttiumd/AGENTS.md`.

Each scenario is a scripted exchange at packet level: named TCP
connections, the exact packets sent, explicit waits for packets or for the
closure of a connection, each with a deadline. A scenario is played against
a fresh instance of the pinned Mosquitto 2.1.4 and against a fresh
`mqttiumd`. The two transcripts are normalized and compared.

**Mosquitto is not the oracle; the MQTT specification is.** Equal
transcripts say that a client sees the same thing on both brokers, not that
either is right. A difference is not a failure by itself: it is read by a
person and classified in `divergences.json`. The lists of `defect` and
`profile-refused` entries are the compatibility backlog of the product.
Where a scenario declares an expectation, mqttiumd's transcript is also
judged alone against what a normative statement requires; the expectations
it fails are listed in `expectations-open.json`. `MATRIX.md` is the result
of the last recorded run.

## Run

Python 3.11 or later, standard library only.

```bash
cd mqttiumd
sh tests/compat/build-reference.sh /tmp/mosquitto-reference    # once; needs a C toolchain, OpenSSL, cJSON and SQLite headers
cargo build --release -p mqttiumd --locked
python3 tests/compat/run.py \
    --reference /tmp/mosquitto-reference \
    --broker target/release/mqttiumd \
    --out target/compat
```

`--reference` takes the output directory of the build script, the
`mosquitto-2.1.4` build directory, or the `mosquitto` executable itself;
another version than the pinned one is refused. `--only GLOB ...` plays the
scenarios whose id matches, `--list` prints them, `-v` prints each
difference and the result of each expectation, `--jobs N` plays N
scenarios at once (default 4). The whole bench takes about 40 seconds.
The reference is the subject of record 0062; on Debian and Ubuntu the
headers are in `libssl-dev`, `libcjson-dev` and `libsqlite3-dev`.
The build includes the bundled SQLite persistence plugin. The runner locates it
in the build tree or takes an explicit `--reference-sqlite-plugin PATH`. When
the corpus uses it, `matrix.json` records its SHA-256 alongside the binaries.
Set `--delivery-shards N` to run the same corpus against another mqttiumd
configuration. Use a distinct output directory per run.

The output directory receives:

- `matrix.md` and `matrix.json` (format `mqttiumd-compat-matrix-v3`): one
  row per scenario (id, protocol version, result, note), the count per
  class, the broker incidents, one row per expectation with its result
  on each broker, and one row per step that counts (`tally`, `measure`)
  with how many packets each broker sent, which is a measurement and
  judges nothing;
- `transcripts/<id>.reference.raw.json` and
  `transcripts/<id>.mqttiumd.raw.json`: what the player recorded from each
  broker, before any normalization: every packet received in the order it
  came with the bytes it took on the wire (`wire_bytes`), how each read
  ended, the capabilities of each CONNACK, the packets a `tally` or a
  `measure` counted;
- `transcripts/<id>.json`: both normalized transcripts, equal or not;
- `transcripts/<id>.diff`: their difference, for a scenario that differs;
- `transcripts/<id>.<backend>.incident.txt`: the end of the output and of
  the event log of a broker that exited by itself, had to be killed, or
  printed a panic;
- `transcripts/<id>.error.txt`: the failure of a scenario that could not
  be played.

Exit status: 0 when every difference is classified, every classified
difference still reproduces exactly as it was read, no broker had an
incident that is not recorded, and every expectation passes or is listed
as open; 1 otherwise; 2 for a wrong command line or an unusable
`divergences.json` or `expectations-open.json`. A run fails on:

- a difference without an entry in `divergences.json`;
- a stale entry: the two brokers now agree on that scenario;
- an entry whose `fingerprint` is not the one of the difference observed:
  the brokers still differ, but not as the entry describes;
- an entry for a scenario that does not exist, or an entry or an
  expectation that cites a statement identifier the specification of the
  scenario does not contain (checked against `docs/spec/*-statements.json`
  of the repository);
- a broker that could not be started;
- a broker incident (it exited by itself, had to be killed at stop, or
  printed a panic) that the entry of the scenario does not record, in
  every class and in a scenario whose transcripts are equal: a crash
  never hides behind a classified difference;
- a recorded incident that did not happen;
- an expectation that fails on mqttiumd's transcript and is not listed in
  `expectations-open.json`;
- a listed expectation that passes or does not apply any more, that no
  scenario declares, or whose `statement` is not the one the scenario
  declares.

After a deliberate change, copy the matrix: `cp target/compat/matrix.md
tests/compat/MATRIX.md`. CI does not compare the two files; it uploads the
matrix it obtained.

The bench has tests of its own, which start no broker:

```bash
python3 -m unittest discover -s tests/compat -p "test_*.py"
```

## Classes

| Class | Meaning | Required fields |
| - | - | - |
| `profile-refused` | mqttiumd deliberately does not support the feature yet, or bounds it lower than Mosquitto | `lot` when known |
| `reference-nonconformant` | Mosquitto departs from the specification and mqttiumd follows it | `statement` (the normative statement, such as `MQTT-4.13.1-1`) and `mqttiumd` (what mqttiumd does) |
| `defect` | mqttiumd is wrong, or different for no reason: a to-do for the product | `lot` when known |
| `unspecified` | the specification allows both behaviours | |

Every entry has `id`, `class`, `difference` (one line: what the reference
does, what mqttiumd does) and `fingerprint`. `statement` is allowed in any
class, and so is `decision`: the four-digit number of the decision record
that chose the behaviour, for a difference the owner decided (record 0049
says how such a choice is made). Entries are sorted by `id`.

An entry may record a broker incident that is part of the difference that
was read:

```json
"incident": {"mqttiumd": ["exited"]}
```

The keys are `reference` and `mqttiumd`; the kinds are `exited` (the
process ended before it was asked to), `killed` (it did not stop within
five seconds of SIGTERM) and `panic` (its output contains `panicked at`),
sorted, each once. An incident of mqttiumd is allowed in a `defect` entry
only: whatever else differs, a broker that crashes is wrong. Exactly the
recorded incidents must happen; no entry records one today.

When a difference appears or changes: run with `-v`, read the diff, write
or correct the entry, then record the fingerprints of what was read with

```bash
python3 tests/compat/run.py --reference ... --broker ... --update-fingerprints
```

That option only rewrites the `fingerprint` of entries that exist and whose
scenario still differs. It never adds or removes an entry: an unclassified
difference and a stale entry keep failing until a person edits the file. A
defect that is fixed makes its entry stale; remove the entry in the change
that fixes the defect.

Do not weaken a scenario to make the brokers agree. Record the divergence.

## Expectations

Equality with Mosquitto cannot say that mqttiumd is right: both brokers
may be wrong together, and a classified difference only says that someone
read it. An expectation is an absolute judge for one point of one
scenario: what a normative statement requires of the broker there.

```python
scenario(
    "subscribe/twelve-filters-v4",
    doc="...",
    steps=[connect("a", "s-twelve"), subscribe("a", 1, [(f"twelve/{index}", 0) for index in range(12)]),
           mark("answer"), quiet("a")],
    expectations=[expectation(
        "suback", statement="MQTT-3.8.4-5",
        requires="A SUBACK with twelve return codes, each a granted QoS or 0x80 (Failure)",
        then=[at("answer", {"got": [{"type": "SUBACK", "packet_id": 1, "codes": Each(12, (0, 1, 2, 0x80))}],
                            "end": "ok"})])],
)
```

`mark(name)` names the record of the step before it. `then` lists the
patterns that must all hold; `when` lists conditions without which the
expectation does not apply (a MQTT 5 broker that announces a feature as
available is not expected to refuse it). `expectations.py` documents the
patterns: a mapping is matched on the keys it names, a list whole and in
order, `one_of(...)` on one alternative, `Each(count, allowed)` on a list
of return codes, `in_any_order(...)` on a list whose packets each match a
pattern of their own in any order, `at_most(bound)` on a number such as
the `wire_bytes` of a packet, `CLOSED` on `eof` or `reset`.

Each expectation is evaluated on the **raw** transcript of mqttiumd and
gives `pass`, `fail` or `not-applicable`, with one line of what was
observed. The reference is evaluated too; its result is printed in the
matrix and judges nothing. `expectations-open.json` lists the expectations
mqttiumd is known to fail:

```json
[{"id": "<scenario>#<expectation>", "statement": "the statement the scenario declares",
  "observed": "one line: what mqttiumd does instead"}]
```

sorted by `id`. Fixing the broker makes the entry stale, and the run fails
until the entry is removed in the change that fixes it. The matrix lists the complete set and its results. These checks cover
subscriptions, retained messages, negotiated packet and flow limits, empty
topic names, Wills, session resumption and redelivery across a broker restart.

An expectation states what conformance requires, not what Mosquitto does
and not what mqttiumd does today; write it from the statement, give every
conformant answer (a refusal with 0x80 conforms where a grant does), and
keep `test_expectations.py` in step: for each expectation it holds
transcripts that conform and transcripts that do not.

## Add a scenario

Scenarios live in `scenarios/<area>.py`, one module per area of
`dsl.AREAS`, and are registered when the module is imported:

```python
scenario(
    "publish/qos1-v5",          # area/what-vN: -v4 is MQTT 3.1.1, -v5 is MQTT 5.0
    doc="One or two sentences: what is played and what is expected.",
    steps=[
        connect("sub", "p-q1-sub", level=5), subscribe("sub", 1, [("q1/t", 1)], level=5),
        connect("pub", "p-q1-pub", level=5), publish_ack("pub", "q1/t", "one", packet_id=21, level=5),
        recv("sub"), ack("sub"), quiet("sub"),
    ],
)
```

`dsl.py` documents the steps. The rules that keep a scenario precise and
stable:

- Wait for a packet with `recv`, never with `sleep`. `sleep` is for
  behaviour that is about time (expiry), with a wide margin.
- Before stating that nothing arrives (`quiet`, `collect`), make the
  broker's work observable: `publish_ack` and `publish_sync` return when
  the broker has read the publication, and `quiet` itself makes a
  PINGREQ/PINGRESP round trip on the connection before its silent window.
- The silent window of `quiet`, `collect`, `tally` and `measure` starts
  again at each packet: the step ends once nothing has come for the
  window, however long the answer took, and as `still-receiving` when
  packets still come after ten seconds. The PINGRESP of the round trip
  does not mark the end of an answer: a broker may answer PINGREQ before
  it has finished a replay that goes through its store.
- Keep apart what a broker owes and what it may shed, in two scenarios
  when one exchange asks both questions:
  - what it owes whatever the timing (QoS 1 deliveries, the content of
    the retained store) is read with `drain`, which acknowledges each
    QoS 1 PUBLISH as it reads it, so that the subscriber is never the
    reason why a delivery is missing, and is judged by an expectation;
  - what it may lawfully shed (QoS 0 deliveries to a subscriber it cannot
    write to fast enough) is counted with `measure`: the transcript
    compares that each message that came is one of those sent and came
    once, and their order where the protocol guarantees one; how many
    came is printed in the matrix for both brokers and never compared.
    An equality on such a count fails on a slow machine: with a
    subscription at QoS 0 and a fixed window, `retained/forty-topics-v4`
    gave another transcript in 1 play of 30 without added load and in 5
    of 30 beside eight busy loops on four cores.
- Never write down a packet identifier the broker chooses: answer with
  `ack(conn, kind)`, which uses the identifier received.
- One write, one read, when the question is what the broker does with a
  given state: packets written back to back may be read by the broker in
  one or several reads, and what it answers before it refuses the last one
  then depends on timing. Where a burst is the question, `tally` compares
  whether all the expected packets came and not how many; give it the
  `sequence` that was sent, and it also compares whether they came in that
  order (`as-sent`), in another (`permuted`), or with a repeated or foreign
  value (`other`). `tally` suits a count whose verdict does not depend on
  timing (all twenty PUBACK, or always fewer than a hundred QoS 0
  messages); where a broker sometimes sends all and sometimes fewer, use
  `measure`.
- A connection the broker closes ends by `eof` (end of stream) or `reset`,
  and the two are compared. Declare `closure="any"` only for a scenario in
  which the kind of closure depends on timing: the broker closes while the
  client is still writing, and whether it had read everything decides
  between the two. Show the variation first (run the scenario alone, with
  `--jobs 1` and `--jobs 8`, under load) and say so in the scenario. No
  scenario is declared so today: the three that write a 70 000-byte packet
  gave one kind each in 48 plays made while eight copies of the bench ran
  at once (mqttiumd shuts the stream down before it closes; Mosquitto
  closes with the rest of the packet unread, which resets).
- Use `sort=True` only where two orders are lawful and the order is not
  the question (SUBACK and the retained messages of that subscription).
- Bytes that are not one well-formed packet are sent with a `note`, which
  replaces the decoded packet in the transcript.
- `reference_config` adds lines to the reference's `mosquitto.conf`, for a
  scenario that needs both brokers to announce the same limit. mqttiumd
  takes no configuration here.
- Each scenario has its own brokers, so client identifiers and topics need
  not be unique across scenarios; distinct names make transcripts readable.

Then run the scenario alone, several times, also under load:

```bash
python3 tests/compat/run.py --reference ... --broker ... --only 'publish/qos1-*' -v
```

A scenario whose transcript changes from one run to the next is fixed or
removed, not retried.

## Normalization

`normalize.py` states what is compared as observed and what is not. In
short, the transcript keeps packet types and their order within a step,
flags, QoS, topics, payloads (long ones as a length and a digest), reason
codes, client-chosen packet identifiers, property values, and whether a
connection was closed by end of stream (`eof`) or by a reset (`reset`). It
replaces the packet identifiers the broker chooses by labels in order of
use (`b1`, `b2` ...), the Assigned Client Identifier by `<assigned>`, and a
remaining Message Expiry Interval by the interval it falls in; it removes
Reason Strings, the size of each packet on the wire and, except in
`connect/connack-capabilities-v5`, the capabilities a broker announces in
CONNACK; it reduces the packets a `tally` counted to the verdicts
`arrived` and `order`, and those a `measure` counted to `values`
(`distinct`, `repeated`, `foreign`) and, where asked, `order`, without
their number; it gives each delivery of a `drain` a label of its own. No
time is recorded and property order is not compared (User Properties keep
theirs).

The raw transcripts are written next to the normalized ones and are not
compared. They are where to look for what normalization removes: the
exact sequence of a permuted burst, how many messages a broker shed, the
capabilities of a CONNACK, the size of a packet. The PINGREQ and PINGRESP
of the round trip that `quiet`, `collect`, `tally` and `measure` make
before their silent window belong to the harness and are in neither form.

## Successor of the frozen reception F-packet-limit

The system reception `tests/system/pubsub_reception.py`, criterion
F-packet-limit, is frozen evidence of an earlier executable and asserts
the rule that record 0025 has since replaced: a QoS 1 delivery larger than
the subscriber's Maximum Packet Size closed the connection and kept the
obligation. It is not repaired. The rule in force (the amendment of 21
November 2026 to record 0025, MQTT-3.1.2-25 of MQTT 5.0) is that the
delivery is discarded as completed, the connection stays open, the next
delivery is sent, and a resumed session is owed nothing for the discarded
message. Its active evidence is:

- `publish/over-maximum-packet-size-qos1-v5` and
  `publish/over-maximum-packet-size-qos2-v5`, each with the expectations
  `discarded` and `not-redelivered` on mqttiumd's transcript. Mosquitto
  2.1.4 answers the same packets the same way: both scenarios are `same`;
- `crates/mqttiumd/tests/packet_limit.rs`, which plays the same story
  against the product executable at QoS 1 and QoS 2 and also reads what
  the wire does not show: the event `delivery_discarded` with the reason
  `packet_too_large`, and a store that holds no obligation and no claim
  after the discard.

## What the bench does not show

- **Conformance.** Only the points that carry an expectation are checked
  against the specification. Elsewhere, scenarios on which
  both brokers agree are not checked against it, and a classified
  difference rests on the reading of the person who classified it.
- **The network below TCP's interface.** `eof` and `reset` are what the
  client's system reports on loopback; segments are not captured.
- **Anything outside the scenarios.** About a hundred exchanges on one
  connection or a few; no claim beyond them.
- **Configuration.** Both brokers run with their defaults: anonymous
  access, no password file, no ACL, no TLS, no WebSocket, plain TCP on
  loopback. Five scenarios set one limit of the reference
  (`reference_config`). The restart scenarios configure persistence.
- **Durability.** Two scenarios perform a graceful restart. Upstream ticket
  3759 uses Mosquitto's snapshot persistence. Ticket 3760 uses its bundled
  SQLite plugin, whose schema the runner checks before reopening it. The
  runner sends SIGTERM with client sockets still open, waits for clean exit
  and reuses the same data directory. mqttiumd reopens with `--existing-db`
  and a fresh event file. The other scenarios use isolated fresh stores.
  A separate crash-recovery campaign exercises abrupt process termination.
- **A single node, a few clients.** No load, no throughput, no latency, no
  bridge, no cluster.
- **Timing.** Nothing is timed. Behaviour that is defined by a duration is
  observed with a wide margin or left out: the keep alive timeout and the
  Will Delay Interval are observed as "it happens within a few seconds",
  Message Expiry as "expired after two and a half times the interval".
  Session Expiry elapsing, keep alive precision and retransmission timing
  are not covered.
- **Lawful non-determinism.** How a share group spreads messages between
  several members is left out (one member is covered). How many QoS 0
  messages of a burst a broker sheds is reduced to "all" or "fewer", and
  how many retained messages reach a subscription at QoS 0 is not
  compared at all; both counts are printed in the matrix as what one run
  measured.
- **MQTT 5 enhanced authentication, Server Keep Alive, Request/Response
  Information, flow control beyond Receive Maximum.** Not covered yet.

## Layout

| File | Role |
| - | - |
| `run.py` | Command line, comparison, matrix |
| `dsl.py` | Vocabulary of the scenarios |
| `scenarios/` | The scenarios, one module per area |
| `harness.py` | Starts the brokers, plays a scenario, records the transcript |
| `mqttwire.py` | MQTT 3.1.1 and 5.0 packet encoder and decoder, independent of the broker's codec and of the `mqttium` client |
| `normalize.py` | Comparable form of a transcript, fingerprint |
| `classify.py` | `divergences.json`, broker incidents and the verdict of a run |
| `divergences.json` | The classification, edited by hand |
| `expectations.py` | Patterns, evaluation of an expectation, `expectations-open.json` and its verdict |
| `expectations-open.json` | The expectations mqttiumd is known to fail, edited by hand |
| `MATRIX.md` | Matrix of the last recorded run |
| `test_bench.py` | Tests of the bench, with a scripted peer in the test process |
| `test_expectations.py` | Tests of the expectations, and for each one transcripts that conform and that do not |

### Listener profiles

`python3 tests/compat/listener_profiles.py --broker target/release/mqttiumd
--reference /tmp/mosquitto-reference --out /tmp/listeners.json` exercises
TLS 1.3 identity and subscriptions on both binaries and compares the
password-file profile. A separate mqttiumd run resumes a loopback
anonymous session and verifies uninterrupted MQTT traffic and continuous
JSONL sequence numbers across a live journal rotation. The report records
the binary digests and both journal ranges. Temporary credentials are
removed at exit.
