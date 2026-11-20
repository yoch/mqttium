# Differential compatibility bench: mqttiumd against Mosquitto

Version 0.1 of `mqttiumd` is a drop-in replacement for Mosquitto on a
supported subset (ADR 0044). This bench says where the two brokers answer
the same packets differently, and what each difference means. It belongs to
the Runtime evidence tier of `mqttiumd/AGENTS.md`.

Each scenario is a scripted exchange at packet level: named TCP
connections, the exact packets sent, explicit waits for packets or for the
closure of a connection, each with a deadline. A scenario is played against
a fresh instance of the pinned Mosquitto 2.0.21 and against a fresh
`mqttiumd`. The two transcripts are normalized and compared.

**Mosquitto is not the oracle; the MQTT specification is.** Equal
transcripts say that a client sees the same thing on both brokers, not that
either is right. A difference is not a failure by itself: it is read by a
person and classified in `divergences.json`. The lists of `defect` and
`profile-refused` entries are the compatibility backlog of the product.
`MATRIX.md` is the result of the last recorded run.

## Run

Python 3.11 or later, standard library only.

```bash
cd mqttiumd
sh preview/build-mosquitto-reference.sh /tmp/mosquitto-reference    # once; needs a C toolchain and OpenSSL headers
cargo build --release -p mqttiumd --locked
python3 tests/compat/run.py \
    --reference /tmp/mosquitto-reference \
    --broker target/release/mqttiumd \
    --out target/compat
```

`--reference` takes the output directory of the build script, the
`mosquitto-2.0.21` build directory, or the `mosquitto` executable itself;
another version than the pinned one is refused. `--only GLOB ...` plays the
scenarios whose id matches, `--list` prints them, `-v` prints each
difference, `--jobs N` plays N scenarios at once (default 4). The whole
bench takes about 40 seconds.

The output directory receives:

- `matrix.md` and `matrix.json`: one row per scenario (id, protocol
  version, result, note) and the count per class;
- `transcripts/<id>.json`: both normalized transcripts;
- `transcripts/<id>.diff`: their difference, for a scenario that differs;
- `transcripts/<id>.<backend>.incident.txt`: the end of the output and of
  the event log of a broker that exited by itself, had to be killed, or
  printed a panic.

Exit status: 0 when every difference is classified and every classified
difference still reproduces exactly as it was read; 1 otherwise; 2 for a
wrong command line or an unusable `divergences.json`. A run fails on:

- a difference without an entry in `divergences.json`;
- a stale entry: the two brokers now agree on that scenario;
- an entry whose `fingerprint` is not the one of the difference observed:
  the brokers still differ, but not as the entry describes;
- an entry for a scenario that does not exist, or one that cites a
  statement identifier the specification does not contain (checked against
  `docs/spec/*-statements.json` of the repository);
- a broker that could not be started;
- a broker incident (exit, hang at stop, panic) in a scenario that is not
  listed as a `defect`.

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
class. Entries are sorted by `id`.

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
- Never write down a packet identifier the broker chooses: answer with
  `ack(conn, kind)`, which uses the identifier received.
- One write, one read, when the question is what the broker does with a
  given state: packets written back to back may be read by the broker in
  one or several reads, and what it answers before it refuses the last one
  then depends on timing. Where a burst is the question, `tally` records
  whether all the expected packets came and not how many.
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
codes, client-chosen packet identifiers and property values. It replaces
the packet identifiers the broker chooses by labels in order of use
(`b1`, `b2` ...), the Assigned Client Identifier by `<assigned>`, and a
remaining Message Expiry Interval by the interval it falls in; it removes
Reason Strings and, except in `connect/connack-capabilities-v5`, the
capabilities a broker announces in CONNACK. No time is recorded, property
order is not compared (User Properties keep theirs), and a closure by end
of stream and one by reset are both `closed`.

## What the bench does not show

- **Conformance.** Scenarios on which both brokers agree are not checked
  against the specification by this bench.
- **Anything outside the scenarios.** About a hundred exchanges on one
  connection or a few; no claim beyond them.
- **Configuration.** Both brokers run with their defaults: anonymous
  access, no password file, no ACL, no TLS, no WebSocket, plain TCP on
  loopback. Two scenarios set one limit of the reference
  (`reference_config`).
- **Durability.** The reference runs with `persistence false` and no
  broker is restarted or killed during a scenario. Crash recovery belongs
  to the core tier.
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
  messages of a burst a broker sheds is reduced to "all" or "fewer".
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
| `classify.py` | `divergences.json` and the verdict of a run |
| `divergences.json` | The classification, edited by hand |
| `MATRIX.md` | Matrix of the last recorded run |
| `test_bench.py` | Tests of the bench |
