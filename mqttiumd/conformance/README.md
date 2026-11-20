# Server-side conformance register

`server-statements.json` lists every numbered normative statement of
MQTT 5.0 and MQTT 3.1.1 and says, for the broker under `mqttiumd/`, whether
the statement binds a server and what stands behind it today: a test, code
without a test, a deliberate refusal, or nothing.

The statements themselves are not copied here. Their identifiers and texts
are those of the vendored indexes of the OASIS specifications,
`docs/spec/mqtt-v5.0-statements.json` (251 statements) and
`docs/spec/mqtt-v3.1.1-statements.json` (139 statements); read a statement
there by its `id`.

| File | Role |
| - | - |
| `server-statements.json` | The register. Maintained by hand |
| `check.py` | Checks the register and every citation in it; rewrites `SUMMARY.md` |
| `SUMMARY.md` | Generated: counts, then the `open` and `profile-refused` rows by feature. These two lists are the conformance backlog of version 0.1 |

## What it is not

It is a map of evidence, not a proof of conformance.

- A `covered` row says that a named test exercises the statement. It does
  not say that every case of the statement is tested, and the note of the
  row often names the case that is not.
- The checker shows that each cited test and file exists. It runs no test
  and cannot tell whether a test is sufficient, or still asserts what the
  note says.
- A row was classified by reading the code and the tests at the revision
  recorded in `reviewed_at`. Code that changed since may have made a row
  wrong without making the checker fail.
- A `not-applicable` or `profile-refused` row is not a conformance claim
  either: it says why the statement asks nothing of the broker, or what the
  broker refuses instead.
- Nothing here was measured against another broker. The differential
  scenarios against Mosquitto that ADR 0044 calls for are a separate body
  of evidence.

## An entry

```json
{
  "id": "MQTT-3.1.2-25",
  "feature": "maximum-packet-size",
  "applies_to_server": true,
  "status": "open",
  "evidence": [],
  "code": ["mqttiumd/crates/mqttiumd-core/src/owner/pubsub.rs:plan_delivery_emit"],
  "note": "One or two precise sentences."
}
```

Entries are grouped by protocol version under `versions`, in the order of
the source index.

| Field | Meaning |
| - | - |
| `id` | Identifier of the statement in the source index of that protocol version |
| `feature` | A lower-case slug that groups rows in `SUMMARY.md`, such as `will`, `topic-alias` or `shared-subscriptions`. Reuse an existing slug when one fits |
| `applies_to_server` | `false` when the statement only binds a client; the note says so, and the status is then `not-applicable`. It follows the reading recorded in `docs/spec/applicability.json` (`client` there is `false` here); a row that departs from it says why in its note |
| `status` | One of the five values below |
| `evidence` | For `covered` only: a list of `{"file", "test"}`, a repository-relative path and the exact name of a test function in it. Empty for every other status |
| `code` | A list of `"path:symbol"`: a repository-relative file and a function or constant in it. Required for `implemented-untested` and `profile-refused`, optional elsewhere. The symbol may be omitted |
| `note` | What the test asserts, what it leaves out, or what a client observes |

## Statuses

| Status | Exact meaning |
| - | - |
| `covered` | The behaviour the statement asks of a server is exercised by at least one named automated test of the maintained suites, at the component that decides it. The test would fail if that behaviour changed |
| `implemented-untested` | The code clearly does it, at the cited `file:function`, but no test was found that would fail if it did not |
| `profile-refused` | The broker deliberately rejects the feature, or does not offer it. `code` cites the place that refuses and the note says what a client observes |
| `open` | The statement binds a server and either no implementation and no deliberate refusal was found, or the behaviour looks non-conformant. The note says which |
| `not-applicable` | The statement asks nothing of this broker: it binds the client only (`applies_to_server` is `false`), or its condition cannot arise here and the note says why |

The register is deliberately conservative. Between `covered` and
`implemented-untested`, choose the latter; between `implemented-untested`
and `open`, choose `open`. One known non-conformant case makes a row
`open`, even when the ordinary case is tested; the note then says both.

### What counts as evidence

- A test of the Rust workspace or a Python test of the maintained tools:
  `crates/mqttiumd` (codec, runtime, network), `crates/mqttiumd-core`,
  `crates/mqttiumd-contract`, the stores and the lab.
- The test may sit below the wire. Routing, sessions, QoS exchanges,
  retained messages and Wills are decided in the core and the store, and a
  test there counts; the note then says "core level" or "store level" and
  what was not observed on a socket.
- The MQTT 3.1.1 rows have few tests of their own: the runtime tests drive
  MQTT 5 connections. A 3.1.1 row cites such a test only where the deciding
  code is shared by both versions, and says so. Where the runtime has a
  3.1.1 branch of its own that no test takes, the row is
  `implemented-untested`.
- The system receptions under `mqttiumd/tests/system/` are frozen
  (ADR 0044). They may be named in a note and are refused as evidence.
- Cite only a test that was read, and that would fail without the
  behaviour. A test whose name sounds right is not evidence.

## Updating it

Run the two commands from the repository root after any change:

```bash
python3 mqttiumd/conformance/check.py           # check, rewrite SUMMARY.md
python3 mqttiumd/conformance/check.py --check   # what CI runs
```

Commit `server-statements.json` and `SUMMARY.md` together; `--check` fails
when the summary is stale.

- **A test is added.** Find the rows it exercises, usually
  `implemented-untested` ones of its feature. Add the test to `evidence`,
  set `covered`, and rewrite the note to say what is now asserted and what
  still is not. Keep `code` if it helps a reader.
- **A test is renamed, moved or removed.** The checker fails on the stale
  citation. Fix the citation, or downgrade the row if nothing exercises
  the statement any more.
- **A feature is added** (for example shared subscriptions). Its
  `profile-refused` rows become `open`, `implemented-untested` or `covered`
  one by one, each on its own evidence. Do not move a whole feature at
  once.
- **A defect is fixed.** The broker's rule is test first: the row goes from
  `open` to `covered` with the discriminating test as evidence.
- **Behaviour changes** in a file a row cites. Read the rows that cite it
  again; update `reviewed_at` when the whole register was read again.
- **The source indexes change.** `check.py` fails on a missing or unknown
  identifier; add or remove the rows in the order of the index.

`python3 mqttiumd/conformance/check.py --selftest` shows that the checker
refuses what it claims to check: a missing, repeated or unknown identifier,
an invalid status, a cited test or file that does not exist, a helper
function cited as a test, a frozen reception cited as evidence, a refusal
without a code location, and a stale summary.

## What `--check` verifies

- every identifier of the two source indexes appears exactly once, in the
  order of its index, and no other identifier appears;
- every entry has the required fields, a valid status, and
  `applies_to_server: false` only together with `not-applicable`;
- a `covered` entry has evidence and no other entry has any;
- every evidence file exists, is not a frozen reception, and contains a
  test function of exactly that name: for Rust, `fn name(` with `#[test]`
  among the attribute lines just above it; for Python, `def name(`;
- every `implemented-untested` and `profile-refused` entry cites code, and
  every cited file exists and contains the cited symbol;
- `SUMMARY.md` is what the register generates.
