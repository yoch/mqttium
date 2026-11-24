# The compatibility bench waits for the SUBACK it is owed

The `compat` job of the CI failed once on `0073ef4` (lot 4 of
`refactor/deslop-layers`), in one scenario. This report records what the
failure was, that it does not come from that commit, and the change made
to the bench at the repository owner's decision. No code of the broker
changes.

## Host and revisions

- **Revision**: branch `refactor/deslop-layers`, `9396dd9` (failing tests
  of the bench), `5e1360d` (the change) and `9645bf4` (one of the tests
  tightened), on top of `0073ef4`.
- **Host**: Linux x86-64 container, 4 cores, loopback.
- **Reference**: Mosquitto 2.1.4 built by `tests/compat/build-reference.sh`.
- **Binaries**: release builds of `290db2b` (the base of the branch),
  `e89f5ee` (lot 3) and `0073ef4` (lot 4).

## The failure

`retained/retain-handling-0-v5` stores a retained QoS 0 message, then a
v5 client subscribes to its topic twice with Retain Handling 0. Each
SUBSCRIBE is sent without waiting (`subscribe(..., expect=0)`) and
followed by `collect`. In the CI transcript (artifact of run 45348777872),
after the second SUBSCRIBE, `mqttiumd` answered the PINGREQ of `collect`
and then sent nothing for the 0.3 s silent window: neither the SUBACK nor
the retained message. Mosquitto sent both.

The SUBACK of a SUBSCRIBE that replays retained messages is sent after
the durable update commits (record 0032), and PINGREQ is answered at
once. `collect` gives such a SUBSCRIBE one PINGREQ round trip and 0.3 s.

## It does not come from lot 4

- Lot 4 rewrote predicates of the core (`serves_connections`,
  `inline_claims_blocked`, the questions of `receipts::State`, the order
  of `deadline_is_live`); each was read against the code it replaced and
  is equivalent.
- Played alone, the three Retain Handling scenarios differed 0 times in 60
  plays on `e89f5ee` and 0 in 60 on `0073ef4`.
- The whole bench, beside six busy loops and synchronous writes, differed
  0 times in 20 plays on each of `290db2b`, `e89f5ee` and `0073ef4`.
- A script timed the second SUBSCRIBE to its SUBACK, 300 rounds per
  binary, idle and loaded. On all three binaries the PINGRESP came before
  the SUBACK in 300 of 300 rounds; the SUBACK followed in about 1 ms at
  the median and 33 ms at most. The three distributions are alike.
- On `290db2b`, with every `fsync` and `fdatasync` of the broker delayed
  by 0.4 s (`strace -e inject=...:delay_enter=400000`), the failure has
  the shape of the CI transcript in 3 of 3 plays: the `collect` after a
  durable SUBSCRIBE comes back empty and the SUBACK shows up in the next
  step.

The failure is consistent with a durable commit on the CI runner slower
than a round trip and 0.3 s, which this host does not reach without
injected delays. It is not established: the CI transcript ends with that
step, so it cannot tell a late SUBACK from one that never came, and the
runner's commit time was not measured. The new step tells the two apart:
a SUBACK that does not come now ends the step as `no-suback`, not
`quiet`.

The job was run again once on `0073ef4` and passed, with every other job
of that commit, `contracts` and `pilot proof` included.

## The change

Decided by the repository owner: a `collect` that follows a SUBSCRIBE
sent without waiting reads up to the SUBACK of that Packet Identifier
first, within the wait of an expected packet (3 s), then makes its round
trip and its silent window as before.

- `dsl.collect` and `dsl.quiet` take `suback=N`; `Player.do_quiet` reads
  to that SUBACK. A SUBACK that does not come ends the step as
  `no-suback`, and the late SUBACK of an earlier SUBSCRIBE does not end
  the wait. The record of the step keeps its shape (`op`, `got`, `end`),
  so the transcripts that are the same today stay the same.
- Eight steps use it: `retained/{replace,clear,tree}`, the two steps of
  each `retained/retain-handling-*`, `properties/message-expiry-elapsed-retained-v5`,
  `subscribe/sys-explicit-filter-v4` and `will/retain-*`. The `drain` and
  `measure` steps after a SUBSCRIBE sent without waiting are unchanged:
  they wait in their own way.
- `tests/compat/README.md` states the rule.

Three tests of `test_bench.py` were committed failing (`9396dd9`): a
scripted peer answers PINGREQ at once and a SUBSCRIBE 0.5 s later. They
failed because `collect` did not take `suback` yet, not on behaviour. One
of them, that a late SUBACK of an earlier SUBSCRIBE does not end the
wait, still passed with a wait that ignored the Packet Identifier; it was
tightened (`9645bf4`) and now fails without the identifier check.

## What remains exposed

With delayed syncs on `290db2b`, three plays each:

| Delay of each sync | Old bench | New bench |
| - | - | - |
| 0.2 s | no difference | no difference |
| 0.3 s (five plays) | RH 0, 1 and 2 differ | RH 0 and 1 differ |
| 0.4 s | RH 0, 1 and 2 differ | RH 0 and 1 differ |

What the new bench still sees is the retained message at QoS 0: it
leaves `mqttiumd` after one more commit, past the SUBACK (record 0032:
consumed before emission at QoS 0), and on such a machine it can come
after the window. It is late, not lost: a client that waits 5 s, on
`290db2b` with each sync delayed by 0.4 s, received it after each of 6
SUBSCRIBE, about 0.4 s after its SUBACK (0.81 s after the SUBSCRIBE, the
SUBACK at 0.41 s). The SUBACK, which the CI transcript lacked, no longer
misses. A wider window, or a barrier on the replay, was not chosen; it is
the owner's to decide.

Branch `perf/lot7-crushing-mosquitto` (yoch/mqttium#670) carries the bench
as it was and stays exposed until this change reaches it.

## Commands run

From `mqttiumd/`:

| Command | Result |
| - | - |
| `python3 -m unittest discover -s tests/compat -p "test_*.py"` | 67 tests, OK (the three new ones failed before `5e1360d`; the tightened one fails without the identifier check) |
| `tests/compat/run.py --only 'retained/retain-handling-*'`, 60 plays per binary, alternated | 0 differences on `e89f5ee`, 0 on `0073ef4` |
| `tests/compat/run.py`, whole bench under load, 20 plays per binary, alternated | 0 differences on each of the three binaries |
| The timing script above | the figures above |
| `tests/compat/run.py --only 'retained/retain-handling-*'` with delayed syncs, old and new bench | the table above |
| `tests/compat/run.py`, whole bench, new bench, `0073ef4` | exit 0: 135 scenarios, 105 `same`, 30 classified, as before; no fingerprint changed |

The plays with delayed syncs also reported that `mqttiumd` "did not stop
when asked to": the stop signal reached `strace`, not the broker. That
incident belongs to the wrapper and was not counted. The brokers it
left behind kept running, idle and with no client, during the later
plays with delayed syncs and the last whole bench; they were stopped
afterwards.

## Not run, not shown

- The commit time of the CI runner.
- The whole bench with delayed syncs.
