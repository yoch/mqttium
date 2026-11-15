#!/usr/bin/env python3
"""What a campaign shows of the kill of its child process.

    python3 tools/check_campaign_kill.py <campaign directory> [--strace <log>]

A campaign played with `--process child` on the SQLite store kills the
process that runs the core and the store with SIGKILL where the scenario
crashes the broker, and starts another one on the same file. This check
reads what the campaign kept and says whether it is that execution, and
not a dropped object or a clean stop:

- `campaign.json` says the process under test ran in a child, and records
  one kill per crash of the scenario: the signal sent is SIGKILL, the wait
  status is signal 9 with no exit code;
- the header of the trace says the broker ran in a child process, and
  each BrokerCrashed comes right after a BrokerKilled of the supervisor
  whose wait status is signal 9 (record 0019); it is followed by a
  BrokerRecovered, and no record of the killed incarnation of the child
  follows it;
- the store of the campaign is one SQLite file with its writer's lock
  file, the same before and after the kill.

With `--strace`, the log of `strace -f -e trace=kill,wait4` of the run is
an outside witness: for each kill the campaign records, the log must hold
`kill(<pid>, SIGKILL) = 0` and then a `wait4(<pid>, …)` that reports
`WTERMSIG(s) == SIGKILL`, both made by the supervisor. The campaign's own
record is the bench's word; the log is the kernel's.

Exit 0 when every check holds, 1 otherwise, 2 on a usage error.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys


# The origins whose records leave the child process; the supervisor and
# the peers stay alive across a kill.
CHILD_ORIGINS = {
    "broker-core",
    "dispatcher",
    "fault-injector",
    "store-adapter",
    "store-evidence",
    "timer",
    "transport",
}


def fail(reason: str) -> None:
    print(f"kill check: {reason}")
    sys.exit(1)


def recorded_kills(campaign: pathlib.Path) -> list[dict]:
    """The kills `campaign.json` records, checked against the trace."""
    summary = json.loads((campaign / "campaign.json").read_text())
    covers = summary["covers"]
    if covers.get("process") != "child":
        fail(f"the process under test did not run in a child: {covers.get('process')!r}")
    if covers.get("backend") != "sqlite":
        fail(f"the store is not SQLite: {covers.get('backend')!r}")
    kills = summary.get("kills") or []
    for kill in kills:
        status = kill.get("wait_status") or {}
        if kill.get("signal_sent") != "SIGKILL":
            fail(f"the signal sent is {kill.get('signal_sent')!r}")
        if status.get("signal") != 9 or status.get("exit_code") is not None:
            fail(f"the wait status is {status!r}, not signal 9 without an exit code")
    return kills


def check_trace(campaign: pathlib.Path, kills: list[dict]) -> None:
    """Each crash ends an incarnation of the child for good, and is recovered."""
    lines = (campaign / "trace.jsonl").read_text().splitlines()
    records = [json.loads(line) for line in lines]
    if records[0]["payload"].get("broker") != "child-process":
        fail("the header of the trace does not say the broker ran in a child process")
    crashes = [index for index, record in enumerate(records) if record["kind"] == "BrokerCrashed"]
    if not crashes:
        fail("the trace holds no crash of the broker")
    if len(kills) != len(crashes):
        fail(f"{len(crashes)} crashes in the trace, {len(kills)} kills recorded")
    for crash in crashes:
        killed_by = records[crash - 1]
        expected = {"signal_sent": "SIGKILL", "wait_status": {"signal": 9}}
        if (
            killed_by["kind"] != "BrokerKilled"
            or killed_by["origin_component"] != "bench-supervisor"
            or killed_by["payload"] != expected
        ):
            fail("a crash of the trace does not follow a BrokerKilled of signal 9")
        killed = {
            (record["origin_component"], record["origin_incarnation"])
            for record in records[:crash]
            if record["origin_component"] in CHILD_ORIGINS
        }
        if not killed:
            fail("no record of the process precedes its crash")
        after = records[crash + 1 :]
        if not any(record["kind"] == "BrokerRecovered" for record in after):
            fail("no recovery follows the crash")
        late = [
            record
            for record in after
            if (record["origin_component"], record["origin_incarnation"]) in killed
        ]
        if late:
            fail(f"{len(late)} records of the killed incarnation follow its crash")


def check_store(campaign: pathlib.Path) -> str:
    """One database, with the lock file of its writer."""
    store = sorted(path.name for path in (campaign / "store").iterdir())
    databases = [name for name in store if name.endswith(".sqlite")]
    if len(databases) != 1 or f"{databases[0]}.writer-lock" not in store:
        fail(f"the store is not one database with its writer's lock: {store}")
    return databases[0]


def check_witness(log: list[str], kills: list[dict]) -> None:
    """The kernel's account of each kill: sent, then reaped, by one process."""
    for kill in kills:
        pid = kill["pid"]
        sent = [
            index
            for index, line in enumerate(log)
            if re.search(rf"\bkill\({pid}, SIGKILL\)\s+= 0\b", line)
        ]
        reaped = [
            index
            for index, line in enumerate(log)
            if re.search(
                rf"\bwait4\({pid}, \[\{{WIFSIGNALED\(s\) && WTERMSIG\(s\) == SIGKILL\}}\]", line
            )
            and line.rstrip().endswith(f"= {pid}")
        ]
        if not sent:
            fail(f"the witness holds no kill({pid}, SIGKILL) = 0")
        if not reaped:
            fail(f"the witness holds no wait4 of {pid} that reports SIGKILL")
        if sent[0] > reaped[0]:
            fail(f"the witness reports the end of {pid} before the kill")
        sender = log[sent[0]].split()[0]
        waiter = log[reaped[0]].split()[0]
        if sender != waiter:
            fail(f"{sender} sent the signal and {waiter} waited: not one supervisor")
        print(f"witness: {log[sent[0]].strip()}")
        print(f"witness: {log[reaped[0]].strip()}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("campaign", type=pathlib.Path)
    parser.add_argument("--strace", type=pathlib.Path)
    options = parser.parse_args()
    kills = recorded_kills(options.campaign)
    check_trace(options.campaign, kills)
    database = check_store(options.campaign)
    print(f"{len(kills)} kill recorded: SIGKILL, wait status signal 9; recovery on {database}")
    if options.strace is None:
        print("no outside witness given: the kill is the bench's word (campaign.json)")
        return
    check_witness(options.strace.read_text().splitlines(), kills)


if __name__ == "__main__":
    main()
