# 0019 — A real kill of the broker: its record, its claim, and what it must show

Status: **Proposed** (15 November 2026, lot L3, second wave). Written by
lot L3 at the request of the integration owner and of lot L4, to be
recorded by them. It completes record 0016 (the failure model of a run)
and DEM-011. It becomes a requirement only when the owner of the
repository accepts it.

## Context

`BrokerCrashed {"failure_model": "process"}` stands today for the crash
the bench simulates inside its own process. Lot L4 runs the broker as a
child process and kills it with SIGKILL. A run that claims such a kill
must show it, and the oracle judges a trace with its scenario, never with
a manifest: the claim has to be in the trace.

## Decision

- **`BrokerKilled`**, stated by the bench supervisor only:
  `{"signal_sent": "SIGKILL", "wait_status": {"signal": <1..64>}}`, both
  required, nothing else. The supervisor writes it after `wait()` returned
  the status of the child, before `BrokerCrashed` (unchanged) and the
  `PeerConnectionClosed` records; its parents are the frontier of the
  child it had read (record 0016: the parents of a record of the bench
  order it, they cause nothing). A status of a process that exited is no
  kill and is never written by this record. No pid enters the trace, so
  that the same seed gives the same bytes; the pid, the signal and the
  status go to `campaign.json`.
- **The claim.** `TraceHeader.broker` and `run-manifest.broker`, optional,
  `in-process` or `child-process`; absent means `in-process`, so every
  existing trace and manifest keeps its meaning and its bytes. The header
  is what the judge reads; the manifest states the same, and `produce`
  requires the two to agree for a qualifying result.
- **The assertion** `broker_killed_for_real`, owed by every run. On a
  trace whose header does not claim a child process it has nothing to
  check and passes, unless the trace holds a `BrokerKilled`, which a run
  in-process cannot write (FAIL). On a trace that claims one:
  - every `BrokerCrashed` follows a `BrokerKilled` of the same crash
    (between it and the previous crash); a missing one is missing
    evidence: INCONCLUSIVE, never PASS;
  - a `wait_status.signal` other than 9 is FAIL;
  - no record of an origin of the child (`broker-core`, `transport`,
    `timer`, `store-adapter`, `dispatcher`, `fault-injector`) of the
    killed incarnation comes after the `BrokerKilled`: FAIL; the evidence
    of the store (`store-evidence`) outlives the broker and is not
    counted;
  - the child's records after the kill belong to a later incarnation:
    FAIL otherwise.

## Not decided here

- Which origins run in the child for the SQLite topology, if it differs:
  the list above is lot L4's to confirm.
