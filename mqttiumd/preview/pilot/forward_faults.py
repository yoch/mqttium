#!/usr/bin/env python3
"""Exercise process cuts with the extracted normal broker and real mTLS clients."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace

from common import Broker, PACKAGE, event_rows, inventory, provision, read_db, require, wait_for, write
from forward_client import TransportLost, Wire, emit, network_args, run_dashboard, run_sensor
from forward_reception import Actor, require_binary
from forward_store import Event, Outbox, QUOTA, backup, snapshot


SCRIPT = Path(__file__).resolve()
CLIENT_ID = "capteur-1"
FIRST = "dashboard-1"
SECOND = "dashboard-2"


def run_actor():
    parser = argparse.ArgumentParser(description="Hold an application boundary for a process-cut witness")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("producer", "dashboard"):
        p = commands.add_parser(command)
        network_args(p, producer=command == "producer")
        p.add_argument("--stage", required=True)
        p.add_argument("--reason", type=int)
        p.add_argument("--sequence", type=int, default=1)
        p.add_argument("--release", type=Path)
    args = parser.parse_args()
    entered = False

    def cut(stage, event, **fields):
        nonlocal entered
        if (entered or stage != args.stage or (args.sequence and event.sequence != args.sequence)
                or (args.reason is not None and fields.get("reason") != args.reason)):
            return
        entered = True
        emit("cut", pid=os.getpid(), stage=stage, event_id=event.event_id, sha256=event.digest.hex(), **fields)
        wait_for(lambda: args.release and args.release.is_file(), "cut_not_released_or_killed", duration=45)

    os.umask(0o077)
    (run_sensor if args.command == "producer" else run_dashboard)(args, cut)


class Case:
    def __init__(self, root, binary, pki):
        self.root = root
        self.binary = binary
        self.pki = pki
        self.broker = None
        self.actors = []
        self.stopped = []
        self.brokers = []
        self.record = {}

    def open(self, name="broker"):
        self.broker = Broker(self.binary, self.root / "broker.sqlite", self.root, self.pki, label=name)

    def close_broker(self, kill=False):
        row = self.broker.stop(kill=kill)
        self.brokers.append(row)
        self.broker = None
        require(row["signalled"] and row["wait_status"] == (-9 if kill else 0), "unexpected_broker_wait_status")
        return row

    def actor(self, command, cid, label, *, stage=None, reason=None, seq=1, release=None, db=None):
        kw = {}
        if stage is not None:
            extra = ["--stage", stage, "--sequence", str(seq)]
            if reason is not None:
                extra += ["--reason", str(reason)]
            if release is not None:
                extra += ["--release", str(release)]
            kw.update(script=SCRIPT, extra=extra)
        actor = Actor(command, cid, db or self.root / f"{cid}.sqlite", self.pki, self.broker.port,
                      self.root / f"{label}.jsonl", **kw)
        self.actors.append(actor)
        return actor

    def ready(self, actor, *, resumed=False, producer=False):
        kind = "producer_ready" if producer else "dashboard_ready"
        result = wait_for(lambda: actor.ready(kind), f"missing_{kind}_{actor.identity}")
        require(result["session_present"] is resumed, "unexpected_session_present_at_boundary")
        return result

    def dashboards(self, offline=False):
        first = self.actor("dashboard", FIRST, "live")
        second = self.actor("dashboard", SECOND, "initial-second")
        for actor in (first, second):
            self.ready(actor)
        if offline:
            self.stop(second)
        return first, second

    def stop(self, actor, kill=False):
        result = actor.stop(kill=kill)
        self.stopped.append(result)
        self.actors.remove(actor)
        require(result["signalled"] and result["wait_status"] == (-9 if kill else 0), "unexpected_actor_wait_status")
        return result

    def cut(self, actor):
        witness = wait_for(lambda: actor.ready("cut"), "application_cut_not_reached", duration=15)
        require(witness["pid"] == actor.child.pid, "cut_pid_mismatch")
        return witness

    def seed(self, count=1):
        db = self.root / f"{CLIENT_ID}.sqlite"
        with Outbox(db, CLIENT_ID) as store:
            events = [store.enqueue(f"cut telemetry {i}".encode()) for i in range(count)]
        return db, events

    def prove(self, dashboards, events, *, duplicates):
        expected = [(e.sensor, e.stream, e.sequence, e.wire) for e in events]
        for dashboard in dashboards:
            wait_for(lambda: snapshot(dashboard.db)["unique_count"] == len(events), "dashboard_durable_count_incomplete")
            with read_db(dashboard.db) as db:
                require(db.execute("SELECT sensor,stream,seq,wire FROM applied ORDER BY sensor,stream,seq").fetchall() == expected,
                        "incident_event_id_or_payload_changed")
            state = snapshot(dashboard.db)
            require(state["duplicates"] == duplicates and state["invalid"] == state["conflicts"] == 0,
                    "incident_deduplication_count_wrong")

    def drained(self, db):
        wait_for(lambda: snapshot(db)["rows"] == 0 and inventory(self.broker.db)["counts"]["obligations"] == 0,
                 "incident_responsibilities_not_finished", duration=15)
        counts = inventory(self.broker.db)
        require(counts["counts"]["outbound"] == 0 and counts["invariants_ok"], "incident_durable_accounts_wrong")
        return {"outbox": snapshot(db), "broker": counts}

    def committed(self, kind, *, events=None):
        return sum(row["kind"] == "store_result" and row.get("command_kind") == kind
                   and row["details"]["committed_now"] for row in event_rows(events or self.broker.events))

    def cleanup(self):
        for actor in self.actors:
            self.stopped.append(actor.stop())
        if self.broker:
            self.brokers.append(self.broker.stop())
        self.record.update(actors=self.stopped, brokers=self.brokers)


def simple_cut(case, stage):
    case.open()
    dashboards = case.dashboards()
    db, events = case.seed()
    current = case.actor("producer", CLIENT_ID, "cut-producer", stage=stage, reason=0 if stage == "result_recorded" else None)
    witness = case.cut(current)
    original = events[0]
    require(witness["event_id"] == original.event_id and witness["sha256"] == original.digest.hex(), "cut_event_identity_mismatch")
    if stage != "before_publish":
        case.prove(dashboards, events, duplicates=0)
        require(case.committed("Admission") == 1, "first_publication_did_not_commit")
    else:
        require(case.committed("Admission") == 0, "publication_preceded_local_cut")
    before = snapshot(db)
    expected_before = {"before_publish": (0, 0, 0), "sent": (1, 1, 0), "result_recorded": (1, 0, 1)}[stage]
    require((before["attempts"], before["inflight"], before["confirmed"]) == expected_before and before["rows"] == 1,
            "unexpected_local_attempt_state_at_cut")
    case.stop(current, kill=True)
    restarted = case.actor("producer", CLIENT_ID, "resumed-producer")
    case.ready(restarted, producer=True)
    final = case.drained(db)
    unknown = int(stage == "sent")
    case.prove(dashboards, events, duplicates=unknown)
    require(final["outbox"]["unknown"] == unknown and final["outbox"]["attempts"] == 1 + unknown,
            "recovery_attempt_count_wrong")
    require(case.committed("Admission") == 1 + unknown, "admission_count_after_recovery_wrong")
    case.record.update(witness=witness, before=before, final=final, dashboards=[snapshot(a.db) for a in dashboards])


def quota_and_drain(case):
    case.open()
    first, second = case.dashboards(offline=True)
    db, events = case.seed(145)
    # The producer reaches its first real quota reply with the durable second
    # dashboard still offline. The watcher selects the exact event from it.
    cut = case.actor("producer", CLIENT_ID, "quota-cut", stage="result_recorded", reason=QUOTA, seq=0)
    witness = case.cut(cut)
    before = snapshot(db)
    require(before["confirmed"] > 0 and before["quota"] == 1 and before["inflight"] == 0
            and before["rows"] >= before["pending"] > 0 and before["last_reason"] == QUOTA
            and witness["event_id"] == events[before["confirmed"]].event_id, "quota_cut_did_not_preserve_refused_event")
    case.stop(cut, kill=True)
    loaded = inventory(case.broker.db)
    require(loaded["counts"]["obligations"] > 0 and snapshot(second.db)["unique_count"] == 0,
            "offline_broker_backlog_not_captured")
    independent = independent_puback(case)
    with read_db(case.broker.db) as store:
        target, = store.execute("SELECT id FROM sessions WHERE client_id=?", (SECOND.encode(),)).fetchone()
    case.close_broker(kill=True)
    case.stop(first)
    case.open("broker-restarted")
    recovered = inventory(case.broker.db)
    require(recovered["backlog_by_session"].get(str(int.from_bytes(target)), 0) == before["confirmed"],
            "broker_crash_lost_offline_responsibility")
    resumed = [case.actor("dashboard", cid, f"resumed-{cid}") for cid in (FIRST, SECOND)]
    for dashboard in resumed:
        case.ready(dashboard, resumed=True)
    seq = before["confirmed"] + 4
    mid_drain = case.actor("producer", CLIENT_ID, "drain-cut", stage="sent", seq=seq)
    drain_witness = case.cut(mid_drain)
    require(drain_witness["event_id"] == events[seq - 1].event_id, "drain_cut_changed_identity")
    wait_for(lambda: all(snapshot(a.db)["unique_count"] == seq for a in resumed), "drain_cut_not_admitted")
    case.stop(mid_drain, kill=True)
    sender = case.actor("producer", CLIENT_ID, "drain-resumed")
    case.ready(sender, producer=True)
    final = case.drained(db)
    case.prove(resumed, events, duplicates=1)
    require(final["outbox"]["unknown"] == 1 and final["outbox"]["confirmed"] == len(events), "drain_recovery_counts_wrong")
    case.record.update(witness=witness, drain_witness=drain_witness, before=before,
                       store_before_broker_crash=loaded, store_after_broker_crash=recovered, final=final,
                       dashboards=[snapshot(a.db) for a in resumed], independent_puback=independent)


def consumer_cut(case):
    case.open()
    held = case.actor("dashboard", FIRST, "commit-cut", stage="consumer_committed")
    case.ready(held)
    other = case.actor("dashboard", SECOND, "other")
    case.ready(other)
    db, events = case.seed()
    sender = case.actor("producer", CLIENT_ID, "sender")
    case.ready(sender, producer=True)
    initial = case.cut(held)
    wait_for(lambda: snapshot(db)["confirmed"] == 1, "producer_positive_ack_absent")
    require(initial["applied"] == "applied" and not initial["duplicate"], "unexpected_initial_consumer_commit")
    wait_for(lambda: inventory(case.broker.db)["counts"]["obligations"] == 1, "original_puback_not_held")
    case.stop(held, kill=True)
    copy = case.root / "inbox-restored.sqlite"
    backup(held.db, copy)
    release = case.root / "release-dup"
    resumed = case.actor("dashboard", FIRST, "resumed-inbox", stage="consumer_committed", release=release, db=copy)
    repeat = case.cut(resumed)
    require((repeat["event_id"], repeat["mid"], repeat["sha256"], repeat["duplicate"], repeat["applied"])
            == (initial["event_id"], initial["mid"], initial["sha256"], True, "duplicate"), "durable_dedup_replay_identity_mismatch")
    release.touch()
    case.ready(resumed, resumed=True)
    final = case.drained(db)
    case.prove([resumed], events, duplicates=1)
    case.prove([other], events, duplicates=0)
    require(case.committed("Admission") == 1, "consumer_replay_created_new_admission")
    case.record.update(initial=initial, repeat=repeat, final=final, restored_consumer=snapshot(copy))


def independent_puback(case):
    args = SimpleNamespace(identity="capteur-3", host="127.0.0.1", port=case.broker.port, pki=case.pki)
    wire = Wire(args, 0)
    try:
        wire.connect(fresh=True)
        control = SimpleNamespace(topic="atelier/capteur-3/control", wire=b"control")
        reason = wire.send(control, None)
        require(reason in (0, 0x10), "independent_publication_stalled")
        return reason
    finally:
        wire.close()


def authorization_and_conflict(case):
    case.open()
    dashboards = case.dashboards()
    db, events = case.seed()
    sender = case.actor("producer", CLIENT_ID, "sender")
    case.ready(sender, producer=True)
    case.drained(db)
    case.prove(dashboards, events, duplicates=0)
    case.stop(sender)
    original = events[0]
    args = SimpleNamespace(identity="capteur-2", host="127.0.0.1", port=case.broker.port, pki=case.pki)
    wire = Wire(args, 0)
    disconnects = []
    wire.client.on_disconnect = lambda _c, _u, flags, reason, _p: disconnects.append((flags.is_disconnect_packet_from_server, reason.value))
    try:
        wire.connect(fresh=True)
        try:
            wire.send(original, None)
            raise ValueError("forbidden_publication_received_puback")
        except TransportLost:
            refusal = [row for row in event_rows(case.broker.events) if row["kind"] == "authorization_refused"]
            case.record.update(forbidden_disconnect=disconnects, forbidden_broker_refusal=refusal)
            require(len(refusal) == 1 and refusal[0]["details"] == {"stage": "publish", "reason": 0x87}
                    and case.committed("Admission") == 1, "forbidden_topic_changed_durable_admission")
    finally:
        wire.close()
    args.identity = CLIENT_ID
    wire = Wire(args, 0)
    conflicting = Event.encode(CLIENT_ID, original.stream, original.sequence, original.created_ns, b"changed telemetry")
    try:
        wire.connect(fresh=True)
        require(wire.send(conflicting, None) == 0, "conflict_witness_was_not_admitted")
    finally:
        wire.close()
    for dashboard in dashboards:
        wait_for(lambda: dashboard.child.poll() is not None, "contradiction_not_signalled")
        require(dashboard.child.returncode == 1 and snapshot(dashboard.db)["conflicts"] == 1,
                "contradiction_not_rejected_after_durable_dedup")
        with read_db(dashboard.db) as inbox:
            require(inbox.execute("SELECT wire FROM applied").fetchall() == [(original.wire,)], "conflict_replaced_application")
    remaining = inventory(case.broker.db)
    require(remaining["counts"]["obligations"] == 2, "contradictory_payload_was_acknowledged")
    case.record.update(forbidden_disconnect=disconnects, conflicting_event=original.event_id,
                       stored_sha256=original.digest.hex(), refused_sha256=conflicting.digest.hex(), broker=remaining,
                       dashboards=[snapshot(a.db) for a in dashboards])


CASES = {
    "enqueue-before-publish": lambda case: simple_cut(case, "before_publish"),
    "sent-before-puback-read": lambda case: simple_cut(case, "sent"),
    "positive-before-collection": lambda case: simple_cut(case, "result_recorded"),
    "quota-broker-and-drain": quota_and_drain,
    "inbox-commit-before-ack": consumer_cut,
    "authorization-and-conflict": authorization_and_conflict,
}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("output", type=Path)
    p.add_argument("--package", type=Path, default=PACKAGE)
    p.add_argument("--binary", type=Path, default=PACKAGE / "mqttiumd")
    p.add_argument("--case", choices=CASES, action="append")
    args = p.parse_args()
    os.umask(0o077)
    args.output.mkdir(mode=0o700)
    results = {"build": require_binary(args.binary, args.package), "cases": {}}
    with tempfile.TemporaryDirectory(prefix="forward-fault-pki-") as private:
        pki = Path(private) / "pki"
        provision(pki, args.package)
        for name in args.case or CASES:
            path = args.output / name
            path.mkdir(mode=0o700)
            case = Case(path, args.binary, pki)
            try:
                CASES[name](case)
                case.record["status"] = "PASS"
            except Exception as error:
                case.record.update(status="FAIL", failure=repr(error))
            finally:
                case.cleanup()
            write(path / "receipt.json", case.record)
            results["cases"][name] = case.record
            print(json.dumps({"case": name, "status": case.record["status"], "failure": case.record.get("failure")}), flush=True)
    results["status"] = "PASS" if all(r["status"] == "PASS" for r in results["cases"].values()) else "FAIL"
    write(args.output / "faults.json", results)
    return 0 if results["status"] == "PASS" else 1


if __name__ == "__main__":
    if sys.argv[1:2] in (["producer"], ["dashboard"]):
        run_actor()
    else:
        raise SystemExit(main())
