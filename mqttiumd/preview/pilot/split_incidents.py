#!/usr/bin/env python3
"""Cut the two links and one producer; verify their separate responsibilities."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time

from common import CAPTEURS, DASHBOARDS, PACKAGE, event_rows, inventory, read_db, require, sha, topic, wait_for, write
from forward_client import Wire, emit
from forward_reception import Actor, RATE, atomically_write, require_binary, totals
from forward_store import Event, snapshot
from segment_store import open_existing
from split_client import HistoryEvent, parser as client_parser, run_sensor, subscribe
from split_reception import CLIENT, Plane, SplitCampaign, independent_control, provision_planes


def producer_worker():
    args = client_parser().parse_args()
    os.umask(0o077)

    def cut(point, event):
        if args.db.with_suffix(".before-live").exists():
            emit("cut_reached", point=point, pid=os.getpid(), event_id=event.event_id,
                 digest=event.digest.hex(), sequence=event.sequence)
            signal.pause()

    run_sensor(args, cut)


def retained_value(db, name):
    with read_db(db) as connection:
        hit = connection.execute("""SELECT b.bytes FROM retained r JOIN publications p ON p.id=r.publication
                                   JOIN payloads b ON b.id=p.payload WHERE r.topic=?""", (name.encode(),)).fetchone()
        return bytes(hit[0]) if hit else None


def late_presence(pki, live, actor, status):
    args = argparse.Namespace(identity=DASHBOARDS[1], pki=pki, host="127.0.0.1", port=live.port)
    wire = Wire(args, 0)
    name = topic(actor, "status")
    seen = []

    def receive(client, _data, message):
        require(message.topic == name and message.payload == status and message.qos == 1
                and message.retain and client.ack(message.mid, 1) == 0, "late_dashboard_wrong_retained_presence")
        seen.append({"topic": name, "value": status.decode("ascii"), "qos": 1, "retain": True})

    wire.client.on_message = receive
    try:
        require(not wire.connect(fresh=True), "late_live_dashboard_unexpected_session")
        subscribe(wire, [(name, 1)])
        wire.until(lambda: bool(seen), "late_presence_not_observed")
        return seen[0]
    finally:
        wire.close()


def wait_arrivals(viewer, begin, end):
    # Bound the incident and count actual live frames, not producer attempts.
    arrivals = [fact for fact in viewer.facts("live_received") if begin <= fact["monotonic_ns"] <= end]
    return {"window_s": (end - begin) / 1e9, "arrivals": len(arrivals),
            "sample_ids": [item["event_id"] for item in arrivals[:8]]}


class Incidents(SplitCampaign):
    def actor(self, command, identity, suffix="", *, schedule=None):
        if command == "producer" and identity == CAPTEURS[0] and not suffix:
            script = Path(__file__).resolve()
            flags = ("--trace", "--live-port", str(self.live.port), "--live-pki", str(self.pkis["live"]))
            actor = Actor(command, identity, self.out / (identity + ".sqlite"), self.pki, self.broker.port,
                          self.out / (command + "-" + identity + ".jsonl"), schedule=schedule, script=script, extra=flags)
            self.actors.append(actor)
            return actor
        return super().actor(command, identity, suffix, schedule=schedule)

    def restart(self, name, *, killed, after_stop=None):
        before = self.broker if name == "history" else self.live
        tick = time.monotonic_ns()
        proof = before.stop(kill=killed)
        require(proof["signalled"] and proof["wait_status"] == (-9 if killed else 0),
                f"{name}_failed_to_stop_by_requested_signal")
        if after_stop:
            additional = after_stop()
        else:
            additional = {}
        resumed = Plane(self.args.binary, self.out, self.pkis[name], name, port=before.port,
                        run=self.runs[name] + 1)
        self.runs[name] += 1
        require(resumed.db == before.db and resumed.port == before.port and resumed.child.pid != before.child.pid,
                f"{name}_restart_identity")
        if name == "history":
            self.broker = resumed
        else:
            self.live = resumed
        return {"prior_process": proof, "new_process": resumed.identity(), "killed_ns": tick,
                "ready_ns": time.monotonic_ns(), **additional}

    def receive(self):
        self.runs = {"live": 1, "history": 1}
        self.begin()
        q = self.receipt
        q["client_id_denied"] = {lane: independent_control(plane, self.pkis[lane], presented=CAPTEURS[1],
                                                           client_id=CAPTEURS[0], refused=True)
                                 for lane, plane in (("live", self.live), ("history", self.broker))}
        pressure = self.until("outage", lambda state: state["total"]["quota"] > 0
                              and state["brokers"]["history"]["accounts"]["references"] >= 127,
                              self.start + 10_000_000_000)
        q["history_pressure_before_producer_kill"] = pressure["brokers"]["history"]
        victim = self.sensors[0]
        victim.db.with_suffix(".before-live").touch()
        cut = wait_for(lambda: victim.ready("cut_reached"), "producer_commit_before_live_not_observed")
        require(cut["pid"] == victim.child.pid and cut["point"] == "enqueued_before_live",
                "wrong_producer_cut_identity")
        with read_db(victim.db) as db:
            row = db.execute("SELECT wire FROM queued WHERE seq=?", (cut["sequence"],)).fetchone()
            require(row is not None and Event.decode(topic(victim.identity), row[0]).digest.hex() == cut["digest"],
                    "producer_cut_did_not_follow_durable_enqueue")
        started_kill = time.monotonic_ns()
        killed = victim.stop(kill=True)
        require(killed["wait_status"] == -9 and killed["pid"] == cut["pid"], "wrong_sensor_killed")
        q["producer_cut"] = {"boundary": cut, "signal": killed, "kill_start_ns": started_kill}
        offline = wait_for(lambda: next((fact for fact in self.viewer.facts("presence")
                                          if fact["identity"] == victim.identity and fact["state"] == "offline"
                                          and fact["monotonic_ns"] >= started_kill), None), "live_will_not_delivered")
        require(retained_value(self.live.db, topic(victim.identity, "status")) == b"offline"
                and inventory(self.broker.db)["accounts"]["references"] >= 127,
                "live_will_blocked_or_history_pressure_lost")
        will = wait_for(lambda: next((row for row in event_rows(self.live.events)
                                      if row["kind"] == "will_admitted"), None), "will_admission_not_observed")
        q["will_under_history_pressure"] = {"wire_observation": offline, "broker_admission": will,
                                             "kill_to_receive_ms": (offline["monotonic_ns"] - started_kill) / 1e6,
                                             "late_dashboard": late_presence(self.pkis["live"], self.live, victim.identity, b"offline")}
        victim.db.with_suffix(".before-live").unlink()
        self.sensors[0] = self.actor("producer", victim.identity, "-recovered", schedule=self.schedule)
        wait_for(lambda: self.sensors[0].ready("live_producer_ready"), "recovered_sensor_live_not_ready")
        wait_for(lambda: self.sensors[0].ready("producer_ready"), "recovered_sensor_history_not_ready")
        q["presence_after_return"] = late_presence(self.pkis["live"], self.live, victim.identity, b"online")

        # Use an intentional visible outage of one lane. The other lane
        # continues interacting with actual clients throughout the interval.
        history_off_start = time.monotonic_ns()
        history_cut = self.restart("history", killed=True, after_stop=lambda: self.hold(.8))
        window = wait_arrivals(self.viewer, history_off_start, history_cut["ready_ns"])
        require(window["arrivals"] >= 20 and self.viewer.child.poll() is None, "history_restart_interrupted_live")
        q["history_restart"] = {**history_cut, "verified_live": window}
        self.dashboards[1] = self.actor("history", DASHBOARDS[1], "-resumed")
        ready = wait_for(lambda: self.dashboards[1].ready("history_ready"), "history_catchup_no_connack")
        require(ready["session_present"], "history_restart_lost_offline_dashboard_session")
        q["history_restart"]["offline_dashboard_return"] = ready

        def both_flow():
            states = [snapshot(dash.db) for dash in self.dashboards]
            return states if min(state["unique_count"] for state in states) > 200 else None

        wait_for(both_flow, "history_catchup_stalled_after_restart", duration=15)
        before = [snapshot(dash.db)["unique_count"] for dash in self.dashboards]
        live_cut = self.restart("live", killed=True, after_stop=lambda: self.hold(1.0))
        after = [snapshot(dash.db)["unique_count"] for dash in self.dashboards]
        require(min(b - a for a, b in zip(before, after)) >= 20, "live_restart_interrupted_history")
        q["live_restart"] = {**live_cut, "history_counts_before": before, "history_counts_after": after,
                              "arrivals_during_live_cut": wait_arrivals(self.viewer, live_cut["killed_ns"], live_cut["ready_ns"])}
        wait_for(lambda: len(list(self.viewer.facts("live_ready"))) >= 2, "live_viewer_did_not_reconnect")
        for sensor in self.sensors:
            wait_for(lambda: len(list(sensor.facts("live_producer_ready"))) >= 2, "live_producer_did_not_reconnect")
        expected = max(750, (time.monotonic_ns() + 1_500_000_000 - self.start) * RATE // 1_000_000_000)
        require(expected < self.maximum, "incident_schedule_exhausted_too_soon")
        atomically_write(self.schedule.with_suffix(".stop.json"), {"events": expected})

        def drained():
            sensors = [snapshot(a.db) for a in self.sensors]
            counts = totals(sensors)
            broker = inventory(self.broker.db)
            if (counts["enqueued"] == counts["confirmed"] == expected and counts["rows"] == 0
                    and all(snapshot(d.db)["unique_count"] == expected for d in self.dashboards)
                    and not broker["accounts"]["references"]):
                return sensors
            return None

        sensors = wait_for(drained, "incident_history_not_drained", duration=30)
        for actor in self.sensors:
            ended = actor.stop()
            require(ended["wait_status"] == 0, "sensor_normal_disconnect_failed")
        wait_for(lambda: inventory(self.live.db)["counts"]["wills"] == 0, "normal_disconnect_left_will")
        q["normal_disconnect_no_will"] = {"intents_left": 0,
                                            "current_retained": late_presence(self.pkis["live"], self.live, victim.identity, b"online")}
        q["live_cold_restart"] = self.restart("live", killed=False)
        q["retained_after_restart"] = late_presence(self.pkis["live"], self.live, victim.identity, b"online")
        q["archived_repeat"] = self.repeat_archived()

        for dashboard in self.dashboards:
            require(dashboard.stop()["wait_status"] == 0, "history_dashboard_failed_to_stop")
        require(self.viewer.stop()["wait_status"] == 0, "live_dashboard_failed_to_stop")
        live = {fact["event_id"]: fact["digest"] for fact in self.viewer.facts("live_received")}
        require(cut["event_id"] not in live, "cut_event_was_live_published")
        absent = []
        with ExitStack() as stack:
            stores = [stack.enter_context(open_existing(dashboard.db)) for dashboard in self.dashboards]
            records = [store.records() for store in stores]
            for state in sensors:
                sensor = state["identity"]
                stream = bytes.fromhex(state["stream"])
                for seq in range(1, state["enqueued"] + 1):
                    a, b = next(records[0], None), next(records[1], None)
                    require(a and b and a[:4] == b[:4] and a[:3] == (sensor, stream, seq),
                            "incident_history_event_lost_or_inconsistent")
                    event = Event.decode(topic(sensor), a[3])
                    found = live.pop(event.event_id, None)
                    if found is None:
                        absent.append(event.event_id)
                    else:
                        require(found == event.digest.hex(), "incident_lane_payload_differs")
            require(all(next(r, None) is None for r in records) and not live, "incident_extra_records")
        require(cut["event_id"] in absent, "intentional_gap_missing_from_history_repair")
        q["reconciliation"] = {"all_events_on_each_history": expected, "gaps_recovered": absent,
                               "fault_event_recovered": cut["event_id"],
                               "latest_live_display": next(self.viewer.facts("live_final"))["latest"]}
        require(not inventory(self.broker.db)["accounts"]["references"], "incident_broker_references_not_drained")

    def repeat_archived(self):
        sensor = self.sensors[0]
        path = self.dashboards[0].db
        with read_db(path) as db:
            row = db.execute("""SELECT archived FROM streams WHERE sensor=? LIMIT 1""", (sensor.identity,)).fetchone()
            require(row and row[0] >= 64, "incident_archive_repeat_precondition")
        # Extract the first committed envelope from a verified history copy.
        original = self.dashboards[0].stop()
        require(original["wait_status"] == 0, "history_swap_before_repeat_failed")
        with open_existing(path) as store:
            item = next(store.records())
            event = Event.decode(topic(item[0]), item[3])
        self.dashboards[0] = self.actor("history", DASHBOARDS[0], "-duplicate")
        wait_for(lambda: self.dashboards[0].ready("history_ready"), "repeat_dashboard_not_ready")
        before = [snapshot(a.db) for a in self.dashboards]
        wire = Wire(argparse.Namespace(identity=sensor.identity, pki=self.pkis["history"],
                                       host="127.0.0.1", port=self.broker.port), 0)
        try:
            wire.connect(fresh=True)
            require(wire.send(HistoryEvent(event.sensor, event.stream, event.sequence, event.created_ns, event.wire), None) == 0,
                    "archive_repeat_not_admitted")
        finally:
            wire.close()
        after = wait_for(lambda: [snapshot(a.db) for a in self.dashboards] if all(
            snapshot(a.db)["duplicates"] == prior["duplicates"] + 1
            for a, prior in zip(self.dashboards, before))
                         and not inventory(self.broker.db)["accounts"]["references"] else None,
                         "archive_repeat_not_deduplicated")
        require(all(a["unique_count"] == b["unique_count"] for a, b in zip(before, after)), "archive_repeat_reapplied")
        return {"event_id": event.event_id, "digest": event.digest.hex(), "pre": before, "post": after}

    @staticmethod
    def hold(seconds):
        time.sleep(seconds)
        return {"intentional_downtime_s": seconds}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("output", type=Path)
    cli.add_argument("--package", type=Path, default=PACKAGE)
    args = cli.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(mode=0o700)
    args.binary = args.package / "mqttiumd"
    args.short, args.outage, args.catchup_limit = True, 45, 60
    os.umask(0o077)
    receipt = {"status": "FAIL", "build": require_binary(args.binary, args.package),
               "files": {p.name: sha(p) for p in (Path(__file__), CLIENT, Path(__file__).with_name("split_reception.py"))}}
    with tempfile.TemporaryDirectory(prefix="mqttiumd-split-incidents-") as private:
        pkis = provision_planes(Path(private), args.package)
        test = Incidents(args, pkis, receipt)
        try:
            test.receive()
            receipt["status"] = "PASS"
        except (Exception, KeyboardInterrupt) as error:
            receipt["failure"] = repr(error)
        finally:
            test.close()
            write(args.output / "result.json", receipt)
    print(json.dumps({"status": receipt["status"], "failure": receipt.get("failure"),
                      "receipt": str(args.output / "result.json")}), flush=True)
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "producer":
        producer_worker()
    else:
        raise SystemExit(main())
