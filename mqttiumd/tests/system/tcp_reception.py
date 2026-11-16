#!/usr/bin/env python3
"""Finite system reception of the actual MQTT 5 incoming QoS 2 product.

This does not fabricate a fixture and is not an oracle qualifying campaign.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from tcp_probe import Observations, ReceiptFailure, Wire, database, read_events, require, write_json


class Broker:
    def __init__(self, binary: Path, case: Path, *, db: Path | None = None,
                 label: str = "broker", pauses: tuple[str, ...] = ()):
        case.mkdir(parents=True, exist_ok=True)
        self.binary = binary.resolve()
        self.case = case
        self.db = db or case / "store.sqlite"
        self.events_path = case / f"{label}.events.jsonl"
        self.control = case / f"{label}.control"
        self.control.mkdir()
        self.observations = Observations(case / f"{label}.observer.jsonl")
        command = [str(self.binary), "--db", str(self.db), "--listen", "127.0.0.1:0",
                   "--events", str(self.events_path), "--test-control", str(self.control)]
        if pauses:
            command += ["--test-pause", ",".join(pauses)]
        self.stdout_path = case / f"{label}.stdout"
        self.stderr_path = case / f"{label}.stderr"
        self.stdout = self.stdout_path.open("wb")
        self.stderr = self.stderr_path.open("wb")
        self.process = subprocess.Popen(command, stdout=self.stdout, stderr=self.stderr)
        self.address = None
        self.observations.record("process_started", pid=self.process.pid, command=command,
                                 binary_sha256=hashlib.sha256(self.binary.read_bytes()).hexdigest())

    def events(self) -> list[dict]:
        events = read_events(self.events_path)
        require(all(event["pid"] == self.process.pid for event in events), "wrong_event_process")
        return events

    def wait_event(self, kind: str, *, point: str | None = None, timeout: float = 8,
                   after: int = -1) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for event in self.events():
                if int(event["seq"]) > after and event["kind"] == kind and (point is None or event.get("point") == point):
                    self.observations.record("event_retained", event=event)
                    return event
            if self.process.poll() is not None:
                raise ReceiptFailure("broker_exited_before_event", f"{kind}: exit {self.process.returncode}; {self.stderr_path}")
            time.sleep(.005)
        raise ReceiptFailure("event_not_reached", kind if point is None else f"{kind}/{point}")

    def ready(self) -> dict:
        event = self.wait_event("ready")
        address = event["details"]["address"]
        host, port = address.rsplit(":", 1)
        self.address = (host, int(port))
        return event

    def release(self, point: str) -> None:
        (self.control / f"release-{point}").touch(exist_ok=False)
        self.observations.record("barrier_release", point=point)

    def stop(self, *, killed: bool = False, timeout: float = 8) -> int:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if killed else signal.SIGTERM)
        try:
            status = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            self.process.kill()
            self.process.wait(timeout=3)
            raise ReceiptFailure("shutdown_deadline", str(self.process.pid)) from error
        finally:
            self.stdout.close()
            self.stderr.close()
        self.observations.record("process_wait", pid=self.process.pid, status=status,
                                 requested="SIGKILL" if killed else "SIGTERM")
        require(status == (-9 if killed else 0), "unexpected_process_status", str(status))
        require(not Path(f"/proc/{self.process.pid}").exists(), "child_survived")
        return status

    def cleanup(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=3)
        self.stdout.close()
        self.stderr.close()


def paho(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    try:
        ready = broker.ready()
        receipts = []
        for index in range(2):
            command = [sys.executable, str(Path(__file__).with_name("external_paho.py")),
                       "--host", broker.address[0], "--port", str(broker.address[1]),
                       "--client-id", "external-paho"]
            if index:
                command.append("--connect-only")
            result = subprocess.run(command, capture_output=True, text=True, timeout=15)
            (case / f"paho-{index}.stderr").write_text(result.stderr)
            require(result.returncode == 0, "external_client_failed", result.stderr[-512:])
            receipt = json.loads(result.stdout)
            write_json(case / f"paho-{index}.json", receipt)
            require(receipt["session_present"] == bool(index), "wrong_session_present")
            if index == 0:
                require(any(event["kind"] == "qos2_completed" for event in receipt["events"]), "external_not_completed")
            receipts.append({"command": command, "result": receipt})
        state = database(broker.db)
        write_json(case / "durable.json", state)
        require(state["counts"]["sessions"] == 1, "session_not_created_once")
        require(len(state["admissions"]) == 1 and len(state["releases"]) == 1, "exchange_not_committed")
        require(all(state["counts"][name] == 0 for name in ("inbound", "publications", "payloads", "obligations")),
                "publication_retained_after_normal_cycle")
        broker.stop()
        return {"criterion": "A", "status": "PASS", "ready": ready,
                "external_clients": receipts, "durable": state}
    finally:
        broker.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--mode", choices=("basic", "complete"), default="basic")
    args = parser.parse_args()
    require(args.binary.is_file(), "binary_missing", str(args.binary))
    args.out.mkdir(parents=True, exist_ok=True)
    require(not any(args.out.iterdir()), "output_not_empty")
    result = {"format": "mqttiumd.tcp-system-reception", "schema_version": 1,
              "qualification": "system-test-only", "scope": "MQTT 5 durable incoming QoS 2; no subscriber delivery or complete broker claim",
              "binary": {"path": str(args.binary.resolve()), "sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()},
              "mode": args.mode, "cases": []}
    try:
        result["cases"].append(paho(args.binary, args.out / "A-external"))
        require(args.mode == "basic", "complete_receipt_not_integrated")
        result["status"] = "PASS"
    except Exception as error:
        result["status"] = "FAIL"
        result["failure"] = {"code": getattr(error, "code", type(error).__name__), "detail": str(error)[:1024]}
    write_json(args.out / "receipt.json", result)
    print(json.dumps({"status": result["status"], "cases": len(result["cases"]), "receipt": str(args.out / "receipt.json")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
