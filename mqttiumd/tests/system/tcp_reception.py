#!/usr/bin/env python3
"""Finite system reception of the actual MQTT 5 incoming QoS 2 product.

This does not fabricate a fixture and is not an oracle qualifying campaign.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import sqlite3
import subprocess
import sys
import time

from tcp_probe import (
    Observations, ReceiptFailure, Wire, assert_no_positive_ack, assert_stale_filtered,
    connect_packet, database, operation_hex, packet, publish_packet, pubrel_packet,
    read_events, require, variable_integer, write_json,
)


class Broker:
    def __init__(self, binary: Path, case: Path, *, db: Path | None = None,
                 label: str = "broker", pauses: tuple[str, ...] = (), test_control: bool = False):
        case.mkdir(parents=True, exist_ok=True)
        self.binary = binary.resolve()
        self.case = case
        self.db = db or case / "store.sqlite"
        self.events_path = case / f"{label}.events.jsonl"
        self.control = case / f"{label}.control"
        self.control.mkdir()
        self.observations = Observations(case / f"{label}.observer.jsonl")
        command = [str(self.binary), "--db", str(self.db), "--listen", "127.0.0.1:0",
                   "--events", str(self.events_path)]
        if pauses or test_control:
            command += ["--test-control", str(self.control)]
        if pauses:
            command += ["--test-pause", ",".join(pauses)]
        self.stdout_path = case / f"{label}.stdout"
        self.stderr_path = case / f"{label}.stderr"
        self.stdout = self.stdout_path.open("wb")
        self.stderr = self.stderr_path.open("wb")
        self.process = subprocess.Popen(command, stdout=self.stdout, stderr=self.stderr)
        self.address = None
        self.cleaned = False
        self.observations.record("process_started", pid=self.process.pid, command=command,
                                 binary_sha256=hashlib.sha256(self.binary.read_bytes()).hexdigest())

    def events(self) -> list[dict]:
        events = read_events(self.events_path)
        require(all(event["pid"] == self.process.pid for event in events), "wrong_event_process")
        return events

    def wait_event(self, kind: str, *, point: str | None = None, timeout: float = 8,
                   after: int = -1) -> dict:
        def retained() -> dict | None:
            for event in self.events():
                if int(event["seq"]) > after and event["kind"] == kind and (point is None or event.get("point") == point):
                    self.observations.record("event_retained", event=event)
                    return event
            return None

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            event = retained()
            if event is not None:
                return event
            if self.process.poll() is not None:
                # The process can append its last fact and exit between the
                # previous scan and poll. After observed death, its completed
                # writes precede this final bounded scan. Preserve that fact;
                # only a genuinely absent event becomes a process-exit failure.
                event = retained()
                if event is not None:
                    return event
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
        self.cleaned = True
        require(status == (-9 if killed else 0), "unexpected_process_status", str(status))
        require(not Path(f"/proc/{self.process.pid}").exists(), "child_survived")
        return status

    def cleanup(self) -> None:
        if self.cleaned:
            return
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=3)
        self.observations.record("process_wait", pid=self.process.pid,
                                 status=self.process.returncode, requested="receipt_cleanup")
        self.cleaned = True
        self.stdout.close()
        self.stderr.close()


def paho(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    try:
        ready = broker.ready()
        receipts = []
        clients = (("mqttium", "external-mqttium", False, True),
                   ("mqttium", "external-mqttium", True, True),
                   ("paho", "external-paho", False, True),
                   ("paho", "external-paho", True, False))
        for index, (name, client_id, present, publishes) in enumerate(clients):
            command = [sys.executable, str(Path(__file__).with_name(f"external_{name}.py")),
                       "--host", broker.address[0], "--port", str(broker.address[1]),
                       "--client-id", client_id]
            if name == "mqttium":
                command += ["--qos", "2", "--expected-session-present", str(int(present))]
            if not publishes:
                command.append("--connect-only")
            result = subprocess.run(command, capture_output=True, text=True, timeout=15)
            (case / f"{name}-{index}.stderr").write_text(result.stderr)
            require(result.returncode == 0, "external_client_failed", result.stderr[-512:])
            receipt = json.loads(result.stdout)
            write_json(case / f"{name}-{index}.json", receipt)
            require(receipt["session_present"] == present, "wrong_session_present")
            if publishes:
                completed = any(event["kind"] == "qos2_completed" or
                                (event["kind"] == "receipt_success" and event.get("qos") == 2)
                                for event in receipt["events"])
                require(completed, "external_not_completed")
            receipts.append({"command": command, "result": receipt})
        state = database(broker.db)
        write_json(case / "durable.json", state)
        require(state["counts"]["sessions"] == 2, "session_not_created_once")
        require(len(state["admissions"]) == 3 and len(state["releases"]) == 3, "exchange_not_committed")
        require(all(state["counts"][name] == 0 for name in ("inbound", "publications", "payloads", "obligations")),
                "publication_retained_after_normal_cycle")
        established = [event for event in broker.events() if event["kind"] == "session_established"]
        require(len(established) == 4 and [event["details"]["session_present"] for event in established] == [False, True, False, True],
                "session_establishment_observation")
        acknowledgements = [event for event in broker.events() if event["kind"] == "frame_queued"
                            and event.get("details", {}).get("packet_kind") == "CONNACK"]
        require(len(acknowledgements) == 4 and all(int(e["seq"]) < int(a["seq"])
                for e, a in zip(established, acknowledgements)), "connack_before_session_confirmation")
        broker.stop()
        return {"criterion": "A", "status": "PASS", "ready": ready,
                "external_clients": receipts, "durable": state}
    finally:
        broker.cleanup()


def writer_available(path: Path) -> None:
    with Path(str(path) + ".writer-lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(lock, fcntl.LOCK_UN)


def expect_rejection(wire: Wire, *, before_connect: bool = False, timeout: float = 2) -> None:
    try:
        frame = wire.poll(timeout)
    except ReceiptFailure as error:
        require(error.code in ("peer_closed", "probe_truncated"), "unexpected_rejection_failure", error.code)
        return
    require(frame is not None, "invalid_frame_not_rejected")
    if frame.kind == 0x20:
        require(before_connect and len(frame.body) >= 2 and frame.body[1] >= 128,
                "invalid_input_succeeded")
    else:
        require(frame.kind == 0xE0 and frame.body and frame.body[0] >= 128,
                "invalid_input_succeeded", hex(frame.header))


def committed_admission_ids(events: list[dict]) -> frozenset[str]:
    """Known commit identities survive retirement of their detailed SQL rows."""
    return frozenset(operation_hex(event["operation"]) for event in events
                     if event.get("kind") == "store_result"
                     and event.get("command_kind") == "Admission"
                     and event.get("details", {}).get("committed_now") is True
                     and event["details"].get("outcome") == "Committed")


def framing(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    try:
        broker.ready()
        with Wire(broker.address, broker.observations, "fragmented") as wire:
            wire.connect("fragmented", session_present=False, stride=1)
            wire.exchange(7, stride=1, concat=True)
        # Every split of these independently encoded small PUBLISH packets,
        # bounded at 64 trials, with a normal completed cycle between uses.
        message = publish_packet("reception/splits", b"split", 19)
        splits = list(range(1, min(len(message), 65)))
        with Wire(broker.address, broker.observations, "splits") as wire:
            wire.connect("split-client", session_present=False)
            for split in splits:
                wire.send(message[:split])
                wire.send(message[split:])
                wire.expect(0x50, 19)
                wire.send(pubrel_packet(19) + b"\xc0\x00")
                wire.expect_batch([(0x70, 19), (0xD0, None)])
        # A valid MQTT feature outside the narrow profile must not be confused
        # with a syntactically malformed fixed header.
        start = len(broker.events())
        with Wire(broker.address, broker.observations, "unsupported-response-information") as wire:
            wire.send(connect_packet("unsupported", properties=b"\x19\x01"))
            expect_rejection(wire, before_connect=True)
        unsupported = broker.wait_event("packet_refused", after=start - 1)
        require(unsupported["details"]["code"] == "ConnectOutsideProfile",
                "unsupported_connect_wrong_cause")
        start = len(broker.events())
        with Wire(broker.address, broker.observations, "malformed-connect") as wire:
            wire.send(connect_packet("malformed", flags=1))
            expect_rejection(wire, before_connect=True)
        malformed = broker.wait_event("packet_refused", after=start - 1)
        require(malformed["details"]["code"] == "ReservedConnectFlag",
                "malformed_connect_wrong_cause")
        require(unsupported["details"]["code"] != malformed["details"]["code"],
                "unsupported_not_distinguished_from_malformed")
        # A declared excessive size is refused without waiting for the body.
        with Wire(broker.address, broker.observations, "oversized") as wire:
            wire.connect("invalid-client", session_present=False)
            before = committed_admission_ids(broker.events())
            wire.send(b"\x34" + variable_integer(65537))
            expect_rejection(wire, timeout=1)
            require(committed_admission_ids(broker.events()) == before, "oversized_admitted")
        # Clean EOF after a partial body is a truncated packet, not clean EOF.
        after = max(int(event["seq"]) for event in broker.events())
        with Wire(broker.address, broker.observations, "truncated") as wire:
            wire.connect("invalid-client", session_present=True)
            wire.send(b"\x34\x20\x00")
            wire.socket.shutdown(socket.SHUT_WR)
            expect_rejection(wire)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not any(event["kind"] == "connection_closed"
                and int(event["seq"]) > after and event.get("details", {}).get("code") == "truncated_packet"
                for event in broker.events()):
            time.sleep(.005)
        require(any(event["kind"] == "connection_closed" and int(event["seq"]) > after
                    and event.get("details", {}).get("code") == "truncated_packet"
                    for event in broker.events()), "truncation_not_identified")
        # Guaranteed malformed fixed flags, on real freshly resumed sockets.
        # This is a finite structured exploration, not arbitrary byte fuzzing.
        invalid_trials = 32
        for index in range(invalid_trials):
            with Wire(broker.address, broker.observations, f"invalid-{index}") as wire:
                wire.connect("invalid-client", session_present=True)
                wire.send(bytes([0xC1 + (index % 15), 0]))
                expect_rejection(wire)
        # Keep Alive applies to complete control packets; PING responses keep
        # a short-lived test connection active, then idle expiry closes it.
        with Wire(broker.address, broker.observations, "keepalive") as wire:
            wire.connect("keepalive", keep_alive=1, session_present=False)
            for _ in range(4):
                time.sleep(.4)
                wire.send(b"\xc0\x00")
                wire.expect(0xD0)
            expect_rejection(wire, timeout=2.5)
        broker.stop()
        writer_available(broker.db)
        return {"criterion": "B", "status": "PASS", "one_byte_fragmentation": True,
                "concatenated_packets": True, "split_trials": len(splits), "invalid_trials": invalid_trials,
                "unsupported_code": unsupported["details"]["code"], "malformed_code": malformed["details"]["code"],
                "keepalive_ping_and_expiry": True}
    finally:
        broker.cleanup()


def held_frames(wire: Wire, header: int, identifier: int) -> list:
    frames = []
    deadline = time.monotonic() + .15
    while time.monotonic() < deadline:
        frame = wire.poll(deadline - time.monotonic())
        if frame is None:
            break
        frames.append(frame)
    assert_no_positive_ack(frames, header, identifier)
    require(not frames, "unexpected_frame_while_held")
    return frames


def barriers(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("admission.prepared", "release.prepared"))
    try:
        broker.ready()
        with Wire(broker.address, broker.observations, "barriers") as wire:
            wire.connect("barrier-client", session_present=False)
            before = database(broker.db)
            wire.send(publish_packet("reception/barrier", b"held", 7))
            admission = broker.wait_event("barrier_hit", point="admission.prepared")
            require(admission["command_kind"] == "Admission", "wrong_barrier_command")
            state = database(broker.db)
            write_json(case / "before-admission-commit.json", state)
            require(state["admissions"] == before["admissions"], "admission_present_before_commit")
            held_frames(wire, 0x50, 7)
            broker.release("admission.prepared")
            wire.expect(0x50, 7)
            after_admit = database(broker.db)
            require(len(after_admit["admissions"]) == len(before["admissions"]) + 1, "admission_not_durable")
            require(any(row["id"] == operation_hex(admission["operation"]) for row in after_admit["admissions"]),
                    "wrong_durable_admission_identity")
            wire.send(pubrel_packet(7))
            release = broker.wait_event("barrier_hit", point="release.prepared")
            require(release["command_kind"] == "Release", "wrong_barrier_command")
            state = database(broker.db)
            write_json(case / "before-release-commit.json", state)
            require(state["counts"]["inbound"] == 1 and not state["releases"], "release_present_before_commit")
            held_frames(wire, 0x70, 7)
            broker.release("release.prepared")
            wire.expect(0x70, 7)
            after = database(broker.db)
            write_json(case / "after-release-commit.json", after)
            require(len(after["releases"]) == 1 and after["counts"]["inbound"] == 0, "release_not_durable")
        broker.stop()
        return {"criterion": "C", "status": "PASS", "admission_hit": admission,
                "release_hit": release, "precommit_ack_absent": True}
    finally:
        broker.cleanup()


def recovery(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("admission.committed",))
    second = None
    wire = None
    try:
        broker.ready()
        wire = Wire(broker.address, broker.observations, "before-kill")
        wire.connect("recover-client", session_present=False)
        wire.send(publish_packet("reception/recovery", b"durable-before-pubrec", 23))
        hit = broker.wait_event("barrier_hit", point="admission.committed")
        require(hit["command_kind"] == "Admission", "wrong_barrier_command")
        held_frames(wire, 0x50, 23)
        before = database(broker.db)
        write_json(case / "before-kill.json", before)
        require(len(before["admissions"]) == 1 and before["counts"]["inbound"] == 1, "admission_not_durable_at_kill")
        require(before["admissions"][0]["id"] == operation_hex(hit["operation"]), "wrong_durable_admission_identity")
        inode = broker.db.stat().st_ino
        status = broker.stop(killed=True)
        wire.close()
        second = Broker(binary, case, db=broker.db, label="restarted")
        second.ready()
        require(broker.db.stat().st_ino == inode, "database_replaced_on_restart")
        with Wire(second.address, second.observations, "after-restart") as resumed:
            resumed.connect("recover-client", session_present=True)
            resumed.send(publish_packet("reception/recovery", b"durable-before-pubrec", 23, dup=True))
            resumed.expect(0x50, 23)
            require(database(broker.db)["admissions"] == before["admissions"], "second_admission_after_retransmission")
            resumed.send(pubrel_packet(23))
            resumed.expect(0x70, 23)
        after = database(broker.db)
        write_json(case / "after-recovery.json", after)
        require(len(after["admissions"]) == 1 and len(after["releases"]) == 1 and after["counts"]["inbound"] == 0,
                "recovered_exchange_not_completed")
        second.stop()
        writer_available(broker.db)
        return {"criterion": "D", "status": "PASS", "kill_status": status, "point": hit,
                "same_file_inode": inode, "admissions": 1, "session_present": True}
    finally:
        if wire:
            wire.close()
        broker.cleanup()
        if second:
            second.cleanup()


def reuse(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    try:
        broker.ready()
        with Wire(broker.address, broker.observations, "reuse") as wire:
            wire.connect("reuse-client", session_present=False)
            wire.exchange(7, b"first")
            first = database(broker.db)
            wire.exchange(7, b"second")
            second = database(broker.db)
        require(len(first["admissions"]) == 1 and len(second["admissions"]) == 2, "packet_identifier_permanently_deduplicated")
        require(len({row["publication"] for row in second["admissions"]}) == 2, "publication_identity_reused")
        require(len(second["releases"]) == 2 and second["counts"]["inbound"] == 0, "reuse_not_completed")
        write_json(case / "first.json", first)
        write_json(case / "second.json", second)
        broker.stop()
        return {"criterion": "E", "status": "PASS", "packet_identifier": 7, "distinct_publications": 2}
    finally:
        broker.cleanup()


def invalid_startup(binary: Path, case: Path) -> dict:
    source = Broker(binary, case / "source")
    try:
        source.ready()
        with Wire(source.address, source.observations, "source-session") as wire:
            wire.connect("corruption-source", session_present=False)
            wire.exchange(5)
        source.stop()
        with sqlite3.connect(source.db) as connection:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        results = []
        for label, sql, expected in (
                ("invalid-data", "UPDATE sessions SET state=99", "InvalidData"),
                ("field-limit", "UPDATE sessions SET client_id=zeroblob(65536)", "LimitExceeded")):
            output = case / label
            output.mkdir()
            db = output / "store.sqlite"
            shutil.copyfile(source.db, db)
            with sqlite3.connect(db) as connection:
                changed = connection.execute(sql).rowcount
                connection.commit()
                require(changed == 1 and connection.execute("PRAGMA quick_check").fetchone()[0] == "ok", "invalid_image_construction")
            before = database(db)
            process = Broker(binary, output, db=db)
            try:
                status = process.process.wait(timeout=8)
                events = process.events()
                require(status != 0, "invalid_startup_succeeded")
                require(not any(event["kind"] == "ready" for event in events), "invalid_store_became_ready")
                failures = [event for event in events if event["kind"] == "startup_or_runtime_refused"]
                require(len(failures) == 1 and failures[0].get("cause") == expected, "startup_cause_lost")
                stopped = [event for event in events if event["kind"] == "stopped"]
                require(stopped and stopped[-1]["details"]["writer_released"], "startup_writer_not_released")
                writer_available(db)
                after = database(db)
                for table in before["tables"]:
                    if table != "meta":
                        require(after["tables"][table] == before["tables"][table], "invalid_startup_changed_business_state", table)
                meta_before = dict(before["tables"]["meta"][0]);meta_after = dict(after["tables"]["meta"][0])
                opened = after["epoch"] - before["epoch"]
                require(opened in (0, 1) and after["revision"] - before["revision"] == opened,
                        "invalid_startup_reset_or_retried_epoch")
                for column in ("last_opened", "opened_by", "revision"):
                    meta_before.pop(column);meta_after.pop(column)
                require(meta_before == meta_after, "invalid_startup_changed_metadata")
                write_json(output / "before.json", before);write_json(output / "after.json", after)
                results.append({"case": label, "quick_check": "ok", "cause": expected,
                                "phase": failures[0].get("phase"), "exit_code": status,
                                "legitimate_epoch_openings": opened, "writer_released": True})
            finally:
                process.cleanup()
        return {"criterion": "F", "status": "PASS", "cases": results}
    finally:
        source.cleanup()


def generations(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, test_control=True)
    sockets = []
    try:
        ready = broker.ready()
        window = ready["details"].get("remembered_connections", 8)
        takeovers = max(18, window + 2)
        for index in range(takeovers):
            wire = Wire(broker.address, broker.observations, f"takeover-{index}")
            sockets.append(wire)
            wire.connect("generation-client", session_present=index != 0)
            if index:
                sockets[index - 1].close()
        captured = broker.wait_event("input_captured")
        old = captured["connection"]
        before = max(int(event["seq"]) for event in broker.events())
        (broker.control / "replay-captured").touch(exist_ok=False)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            events = [event for event in broker.events() if int(event["seq"]) > before]
            if any(event["kind"] in ("stale_generation_ignored", "stale_generation_accepted") for event in events):
                break
            time.sleep(.005)
        assert_stale_filtered(events, old)
        sockets[-1].exchange(37, b"current-generation")
        broker.stop()
        return {"criterion": "G-generation", "status": "PASS", "takeovers": takeovers,
                "core_window": window, "old_generation": old, "stale_event_ignored": True}
    finally:
        for wire in sockets:
            wire.close()
        broker.cleanup()


def concurrency(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    try:
        broker.ready()
        def client(name, identifier):
            with Wire(broker.address, broker.observations, name) as wire:
                wire.connect(name, session_present=False)
                wire.exchange(identifier)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = [pool.submit(client, "parallel-a", 7), pool.submit(client, "parallel-b", 8)]
            for result in results:
                result.result(timeout=10)
        with Wire(broker.address, broker.observations, "partial-client") as partial:
            partial.connect("partial-client", session_present=False)
            partial.send(b"\x34\x64\x00")
            client("unblocked-client", 9)
            # No new bytes or complete packet can reset the absolute partial
            # packet deadline. A later step checks its explicit close cause.
            deadline = time.monotonic() + 7
            while time.monotonic() < deadline and not any(event["kind"] == "connection_closed"
                    and event.get("details", {}).get("code") == "partial_packet_timeout" for event in broker.events()):
                time.sleep(.01)
            require(any(event["kind"] == "connection_closed" and event.get("details", {}).get("code") == "partial_packet_timeout"
                        for event in broker.events()), "partial_reader_not_bounded")
        # A real nonreader fills the product's actual four-slot egress queue.
        # Kernel buffer values and the reached pressure point remain recorded.
        with Wire(broker.address, broker.observations, "nonreader", receive_buffer=1024) as stalled:
            stalled.connect("nonreader", session_present=False)
            baseline = database(broker.db)
            before = max(int(event["seq"]) for event in broker.events())
            stalled.send(publish_packet("reception/pressure", b"committed-unread", 55))
            admission = broker.wait_event("store_result", after=before)
            require(admission.get("command_kind") == "Admission" and admission["details"]["committed_now"],
                    "nonreader_admission_not_committed")
            admitted = database(broker.db)
            target = [row for row in admitted["admissions"] if row["id"] == operation_hex(admission["operation"])]
            require(len(target) == 1 and len(admitted["admissions"]) == len(baseline["admissions"]) + 1,
                    "nonreader_durable_admission_identity")
            queued = broker.wait_event("frame_queued", after=before)
            require(queued["details"].get("packet_kind") == "PUBREC"
                    and queued["details"].get("packet_id") == 55, "nonreader_pubrec_not_queued")
            write_json(case / "nonreader-admission.json", admitted)
            # The socket has not read since CONNACK: even its committed PUBREC
            # remains unknown to the client while PING pressure is introduced.
            before = max(int(event["seq"]) for event in broker.events())
            stalled.socket.settimeout(.1)
            sent = 0
            maximum = 128 * 1024
            started = time.monotonic()
            deadline = started + 12
            upload = b"\xc0\x00" * (maximum // 2)
            generation = queued["connection"]
            broker.observations.record("nonreader_pressure_started", byte_budget=maximum,
                                       deadline_ms=12000, deadline_monotonic_ns=str(int(deadline * 1_000_000_000)),
                                       target="output_queue_full", connection=generation)
            retained = []
            send_error = None
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                healthy = pool.submit(client, "after-pressure-client", 10)
                while time.monotonic() < deadline:
                    events = [event for event in broker.events() if int(event["seq"]) > before
                              and event.get("connection") == generation]
                    retained = [event for event in events if event["kind"] == "effect_retained"
                                and event.get("details", {}).get("code") == "output_queue_full"]
                    closed = [event for event in events if event["kind"] == "connection_closed"]
                    if retained or closed:
                        break
                    if sent < maximum and send_error is None:
                        try:
                            # Track actual partial sends, preserving the exact
                            # stream offset. A queued upload is not evidence
                            # that its MQTT packets have reached the core.
                            count = stalled.socket.send(upload[sent:min(sent + 256, maximum)])
                            if count == 0:
                                send_error = "zero_write"
                            sent += count
                        except (TimeoutError, BlockingIOError):
                            pass
                        except OSError as error:
                            send_error = type(error).__name__
                    # Continue observing after reaching the byte cap, bounded
                    # by the same deadline, until the actual pressure event.
                    time.sleep(.002)
                healthy.result(timeout=10)
            broker.observations.record("nonreader_flood", bytes_sent=sent, byte_budget=maximum,
                                       deadline_ms=12000, elapsed_ms=int((time.monotonic() - started) * 1000),
                                       send_error=send_error or "none", output_queue_full=bool(retained))
            require(not any(event["kind"] == "connection_closed"
                            and event.get("details", {}).get("code") == "input_queue_full" for event in events),
                    "input_saturation_instead_of_output")
            require(retained, "output_saturation_not_reached")
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                events = [event for event in broker.events() if int(event["seq"]) > before
                          and event.get("connection") == generation]
                retained = [event for event in events if event["kind"] == "effect_retained"
                            and event.get("details", {}).get("code") == "output_queue_full"]
                if retained and any(event["kind"] == "frame_failed" and event.get("details", {}).get("effect")
                                   in {r["details"]["effect"] for r in retained} for event in events):
                    break
                time.sleep(.01)
            require(retained, "output_saturation_not_reached")
            retained_ids = {event["details"]["effect"] for event in retained}
            terminal = {event.get("details", {}).get("effect") for event in events
                        if event["kind"] in ("frame_failed", "frame_written")}
            require(retained_ids <= terminal, "retained_effect_lost", repr(retained_ids - terminal))
            require(any(event["kind"] == "connection_closed" and event.get("details", {}).get("code")
                        in ("outgoing_queue_full", "write_timeout", "write_io") for event in events),
                    "nonreader_not_closed_within_budget")
            pressure = database(broker.db)
            require(target[0] in pressure["admissions"], "nonreader_committed_admission_lost")
            write_json(case / "after-pressure.json", pressure)
        with Wire(broker.address, broker.observations, "nonreader-resumed") as resumed:
            resumed.connect("nonreader", session_present=True)
            resumed.send(publish_packet("reception/pressure", b"committed-unread", 55, dup=True))
            resumed.expect(0x50, 55)
            require(database(broker.db)["admissions"] == pressure["admissions"],
                    "pressure_retransmission_readmitted")
            resumed.send(pubrel_packet(55));resumed.expect(0x70, 55)
        final = database(broker.db)
        require(final["admissions"] == pressure["admissions"] and final["counts"]["inbound"] == 0,
                "pressure_exchange_not_completed")
        write_json(case / "after-pressure-recovery.json", final)
        broker.stop()
        return {"criterion": "G-concurrency", "status": "PASS", "two_clients_completed": True,
                "partial_reader_timeout": True, "output_queue_saturation_reached": True,
                "retained_effects_terminal": len(retained_ids), "flood_bytes": sent, "flood_budget": maximum,
                "pressure_deadline_ms": 12000, "receive_buffer_requested": 1024,
                "unread_pubrec_operation": admission["operation"], "unread_pubrec_packet_identifier": 55,
                "pressure_retransmission_admissions": 0, "pressure_exchange_completed": True}
    finally:
        broker.cleanup()


def shutdown(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("admission.prepared",))
    resumed = None
    try:
        broker.ready()
        with Wire(broker.address, broker.observations, "shutdown") as wire:
            wire.connect("shutdown-client", session_present=False)
            wire.send(publish_packet("reception/shutdown", b"accepted-before-stop", 41))
            hit = broker.wait_event("barrier_hit", point="admission.prepared")
            submitted = [event for event in broker.events() if event["kind"] == "command_submitted"]
            require(submitted, "no_submitted_command_at_stop")
            held_frames(wire, 0x50, 41)
            broker.stop()
        events = broker.events()
        stopped = [event for event in events if event["kind"] == "stopped"]
        require(stopped and stopped[-1]["details"]["writer_released"], "shutdown_writer_held")
        require(stopped[-1]["details"].get("accepted_commands_certainly_rejected") is False,
                "submitted_command_called_certainly_rejected")
        writer_available(broker.db)
        after = database(broker.db);write_json(case / "after-stop.json", after)
        resumed = Broker(binary, case, db=broker.db, label="reopened")
        resumed.ready()
        with Wire(resumed.address, resumed.observations, "after-stop") as wire:
            wire.connect("shutdown-client", session_present=True)
            wire.send(publish_packet("reception/shutdown", b"accepted-before-stop", 41, dup=True))
            wire.expect(0x50, 41)
            wire.send(pubrel_packet(41));wire.expect(0x70, 41)
        require(len(database(broker.db)["admissions"]) == 1, "shutdown_reconciliation_readmitted")
        resumed.stop()
        return {"criterion": "H", "status": "PASS", "held_operation": hit["operation"],
                "accepted_commands_certainly_rejected": False, "writer_reopened": True}
    finally:
        broker.cleanup()
        if resumed:
            resumed.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--mode", choices=("basic", "normal", "complete"), default="basic")
    parser.add_argument("--case", choices=("A", "B", "C", "D", "E", "F", "G-concurrency", "G-generation", "H"))
    args = parser.parse_args()
    require(args.binary.is_file(), "binary_missing", str(args.binary))
    args.out.mkdir(parents=True, exist_ok=True)
    require(not any(args.out.iterdir()), "output_not_empty")
    result = {"format": "mqttiumd.tcp-system-reception", "schema_version": 1,
              "qualification": "system-test-only", "scope": "MQTT 5 durable incoming QoS 2; no subscriber delivery or complete broker claim",
              "binary": {"path": str(args.binary.resolve()), "sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()},
              "mode": args.mode, "cases": []}
    try:
        cases = [("A", paho)]
        if args.mode == "normal" and args.case is None:
            cases += [("B", framing), ("E", reuse), ("F", invalid_startup),
                      ("G-concurrency", concurrency)]
        if args.mode == "complete" or args.case:
            cases += [("B", framing), ("C", barriers), ("D", recovery), ("E", reuse),
                      ("F", invalid_startup), ("G-concurrency", concurrency),
                      ("G-generation", generations), ("H", shutdown)]
        for name, function in cases:
            if args.case is None or args.case == name:
                value = function(args.binary, args.out / name)
                result["cases"].append(value)
                print(f"{name}: {value['status']}", flush=True)
        result["status"] = "PASS"
    except Exception as error:
        result["status"] = "FAIL"
        result["failure"] = {"code": getattr(error, "code", type(error).__name__), "detail": str(error)[:1024]}
    write_json(args.out / "receipt.json", result)
    print(json.dumps({"status": result["status"], "cases": len(result["cases"]), "receipt": str(args.out / "receipt.json")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
