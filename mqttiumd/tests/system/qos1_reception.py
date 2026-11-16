#!/usr/bin/env python3
"""Finite incoming QoS 1 product tests, distinct from qualifying lab campaigns.

Expected restart semantics are fixed here before running: an internal command
keeps its OperationId and executes once; a MQTT retransmission after losing a
PUBACK may be admitted again. No durable QoS 2 marker is invented for QoS 1.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import select
import socket
import sqlite3
import subprocess
import sys
import threading
import time

from tcp_probe import (
    Frame, FrameDecoder, Observations, ReceiptFailure, Wire, assert_no_positive_ack,
    database, operation_hex, packet, publish_packet, pubrel_packet, require, write_json,
)
from tcp_reception import Broker, writer_available


def finished_state(broker: Broker, case: Path, *, admissions: int, releases: int) -> dict:
    state = database(broker.db)
    write_json(case / "durable.json", state)
    require(len(state["admissions"]) == admissions, "wrong_admission_count")
    require(len(state["releases"]) == releases, "wrong_release_count")
    require(all(state["counts"][name] == 0 for name in
                ("inbound", "publications", "payloads", "obligations")),
            "publication_retained_after_normal_cycle")
    return state


def external(broker: Broker, case: Path, name: str, client_id: str, qoses: tuple[int, ...],
             present: bool, *, address: tuple[str, int] | None = None) -> dict:
    host, port = address or broker.address
    command = [sys.executable, str(Path(__file__).with_name(f"external_{name}.py")),
               "--host", host, "--port", str(port), "--client-id", client_id]
    if name == "mqttium":
        command += ["--expected-session-present", str(int(present))]
    for qos in qoses:
        command += ["--qos", str(qos)]
    if not qoses:
        command.append("--connect-only")
    outcome = subprocess.run(command, capture_output=True, text=True, timeout=25)
    index = len(list(case.glob("client-*.json")))
    (case / f"client-{index}-{name}.stderr").write_text(outcome.stderr)
    require(outcome.returncode == 0, "external_client_failed", outcome.stderr[-512:])
    receipt = json.loads(outcome.stdout)
    write_json(case / f"client-{index}-{name}.json", receipt)
    require(receipt["session_present"] == present, "wrong_session_present")
    return {"command": command, "result": receipt}


def nominal(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    second = None
    try:
        broker.ready()
        clients = [external(broker, case, "mqttium", "qos1-mqttium", (1, 1), False),
                   external(broker, case, "mqttium", "qos1-mqttium", (1, 2), True),
                   external(broker, case, "paho", "qos1-paho", (1,), False),
                   external(broker, case, "paho", "qos1-paho", (), True)]
        before = finished_state(broker, case, admissions=5, releases=1)
        broker.stop()
        second = Broker(binary, case, db=broker.db, label="restarted")
        second.ready()
        clients += [external(second, case, "mqttium", "qos1-mqttium", (1, 2), True),
                    external(second, case, "paho", "qos1-paho", (1,), True)]
        after = finished_state(second, case, admissions=8, releases=2)
        require(after["counts"]["sessions"] == 2, "session_not_reused")
        second.stop()
        writer_available(broker.db)
        return {"criterion": "A", "status": "PASS", "clients": clients,
                "admissions_before_restart": len(before["admissions"]), "admissions": 8,
                "qos2_releases": 2, "writer_released": True}
    finally:
        broker.cleanup()
        if second:
            second.cleanup()


def reuse(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    try:
        broker.ready()
        observations = []
        with Wire(broker.address, broker.observations, "identifiers") as wire:
            wire.connect("qos1-identifiers", session_present=False)
            # MQTT-4.3.2-5 applies even when content and DUP are identical.
            for qos, dup, body in ((1, False, b"same"), (1, False, b"same"),
                                   (1, True, b"same"), (2, False, b"qos2"),
                                   (1, True, b"after-qos2"), (2, False, b"qos2-again")):
                wire.exchange(7, body, qos=qos, dup=dup)
                state = database(broker.db)
                observations.append({"qos": qos, "dup": dup, "packet_identifier": 7,
                                     "admissions": len(state["admissions"]),
                                     "publication": state["admissions"][-1]["publication"]})
            # Complete frames may be fragmented and coalesced with PINGREQ.
            wire.send(publish_packet("reception/qos1", b"fragmented", 8, qos=1), stride=1)
            wire.expect(0x40, 8)
            wire.send(publish_packet("reception/qos1", b"concat", 8, qos=1) + b"\xc0\x00")
            wire.expect_batch([(0x40, 8), (0xD0, None)])
        require([v["admissions"] for v in observations] == list(range(1, 7)),
                "packet_identifier_permanently_deduplicated")
        state = finished_state(broker, case, admissions=8, releases=2)
        require(len({row["publication"] for row in state["admissions"]}) == 8,
                "publication_identity_reused")
        broker.stop()
        return {"criterion": "C-reuse", "status": "PASS", "occurrences": observations,
                "admissions": 8, "qos2_releases": 2}
    finally:
        broker.cleanup()


class Proxy:
    """Small packet-aware independent witness; never uses either client codec.

    It can discard exactly one complete server PUBACK before forwarding any of
    its bytes. The retained frame proves server emission, not client receipt.
    """
    def __init__(self, upstream: tuple[str, int], observations: Observations, *, drop_ack: bool = False):
        self.upstream = upstream
        self.observations = observations
        self.drop_ack = drop_ack
        self.ack_dropped = threading.Event()
        self.reconnect_allowed = threading.Event()
        self.cancel = threading.Event()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(4)
        self.listener.settimeout(.05)
        self.address = self.listener.getsockname()
        self.sockets = []
        self.workers = []
        self.frames: list[Frame] = []
        self.lock = threading.Lock()
        self.errors = []
        self.thread = threading.Thread(target=self.accept, name="qos1-proxy", daemon=True)
        self.thread.start()

    def accept(self):
        for index in range(32):
            while not self.cancel.is_set():
                try:
                    client, _ = self.listener.accept()
                    break
                except socket.timeout:
                    continue
                except OSError:
                    return
            else:
                return
            if self.ack_dropped.is_set():
                deadline = time.monotonic() + 10
                while not self.cancel.is_set() and not self.reconnect_allowed.wait(.05):
                    if time.monotonic() >= deadline:
                        client.close()
                        self.errors.append("proxy_restart_deadline")
                        return
            try:
                upstream = socket.create_connection(self.upstream, timeout=1)
            except OSError:
                client.close()
                continue
            self.sockets.extend((client, upstream))
            worker = threading.Thread(target=self.forward, args=(client, upstream, index), daemon=True)
            self.workers.append(worker)
            worker.start()
        self.errors.append("proxy_connection_budget")

    def forward(self, client: socket.socket, upstream: socket.socket, index: int):
        decoder = FrameDecoder()
        try:
            client.settimeout(.25)
            upstream.settimeout(.25)
            deadline = time.monotonic() + 20
            while not self.cancel.is_set() and time.monotonic() < deadline:
                readable, _, _ = select.select([client, upstream], [], [], .05)
                for source in readable:
                    data = source.recv(4096)
                    if not data:
                        return
                    if source is client:
                        self.observations.record_bytes("proxy_client_tx", data, connection=index)
                        upstream.sendall(data)
                    else:
                        for frame in decoder.feed(data):
                            with self.lock:
                                require(len(self.frames) < 128, "proxy_frame_budget")
                                self.frames.append(frame)
                            self.observations.record_bytes("proxy_server_frame", packet(frame.header, frame.body),
                                                           connection=index, packet_kind=frame.kind)
                            if self.drop_ack and frame.kind == 0x40:
                                self.drop_ack = False
                                self.observations.record("proxy_puback_discarded", packet_id=frame.ack()[0],
                                                         forwarded=False, connection=index)
                                self.ack_dropped.set()
                                return
                            # Re-encode only the independently decoded framing.
                            client.sendall(packet(frame.header, frame.body))
        except (OSError, ReceiptFailure) as error:
            self.observations.record("proxy_transport_closed", connection=index,
                                     code=getattr(error, "code", type(error).__name__))
        finally:
            for peer in (client, upstream):
                try:
                    peer.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                peer.close()

    def close(self):
        self.cancel.set()
        self.listener.close()
        for peer in self.sockets:
            try:
                peer.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            peer.close()
        self.thread.join(timeout=1)
        for worker in self.workers:
            worker.join(timeout=1)
        require(not self.thread.is_alive() and not any(w.is_alive() for w in self.workers),
                "proxy_survived")
        require(not self.errors, "proxy_failed", repr(self.errors))


def mqttium_process(address: tuple[str, int], case: Path, *, reconnect: bool = False,
                    client_id: str = "qos1-held-mqttium") -> tuple[subprocess.Popen, list[str]]:
    command = [sys.executable, str(Path(__file__).with_name("external_mqttium.py")),
               "--host", address[0], "--port", str(address[1]), "--client-id", client_id,
               "--qos", "1", "--events", str(case / "mqttium.events.jsonl"),
               "--wait-timeout", "15"]
    if reconnect:
        command.append("--reconnect")
    process = subprocess.Popen(command, stdout=(case / "mqttium.stdout").open("w"),
                               stderr=(case / "mqttium.stderr").open("w"))
    return process, command


def collect_client(process: subprocess.Popen, case: Path) -> dict:
    status = process.wait(timeout=20)
    require(status == 0, "external_client_failed", (case / "mqttium.stderr").read_text()[-512:])
    value = json.loads((case / "mqttium.stdout").read_text())
    write_json(case / "mqttium.json", value)
    return value


def stop_client(process: subprocess.Popen | None):
    if process and process.poll() is None:
        process.kill()
        process.wait(timeout=3)


def precommit(binary: Path, case: Path) -> dict:
    # First a raw witness: the premature-ACK mutant must fail for precisely this
    # semantic reason, not because a pilot does not recognize PUBACK.
    broker = Broker(binary, case / "raw", pauses=("admission.prepared",))
    try:
        broker.ready()
        with Wire(broker.address, broker.observations, "held") as wire:
            wire.connect("qos1-precommit", session_present=False)
            before = database(broker.db)
            wire.send(publish_packet("reception/held", b"held-before-commit", 7, qos=1))
            hit = broker.wait_event("barrier_hit", point="admission.prepared")
            held = database(broker.db)
            write_json(case / "before-commit.json", held)
            require(held["admissions"] == before["admissions"], "admission_present_before_commit")
            frames = []
            frame = wire.poll(.2)
            if frame:
                frames.append(frame)
            assert_no_positive_ack(frames, 0x40, 7)
            require(not frames, "unexpected_frame_while_held")
            broker.release("admission.prepared")
            wire.expect(0x40, 7)
            after = database(broker.db)
            require(len(after["admissions"]) == 1 and
                    after["admissions"][0]["id"] == operation_hex(hit["operation"]),
                    "wrong_durable_admission_identity")
        broker.stop()
    finally:
        broker.cleanup()
    live = Broker(binary, case / "mqttium", pauses=("admission.prepared",))
    proxy = None
    client = None
    try:
        live.ready()
        proxy = Proxy(live.address, live.observations)
        client, command = mqttium_process(proxy.address, case / "mqttium")
        live_hit = live.wait_event("barrier_hit", point="admission.prepared")
        state = database(live.db)
        require(not state["admissions"], "admission_present_before_commit")
        time.sleep(.15)
        assert_no_positive_ack(proxy.frames.copy(), 0x40, 1)
        require(client.poll() is None, "mqttium_completed_before_commit")
        live.release("admission.prepared")
        receipt = collect_client(client, case / "mqttium")
        after = database(live.db)
        require(len(after["admissions"]) == 1 and
                after["admissions"][0]["id"] == operation_hex(live_hit["operation"]),
                "wrong_durable_admission_identity")
        live.stop()
        return {"criterion": "B", "status": "PASS", "raw_hit": hit,
                "mqttium_hit": live_hit, "command": command, "client": receipt,
                "precommit_admissions": 0, "precommit_positive_puback": False}
    finally:
        stop_client(client)
        if proxy:
            proxy.close()
        live.cleanup()


def restart_before_ack(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("admission.committed",))
    second = None
    try:
        broker.ready()
        with Wire(broker.address, broker.observations, "before") as wire:
            wire.connect("qos1-restart", session_present=False)
            wire.send(publish_packet("reception/restart", b"possible-duplicate", 23, qos=1))
            hit = broker.wait_event("barrier_hit", point="admission.committed")
            assert_no_positive_ack([f for f in [wire.poll(.1)] if f], 0x40, 23)
            before = database(broker.db)
            write_json(case / "before-kill.json", before)
            require(len(before["admissions"]) == 1 and not before["counts"]["inbound"],
                    "qos1_admission_not_durable_at_kill")
            require(before["admissions"][0]["id"] == operation_hex(hit["operation"]),
                    "wrong_durable_admission_identity")
            inode = broker.db.stat().st_ino
            status = broker.stop(killed=True)
        second = Broker(binary, case, db=broker.db, label="restarted")
        second.ready()
        require(broker.db.stat().st_ino == inode, "database_replaced_on_restart")
        with Wire(second.address, second.observations, "after") as wire:
            wire.connect("qos1-restart", session_present=True)
            wire.exchange(23, b"possible-duplicate", qos=1, dup=True)
        after = finished_state(second, case, admissions=2, releases=0)
        require({row["id"] for row in before["admissions"]} <= {row["id"] for row in after["admissions"]},
                "committed_admission_lost")
        second.stop()
        writer_available(broker.db)
        return {"criterion": "D-before-ack", "status": "PASS", "kill_status": status,
                "hit": hit, "same_file_inode": inode, "expected_admissions": 2,
                "observed_admissions": len(after["admissions"]),
                "semantics": "A MQTT QoS 1 retransmission is a new admission after restart; the original command remains committed once."}
    finally:
        broker.cleanup()
        if second:
            second.cleanup()


def lost_ack_live_client(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    second = None
    proxy = None
    client = None
    try:
        broker.ready()
        proxy = Proxy(broker.address, broker.observations, drop_ack=True)
        client, command = mqttium_process(proxy.address, case, reconnect=True,
                                          client_id="qos1-live-reconnect")
        require(proxy.ack_dropped.wait(5), "puback_loss_not_reached")
        before = database(broker.db)
        write_json(case / "before-restart.json", before)
        require(len(before["admissions"]) >= 1, "lost_puback_without_commit")
        # The proxy has observed and discarded a complete PUBACK. Client process
        # and its original public receipt stay alive across broker restart.
        status = broker.stop(killed=True)
        second = Broker(binary, case, db=broker.db, label="restarted")
        second.ready()
        proxy.upstream = second.address
        proxy.reconnect_allowed.set()
        receipt = collect_client(client, case)
        require(receipt["pid"] == client.pid, "client_process_replaced")
        publications = [event for event in receipt["events"] if event["kind"] == "publish"]
        completions = [event for event in receipt["events"] if event["kind"] == "receipt_success"]
        connacks = [event for event in receipt["events"] if event["kind"] == "connack"]
        require(len(publications) == len(completions) == 1 and
                publications[0]["packet_id"] == completions[0]["packet_id"] and
                completions[0]["is_done"], "original_receipt_not_completed")
        require(len(connacks) >= 2 and not connacks[0]["session_present"] and
                all(event["session_present"] and event["reason_code"] == 0 for event in connacks[1:]),
                "live_client_session_not_resumed")
        after = database(second.db)
        write_json(case / "durable.json", after)
        require(len(after["admissions"]) == 2 and after["counts"]["inbound"] == 0,
                "qos1_replay_not_admitted")
        require(len(after["releases"]) == 0, "qos1_used_release_command")
        require(all(after["counts"][n] == 0 for n in ("publications", "payloads", "obligations")),
                "qos1_publication_retained")
        second.stop()
        return {"criterion": "D-lost-ack", "status": "PASS", "command": command,
                "client": receipt, "client_process": client.pid, "kill_status": status,
                "independent_cut": "proxy observed complete PUBACK and forwarded none of its bytes",
                "before_admissions": len(before["admissions"]), "after_admissions": len(after["admissions"]),
                "original_receipt_survived": True}
    finally:
        stop_client(client)
        if proxy:
            proxy.close()
        broker.cleanup()
        if second:
            second.cleanup()


def concurrency(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    try:
        broker.ready()
        def client(name: str, qos: int):
            with Wire(broker.address, broker.observations, name) as wire:
                wire.connect(name, session_present=False)
                for _ in range(3):
                    wire.exchange(7, name.encode(), qos=qos)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(client, "qos1-parallel", 1), pool.submit(client, "qos2-parallel", 2)]
            for future in futures:
                future.result(timeout=10)
        with Wire(broker.address, broker.observations, "partial") as partial:
            partial.connect("qos1-partial", session_present=False)
            partial.send(b"\x32\x64\x00")
            client("qos1-unblocked", 1)
        finished_state(broker, case, admissions=9, releases=3)
        broker.stop()
        return {"criterion": "E-concurrency", "status": "PASS", "admissions": 9,
                "partial_client_did_not_block": True}
    finally:
        broker.cleanup()


def late_receipt(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("ack.receipt",))
    try:
        broker.ready()
        with Wire(broker.address, broker.observations, "late-receipt") as wire:
            wire.connect("qos1-late-receipt", session_present=False)
            wire.exchange(1, b"first", qos=1)
            hit = broker.wait_event("barrier_hit", point="ack.receipt")
            require(hit.get("details", {}).get("packet_id") == 1, "wrong_deferred_ack")
            # The client has consumed PUBACK. Its internal Written receipt is
            # still held. A new occurrence can reuse PI1 and even change QoS.
            wire.send(publish_packet("reception/late", b"new-qos2", 1, qos=2))
            wire.expect(0x50, 1)
            for identifier in range(2, 9):
                wire.send(publish_packet("reception/late", b"held-qos2", identifier, qos=2))
                wire.expect(0x50, identifier)
            before = database(broker.db)
            require(len(before["admissions"]) == 9 and before["counts"]["inbound"] == 8,
                    "receive_window_not_filled")
            write_json(case / "before-old-receipt.json", before)
            broker.release("ack.receipt")
            released = broker.wait_event("barrier_released", point="ack.receipt", after=int(hit["seq"]))
            require(released.get("details", {}).get("effect") == hit.get("details", {}).get("effect"),
                    "wrong_deferred_receipt_released")
            wire.send(publish_packet("reception/late", b"ninth", 9, qos=1))
            frame = wire.poll(3)
            require(frame is not None, "receive_maximum_refusal_missing")
            if frame.kind == 0x40:
                identifier, reason = frame.ack()
                require(not (identifier == 9 and reason < 128), "stale_receipt_released",
                        "Old PUBACK freed a new QoS 2 occurrence and allowed the ninth live identifier")
            require(frame.kind == 0xE0 and frame.body and frame.body[0] == 0x93,
                    "wrong_receive_maximum_refusal", hex(frame.header))
            after = database(broker.db)
            require(after["admissions"] == before["admissions"], "stale_receipt_released")
        broker.stop()
        return {"criterion": "C-late-receipt", "status": "PASS", "deferred_ack": hit,
                "released_ack": released,
                "active_new_qos2": 8, "ninth_identifier_refused": True,
                "client_received_first_puback": True}
    finally:
        broker.cleanup()


def late_same_qos_receipt(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("admission.prepared", "ack.receipt"))
    try:
        broker.ready()
        with Wire(broker.address, broker.observations, "late-same-qos") as wire:
            wire.connect("qos1-late-same-qos", session_present=False)
            wire.send(publish_packet("reception/late-same", b"same-payload", 7, qos=1))
            first = broker.wait_event("barrier_hit", point="admission.prepared")
            broker.release("admission.prepared")
            wire.expect(0x40, 7)
            deferred = broker.wait_event("barrier_hit", point="ack.receipt", after=int(first["seq"]))
            require(deferred["details"]["packet_id"] == 7 and
                    deferred["details"]["socket_write_complete"], "wrong_deferred_ack")
            # MQTT-4.3.2-5: the client has received PUBACK, so even an equal
            # payload with DUP=1 and the same PI is a new publication. Retain
            # its actual SQL transaction while the old Written result arrives.
            wire.send(publish_packet("reception/late-same", b"same-payload", 7, qos=1, dup=True))
            second = broker.wait_event("barrier_hit", point="admission.prepared", after=int(deferred["seq"]))
            require(second["operation"] != first["operation"], "new_occurrence_reused_operation")
            before = database(broker.db)
            write_json(case / "second-command-before-commit.json", before)
            require(len(before["admissions"]) == 1 and
                    before["admissions"][0]["id"] == operation_hex(first["operation"]),
                    "second_admission_visible_before_commit")
            broker.release("ack.receipt")
            released = broker.wait_event("barrier_released", point="ack.receipt", after=int(second["seq"]))
            require(released["details"]["effect"] == deferred["details"]["effect"],
                    "wrong_deferred_receipt_released")
            old_credits = [event for event in broker.events() if event["kind"] == "receive_credit" and
                           event.get("details", {}).get("effect") == deferred["details"]["effect"]]
            require(len(old_credits) == 1 and not old_credits[0]["details"]["released"],
                    "stale_receipt_released")
            frame = wire.poll(.15)
            assert_no_positive_ack([frame] if frame else [], 0x40, 7)
            require(frame is None, "unexpected_frame_while_held")
            require(database(broker.db)["admissions"] == before["admissions"],
                    "second_admission_visible_before_commit")
            broker.release("admission.prepared")
            wire.expect(0x40, 7)
            credit = broker.wait_event("receive_credit", after=int(released["seq"]))
            require(credit["details"]["packet_kind"] == "PUBACK" and
                    credit["details"]["packet_id"] == 7 and credit["details"]["released"] and
                    credit["details"]["effect"] != deferred["details"]["effect"] and
                    credit["details"]["publish_order"] != deferred["details"]["publish_order"],
                    "new_occurrence_credit_not_released")
            after = finished_state(broker, case, admissions=2, releases=0)
            require({row["id"] for row in after["admissions"]} ==
                    {operation_hex(first["operation"]), operation_hex(second["operation"])},
                    "wrong_durable_admission_identity")
        broker.stop()
        writer_available(broker.db)
        return {"criterion": "C-late-same-qos", "status": "PASS", "first_prepared": first,
                "second_prepared": second, "deferred_ack": deferred, "released_ack": released,
                "old_credit": old_credits[0], "new_credit": credit, "admissions": 2,
                "client_received_first_puback": True, "second_publish_dup": True,
                "same_packet_identifier": 7, "same_payload": True, "writer_released": True}
    finally:
        broker.cleanup()


def quota_refusal(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    original = None
    try:
        broker.ready()
        # This isolated intervention tests a real store quota guard. It is
        # applied only after readiness, with receipt space still available for
        # recording certain rejection, then restored before shutdown/recovery.
        with sqlite3.connect(broker.db, timeout=.25) as connection:
            original = connection.execute("SELECT limit_payload_bytes FROM meta").fetchone()[0]
            connection.execute("UPDATE meta SET limit_payload_bytes = ?", ((0).to_bytes(8, "big"),))
        broker.observations.record("quota_intervention", field="limit_payload_bytes", value="0",
                                   scope="isolated artificial guard control; not qualifying")
        refusals = []
        with Wire(broker.address, broker.observations, "certain-refusals") as wire:
            wire.connect("qos1-refusal", session_present=False)
            for identifier in range(1, 10):
                wire.send(publish_packet("reception/refusal", b"rejected", identifier, qos=1))
                frame = wire.expect(0x40)
                require(frame.ack() == (identifier, 0x97), "wrong_puback_refusal", repr(frame.ack()))
                refusals.append({"qos": 1, "packet_id": identifier, "reason": 0x97})
            for identifier in range(10, 19):
                wire.send(publish_packet("reception/refusal", b"rejected", identifier, qos=2))
                frame = wire.expect(0x50)
                require(frame.ack() == (identifier, 0x97), "wrong_pubrec_refusal", repr(frame.ack()))
                refusals.append({"qos": 2, "packet_id": identifier, "reason": 0x97})
            rejected = database(broker.db)
            write_json(case / "rejected-durable.json", rejected)
            require(not rejected["admissions"] and rejected["counts"]["operations"] == 18,
                    "certain_refusal_not_durable")
            command = [sys.executable, str(Path(__file__).with_name("external_mqttium.py")),
                       "--host", broker.address[0], "--port", str(broker.address[1]),
                       "--client-id", "qos1-refusal-mqttium", "--qos", "1"]
            outcome = subprocess.run(command, capture_output=True, text=True, timeout=15)
            (case / "mqttium-refusal.stderr").write_text(outcome.stderr)
            require(outcome.returncode == 2, "mqttium_refusal_not_distinguished", outcome.stderr[-512:])
            client = json.loads(outcome.stdout)
            write_json(case / "mqttium-refusal.json", client)
            require(client["status"] == "rejected" and client["publications"][0]["reason_code"] == 0x97 and
                    client["publications"][0]["exception"] == "PublishRejectedError",
                    "mqttium_refusal_cause_lost")
            with sqlite3.connect(broker.db, timeout=.25) as connection:
                connection.execute("UPDATE meta SET limit_payload_bytes = ?", (original,))
            original = None
            wire.exchange(19, b"credit-reused", qos=1)
            wire.exchange(19, b"mixed-after-refusal", qos=2)
        finished_state(broker, case, admissions=2, releases=1)
        broker.stop()
        return {"criterion": "E-refusal", "status": "PASS", "refusals": refusals,
                "credit_reused": True, "mqttium": {"command": command, "result": client},
                "intervention": "Isolated post-readiness payload quota guard; restored before restart"}
    finally:
        if original is not None and broker.process.poll() is None:
            with sqlite3.connect(broker.db, timeout=.25) as connection:
                connection.execute("UPDATE meta SET limit_payload_bytes = ?", (original,))
        broker.cleanup()


def unknown_outcome(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    original = None
    try:
        broker.ready()
        with Wire(broker.address, broker.observations, "unknown") as wire:
            wire.connect("qos1-unknown", session_present=False)
            with sqlite3.connect(broker.db, timeout=.25) as connection:
                original = connection.execute("SELECT limit_receipt_slots FROM meta").fetchone()[0]
                connection.execute("UPDATE meta SET limit_receipt_slots = ?", ((0).to_bytes(8, "big"),))
            broker.observations.record("quota_intervention", field="limit_receipt_slots", value="0",
                                       scope="isolated unknown-outcome guard; restored before resolution")
            wire.send(publish_packet("reception/unknown", b"uncertain", 7, qos=1))
            hit = broker.wait_event("store_result")
            require(hit["details"]["outcome"] == "PendingOrUnknown", "unknown_not_reached")
            frame = wire.poll(.2)
            require(frame is None, "unknown_outcome_acknowledged")
            before = database(broker.db)
            require(not before["admissions"] and not before["tables"]["operations"],
                    "unknown_operation_unexpectedly_decided")
            write_json(case / "unknown-durable.json", before)
            with sqlite3.connect(broker.db, timeout=.25) as connection:
                connection.execute("UPDATE meta SET limit_receipt_slots = ?", (original,))
            original = None
            frame = wire.expect(0x40, timeout=12)
            require(frame.ack() == (7, 0x83), "resolved_fence_reason_lost", repr(frame.ack()))
            resolved = database(broker.db)
            write_json(case / "resolved-durable.json", resolved)
            require(any(row["id"] == operation_hex(hit["operation"]) for row in resolved["tables"]["fences"]),
                    "resolution_did_not_keep_operation_identity")
            require(not resolved["admissions"], "unknown_replayed_under_new_operation")
            wire.exchange(7, b"new-publication", qos=1)
        finished_state(broker, case, admissions=1, releases=0)
        broker.stop()
        return {"criterion": "E-unknown", "status": "PASS", "unknown_operation": hit["operation"],
                "acknowledgement_while_unknown": False, "terminal_puback": 0x83,
                "same_operation_fenced": True, "credit_reused": True,
                "intervention": "Isolated post-readiness receipt quota; restored before 10-second resolution"}
    finally:
        if original is not None and broker.process.poll() is None:
            with sqlite3.connect(broker.db, timeout=.25) as connection:
                connection.execute("UPDATE meta SET limit_receipt_slots = ?", (original,))
        broker.cleanup()


def takeover_late_commit(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case, pauses=("admission.committed",))
    first = None
    second = None
    try:
        broker.ready()
        first = Wire(broker.address, broker.observations, "closed-generation")
        first.connect("qos1-takeover", session_present=False)
        first.send(publish_packet("reception/takeover", b"first", 7, qos=1))
        hit = broker.wait_event("barrier_hit", point="admission.committed")
        first_generation = next(event["connection"] for event in broker.events()
                                if event["kind"] == "session_established")
        first.close()
        # Existing sessions reconnect from the confirmed runtime cache while
        # the writer still holds the committed command's result.
        second = Wire(broker.address, broker.observations, "new-generation")
        second.connect("qos1-takeover", session_present=True)
        before = max(int(event["seq"]) for event in broker.events())
        broker.release("admission.committed")
        result = broker.wait_event("store_result", after=before)
        require(result["operation"] == hit["operation"], "late_result_changed_operation")
        # A pending operation can answer the currently owning connection; its
        # old socket must not receive a newly emitted business acknowledgement.
        deadline = time.monotonic() + .25
        while time.monotonic() < deadline:
            frame = second.poll(max(0, deadline - time.monotonic()))
            if frame is None:
                break
            require(frame.kind == 0x40 and frame.ack() == (7, 0), "unexpected_takeover_frame")
        frames = [event for event in broker.events() if int(event["seq"]) > before and
                  event["kind"] == "frame_queued" and event.get("connection") == first_generation and
                  event.get("details", {}).get("packet_kind") == "PUBACK"]
        require(not frames, "ack_on_closed_generation")
        state = database(broker.db)
        require(len(state["admissions"]) == 1, "late_commit_readmitted")
        second.send(publish_packet("reception/takeover", b"second", 7, qos=1))
        next_hit = broker.wait_event("barrier_hit", point="admission.committed", after=int(hit["seq"]))
        require(next_hit["operation"] != hit["operation"], "new_occurrence_reused_operation")
        broker.release("admission.committed")
        second.expect(0x40, 7)
        finished_state(broker, case, admissions=2, releases=0)
        broker.stop()
        return {"criterion": "E-takeover", "status": "PASS", "first_operation": hit["operation"],
                "next_operation": next_hit["operation"], "closed_generation": first_generation,
                "no_ack_on_closed_generation": True}
    finally:
        if first:
            first.close()
        if second:
            second.close()
        broker.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--mode", choices=("normal", "complete"), default="normal")
    parser.add_argument("--case")
    args = parser.parse_args()
    require(args.binary.is_file(), "binary_missing")
    args.out.mkdir(parents=True, exist_ok=True)
    require(not any(args.out.iterdir()), "output_not_empty")
    cases = [("A", nominal), ("C-reuse", reuse), ("E-concurrency", concurrency)]
    if args.mode == "complete" or args.case:
        cases += [("B", precommit), ("C-late-receipt", late_receipt),
                  ("C-late-same-qos", late_same_qos_receipt),
                  ("D-before-ack", restart_before_ack), ("D-lost-ack", lost_ack_live_client),
                  ("E-refusal", quota_refusal), ("E-unknown", unknown_outcome),
                  ("E-takeover", takeover_late_commit)]
    result = {"format": "mqttiumd.qos1-system-reception", "schema_version": 1,
              "qualification": "system-test-only", "mode": args.mode,
              "binary": {"path": str(args.binary.resolve()),
                         "sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()},
              "scope": "Finite durable incoming MQTT 5 QoS 1 and coexistence with QoS 2; no full-conformance claim",
              "cases": []}
    try:
        require(args.case is None or args.case in {name for name, _ in cases}, "unknown_case")
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
    print(json.dumps({"status": result["status"], "receipt": str(args.out / "receipt.json")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
