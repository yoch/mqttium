#!/usr/bin/env python3
"""Identified mqttium 1.3.0 source client, using only its public async API.

The optional reconnect policy keeps the same client and publication receipt
alive. Only the broker/proxy orchestrator causes transport loss; this driver
never publishes again to imitate protocol replay.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

# The dependency-free client is imported from the identified repository, not a
# potentially unrelated site-package. Its actual imported path is checked below.
CLIENT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(CLIENT_ROOT / "src"))

import mqttium
from mqttium import MQTTProtocolVersion, MQTTTimeoutError, PublishRejectedError
from mqttium.api import AsyncClient, Properties, ReconnectPolicy


def client_identity() -> dict:
    package = CLIENT_ROOT / "src" / "mqttium"
    module_path = Path(mqttium.__file__).resolve()
    if module_path != package / "__init__.py" or mqttium.__version__ != "1.3.0":
        raise RuntimeError("mqttium 1.3.0 must be imported from this checkout's src")
    digest = hashlib.sha256()
    files = sorted(path for path in package.rglob("*")
                   if path.is_file() and path.suffix == ".py")
    for path in files:
        relative = path.relative_to(CLIENT_ROOT).as_posix().encode()
        data = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    revision = subprocess.run(["git", "-C", str(CLIENT_ROOT), "rev-parse", "HEAD"],
                              check=True, capture_output=True, text=True, timeout=3).stdout.strip()
    return {"name": "mqttium", "version": mqttium.__version__, "module_path": str(module_path),
            "source_revision": revision, "source_sha256": digest.hexdigest(),
            "source_files": len(files), "source_hash_recipe": "path-and-length-prefixed-python-files-v1"}


class Events:
    def __init__(self, path: Path | None):
        self.events: list[dict] = []
        self.stream = path.open("x", encoding="utf-8") if path else None

    def record(self, kind: str, **fields) -> None:
        if len(self.events) >= 256:
            raise RuntimeError("external client event budget exceeded")
        event = {"kind": kind, "monotonic_ns": str(time.monotonic_ns()), **fields}
        encoded = json.dumps(event, sort_keys=True)
        if len(encoded.encode()) > 4096:
            raise RuntimeError("external client event byte budget exceeded")
        self.events.append(event)
        if self.stream:
            self.stream.write(encoded + "\n")
            self.stream.flush()

    def close(self) -> None:
        if self.stream:
            self.stream.close()
            self.stream = None


async def exchange(host: str, port: int, client_id: str, *, qoses: tuple[int, ...] = (2,),
                   expected_session_present: bool | None = None, events_path: Path | None = None,
                   reconnect: bool = False, wait_timeout: float = 3) -> dict:
    if not 0 < wait_timeout <= 30 or len(qoses) > 32 or any(qos not in (1, 2) for qos in qoses):
        raise ValueError("bounded QoS 1/2 sequence and wait timeout in (0,30] required")
    identity = client_identity()
    observations = Events(events_path)
    policy = ReconnectPolicy(initial_delay=.05, max_delay=.2, max_retries=30) if reconnect else None
    client = AsyncClient(client_id, protocol=MQTTProtocolVersion.MQTTv5, clean_start=False,
                         keepalive=10,
                         connect_properties=Properties({"session_expiry_interval": 0xFFFFFFFF}),
                         maximum_packet_size=65536, max_outbound_inflight=8,
                         max_unacknowledged_messages=32, max_unacknowledged_bytes=65536,
                         max_write_queue_messages=32, max_write_queue_bytes=65536,
                         reconnect=policy, connect_timeout=3)
    result = {"client": "mqttium", "version": identity["version"], "identity": identity,
              "python": platform.python_version(), "pid": os.getpid(),
              "host": host, "port": port, "client_id": client_id, "status": "success",
              "parameters": {"protocol": 5, "clean_start": False, "session_expiry_interval": 0xFFFFFFFF,
                             "keep_alive": 10, "maximum_packet_size": 65536, "max_outbound_inflight": 8,
                             "qoses": list(qoses), "wait_timeout": wait_timeout,
                             "reconnect": {"initial_delay": .05, "max_delay": .2, "max_retries": 30}
                                          if reconnect else None},
              "publications": [], "events": observations.events}
    disconnected = asyncio.Event()
    connected_notification = asyncio.Event()
    last_notified_connection = 0

    def on_connect(connack):
        nonlocal last_notified_connection
        last_notified_connection = client.stats().connections
        observations.record("connack", reason_code=connack.reason_code,
                            session_present=connack.session_present, callback=True,
                            connections=last_notified_connection)
        connected_notification.set()

    def on_disconnect(cause):
        observations.record("disconnected", cause=type(cause).__name__ if cause else None,
                            state=client.state.name, connections=client.stats().connections)
        disconnected.set()

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    observations.record("client_identity", **identity)
    observations.record("connect", client_id=client_id, protocol=5, clean_start=False,
                        session_expiry_interval=0xFFFFFFFF, keep_alive=10)
    try:
        connack = await client.connect(host, port, timeout=3)
        result["reason_code"] = connack.reason_code
        result["session_present"] = connack.session_present
        if connack.reason_code != 0 or (expected_session_present is not None
                                       and connack.session_present != expected_session_present):
            raise RuntimeError("unexpected CONNACK or Session Present")
        for index, qos in enumerate(qoses):
            topic = "reception/mqttium"
            payload = f"mqttium-qos{qos}-publication-{index}".encode()
            receipt = await client.publish(topic, payload, qos=qos, retain=False, timeout=3)
            if not isinstance(receipt.mid, int) or not 1 <= receipt.mid <= 65535 or receipt.qos != qos:
                raise RuntimeError("mqttium returned an invalid publication identity")
            publication = {"index": index, "qos": qos, "packet_id": receipt.mid,
                           "topic": topic, "payload_sha256": hashlib.sha256(payload).hexdigest()}
            observations.record("publish", **publication, retain=False)
            observations.record("receipt_pending", index=index, qos=qos, packet_id=receipt.mid,
                                is_done=receipt.is_done())
            try:
                await receipt.wait(timeout=wait_timeout)
            except PublishRejectedError as error:
                if not receipt.is_done() or receipt.exception() is not error:
                    raise RuntimeError("rejected receipt did not preserve its terminal cause") from error
                publication.update(result="rejected", reason_code=error.reason_code,
                                   exception=type(error).__name__, is_done=True)
                result["status"] = "rejected"
                observations.record("receipt_rejected", **publication)
            except MQTTTimeoutError:
                publication.update(result="timeout", is_done=receipt.is_done())
                result["status"] = "timeout"
                observations.record("receipt_timeout", **publication)
            else:
                if not receipt.is_done() or receipt.exception() is not None:
                    raise RuntimeError("receipt wait returned without successful protocol completion")
                publication.update(result="success", is_done=True)
                observations.record("receipt_success", **publication)
            result["publications"].append(publication)
            if publication["result"] != "success":
                break
        # Let the public callback retain the latest successful CONNACK before
        # intentional shutdown can supersede a pending lifecycle notification.
        async with asyncio.timeout(3):
            while client.is_connected and last_notified_connection < client.stats().connections:
                connected_notification.clear()
                await connected_notification.wait()
        result["stats_before_cleanup"] = asdict(client.stats())
    except Exception as error:
        result["status"] = "error"
        result["error"] = {"type": type(error).__name__, "message": str(error)[:256]}
        observations.record("client_error", **result["error"])
    finally:
        try:
            async with asyncio.timeout(3):
                was_connected = client.is_connected
                if was_connected:
                    disconnected.clear()
                await client.disconnect()
                # The public lifecycle hook runs after transport cleanup;
                # disconnect() deliberately need not await its notification.
                if was_connected:
                    await disconnected.wait()
        except Exception as error:
            result["cleanup_error"] = {"type": type(error).__name__, "message": str(error)[:256]}
            result["status"] = "error"
        observations.record("cleanup", state=client.state.name, connections=client.stats().connections,
                            pending_receipts=client.stats().receipts.publish)
        result["stats_after_cleanup"] = asdict(client.stats())
        observations.close()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--client-id", default="integration-mqttium")
    parser.add_argument("--qos", type=int, choices=(1, 2), action="append")
    parser.add_argument("--connect-only", action="store_true")
    parser.add_argument("--expected-session-present", type=int, choices=(0, 1))
    parser.add_argument("--events", type=Path)
    parser.add_argument("--reconnect", action="store_true")
    parser.add_argument("--wait-timeout", type=float, default=3)
    args = parser.parse_args()
    result = asyncio.run(exchange(args.host, args.port, args.client_id,
                                  qoses=() if args.connect_only else tuple(args.qos or [2]),
                                  expected_session_present=None if args.expected_session_present is None
                                                           else bool(args.expected_session_present),
                                  events_path=args.events, reconnect=args.reconnect,
                                  wait_timeout=args.wait_timeout))
    print(json.dumps(result, indent=2, sort_keys=True))
    return {"success": 0, "rejected": 2, "timeout": 3, "error": 1}[result["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
