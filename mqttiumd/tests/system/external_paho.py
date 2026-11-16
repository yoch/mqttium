#!/usr/bin/env python3
"""Thin independent paho-mqtt 2.1.0 MQTT 5 client, not the product codec."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import threading
import time


def exchange(host: str, port: int, client_id: str, *, publish: bool = True, qos: int = 2) -> dict:
    import paho.mqtt.client as mqtt
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties

    version = importlib.metadata.version("paho-mqtt")
    if version != "2.1.0":
        raise RuntimeError(f"paho-mqtt 2.1.0 required, found {version}")
    if qos not in (1, 2):
        raise ValueError("QoS 1 or 2 required")
    connected = threading.Event()
    events = []
    result = {"status": "success", "publications": []}
    lock = threading.Lock()

    def record(kind, **fields):
        with lock:
            if len(events) >= 256:
                raise RuntimeError("external client event budget exceeded")
            events.append({"kind": kind, "monotonic_ns": str(time.monotonic_ns()), **fields})

    def on_connect(_client, _userdata, flags, reason_code, _properties):
        result["reason_code"] = reason_code.value
        result["session_present"] = flags.session_present
        record("connack", reason_code=reason_code.value, session_present=flags.session_present)
        connected.set()

    def on_publish(_client, _userdata, mid, reason_code, _properties):
        rejected = reason_code.value >= 128
        with lock:
            result["publications"].append({"packet_id": mid, "qos": qos,
                                           "result": "rejected" if rejected else "success",
                                           "reason_code": reason_code.value})
            if rejected:
                result["status"] = "rejected"
        record("publish_rejected" if rejected else f"qos{qos}_completed",
               packet_id=mid, qos=qos, reason_code=reason_code.value)

    def on_disconnect(_client, _userdata, _flags, reason_code, _properties):
        record("disconnected", reason_code=reason_code.value)

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id=client_id, protocol=mqtt.MQTTv5, reconnect_on_failure=False)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    properties = Properties(PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 0xFFFFFFFF
    record("connect", client_id=client_id, clean_start=False, session_expiry_interval=0xFFFFFFFF,
           keep_alive=30)
    try:
        client.connect(host, port, keepalive=30, clean_start=False, properties=properties)
        client.loop_start()
        if not connected.wait(5):
            raise RuntimeError("external CONNACK deadline exceeded")
        if result["reason_code"] != 0:
            raise RuntimeError(f"external CONNECT refused with code {result['reason_code']}")
        if publish:
            publication = client.publish("reception/external", f"external-paho-qos{qos}".encode(),
                                         qos=qos, retain=False)
            record("publish", packet_id=publication.mid, qos=qos, retain=False)
            publication.wait_for_publish(timeout=5)
            if not publication.is_published():
                raise RuntimeError(f"external QoS {qos} completion deadline exceeded")
    finally:
        client.disconnect()
        client.loop_stop()
    return {"client": "paho-mqtt", "version": version,
            "module_path": str(Path(mqtt.__file__).resolve()),
            "module_sha256": hashlib.sha256(Path(mqtt.__file__).read_bytes()).hexdigest(),
            "python": platform.python_version(),
            "host": host, "port": port, "client_id": client_id, "events": events, **result}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--client-id", default="paho-durable")
    parser.add_argument("--connect-only", action="store_true")
    parser.add_argument("--qos", type=int, choices=(1, 2), default=2)
    args = parser.parse_args()
    result = exchange(args.host, args.port, args.client_id, publish=not args.connect_only, qos=args.qos)
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "success" else 2


if __name__ == "__main__":
    raise SystemExit(main())
