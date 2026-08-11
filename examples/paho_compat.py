"""Paho VERSION2-shaped pub/sub while migrating to MQTTium."""

from __future__ import annotations

import argparse
import threading
import uuid

from mqttium.compat import CallbackAPIVersion, Client, MQTTMessage


def run(host: str, port: int, topic: str, timeout: float) -> None:
    connected = threading.Event()
    received = threading.Event()
    received_payload: list[bytes] = []
    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id=f"mqttium-paho-{uuid.uuid4().hex}",
    )

    def on_connect(
        current: Client,
        userdata: object,
        flags: object,
        reason_code: int,
        properties: object,
    ) -> None:
        del userdata, flags, properties
        if reason_code != 0:
            raise RuntimeError(f"connection rejected: {reason_code}")
        result, _mid = current.subscribe(topic, qos=1)
        if result != 0:
            raise RuntimeError(f"subscription rejected: {result}")
        connected.set()

    def on_message(current: Client, userdata: object, message: MQTTMessage) -> None:
        del current, userdata
        if message.topic == topic:
            received_payload.append(message.payload)
            received.set()

    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_start()
    try:
        client.connect(host, port)
        if not connected.wait(timeout):
            raise TimeoutError("subscription did not become ready")
        info = client.publish(topic, b"hello through the Paho facade", qos=1)
        info.wait_for_publish(timeout=timeout)
        if not received.wait(timeout):
            raise TimeoutError("published message was not received")
        print(f"{topic} => {received_payload[0]!r}")
    finally:
        client.disconnect()
        client.loop_stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", default=f"mqttium/paho/{uuid.uuid4().hex}")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()
    run(args.host, args.port, args.topic, args.timeout)


if __name__ == "__main__":
    main()
