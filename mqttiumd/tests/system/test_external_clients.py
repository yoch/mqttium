"""Focused checks of external-client observations, through their public API."""

import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from external_paho import exchange as paho_exchange
from external_mqttium import exchange as mqttium_exchange


async def read_packet(reader):
    async with asyncio.timeout(3):
        header = (await reader.readexactly(1))[0]
        remaining = 0
        for count in range(4):
            digit = (await reader.readexactly(1))[0]
            remaining += (digit & 127) << (7 * count)
            if not digit & 128:
                if remaining > 65536:
                    raise AssertionError("test peer packet budget exceeded")
                return header, await reader.readexactly(remaining)
        raise AssertionError("test peer malformed Remaining Length")


class ScriptedBroker:
    """Small independent wire peer: no mqttium codec or client private API."""

    def __init__(self, script):
        self.script = script
        self.tasks = []
        self.errors = []
        self.connections = 0

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.accept, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    def accept(self, reader, writer):
        self.connections += 1
        self.tasks.append(asyncio.create_task(self.serve(reader, writer, self.connections)))

    async def serve(self, reader, writer, connection):
        try:
            await self.script(reader, writer, connection)
        except Exception as error:
            self.errors.append(error)
        finally:
            writer.close()
            await writer.wait_closed()

    async def __aexit__(self, *_args):
        self.server.close()
        await self.server.wait_closed()
        if self.tasks:
            await asyncio.wait_for(asyncio.gather(*self.tasks), timeout=3)
        if self.errors:
            raise self.errors[0]


async def connack(reader, writer, *, session_present=False):
    header, body = await read_packet(reader)
    if header != 0x10 or body[:10] != b"\x00\x04MQTT\x05\x00\x00\x0a":
        raise AssertionError("driver did not send the explicit accepted MQTT 5 profile")
    properties = body[11:11 + body[10]]
    values = {}
    while properties:
        key = properties[0]
        size = {0x11: 4, 0x21: 2, 0x22: 2, 0x27: 4}[key]
        values[key] = int.from_bytes(properties[1:1 + size], "big")
        properties = properties[1 + size:]
    if values.get(0x11) != 0xFFFFFFFF or values.get(0x27) != 65536:
        raise AssertionError("missing profile expiry or packet-size property")
    writer.write(b"\x20\x03" + bytes((int(session_present), 0, 0)))
    await writer.drain()


def publication_id(body):
    topic_size = int.from_bytes(body[:2], "big")
    return body[2 + topic_size:4 + topic_size]


class ExternalMqttiumTests(unittest.IsolatedAsyncioTestCase):
    async def test_mixed_publications_wait_for_real_protocol_completion(self):
        async def script(reader, writer, _connection):
            await connack(reader, writer)
            for qos in (1, 1, 2):
                header, body = await read_packet(reader)
                self.assertEqual((header >> 1) & 3, qos)
                mid = publication_id(body)
                writer.write(bytes((0x40 if qos == 1 else 0x50, 2)) + mid)
                await writer.drain()
                if qos == 2:
                    self.assertEqual(await read_packet(reader), (0x62, mid))
                    writer.write(b"\x70\x02" + mid)
                    await writer.drain()
            self.assertEqual(await read_packet(reader), (0xE0, b""))

        async with ScriptedBroker(script) as broker:
            receipt = await mqttium_exchange("127.0.0.1", broker.port, "mixed", qoses=(1, 1, 2),
                                            expected_session_present=False)
        self.assertEqual(receipt["status"], "success")
        self.assertEqual([row["qos"] for row in receipt["publications"]], [1, 1, 2])
        self.assertTrue(all(row["is_done"] for row in receipt["publications"]))
        self.assertEqual(receipt["identity"]["version"], "1.3.0")
        self.assertEqual(len(receipt["identity"]["source_sha256"]), 64)
        self.assertTrue(receipt["identity"]["module_path"].endswith("/src/mqttium/__init__.py"))
        self.assertEqual(receipt["stats_after_cleanup"]["receipts"]["publish"], 0)

    async def test_wait_timeout_is_distinct_from_a_successful_publish_call(self):
        async def script(reader, writer, _connection):
            await connack(reader, writer)
            header, _body = await read_packet(reader)
            self.assertEqual(header, 0x32)
            # Deliberately no PUBACK. The public publish call has succeeded,
            # but its original receipt is still pending when its wait expires.
            self.assertEqual(await read_packet(reader), (0xE0, b""))

        with tempfile.TemporaryDirectory() as directory:
            events_path = Path(directory) / "client.jsonl"
            async with ScriptedBroker(script) as broker:
                receipt = await mqttium_exchange("127.0.0.1", broker.port, "pending", qoses=(1,),
                                                events_path=events_path, wait_timeout=.05)
            streamed = [json.loads(line) for line in events_path.read_text().splitlines()]
        self.assertEqual(receipt["status"], "timeout")
        self.assertEqual(receipt["publications"][0]["result"], "timeout")
        self.assertFalse(receipt["publications"][0]["is_done"])
        self.assertTrue(any(event["kind"] == "receipt_pending" and not event["is_done"] for event in streamed))
        self.assertFalse(any(event["kind"] == "receipt_success" for event in streamed))

    async def test_negative_puback_preserves_publish_rejected_error(self):
        async def script(reader, writer, _connection):
            await connack(reader, writer)
            _header, body = await read_packet(reader)
            writer.write(b"\x40\x04" + publication_id(body) + b"\x97\x00")
            await writer.drain()
            self.assertEqual(await read_packet(reader), (0xE0, b""))

        async with ScriptedBroker(script) as broker:
            receipt = await mqttium_exchange("127.0.0.1", broker.port, "rejected", qoses=(1,))
        self.assertEqual(receipt["status"], "rejected")
        publication = receipt["publications"][0]
        self.assertEqual(publication["result"], "rejected")
        self.assertEqual(publication["reason_code"], 0x97)
        self.assertEqual(publication["exception"], "PublishRejectedError")
        self.assertFalse(any(event["kind"] == "receipt_success" for event in receipt["events"]))

    async def test_transport_loss_reconnects_same_client_and_original_receipt(self):
        wire_publications = []

        async def script(reader, writer, connection):
            await connack(reader, writer, session_present=connection == 2)
            header, body = await read_packet(reader)
            wire_publications.append((header, body))
            if connection == 1:
                return  # Actual transport loss, no ACK and no client disconnect().
            self.assertEqual(connection, 2)
            self.assertEqual(header, 0x3A, "QoS 1 reconnect replay must set DUP")
            self.assertEqual(body, wire_publications[0][1], "replay retains mid and payload")
            writer.write(b"\x40\x02" + publication_id(body))
            await writer.drain()
            self.assertEqual(await read_packet(reader), (0xE0, b""))

        async with ScriptedBroker(script) as broker:
            receipt = await mqttium_exchange("127.0.0.1", broker.port, "reconnect", qoses=(1,),
                                            expected_session_present=False, reconnect=True)
        self.assertEqual(receipt["status"], "success")
        self.assertEqual(len(wire_publications), 2)
        self.assertEqual(len(receipt["publications"]), 1)
        self.assertEqual(sum(event["kind"] == "publish" for event in receipt["events"]), 1)
        self.assertEqual(receipt["stats_before_cleanup"]["connections"], 2)
        self.assertEqual([event["session_present"] for event in receipt["events"] if event["kind"] == "connack"],
                         [False, True])


class ExternalPahoTests(unittest.TestCase):
    def test_a_negative_ack_is_reported_as_rejection_not_success(self):
        class RefusedClient:
            def __init__(self, **_options):
                pass

            def connect(self, *_args, **_kwargs):
                self.on_connect(self, None, SimpleNamespace(session_present=False),
                                SimpleNamespace(value=0), None)

            def loop_start(self):
                pass

            def publish(self, *_args, **_kwargs):
                self.on_publish(self, None, 7, SimpleNamespace(value=0x97), None)
                return SimpleNamespace(mid=7, wait_for_publish=lambda **_kwargs: None,
                                       is_published=lambda: True)

            def disconnect(self):
                pass

            def loop_stop(self):
                pass

        with patch("paho.mqtt.client.Client", RefusedClient):
            receipt = paho_exchange("127.0.0.1", 1883, "negative-paho")
        self.assertFalse(any(event["kind"] == "qos2_completed" for event in receipt["events"]),
                         "a negative terminal ACK is rejection, not successful completion")
        self.assertEqual(receipt["status"], "rejected")
        self.assertEqual(receipt["publications"][0]["reason_code"], 0x97)


if __name__ == "__main__":
    unittest.main()
