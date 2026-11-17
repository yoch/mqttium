#!/usr/bin/env python3
"""Public mqttium/Paho APIs and identified Mosquitto CLI for wildcard reception."""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import shutil
import subprocess
import time

from external_sessions import INFINITE, MqttiumSession, PahoSession, message_row
from mqttium.api import SubscribeOptions
from pubsub_probe import decode_publish, decode_filter_ack
from qos1_reception import Proxy
from tcp_probe import ReceiptFailure, packet, require, utf8


TIMEOUT = 5


@dataclass(frozen=True)
class Subscription:
    filter: str
    qos: int = 1
    no_local: bool = False
    retain_as_published: bool = False
    retain_handling: int = 0

    def bits(self):
        if (self.qos not in (0, 1, 2) or type(self.no_local) is not bool
                or type(self.retain_as_published) is not bool or self.retain_handling not in (0, 1, 2)):
            raise ValueError('invalid subscription options')
        return self.qos | (int(self.no_local) << 2) | (int(self.retain_as_published) << 3) | (self.retain_handling << 4)


def requests(values):
    values = tuple(values)
    if not 1 <= len(values) <= 8:
        raise ValueError('one to eight subscription filters required')
    for value in values:
        if not isinstance(value, Subscription) or not 0 < len(value.filter.encode()) <= 1024:
            raise ValueError('bounded explicit subscription required')
        value.bits()
    return values


def subscribe_packet(identifier, values):
    values = requests(values)
    if not 1 <= identifier <= 65535:
        raise ValueError('Packet Identifier required')
    body = identifier.to_bytes(2, 'big') + b'\x00'
    return packet(0x82, body + b''.join(utf8(value.filter) + bytes([value.bits()]) for value in values))


class PacketTap(Proxy):
    """Use the existing independent bounded packet observer without mutations.

    Its fixed128-frame/20-second-per-connection bounds are sufficient for each
    deliberately short client case. Long routing measurements use raw sockets.
    """
    def server_frames(self):
        with self.lock:
            return tuple(self.frames)

    async def subscribed(self, count, expected):
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            frames = [frame for frame in self.server_frames() if frame.kind == 0x90]
            if len(frames) >= count:
                identifier, reasons = decode_filter_ack(frames[count - 1])
                require(reasons == tuple(expected), 'wildcard_suback_wire_mismatch', str(reasons))
                return {'packet_id': identifier, 'reasons': list(reasons)}
            await asyncio.sleep(.005)
        raise ReceiptFailure('wildcard_suback_not_observed')

    def publications(self, payload=None):
        rows = [decode_publish(frame) for frame in self.server_frames() if frame.kind == 0x30]
        return rows if payload is None else [row for row in rows if row.payload == payload]


class MqttiumPeer(MqttiumSession):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.incoming = asyncio.Queue(32)
        self.reader = None
        self.reader_error = None

    async def open(self, expected=None):
        present = await super().open(expected)
        self.reader = asyncio.create_task(self._read_messages())
        return present

    async def _read_messages(self):
        try:
            async for message in self.messages:
                require(not self.incoming.full(), 'wildcard_client_message_budget')
                self.incoming.put_nowait(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.reader_error = error

    async def subscribe(self, values):
        values = requests(values)
        ack = await self.client.subscribe([(value.filter, SubscribeOptions(
            qos=value.qos, no_local=value.no_local, retain_as_published=value.retain_as_published,
            retain_handling=value.retain_handling)) for value in values], timeout=TIMEOUT)
        self.log.record('suback', packet_id=ack.mid, reasons=list(ack.reason_codes),
                        subscriptions=[asdict(value) for value in values])
        require(ack.reason_codes == tuple(min(value.qos, 1) for value in values),
                'wildcard_public_subscription_refused')
        return ack.mid

    async def unsubscribe(self, filters):
        ack = await self.client.unsubscribe(tuple(filters), timeout=TIMEOUT)
        self.log.record('unsuback', packet_id=ack.mid, filters=list(filters), reasons=list(ack.reason_codes))
        require(all(code in (0, 0x11) for code in ack.reason_codes), 'wildcard_public_unsubscribe_refused')
        return tuple(ack.reason_codes)

    async def publish(self, topic, payload, qos=1):
        receipt = await self.client.publish(topic, payload, qos=qos, retain=False, timeout=TIMEOUT)
        self.log.record('publication_submitted', qos=qos,
                        packet_id=receipt.mid if qos else None, client_receipt_id=receipt.mid,
                        payload_sha256=hashlib.sha256(payload).hexdigest())
        await receipt.wait(timeout=TIMEOUT)
        require(receipt.is_done() and receipt.exception() is None, 'wildcard_publication_not_completed')
        self.log.record('publication_completed', qos=qos, packet_id=receipt.mid if qos else None,
                        completion_scope='local-write-only' if qos == 0 else 'MQTT acknowledgement')
        return receipt.mid

    async def receive(self, payload=None, qos=None, *, acknowledge=True):
        if self.reader_error:
            raise self.reader_error
        message = await asyncio.wait_for(self.incoming.get(), timeout=TIMEOUT)
        row = message_row(message)
        self.log.record('message', **row)
        require(payload is None or bytes(message.payload) == payload, 'wildcard_payload_mismatch')
        require(qos is None or row['qos'] == qos, 'wildcard_qos_mismatch')
        if row['qos'] and acknowledge:
            await self.client.ack(message)
            self.log.record('puback_submitted', packet_id=message.mid)
        return row

    async def no_message(self, timeout=.15):
        try:
            message = await asyncio.wait_for(self.incoming.get(), timeout=timeout)
        except TimeoutError:
            if self.reader_error:
                raise self.reader_error
            return
        row = message_row(message)
        self.log.record('unexpected_message', **row)
        raise ReceiptFailure('wildcard_unexpected_or_duplicate_delivery')

    async def disconnect(self, expiry=None):
        if self.reader is not None:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
            self.reader = None
        return await super().disconnect(expiry)


class PahoPeer(PahoSession):
    def _published(self, _client, _data, mid, reason, _properties):
        self.log.record('publication_callback', client_receipt_id=mid, reason_code=reason.value)
        self._put(self.pubacks, (mid, reason.value))

    async def publish(self, topic, payload, qos=1):
        info = self.client.publish(topic, payload, qos=qos, retain=False)
        require(info.rc == self.mqtt.MQTT_ERR_SUCCESS, 'paho_wildcard_publish_submission')
        await asyncio.to_thread(info.wait_for_publish, TIMEOUT)
        require(info.is_published(), 'wildcard_publication_not_completed')
        mid, reason = await asyncio.to_thread(self._get, self.pubacks)
        require(mid == info.mid and reason < 128, 'wildcard_publication_not_completed')
        self.log.record('publication_completed', qos=qos, packet_id=mid if qos else None,
                        client_receipt_id=mid, completion_scope='local-write-only' if qos == 0 else 'MQTT acknowledgement')
        return mid

    async def subscribe(self, values):
        from paho.mqtt.subscribeoptions import SubscribeOptions as PahoOptions
        values = requests(values)
        code, identifier = self.client.subscribe([(value.filter, PahoOptions(
            qos=value.qos, noLocal=value.no_local, retainAsPublished=value.retain_as_published,
            retainHandling=value.retain_handling)) for value in values])
        require(code == self.mqtt.MQTT_ERR_SUCCESS, 'paho_wildcard_subscribe_submission')
        actual, reasons = await asyncio.to_thread(self._get, self.subacks)
        self.log.record('subscription_options', subscriptions=[asdict(value) for value in values])
        require(actual == identifier and reasons == tuple(min(value.qos, 1) for value in values),
                'wildcard_public_subscription_refused')
        return identifier

    async def unsubscribe(self, filters):
        completed = asyncio.get_running_loop().create_future()
        loop = asyncio.get_running_loop()
        def received(_client, _data, mid, reasons, _properties):
            row = (mid, tuple(reason.value for reason in reasons))
            loop.call_soon_threadsafe(completed.set_result, row)
        self.client.on_unsubscribe = received
        code, expected = self.client.unsubscribe(list(filters))
        require(code == self.mqtt.MQTT_ERR_SUCCESS, 'paho_wildcard_unsubscribe_submission')
        actual, reasons = await asyncio.wait_for(completed, timeout=TIMEOUT)
        self.log.record('unsuback', packet_id=actual, filters=list(filters), reasons=list(reasons))
        require(actual == expected and all(code in (0, 0x11) for code in reasons),
                'wildcard_public_unsubscribe_refused')
        return reasons

    async def receive(self, payload=None, qos=None, *, acknowledge=True):
        row = await asyncio.to_thread(self._get, self.messages)
        require(payload is None or row['payload_sha256'] == hashlib.sha256(payload).hexdigest(),
                'wildcard_payload_mismatch')
        require(qos is None or row['qos'] == qos, 'wildcard_qos_mismatch')
        if row['qos'] and acknowledge:
            require(self.client.ack(row['packet_id'], row['qos']) == self.mqtt.MQTT_ERR_SUCCESS,
                    'paho_wildcard_ack_submission')
            self.log.record('puback_submitted', packet_id=row['packet_id'])
        return row

    async def no_message(self, timeout=.15):
        try:
            row = await asyncio.to_thread(self.messages.get, True, timeout)
        except queue.Empty:
            if self.failure:
                raise ReceiptFailure(self.failure)
            return
        raise ReceiptFailure('wildcard_unexpected_or_duplicate_delivery', str(row))


def mosquitto_paths():
    directory = os.environ.get('MQTTIUMD_MOSQUITTO_BIN_DIR')
    result = {}
    for name in ('mosquitto_pub', 'mosquitto_sub'):
        path = Path(directory) / name if directory else Path(shutil.which(name) or '')
        if not path.is_file() or not os.access(path, os.X_OK):
            raise RuntimeError('Mosquitto CLI required; set MQTTIUMD_MOSQUITTO_BIN_DIR or PATH')
        result[name] = path.resolve()
    return result


def mosquitto_identity():
    result = {}
    for name, path in mosquitto_paths().items():
        help_result = subprocess.run([str(path), '--help'], capture_output=True, text=True, timeout=3)
        version = next((line for line in (help_result.stdout + help_result.stderr).splitlines()
                        if re.match(name + r' version 2\.', line)), None)
        require(version is not None, 'mosquitto_version_unidentified')
        ldd = subprocess.run(['ldd', str(path)], capture_output=True, text=True, timeout=3)
        require(ldd.returncode == 0 and 'not found' not in ldd.stdout, 'mosquitto_library_unavailable')
        libraries = []
        for line in ldd.stdout.splitlines():
            candidate = line.split('=>', 1)[1].strip().split(' ', 1)[0] if '=>' in line else line.strip().split(' ', 1)[0]
            library = Path(candidate)
            if library.is_absolute() and library.is_file():
                libraries.append({'path': str(library.resolve()),
                                  'sha256': hashlib.sha256(library.read_bytes()).hexdigest()})
        result[name] = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                        'version': version, 'help_exit_status': help_result.returncode,
                        'libraries': libraries}
    return result


class MosquittoReceiver:
    """Actual CLI process; stdout verifies what the client delivered locally."""
    def __init__(self, address, client_id, filters, *, qos, count, case):
        if not 1 <= len(filters) <= 8 or not 1 <= count <= 32:
            raise ValueError('bounded Mosquitto subscription required')
        self.identity = mosquitto_identity()['mosquitto_sub']
        self.command = [self.identity['path'], '-V', 'mqttv5', '-h', address[0], '-p', str(address[1]),
                        '-i', client_id, '-c', '-x', str(INFINITE), '-q', str(qos),
                        '-C', str(count), '-W', '15', '-F', '%j']
        for value in filters:
            self.command += ['-t', value]
        self.stdout_path, self.stderr_path = case / 'mosquitto.stdout', case / 'mosquitto.stderr'
        self.stdout, self.stderr = self.stdout_path.open('xb'), self.stderr_path.open('xb')
        self.process = subprocess.Popen(self.command, stdout=self.stdout, stderr=self.stderr)
        self.count, self.closed = count, False

    async def finish(self):
        status = await asyncio.to_thread(self.process.wait, 18)
        self.stdout.close(); self.stderr.close(); self.closed = True
        require(status == 0, 'mosquitto_subscriber_failed', self.stderr_path.read_text()[-256:])
        data = self.stdout_path.read_bytes()
        require(len(data) <= 65536, 'mosquitto_stdout_budget')
        messages = [json.loads(line) for line in data.splitlines()]
        require(len(messages) == self.count, 'mosquitto_wrong_message_count')
        return {'client': 'mosquitto_sub', 'identity': self.identity, 'command': self.command,
                'pid': self.process.pid, 'status': status, 'messages': messages,
                'scope': 'actual CLI stdout, independently corroborated by the packet tap'}

    def close(self):
        if self.closed:
            return
        if self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.kill(); self.process.wait(timeout=3)
        self.stdout.close(); self.stderr.close(); self.closed = True


async def mosquitto_publish(address, client_id, topic, payload, qos, case):
    identity = mosquitto_identity()['mosquitto_pub']
    path = case / 'mosquitto-payload.bin'; path.write_bytes(payload)
    command = [identity['path'], '-V', 'mqttv5', '-h', address[0], '-p', str(address[1]),
               '-i', client_id, '-c', '-x', str(INFINITE), '-q', str(qos), '-t', topic, '-f', str(path)]
    process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE,
                                                  stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=8)
    except BaseException:
        if process.returncode is None:
            process.kill(); await process.wait()
        raise
    require(len(stdout) + len(stderr) <= 65536, 'mosquitto_publisher_output_budget')
    require(process.returncode == 0, 'mosquitto_publisher_failed', stderr.decode(errors='replace')[-256:])
    return {'client': 'mosquitto_pub', 'identity': identity, 'command': command,
            'pid': process.pid, 'status': process.returncode,
            'payload_sha256': hashlib.sha256(payload).hexdigest(),
            'completion_scope': 'local-write-only' if qos == 0 else 'MQTT acknowledgement'}
