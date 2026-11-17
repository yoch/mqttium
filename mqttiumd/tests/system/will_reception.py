#!/usr/bin/env python3
"""Finite actual-process Will reception; system evidence, never qualification."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from external_mqttium import client_identity
from external_will import MqttiumWill, PahoWill, PacketTap
from external_wildcard import Subscription
from pubsub_probe import durable
from tcp_probe import FrameDecoder, Observations, ReceiptFailure, require, write_json
from tcp_reception import Broker, writer_available
from will_probe import assert_will_frame, decode_will_connect


async def peer(kind, broker, case, label, *, will=None, expiry=0xFFFFFFFF,
               client_id=None, present=False):
    tap = PacketTap(broker.address, Observations(case / f'{label}.wire.jsonl'))
    constructor = MqttiumWill if kind == 'mqttium' else PahoWill
    client = constructor(tap.address, client_id or label, will=will, expiry=expiry,
                         events_path=case / f'{label}.client.jsonl')
    try:
        await client.open(present)
    except BaseException:
        await client.disconnect()
        tap.close()
        raise
    return client, tap


async def close_peers(values):
    for client, tap in values:
        try:
            await client.disconnect()
        finally:
            tap.close()


class Sensor:
    """An actual child PID; the parent, not the client, causes transport loss."""
    def __init__(self, kind, address, case, label, topic, observations, *, expiry=0xFFFFFFFF):
        self.case, self.observations, self.label = case, observations, label
        self.ready = case / f'{label}.ready.json'
        self.finish = case / f'{label}.finish'
        self.result = case / f'{label}.result.json'
        self.command = [sys.executable, str(Path(__file__).with_name('external_will.py')),
                        '--client', kind, '--host', address[0], '--port', str(address[1]),
                        '--client-id', label, '--topic', topic, '--expiry', str(expiry),
                        '--events', str(case / f'{label}.client.jsonl'), '--ready', str(self.ready),
                        '--finish', str(self.finish), '--result', str(self.result)]
        self.stdout = (case / f'{label}.stdout').open('xb')
        self.stderr = (case / f'{label}.stderr').open('xb')
        self.process = subprocess.Popen(self.command, stdout=self.stdout, stderr=self.stderr)
        self.closed = False
        self.observations.record('sensor_process_started', pid=self.process.pid, command=self.command)

    async def wait_ready(self):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.ready.exists():
                require(self.ready.stat().st_size <= 16384, 'will_sensor_result_budget')
                row = json.loads(self.ready.read_text())
                require(row['pid'] == self.process.pid and row['status'] == 'ready'
                        and row['reason_code'] == 0, 'will_sensor_not_established')
                self.observations.record('sensor_ready_retained', result=row)
                return row
            if self.process.poll() is not None:
                require(self.result.is_file(), 'will_sensor_exited_without_result')
                require(self.result.stat().st_size <= 65536, 'will_sensor_result_budget')
                row = json.loads(self.result.read_text())
                raise ReceiptFailure(row.get('code', 'will_sensor_failed'), row.get('detail', ''))
            await asyncio.sleep(.005)
        raise ReceiptFailure('will_sensor_readiness_not_reached')

    async def stop(self, *, killed):
        require(self.process.poll() is None, 'will_sensor_died_before_requested_signal')
        if killed:
            os.kill(self.process.pid, signal.SIGKILL)
            self.observations.record('sensor_signal_sent', pid=self.process.pid, signal=9)
        else:
            self.finish.touch(exist_ok=False)
            self.observations.record('sensor_normal_disconnect_requested', pid=self.process.pid)
        status = await asyncio.to_thread(self.process.wait, 8)
        self.observations.record('sensor_process_wait', pid=self.process.pid, status=status,
                                 requested='SIGKILL' if killed else 'MQTT normal DISCONNECT')
        require(status == (-9 if killed else 0), 'will_sensor_wrong_wait_status', str(status))
        require(not Path(f'/proc/{self.process.pid}').exists(), 'will_sensor_survived')
        self.stdout.close()
        self.stderr.close()
        self.closed = True
        return {'pid': self.process.pid, 'wait_status': status, 'command': self.command,
                'requested': 'SIGKILL' if killed else 'MQTT normal DISCONNECT'}

    def cleanup(self):
        if self.closed:
            return
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=3)
        self.observations.record('sensor_process_wait', pid=self.process.pid,
                                 status=self.process.returncode, requested='receipt_cleanup')
        self.stdout.close()
        self.stderr.close()
        self.closed = True


def client_frames(tap):
    data = tap.observations.path.read_bytes()
    require(len(data) <= 16 * 1024 * 1024, 'will_wire_observation_budget')
    decoders, frames = {}, []
    for line in data.splitlines():
        row = json.loads(line)
        if row['kind'] != 'proxy_client_tx':
            continue
        require('bytes' in row, 'will_connect_witness_was_not_complete')
        decoder = decoders.setdefault(row['connection'], FrameDecoder(65536))
        frames.extend(decoder.feed(bytes.fromhex(row['bytes'])))
    for decoder in decoders.values():
        decoder.finish()
    return frames


async def drained(broker, *, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = durable(broker.db)
        if all(state['counts'][name] == 0 for name in ('obligations', 'outbound', 'inbound')):
            return state
        await asyncio.sleep(.005)
    raise ReceiptFailure('will_obligations_not_durably_finished')


async def public_sensor(binary, case, sensor_kind, dashboard_kind):
    """The first end-to-end milestone, with explicit client and wire facts."""
    broker = Broker(binary, case)
    clients, taps, sensors = [], [], []
    try:
        broker.ready()
        dashboard, dashboard_tap = await peer(dashboard_kind, broker, case, 'dashboard')
        clients.append((dashboard, dashboard_tap))
        await dashboard.subscribe((Subscription('atelier/+/status', 1),))
        suback = await dashboard_tap.subscribed(1, (1,))
        topic = 'atelier/capteur-1/status'
        sensor_tap = PacketTap(broker.address, Observations(case / 'sensor.wire.jsonl'))
        taps.append(sensor_tap)
        sensor = Sensor(sensor_kind, sensor_tap.address, case, 'capteur-1', topic, broker.observations)
        sensors.append(sensor)
        sensor_ready = await sensor.wait_ready()
        online = await dashboard.receive(b'online', 1, False)
        stopped = await sensor.stop(killed=True)
        offline = await dashboard.receive(b'offline', 1, False)
        await drained(broker)
        incoming = client_frames(sensor_tap)
        connect = [frame for frame in incoming if frame.kind == 0x10]
        require(len(connect) == 1, 'will_sensor_reconnected')
        observed_connect = decode_will_connect(connect[0])
        require(observed_connect['will_qos'] == 1 and not observed_connect['will_retain']
                and observed_connect['will_delay_interval'] == 0, 'will_client_profile_mismatch')
        require(not any(frame.kind == 0xE0 for frame in incoming),
                'will_sigkill_was_voluntary_disconnect')
        values = dashboard_tap.publications(b'offline')
        require(len(values) == 1, 'will_missing_or_duplicate_network_admission')
        offline_wire = assert_will_frame(values[0], topic)

        new, new_tap = await peer(sensor_kind, broker, case, 'new-dashboard')
        clients.append((new, new_tap))
        await new.subscribe((Subscription(topic, 1),))
        replay = await new.receive(b'online', 1, True)
        require(len(new_tap.publications(b'online')) == 1 and not new_tap.publications(b'offline'),
                'will_incorrectly_replaced_retained_online')

        normal_tap = PacketTap(broker.address, Observations(case / 'normal-sensor.wire.jsonl'))
        taps.append(normal_tap)
        normal = Sensor(sensor_kind, normal_tap.address, case, 'normal-capteur', topic, broker.observations)
        sensors.append(normal)
        await normal.wait_ready()
        await dashboard.receive(b'online', 1, False)
        await new.receive(b'online', 1, False)
        normal_stopped = await normal.stop(killed=False)
        await dashboard.no_message(.2)
        await new.no_message(.2)
        normal_frames = client_frames(normal_tap)
        disconnects = [frame for frame in normal_frames if frame.kind == 0xE0]
        require(len(disconnects) == 1 and (not disconnects[0].body or disconnects[0].body[0] == 0),
                'will_normal_disconnect_not_observed')
        require(len(dashboard_tap.publications(b'offline')) == 1,
                'will_published_despite_normal_disconnect')
        state = await drained(broker)
        write_json(case / 'durable-final.json', state)
        client_results = [client.result for client, _ in clients]
        await close_peers(clients)
        clients.clear()
        broker.stop()
        writer_available(broker.db)
        return {'group': 'A-public-sensor', 'status': 'PASS', 'sensor_client': sensor_kind,
                'dashboard_client': dashboard_kind, 'sensor_ready': sensor_ready,
                'sensor_connect_observed': observed_connect, 'suback': suback,
                'sigkill': stopped, 'normal_stop': normal_stopped, 'online': online,
                'offline': offline, 'offline_wire': offline_wire, 'retained_online_replay': replay,
                'clients': client_results, 'durable_counts': state['counts'],
                'broker_pid': broker.process.pid, 'broker_status': broker.process.returncode,
                'scope': 'actual TCP processes and public clients; not oracle qualification'}
    finally:
        for sensor in sensors:
            sensor.cleanup()
        try:
            await close_peers(clients)
        finally:
            for tap in taps:
                tap.close()
            broker.cleanup()


async def main_async(arguments):
    arguments.output.mkdir(parents=True, exist_ok=False)
    binary = arguments.binary.resolve()
    require(binary.is_file(), 'will_broker_binary_missing')
    result = {'source_revision': arguments.source, 'binary_path': str(binary),
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'features': [], 'scope': 'normal product system test; not qualifying',
              'client_source': client_identity(), 'cases': [], 'status': 'RUNNING'}
    try:
        roles = [('mqttium', 'paho'), ('paho', 'mqttium')]
        if arguments.roles != 'both':
            roles = [roles[0 if arguments.roles == 'mqttium-sensor' else 1]]
        for sensor_kind, dashboard_kind in roles:
            case = arguments.output / f'A-{sensor_kind}-to-{dashboard_kind}'
            value = await public_sensor(binary, case, sensor_kind, dashboard_kind)
            write_json(case / 'result.json', value)
            result['cases'].append(value)
        result['status'] = 'PASS'
        return 0
    except Exception as error:
        result.update(status='FAIL', code=getattr(error, 'code', type(error).__name__),
                      detail=str(error)[:512])
        return 1
    finally:
        write_json(arguments.output / 'result.json', result)
        print(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--roles', choices=('both', 'mqttium-sensor', 'paho-sensor'), default='both')
    arguments = parser.parse_args()
    return asyncio.run(main_async(arguments))


if __name__ == '__main__':
    raise SystemExit(main())
