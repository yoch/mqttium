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
import socket
import struct
import subprocess
import sys
import time

from external_mqttium import client_identity
from external_will import MqttiumWill, PahoWill, PacketTap
from external_wildcard import Subscription
from pubsub_probe import durable, session_key
from tcp_probe import FrameDecoder, Observations, ReceiptFailure, operation_hex, packet, require, write_json
from tcp_reception import Broker, expect_rejection, writer_available
from will_probe import WillWire, assert_will_frame, decode_will_connect, will_connect_packet


async def peer(kind, broker, case, label, *, will=None, expiry=0xFFFFFFFF,
               client_id=None, present=False):
    tap = PacketTap(broker.address, Observations(case / f'{label}.wire.jsonl'))
    constructor = MqttiumWill if kind == 'mqttium' else PahoWill
    client = constructor(tap.address, client_id or label, will=will, expiry=expiry,
                         events_path=case / f'{label}.client.jsonl')
    try:
        await client.open(present)
    except BaseException:
        try:
            await client.disconnect()
        finally:
            tap.close()
        raise
    return client, tap


async def close_peers(values):
    failures = []
    for client, tap in values:
        try:
            await client.disconnect()
        except Exception as error:
            failures.append(error)
        finally:
            try:
                tap.close()
            except Exception as error:
                failures.append(error)
    if failures:
        raise failures[0]


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


async def until_state(broker, predicate, code, *, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = durable(broker.db)
        if predicate(state):
            return state
        await asyncio.sleep(.005)
    raise ReceiptFailure(code)


async def triggers(binary, case):
    """Independent TCP EOF/RST/KeepAlive/protocol and DISCONNECT witnesses."""
    broker = Broker(binary, case)
    clients, wires = [], []
    try:
        broker.ready()
        dashboard, tap = await peer('mqttium', broker, case, 'trigger-dashboard')
        clients.append((dashboard, tap))
        await dashboard.subscribe((Subscription('will/triggers/+', 1),))
        await tap.subscribed(1, (1,))
        rows = []
        for name in ('eof', 'rst', 'keep_alive', 'protocol_error', 'disconnect04', 'disconnect00'):
            topic = 'will/triggers/' + name
            payload = ('offline:' + name).encode()
            wire = WillWire(broker.address, broker.observations, 'trigger-' + name)
            wires.append(wire)
            await asyncio.to_thread(wire.will_connect, 'trigger-' + name, topic, payload,
                                    expiry=0, keep_alive=1 if name == 'keep_alive' else 10)
            observed = {'case': name, 'connected_monotonic_ns': str(time.monotonic_ns()),
                        'requested_expiry': 0}
            if name == 'eof':
                wire.close()
            elif name == 'rst':
                wire.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                wire.close()
            elif name == 'protocol_error':
                wire.send(b'\xc1\x00')  # PINGREQ's reserved flag is invalid.
                await asyncio.to_thread(expect_rejection, wire)
            elif name == 'disconnect04':
                wire.send(packet(0xE0, b'\x04\x00'))
            elif name == 'disconnect00':
                wire.send(b'\xe0\x00')
            # keep_alive deliberately sends nothing after its accepted CONNECT.
            if name == 'disconnect00':
                state = await until_state(broker, lambda value: not any(
                    row['client_id'] == ('trigger-' + name).encode().hex()
                    for row in value['tables']['sessions']), 'will_normal_session_not_closed')
                await dashboard.no_message(.2)
                require(not tap.publications(payload), 'will_published_despite_normal_disconnect')
                observed['no_publication'] = True
            else:
                received = await dashboard.receive(payload, 1, False)
                frames = tap.publications(payload)
                require(len(frames) == 1, 'will_trigger_missing_or_duplicate', name)
                observed['received'] = received
                observed['wire'] = assert_will_frame(frames[0], topic, payload)
                state = await drained(broker)
            observed['completed_monotonic_ns'] = str(time.monotonic_ns())
            observed['counts'] = state['counts']
            rows.append(observed)
            wire.close()

        # All refusals happen before establishment; none may arm a Will.
        refusals = []
        for name, options in (('qos0', {'qos': 0}), ('qos2', {'qos': 2}),
                              ('retained', {'retain': True}), ('delay1', {'delay': 1}),
                              ('expiry', {'will_properties': b'\x02\0\0\0\x01'})):
            wire = WillWire(broker.address, broker.observations, 'unsupported-' + name)
            wires.append(wire)
            wire.send(will_connect_packet('unsupported-' + name, 'will/triggers/refused',
                                          ('refused:' + name).encode(), **options))
            await asyncio.to_thread(expect_rejection, wire, before_connect=True)
            wire.close()
            await dashboard.no_message(.08)
            require(not tap.publications(('refused:' + name).encode()),
                    'will_published_for_refused_connection', name)
            refusals.append(name)
        # This independent client keeps progressing after all closure paths.
        await dashboard.publish('will/control', b'control-after-trigger-matrix', 1, retain=False)
        state = await drained(broker)
        write_json(case / 'durable-final.json', state)
        await close_peers(clients)
        clients.clear()
        broker.stop()
        writer_available(broker.db)
        return {'group': 'B-triggers', 'status': 'PASS', 'cases': rows,
                'refused_profiles': refusals, 'durable_counts': state['counts'],
                'broker_pid': broker.process.pid, 'broker_status': broker.process.returncode}
    finally:
        for wire in wires:
            wire.close()
        try:
            await close_peers(clients)
        finally:
            broker.cleanup()


async def shared_responsibilities(binary, case):
    """A producer's expiry0 cleanup preserves both recipients, including offline."""
    broker, restarted = Broker(binary, case), None
    clients, source = [], None
    try:
        broker.ready()
        dashboard, dashboard_tap = await peer('mqttium', broker, case, 'shared-dashboard')
        clients.append((dashboard, dashboard_tap))
        await dashboard.subscribe((Subscription('will/shared/#', 1),))
        offline, offline_tap = await peer('paho', broker, case, 'shared-offline')
        await offline.subscribe((Subscription('will/shared/#', 1),))
        await offline_tap.subscribed(1, (1,))
        await close_peers(((offline, offline_tap),))
        before = durable(broker.db)
        offline_id = session_key(before, 'shared-offline')
        source = WillWire(broker.address, broker.observations, 'shared-source')
        source.will_connect('shared-source', 'will/shared/status', b'shared-offline', expiry=0)
        source.close()
        received = await dashboard.receive(b'shared-offline', 1, False)
        retained = await until_state(broker, lambda state: len(state['tables']['obligations']) == 1
                                    and not state['tables']['outbound'] and not any(
                                        row['client_id'] == b'shared-source'.hex()
                                        for row in state['tables']['sessions']),
                                    'will_source_cleanup_destroyed_other_responsibility')
        obligation = retained['tables']['obligations'][0]
        require(obligation['target'] == offline_id, 'will_offline_obligation_wrong_target')
        require(len(retained['tables']['publications']) == len(retained['tables']['payloads']) == 1,
                'will_shared_payload_missing_or_duplicated')
        write_json(case / 'durable-before-restart.json', retained)
        await close_peers(clients)
        clients.clear()
        broker.stop(killed=True)
        restarted = Broker(binary, case, db=broker.db, label='restarted')
        restarted.ready()
        resumed, tap = await peer('paho', restarted, case, 'shared-resumed',
                                  client_id='shared-offline', present=True)
        clients.append((resumed, tap))
        # No SUBSCRIBE: the recovered durable session is the only routing fact.
        restored = await resumed.receive(b'shared-offline', 1, False)
        frames = tap.publications(b'shared-offline')
        require(len(frames) == 1, 'will_offline_replay_missing_or_duplicate')
        assert_will_frame(frames[0], 'will/shared/status', b'shared-offline')
        final = await drained(restarted)
        require(all(final['counts'][name] == 0 for name in ('publications', 'payloads')),
                'will_last_reference_not_released')
        write_json(case / 'durable-final.json', final)
        await close_peers(clients)
        clients.clear()
        restarted.stop()
        writer_available(broker.db)
        return {'group': 'D-shared-responsibilities', 'status': 'PASS',
                'online_received': received, 'offline_received': restored,
                'offline_session': offline_id, 'obligation_before_restart': obligation,
                'source_session_deleted': True, 'before_counts': retained['counts'],
                'final_counts': final['counts'], 'original_broker_pid': broker.process.pid,
                'original_broker_status': broker.process.returncode,
                'restarted_broker_pid': restarted.process.pid,
                'restarted_broker_status': restarted.process.returncode}
    finally:
        if source:
            source.close()
        try:
            await close_peers(clients)
        finally:
            broker.cleanup()
            if restarted:
                restarted.cleanup()


async def takeover_order(binary, case):
    """Delay0 publishes the old occurrence; its late physical Close cannot fire B."""
    broker = Broker(binary, case)
    clients, wires = [], []
    try:
        broker.ready()
        dashboard, tap = await peer('paho', broker, case, 'takeover-dashboard')
        clients.append((dashboard, tap))
        await dashboard.subscribe((Subscription('will/takeover/#', 1),))
        first = WillWire(broker.address, broker.observations, 'takeover-old')
        wires.append(first)
        first.will_connect('takeover-sensor', 'will/takeover/old', b'old-attachment')
        second = WillWire(broker.address, broker.observations, 'takeover-new')
        wires.append(second)
        second.will_connect('takeover-sensor', 'will/takeover/new', b'new-attachment',
                            expected_present=True)
        old_received = await dashboard.receive(b'old-attachment', 1, False)
        first.close()
        # The replacement transport remains usable despite old socket cleanup.
        second.send(b'\xc0\x00')
        require(second.wait(0xD0).body == b'', 'will_takeover_new_transport_not_progressing')
        second.send(b'\xe0\x00')
        await dashboard.no_message(.2)
        require(len(tap.publications(b'old-attachment')) == 1 and not tap.publications(b'new-attachment'),
                'will_old_close_fired_new_attachment')
        state = await drained(broker)
        write_json(case / 'durable-final.json', state)
        await close_peers(clients)
        clients.clear()
        broker.stop()
        return {'group': 'E-takeover', 'status': 'PASS', 'old_received': old_received,
                'old_occurrences': 1, 'new_normal_disconnect_occurrences': 0,
                'durable_counts': state['counts'], 'broker_pid': broker.process.pid,
                'broker_status': broker.process.returncode}
    finally:
        for wire in wires:
            wire.close()
        try:
            await close_peers(clients)
        finally:
            broker.cleanup()


async def full_session_replacement(binary, case):
    """CS1 replaces its own slot128 after the old Delay0 Will is admitted."""
    broker = Broker(binary, case)
    wires, source, replacement = [], None, None
    topic, payload = 'will/lifecycle/full/status', b'old-will-before-full-capacity-replacement'
    try:
        broker.ready()
        source = WillWire(broker.address, broker.observations, 'full-source')
        wires.append(source)
        source.will_connect('full-source', topic, payload)
        source_before = durable(broker.db)
        source_id = session_key(source_before, 'full-source')
        old_will = source_before['tables']['wills'][0]['id']
        for index in range(127):
            wire = WillWire(broker.address, broker.observations, f'full-offline-{index:03}')
            wires.append(wire)
            wire.session_connect(f'full-offline-{index:03}', expected_present=False)
            if index == 0:
                wire.subscribe_options(1, (Subscription('will/lifecycle/#', 1),))
            marker = max(int(row['seq']) for row in broker.events())
            wire.send(b'\xe0\x00')
            wire.close()
            await asyncio.to_thread(broker.wait_event, 'connection_closed', after=marker)
        full = durable(broker.db)
        require(full['counts']['sessions'] == 128
                and int(full['tables']['meta'][0]['max_sessions'], 16) == 128,
                'will_full_capacity_was_not_reached_with_original_quota')
        witness_id = session_key(full, 'full-offline-000')
        overflow = WillWire(broker.address, broker.observations, 'full-overflow')
        wires.append(overflow)
        overflow.send(will_connect_packet('full-overflow', topic, b'unaccepted-will'))
        await asyncio.to_thread(expect_rejection, overflow, before_connect=True)
        overflow.close()
        refused = durable(broker.db)
        require(refused['counts']['sessions'] == 128 and refused['tables']['wills'] == full['tables']['wills']
                and not refused['tables']['obligations'], 'will_full_refusal_evicted_or_armed_new_identity')
        marker = max(int(row['seq']) for row in broker.events())
        replacement = WillWire(broker.address, broker.observations, 'full-replacement')
        wires.append(replacement)
        replacement.will_connect('full-source', topic, b'new-will-must-be-cancelled',
                                 clean_start=True, expected_present=False)
        changed = durable(broker.db)
        require(changed['counts']['sessions'] == 128
                and session_key(changed, 'full-source') != source_id
                and session_key(changed, 'full-offline-000') == witness_id,
                'will_clean_start_at_full_capacity_reused_or_evicted_identity')
        admitted = [row for row in broker.events() if row['kind'] == 'will_admitted'
                    and operation_hex(row['will']) == old_will]
        connacks = [row for row in broker.events() if int(row['seq']) > marker
                    and row['kind'] == 'frame_queued' and row['details'].get('packet_kind') == 'CONNACK']
        require(len(admitted) == len(connacks) == 1 and int(admitted[0]['seq']) < int(connacks[0]['seq']),
                'will_replacement_connack_preceded_old_will_admission')
        require(len(changed['tables']['obligations']) == 1
                and changed['tables']['obligations'][0]['target'] == witness_id,
                'will_clean_start_lost_other_session_responsibility')
        source.close()  # Late old transport cleanup, after the new CONNACK.
        replacement.send(b'\xe0\x00')
        replacement.close()
        await until_state(broker, lambda state: not state['tables']['wills'],
                          'will_new_full_capacity_attachment_was_not_cancelled')
        resumed = WillWire(broker.address, broker.observations, 'full-witness-resumed')
        wires.append(resumed)
        resumed.session_connect('full-offline-000', expected_present=True)
        value = resumed.next_publication(timeout=8)
        received = assert_will_frame(value, topic, payload)
        resumed.acknowledge(value)
        resumed.no_publication(.1, code='will_new_attachment_published_after_normal_close')
        final = await drained(broker)
        require(final['counts']['sessions'] == 128 and all(final['counts'][name] == 0
                for name in ('wills', 'publications', 'payloads')), 'will_full_capacity_cleanup_or_eviction_wrong')
        write_json(case / 'full.sql.json', full)
        write_json(case / 'replaced.sql.json', changed)
        write_json(case / 'final.sql.json', final)
        resumed.send(b'\xe0\x00')
        resumed.close()
        broker.stop()
        return {'criterion': 'clean-start-at-full128', 'status': 'PASS', 'sessions': 128,
                'old_source_session': source_id, 'new_source_session': session_key(changed, 'full-source'),
                'witness_session': witness_id, 'old_will': old_will, 'admission': admitted[0],
                'replacement_connack': connacks[0], 'received': received,
                'final_counts': final['counts'], 'broker_pid': broker.process.pid,
                'broker_status': broker.process.returncode}
    finally:
        for wire in wires:
            wire.close()
        broker.cleanup()


async def finite_source(binary, case):
    """Finite source expiry leaves the admitted Will due to another session."""
    broker, restart = Broker(binary, case), None
    wires = []
    topic, payload = 'will/lifecycle/finite/status', b'will-survives-finite-producer-expiry'
    try:
        broker.ready()
        target = WillWire(broker.address, broker.observations, 'finite-offline')
        wires.append(target)
        target.session_connect('finite-offline', expected_present=False)
        target.subscribe_options(1, (Subscription('will/lifecycle/finite/#', 1),))
        target.send(b'\xe0\x00')
        target.close()
        source = WillWire(broker.address, broker.observations, 'finite-source')
        wires.append(source)
        source.will_connect('finite-source', topic, payload, expiry=1)
        initial = durable(broker.db)
        source_id = session_key(initial, 'finite-source')
        target_id = session_key(initial, 'finite-offline')
        will = initial['tables']['wills'][0]['id']
        await asyncio.sleep(1.1)
        source.send(b'\xc0\x00')
        require(source.wait(0xD0).body == b'', 'will_active_finite_source_expired')
        active = durable(broker.db)
        require(session_key(active, 'finite-source') == source_id and not active['tables']['obligations']
                and len(active['tables']['wills']) == 1 and active['tables']['wills'][0]['state'] == 1,
                'will_active_finite_source_expired_or_published')
        source.close()
        retained = await until_state(broker, lambda state: not any(
            row['id'] == source_id for row in state['tables']['sessions'])
            and len(state['tables']['obligations']) == 1 and not state['tables']['wills'],
            'will_finite_source_expiry_destroyed_other_responsibility', timeout=8)
        obligation = retained['tables']['obligations'][0]
        require(obligation['target'] == target_id and len(retained['tables']['payloads']) == 1,
                'will_finite_source_expiry_released_other_payload')
        write_json(case / 'source-expired.sql.json', retained)
        inode = broker.db.stat().st_ino
        broker.stop(killed=True)
        writer_available(broker.db)
        restart = Broker(binary, case, db=broker.db, label='restarted')
        restart.ready()
        require(restart.db.stat().st_ino == inode, 'will_finite_database_replaced')
        resumed = WillWire(restart.address, restart.observations, 'finite-resumed')
        wires.append(resumed)
        resumed.session_connect('finite-offline', expected_present=True)
        value = resumed.next_publication(timeout=8)
        received = assert_will_frame(value, topic, payload)
        before_ack = durable(restart.db)
        require(before_ack['tables']['obligations'] == [obligation],
                'will_finite_recovery_recreated_other_obligation')
        resumed.acknowledge(value)
        final = await drained(restart)
        require(all(final['counts'][name] == 0 for name in ('wills', 'publications', 'payloads')),
                'will_finite_last_reference_not_released')
        resumed.send(b'\xe0\x00')
        resumed.close()
        restart.stop()
        write_json(case / 'final.sql.json', final)
        return {'criterion': 'finite-source-expiry', 'status': 'PASS', 'source_session': source_id,
                'target_session': target_id, 'will': will, 'active_beyond_interval': True,
                'obligation_after_source_expiry': obligation, 'received': received,
                'final_counts': final['counts'], 'killed_pid': broker.process.pid,
                'kill_status': broker.process.returncode, 'restarted_pid': restart.process.pid,
                'restart_status': restart.process.returncode, 'same_file': True}
    finally:
        for wire in wires:
            wire.close()
        broker.cleanup()
        if restart:
            restart.cleanup()


async def takeover(binary, case):
    first = await takeover_order(binary, case / 'same-session-takeover')
    first['clean_start_at_full_capacity'] = await full_session_replacement(binary, case / 'full-capacity')
    first['finite_source_expiry'] = await finite_source(binary, case / 'finite-source')
    return first


async def fanout_pressure(binary, case):
    """Seventeen eligible sessions defer the complete Will; sixteen later progress."""
    broker = Broker(binary, case)
    targets, source = [], None
    levels = [f'level{index:02}' for index in range(16)]
    payload, topic = b'pending-will-survives-fanout-pressure', 'will/pressure/' + '/'.join(levels)
    try:
        broker.ready()
        for index in range(17):
            wire = WillWire(broker.address, broker.observations, f'pressure-target-{index:02}')
            targets.append(wire)
            wire.session_connect(f'pressure-target-{index:02}', expected_present=False)
            # The per-literal-filter cap16 remains useful. Distinct overlapping
            # filters exercise the aggregate fanout limit without evading it.
            pattern = 'will/pressure/#' if index == 16 else 'will/pressure/' + '/'.join(
                '+' if at == index else level for at, level in enumerate(levels))
            wire.subscribe_options(1, (Subscription(pattern, 1),))
            marker = max(int(event['seq']) for event in broker.events())
            wire.send(b'\xe0\x00')
            wire.close()
            await asyncio.to_thread(broker.wait_event, 'connection_closed', after=marker)
        source = WillWire(broker.address, broker.observations, 'pressure-source')
        source.will_connect('pressure-source', topic, payload, expiry=0)
        source.close()
        pending = await until_state(broker, lambda state: len(state['tables']['wills']) == 1
                                   and state['tables']['wills'][0]['state'] == 2,
                                   'will_pressure_intent_was_lost')
        pending_will = pending['tables']['wills'][0]
        deferred_fact = await asyncio.to_thread(broker.wait_event, 'will_deferred')
        require(operation_hex(deferred_fact['operation']) == pending_will['id']
                and deferred_fact['details'].get('code') == 'fanout_capacity'
                and deferred_fact['details'].get('durable_intention_preserved') is True,
                'will_pressure_refusal_not_observable')
        pending = durable(broker.db)
        require(all(pending['counts'][name] == 0 for name in
                    ('obligations', 'outbound', 'publications', 'payloads')),
                'will_fanout_was_silently_truncated')
        # Existing connection quota16 is unchanged. Most destinations remain
        # offline; one independently resumed client proves control progress.
        control = WillWire(broker.address, broker.observations, 'pressure-control')
        targets.append(control)
        control.session_connect('pressure-target-00', expected_present=True)
        control.no_publication(.05, code='will_partial_fanout_before_capacity')
        control.send(b'\xc0\x00')
        require(control.wait(0xD0).body == b'', 'will_pressure_blocked_ping')
        still_pending = durable(broker.db)
        require(still_pending['tables']['wills'] == [pending_will]
                and not still_pending['tables']['obligations'],
                'will_pressure_changed_or_discarded_intent')
        write_json(case / 'fanout17-pending.sql.json', still_pending)
        unsubscribe = WillWire(broker.address, broker.observations, 'pressure-unsubscribe')
        targets.append(unsubscribe)
        unsubscribe.session_connect('pressure-target-16', expected_present=True)
        unsubscribe.unsubscribe(2, ['will/pressure/#'])
        unsubscribe.no_publication(.04, code='will_unsubscribed_target_selected')
        unsubscribe.send(b'\xe0\x00')
        unsubscribe.close()
        deliveries = []
        for index in range(16):
            wire = control
            if index:
                wire = WillWire(broker.address, broker.observations, f'pressure-resumed-{index:02}')
                targets.append(wire)
                wire.session_connect(f'pressure-target-{index:02}', expected_present=True)
            value = wire.next_publication(timeout=8)
            deliveries.append(assert_will_frame(value, topic, payload))
            wire.acknowledge(value)
            wire.send(b'\xe0\x00')
            wire.close()
        final = await drained(broker)
        require(final['counts']['wills'] == 0 and all(final['counts'][name] == 0
                for name in ('publications', 'payloads')), 'will_pressure_resources_not_reclaimed')
        admitted = [event for event in broker.events() if event['kind'] == 'will_admitted']
        require(len(admitted) == 1 and operation_hex(admitted[0]['will']) == pending_will['id']
                and admitted[0]['details']['targets'] == 16,
                'will_pressure_recovery_changed_occurrence_or_fanout')
        deferred = [event for event in broker.events() if event['kind'] == 'will_deferred']
        require(deferred and any(operation_hex(event['operation']) == pending_will['id']
                and event['details'].get('code') == 'fanout_capacity'
                and event['details'].get('durable_intention_preserved') is True for event in deferred),
                'will_pressure_refusal_not_observable')
        write_json(case / 'durable-final.json', final)
        for wire in targets:
            wire.close()
        broker.stop()
        return {'group': 'E-fanout-pressure', 'status': 'PASS', 'initial_destinations': 17,
                'final_destinations': 16, 'pending_will': pending_will['id'],
                'retained_pending_counts': still_pending['counts'], 'deferred': deferred,
                'admitted': admitted[0], 'deliveries': deliveries, 'final_counts': final['counts'],
                'broker_pid': broker.process.pid, 'broker_status': broker.process.returncode}
    finally:
        for wire in [*targets, source]:
            if wire:
                wire.close()
        broker.cleanup()


async def no_local(binary, case):
    """No Local filters contributions, preserving an eligible QoS0 overlap."""
    broker = Broker(binary, case)
    clients, wires = [], []
    try:
        broker.ready()
        dashboard, tap = await peer('paho', broker, case, 'options-dashboard')
        clients.append((dashboard, tap))
        await dashboard.subscribe((Subscription('will/options/#', 1),))
        rows = []
        for overlap in (False, True):
            label = 'scope' if overlap else 'excluded'
            client_id = 'will-options-' + label
            topic, payload = 'will/options/' + label + '/status', ('offline-' + label).encode()
            source = WillWire(broker.address, broker.observations, client_id)
            wires.append(source)
            source.will_connect(client_id, topic, payload)
            values = [Subscription('will/options/' + label + '/#', 1, no_local=True)]
            if overlap:
                values.append(Subscription(topic, 0, no_local=False))
            source.subscribe_options(1, tuple(values))
            state = durable(broker.db)
            source_id = session_key(state, client_id)
            source.close()
            other_received = await dashboard.receive(payload, 1, False)
            pending = await until_state(broker, lambda state: state['counts']['wills'] == 0
                                        and not state['tables']['outbound'],
                                        'will_no_local_admission_not_finished')
            own = [row for row in pending['tables']['obligations'] if row['target'] == source_id]
            require(len(own) == int(overlap) and all(row['qos'] == 0 for row in own),
                    'will_no_local_contribution_or_max_qos_wrong')
            resumed = WillWire(broker.address, broker.observations, client_id + '-resumed')
            wires.append(resumed)
            resumed.session_connect(client_id, expected_present=True)
            own_received = None
            if overlap:
                value = resumed.next_publication(timeout=8)
                require(value.payload == payload and value.qos == 0 and not value.retain,
                        'will_no_local_applied_to_entire_session')
                own_received = value.observation()
            resumed.no_publication(.1, code='will_no_local_ignored')
            resumed.send(b'\xe0\x00')
            resumed.close()
            final = await drained(broker)
            rows.append({'overlapping_eligible_qos0': overlap, 'source_session': source_id,
                         'other_received': other_received, 'own_received': own_received,
                         'own_pending': own, 'counts': final['counts']})
        write_json(case / 'durable-final.json', final)
        await close_peers(clients)
        clients.clear()
        broker.stop()
        return {'group': 'E-no-local', 'status': 'PASS', 'cases': rows,
                'broker_pid': broker.process.pid, 'broker_status': broker.process.returncode}
    finally:
        for wire in wires:
            wire.close()
        try:
            await close_peers(clients)
        finally:
            broker.cleanup()


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
              'features': ['test-hooks'] if arguments.instrumented else [],
              'scope': ('instrumented' if arguments.instrumented else 'normal') +
                       ' product system test; not qualifying',
              'client_source': client_identity(), 'cases': [], 'status': 'RUNNING'}
    try:
        groups = arguments.groups.split(',')
        require(groups and len(groups) == len(set(groups))
                and set(groups) <= {'public', 'triggers', 'responsibilities', 'takeover', 'capacity', 'options'},
                'will_unknown_or_repeated_group')
        if 'public' in groups:
            roles = [('mqttium', 'paho'), ('paho', 'mqttium')]
            if arguments.roles != 'both':
                roles = [roles[0 if arguments.roles == 'mqttium-sensor' else 1]]
            for sensor_kind, dashboard_kind in roles:
                case = arguments.output / f'A-{sensor_kind}-to-{dashboard_kind}'
                value = await public_sensor(binary, case, sensor_kind, dashboard_kind)
                write_json(case / 'result.json', value)
                result['cases'].append(value)
        for group, action in (('triggers', triggers), ('responsibilities', shared_responsibilities),
                              ('takeover', takeover), ('capacity', fanout_pressure), ('options', no_local)):
            if group in groups:
                case = arguments.output / group
                value = await action(binary, case)
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
    parser.add_argument('--groups', default='public,triggers,responsibilities,takeover,capacity,options')
    parser.add_argument('--instrumented', action='store_true',
                        help='record test-hooks for an explicitly supplied development binary')
    arguments = parser.parse_args()
    return asyncio.run(main_async(arguments))


if __name__ == '__main__':
    raise SystemExit(main())
