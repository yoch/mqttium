#!/usr/bin/env python3
"""Finite public session lifecycle system reception, not oracle qualification."""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, deque
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time

from external_mqttium import client_identity
from external_sessions import INFINITE, MqttiumSession, PahoSession
from pubsub_probe import PubsubWire, durable
from receipt_reception import storage_sample
from tcp_probe import (Observations, ReceiptFailure, connect_packet, packet, publish_packet,
                       require, write_json)
from tcp_reception import Broker, writer_available


MAX_SESSIONS = 128
TARGET_CYCLES = 512
ACTION_TIMEOUT = 12
CARDINALITY_ZERO = ('session_cache', 'owner_sessions', 'owner_bindings', 'work_requests',
                    'work_ready', 'emit_registrations', 'lifecycle_requests', 'retired_flights',
                    'owner_inbound', 'owner_outgoing', 'frames', 'timers')


class EventCursor:
    """Bounded contiguous product facts; interrupted rotations consume nothing."""
    def __init__(self, broker):
        self.broker, self.offsets, self.last = broker, {}, 0
        self.counts, self.recent = Counter(), deque(maxlen=256)

    def read(self):
        offsets, records, live = dict(self.offsets), [], set()
        paths = sorted(self.broker.events_path.parent.glob(self.broker.events_path.name + '*'))
        if not paths:
            return
        require(len(paths) <= 4, 'session_product_log_budget')
        for path in paths:
            try:
                stream = path.open('rb')
            except FileNotFoundError:
                return
            with stream:
                first = stream.readline(2049)
                if not first or not first.endswith(b'\n'):
                    return
                require(len(first) <= 2048, 'session_product_record_budget')
                generation = (os.fstat(stream.fileno()).st_ino, int(json.loads(first)['seq']))
                live.add(generation)
                offset = offsets.get(generation, 0)
                stream.seek(offset)
                while True:
                    line = stream.readline(2049)
                    if not line or not line.endswith(b'\n'):
                        break
                    require(len(line) <= 2048, 'session_product_record_budget')
                    records.append(json.loads(line))
                    require(len(records) <= 16384, 'session_product_scan_budget')
                    offset = stream.tell()
                offsets[generation] = offset
        # Do not consume any file position until the scan is complete.
        self.offsets = {key: value for key, value in offsets.items() if key in live}
        for event in sorted(records, key=lambda row: int(row['seq'])):
            require(event['pid'] == self.broker.process.pid, 'session_wrong_process')
            sequence = int(event['seq'])
            if sequence <= self.last:
                continue
            require(sequence == self.last + 1, 'session_product_observation_gap', str(sequence))
            self.last = sequence
            self.counts[event['kind']] += 1
            self.recent.append(event)


def session_snapshot(broker):
    """Independent read-only SQL, bounded before materializing session values."""
    for _ in range(8):
        sample = storage_sample(broker)
        rows, states, revision = _session_rows(broker)
        if revision == sample['metadata']['revision']:
            break
        time.sleep(.001)
    else:
        raise ReceiptFailure('session_inspection_frontier_moved')
    sample['sessions'] = rows
    sample['active_sessions'] = states.get(1, 0)
    sample['terminal_sessions'] = states.get(2, 0)
    require(int(sample['metadata']['max_sessions'], 16) == MAX_SESSIONS, 'session_quota_changed')
    require(int(sample['metadata']['limit_receipt_slots'], 16) == 8192, 'session_receipt_quota_changed')
    return sample


def _session_rows(broker):
    db = sqlite3.connect(f'file:{broker.db}?mode=ro', uri=True, timeout=2)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        states = dict(db.execute('SELECT state,count(*) FROM sessions GROUP BY state LIMIT 4'))
        require(set(states) <= {1, 2}, 'session_state_invalid')
        oversized = db.execute('SELECT count(*) FROM sessions WHERE length(client_id)>1024').fetchone()[0]
        require(not oversized, 'session_client_field_budget')
        require(sum(states.values()) <= 2 * MAX_SESSIONS, 'session_inspection_row_budget')
        columns = [row[1] for row in db.execute('PRAGMA table_info(sessions)')]
        rows = []
        for values in db.execute('SELECT * FROM sessions ORDER BY id LIMIT 257'):
            row = {key: value.hex() if isinstance(value, bytes) else value
                   for key, value in zip(columns, values)}
            rows.append(row)
        revision = db.execute('SELECT revision FROM meta').fetchone()[0].hex()
        db.execute('COMMIT')
    finally:
        db.close()
    return rows, states, revision


def session_row(snapshot, client_id, *, absent=False):
    rows = [row for row in snapshot['sessions'] if row['client_id'] == client_id.encode().hex()]
    if absent:
        require(not rows, 'session_still_present', client_id)
        return None
    require(len(rows) == 1 and rows[0]['state'] == 1, 'session_identity_missing', client_id)
    return rows[0]


def assert_drained(snapshot, *, sessions=0):
    require(snapshot['terminal_sessions'] == 0, 'terminal_session_rows_retained')
    require(snapshot['active_sessions'] == sessions, 'session_active_cardinality')
    require(snapshot['counts']['sessions'] == sessions, 'session_rows_not_drained')
    for table in ('routes', 'inbound', 'outbound', 'obligations', 'publications', 'payloads'):
        require(snapshot['counts'].get(table, 0) == 0, 'session_business_not_drained', table)
    for column in ('used_payload_bytes', 'used_exchange_slots', 'used_references'):
        require(int(snapshot['metadata'][column], 16) == 0, 'session_credit_not_returned', column)


def cardinality_drained(event):
    details = event.get('details', {})
    required = CARDINALITY_ZERO + ('closed_generations',)
    require(all(name in details for name in required), 'session_cardinality_fields_missing')
    require(all(type(details[name]) is int and details[name] >= 0 for name in required),
            'session_cardinality_invalid')
    require(details['closed_generations'] <= 8, 'session_closed_generation_memory_grew')
    return all(details[name] == 0 for name in CARDINALITY_ZERO)


async def closed_and_drained(broker, cursor, session_id, after):
    deadline, changed = time.monotonic() + ACTION_TIMEOUT, None
    while time.monotonic() < deadline:
        cursor.read()
        for event in cursor.recent:
            if (int(event['seq']) > after and event['kind'] == 'session_changed'
                    and event.get('details', {}).get('action') == 'Closed'
                    and event['details'].get('session') == str(session_id)):
                require(isinstance(event.get('operation'), dict), 'session_close_operation_missing')
                changed = event
            if (changed is not None and event['kind'] == 'session_cardinality'
                    and int(event['seq']) > int(changed['seq']) and cardinality_drained(event)):
                return changed, event
        require(broker.process.poll() is None, 'session_broker_exited')
        await asyncio.sleep(.005)
    raise ReceiptFailure('session_cardinality_observation_missing',
                         f'no fresh drained observation after close of session {session_id}')


async def until_state(broker, cursor, predicate, *, code='session_state_not_reached'):
    deadline = time.monotonic() + ACTION_TIMEOUT
    latest = None
    while time.monotonic() < deadline:
        cursor.read()
        latest = session_snapshot(broker)
        if predicate(latest):
            return latest
        require(broker.process.poll() is None, 'session_broker_exited', str(broker.process.returncode))
        await asyncio.sleep(.005)
    raise ReceiptFailure(code, json.dumps(latest['counts'] if latest else {}, sort_keys=True))


async def gone(broker, cursor, client_id):
    key = client_id.encode().hex()
    return await until_state(broker, cursor, lambda state: not any(
        row['client_id'] == key for row in state['sessions']), code='session_close_not_durable')


def identity(row):
    return int(row['id'], 16)


def policy(row, expiry):
    require(row.get('expiry') == int(expiry == INFINITE), 'session_policy_not_durable')
    token = row.get('opened_by')
    require(isinstance(token, str) and len(token) == 64, 'session_open_guard_not_durable')
    return row


def connect_bytes(client_id, *, clean_start=False, expiry=INFINITE):
    properties = b'' if expiry is None else b'\x11' + expiry.to_bytes(4, 'big')
    properties += b'\x21\x00\x08\x27\x00\x01\x00\x00'
    return connect_packet(client_id, flags=2 if clean_start else 0, properties=properties)


def disconnect_bytes(expiry=None):
    return packet(0xE0, b'' if expiry is None else b'\x00\x05\x11' + expiry.to_bytes(4, 'big'))


class SessionWire(PubsubWire):
    def session_connect(self, client_id, *, clean_start=False, expiry=INFINITE,
                        expected_present=None, expected_reason=0):
        self.send(connect_bytes(client_id, clean_start=clean_start, expiry=expiry))
        frame = self.wait(0x20)
        require(len(frame.body) >= 3 and frame.body[0] in (0, 1), 'session_bad_connack')
        require(frame.body[1] == expected_reason, 'session_connack_reason', str(frame.body[1]))
        present = bool(frame.body[0])
        require(expected_present is None or present == expected_present, 'session_present_mismatch')
        self.observations.record('session_connack', label=self.label, session_present=present,
                                 reason_code=frame.body[1], clean_start=clean_start, expiry=expiry)
        return present

    def session_disconnect(self, expiry=None):
        self.send(disconnect_bytes(expiry))
        self.observations.record('disconnect_emitted', label=self.label, expiry=expiry)
        self.close()


async def matrix(binary, case):
    broker = Broker(binary, case)
    clients, rows = [], []
    try:
        startup = broker.ready()
        cursor = EventCursor(broker)
        for index, expiry in enumerate((None, 0, INFINITE)):
            name = f'matrix-create-{index}'
            client = MqttiumSession(broker.address, name, clean_start=True, expiry=expiry,
                                    events_path=case / f'client-create-{index}.jsonl')
            clients.append(client)
            await client.open(False)
            state = session_snapshot(broker)
            before = policy(session_row(state, name), expiry)
            rows.append({'case': 'create', 'expiry': expiry, 'session': before})
            await client.disconnect()
            if expiry != INFINITE:
                await gone(broker, cursor, name)
            else:
                await until_state(broker, cursor, lambda state: any(
                    row['id'] == before['id'] for row in state['sessions']))
        for index, expiry in enumerate((None, 0)):
            name = f'matrix-cs0-{index}'
            zero = MqttiumSession(broker.address, name, expiry=expiry,
                                  events_path=case / f'cs0-{index}.jsonl')
            clients.append(zero)
            await zero.open(False)
            await zero.disconnect()
            await gone(broker, cursor, name)
        # Existing persistent state is resumed before expiry0 applies at close.
        owner = MqttiumSession(broker.address, 'matrix-resume', expiry=INFINITE,
                               events_path=case / 'owner.jsonl')
        clients.append(owner)
        await owner.open(False)
        await owner.subscribe('lifecycle/matrix')
        original = session_row(session_snapshot(broker), 'matrix-resume')
        policy(original, INFINITE)
        await owner.disconnect()
        retained = MqttiumSession(broker.address, 'matrix-resume', expiry=INFINITE,
                                  events_path=case / 'retained.jsonl')
        clients.append(retained)
        await retained.open(True)
        require(session_row(session_snapshot(broker), 'matrix-resume')['id'] == original['id'],
                'persistent_resume_changed_identity')
        await retained.disconnect()
        source = PahoSession(broker.address, 'matrix-source', clean_start=True, expiry=0,
                              events_path=case / 'source.jsonl')
        clients.append(source)
        await source.open(False)
        await source.publish('lifecycle/matrix', b'old-state-before-zero-close', qos=2)
        await source.disconnect()
        await gone(broker, cursor, 'matrix-source')
        resumed = MqttiumSession(broker.address, 'matrix-resume', expiry=0,
                                 events_path=case / 'resumed.jsonl')
        clients.append(resumed)
        await resumed.open(True)
        require(session_row(session_snapshot(broker), 'matrix-resume')['id'] == original['id'],
                'expiry_zero_discarded_resumed_state')
        policy(session_row(session_snapshot(broker), 'matrix-resume'), 0)
        await resumed.receive(b'old-state-before-zero-close')
        await until_state(broker, cursor, lambda state: state['counts']['outbound'] == 0
                          and state['counts']['obligations'] == 0)
        await resumed.disconnect()
        await gone(broker, cursor, 'matrix-resume')
        replacement = MqttiumSession(broker.address, 'matrix-resume', expiry=INFINITE,
                                     events_path=case / 'replacement.jsonl')
        clients.append(replacement)
        await replacement.open(False)
        new = session_row(session_snapshot(broker), 'matrix-resume')
        require(identity(new) > identity(original), 'session_identity_reused')
        await replacement.disconnect()
        # A persistent session is explicitly ended through public Paho properties.
        terminate = PahoSession(broker.address, 'matrix-resume', expiry=INFINITE,
                                 events_path=case / 'terminate.jsonl')
        clients.append(terminate)
        await terminate.open(True)
        await terminate.disconnect(0)
        await gone(broker, cursor, 'matrix-resume')
        rows.append({'case': 'resume_then_zero_close', 'before': original, 'next': new})
        absent = MqttiumSession(broker.address, 'matrix-absent-policy', expiry=INFINITE,
                                events_path=case / 'absent-policy-before.jsonl')
        clients.append(absent)
        await absent.open(False)
        absent_id = session_row(session_snapshot(broker), 'matrix-absent-policy')['id']
        await absent.disconnect()
        absent = MqttiumSession(broker.address, 'matrix-absent-policy', expiry=None,
                                events_path=case / 'absent-policy-after.jsonl')
        clients.append(absent)
        await absent.open(True)
        require(session_row(session_snapshot(broker), 'matrix-absent-policy')['id'] == absent_id,
                'absent_expiry_discarded_resumed_state')
        policy(session_row(session_snapshot(broker), 'matrix-absent-policy'), None)
        await absent.disconnect()
        await gone(broker, cursor, 'matrix-absent-policy')
        kept = PahoSession(broker.address, 'matrix-create-2', expiry=INFINITE,
                           events_path=case / 'disconnect-infinity.jsonl')
        clients.append(kept)
        await kept.open(True)
        await kept.disconnect(INFINITE)
        # Replace a durable existing identity and prove its old filters disappear.
        clean = MqttiumSession(broker.address, 'matrix-create-2', clean_start=True, expiry=0,
                               events_path=case / 'clean-replace.jsonl')
        clients.append(clean)
        await clean.open(False)
        require(identity(session_row(session_snapshot(broker), 'matrix-create-2')) >
                identity(rows[2]['session']), 'clean_start_did_not_replace')
        await clean.disconnect()
        final = await gone(broker, cursor, 'matrix-create-2')
        assert_drained(final)
        protocol = await protocol_matrix(broker, cursor, case)
        final = session_snapshot(broker)
        assert_drained(final)
        write_json(case / 'durable-final.json', final)
        broker.stop()
        return {'criterion': 'A', 'status': 'PASS', 'startup': startup, 'decisions': rows,
                'clients': [client.result for client in clients], 'final_sql': final,
                'protocol_cases': protocol,
                'event_counts': dict(cursor.counts), 'stop_status': broker.process.returncode}
    finally:
        for client in clients:
            if not client.closed:
                await client.disconnect()
        broker.cleanup()


async def protocol_matrix(broker, cursor, case):
    observed, observations = [], Observations(case / 'protocol-wire.jsonl')
    def rejected(wire, kind, code):
        frame = wire.wait(kind)
        actual = frame.body[1] if kind == 0x20 else (frame.body[0] if frame.body else 0)
        require(actual == code, 'session_protocol_reason_mismatch', f'{actual}/{code}')
        return {'header': frame.header, 'reason': actual, 'body': frame.body.hex()}
    # Reject a well-formed unsupported CONNECT before touching a live same-name peer.
    owner = SessionWire(broker.address, observations, 'protocol-owner')
    try:
        owner.session_connect('protocol-owner', expiry=INFINITE, expected_present=False)
        original = session_row(session_snapshot(broker), 'protocol-owner')
        duplicate = b'\x11\xff\xff\xff\xff' * 2
        for name, command, reason in (
            ('finite_connect', connect_bytes('protocol-owner', clean_start=True, expiry=1), 0x83),
            ('duplicate_connect_expiry', connect_packet('protocol-owner', flags=2, properties=duplicate), 0x82),
            ('truncated_connect_expiry', connect_packet('protocol-owner', flags=2, properties=b'\x11\0\0\0'), 0x81),
        ):
            wire = SessionWire(broker.address, observations, name)
            try:
                wire.send(command)
                observed.append({'case': name, 'response': rejected(wire, 0x20, reason)})
            finally:
                wire.close()
            owner.send(b'\xc0\x00')
            require(owner.wait(0xD0).body == b'', 'invalid_connect_disrupted_existing_peer')
            require(session_row(session_snapshot(broker), 'protocol-owner')['id'] == original['id'],
                    'invalid_connect_replaced_session')
        owner.session_disconnect(0)
        await gone(broker, cursor, 'protocol-owner')
    finally:
        owner.close()
    for index, value in enumerate((1, INFINITE)):
        name = f'zero-increase-{index}'
        wire = SessionWire(broker.address, observations, name)
        try:
            wire.session_connect(name, expiry=0, expected_present=False)
            wire.send(disconnect_bytes(value))
            observed.append({'case': 'zero_to_nonzero_disconnect', 'requested_expiry': value,
                             'response': rejected(wire, 0xE0, 0x82)})
        finally:
            wire.close()
        await gone(broker, cursor, name)
    for name, properties, reason in (
        ('duplicate_disconnect', b'\x11\0\0\0\0' * 2, 0x82),
        ('finite_disconnect', b'\x11\0\0\0\x01', 0x83),
        ('truncated_disconnect', b'\x11\0\0\0', 0x81),
    ):
        wire = SessionWire(broker.address, observations, name)
        try:
            wire.session_connect(name, expiry=INFINITE, expected_present=False)
            old = session_row(session_snapshot(broker), name)
            declared = len(properties) + (name == 'truncated_disconnect')
            wire.send(packet(0xE0, b'\x00' + bytes([declared]) + properties))
            observed.append({'case': name, 'response': rejected(wire, 0xE0, reason)})
        finally:
            wire.close()
        persisted = session_row(session_snapshot(broker), name)
        require(persisted['id'] == old['id'], 'invalid_disconnect_changed_session')
        cleanup = SessionWire(broker.address, observations, name + '-cleanup')
        try:
            cleanup.session_connect(name, expiry=INFINITE, expected_present=True)
            cleanup.session_disconnect(0)
        finally:
            cleanup.close()
        await gone(broker, cursor, name)
    return observed


async def cycles(binary, case, count=TARGET_CYCLES):
    require(1 <= count <= 1024, 'session_cycle_budget')
    broker = Broker(binary, case)
    ledger_path, samples = case / 'cycles.jsonl', []
    digest, last_session = hashlib.sha256(), 0
    client = None
    try:
        startup = broker.ready()
        cursor, client_info = EventCursor(broker), client_identity()
        pid, inode = broker.process.pid, broker.db.stat().st_ino
        started = time.monotonic_ns()
        with ledger_path.open('xb') as ledger:
            for number in range(count):
                name = f'ephemeral-{number:04}'
                client = MqttiumSession(broker.address, name, clean_start=True,
                                       expiry=None if number % 2 else 0, identity=client_info,
                                       events_path=case / f'client-{number:04}.jsonl')
                await client.open(False)
                opened = policy(session_row(session_snapshot(broker), name),
                                None if number % 2 else 0)
                require(identity(opened) > last_session, 'session_identity_reused')
                last_session = identity(opened)
                await client.subscribe('lifecycle/cycle')
                payload = f'ephemeral-cycle:{number:04}'.encode()
                await client.publish('lifecycle/cycle', payload, qos=1 + number % 2)
                received = await client.receive(payload)
                await until_state(broker, cursor, lambda state: state['counts']['obligations'] == 0
                                  and state['counts']['outbound'] == 0)
                cursor.read()
                before_close = cursor.last
                await client.disconnect()
                final = await gone(broker, cursor, name)
                assert_drained(final)
                changed, cardinality = await closed_and_drained(broker, cursor, last_session, before_close)
                require(broker.process.pid == pid and final['file_inode'] == inode,
                        'session_process_or_file_changed')
                require(client.result['connections'] == 1, 'session_unexpected_reconnect')
                record = {'number': number, 'client_id': name, 'session': opened,
                          'recipient': received, 'connections': client.result['connections'],
                          'close_changed': changed, 'post_close_cardinality': cardinality,
                          'close_verified_by': 'known correlated close, independent SQL absence and drained owner/runtime',
                          'monotonic_ns': str(time.monotonic_ns())}
                raw = (json.dumps(record, sort_keys=True) + '\n').encode()
                require(len(raw) <= 4096 and ledger.tell() + len(raw) <= 8 * 1024 * 1024,
                        'session_cycle_ledger_budget')
                ledger.write(raw);digest.update(raw)
                if (number + 1) % 16 == 0 or number + 1 == count:
                    samples.append(final)
                    write_json(case / 'progress.json', {'status': 'RUNNING', 'completed': number + 1,
                                                      'requested': count, 'samples': samples})
                    print(json.dumps({'kind': 'session_cycle_window', 'completed': number + 1}), flush=True)
                client = None
        state_before_stop = final
        broker.stop()
        writer_available(broker.db)
        restart = Broker(binary, case, db=broker.db, label='restart')
        try:
            restart_ready = restart.ready()
            after_restart = session_snapshot(restart)
            assert_drained(after_restart)
            require(int(after_restart['metadata']['next_session'], 16) > last_session,
                    'session_counter_regressed_after_restart')
            restart.stop()
        finally:
            restart.cleanup()
        return {'criterion': 'B', 'status': 'PASS', 'required_cycles': TARGET_CYCLES,
                'completed_cycles': count, 'long_requirement_met': count >= TARGET_CYCLES,
                'same_main_pid': pid, 'same_sqlite_inode': inode, 'session_quota': MAX_SESSIONS,
                'elapsed_ms': (time.monotonic_ns() - started) / 1e6,
                'ledger': {'path': str(ledger_path), 'sha256': digest.hexdigest(), 'records': count},
                'startup': startup, 'restart_ready': restart_ready,
                'client': client_info, 'last_session': str(last_session),
                'samples': samples, 'final_before_restart': state_before_stop,
                'final_after_restart': after_restart, 'event_counts': dict(cursor.counts),
                'stop_status': broker.process.returncode, 'restart_status': restart.process.returncode}
    finally:
        if client is not None and not client.closed:
            await client.disconnect()
        broker.cleanup()


async def capacity(binary, case):
    broker, clients = Broker(binary, case), []
    try:
        startup = broker.ready()
        cursor, info = EventCursor(broker), client_identity()
        ids, witness_name = [], 'capacity-127'
        for number in range(MAX_SESSIONS):
            name = f'capacity-{number:03}'
            client = MqttiumSession(broker.address, name, expiry=INFINITE, identity=info,
                                    events_path=case / f'create-{number:03}.jsonl')
            clients.append(client)
            await client.open(False)
            if name == witness_name:
                await client.subscribe('lifecycle/capacity-witness')
            ids.append(policy(session_row(session_snapshot(broker), name), INFINITE))
            await client.disconnect()
        full = session_snapshot(broker)
        require(full['active_sessions'] == MAX_SESSIONS and full['terminal_sessions'] == 0,
                'persistent_capacity_not_full')
        # Use an existing producer; creating a new one would itself be a129th identity.
        producer = MqttiumSession(broker.address, 'capacity-000', expiry=INFINITE, identity=info,
                                  events_path=case / 'producer.jsonl')
        clients.append(producer)
        await producer.open(True)
        await producer.publish('lifecycle/capacity-witness', b'preserve-offline-witness', qos=2)
        await producer.disconnect()
        rejected = PahoSession(broker.address, 'capacity-129', expiry=INFINITE,
                               events_path=case / 'rejected.jsonl')
        clients.append(rejected)
        try:
            await rejected.open(False)
        except ConnectionError:
            require(rejected.result.get('reason_code', 0) >= 128, 'capacity_refusal_not_observed')
        else:
            raise ReceiptFailure('session_129th_admitted_without_release')
        await rejected.disconnect()
        require(session_snapshot(broker)['active_sessions'] == MAX_SESSIONS,
                'capacity_refusal_evicted_session')
        replaced = MqttiumSession(broker.address, 'capacity-000', clean_start=True,
                                 expiry=INFINITE, identity=info, events_path=case / 'replaced.jsonl')
        clients.append(replaced)
        await replaced.open(False)
        current = session_snapshot(broker)
        new = session_row(current, 'capacity-000')
        require(identity(new) > identity(ids[0]) and current['active_sessions'] == MAX_SESSIONS,
                'full_capacity_same_client_replacement_failed')
        require(session_row(current, witness_name)['id'] == ids[127]['id'], 'capacity_witness_evicted')
        require(current['counts']['obligations'] == 1, 'unrelated_destination_obligation_removed')
        await replaced.disconnect()
        terminated = PahoSession(broker.address, 'capacity-001', expiry=INFINITE,
                                 events_path=case / 'terminated.jsonl')
        clients.append(terminated)
        await terminated.open(True)
        await terminated.disconnect(0)
        freed = await gone(broker, cursor, 'capacity-001')
        require(freed['active_sessions'] == MAX_SESSIONS - 1, 'session_slot_not_returned')
        admitted = MqttiumSession(broker.address, 'capacity-129', expiry=INFINITE,
                                 identity=info, events_path=case / 'admitted.jsonl')
        clients.append(admitted)
        await admitted.open(False)
        await admitted.disconnect()
        witness = MqttiumSession(broker.address, witness_name, expiry=INFINITE,
                                identity=info, events_path=case / 'witness.jsonl')
        clients.append(witness)
        await witness.open(True)
        require(session_row(session_snapshot(broker), witness_name)['id'] == ids[127]['id'],
                'capacity_witness_identity_changed')
        await witness.receive(b'preserve-offline-witness')
        final = await until_state(broker, cursor, lambda state: state['counts']['obligations'] == 0
                                  and state['counts']['outbound'] == 0)
        require(final['active_sessions'] == MAX_SESSIONS, 'session_capacity_changed')
        await witness.disconnect()
        write_json(case / 'full.json', full);write_json(case / 'final.json', final)
        broker.stop()
        return {'criterion': 'C', 'status': 'PASS', 'startup': startup, 'full': full,
                'replaced_before': ids[0], 'replaced_after': new, 'released': ids[1],
                'witness_session': ids[127], 'final_sql': final,
                'client_results': [client.result for client in clients],
                'stop_status': broker.process.returncode}
    finally:
        for client in clients:
            if not client.closed:
                await client.disconnect()
        broker.cleanup()


async def loaded_termination(binary, case):
    broker, clients = Broker(binary, case), []
    observations = Observations(case / 'loaded-wire.jsonl')
    source_wire = None
    try:
        startup = broker.ready()
        cursor = EventCursor(broker)
        witness = MqttiumSession(broker.address, 'loaded-witness', expiry=INFINITE,
                                 events_path=case / 'witness-setup.jsonl')
        clients.append(witness)
        await witness.open(False)
        await witness.subscribe('lifecycle/shared')
        await witness.subscribe('lifecycle/producer')
        witness_id = session_row(session_snapshot(broker), 'loaded-witness')['id']
        await witness.disconnect()
        source_wire = SessionWire(broker.address, observations, 'loaded-owner')
        source_wire.session_connect('loaded-owner', expiry=INFINITE, expected_present=False)
        source_wire.subscribe(1, [('lifecycle/shared', 1)])
        source_id = session_row(session_snapshot(broker), 'loaded-owner')['id']
        publisher = PahoSession(broker.address, 'loaded-publisher', clean_start=True, expiry=0,
                                events_path=case / 'publisher.jsonl')
        clients.append(publisher)
        await publisher.open(False)
        await publisher.publish('lifecycle/shared', b'shared-with-another-destination', qos=2)
        await publisher.disconnect()
        await gone(broker, cursor, 'loaded-publisher')
        held = source_wire.next_publication()
        require(held.payload == b'shared-with-another-destination' and held.qos == 1,
                'loaded_claim_not_observed')
        # This client's second publication is admitted but deliberately waits for PUBREL.
        source_wire.send(publish_packet('lifecycle/producer', b'producer-ended-but-still-due',
                                         7, qos=2))
        require(source_wire.wait(0x50, 7).ack() == (7, 0), 'loaded_inbound_not_admitted')
        await until_state(broker, cursor, lambda state: state['counts']['outbound'] == 1
                          and state['counts']['inbound'] == 1 and state['counts']['obligations'] == 3)
        before = durable(broker.db)
        owed = [row for row in before['tables']['obligations'] if row['target'] == witness_id]
        require(len(owed) == 2 and len(before['tables']['payloads']) == 2,
                'loaded_shared_fixture_incomplete')
        require(any(row['source'] == source_id for row in before['tables']['publications']),
                'loaded_producer_publication_missing')
        cursor.read();before_close = cursor.last
        source_wire.session_disconnect(0)
        source_wire = None
        await gone(broker, cursor, 'loaded-owner')
        after = durable(broker.db)
        require([row for row in after['tables']['obligations'] if row['target'] == witness_id] == owed,
                'unrelated_destination_obligation_removed')
        require(not any(row['target'] == source_id for row in after['tables']['obligations']),
                'closed_destination_obligation_survived')
        require(not any(row['session'] == source_id for row in after['tables']['inbound']),
                'closed_inbound_survived')
        require(not after['tables']['outbound'], 'closed_claim_survived')
        require(len(after['tables']['payloads']) == 2
                and all(int(row['refs'], 16) == 1 for row in after['tables']['payloads']),
                'session_end_payload_reference_accounting')
        cursor.read()
        changes = [event for event in cursor.recent if int(event['seq']) > before_close
                   and event['kind'] == 'session_changed'
                   and event.get('details', {}).get('action') == 'Closed'
                   and event['details'].get('session') == str(int(source_id, 16))]
        require(len(changes) == 1, 'loaded_close_result_not_observed')
        cancelled = changes[0]['details']
        require(cancelled['cancelled_inbound'] == 1 and cancelled['cancelled_deliveries'] == 1
                and cancelled['cancelled_routes'] == 1, 'session_cancellation_not_distinct')
        new = MqttiumSession(broker.address, 'loaded-owner', expiry=0,
                             events_path=case / 'new-owner.jsonl')
        clients.append(new)
        await new.open(False)
        new_id = session_row(session_snapshot(broker), 'loaded-owner')['id']
        require(new_id != source_id, 'session_identity_reused')
        require(not any(row['target'] == new_id for row in durable(broker.db)['tables']['obligations']),
                'old_work_rebound_to_new_incarnation')
        recipient = MqttiumSession(broker.address, 'loaded-witness', expiry=0,
                                   events_path=case / 'witness-resume.jsonl')
        clients.append(recipient)
        await recipient.open(True)
        require(session_row(session_snapshot(broker), 'loaded-witness')['id'] == witness_id,
                'loaded_witness_identity_changed')
        received = [await recipient.receive() for _ in range(2)]
        require({row['payload_sha256'] for row in received} == {
            hashlib.sha256(b'shared-with-another-destination').hexdigest(),
            hashlib.sha256(b'producer-ended-but-still-due').hexdigest()},
            'loaded_witness_publication_missing')
        await until_state(broker, cursor, lambda state: all(state['counts'][name] == 0
                          for name in ('outbound', 'obligations', 'inbound', 'payloads', 'publications')))
        await recipient.disconnect()
        await gone(broker, cursor, 'loaded-witness')
        await new.disconnect()
        final = await gone(broker, cursor, 'loaded-owner')
        assert_drained(final)
        write_json(case / 'before-close.json', before)
        write_json(case / 'after-close.json', after)
        write_json(case / 'final.json', final)
        broker.stop()
        return {'criterion': 'D', 'status': 'PASS', 'startup': startup,
                'closed_session': source_id, 'new_session': new_id, 'witness_session': witness_id,
                'held_publish': held.observation(), 'cancelled': changes[0],
                'original_witness_obligations': owed, 'received': received, 'final_sql': final,
                'stop_status': broker.process.returncode}
    finally:
        if source_wire is not None:
            source_wire.close()
        for client in clients:
            if not client.closed:
                await client.disconnect()
        broker.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary-source-head', required=True)
    parser.add_argument('--case', choices=('A', 'B', 'C', 'D', 'nominal'), default='nominal')
    parser.add_argument('--cycles', type=int, default=TARGET_CYCLES)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    require(not any(args.out.iterdir()), 'session_output_not_empty')
    source = subprocess.run(['git', '-C', str(Path(__file__).resolve().parents[3]), 'rev-parse', 'HEAD'],
                            check=True, capture_output=True, text=True, timeout=3).stdout.strip()
    receipt = {'format': 'mqttiumd.session-system-reception', 'schema_version': 1,
               'qualification': 'system-test-only', 'source_head': args.binary_source_head,
               'harness_source_head': source, 'status': 'FAIL', 'cases': [],
               'binary': {'path': str(args.binary.resolve()),
                          'sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest()}}
    try:
        names = ('A', 'B', 'C', 'D') if args.case == 'nominal' else (args.case,)
        for name in names:
            directory = args.out / name
            directory.mkdir()
            function = {'A': matrix, 'B': cycles, 'C': capacity, 'D': loaded_termination}[name]
            parameters = (args.cycles,) if name == 'B' else ()
            receipt['cases'].append(asyncio.run(function(args.binary.resolve(), directory, *parameters)))
        receipt['status'] = 'PASS'
    except Exception as error:
        receipt['failure'] = {'code': getattr(error, 'code', type(error).__name__),
                              'detail': str(error)[:512]}
    finally:
        write_json(args.out / 'receipt.json', receipt)
    print(json.dumps({'status': receipt['status'], 'failure': receipt.get('failure'),
                      'receipt': str(args.out / 'receipt.json')}))
    return 0 if receipt['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
