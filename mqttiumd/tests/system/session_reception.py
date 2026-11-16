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
from tcp_probe import (Observations, ReceiptFailure, connect_packet, operation_hex, packet,
                       publish_packet, require, write_json)
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


async def fact(broker, cursor, kind, *, after=0, predicate=lambda event: True, timeout=ACTION_TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cursor.read()
        for event in cursor.recent:
            if int(event['seq']) > after and event['kind'] == kind and predicate(event):
                broker.observations.record('session_fact_retained', event=event)
                return event
        require(broker.process.poll() is None, 'session_broker_exited', str(broker.process.returncode))
        await asyncio.sleep(.005)
    raise ReceiptFailure('session_fact_not_reached', kind)


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
    def rejected(wire, kind, code, *, allow_clean_close=False):
        try:
            frame = wire.wait(kind)
        except ReceiptFailure as error:
            # An invalid CONNECT does not establish a trusted peer packet
            # limit. A clean close is permitted, but its typed product cause
            # must be retained independently; truncation is not an EOF.
            require(allow_clean_close and error.code == 'peer_closed',
                    'session_protocol_response_missing', error.code)
            return {'clean_close': True, 'reason': code}
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
                cursor.read();before = cursor.last
                wire.send(command)
                response = rejected(wire, 0x20, reason, allow_clean_close=name != 'finite_connect')
                cursor.read()
                refusals = [event for event in cursor.recent if int(event['seq']) > before
                            and event['kind'] == 'packet_refused']
                require(len(refusals) == 1, 'session_protocol_cause_missing')
                event = refusals[0]
                expected = {'finite_connect': 'ConnectOutsideProfile',
                            'duplicate_connect_expiry': 'DuplicateProperty',
                            'truncated_connect_expiry': 'TruncatedField'}[name]
                require(event['details']['reason'] == reason and event['details']['code'] == expected,
                        'session_protocol_cause_mismatch')
                observed.append({'case': name, 'response': response, 'refusal': event})
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
                changed, cardinality = await closed_and_drained(broker, cursor, last_session, before_close)
                final = session_snapshot(broker)
                # Atomic physical cleanup is already a known fact here. A
                # retained Terminal row is a cardinality violation, not a
                # generic wait-for-absence timeout.
                assert_drained(final)
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


async def uncertain_lifecycle(binary, case):
    """Authentic committed results stay unknown until same-OperationId resolve."""
    broker = Broker(binary, case, test_control=True)
    clients, rows, pending = [], [], None
    independent = None
    try:
        startup = broker.ready();cursor = EventCursor(broker)
        independent = SessionWire(broker.address, broker.observations, 'independent')
        independent.session_connect('unknown-independent', expiry=INFINITE, expected_present=False)
        for name, replacing in (('unknown-open', False), ('unknown-replace', True)):
            original = None
            if replacing:
                prior = MqttiumSession(broker.address, name, expiry=INFINITE,
                                        events_path=case / f'{name}-before.jsonl')
                clients.append(prior)
                await prior.open(False)
                await prior.subscribe('lifecycle/unknown-old-route')
                original = session_row(session_snapshot(broker), name)
                await prior.disconnect()
            hold = broker.control / 'hold-session-open-result'
            hold.touch(exist_ok=False)
            cursor.read();before = cursor.last
            client = MqttiumSession(broker.address, name, clean_start=replacing, expiry=0,
                                    events_path=case / f'{name}.jsonl')
            clients.append(client)
            pending = asyncio.create_task(client.open(False))
            held = await fact(broker, cursor, 'store_result_held', after=before,
                              predicate=lambda e: e.get('command_kind') == 'SessionOpen')
            op = held['operation']
            committed = session_row(session_snapshot(broker), name)
            require(committed['opened_by'] == operation_hex(op), 'session_unknown_wrong_operation')
            if original is not None:
                require(committed['id'] != original['id'], 'session_replace_not_committed')
                require(not any(row['session'] == original['id'] for row in
                                durable(broker.db)['tables']['routes']), 'replaced_route_retained')
            require(not pending.done(), 'connack_before_session_result_known')
            independent.send(b'\xc0\x00')
            require(independent.wait(0xD0).body == b'', 'session_unknown_control_blocked')
            resolved = await fact(broker, cursor, 'store_resolved', after=int(held['seq']),
                                  predicate=lambda e: e.get('operation') == op)
            require(resolved['details']['outcome'] == 'pendingorunknown',
                    'session_held_resolution_became_certain')
            # The normal server's three-second CONNECT deadline precedes its
            # ten-second resolution delay. This transport must therefore fail
            # safely; durable knowledge still resolves the original operation.
            outcome = (await asyncio.gather(pending, return_exceptions=True))[0]
            pending = None
            require(isinstance(outcome, Exception)
                    and not any(e['kind'] == 'connack' for e in client.log.events),
                    'connack_before_session_result_known')
            hold.unlink()
            broker.observations.record('session_result_released', operation=op)
            known = await fact(broker, cursor, 'session_changed', after=int(held['seq']),
                               predicate=lambda e: e.get('operation') == op)
            require(known['details']['action'] == 'Opened'
                    and known['details']['session'] == str(identity(committed)),
                    'session_resolution_changed_identity')
            await client.disconnect()
            await gone(broker, cursor, name)
            rows.append({'case': name, 'before': original, 'committed_while_unknown': committed,
                         'held': held, 'unknown_resolution': resolved, 'known': known,
                         'original_connect_result': type(outcome).__name__,
                         'positive_connack_observed': False})
        # The local DISCONNECT is not a server acknowledgement. Its actual
        # durable close is inspected while the genuine result is withheld.
        closing = PahoSession(broker.address, 'unknown-close', expiry=INFINITE,
                               events_path=case / 'unknown-close.jsonl')
        clients.append(closing)
        await closing.open(False)
        old = session_row(session_snapshot(broker), 'unknown-close')
        hold = broker.control / 'hold-session-close-result'
        hold.touch(exist_ok=False)
        cursor.read();before = cursor.last
        await closing.disconnect(0)
        held = await fact(broker, cursor, 'store_result_held', after=before,
                          predicate=lambda e: e.get('command_kind') == 'SessionClose')
        op = held['operation']
        state = await gone(broker, cursor, 'unknown-close')
        require(not any(e['kind'] == 'session_changed' and e.get('operation') == op
                        for e in cursor.recent), 'session_close_known_before_held_result')
        independent.send(b'\xc0\x00')
        require(independent.wait(0xD0).body == b'', 'session_unknown_control_blocked')
        resolved = await fact(broker, cursor, 'store_resolved', after=int(held['seq']),
                              predicate=lambda e: e.get('operation') == op)
        require(resolved['details']['outcome'] == 'pendingorunknown',
                'session_held_resolution_became_certain')
        hold.unlink()
        known = await fact(broker, cursor, 'session_changed', after=int(held['seq']),
                           predicate=lambda e: e.get('operation') == op)
        require(known['details']['action'] == 'Closed'
                and known['details']['session'] == str(identity(old)),
                'session_close_resolution_changed_identity')
        rows.append({'case': 'unknown-close', 'before': old, 'held': held,
                     'sql_while_unknown': state, 'unknown_resolution': resolved, 'known': known})
        takeover = await takeover_and_old_events(broker, cursor)
        independent.session_disconnect(0);independent = None
        final = await gone(broker, cursor, 'unknown-independent')
        assert_drained(final)
        broker.stop()
        return {'criterion': 'E', 'status': 'PASS', 'startup': startup, 'cases': rows,
                'takeovers_and_old_events': takeover,
                'final_sql': final, 'clients': [c.result for c in clients],
                'stop_status': broker.process.returncode}
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        if independent is not None:
            independent.close()
        broker.cleanup()
        for client in clients:
            if not client.closed:
                await client.disconnect()


async def takeover_and_old_events(broker, cursor):
    """Real socket takeovers plus copies captured at the actual runtime guards."""
    rows = []
    for index, (old_expiry, clean, new_expiry, present, same) in enumerate((
            (INFINITE, False, INFINITE, True, True),
            (INFINITE, True, INFINITE, False, False),
            (0, False, INFINITE, False, False),
            (INFINITE, False, 0, True, True))):
        name = f'takeover-{index}'
        old = replacement = None
        try:
            if index in (0, 1):
                (broker.control / 'capture-next-session-change').touch(exist_ok=False)
            cursor.read();before = cursor.last
            old = SessionWire(broker.address, broker.observations, name + '-old')
            old.session_connect(name, expiry=old_expiry, expected_present=False)
            original = session_row(session_snapshot(broker), name)
            old_open = await fact(broker, cursor, 'session_changed', after=before,
                                  predicate=lambda e: e.get('details', {}).get('action') == 'Opened'
                                  and e['details'].get('session') == str(identity(original)))
            if index in (0, 1):
                captured_change = await fact(broker, cursor, 'session_change_copied', after=before)
                require(captured_change['operation'] == old_open['operation'],
                        'session_change_not_captured_at_real_effect')
                (broker.control / 'capture-next-session-close').touch(exist_ok=False)
            cursor.read();before_takeover = cursor.last
            replacement = SessionWire(broker.address, broker.observations, name + '-new')
            replacement.session_connect(name, clean_start=clean, expiry=new_expiry,
                                         expected_present=present)
            disconnect = old.wait(0xE0)
            require(disconnect.body and disconnect.body[0] == 0x8E, 'takeover_reason_wrong')
            current = session_row(session_snapshot(broker), name)
            require((current['id'] == original['id']) == same, 'takeover_session_identity_wrong')
            policy(current, new_expiry)
            known = await fact(broker, cursor, 'session_changed', after=before_takeover,
                               predicate=lambda e: e.get('details', {}).get('action') == 'Opened'
                               and e['details'].get('session') == str(identity(current)))
            closed = await fact(broker, cursor, 'connection_closed', after=before_takeover,
                                predicate=lambda e: e.get('connection') == old_open['connection'])
            require(int(closed['seq']) < int(known['seq']), 'takeover_open_preceded_old_close')
            replay = None
            if index in (0, 1):
                copied_close = await fact(broker, cursor, 'session_close_copied', after=before_takeover)
                require(copied_close['connection'] == old_open['connection']
                        and copied_close['details']['session'] == str(identity(original))
                        and copied_close['operation'] == old_open['operation'],
                        'session_close_lost_original_transport_identity')
                cursor.read();before_replay = cursor.last
                (broker.control / 'replay-session-close').touch(exist_ok=False)
                (broker.control / 'replay-session-change').touch(exist_ok=False)
                replayed_close = await fact(broker, cursor, 'session_close_replayed', after=before_replay)
                replayed_change = await fact(broker, cursor, 'session_change_replayed', after=before_replay)
                ignored = await fact(broker, cursor, 'session_change_ignored', after=before_replay,
                                     predicate=lambda e: e.get('operation') == old_open['operation'])
                require(ignored['details']['code'] == 'stale_lifecycle_result',
                        'stale_session_change_rebound')
                require(replayed_change['operation'] == captured_change['operation']
                        and replayed_close['connection'] == copied_close['connection'],
                        'old_session_fact_recorrelated')
                replay = {'captured_change': captured_change, 'captured_close': copied_close,
                          'replayed_change': replayed_change, 'replayed_close': replayed_close,
                          'ignored_change': ignored}
            replacement.send(b'\xc0\x00')
            require(replacement.wait(0xD0).body == b'', 'old_close_ended_new_incarnation')
            stable = session_row(session_snapshot(broker), name)
            require(stable['id'] == current['id'] and stable['opened_by'] == current['opened_by'],
                    'stale_session_change_rebound')
            replacement.session_disconnect(0);replacement = None
            await gone(broker, cursor, name)
            rows.append({'case': name, 'clean_start': clean, 'old_expiry': old_expiry,
                         'new_expiry': new_expiry, 'expected_session_present': present,
                         'original': original, 'current': current, 'old_transport_closed': closed,
                         'known_new_attachment': known, 'old_wire_disconnect': disconnect.body.hex(),
                         'replayed_internal_copies': replay})
        finally:
            for wire in (old, replacement):
                if wire is not None:
                    wire.close()
    return rows


async def lifecycle_crashes(binary, case):
    """Each cut has its own process witness and restarts its own same file."""
    rows = []
    for index, (action, point, expected_present) in enumerate((
            ('replace', 'session.open.prepared', True),
            ('replace', 'session.open.committed', True),
            ('close', 'session.close.prepared', True),
            ('close', 'session.close.committed', False))):
        directory = case / f'{index}-{action}-{point}'
        setup = Broker(binary, directory, label='setup')
        broker = restarted = wire = None
        try:
            setup.ready();cursor = EventCursor(setup)
            wire = SessionWire(setup.address, setup.observations, 'setup-owner')
            wire.session_connect('crash-owner', expiry=INFINITE, expected_present=False)
            wire.subscribe(1, [('lifecycle/crash', 1)])
            original = session_row(session_snapshot(setup), 'crash-owner')
            wire.session_disconnect();wire = None
            setup.stop();writer_available(setup.db)
            broker = Broker(binary, directory, db=setup.db, label='cut', pauses=(point,))
            broker.ready();cursor = EventCursor(broker)
            wire = SessionWire(broker.address, broker.observations, 'cut-owner')
            if action == 'replace':
                wire.send(connect_bytes('crash-owner', clean_start=True, expiry=INFINITE))
            else:
                wire.session_connect('crash-owner', expiry=INFINITE, expected_present=True)
                wire.send(disconnect_bytes(0))
            reached = await fact(broker, cursor, 'barrier_hit',
                                 predicate=lambda e: e.get('point') == point)
            paused = session_snapshot(broker)
            committed = point.endswith('.committed')
            if action == 'replace':
                paused_row = session_row(paused, 'crash-owner')
                require((paused_row['id'] != original['id']) == committed,
                        'replace_crash_frontier_wrong')
                require(wire.poll(.03) is None, 'connack_before_session_result_known')
            else:
                require(bool(paused['sessions']) != committed, 'close_crash_frontier_wrong')
            killed = broker.stop(killed=True)
            writer_available(broker.db)
            wire.close();wire = None
            restarted = Broker(binary, directory, db=broker.db, label='restart')
            restart_ready = restarted.ready();restart_cursor = EventCursor(restarted)
            before_connect = session_snapshot(restarted)
            require(before_connect['file_inode'] == paused['file_inode'],
                    'session_crash_restarted_different_file')
            if action == 'replace':
                restored = session_row(before_connect, 'crash-owner')
                require(restored['id'] == paused_row['id'], 'replaced_session_not_durable')
                require(before_connect['counts']['routes'] == (0 if committed else 1),
                        'replace_route_frontier_wrong')
            elif committed:
                assert_drained(before_connect)
            else:
                require(session_row(before_connect, 'crash-owner')['id'] == original['id'],
                        'uncommitted_close_erased_session')
            wire = SessionWire(restarted.address, restarted.observations, 'restart-owner')
            wire.session_connect('crash-owner', expiry=INFINITE, expected_present=expected_present)
            resumed = session_row(session_snapshot(restarted), 'crash-owner')
            if action == 'close' and committed:
                require(identity(resumed) > identity(original), 'session_identity_reused')
            wire.session_disconnect(0);wire = None
            final = await gone(restarted, restart_cursor, 'crash-owner')
            assert_drained(final)
            restarted.stop();writer_available(restarted.db)
            rows.append({'action': action, 'point': point, 'reached': reached,
                         'original_session': original, 'sql_at_cut': paused,
                         'signal_status': killed, 'same_sqlite_inode': paused['file_inode'],
                         'restart_ready': restart_ready, 'restart_before_connect': before_connect,
                         'resumed_session': resumed, 'final_sql': final,
                         'restart_stop_status': restarted.process.returncode})
        finally:
            if wire is not None:
                wire.close()
            for process in (setup, broker, restarted):
                if process is not None:
                    process.cleanup()
    rows.append(await connected_crash(binary, case / '4-connected-ephemeral'))
    return {'criterion': 'F', 'status': 'PASS', 'cases': rows,
            'atomic_cleanup': True,
            'scope': 'Product processes killed at real SQLite prepared/committed barriers; '
                     'connected ephemeral session and persistent offline work share a separate run.'}


async def connected_crash(binary, case):
    broker = Broker(binary, case)
    restarted = source = witness = None
    try:
        startup = broker.ready();cursor = EventCursor(broker)
        witness = MqttiumSession(broker.address, 'crash-persistent', expiry=INFINITE,
                                 events_path=case / 'witness-setup.jsonl')
        await witness.open(False)
        await witness.subscribe('lifecycle/crash-owned-by-other')
        witness_id = session_row(session_snapshot(broker), 'crash-persistent')['id']
        await witness.disconnect();witness = None
        source = SessionWire(broker.address, broker.observations, 'ephemeral-source')
        source.session_connect('crash-ephemeral', expiry=0, expected_present=False)
        source.subscribe(1, [('lifecycle/ephemeral-filter', 1)])
        old = session_row(session_snapshot(broker), 'crash-ephemeral')
        source.send(publish_packet('lifecycle/crash-owned-by-other', b'survives-source-crash', 7, qos=2))
        require(source.wait(0x50, 7).ack() == (7, 0), 'crash_source_not_admitted')
        before = durable(broker.db)
        owed = before['tables']['obligations']
        require(len(owed) == 1 and owed[0]['target'] == witness_id, 'crash_witness_work_missing')
        killed = broker.stop(killed=True);writer_available(broker.db)
        source.close();source = None
        restarted = Broker(binary, case, db=broker.db, label='restart')
        restart_ready = restarted.ready();restart_cursor = EventCursor(restarted)
        recovered = session_snapshot(restarted)
        session_row(recovered, 'crash-ephemeral', absent=True)
        require(session_row(recovered, 'crash-persistent')['id'] == witness_id,
                'persistent_witness_erased_on_restart')
        require(durable(restarted.db)['tables']['obligations'] == owed,
                'other_destination_lost_during_ephemeral_startup_close')
        require(recovered['counts']['inbound'] == 0, 'ephemeral_inbound_resurrected')
        source = SessionWire(restarted.address, restarted.observations, 'new-ephemeral')
        source.session_connect('crash-ephemeral', expiry=0, expected_present=False)
        fresh = session_row(session_snapshot(restarted), 'crash-ephemeral')
        require(identity(fresh) > identity(old), 'ephemeral_identity_reused_after_crash')
        witness = MqttiumSession(restarted.address, 'crash-persistent', expiry=0,
                                 events_path=case / 'witness-resumed.jsonl')
        await witness.open(True)
        received = await witness.receive(b'survives-source-crash')
        await until_state(restarted, restart_cursor, lambda state: state['counts']['obligations'] == 0
                          and state['counts']['outbound'] == 0)
        await witness.disconnect();witness = None
        source.session_disconnect();source = None
        await gone(restarted, restart_cursor, 'crash-persistent')
        final = await gone(restarted, restart_cursor, 'crash-ephemeral')
        assert_drained(final)
        restarted.stop();writer_available(restarted.db)
        write_json(case / 'durable-before-kill.json', before)
        return {'case': 'connected_ephemeral_and_persistent_offline_work', 'startup': startup,
                'original_session': old, 'new_session': fresh, 'witness_session': witness_id,
                'signal_status': killed, 'restart_ready': restart_ready,
                'recovered_sql': recovered, 'received': received, 'final_sql': final,
                'restart_stop_status': restarted.process.returncode}
    finally:
        if source is not None:
            source.close()
        for process in (broker, restarted):
            if process is not None:
                process.cleanup()
        if witness is not None and not witness.closed:
            await witness.disconnect()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary-source-head', required=True)
    parser.add_argument('--case', choices=('A', 'B', 'C', 'D', 'E', 'F', 'nominal', 'faults'),
                        default='nominal')
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
        names = {'nominal': ('A', 'B', 'C', 'D'), 'faults': ('E', 'F')}.get(args.case, (args.case,))
        for name in names:
            directory = args.out / name
            directory.mkdir()
            function = {'A': matrix, 'B': cycles, 'C': capacity, 'D': loaded_termination,
                        'E': uncertain_lifecycle, 'F': lifecycle_crashes}[name]
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
