#!/usr/bin/env python3
"""Finite MQTT5 session-expiry system evidence, never oracle qualification.

Only client observations plus separately read durable state establish outcomes.
The local completion of DISCONNECT is not a broker acknowledgement. Clock-domain
continuity is described by the product; normal-process crash tests do not claim
that an unrecognized domain counts unknown downtime.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import time

from external_mqttium import client_identity
from external_sessions import INFINITE, MqttiumSession, PahoSession
from pubsub_probe import durable
from session_reception import EventCursor, SessionWire, identity, session_row, session_snapshot, fact
from tcp_probe import ReceiptFailure, operation_hex, require, write_json
from tcp_reception import Broker, writer_available


QUOTA = 128
REARMS = 64


def temporal_snapshot(broker):
    state = session_snapshot(broker)
    connection = sqlite3.connect(f'file:{broker.db}?mode=ro', uri=True, timeout=2)
    try:
        connection.execute('PRAGMA query_only=ON')
        rows = connection.execute('SELECT domain,tick,logical FROM session_clock LIMIT 2').fetchall()
        require(len(rows) == 1, 'expiry_clock_anchor_missing')
        domain, tick, logical = rows[0]
        require(isinstance(domain, bytes) and len(domain) == 32
                and isinstance(tick, bytes) and len(tick) == 8
                and isinstance(logical, bytes) and len(logical) == 8,
                'expiry_clock_anchor_invalid')
        state['session_clock'] = {'domain': domain.hex(), 'tick': str(int.from_bytes(tick, 'big')),
                                  'logical': str(int.from_bytes(logical, 'big'))}
    finally:
        connection.close()
    require(state['counts']['session_clock'] == 1, 'expiry_clock_history_grew')
    return state


def check_policy(row, requested):
    code = 0 if requested in (None, 0) else 1 if requested == INFINITE else 2 + requested
    require(row.get('expiry') == code, 'expiry_policy_not_durable', str(requested))
    require(isinstance(row.get('opened_by'), str) and len(row['opened_by']) == 64,
            'expiry_attachment_not_durable')
    return row


def detachment(row, *, finite=True):
    by, deadline = row.get('detached_by'), row.get('deadline')
    require(isinstance(by, str) and len(by) == 64, 'expiry_detachment_not_durable')
    if finite:
        require(isinstance(deadline, str) and len(deadline) == 16,
                'expiry_deadline_not_durable')
    else:
        require(deadline is None, 'infinite_detachment_has_deadline')
    return {'operation': by, 'deadline': str(int(deadline, 16)) if deadline else None}


async def state_until(broker, cursor, predicate, *, reason, timeout=12):
    end = time.monotonic() + timeout
    latest = None
    while time.monotonic() < end:
        cursor.read()
        latest = temporal_snapshot(broker)
        if predicate(latest):
            return latest
        require(broker.process.poll() is None, 'expiry_broker_exited', str(broker.process.returncode))
        await asyncio.sleep(.01)
    raise ReceiptFailure(reason, json.dumps(latest['counts'] if latest else {}, sort_keys=True))


def has_client(state, client_id):
    return any(row['client_id'] == client_id.encode().hex() for row in state['sessions'])


async def detached(broker, cursor, client_id):
    state = await state_until(broker, cursor, lambda state: any(
        row['client_id'] == client_id.encode().hex() and row.get('detached_by') is not None
        for row in state['sessions']), reason='expiry_detachment_not_durable')
    return session_row(state, client_id)


async def absent(broker, cursor, client_id, timeout=12):
    return await state_until(broker, cursor, lambda state: not has_client(state, client_id),
                             reason='session_did_not_expire', timeout=timeout)


async def close_clients(clients):
    for client in clients:
        if not client.closed:
            try:
                await client.disconnect()
            except (OSError, TimeoutError, ConnectionError):
                # Its original failure remains the receipt cause; the process
                # supervisor below still confirms teardown of the broker.
                pass


async def public_paths(binary, case):
    broker = Broker(binary, case)
    clients, paths = [], []
    try:
        ready = broker.ready(); cursor = EventCursor(broker)
        for implementation, constructor in (('mqttium', MqttiumSession), ('paho', PahoSession)):
            name = 'expiry-active-' + implementation
            client = constructor(broker.address, name, expiry=1,
                                 events_path=case / f'{implementation}-active.jsonl')
            clients.append(client)
            await client.open(False)
            await client.subscribe('expiry/' + name)
            before = check_policy(session_row(temporal_snapshot(broker), name), 1)
            started = time.monotonic_ns()
            await asyncio.sleep(1.15)
            active = session_row(temporal_snapshot(broker), name)
            require(active['id'] == before['id'] and active.get('detached_by') is None,
                    'active_session_expired')
            payload = ('active-after-ttl-' + implementation).encode()
            await client.publish('expiry/' + name, payload, qos=2)
            received = await client.receive(payload)
            await client.disconnect()
            offline = await detached(broker, cursor, name)
            observed_detachment = detachment(offline)
            await absent(broker, cursor, name)
            paths.append({'case': 'active_past_interval', 'implementation': implementation,
                          'elapsed_ms': (time.monotonic_ns() - started) / 1e6,
                          'before': before, 'active_after_interval': active,
                          'detached': offline, 'detachment': observed_detachment,
                          'received': received})

            name = 'expiry-resume-' + implementation
            first = constructor(broker.address, name, expiry=4,
                                events_path=case / f'{implementation}-offline.jsonl')
            clients.append(first)
            await first.open(False)
            await first.subscribe('expiry/offline/' + implementation)
            original = session_row(temporal_snapshot(broker), name)
            await first.disconnect()
            initial_detach = await detached(broker, cursor, name)
            source = MqttiumSession(broker.address, 'expiry-source-' + implementation,
                                    clean_start=True, expiry=0,
                                    events_path=case / f'{implementation}-source.jsonl')
            clients.append(source)
            await source.open(False)
            payload = ('queued-before-expiry-' + implementation).encode()
            await source.publish('expiry/offline/' + implementation, payload, qos=2)
            await source.disconnect()
            resumed = constructor(broker.address, name, expiry=1,
                                  events_path=case / f'{implementation}-before-deadline.jsonl')
            clients.append(resumed)
            await resumed.open(True)
            same = check_policy(session_row(temporal_snapshot(broker), name), 1)
            require(same['id'] == original['id'] and same.get('detached_by') is None,
                    'resume_before_expiry_changed_incarnation')
            received = await resumed.receive(payload)
            await resumed.disconnect()
            next_detach = await detached(broker, cursor, name)
            require(next_detach['detached_by'] != initial_detach['detached_by'],
                    'new_detachment_reused_occurrence')
            expired = await absent(broker, cursor, name)
            fresh = constructor(broker.address, name, expiry=0,
                                events_path=case / f'{implementation}-after-deadline.jsonl')
            clients.append(fresh)
            await fresh.open(False)
            recreated = session_row(temporal_snapshot(broker), name)
            require(identity(recreated) > identity(original), 'expired_session_identity_reused')
            await fresh.disconnect()
            await absent(broker, cursor, name)
            paths.append({'case': 'offline_before_and_after_deadline', 'implementation': implementation,
                          'original': original, 'initial_detach': initial_detach,
                          'resumed': same, 'received': received, 'next_detach': next_detach,
                          'expired_counts': expired['counts'], 'new_incarnation': recreated})

        # mqttium's public DISCONNECT has no properties. The independent Paho
        # API provides the override witness without private client attributes.
        name = 'expiry-disconnect-override'
        paho = PahoSession(broker.address, name, expiry=INFINITE,
                           events_path=case / 'paho-override-finite.jsonl')
        clients.append(paho); await paho.open(False)
        original = session_row(temporal_snapshot(broker), name)
        await paho.disconnect(3)
        finite = check_policy(await detached(broker, cursor, name), 3)
        resumed = PahoSession(broker.address, name, expiry=1,
                              events_path=case / 'paho-override-infinite.jsonl')
        clients.append(resumed); await resumed.open(True)
        require(session_row(temporal_snapshot(broker), name)['id'] == original['id'],
                'override_resume_changed_incarnation')
        await resumed.disconnect(INFINITE)
        kept = check_policy(await detached(broker, cursor, name), INFINITE)
        detachment(kept, finite=False)
        await asyncio.sleep(1.15)
        require(session_row(temporal_snapshot(broker), name)['id'] == original['id'],
                'infinite_override_expired')
        zero = MqttiumSession(broker.address, name, expiry=0,
                              events_path=case / 'mqttium-zero-policy.jsonl')
        clients.append(zero); await zero.open(True)
        require(session_row(temporal_snapshot(broker), name)['id'] == original['id'],
                'zero_override_discarded_resumed_state')
        await zero.disconnect(); await absent(broker, cursor, name)
        paths.append({'case': 'public_disconnect_override', 'client': 'paho',
                      'original': original, 'finite': finite, 'infinite': kept,
                      'mqttium_limitation': 'public disconnect has no property argument'})

        for implementation, constructor in (('mqttium', MqttiumSession), ('paho', PahoSession)):
            for index, expiry in enumerate((None, 0, INFINITE)):
                name = f'expiry-regression-{implementation}-{index}'
                client = constructor(broker.address, name, clean_start=True, expiry=expiry,
                                     events_path=case / f'regression-{implementation}-{index}.jsonl')
                clients.append(client); await client.open(False)
                old = check_policy(session_row(temporal_snapshot(broker), name), expiry)
                await client.disconnect()
                if expiry == INFINITE:
                    ender = PahoSession(broker.address, name, expiry=INFINITE,
                                        events_path=case / f'ender-{implementation}.jsonl')
                    clients.append(ender); await ender.open(True); await ender.disconnect(0)
                await absent(broker, cursor, name)
                paths.append({'case': 'zero_infinity_regression', 'implementation': implementation,
                              'requested_expiry': expiry, 'old': old})
        final = temporal_snapshot(broker)
        require(final['counts']['sessions'] == 0 and final['terminal_sessions'] == 0,
                'public_expiry_metadata_not_drained')
        write_json(case / 'durable-final.json', final)
        broker.stop()
        return {'criterion': 'B', 'status': 'PASS', 'ready': ready, 'paths': paths,
                'clients': [client.result for client in clients], 'final': final,
                'stop_status': broker.process.returncode}
    finally:
        await close_clients(clients)
        broker.cleanup()


async def shared_responsibilities(binary, case):
    broker = Broker(binary, case)
    clients = []
    try:
        broker.ready(); cursor = EventCursor(broker)
        topic, payload = 'expiry/shared', b'other-destination-survives-expiration'
        short = MqttiumSession(broker.address, 'expiry-shared-short', expiry=2,
                               events_path=case / 'short.jsonl')
        kept = PahoSession(broker.address, 'expiry-shared-kept', expiry=INFINITE,
                           events_path=case / 'kept.jsonl')
        clients.extend((short, kept))
        await short.open(False); await short.subscribe(topic)
        await kept.open(False); await kept.subscribe(topic)
        short_row = session_row(temporal_snapshot(broker), short.client_id)
        kept_row = session_row(temporal_snapshot(broker), kept.client_id)
        await short.disconnect(); await detached(broker, cursor, short.client_id)
        await kept.disconnect()
        source = MqttiumSession(broker.address, 'expiry-shared-source', expiry=1,
                                events_path=case / 'source.jsonl')
        clients.append(source); await source.open(False)
        source_row = session_row(temporal_snapshot(broker), source.client_id)
        await source.publish(topic, payload, qos=2)
        admitted = durable(broker.db)
        obligations = admitted['tables']['obligations']
        expected = [row for row in obligations if row['target'] == kept_row['id']]
        require(len(obligations) == 2 and len(expected) == 1
                and len(admitted['tables']['payloads']) == 1, 'expiry_shared_admission_missing')
        await source.disconnect(); await detached(broker, cursor, source.client_id)
        await absent(broker, cursor, short.client_id)
        await absent(broker, cursor, source.client_id)
        after = durable(broker.db)
        require(after['tables']['obligations'] == expected
                and after['tables']['payloads'] == admitted['tables']['payloads'],
                'expiration_cancelled_other_destination')
        require(not any(row['id'] in (source_row['id'], short_row['id'])
                        for row in after['tables']['sessions']), 'expired_source_or_target_survived')
        require(int(temporal_snapshot(broker)['metadata']['used_references'], 16) == 1,
                'expiry_shared_reference_count')
        recipient = PahoSession(broker.address, kept.client_id, expiry=INFINITE,
                                events_path=case / 'recipient-resumed.jsonl')
        clients.append(recipient); await recipient.open(True)
        require(session_row(temporal_snapshot(broker), kept.client_id)['id'] == kept_row['id'],
                'other_destination_identity_changed')
        received = await recipient.receive(payload)
        settled = await state_until(broker, cursor, lambda state: all(state['counts'][table] == 0
            for table in ('obligations', 'outbound', 'publications', 'payloads')),
            reason='expiry_shared_last_reference_not_released')
        require(int(settled['metadata']['used_references'], 16) == 0
                and int(settled['metadata']['used_payload_bytes'], 16) == 0,
                'expiry_shared_credits_not_returned')
        await recipient.disconnect(0); await absent(broker, cursor, kept.client_id)
        write_json(case / 'admitted.json', admitted); write_json(case / 'after-expiration.json', after)
        write_json(case / 'after-last-reference.json', settled)
        broker.stop()
        return {'criterion': 'E', 'status': 'PASS', 'short': short_row, 'source': source_row,
                'survivor': kept_row, 'original_surviving_obligation': expected[0],
                'received': received, 'after_expiration': after, 'settled': settled,
                'clients': [client.result for client in clients], 'stop_status': broker.process.returncode}
    finally:
        await close_clients(clients); broker.cleanup()


async def capacity_and_rearming(binary, case):
    broker = Broker(binary, case)
    clients, rows = [], []
    try:
        ready = broker.ready(); cursor = EventCursor(broker)
        witness = MqttiumSession(broker.address, 'expiry-infinite-witness', expiry=INFINITE,
                                 events_path=case / 'witness.jsonl')
        clients.append(witness); await witness.open(False); await witness.subscribe('expiry/capacity')
        witness_row = session_row(temporal_snapshot(broker), witness.client_id)
        await witness.disconnect()
        # Thirty seconds allows this bounded fill to finish before the first
        # expiry; the full inventory is checked instead of assuming timing.
        for index in range(QUOTA - 1):
            client = MqttiumSession(broker.address, f'expiry-capacity-{index}', expiry=30,
                                    events_path=case / f'fill-{index:03}.jsonl')
            clients.append(client); await client.open(False)
            row = session_row(temporal_snapshot(broker), client.client_id)
            await client.disconnect(); await detached(broker, cursor, client.client_id)
            rows.append(row)
        full = temporal_snapshot(broker)
        require(full['active_sessions'] == QUOTA and full['terminal_sessions'] == 0,
                'expiry_capacity_not_really_full')
        rejected = MqttiumSession(broker.address, 'expiry-capacity-rejected', expiry=0,
                                  events_path=case / 'rejected.jsonl')
        clients.append(rejected)
        try:
            await rejected.open(False)
        except Exception as error:
            require(getattr(error, 'reason_code', None) == 0x97,
                    'expiry_capacity_wrong_refusal', type(error).__name__)
        else:
            raise ReceiptFailure('expiry_capacity_evicted_valid_session')
        after_refusal = temporal_snapshot(broker)
        require({row['id'] for row in after_refusal['sessions']} == {row['id'] for row in full['sessions']},
                'expiry_capacity_evicted_valid_session')
        freed = await state_until(broker, cursor, lambda state: state['active_sessions'] < QUOTA,
                                  reason='expiration_did_not_return_capacity', timeout=45)
        require(session_row(freed, witness.client_id)['id'] == witness_row['id'],
                'expiration_evicted_infinite_witness')
        newcomer = MqttiumSession(broker.address, 'expiry-capacity-new', expiry=0,
                                  events_path=case / 'newcomer.jsonl')
        clients.append(newcomer); await newcomer.open(False)
        newcomer_row = session_row(temporal_snapshot(broker), newcomer.client_id)
        await newcomer.disconnect(); await absent(broker, cursor, newcomer.client_id)
        drained = await state_until(broker, cursor, lambda state: state['active_sessions'] == 1,
                                    reason='expiration_capacity_backlog_not_drained', timeout=45)
        require(session_row(drained, witness.client_id)['id'] == witness_row['id'],
                'expiration_evicted_infinite_witness')
        windows = []
        for index in range(REARMS):
            client = MqttiumSession(broker.address, 'expiry-rearming', expiry=10,
                                    events_path=case / f'rearm-{index:03}.jsonl')
            clients.append(client); await client.open(index != 0)
            current = session_row(temporal_snapshot(broker), client.client_id)
            await client.disconnect()
            closed = await detached(broker, cursor, client.client_id)
            require(closed['opened_by'] == current['opened_by'], 'rearm_detached_wrong_attachment')
            if index % 8 == 0 or index == REARMS - 1:
                cursor.read(); sample = temporal_snapshot(broker)
                inventory = [event for event in cursor.recent if event['kind'] == 'expiry_inventory']
                require(bool(inventory), 'expiry_inventory_missing')
                details = inventory[-1]['details']
                # The product must report the separate session-expiry index;
                # causal command deadlines are not a substitute for its size.
                require('finite_timers' in details and 0 <= details['finite_timers'] <= QUOTA,
                        'expiry_timer_history_grew')
                require(sample['counts']['sessions'] == 2 and sample['terminal_sessions'] == 0
                        and sample['counts']['session_clock'] == 1,
                        'expiry_rearm_durable_history_grew')
                windows.append({'index': index, 'sample': sample, 'inventory': inventory[-1]})
        finish = PahoSession(broker.address, 'expiry-rearming', expiry=INFINITE,
                             events_path=case / 'rearm-finish.jsonl')
        clients.append(finish); await finish.open(True); await finish.disconnect(0)
        await absent(broker, cursor, finish.client_id)
        source = PahoSession(broker.address, 'expiry-witness-source', expiry=0,
                             events_path=case / 'witness-source.jsonl')
        clients.append(source); await source.open(False)
        await source.publish('expiry/capacity', b'infinite-witness-still-owed', qos=2)
        await source.disconnect(); await absent(broker, cursor, source.client_id)
        resumed = MqttiumSession(broker.address, witness.client_id, expiry=INFINITE,
                                 events_path=case / 'witness-resumed.jsonl')
        clients.append(resumed); await resumed.open(True)
        require(session_row(temporal_snapshot(broker), witness.client_id)['id'] == witness_row['id'],
                'expiration_evicted_infinite_witness')
        received = await resumed.receive(b'infinite-witness-still-owed')
        await resumed.disconnect()
        final = temporal_snapshot(broker)
        write_json(case / 'capacity-full.json', full); write_json(case / 'capacity-final.json', final)
        write_json(case / 'rearm-windows.json', windows)
        broker.stop(); writer_available(broker.db)
        return {'criterion': 'F', 'status': 'PASS', 'ready': ready, 'quota': QUOTA,
                'same_pid': broker.process.pid, 'same_file_inode': broker.db.stat().st_ino,
                'full': full, 'rejected': rejected.result, 'after_refusal': after_refusal,
                'first_capacity_returned': freed, 'newcomer': newcomer_row,
                'detached_ttl_seconds': 30, 'rearms': REARMS, 'windows': windows,
                'infinite_witness': witness_row, 'received': received, 'final': final,
                'clients': [client.result for client in clients], 'stop_status': broker.process.returncode}
    finally:
        await close_clients(clients); broker.cleanup()


async def unknown_results(binary, case):
    broker = Broker(binary, case, test_control=True)
    clients, rows = [], []
    independent = None
    try:
        broker.ready(); cursor = EventCursor(broker)
        independent = SessionWire(broker.address, broker.observations, 'expiry-independent')
        independent.session_connect('expiry-independent', expiry=INFINITE, expected_present=False)
        for action in ('detach', 'expire'):
            name = 'expiry-unknown-' + action
            client = PahoSession(broker.address, name, expiry=1,
                                 events_path=case / f'{action}.jsonl')
            clients.append(client); await client.open(False)
            original = session_row(temporal_snapshot(broker), name)
            hold = broker.control / f'hold-session-{action}-result'
            hold.touch(exist_ok=False); cursor.read(); before = cursor.last
            await client.disconnect()
            held = await fact(broker, cursor, 'store_result_held', after=before,
                              predicate=lambda event: event.get('command_kind') == 'Session' + action.title())
            operation = held['operation']
            require(held['details']['outcome'] == 'Committed', 'expiry_held_result_not_committed')
            committed = temporal_snapshot(broker)
            if action == 'detach':
                row = session_row(committed, name)
                proof = detachment(row)
                require(proof['operation'] == operation_hex(operation),
                        'expiry_detachment_operation_changed')
            else:
                require(not has_client(committed, name), 'expiry_held_finish_not_committed')
                row = proof = None
            independent.send(b'\xc0\x00')
            require(independent.wait(0xD0).body == b'', 'expiry_unknown_blocked_control')
            unknown = await fact(broker, cursor, 'store_resolved', after=int(held['seq']),
                                 predicate=lambda event: event.get('operation') == operation)
            require(unknown['details']['outcome'] == 'pendingorunknown',
                    'expiry_held_resolution_became_certain')
            require(not any(event['kind'] == 'session_changed' and event.get('operation') == operation
                            for event in cursor.recent), 'expiry_unknown_result_applied_early')
            hold.unlink(); broker.observations.record('expiry_result_released', operation=operation)
            known = await fact(broker, cursor, 'session_changed', after=int(held['seq']),
                               predicate=lambda event: event.get('operation') == operation)
            require(known['details']['session'] == str(identity(original)),
                    'expiry_resolution_changed_session')
            if action == 'detach':
                # If a due expiration already followed the known detach, its
                # command still follows this original fixed deadline. The
                # immutable committed snapshot is the authority under hold.
                still = temporal_snapshot(broker)
                if has_client(still, name):
                    require(session_row(still, name)['deadline'] == row['deadline'],
                            'expiry_deadline_rearmed_after_resolution')
            await absent(broker, cursor, name)
            rows.append({'action': action, 'original': original, 'held': held,
                         'committed_while_unknown': committed, 'detachment': proof,
                         'unknown_resolution': unknown, 'known': known})
        independent.session_disconnect(0); independent = None
        await absent(broker, cursor, 'expiry-independent')
        broker.stop()
        return {'criterion': 'D-unknown', 'status': 'PASS', 'cases': rows,
                'clients': [client.result for client in clients], 'stop_status': broker.process.returncode}
    finally:
        if independent is not None:
            independent.close()
        for hold in broker.control.glob('hold-session-*-result'):
            hold.unlink()
        await close_clients(clients); broker.cleanup()


async def product_crashes(binary, case):
    cases = []
    for action, phase in (('detach', 'prepared'), ('detach', 'committed'),
                          ('expire', 'prepared'), ('expire', 'committed')):
        name = f'{action}-{phase}'
        directory = case / name
        point = f'session.{action}.{phase}'
        broker = Broker(binary, directory, pauses=(point,))
        restarted = wire = None
        try:
            broker.ready(); cursor = EventCursor(broker)
            wire = SessionWire(broker.address, broker.observations, name)
            wire.session_connect('expiry-crash', expiry=2, expected_present=False)
            original = session_row(temporal_snapshot(broker), 'expiry-crash')
            wire.session_disconnect(); wire = None
            barrier = await fact(broker, cursor, 'barrier_hit',
                                 predicate=lambda event: event.get('point') == point)
            committed = temporal_snapshot(broker)
            present = has_client(committed, 'expiry-crash')
            if action == 'detach':
                require(present, 'detach_crash_removed_session')
                row = session_row(committed, 'expiry-crash')
                require((row['detached_by'] is not None) == (phase == 'committed'),
                        'detach_crash_boundary_not_observed')
                if phase == 'committed':
                    require(row['detached_by'] == operation_hex(barrier['operation']),
                            'detach_crash_wrong_operation')
            else:
                require(present == (phase == 'prepared'), 'expiry_crash_boundary_not_observed')
            pid, inode = broker.process.pid, broker.db.stat().st_ino
            status = broker.stop(killed=True); writer_available(broker.db)
            restarted = Broker(binary, directory, db=broker.db, label='restart')
            ready = restarted.ready()
            recovered = temporal_snapshot(restarted)
            require(restarted.process.pid != pid and restarted.db.stat().st_ino == inode,
                    'expiry_crash_did_not_reuse_file')
            has_old = has_client(recovered, 'expiry-crash')
            if action == 'expire' and phase == 'committed':
                require(not has_old, 'committed_expiration_resurrected')
            elif action == 'detach' and phase == 'committed' and has_old:
                old = session_row(committed, 'expiry-crash')
                new = session_row(recovered, 'expiry-crash')
                require(new['id'] == old['id'] and new['deadline'] == old['deadline']
                        and new['detached_by'] == old['detached_by'],
                        'durable_detachment_rearmed_on_restart')
            # No assumption that unproven offline time was counted: readiness
            # may conservatively preserve a pre-commit expiration's old row.
            wire = SessionWire(restarted.address, restarted.observations, 'resumed')
            wire.session_connect('expiry-crash', expiry=0, expected_present=has_old)
            current = session_row(temporal_snapshot(restarted), 'expiry-crash')
            require((current['id'] == original['id']) == has_old,
                    'expiry_crash_wrong_session_identity')
            wire.session_disconnect(); wire = None
            restart_cursor = EventCursor(restarted)
            final = await absent(restarted, restart_cursor, 'expiry-crash')
            restarted.stop(); writer_available(restarted.db)
            write_json(directory / 'durable-at-barrier.json', committed)
            write_json(directory / 'durable-recovered.json', recovered)
            cases.append({'action': action, 'phase': phase, 'point': point, 'barrier': barrier,
                          'killed_pid': pid, 'wait_status': status, 'same_file_inode': inode,
                          'original': original, 'at_barrier': committed, 'restart_ready': ready,
                          'recovered': recovered, 'resumed': current, 'final': final,
                          'stop_status': restarted.process.returncode})
        finally:
            if wire is not None:
                wire.close()
            broker.cleanup()
            if restarted is not None:
                restarted.cleanup()
    cases.append(await connected_crash(binary, case / 'attached-at-crash'))
    return {'criterion': 'D-crashes', 'status': 'PASS', 'cases': cases,
            'scope': 'Each cut owns its SIGKILL/status/same-file witness; recognized clock-domain continuity is separate.'}


async def connected_crash(binary, case):
    broker = Broker(binary, case)
    restarted = wire = None
    try:
        broker.ready()
        wire = SessionWire(broker.address, broker.observations, 'attached-finite')
        wire.session_connect('expiry-attached-crash', expiry=4, expected_present=False)
        original = session_row(temporal_snapshot(broker), 'expiry-attached-crash')
        require(original['detached_by'] is None and original['deadline'] is None,
                'attached_session_had_offline_deadline')
        pid, inode = broker.process.pid, broker.db.stat().st_ino
        status = broker.stop(killed=True); writer_available(broker.db)
        wire.close(); wire = None
        restarted = Broker(binary, case, db=broker.db, label='restart')
        ready = restarted.ready()
        state = temporal_snapshot(restarted)
        recovered = session_row(state, 'expiry-attached-crash')
        require(recovered['id'] == original['id'] and recovered['opened_by'] == original['opened_by'],
                'attached_crash_changed_session_identity')
        detachment(recovered)
        require(int(recovered['deadline'], 16) == int(state['session_clock']['logical']) + 4_000_000_000,
                'attached_crash_did_not_capture_first_recovery_observation')
        require(restarted.process.pid != pid and restarted.db.stat().st_ino == inode,
                'expiry_crash_did_not_reuse_file')
        wire = SessionWire(restarted.address, restarted.observations, 'attached-resumed')
        wire.session_connect('expiry-attached-crash', expiry=0, expected_present=True)
        current = session_row(temporal_snapshot(restarted), 'expiry-attached-crash')
        require(current['id'] == original['id'] and current['detached_by'] is None,
                'attached_crash_resume_did_not_cancel_deadline')
        wire.session_disconnect(); wire = None
        cursor = EventCursor(restarted)
        final = await absent(restarted, cursor, 'expiry-attached-crash')
        restarted.stop(); writer_available(restarted.db)
        write_json(case / 'durable-before-kill.json', original)
        write_json(case / 'durable-recovered.json', state)
        return {'action': 'attached', 'phase': 'process-crash', 'point': None,
                'killed_pid': pid, 'wait_status': status, 'same_file_inode': inode,
                'original': original, 'recovered': recovered, 'restart_ready': ready,
                'resumed': current, 'final': final, 'stop_status': restarted.process.returncode}
    finally:
        if wire is not None:
            wire.close()
        broker.cleanup()
        if restarted is not None:
            restarted.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary-source-head', required=True)
    parser.add_argument('--case', choices=('normal', 'faults', 'B', 'E', 'F', 'unknown', 'crashes'), default='normal')
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).resolve().parents[3]
    head = subprocess.run(['git', '-C', str(source), 'rev-parse', 'HEAD'],
                          check=True, capture_output=True, text=True, timeout=3).stdout.strip()
    receipt = {'format': 'mqttiumd.finite-expiry-system-reception', 'schema_version': 1,
               'status': 'FAIL', 'qualification': 'system-test-only',
               'source_head': args.binary_source_head, 'harness_source_head': head,
               'client_identity': client_identity(), 'cases': [],
               'binary': {'path': str(args.binary.resolve()),
                          'sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest()}}
    require(head == args.binary_source_head, 'expiry_reception_source_mismatch')
    try:
        selected = {'normal': ('B', 'E', 'F'), 'faults': ('unknown', 'crashes')}.get(args.case, (args.case,))
        functions = {'B': public_paths, 'E': shared_responsibilities, 'F': capacity_and_rearming,
                     'unknown': unknown_results, 'crashes': product_crashes}
        for name in selected:
            receipt['cases'].append(asyncio.run(functions[name](args.binary.resolve(), args.out / name)))
        receipt['status'] = 'PASS'
    except Exception as error:
        receipt['failure'] = {'code': getattr(error, 'code', type(error).__name__), 'detail': str(error)[:512]}
    finally:
        write_json(args.out / 'receipt.json', receipt)
    print(json.dumps({'status': receipt['status'], 'failure': receipt.get('failure'),
                      'receipt': str(args.out / 'receipt.json')}))
    return 0 if receipt['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
