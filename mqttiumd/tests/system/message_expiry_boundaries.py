#!/usr/bin/env python3
"""Real Message Expiry process cuts and independently read durable outcomes."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import subprocess
import time

from external_wildcard import Subscription
from message_expiry_reception import (close_peers, peer, physically_drained, received_ingress,
                                       snapshot, until_state, one_delivery, until_connection_closed, expiry_stamp)
from pubsub_probe import durable
from tcp_probe import ReceiptFailure, operation_hex, require, write_json
from tcp_reception import Broker, writer_available


def startup_clock(broker):
    values = [event for event in broker.events() if event['kind'] == 'session_clock_anchor']
    require(len(values) == 1, 'message_expiry_restart_clock_anchor_missing')
    details = values[0]['details']
    domain = details.get('domain')
    require(isinstance(domain, str) and len(domain) == 64, 'message_expiry_restart_clock_domain_missing')
    return {'domain': domain, 'origin_status': details.get('origin_status'), 'event': values[0]}


def continuous(before, after):
    return (before['domain'] == after['domain'] and
            before['origin_status'] == after['origin_status'] == 'boottime_origin_recognized')


def _positive(value):
    if type(value) is int:
        return value > 0
    if isinstance(value, str) and value.isdecimal():
        return int(value) > 0
    return isinstance(value, (list, dict)) and bool(value)


def quarantine_events(broker):
    values = []
    for event in broker.events():
        if event['kind'] not in ('message_expiry_sweep', 'message_expiry_quarantined'):
            continue
        details = event.get('details', {})
        if event['kind'] == 'message_expiry_quarantined' or any(
                'quarantin' in key and _positive(value) for key, value in details.items()):
            values.append(event)
    return values


async def until_quarantined(broker):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        values = quarantine_events(broker)
        if values:
            return values
        require(broker.process.poll() is None, 'message_expiry_broker_exited')
        await asyncio.sleep(.01)
    raise ReceiptFailure('message_expiry_uncertain_work_not_explicitly_quarantined')


def canonical_start(state, event):
    identity = operation_hex(event['operation'])
    rows = [row for row in state['tables']['operations'] if row['id'] == identity]
    require(len(rows) == 1, 'message_expiry_started_receipt_missing')
    raw = bytes.fromhex(rows[0]['recipe'])
    require(raw[:7] == b'MQDR\x00\x01\x0f' and raw[7:39].hex() == identity,
            'message_expiry_started_canonical_recipe_changed')
    return {'operation': event['operation'], 'tag': 15, 'bytes': len(raw),
            'sha256': hashlib.sha256(raw).hexdigest()}


def canonical_timed(state, event, tag):
    """Read actual nested recipe lengths/identities without a product decoder."""
    identity = operation_hex(event['operation'])
    rows = [row for row in state['tables']['operations'] if row['id'] == identity]
    require(len(rows) == 1, 'message_expiry_timed_receipt_missing')
    raw = bytes.fromhex(rows[0]['recipe'])
    require(raw[:7] == b'MQDR\x00\x01' + bytes([tag]), 'message_expiry_timed_canonical_tag_changed')
    current = raw
    path = []
    children = {13: (1, 10, 11), 14: (12,), 10: (1,), 12: (7, 9)}
    for _ in range(4):
        require(len(current) >= 7 and current[:6] == b'MQDR\x00\x01',
                'message_expiry_timed_canonical_header_changed')
        kind = current[6]
        path.append(kind)
        if kind not in children:
            require(current[7:39].hex() == identity, 'message_expiry_timed_canonical_identity_changed')
            break
        require(len(current) >= 11, 'message_expiry_timed_canonical_truncated')
        size = int.from_bytes(current[7:11], 'big')
        require(39 <= size <= 65536 and 11 + size <= len(current) and
                current[17] in children[kind], 'message_expiry_timed_canonical_inner_changed')
        current = current[11:11 + size]
    else:
        raise ReceiptFailure('message_expiry_timed_canonical_nesting_budget')
    size = int.from_bytes(raw[7:11], 'big')
    tail = raw[11 + size:]
    require(len(tail) == (44 if tag == 13 else 40), 'message_expiry_timed_canonical_clock_changed')
    return {'operation': event['operation'], 'tag': tag, 'nested_tags': path,
            'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
            'domain': tail[:32].hex(), 'tick': str(int.from_bytes(tail[32:40], 'big')),
            'interval': int.from_bytes(tail[40:44], 'big') if tag == 13 else None}


def canonical_expire(state, event, claim, stamp):
    identity = operation_hex(event['operation'])
    rows = [row for row in state['tables']['operations'] if row['id'] == identity]
    require(len(rows) == 1, 'message_expiry_expire_receipt_missing')
    raw = bytes.fromhex(rows[0]['recipe'])
    require(len(raw) == 209 and raw[:7] == b'MQDR\x00\x01\x10' and raw[7:39].hex() == identity,
            'message_expiry_expire_canonical_identity_changed')
    require(raw[39:71].hex() == claim['publication'] and int.from_bytes(raw[71:75], 'big') == claim['idx'] and
            raw[75:83].hex() == claim['target'] and raw[83:91].hex() == claim['target_revision'] and
            int.from_bytes(raw[91:93], 'big') == claim['packet_id'] and raw[93:125].hex() == claim['claim'],
            'message_expiry_expire_canonical_claim_changed')
    original = {'domain': raw[125:157].hex(), 'tick': str(int.from_bytes(raw[157:165], 'big')),
                'interval': int.from_bytes(raw[165:169], 'big')}
    observed = {'domain': raw[169:201].hex(), 'tick': str(int.from_bytes(raw[201:209], 'big'))}
    require(original == stamp and observed['domain'] == stamp['domain'] and
            int(observed['tick']) >= int(stamp['tick']) + stamp['interval'] * 1_000_000_000,
            'message_expiry_expire_canonical_clock_changed')
    return {'operation': event['operation'], 'tag': 16, 'bytes': len(raw),
            'sha256': hashlib.sha256(raw).hexdigest(), 'original': original, 'observed': observed}


def recipe_quarantine_inventory(before, after, kind, committed, command_operation):
    require(after['counts']['retained'] == after['counts']['publications'] ==
            after['counts']['payloads'] == 1, 'message_expiry_recipe_uncertain_value_not_retained')
    return {'original_publication': after['tables']['retained'][0]['publication'],
            'publication_ids': [row['id'] for row in after['tables']['publications']],
            'payload': after['tables']['payloads'][0]['id'],
            'references': int(after['tables']['payloads'][0]['refs'], 16)}


async def settle(task):
    if task is None:
        return None
    result = await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=6)
    value = result[0]
    return {'completed': not isinstance(value, BaseException),
            'exception': type(value).__name__ if isinstance(value, BaseException) else None}


async def witness(broker, case, peers, target, tap, label):
    source, source_tap = await peer('mqttium', broker, case, label + '-witness-source', session_expiry=0)
    peers.append((source, source_tap))
    payload = ('fresh-timeless-after-' + label).encode()
    await source.publish('message-expiry/cut/witness', payload, 1)
    row = await target.receive(payload, 1, False)
    wire = one_delivery(tap, payload, 1, False, None)
    return {'message': row, 'wire': wire}


async def transfer_cut(binary, case, phase):
    """Claim-only and uncommitted start proof quarantine; committed starts finish."""
    point = 'claim.committed' if phase == 'claim' else 'delivery.started.' + phase
    broker = Broker(binary, case, pauses=(point,))
    restart = None
    peers = []
    publication = None
    label = 'transfer-' + phase
    payload = ('expiring-' + label).encode()
    try:
        ready = broker.ready()
        before_clock = startup_clock(broker)
        source, source_tap = await peer('mqttium', broker, case, label + '-source', session_expiry=0)
        target, target_tap = await peer('paho', broker, case, label + '-target', client_id='cut-target')
        peers.extend(((source, source_tap), (target, target_tap)))
        await target.subscribe((Subscription('message-expiry/cut/#', 1, retain_handling=2),))
        marker = max(int(event['seq']) for event in broker.events())
        publication = asyncio.create_task(source.publish('message-expiry/cut/value', payload, 1, message_expiry=2))
        hit = await asyncio.to_thread(broker.wait_event, 'barrier_hit', point=point)
        held = snapshot(broker, 'barrier-held')
        require(held['counts']['outbound'] == held['counts']['obligations'] == 1 and
                held['counts']['publications'] == held['counts']['payloads'] == 1,
                'message_expiry_claim_cut_inventory_missing', repr(held['counts']))
        known_start = phase == 'committed'
        if phase == 'claim':
            require(hit.get('command_kind') == 'Claim', 'message_expiry_claim_cut_wrong_command')
            await target.no_message(.04)
            require(not target_tap.publications(payload), 'message_expiry_claim_already_emitted')
            emitted = None
            canonical = None
        else:
            require(hit.get('command_kind') == 'DeliveryStarted', 'message_expiry_started_cut_wrong_command')
            emitted = await target.receive(payload, 1, False, acknowledge=False)
            one_delivery(target_tap, payload, 1, False, 2)
            identity = operation_hex(hit['operation'])
            require(any(row['id'] == identity for row in held['tables']['operations']) == known_start,
                    'message_expiry_started_commit_boundary_not_established')
            canonical = canonical_start(held, hit) if known_start else None
        require(not [event for event in broker.events() if int(event['seq']) > marker and
                     event['kind'] == 'store_result' and event.get('command_kind') == 'DeliveryStarted'],
                'message_expiry_start_result_applied_before_cut')
        inode = broker.db.stat().st_ino
        kill_status = broker.stop(killed=True)
        writer_available(broker.db)
        original_receipt = await settle(publication)
        publication = None
        await close_peers(peers)
        peers = []
        await asyncio.sleep(2.15)
        restart = Broker(binary, case, db=broker.db, label='restarted')
        restart.ready()
        require(restart.db.stat().st_ino == inode, 'message_expiry_cut_database_was_replaced')
        after_clock = startup_clock(restart)
        target, tap = await peer('paho', restart, case, label + '-resumed-target',
                                 client_id='cut-target', present=True)
        peers.append((target, tap))
        if known_start:
            replay = await target.receive(payload, 1, False)
            values = tap.publications(payload)
            require(len(values) == 1 and values[0].dup and values[0].message_expiry_interval is not None and
                    0 <= values[0].message_expiry_interval <= 2,
                    'message_expiry_known_started_transfer_not_recovered')
            final = await until_state(restart, physically_drained, 'message_expiry_recovered_started_ack_not_finished')
            quarantined = []
            result_wire = values[0].observation()
        else:
            await target.no_message(.1)
            require(not tap.publications(payload), 'message_expiry_unproved_start_replayed_as_fresh')
            quarantined = await until_quarantined(restart)
            final = snapshot(restart, 'quarantined')
            require(final['counts']['outbound'] == final['counts']['obligations'] == 1 and
                    final['counts']['publications'] == final['counts']['payloads'] == 1,
                    'message_expiry_unproved_claim_quarantine_not_retained', repr(final['counts']))
            replay = result_wire = None
        fresh = await witness(restart, case, peers, target, tap, label)
        await target.no_message(.04)
        write_json(case / 'recovered-final.sql.json', final)
        await close_peers(peers)
        peers = []
        restart.stop()
        writer_available(restart.db)
        return {'criterion': label, 'status': 'PASS', 'ready': ready, 'barrier': hit,
                'killed_pid': broker.process.pid, 'kill_status': kill_status, 'same_sqlite_inode': inode,
                'same_database': str(broker.db), 'known_started_proof': known_start,
                'pre_cut_client_delivery': emitted, 'pre_cut_canonical': canonical,
                'source_original_receipt': original_receipt, 'clocks': [before_clock, after_clock],
                'recognized_continuity': continuous(before_clock, after_clock),
                'quarantine': quarantined, 'replayed': replay, 'replayed_wire': result_wire,
                'fresh_timeless_witness': fresh, 'final_counts': final['counts'],
                'restart_status': restart.process.returncode}
    finally:
        if broker.process.poll() is None:
            broker.cleanup()
        if publication is not None:
            await settle(publication)
        await close_peers(peers, cleanup=True)
        if restart is not None:
            if restart.db.is_file():
                snapshot(restart, 'last-observed')
            restart.cleanup()
        broker.cleanup()


async def sweep_cut(binary, case, phase):
    """The receiptless physical expiry sweep is cut in its actual transaction."""
    point = 'message.expire.' + phase
    broker = Broker(binary, case, pauses=(point,))
    restart = None
    peers = []
    try:
        ready = broker.ready()
        before_clock = startup_clock(broker)
        source, source_tap = await peer('mqttium', broker, case, 'sweep-source', session_expiry=0)
        peers.append((source, source_tap))
        marker = max(int(event['seq']) for event in broker.events())
        await source.publish('message-expiry/cut/value', b'expiring-receiptless-sweep', 1,
                             retain=True, message_expiry=1)
        ingress = await received_ingress(broker, 'message-expiry/cut/value', marker)
        admitted = snapshot(broker, 'sweep-admitted')
        require(admitted['counts']['retained'] == admitted['counts']['payloads'] == 1,
                'message_expiry_sweep_cut_not_admitted')
        hit = await asyncio.to_thread(broker.wait_event, 'barrier_hit', point=point)
        require('operation' not in hit, 'message_expiry_receiptless_sweep_invented_operation')
        held = snapshot(broker, 'sweep-barrier-held')
        committed = phase == 'committed'
        require(physically_drained(held) if committed else held['counts']['retained'] ==
                held['counts']['publications'] == held['counts']['payloads'] == 1,
                'message_expiry_sweep_transaction_boundary_not_established', repr(held['counts']))
        inode = broker.db.stat().st_ino
        kill_status = broker.stop(killed=True)
        writer_available(broker.db)
        await close_peers(peers)
        peers = []
        restart = Broker(binary, case, db=broker.db, label='restarted')
        restart.ready()
        require(restart.db.stat().st_ino == inode, 'message_expiry_sweep_database_was_replaced')
        after_clock = startup_clock(restart)
        same_domain = continuous(before_clock, after_clock)
        if committed or same_domain:
            final = await until_state(restart, physically_drained, 'message_expiry_sweep_not_physically_recovered')
            quarantined = []
            outcome = 'physical-reclamation'
        else:
            quarantined = await until_quarantined(restart)
            final = snapshot(restart, 'sweep-quarantined')
            require(final['counts']['retained'] == final['counts']['publications'] ==
                    final['counts']['payloads'] == 1, 'message_expiry_unknown_domain_lost_quarantine')
            outcome = 'quarantined-unproved-domain'
        target, tap = await peer('paho', restart, case, 'sweep-new-target', session_expiry=0)
        peers.append((target, tap))
        await target.subscribe((Subscription('message-expiry/cut/#', 1),))
        await target.no_message(.08)
        require(not tap.publications(), 'message_expiry_sweep_expired_or_uncertain_value_became_fresh')
        fresh = await witness(restart, case, peers, target, tap, 'sweep-' + phase)
        write_json(case / 'sweep-recovered.sql.json', final)
        await close_peers(peers)
        peers = []
        restart.stop()
        writer_available(restart.db)
        return {'criterion': 'sweep-' + phase, 'status': 'PASS', 'ready': ready,
                'ingress': ingress, 'barrier': hit, 'killed_pid': broker.process.pid,
                'kill_status': kill_status, 'same_sqlite_inode': inode, 'same_database': str(broker.db),
                'committed_before_kill': committed, 'clocks': [before_clock, after_clock],
                'recognized_continuity': same_domain, 'recovery_outcome': outcome,
                'quarantine': quarantined, 'final_counts': final['counts'],
                'fresh_timeless_witness': fresh, 'restart_status': restart.process.returncode}
    finally:
        await close_peers(peers, cleanup=True)
        if restart is not None:
            if restart.db.is_file():
                snapshot(restart, 'last-observed')
            restart.cleanup()
        broker.cleanup()


async def restart_offline(binary, case):
    """A normal process restart never grants a new TTL to retained/offline work."""
    broker = Broker(binary, case)
    restart = None
    peers = []
    payload = b'original-expiry-survives-normal-process-restart'
    try:
        ready = broker.ready()
        before_clock = startup_clock(broker)
        target, tap = await peer('paho', broker, case, 'restart-target', client_id='restart-target')
        await target.subscribe((Subscription('message-expiry/cut/#', 1, retain_handling=2),))
        await close_peers(((target, tap),))
        await until_connection_closed(broker, 'restart-target')
        source, source_tap = await peer('mqttium', broker, case, 'restart-source', session_expiry=0)
        peers.append((source, source_tap))
        marker = max(int(event['seq']) for event in broker.events())
        await source.publish('message-expiry/cut/value', payload, 2, retain=True, message_expiry=2)
        ingress = await received_ingress(broker, 'message-expiry/cut/value', marker)
        admitted = await until_state(broker, lambda state: state['counts']['obligations'] ==
                                    state['counts']['retained'] == state['counts']['publications'] ==
                                    state['counts']['payloads'] == 1 and state['counts']['outbound'] == 0,
                                    'message_expiry_restart_positive_inventory_missing')
        write_json(case / 'restart-before.sql.json', admitted)
        await close_peers(peers)
        peers = []
        inode = broker.db.stat().st_ino
        stop_status = broker.stop()
        writer_available(broker.db)
        await asyncio.sleep(2.15)
        restart = Broker(binary, case, db=broker.db, label='restarted')
        restart.ready()
        require(restart.db.stat().st_ino == inode, 'message_expiry_restart_database_was_replaced')
        after_clock = startup_clock(restart)
        same_domain = continuous(before_clock, after_clock)
        if same_domain:
            final = await until_state(restart, physically_drained, 'message_expiry_restart_old_deadline_reset')
            quarantined = []
            outcome = 'physical-reclamation'
        else:
            quarantined = await until_quarantined(restart)
            final = snapshot(restart, 'restart-quarantined')
            require(final['counts']['retained'] == final['counts']['obligations'] ==
                    final['counts']['publications'] == final['counts']['payloads'] == 1,
                    'message_expiry_restart_unproved_domain_lost_quarantine')
            outcome = 'quarantined-unproved-domain'
        target, tap = await peer('paho', restart, case, 'restart-resumed-target',
                                 client_id='restart-target', present=True)
        peers.append((target, tap))
        await target.no_message(.08)
        require(not tap.publications(payload), 'message_expiry_restart_expired_pending_work_replayed')
        newcomer, newcomer_tap = await peer('mqttium', restart, case, 'restart-new-wildcard', session_expiry=0)
        peers.append((newcomer, newcomer_tap))
        await newcomer.subscribe((Subscription('message-expiry/cut/#', 1),))
        await newcomer.no_message(.08)
        require(not newcomer_tap.publications(payload), 'message_expiry_restart_expired_retained_work_replayed')
        await newcomer.unsubscribe(('message-expiry/cut/#',))
        fresh = await witness(restart, case, peers, target, tap, 'normal-restart')
        write_json(case / 'restart-final.sql.json', final)
        await close_peers(peers)
        peers = []
        restart.stop()
        writer_available(restart.db)
        return {'criterion': 'normal-restart', 'status': 'PASS', 'ready': ready, 'ingress': ingress,
                'same_sqlite_inode': inode, 'same_database': str(broker.db), 'stop_status': stop_status,
                'clocks': [before_clock, after_clock], 'recognized_continuity': same_domain,
                'recovery_outcome': outcome, 'quarantine': quarantined, 'final_counts': final['counts'],
                'fresh_timeless_witness': fresh, 'restart_status': restart.process.returncode}
    finally:
        await close_peers(peers, cleanup=True)
        if restart is not None:
            if restart.db.is_file():
                snapshot(restart, 'last-observed')
            restart.cleanup()
        broker.cleanup()


async def recipe_cut(binary, case, value):
    """Timed admission/replay commits keep their frozen canonical time fields."""
    kind, phase = value
    prefix = {'qos1': 'admission', 'qos0': 'retained', 'replay': 'subscription'}[kind]
    point = prefix + '.' + phase
    broker = Broker(binary, case, pauses=(point,))
    restart = None
    peers = []
    action = None
    payload = f'expiring-recipe-cut:{kind}:{phase}'.encode()
    try:
        ready = broker.ready()
        before_clock = startup_clock(broker)
        source, source_tap = await peer('mqttium', broker, case, 'recipe-source', session_expiry=0)
        target, tap = await peer('paho', broker, case, 'recipe-target', client_id='recipe-target')
        peers.extend(((source, source_tap), (target, tap)))
        if kind == 'replay':
            await source.publish('message-expiry/cut/value', payload, 1, retain=True, message_expiry=2)
            action = asyncio.create_task(target.subscribe((Subscription('message-expiry/cut/#', 1),)))
        else:
            await target.subscribe((Subscription('message-expiry/cut/#', 1, retain_handling=2),))
            action = asyncio.create_task(source.publish('message-expiry/cut/value', payload,
                                        0 if kind == 'qos0' else 1, retain=True, message_expiry=2))
        hit = await asyncio.to_thread(broker.wait_event, 'barrier_hit', point=point)
        expected_kind = {'qos1': 'Admission', 'qos0': 'RetainedQoS0', 'replay': 'Subscriptions'}[kind]
        require(hit.get('command_kind') == expected_kind, 'message_expiry_recipe_cut_wrong_command')
        held = snapshot(broker, 'recipe-barrier-held')
        committed = phase == 'committed'
        identity = operation_hex(hit['operation'])
        require(any(row['id'] == identity for row in held['tables']['operations']) == committed,
                'message_expiry_recipe_cut_commit_boundary_missing')
        canonical = canonical_timed(held, hit, 14 if kind == 'replay' else 13) if committed else None
        require(held['counts']['retained'] == int(kind == 'replay' or committed),
                'message_expiry_recipe_cut_retained_boundary_missing')
        await target.no_message(.04)
        require(not tap.publications(payload), 'message_expiry_recipe_emitted_before_known_commit')
        inode = broker.db.stat().st_ino
        kill_status = broker.stop(killed=True)
        writer_available(broker.db)
        action_result = await settle(action)
        action = None
        await close_peers(peers)
        peers = []
        await asyncio.sleep(2.15)
        restart = Broker(binary, case, db=broker.db, label='restarted')
        restart.ready()
        require(restart.db.stat().st_ino == inode, 'message_expiry_recipe_cut_database_was_replaced')
        after_clock = startup_clock(restart)
        same_domain = continuous(before_clock, after_clock)
        has_value = kind == 'replay' or committed
        if same_domain or not has_value:
            final = await until_state(restart, physically_drained, 'message_expiry_recipe_original_deadline_reset')
            quarantined = []
            outcome = 'physical-reclamation' if has_value else 'admission-not-committed'
        else:
            quarantined = await until_quarantined(restart)
            final = snapshot(restart, 'recipe-quarantined')
            recipe_quarantine_inventory(held, final, kind, committed, identity)
            outcome = 'quarantined-unproved-domain'
        target, tap = await peer('paho', restart, case, 'recipe-resumed-target',
                                 client_id='recipe-target', present=True)
        peers.append((target, tap))
        await target.subscribe((Subscription('message-expiry/cut/#', 1, retain_handling=2),))
        await target.no_message(.08)
        require(not tap.publications(payload), 'message_expiry_recipe_recovered_as_fresh')
        fresh = await witness(restart, case, peers, target, tap, kind + '-' + phase)
        write_json(case / 'recipe-recovered.sql.json', final)
        await close_peers(peers)
        peers = []
        restart.stop()
        writer_available(restart.db)
        return {'criterion': f'recipe-{kind}-{phase}', 'status': 'PASS', 'ready': ready,
                'barrier': hit, 'canonical': canonical, 'killed_pid': broker.process.pid,
                'kill_status': kill_status, 'same_sqlite_inode': inode, 'same_database': str(broker.db),
                'action_original_receipt': action_result, 'clocks': [before_clock, after_clock],
                'recognized_continuity': same_domain, 'recovery_outcome': outcome, 'quarantine': quarantined,
                'final_counts': final['counts'], 'fresh_timeless_witness': fresh,
                'restart_status': restart.process.returncode}
    finally:
        if broker.process.poll() is None:
            broker.cleanup()
        if action is not None:
            await settle(action)
        await close_peers(peers, cleanup=True)
        if restart is not None:
            if restart.db.is_file():
                snapshot(restart, 'last-observed')
            restart.cleanup()
        broker.cleanup()


async def held_admission(binary, case):
    """An authentic unknown admission outcome cannot resurrect expired state."""
    broker = Broker(binary, case, test_control=True)
    peers = []
    action = None
    payload = b'expired-while-the-original-admission-result-was-unknown'
    hold = broker.control / 'hold-admission-result'
    try:
        ready = broker.ready()
        target, tap = await peer('paho', broker, case, 'held-target', client_id='held-target')
        await target.subscribe((Subscription('message-expiry/cut/#', 1, retain_handling=2),))
        await close_peers(((target, tap),))
        await until_connection_closed(broker, 'held-target')
        source, source_tap = await peer('mqttium', broker, case, 'held-source', session_expiry=0)
        peers.append((source, source_tap))
        hold.touch(exist_ok=False)
        action = asyncio.create_task(source.publish('message-expiry/cut/value', payload, 1,
                                                    retain=True, message_expiry=1))
        held = await asyncio.to_thread(broker.wait_event, 'store_result_held')
        require(held.get('command_kind') == 'Admission' and held['details'].get('outcome') == 'Committed',
                'message_expiry_held_admission_not_committed')
        admitted = snapshot(broker, 'held-admission-committed')
        require(admitted['counts']['retained'] == admitted['counts']['obligations'] ==
                admitted['counts']['publications'] == admitted['counts']['payloads'] == 1,
                'message_expiry_held_admission_inventory_missing')
        canonical = canonical_timed(admitted, held, 13)
        require(canonical['interval'] == 1, 'message_expiry_held_original_ttl_changed')
        require(not action.done(), 'message_expiry_held_result_reached_publisher')
        expired = await until_state(broker, physically_drained, 'message_expiry_unknown_admission_blocked_physical_expiry')
        write_json(case / 'held-admission-expired.sql.json', expired)
        still = canonical_timed(expired, held, 13)
        require(still == canonical, 'message_expiry_held_recipe_changed_after_physical_expiry')
        hold.unlink()
        broker.observations.record('held_admission_result_released', operation=held['operation'])
        resolved = await asyncio.to_thread(broker.wait_event, 'store_resolved', after=int(held['seq']))
        require(resolved.get('operation') == held['operation'] and
                resolved['details'].get('outcome') == 'committed',
                'message_expiry_held_resolution_changed_original_operation')
        await asyncio.wait_for(action, timeout=5)
        action = None
        target, tap = await peer('paho', broker, case, 'held-resumed-target',
                                 client_id='held-target', present=True)
        peers.append((target, tap))
        await target.no_message(.08)
        require(not tap.publications(payload), 'message_expiry_resolved_admission_resurrected_expired_work')
        final = await until_state(broker, physically_drained, 'message_expiry_resolved_admission_recreated_references')
        fresh = await witness(broker, case, peers, target, tap, 'held-admission')
        write_json(case / 'held-admission-final.sql.json', final)
        await close_peers(peers)
        peers = []
        broker.stop()
        writer_available(broker.db)
        return {'criterion': 'held-admission', 'status': 'PASS', 'ready': ready,
                'held': held, 'resolved': resolved, 'same_operation': True, 'canonical': canonical,
                'expired_while_unknown_counts': expired['counts'], 'final_counts': final['counts'],
                'fresh_timeless_witness': fresh, 'process_status': broker.process.returncode}
    finally:
        if hold.exists():
            hold.unlink()
        if broker.process.poll() is None and action is not None:
            broker.cleanup()
        if action is not None:
            await settle(action)
        await close_peers(peers, cleanup=True)
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        broker.cleanup()


async def certain_unsent(binary, case):
    """A real zero-byte net refusal authorizes guarded tag16 reclamation."""
    point = 'message.delivery.expire.committed'
    broker = Broker(binary, case, pauses=('publish.emission', point))
    peers = []
    fresh_action = None
    payload = b'claimed-before-ttl-but-certainly-never-started'
    try:
        ready = broker.ready()
        source, source_tap = await peer('mqttium', broker, case, 'unsent-source', session_expiry=0)
        target, tap = await peer('paho', broker, case, 'unsent-target', session_expiry=0)
        peers.extend(((source, source_tap), (target, tap)))
        await target.subscribe((Subscription('message-expiry/cut/#', 1, retain_handling=2),))
        await source.publish('message-expiry/cut/value', payload, 1, message_expiry=2)
        emitted = await asyncio.to_thread(broker.wait_event, 'barrier_hit', point='publish.emission')
        held = snapshot(broker, 'unsent-held')
        require(held['counts']['outbound'] == held['counts']['obligations'] ==
                held['counts']['publications'] == held['counts']['payloads'] == 1,
                'message_expiry_unsent_claim_missing')
        claim = held['tables']['outbound'][0]
        stamp = expiry_stamp(held['tables']['publications'][0])
        require(claim['started'] == 0 and operation_hex(emitted['operation']) == claim['claim'],
                'message_expiry_unsent_emission_claim_mismatch')
        await target.no_message(.04)
        require(not tap.publications(payload), 'message_expiry_unsent_emitted_before_release')
        await asyncio.sleep(2.15)
        past_ttl = snapshot(broker, 'unsent-past-ttl')
        require(past_ttl['counts']['outbound'] == past_ttl['counts']['obligations'] ==
                past_ttl['counts']['publications'] == past_ttl['counts']['payloads'] == 1,
                'message_expiry_unsent_freed_before_certain_refusal')
        broker.release('publish.emission')
        cancelled = await asyncio.to_thread(broker.wait_event, 'barrier_hit', point=point)
        require(cancelled.get('command_kind') == 'MessageExpire', 'message_expiry_unsent_wrong_cancellation')
        committed = snapshot(broker, 'unsent-cancellation-committed')
        canonical = canonical_expire(committed, cancelled, claim, stamp)
        require(physically_drained(committed), 'message_expiry_certain_refusal_did_not_physically_reclaim')
        never_started = [event for event in broker.events() if event['kind'] == 'message_transmission_not_started'
                         and int(event['seq']) > int(emitted['seq'])]
        require(len(never_started) == 1 and never_started[0]['details'].get('authority') == 'zero_socket_bytes'
                and never_started[0]['details'].get('reason') == 'Expired',
                'message_expiry_unsent_zero_byte_proof_missing')
        queued = [event for event in broker.events() if event['kind'] == 'frame_queued' and
                  event['details'].get('packet_kind') == 'PUBLISH' and
                  event['details'].get('effect') == never_started[0]['details'].get('effect')]
        claim_bytes = bytes.fromhex(claim['claim'])
        claim_text = '/'.join(str(int.from_bytes(claim_bytes[offset:offset + 8], 'big'))
                              for offset in range(0, 32, 8))
        require(len(queued) == 1 and queued[0]['details'].get('claim') == claim_text and
                queued[0]['details'].get('packet_id') == claim['packet_id'] and not [event for event in broker.events() if
                event.get('command_kind') == 'DeliveryStarted'], 'message_expiry_unsent_claim_was_started')
        await target.no_message(.04)
        require(not tap.publications(payload), 'message_expiry_unsent_expired_bytes_reached_target')
        broker.release(point)
        fresh_action = asyncio.create_task(witness(broker, case, peers, target, tap, 'certain-unsent'))
        fresh_barrier = await asyncio.to_thread(broker.wait_event, 'barrier_hit', point='publish.emission',
                                                after=int(cancelled['seq']))
        broker.release('publish.emission')
        fresh = await asyncio.wait_for(fresh_action, timeout=5)
        fresh_action = None
        final = await until_state(broker, physically_drained, 'message_expiry_unsent_cached_references_not_released')
        write_json(case / 'unsent-final.sql.json', final)
        await close_peers(peers)
        peers = []
        broker.stop()
        writer_available(broker.db)
        return {'criterion': 'certain-unsent', 'status': 'PASS', 'ready': ready,
                'emission_hold': emitted, 'claim': claim, 'original_stamp': stamp,
                'pending_past_ttl': past_ttl['counts'], 'zero_byte_refusal': never_started[0],
                'queued_frame': queued[0], 'cancellation_commit': cancelled, 'canonical': canonical,
                'reclaimed_counts': committed['counts'], 'fresh_emission_hold': fresh_barrier,
                'fresh_timeless_witness': fresh, 'final_counts': final['counts'],
                'process_status': broker.process.returncode}
    finally:
        if fresh_action is not None:
            broker.cleanup()
            await settle(fresh_action)
        await close_peers(peers, cleanup=True)
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        broker.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary-source-head', required=True)
    parser.add_argument('--case', choices=('all', 'claim', 'started-prepared', 'started-committed',
                                          'sweep-prepared', 'sweep-committed', 'restart', 'recipes',
                                          'held-admission', 'certain-unsent'), default='all')
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    head = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD']).decode().strip()
    receipt = {'format': 'mqttiumd.message-expiry-boundary-system-reception', 'schema_version': 1,
               'status': 'FAIL', 'qualification': 'system-test-only', 'source_head': args.binary_source_head,
               'harness_source_head': head, 'binary': {'path': str(args.binary.resolve()),
               'sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest()}, 'cases': []}
    try:
        require(head == args.binary_source_head, 'message_expiry_boundary_source_mismatch')
        functions = {
            'claim': (transfer_cut, 'claim'), 'started-prepared': (transfer_cut, 'prepared'),
            'started-committed': (transfer_cut, 'committed'),
            'sweep-prepared': (sweep_cut, 'prepared'), 'sweep-committed': (sweep_cut, 'committed'),
            'restart': (restart_offline, None), 'held-admission': (held_admission, None),
            'certain-unsent': (certain_unsent, None)}
        selected = tuple(functions) if args.case == 'all' else (args.case,)
        if args.case in ('all', 'recipes'):
            for kind in ('qos1', 'qos0', 'replay'):
                for phase in ('prepared', 'committed'):
                    name = f'recipe-{kind}-{phase}'
                    receipt['cases'].append(asyncio.run(recipe_cut(args.binary.resolve(), args.out / name,
                                                                  (kind, phase))))
        if args.case == 'recipes':
            selected = ()
        for name in selected:
            function, phase = functions[name]
            call = function(args.binary.resolve(), args.out / name) if phase is None else function(
                args.binary.resolve(), args.out / name, phase)
            receipt['cases'].append(asyncio.run(call))
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
