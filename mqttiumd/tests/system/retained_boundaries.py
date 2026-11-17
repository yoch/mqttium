#!/usr/bin/env python3
"""Actual SQLite retained/replay cuts and unknown results; system tests only."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

from external_wildcard import Subscription, subscribe_packet
from pubsub_probe import durable
from retained_probe import RetainedWire, retained_publish_packet
from tcp_probe import ReceiptFailure, operation_hex, require, write_json
from tcp_reception import Broker, writer_available


def inventory(state):
    require('retained' in state['tables'], 'retained_inventory_table_missing')
    return state['tables']['retained']


def snapshot(broker, label):
    state = durable(broker.db)
    write_json(broker.case / f'{label}.sql.json', state)
    return state


def wait_state(broker, predicate, code, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = durable(broker.db)
        if predicate(state): return state
        require(broker.process.poll() is None, 'retained_boundary_broker_exited')
        time.sleep(.01)
    raise ReceiptFailure(code)


def canonical_receipt(state, event, tag):
    identity = operation_hex(event['operation'])
    rows = [row for row in state['tables']['operations'] if row['id'] == identity]
    require(len(rows) == 1, 'retained_result_receipt_missing')
    raw = bytes.fromhex(rows[0]['recipe'])
    require(raw[:6] == b'MQDR\x00\x01' and raw[6] == tag, 'retained_result_canonical_tag_changed')
    if tag in (10, 12):
        size = int.from_bytes(raw[7:11], 'big')
        require(39 <= size <= 65536 and 11 + size <= len(raw)
                and raw[11:17] == b'MQDR\x00\x01'
                and raw[17] == (1 if tag == 10 else 9), 'retained_result_canonical_inner_invalid')
        offset = 18
    else:
        offset = 7
    require(len(raw) >= offset + 32 and raw[offset:offset + 32].hex() == identity,
            'retained_result_canonical_identity_changed')
    return {'operation': event['operation'], 'tag': tag, 'canonical_sha256': hashlib.sha256(raw).hexdigest(),
            'canonical_bytes': len(raw)}


def crash(binary, case, kind, phase):
    point = {'qos1': 'admission', 'qos0': 'retained', 'replay': 'subscription'}[kind] + '.' + phase
    broker = Broker(binary, case, pauses=(point,)); restart = None; source = target = None
    topic, payload = f'cut/{kind}/value', f'retained-cut:{kind}:{phase}'.encode()
    try:
        broker.ready()
        source = RetainedWire(broker.address, broker.observations, 'source')
        source.session_connect('retained-cut-source', expected_present=False)
        if kind == 'replay':
            source.publish(1, topic, payload, qos=2)
            target = RetainedWire(broker.address, broker.observations, 'target-before')
            target.session_connect('retained-cut-target', expected_present=False)
            sent = subscribe_packet(1, (Subscription('cut/#', 1),))
            target.send(sent)
        else:
            sent = retained_publish_packet(topic, payload, 1 if kind == 'qos1' else None,
                                           qos=1 if kind == 'qos1' else 0)
            source.send(sent)
        hit = broker.wait_event('barrier_hit', point=point)
        expected_kind = {'qos1': 'Admission', 'qos0': 'RetainedQoS0', 'replay': 'Subscriptions'}[kind]
        require(hit.get('command_kind') == expected_kind, 'retained_wrong_barrier_command')
        held = snapshot(broker, 'barrier-held')
        committed = phase == 'committed'
        identity = operation_hex(hit['operation'])
        require(any(row['id'] == identity for row in held['tables']['operations']) == committed,
                'retained_commit_boundary_not_established')
        if kind == 'replay':
            require(len(inventory(held)) == 1 and held['counts']['obligations'] == int(committed),
                    'retained_subscription_replay_not_atomic')
            require(held['counts']['routes'] == int(committed), 'retained_subscription_route_not_atomic')
            target.no_publication(.08, code='retained_replay_emitted_before_known_subscription_commit')
        else:
            require(len(inventory(held)) == int(committed), 'retained_publication_not_atomic')
            if kind == 'qos1':
                require(source.poll(.08) is None, 'premature_retained_publish_ack')
        inode = broker.db.stat().st_ino
        status = broker.stop(killed=True); writer_available(broker.db)
        source.close(); source = None
        if target is not None: target.close(); target = None
        restart = Broker(binary, case, db=broker.db, label='restarted'); restart.ready()
        require(restart.db.stat().st_ino == inode, 'retained_crash_restored_fixture')
        recovered = snapshot(restart, 'after-restart')
        require(inventory(recovered) == inventory(held), 'retained_crash_current_index_changed')
        if kind == 'replay':
            require(recovered['tables']['obligations'] == held['tables']['obligations'],
                    'retained_replay_recreated_after_crash')
        target = RetainedWire(restart.address, restart.observations, 'target-after')
        target.session_connect('retained-cut-target' if kind == 'replay' else 'retained-after-cut',
                               expected_present=kind == 'replay')
        if kind != 'replay' or not committed:
            target.subscribe_options(1, (Subscription('cut/#', 1),))
        expected_delivery = kind == 'replay' or committed
        delivery = target.receive(payload, 1 if kind != 'qos0' else 0, True).observation() if expected_delivery else None
        target.no_publication(.08, code='retained_replay_resurrected_or_duplicated')
        final = wait_state(restart, lambda state: state['counts']['obligations'] == state['counts']['outbound'] == 0,
                           'retained_crash_delivery_not_finished')
        write_json(case / 'final.sql.json', final)
        target.close(); target = None; restart.stop(); writer_available(restart.db)
        return {'criterion': f'crash-{kind}-{phase}', 'status': 'PASS', 'barrier': hit,
                'frame_sent_hex': sent.hex(), 'kill_status': status, 'killed_pid': broker.process.pid,
                'same_sqlite_inode': inode, 'same_database': str(broker.db), 'expected_committed': committed,
                'retained_current_after_restart': inventory(recovered), 'delivery': delivery,
                'restart_status': restart.process.returncode}
    finally:
        if source is not None: source.close()
        if target is not None: target.close()
        if restart is not None: restart.cleanup()
        broker.cleanup()


def lost_result(binary, case, kind):
    broker = Broker(binary, case, test_control=True); source = target = None
    topic, payload = f'lost/{kind}/value', f'retained-lost:{kind}'.encode()
    try:
        broker.ready()
        source = RetainedWire(broker.address, broker.observations, 'source')
        source.session_connect('retained-lost-source', expected_present=False)
        target = RetainedWire(broker.address, broker.observations, 'target')
        target.session_connect('retained-lost-target', expected_present=False)
        if kind == 'replay':
            source.publish(1, topic, payload, qos=2)
        else:
            target.subscribe_options(1, (Subscription('lost/#', 1, retain_as_published=True, retain_handling=2),))
        control_name = {'qos1': 'drop-next-admission-result', 'qos0': 'drop-next-retainedqos0-result',
                        'replay': 'drop-next-subscriptions-result'}[kind]
        control = broker.control / control_name; control.touch(exist_ok=False)
        if kind == 'replay':
            target.send(subscribe_packet(2, (Subscription('lost/#', 1),)))
        else:
            source.send(retained_publish_packet(topic, payload, 2 if kind == 'qos1' else None,
                                               qos=1 if kind == 'qos1' else 0))
        dropped = broker.wait_event('store_result_dropped')
        expected_kind = {'qos1': 'Admission', 'qos0': 'RetainedQoS0', 'replay': 'Subscriptions'}[kind]
        require(dropped.get('command_kind') == expected_kind and not control.exists(), 'retained_result_drop_not_reached')
        committed = snapshot(broker, 'committed-result-lost')
        canonical = canonical_receipt(committed, dropped, {'qos1': 10, 'qos0': 11, 'replay': 12}[kind])
        require(len(inventory(committed)) == 1, 'retained_lost_result_not_committed')
        resolved = broker.wait_event('store_resolved', after=int(dropped['seq']), timeout=15)
        require(resolved.get('operation') == dropped['operation'] and resolved['details'].get('outcome') == 'committed',
                'retained_resolution_changed_operation')
        if kind == 'qos1': require(source.wait(0x40, 2).ack() == (2, 0), 'retained_resolved_puback_missing')
        if kind == 'replay':
            from pubsub_probe import decode_filter_ack
            require(decode_filter_ack(target.wait(0x90, 2)) == (2, (1,)), 'retained_resolved_suback_missing')
        delivery = target.receive(payload, 0 if kind == 'qos0' else 1, True).observation()
        target.no_publication(.15, code='retained_lost_result_duplicated_delivery')
        final = wait_state(broker, lambda state: state['counts']['obligations'] == state['counts']['outbound'] == 0,
                           'retained_lost_result_not_finished')
        write_json(case / 'final.sql.json', final)
        source.close(); source = None; target.close(); target = None; broker.stop(); writer_available(broker.db)
        return {'criterion': f'lost-{kind}', 'status': 'PASS', 'drop': dropped, 'resolve': resolved,
                'same_operation': True, 'canonical': canonical, 'delivery': delivery,
                'process_status': broker.process.returncode}
    finally:
        if source is not None: source.close()
        if target is not None: target.close()
        broker.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True); parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary-source-head', required=True)
    parser.add_argument('--case', choices=('all', 'crashes', 'lost'), default='all')
    args = parser.parse_args(); args.out.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    head = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD']).decode().strip()
    receipt = {'format': 'mqttiumd.retained-boundary-system-reception', 'schema_version': 1,
               'status': 'FAIL', 'qualification': 'system-test-only', 'source_head': args.binary_source_head,
               'harness_source_head': head, 'binary': {'path': str(args.binary.resolve()),
                 'sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest()}, 'cases': []}
    try:
        require(head == args.binary_source_head, 'retained_source_mismatch')
        if args.case in ('all', 'crashes'):
            for kind in ('qos1', 'qos0', 'replay'):
                for phase in ('prepared', 'committed'):
                    receipt['cases'].append(crash(args.binary.resolve(), args.out / f'crash-{kind}-{phase}', kind, phase))
        if args.case in ('all', 'lost'):
            for kind in ('qos1', 'qos0', 'replay'):
                receipt['cases'].append(lost_result(args.binary.resolve(), args.out / f'lost-{kind}', kind))
        receipt['status'] = 'PASS'
    except Exception as error:
        receipt['failure'] = {'code': getattr(error, 'code', type(error).__name__), 'detail': str(error)[:512]}
    finally: write_json(args.out / 'receipt.json', receipt)
    print(json.dumps({'status': receipt['status'], 'failure': receipt.get('failure'), 'receipt': str(args.out / 'receipt.json')}))
    return 0 if receipt['status'] == 'PASS' else 1


if __name__ == '__main__': raise SystemExit(main())
