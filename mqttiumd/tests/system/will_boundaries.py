#!/usr/bin/env python3
"""Finite Will crash, lost-result and pressure witnesses on the real TCP product.

Every cut kills its own broker PID and reopens that exact SQLite inode. Setup
uses only normal CONNECT/SUBSCRIBE; no fixture or repair SQL is installed.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import subprocess
import time

from external_wildcard import Subscription
from pubsub_probe import durable, session_key
from tcp_probe import ReceiptFailure, operation_hex, packet, require, write_json
from tcp_reception import Broker, writer_available
from will_probe import WillWire, assert_will_frame, will_connect_packet
from will_reception import drained, until_state


TOPIC = 'will/cut/status'
TARGET = 'cut-dashboard'
CUTS = ('open-prepared', 'open-committed', 'admission-prepared', 'admission-committed',
        'publish-observed', 'finish-committed')


def will_commands(state):
    """Independent stable kind17 header and causal identities; not a product parser."""
    result = []
    for row in state['tables']['operations']:
        raw = bytes.fromhex(row['recipe'])
        if raw[:7] != b'MQDR\0\x01\x11':
            continue
        require(len(raw) >= 103 and raw[7:39].hex() == row['id'],
                'will_canonical_receipt_identity_invalid')
        result.append({'operation': row['id'], 'will': raw[39:71].hex(),
                       'publication': raw[71:103].hex(),
                       'recipe_sha256': hashlib.sha256(raw).hexdigest(), 'decision': row['decision']})
    return result


def assert_cut_inventory(name, state):
    wills = state['tables']['wills']
    counts = state['counts']
    if name == 'open-prepared':
        require(not wills and not counts['obligations'], 'will_open_prepared_became_durable')
    elif name == 'open-committed':
        require(len(wills) == 1 and wills[0]['state'] == 1 and not counts['obligations'],
                'will_open_commit_did_not_install_intent')
    elif name == 'admission-prepared':
        require(len(wills) == 1 and wills[0]['state'] == 2 and not counts['obligations'],
                'will_admission_prepared_lost_or_consumed_intent')
    elif name in ('admission-committed', 'publish-observed'):
        require(not wills and counts['obligations'] == counts['publications'] == counts['payloads'] == 1,
                'will_admission_commit_not_atomic')
        if name == 'publish-observed':
            require(counts['outbound'] == 1, 'will_observed_publication_has_no_durable_claim')
    elif name == 'finish-committed':
        require(not wills and all(counts[table] == 0 for table in
                                  ('obligations', 'outbound', 'publications', 'payloads')),
                'will_finish_commit_not_terminal')
    else:
        raise ValueError('unknown Will cut')


async def fact(broker, kind, *, after=-1, predicate=lambda _row: True, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for row in broker.events():
            if int(row['seq']) > after and row['kind'] == kind and predicate(row):
                broker.observations.record('event_retained', event=row)
                return row
        require(broker.process.poll() is None, 'will_broker_died_before_fact', kind)
        await asyncio.sleep(.005)
    raise ReceiptFailure('will_product_fact_not_reached', kind)


async def seed(binary, case):
    setup = Broker(binary, case, label='setup')
    wire = None
    try:
        ready = setup.ready()
        wire = WillWire(setup.address, setup.observations, 'setup-dashboard')
        wire.session_connect(TARGET, expected_present=False)
        wire.subscribe_options(1, (Subscription('will/cut/#', 1),))
        marker = max(int(row['seq']) for row in setup.events())
        wire.send(b'\xe0\x00')
        wire.close()
        await fact(setup, 'connection_closed', after=marker,
                   predicate=lambda row: row.get('details', {}).get('code') == 'client_disconnect')
        state = durable(setup.db)
        require(state['counts']['sessions'] == state['counts']['routes'] == 1,
                'will_setup_dashboard_session_or_subscription_missing')
        write_json(case / 'setup.sql.json', state)
        setup.stop()
        writer_available(setup.db)
        return {'pid': setup.process.pid, 'status': setup.process.returncode, 'ready': ready,
                'db': str(setup.db), 'target': session_key(state, TARGET)}
    finally:
        if wire:
            wire.close()
        setup.cleanup()


def receive_recovered(wire, payload, *, expected_claim=None, state=None):
    value = wire.next_publication(timeout=8)
    assert_will_frame(value, TOPIC, payload)
    if expected_claim is not None:
        require(value.dup and value.packet_id == expected_claim['packet_id'],
                'will_recovered_claim_changed_packet_id_or_dup')
        claims = state['tables']['outbound'] if state is not None else []
        require(len(claims) == 1 and all(claims[0][key] == expected_claim[key]
                for key in ('claim', 'publication', 'idx', 'packet_id', 'target')),
                'will_recovered_claim_identity_changed')
    wire.acknowledge(value)
    return value.observation()


async def crash_cut(binary, case, name):
    setup = await seed(binary, case)
    points = {'open-prepared': 'session.open.prepared', 'open-committed': 'session.open.committed',
              'admission-prepared': 'will.admission.prepared',
              'admission-committed': 'will.admission.committed', 'finish-committed': 'finish.committed'}
    point = points.get(name)
    broker = Broker(binary, case, db=Path(setup['db']), label='cut',
                    pauses=() if point is None else (point,), test_control=True)
    restart, source, target, resumed = None, None, None, None
    payload = ('will-' + name).encode()
    before_claim, observed, hit = None, None, None
    try:
        broker.ready()
        if name in ('publish-observed', 'finish-committed'):
            target = WillWire(broker.address, broker.observations, 'cut-dashboard')
            target.session_connect(TARGET, expected_present=True)
        source = WillWire(broker.address, broker.observations, 'cut-source')
        if name.startswith('open-'):
            source.send(will_connect_packet('cut-source', TOPIC, payload, expiry=0))
        else:
            source.will_connect('cut-source', TOPIC, payload, expiry=0)
            source.close()
        if name in ('publish-observed', 'finish-committed'):
            publication = target.next_publication(timeout=8)
            observed = assert_will_frame(publication, TOPIC, payload)
            before_claim = durable(broker.db)['tables']['outbound'][0]
            require(publication.packet_id == before_claim['packet_id'], 'will_claim_not_wire_correlated')
            if name == 'finish-committed':
                target.acknowledge(publication)
        if point:
            hit = await asyncio.to_thread(broker.wait_event, 'barrier_hit', point=point)
            require(hit.get('command_kind') == ('SessionOpen' if name.startswith('open-') else
                    'Finish' if name == 'finish-committed' else 'WillAdmission'),
                    'will_cut_hit_wrong_command')
        if name.startswith('open-'):
            # The selected writer point, rather than a delay, establishes that
            # no positive CONNACK can be justified by an observed result yet.
            require(source.poll(.04) is None, 'will_connack_before_durable_open_result')
        state = durable(broker.db)
        assert_cut_inventory(name, state)
        commands = will_commands(state)
        require(len(commands) == (1 if name in ('admission-committed', 'publish-observed',
                                               'finish-committed') else 0),
                'will_cut_wrong_admission_count')
        write_json(case / 'at-cut.sql.json', state)
        expected_publication = commands[0]['publication'] if commands else None
        expected_will = commands[0]['will'] if commands else (
            state['tables']['wills'][0]['id'] if state['tables']['wills'] else None)
        identity = broker.db.stat().st_ino
        before_events = broker.events()
        status = broker.stop(killed=True)
        writer_available(broker.db)
        for wire in (source, target):
            if wire:
                wire.close()
        restart = Broker(binary, case, db=broker.db, label='restarted')
        restart.ready()
        require(restart.db.stat().st_ino == identity, 'will_cut_database_replaced')
        resumed = WillWire(restart.address, restart.observations, 'resumed-dashboard')
        resumed.session_connect(TARGET, expected_present=True)
        should_deliver = name not in ('open-prepared', 'finish-committed')
        recovered = None
        recovered_state = durable(restart.db)
        if should_deliver:
            recovered = receive_recovered(resumed, payload, expected_claim=before_claim,
                                          state=recovered_state)
            if expected_publication is not None:
                require(all(row['publication'] == expected_publication
                            for row in recovered_state['tables']['obligations']),
                        'will_recovery_created_new_publication')
        else:
            resumed.no_publication(.2, code='will_never_accepted_or_finished_was_resurrected')
        resumed.no_publication(.12, code='will_duplicate_obligation_after_recovery')
        final = await drained(restart)
        require(final['counts']['wills'] == 0 and all(final['counts'][name] == 0
                for name in ('publications', 'payloads')), 'will_cut_resources_not_reclaimed')
        all_events = before_events + restart.events()
        admitted = [row for row in all_events if row['kind'] == 'will_admitted']
        require(len(admitted) <= 1, 'will_double_admission_after_recovery')
        require(not admitted or expected_will is not None and
                operation_hex(admitted[0]['will']) == expected_will,
                'will_recovery_changed_occurrence')
        write_json(case / 'recovered.sql.json', recovered_state)
        write_json(case / 'final.sql.json', final)
        resumed.send(b'\xe0\x00')
        resumed.close()
        restart.stop()
        writer_available(restart.db)
        return {'criterion': name, 'status': 'PASS', 'setup': setup, 'point': point, 'hit': hit,
                'killed_pid': broker.process.pid, 'kill_status': status,
                'restarted_pid': restart.process.pid, 'restart_status': restart.process.returncode,
                'database_inode': identity, 'same_file': True, 'expected_delivery': should_deliver,
                'observed_before_cut': observed, 'recovered': recovered, 'claim_before_cut': before_claim,
                'commands_at_cut': commands, 'will_admission_events': admitted,
                'at_cut_counts': state['counts'], 'final_counts': final['counts']}
    finally:
        for wire in (source, target, resumed):
            if wire:
                wire.close()
        broker.cleanup()
        if restart:
            restart.cleanup()


async def lost_result(binary, case, kind, *, held):
    """A real engaged result remains one OperationId/canonical recipe through Resolve."""
    broker = Broker(binary, case, test_control=True)
    source, target = None, None
    try:
        broker.ready()
        target = WillWire(broker.address, broker.observations, 'unknown-target')
        target.session_connect(TARGET, expected_present=False)
        target.subscribe_options(1, (Subscription('will/cut/#', 1),))
        source = WillWire(broker.address, broker.observations, 'unknown-source')
        source.will_connect('unknown-source', TOPIC, ('will-unknown-' + kind).encode(), expiry=0)
        file = broker.control / (('hold-' if held else 'drop-next-') + 'will-' + kind + '-result')
        file.touch(exist_ok=False)
        marker = max(int(event['seq']) for event in broker.events())
        source.close()
        expected_kind = 'WillAdmission' if kind == 'admission' else 'WillEnd'
        lost = await fact(broker, 'store_result_held' if held else 'store_result_dropped',
                          after=marker, predicate=lambda row: row.get('command_kind') == expected_kind)
        operation = operation_hex(lost['operation'])
        initial = durable(broker.db)
        receipts = [row for row in initial['tables']['operations'] if row['id'] == operation]
        require(len(receipts) == 1, 'will_lost_result_was_not_durably_committed')
        canonical = receipts[0]['recipe']
        require(receipts[0]['decision'] == (12 if kind == 'admission' else 8),
                'will_lost_result_not_committed_lifecycle_or_admission')
        target.send(b'\xc0\x00')
        target.wait(0xD0)
        resolved_held = None
        if held:
            resolved_held = await fact(broker, 'store_result_held', after=int(lost['seq']),
                                       predicate=lambda row: row.get('operation') == lost['operation']
                                       and row.get('details', {}).get('source') == 'Resolution')
            interim = durable(broker.db)
            require([row['recipe'] for row in interim['tables']['operations']
                    if row['id'] == operation] == [canonical], 'will_unknown_resolution_changed_recipe')
            file.unlink()
        resolved = await fact(broker, 'store_resolved', after=int(lost['seq']),
                              predicate=lambda row: row.get('operation') == lost['operation']
                              and row.get('details', {}).get('outcome') == 'committed')
        value = target.next_publication(timeout=8)
        received = assert_will_frame(value, TOPIC, ('will-unknown-' + kind).encode())
        target.acknowledge(value)
        target.no_publication(.15, code='will_duplicate_after_lost_result')
        final = await drained(broker)
        commands = will_commands(final)
        require(len(commands) == 1, 'will_resolution_created_second_admission')
        require([row['recipe'] for row in final['tables']['operations']
                if row['id'] == operation] == [canonical], 'will_resolution_replaced_original_recipe')
        require(final['counts']['wills'] == final['counts']['publications'] == final['counts']['payloads'] == 0,
                'will_unknown_result_resources_not_reclaimed')
        write_json(case / 'result-committed.sql.json', initial)
        write_json(case / 'final.sql.json', final)
        target.send(b'\xe0\x00')
        target.close()
        broker.stop()
        return {'criterion': ('held-' if held else 'lost-') + kind, 'status': 'PASS',
                'lost': lost, 'held_resolution': resolved_held, 'resolved': resolved,
                'same_operation': operation, 'canonical_sha256': hashlib.sha256(bytes.fromhex(canonical)).hexdigest(),
                'received': received, 'commands': commands, 'final_counts': final['counts'],
                'broker_pid': broker.process.pid, 'broker_status': broker.process.returncode}
    finally:
        for wire in (source, target):
            if wire:
                wire.close()
        broker.cleanup()


async def main_async(arguments):
    arguments.out.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    head = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD']).decode().strip()
    result = {'format': 'mqttiumd.will-boundary-system-reception', 'schema_version': 1,
              'status': 'FAIL', 'qualification': 'system-test-only',
              'source_head': arguments.binary_source_head, 'harness_source_head': head,
              'binary': {'path': str(arguments.binary.resolve()),
                         'sha256': hashlib.sha256(arguments.binary.read_bytes()).hexdigest(),
                         'features': ['test-hooks']}, 'cases': []}
    try:
        require(head == arguments.binary_source_head, 'will_boundary_source_mismatch')
        if arguments.case == 'all' or arguments.case in CUTS:
            for name in CUTS if arguments.case == 'all' else (arguments.case,):
                result['cases'].append(await crash_cut(arguments.binary.resolve(), arguments.out / name, name))
        if arguments.case in ('all', 'results'):
            for kind in ('end', 'admission'):
                for held in (False, True):
                    name = ('held-' if held else 'lost-') + kind
                    result['cases'].append(await lost_result(arguments.binary.resolve(), arguments.out / name,
                                                           kind, held=held))
        result['status'] = 'PASS'
        return 0
    except Exception as error:
        result['failure'] = {'code': getattr(error, 'code', type(error).__name__), 'detail': str(error)[:512]}
        return 1
    finally:
        write_json(arguments.out / 'receipt.json', result)
        print(json.dumps({'status': result['status'], 'failure': result.get('failure'),
                          'receipt': str(arguments.out / 'receipt.json')}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary-source-head', required=True)
    parser.add_argument('--case', choices=('all', 'results', *CUTS), default='all')
    return asyncio.run(main_async(parser.parse_args()))


if __name__ == '__main__':
    raise SystemExit(main())
