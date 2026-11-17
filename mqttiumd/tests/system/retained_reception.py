#!/usr/bin/env python3
"""Finite retained-value product reception; system evidence, not qualification."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import subprocess
import time

from external_mqttium import client_identity
from external_retained import MqttiumRetained, PahoRetained, PacketTap
from external_sessions import INFINITE
from external_wildcard import Subscription, MosquittoReceiver, mosquitto_identity
from pubsub_probe import durable, session_key
from retained_probe import RetainedWire
from tcp_probe import Observations, ReceiptFailure, require, write_json
from tcp_reception import Broker, writer_available


async def peer(kind, broker, case, label, *, expiry=INFINITE, client_id=None, present=False):
    tap = PacketTap(broker.address, Observations(case / f'{label}.wire.jsonl'))
    constructor = MqttiumRetained if kind == 'mqttium' else PahoRetained
    client = constructor(tap.address, client_id or label, expiry=expiry,
                         events_path=case / f'{label}.client.jsonl')
    try:
        await client.open(present)
    except BaseException:
        await client.disconnect(); tap.close()
        raise
    return client, tap


async def close_peers(values):
    for client, tap in values:
        try:
            await client.disconnect()
        finally:
            tap.close()


async def until_state(broker, predicate, reason, timeout=8):
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = durable(broker.db)
        if predicate(state):
            return state
        require(broker.process.poll() is None, 'retained_broker_exited', str(broker.process.returncode))
        await asyncio.sleep(.01)
    raise ReceiptFailure(reason, repr(state['counts'] if state else None))


def retained_rows(state):
    require('retained' in state['tables'], 'retained_inventory_table_missing')
    return state['tables']['retained']


def business_drained(state):
    # Current retained references deliberately keep their publication/payload.
    return all(state['counts'][name] == 0 for name in ('obligations', 'outbound', 'inbound'))


def one_delivery(tap, payload, qos, retain, *, count=1):
    values = tap.publications(payload)
    require(len(values) == count and all(value.qos == qos and value.retain == retain for value in values),
            'retained_wire_count_qos_or_flag', repr([value.observation() for value in values]))
    return [value.observation() for value in values]


async def minimal(binary, case):
    broker = Broker(binary, case)
    restart = None
    peers = []
    topic, payload = 'retained/minimal/value', b'retained-after-producer-end-and-broker-restart'
    try:
        ready = broker.ready()
        producer, tap = await peer('mqttium', broker, case, 'minimal-producer', expiry=0)
        peers.append((producer, tap))
        await producer.publish(topic, payload, 1)
        before_end = await until_state(broker, lambda state: len(retained_rows(state)) == 1,
                                       'retained_without_live_subscriber_missing')
        producer_session = session_key(before_end, 'minimal-producer')
        await close_peers(peers); peers = []
        after_end = await until_state(broker, lambda state: all(row['id'] != producer_session
            for row in state['tables']['sessions']), 'retained_producer_session_not_terminated')
        require(retained_rows(after_end) == retained_rows(before_end), 'retained_value_destroyed_with_producer')
        write_json(case / 'before-restart.sql.json', after_end)
        first_status = broker.stop(); writer_available(broker.db)
        restart = Broker(binary, case, db=broker.db, label='restarted')
        restart.ready()
        target, target_tap = await peer('paho', restart, case, 'minimal-subscriber')
        peers.append((target, target_tap))
        await target.subscribe((Subscription('retained/+/#', 1),))
        observed = await target.receive(payload, 1, True)
        await target.no_message()
        wire = one_delivery(target_tap, payload, 1, True)
        final = await until_state(restart, business_drained, 'retained_replay_not_durably_finished')
        require(retained_rows(final) == retained_rows(before_end), 'replay_changed_retained_current_value')
        write_json(case / 'after-restart.sql.json', final)
        await close_peers(peers); peers = []
        final_status = restart.stop(); writer_available(restart.db)
        return {'criterion': 'minimal', 'status': 'PASS', 'ready': ready,
                'producer_session_terminated': producer_session, 'same_database': str(broker.db),
                'producer_client': producer.result, 'subscriber_client': target.result,
                'observed': observed, 'independent_wire': wire,
                'retained_before': retained_rows(before_end), 'retained_after': retained_rows(final),
                'process_statuses': [first_status, final_status]}
    finally:
        await close_peers(peers)
        if restart is not None: restart.cleanup()
        broker.cleanup()


async def matrix(binary, case):
    broker = Broker(binary, case); peers = []; cells = []
    try:
        ready = broker.ready()
        for direction, (publisher_kind, target_kind) in enumerate((('mqttium', 'paho'), ('paho', 'mqttium'))):
            source, source_tap = await peer(publisher_kind, broker, case, f'matrix-source-{direction}')
            target, target_tap = await peer(target_kind, broker, case, f'matrix-target-{direction}')
            peers.extend(((source, source_tap), (target, target_tap)))
            for granted in (0, 1):
                for qos in (0, 1, 2):
                    for rap in (False, True):
                        index = len(cells)
                        topic = f'matrix/{direction}/{granted}/{qos}/{int(rap)}'
                        payload = f'numbered-retained-cell:{index:02}'.encode()
                        await target.subscribe((Subscription(topic, granted, retain_as_published=rap,
                                                            retain_handling=2),))
                        await source.publish(topic, payload, qos)
                        live = await target.receive(payload, min(qos, granted), rap)
                        await target.no_message(.04)
                        live_wire = one_delivery(target_tap, payload, min(qos, granted), rap)
                        await until_state(broker, business_drained, 'retained_live_not_finished')
                        await target.subscribe((Subscription(topic, granted),))
                        replay = await target.receive(payload, min(qos, granted), True)
                        await target.no_message(.04)
                        wires = target_tap.publications(payload)
                        require(len(wires) == 2 and wires[-1].retain and wires[-1].qos == min(qos, granted),
                                'retained_replay_wire_missing')
                        await until_state(broker, business_drained, 'retained_matrix_replay_not_finished')
                        await target.unsubscribe((topic,))
                        await source.publish(topic, b'', 1)
                        await until_state(broker, lambda state: not retained_rows(state), 'retained_clear_failed')
                        cells.append({'publisher': publisher_kind, 'receiver': target_kind, 'published_qos': qos,
                                      'granted_qos': granted, 'rap': rap, 'effective_qos': min(qos, granted),
                                      'live': live, 'replay': replay, 'live_wire': live_wire,
                                      'replay_wire': wires[-1].observation()})
            await close_peers(peers); peers = []
        final = durable(broker.db); write_json(case / 'durable-final.json', final)
        require(not retained_rows(final) and business_drained(final), 'retained_matrix_not_drained')
        broker.stop()
        return {'criterion': 'matrix', 'status': 'PASS', 'ready': ready, 'cells': cells,
                'cells_per_direction': 12, 'six_qos_cells_each_rap': True,
                'process_status': broker.process.returncode}
    finally:
        await close_peers(peers); broker.cleanup()


async def options(binary, case):
    broker = Broker(binary, case); source = target = None; decisions = []
    try:
        broker.ready()
        source = RetainedWire(broker.address, broker.observations, 'options-producer')
        source.session_connect('retained-options-producer', expiry=0, expected_present=False)
        topic, payload = 'options/room/value', b'frozen-retained-value'
        source.publish(1, topic, payload, qos=2)
        target = RetainedWire(broker.address, broker.observations, 'options-target')
        target.session_connect('retained-options-target', expected_present=False)
        requests = (
            ((Subscription('options/#', 1, retain_handling=1),) * 2, 1, 'RH1-ordered-duplicate'),
            ((Subscription('options/#', 1),) * 2, 2, 'RH0-duplicate-occurrences'),
            ((Subscription('options/#', 1), Subscription('options/+/value', 1)), 2, 'overlapping-filter-replays'),
            ((Subscription('options/#', 1, retain_handling=2),), 0, 'RH2-suppressed'),
            ((Subscription('options/#', 1, retain_handling=1),), 0, 'RH1-existing-suppressed'),
        )
        for identifier, (filters, count, label) in enumerate(requests, 1):
            target.subscribe_options(identifier, filters)
            received = [target.receive(payload, 1, True).observation() for _ in range(count)]
            target.no_publication(.08, code='retained_extra_filter_occurrence')
            await until_state(broker, business_drained, 'retained_options_not_finished')
            decisions.append({'case': label, 'filters': [asdict(value) for value in filters],
                              'expected_replays': count, 'received': received})
        # The historical producer session is gone; No Local uses ClientID,
        # never a recycled session identity or current connection generation.
        source.session_disconnect(); source = None
        await until_state(broker, lambda state: all(row['client_id'] != b'retained-options-producer'.hex()
            for row in state['tables']['sessions']), 'retained_options_producer_not_gone')
        source = RetainedWire(broker.address, broker.observations, 'new-incarnation-same-producer-name')
        source.session_connect('retained-options-producer', expiry=0, expected_present=False)
        source.subscribe_options(1, (Subscription('options/#', 1, no_local=True),))
        source.no_publication(.1, code='retained_no_local_used_session_instead_of_client_id')
        source.subscribe_options(2, (Subscription('options/+/value', 0),))
        self_replay = source.receive(payload, 0, True).observation()
        source.no_publication(.08)
        final = await until_state(broker, business_drained, 'retained_no_local_replay_not_consumed')
        write_json(case / 'durable-final.json', final)
        source.close(); source = None; target.close(); target = None; broker.stop()
        return {'criterion': 'options', 'status': 'PASS', 'ordered_steps': decisions,
                'producer_name_no_local_after_session_end': True, 'eligible_other_contribution': self_replay,
                'process_status': broker.process.returncode}
    finally:
        if source is not None: source.close()
        if target is not None: target.close()
        broker.cleanup()


async def replacement(binary, case):
    broker = Broker(binary, case); peers = []; owed = None
    topic = 'versions/value'
    try:
        broker.ready()
        target, tap = await peer('paho', broker, case, 'versions-target-before', client_id='versions-target')
        peers.append((target, tap)); await target.subscribe((Subscription('versions/#', 1, retain_handling=2),))
        await close_peers(peers); peers = []
        source, source_tap = await peer('mqttium', broker, case, 'versions-source', expiry=0)
        peers.append((source, source_tap))
        first, second, plain = b'old-retained-still-owed', b'new-current-retained', b'nonretained-does-not-change-current'
        await source.publish(topic, first, 2)
        before = await until_state(broker, lambda state: state['counts']['obligations'] == 1,
                                   'retained_first_offline_obligation_missing')
        await source.publish(topic, second, 1)
        changed = await until_state(broker, lambda state: state['counts']['obligations'] == 2,
                                    'retained_replace_lost_old_owed_version')
        require(len(retained_rows(changed)) == 1 and changed['counts']['payloads'] == 2,
                'retained_replace_freed_payload_still_owed')
        current = retained_rows(changed)
        await source.publish(topic, plain, 1, retain=False)
        plain_state = await until_state(broker, lambda state: state['counts']['obligations'] == 3,
                                        'nonretained_offline_obligation_missing')
        require(retained_rows(plain_state) == current, 'ordinary_publish_replaced_retained_current')
        await source.publish(topic, b'', 1)
        cleared = await until_state(broker, lambda state: not retained_rows(state)
                                    and state['counts']['obligations'] == 4, 'clear_not_atomic_with_live_empty_delivery')
        target, tap = await peer('paho', broker, case, 'versions-target-after',
                                 client_id='versions-target', present=True)
        peers.append((target, tap))
        messages = [await target.receive(value, 1, False) for value in (first, second, plain, b'')]
        wire = [one_delivery(tap, value, 1, False) for value in (first, second, plain, b'')]
        final = await until_state(broker, lambda state: business_drained(state)
            and state['counts']['publications'] == 0 and state['counts']['payloads'] == 0,
            'retained_old_versions_or_references_not_freed')
        newcomer, newcomer_tap = await peer('mqttium', broker, case, 'versions-no-current')
        peers.append((newcomer, newcomer_tap)); await newcomer.subscribe((Subscription('versions/#', 1),))
        await newcomer.no_message(.1)
        for label, state in (('before', before), ('changed', changed), ('cleared', cleared), ('final', final)):
            write_json(case / f'{label}.sql.json', state)
        await close_peers(peers); peers = []; broker.stop()
        return {'criterion': 'replacement', 'status': 'PASS', 'messages': messages, 'wire': wire,
                'nonretained_index_unchanged': True, 'clear_delivered_empty_live': True,
                'old_owed_payload_survived': True, 'last_reference_released': True,
                'final_counts': final['counts'], 'process_status': broker.process.returncode}
    finally:
        if owed is not None: owed.close()
        await close_peers(peers); broker.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary-source-head', required=True)
    parser.add_argument('--case', choices=('normal', 'minimal', 'matrix', 'options', 'replacement'), default='normal')
    args = parser.parse_args(); args.out.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    head = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD']).decode().strip()
    receipt = {'format': 'mqttiumd.retained-system-reception', 'schema_version': 1,
               'status': 'FAIL', 'qualification': 'system-test-only', 'source_head': args.binary_source_head,
               'harness_source_head': head, 'client_identity': client_identity(),
               'binary': {'path': str(args.binary.resolve()), 'sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest()},
               'cases': []}
    try:
        require(head == args.binary_source_head, 'retained_source_mismatch')
        functions = {'minimal': minimal, 'matrix': matrix, 'options': options, 'replacement': replacement}
        selected = tuple(functions) if args.case == 'normal' else (args.case,)
        for name in selected:
            receipt['cases'].append(asyncio.run(functions[name](args.binary.resolve(), args.out / name)))
        receipt['status'] = 'PASS'
    except Exception as error:
        receipt['failure'] = {'code': getattr(error, 'code', type(error).__name__), 'detail': str(error)[:512]}
    finally:
        write_json(args.out / 'receipt.json', receipt)
    print(json.dumps({'status': receipt['status'], 'failure': receipt.get('failure'), 'receipt': str(args.out / 'receipt.json')}))
    return 0 if receipt['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
