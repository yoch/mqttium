#!/usr/bin/env python3
"""Finite Message Expiry product evidence; system tests, never qualification."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import subprocess
import time

from external_message_expiry import MqttiumMessageExpiry, PahoMessageExpiry, PacketTap
from external_mqttium import client_identity
from external_sessions import INFINITE
from external_wildcard import Subscription
from pubsub_probe import durable, session_key
from tcp_probe import Observations, ReceiptFailure, require, write_json
from tcp_reception import Broker, writer_available

BUSINESS = ('retained', 'obligations', 'outbound', 'inbound', 'publications', 'payloads')


async def peer(kind, broker, case, label, *, client_id=None, present=False, session_expiry=INFINITE):
    tap = PacketTap(broker.address, Observations(case / f'{label}.wire.jsonl'))
    constructor = MqttiumMessageExpiry if kind == 'mqttium' else PahoMessageExpiry
    client = constructor(tap.address, client_id or label, expiry=session_expiry,
                         events_path=case / f'{label}.client.jsonl')
    try:
        await client.open(present)
    except BaseException:
        await client.disconnect()
        tap.close()
        raise
    return client, tap


async def close_peers(values, *, cleanup=False):
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
    if failures and not cleanup:
        raise failures[0]


def snapshot(broker, label):
    state = durable(broker.db)
    write_json(broker.case / f'{label}.sql.json', state)
    return state


def physically_drained(state):
    require(all(name in state['counts'] for name in BUSINESS), 'message_expiry_business_table_missing')
    if not all(state['counts'][name] == 0 for name in BUSINESS):
        return False
    meta = state['tables']['meta'][0]
    return all(meta[name] == '0000000000000000' for name in
               ('used_payload_bytes', 'used_references', 'used_exchange_slots'))


def expiry_stamp(publication):
    domain = publication.get('expiry_domain')
    tick = publication.get('expiry_tick')
    interval = publication.get('expiry_interval')
    require(isinstance(domain, str) and len(domain) == 64 and int(domain, 16) > 0 and
            isinstance(tick, str) and len(tick) == 16 and type(interval) is int and
            0 <= interval <= INFINITE, 'message_expiry_original_stamp_missing')
    return {'domain': domain, 'tick': str(int(tick, 16)), 'interval': interval}


def original_copies(state, original_id, stamp):
    values = state['tables']['publications']
    require(any(row['id'] == original_id for row in values) and all(
            (row['id'] == original_id or row.get('retained_version') == original_id) and
            expiry_stamp(row) == stamp for row in values), 'message_expiry_replay_changed_original_stamp')
    return [row['id'] for row in values]


def offline_inventory(state, cells):
    # ADR0026 keeps effective-QoS0 obligations from durable admissions while
    # their target is absent. Initially QoS0 non-retained input is volatile.
    expected = {cell['topic']: cell for cell in cells if cell['published_qos'] != 0 or cell['retain']}
    publications = {bytes.fromhex(row['topic']).decode(): row for row in state['tables']['publications']}
    obligations = state['tables']['obligations']
    payloads = {row['id']: row for row in state['tables']['payloads']}
    require(set(publications) == set(expected) and len(obligations) == len(payloads) == len(expected),
            'message_expiry_offline_positive_inventory_missing', repr(state['counts']))
    by_id = {row['id']: expected[topic] for topic, row in publications.items()}
    require({row['publication'] for row in obligations} == set(by_id) and all(
            row['qos'] == min(by_id[row['publication']]['published_qos'], by_id[row['publication']]['granted_qos'])
            for row in obligations), 'message_expiry_offline_copy_qos_or_identity_wrong')
    for topic, publication in publications.items():
        cell = expected[topic]
        require(expiry_stamp(publication) == {key: cell['ingress']['details'][key]
                for key in ('domain', 'tick', 'interval')}, 'message_expiry_offline_original_stamp_changed')
        payload = bytes.fromhex(payloads[publication['payload']]['bytes'])
        require(hashlib.sha256(payload).hexdigest() == cell['payload_sha256'],
                'message_expiry_offline_durable_payload_wrong')
    retained = {bytes.fromhex(row['topic']).decode(): row['publication'] for row in state['tables']['retained']}
    require(retained == {cell['topic']: publications[cell['topic']]['id'] for cell in cells if cell['retain']},
            'message_expiry_offline_retained_inventory_wrong')
    counts = {qos: sum(row['qos'] == qos for row in obligations) for qos in (0, 1)}
    return {'pending_qos0': counts[0], 'pending_qos1': counts[1], 'retained': len(retained),
            'publications': len(publications), 'payloads': len(payloads)}


async def until_state(broker, predicate, code, timeout=8):
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = durable(broker.db)
        if predicate(state):
            return state
        require(broker.process.poll() is None, 'message_expiry_broker_exited', str(broker.process.returncode))
        await asyncio.sleep(.01)
    if state is not None:
        write_json(broker.case / f'{code}.sql.json', state)
    raise ReceiptFailure(code, repr(state['counts'] if state else None))


def closed_connection(state, events, client_id):
    identity = str(int(session_key(state, client_id), 16))
    opened = [event for event in events if event['kind'] == 'session_established' and
              event['details'].get('session') == identity]
    require(bool(opened), 'message_expiry_session_connection_not_observed', client_id)
    latest = max(opened, key=lambda event: int(event['seq']))
    values = [event for event in events if event['kind'] == 'connection_closed' and
              event.get('connection') == latest['connection'] and int(event['seq']) > int(latest['seq'])]
    return values[-1] if values else None


async def until_connection_closed(broker, client_id):
    """Infinite sessions need no durable detach command; observe exact transport."""
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        value = closed_connection(durable(broker.db), broker.events(), client_id)
        if value is not None:
            return value
        require(broker.process.poll() is None, 'message_expiry_broker_exited')
        await asyncio.sleep(.005)
    raise ReceiptFailure('message_expiry_offline_connection_not_closed', client_id)


def one_delivery(tap, payload, qos, retain, expiry):
    values = tap.publications(payload)
    require(len(values) == 1, 'message_expiry_wire_delivery_count', str(len(values)))
    value = values[0]
    require((value.qos, value.retain) == (qos, retain), 'message_expiry_wire_qos_or_retain')
    require(value.message_expiry_interval == expiry if expiry is None else
            value.message_expiry_interval is not None and 0 < value.message_expiry_interval <= expiry,
            'message_expiry_wire_interval_wrong', repr(value.observation()))
    return value.observation()


def negative_puback(tap, identifier, *, after):
    """A Packet Identifier can be reused after each completed publication."""
    frames = tap.server_frames()
    require(type(after) is int and 0 <= after <= len(frames), 'message_expiry_ack_cursor_invalid')
    values = [frame for frame in frames[after:] if frame.kind == 0x40 and frame.ack()[0] == identifier]
    require(len(values) == 1 and values[0].ack() == (identifier, 0x97),
            'message_expiry_capacity_negative_puback_not_observed')
    return values[0]


def ingress_events(broker, *, after=-1, topic=None):
    values = [event for event in broker.events() if event['kind'] == 'message_expiry_received'
              and int(event['seq']) > after]
    return values if topic is None else [event for event in values if event['details'].get('topic') == topic]


async def received_ingress(broker, topic, after):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        values = ingress_events(broker, after=after, topic=topic)
        if values:
            require(len(values) == 1, 'message_expiry_ingress_not_unique')
            return values[0]
        require(broker.process.poll() is None, 'message_expiry_broker_exited')
        await asyncio.sleep(.005)
    raise ReceiptFailure('message_expiry_ingress_not_observed', topic)


def pending_drained(state):
    return all(state['counts'][name] == 0 for name in ('obligations', 'outbound', 'inbound'))


async def matrix(binary, case):
    """Both public client directions; every Q0/1/2 × Q0/1 expiry boundary."""
    broker = Broker(binary, case)
    peers, cells = [], []
    try:
        ready = broker.ready()
        directions = (('mqttium', 'paho'), ('paho', 'mqttium'))
        for direction, (publisher, receiver) in enumerate(directions):
            for qos in (0, 1, 2):
                for granted in (0, 1):
                    for retain in (False, True):
                        label = f'matrix-{direction}-{qos}-{granted}-{int(retain)}'
                        source, source_tap = await peer(publisher, broker, case, label + '-source', session_expiry=0)
                        target, target_tap = await peer(receiver, broker, case, label + '-target', session_expiry=0)
                        peers.extend(((source, source_tap), (target, target_tap)))
                        for expiry in (None, 0, 1, 2, INFINITE):
                            topic = f'message-expiry/{label}/{expiry}'
                            payload = f'expiry-cell:{len(cells):03}'.encode()
                            await target.subscribe((Subscription(topic, granted, retain_as_published=True,
                                                                retain_handling=2),))
                            marker = max(int(event['seq']) for event in broker.events())
                            await source.publish(topic, payload, qos, retain=retain, message_expiry=expiry)
                            received = await received_ingress(broker, topic, marker) if expiry is not None else None
                            effective = min(qos, granted)
                            if expiry == 0:
                                await target.no_message(.04)
                                require(not target_tap.publications(payload), 'message_expiry_zero_forwarded')
                                delivery = wire = None
                            else:
                                delivery = await target.receive(payload, effective, retain)
                                wire = one_delivery(target_tap, payload, effective, retain, expiry)
                                require(delivery['message_expiry_interval'] == wire['message_expiry_interval'],
                                        'message_expiry_client_wire_disagreed')
                            await target.unsubscribe((topic,))
                            if retain and expiry != 0:
                                await source.publish(topic, b'', 1, retain=True)
                            # A QoS0 local write is corroborated by the ingress
                            # event above, and by the timed admission commit or
                            # explicit message_expired drop below for TTL0.
                            if qos == 0 and expiry == 0:
                                deadline = time.monotonic() + 5
                                processed = []
                                while time.monotonic() < deadline:
                                    processed = [event for event in broker.events() if int(event['seq']) > marker and
                                        ((event['kind'] == 'qos0_dropped' and
                                          event.get('details', {}).get('reason') == 'message_expired') or
                                         (retain and event['kind'] == 'store_result' and
                                          event.get('command_kind') == 'RetainedQoS0' and
                                          event.get('details', {}).get('outcome') == 'Committed'))]
                                    if processed:
                                        break
                                    await asyncio.sleep(.005)
                                require(bool(processed), 'message_expiry_zero_qos0_processing_unproved')
                            final = await until_state(broker, physically_drained, 'message_expiry_matrix_reference_leak')
                            cells.append({'publisher': publisher, 'receiver': receiver, 'published_qos': qos,
                                          'granted_qos': granted, 'effective_qos': effective, 'retain': retain,
                                          'requested_expiry': expiry, 'ingress': received, 'delivery': delivery,
                                          'wire': wire, 'final_counts': final['counts']})
                        await close_peers(peers)
                        peers = []
        write_json(case / 'final.sql.json', durable(broker.db))
        broker.stop()
        writer_available(broker.db)
        return {'criterion': 'matrix', 'status': 'PASS', 'ready': ready, 'cells': cells,
                'cells_per_direction': 60, 'process_status': broker.process.returncode}
    finally:
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        await close_peers(peers, cleanup=True)
        broker.cleanup()


async def offline(binary, case):
    """Expired pending work is physically reclaimed before session resumption."""
    broker = Broker(binary, case)
    peers, targets, cells = [], [], []
    try:
        ready = broker.ready()
        for direction, (publisher, receiver) in enumerate((('mqttium', 'paho'), ('paho', 'mqttium'))):
            for granted in (0, 1):
                name = f'offline-{direction}-{granted}'
                client, tap = await peer(receiver, broker, case, name)
                await client.subscribe((Subscription(f'message-expiry/offline/{direction}/{granted}/#', granted,
                                                    retain_handling=2),))
                await close_peers(((client, tap),))
                await until_connection_closed(broker, name)
                targets.append((receiver, name, granted, direction))
            source, source_tap = await peer(publisher, broker, case, f'offline-source-{direction}', session_expiry=0)
            peers.append((source, source_tap))
            for granted in (0, 1):
                for qos in (0, 1, 2):
                    for retain in (False, True):
                        topic = f'message-expiry/offline/{direction}/{granted}/{qos}/{int(retain)}'
                        payload = f'expired-offline-cell:{len(cells):02}'.encode()
                        marker = max(int(event['seq']) for event in broker.events())
                        await source.publish(topic, payload, qos, retain=retain, message_expiry=2)
                        received = await received_ingress(broker, topic, marker)
                        cells.append({'publisher': publisher, 'receiver': receiver, 'published_qos': qos,
                                      'granted_qos': granted, 'retain': retain, 'topic': topic,
                                      'payload_sha256': hashlib.sha256(payload).hexdigest(), 'ingress': received})
        admitted = snapshot(broker, 'offline-admitted')
        admitted_inventory = offline_inventory(admitted, cells)
        require(admitted_inventory == {'pending_qos0': 12, 'pending_qos1': 8, 'retained': 12,
                'publications': 20, 'payloads': 20}, 'message_expiry_offline_positive_inventory_missing')
        require(admitted['counts']['outbound'] == 0, 'message_expiry_offline_work_started_without_recipient')
        final = await until_state(broker, physically_drained, 'message_expiry_offline_references_not_reclaimed')
        write_json(case / 'offline-expired.sql.json', final)
        resumed = []
        for receiver, name, granted, direction in targets:
            client, tap = await peer(receiver, broker, case, name + '-resumed', client_id=name, present=True)
            peers.append((client, tap))
            await client.no_message(.08)
            require(not tap.publications(), 'message_expiry_offline_expired_work_replayed')
            witness, witness_tap = await peer('mqttium', broker, case, name + '-witness', session_expiry=0)
            peers.append((witness, witness_tap))
            payload = ('timeless-after-offline-expiry:' + name).encode()
            await witness.publish(f'message-expiry/offline/{direction}/{granted}/witness', payload, 1)
            delivery = await client.receive(payload, granted, False)
            wire = one_delivery(tap, payload, granted, False, None)
            resumed.append({'client': receiver, 'session_present': True, 'grant': granted,
                            'delivery': delivery, 'wire': wire})
        newcomer, tap = await peer('paho', broker, case, 'offline-new-wildcard', session_expiry=0)
        peers.append((newcomer, tap))
        await newcomer.subscribe((Subscription('message-expiry/offline/#', 1),))
        await newcomer.no_message(.08)
        require(not tap.publications(), 'message_expiry_offline_expired_current_replayed')
        await until_state(broker, physically_drained, 'message_expiry_offline_witness_not_finished')
        await close_peers(peers)
        peers = []
        broker.stop()
        writer_available(broker.db)
        return {'criterion': 'offline', 'status': 'PASS', 'ready': ready, 'cells': cells,
                'admitted_counts': admitted['counts'], 'expired_counts': final['counts'],
                'admitted_inventory': admitted_inventory,
                'resumed_sessions': resumed, 'process_status': broker.process.returncode}
    finally:
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        await close_peers(peers, cleanup=True)
        broker.cleanup()


async def original_ttl(binary, case):
    """Offline delivery and retained replay share the original ingress deadline."""
    broker = Broker(binary, case)
    peers = []
    try:
        ready = broker.ready()
        target, tap = await peer('paho', broker, case, 'original-offline')
        await target.subscribe((Subscription('message-expiry/original/#', 1, retain_handling=2),))
        await close_peers(((target, tap),))
        await until_connection_closed(broker, 'original-offline')
        source, source_tap = await peer('mqttium', broker, case, 'original-source', session_expiry=0)
        peers.append((source, source_tap))
        payload = b'one-original-expiry-for-offline-and-retained'
        started = time.monotonic_ns()
        marker = max(int(event['seq']) for event in broker.events())
        await source.publish('message-expiry/original/value', payload, 2, retain=True, message_expiry=2)
        ingress = await received_ingress(broker, 'message-expiry/original/value', marker)
        admitted = await until_state(broker, lambda state: state['counts']['obligations'] == 1 and
                                    state['counts']['retained'] == 1, 'message_expiry_original_not_admitted')
        stamp = expiry_stamp(admitted['tables']['publications'][0])
        require(stamp == {key: ingress['details'][key] for key in ('domain', 'tick', 'interval')},
                'message_expiry_first_ingress_stamp_changed')
        write_json(case / 'original-admitted.sql.json', admitted)
        await asyncio.sleep(1.1)
        target, target_tap = await peer('paho', broker, case, 'original-resumed',
                                       client_id='original-offline', present=True)
        peers.append((target, target_tap))
        offline_row = await target.receive(payload, 1, False)
        offline_wire = one_delivery(target_tap, payload, 1, False, 1)
        newcomer, newcomer_tap = await peer('mqttium', broker, case, 'original-replay', session_expiry=0)
        peers.append((newcomer, newcomer_tap))
        await newcomer.subscribe((Subscription('message-expiry/original/#', 1),))
        retained_row = await newcomer.receive(payload, 1, True)
        retained_wire = one_delivery(newcomer_tap, payload, 1, True, 1)
        replayed = snapshot(broker, 'original-after-replays')
        copies = original_copies(replayed, admitted['tables']['publications'][0]['id'], stamp)
        final = await until_state(broker, physically_drained, 'message_expiry_original_deadline_was_reset')
        elapsed = time.monotonic_ns() - started
        require(elapsed < 3_000_000_000, 'message_expiry_original_deadline_was_reset', str(elapsed))
        await target.no_message(.04)
        await newcomer.no_message(.04)
        write_json(case / 'original-expired.sql.json', final)
        await close_peers(peers)
        peers = []
        broker.stop()
        writer_available(broker.db)
        return {'criterion': 'original-ttl', 'status': 'PASS', 'ready': ready,
                'original_ttl': 2, 'delay_before_replay_seconds': 1.1, 'elapsed_ns': str(elapsed),
                'ingress': ingress, 'original_durable_stamp': stamp,
                'observed_original_and_replay_copies': copies,
                'offline_delivery': offline_row, 'offline_wire': offline_wire,
                'retained_delivery': retained_row, 'retained_wire': retained_wire,
                'expired_counts': final['counts'], 'process_status': broker.process.returncode}
    finally:
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        await close_peers(peers, cleanup=True)
        broker.cleanup()


async def replay32(binary, case):
    """All 32 valid expiring retained values replay to one durable target."""
    broker = Broker(binary, case)
    peers = []
    try:
        ready = broker.ready()
        source, source_tap = await peer('mqttium', broker, case, 'replay32-source', session_expiry=0)
        peers.append((source, source_tap))
        payloads = {f'message-expiry/replay32/{index:02}': f'expiring-current:{index:02}'.encode()
                    for index in range(32)}
        for topic, payload in payloads.items():
            await source.publish(topic, payload, 1, retain=True, message_expiry=INFINITE)
        before = snapshot(broker, 'replay32-before')
        require(before['counts']['retained'] == before['counts']['publications'] == before['counts']['payloads'] == 32,
                'message_expiry_replay32_inventory_incomplete')
        target, tap = await peer('paho', broker, case, 'replay32-target', session_expiry=0)
        peers.append((target, tap))
        await target.subscribe((Subscription('message-expiry/replay32/#', 1),))
        received = [await target.receive(qos=1, retain=True) for _ in range(32)]
        require(len({row['topic'] for row in received}) == 32 and
                all(row['topic'] in payloads and row['payload_sha256'] == hashlib.sha256(payloads[row['topic']]).hexdigest()
                    and row['message_expiry_interval'] is not None and
                    0 < row['message_expiry_interval'] <= INFINITE for row in received),
                'message_expiry_replay32_wrong_inventory')
        wire = [one_delivery(tap, payload, 1, True, INFINITE) for payload in payloads.values()]
        await target.no_message(.08)
        await until_state(broker, pending_drained, 'message_expiry_replay32_not_durably_finished')
        await target.unsubscribe(('message-expiry/replay32/#',))
        for topic in payloads:
            await source.publish(topic, b'', 1, retain=True)
        final = await until_state(broker, physically_drained, 'message_expiry_replay32_clear_leaked')
        write_json(case / 'replay32-final.sql.json', final)
        await close_peers(peers)
        peers = []
        broker.stop()
        writer_available(broker.db)
        return {'criterion': 'replay32', 'status': 'PASS', 'ready': ready, 'count': 32,
                'received': received, 'wire': wire, 'final_counts': final['counts'],
                'process_status': broker.process.returncode}
    finally:
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        await close_peers(peers, cleanup=True)
        broker.cleanup()


async def options(binary, case):
    """RH, RAP and No Local eligibility are applied to expiring values."""
    broker = Broker(binary, case)
    peers, decisions = [], []
    topic, payload = 'message-expiry/options/value', b'expiring-retained-options'
    try:
        ready = broker.ready()
        source, source_tap = await peer('mqttium', broker, case, 'options-first-source',
                                       client_id='options-producer-name', session_expiry=0)
        await source.publish(topic, payload, 2, retain=True, message_expiry=INFINITE)
        await close_peers(((source, source_tap),))
        await until_state(broker, lambda state: all(row['client_id'] != b'options-producer-name'.hex()
                          for row in state['tables']['sessions']), 'message_expiry_options_source_not_terminated')
        source, source_tap = await peer('paho', broker, case, 'options-same-producer-new-session',
                                       client_id='options-producer-name', session_expiry=0)
        peers.append((source, source_tap))
        await source.subscribe((Subscription('message-expiry/options/#', 1, no_local=True,
                                             retain_as_published=True),))
        await source.no_message(.04)
        require(not source_tap.publications(payload), 'message_expiry_no_local_retained_name_lost')
        await source.subscribe((Subscription(topic, 0),))
        self_replay = await source.receive(payload, 0, True)
        one_delivery(source_tap, payload, 0, True, INFINITE)
        target, tap = await peer('mqttium', broker, case, 'options-other-target', session_expiry=0)
        peers.append((target, tap))
        requests = (
            ((Subscription('message-expiry/options/#', 1, retain_handling=1),) * 2, 1, 'RH1-ordered-duplicate'),
            ((Subscription('message-expiry/options/#', 1),) * 2, 2, 'RH0-duplicate-occurrences'),
            ((Subscription('message-expiry/options/#', 1, retain_handling=2),), 0, 'RH2-suppressed'),
            ((Subscription('message-expiry/options/#', 1, retain_handling=1),), 0, 'RH1-existing-suppressed'),
            ((Subscription('message-expiry/options/#', 1), Subscription(topic, 1)), 2, 'overlap-replays'),
        )
        for filters, count, label in requests:
            prior = len(tap.publications(payload))
            await target.subscribe(filters)
            received = [await target.receive(payload, 1, True) for _ in range(count)]
            await target.no_message(.04)
            wire = tap.publications(payload)[prior:]
            require(len(wire) == count and all(row.retain and row.message_expiry_interval is not None and
                    0 < row.message_expiry_interval <= INFINITE for row in wire),
                    'message_expiry_options_replay_count_or_property')
            await until_state(broker, pending_drained, 'message_expiry_options_replay_not_finished')
            decisions.append({'case': label, 'expected_replays': count, 'received': received,
                              'wire': [row.observation() for row in wire]})
        first = b'no-local-excluded-rap-and-higher-qos'
        await source.publish(topic, first, 1, retain=True, message_expiry=INFINITE)
        excluded = await source.receive(first, 0, False)
        excluded_wire = one_delivery(source_tap, first, 0, False, INFINITE)
        other_first = await target.receive(first, 1, False)
        one_delivery(tap, first, 1, False, INFINITE)
        await source.subscribe((Subscription('message-expiry/options/#', 1, no_local=False,
                                             retain_as_published=True, retain_handling=2),))
        second = b'eligible-overlap-contributes-rap-and-higher-qos'
        await source.publish(topic, second, 2, retain=True, message_expiry=INFINITE)
        eligible = await source.receive(second, 1, True)
        eligible_wire = one_delivery(source_tap, second, 1, True, INFINITE)
        other_second = await target.receive(second, 1, False)
        one_delivery(tap, second, 1, False, INFINITE)
        await until_state(broker, pending_drained, 'message_expiry_options_live_not_finished')
        await source.unsubscribe(('message-expiry/options/#', topic))
        await target.unsubscribe(('message-expiry/options/#', topic))
        await source.publish(topic, b'', 1, retain=True)
        final = await until_state(broker, physically_drained, 'message_expiry_options_clear_leaked')
        write_json(case / 'options-final.sql.json', final)
        await close_peers(peers)
        peers = []
        broker.stop()
        writer_available(broker.db)
        return {'criterion': 'options', 'status': 'PASS', 'ready': ready, 'ordered_replays': decisions,
                'producer_name_after_session_end': True, 'eligible_self_replay': self_replay,
                'excluded_contribution': excluded, 'excluded_wire': excluded_wire,
                'eligible_contribution': eligible, 'eligible_wire': eligible_wire,
                'other_live_deliveries': [other_first, other_second], 'process_status': broker.process.returncode}
    finally:
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        await close_peers(peers, cleanup=True)
        broker.cleanup()


async def late_puback(binary, case):
    """A real started QoS1 transfer completes when its PUBACK arrives after TTL."""
    broker = Broker(binary, case)
    peers, results = [], []
    try:
        ready = broker.ready()
        for retain in (False, True):
            label = f'late-{int(retain)}'
            source, source_tap = await peer('mqttium', broker, case, label + '-source', session_expiry=0)
            target, tap = await peer('paho', broker, case, label + '-target', session_expiry=0)
            peers.extend(((source, source_tap), (target, tap)))
            topic, payload = f'message-expiry/{label}/value', f'late-puback:{int(retain)}'.encode()
            await target.subscribe((Subscription(topic, 1, retain_as_published=True, retain_handling=2),))
            marker = max(int(event['seq']) for event in broker.events())
            started = time.monotonic_ns()
            await source.publish(topic, payload, 1, retain=retain, message_expiry=2)
            row = await target.receive(payload, 1, retain, acknowledge=False)
            wire = one_delivery(tap, payload, 1, retain, 2)
            held = await until_state(broker, lambda state: state['counts']['outbound'] == 1 and
                state['counts']['obligations'] == 1, 'message_expiry_late_puback_not_held')
            write_json(case / f'{label}-held.sql.json', held)
            # The actual receiving application has deliberately retained its
            # manual ACK; this is not a sleep used to infer an unobserved ACK.
            await asyncio.sleep(2.15)
            expired = snapshot(broker, f'{label}-past-ttl')
            require(expired['counts']['outbound'] == expired['counts']['obligations'] == 1 and
                    expired['counts']['publications'] == expired['counts']['payloads'] == 1 and
                    expired['counts']['retained'] == 0,
                    'message_expiry_started_transfer_expired_before_puback', repr(expired['counts']))
            proof = [event for event in broker.events() if int(event['seq']) > marker and
                     event['kind'] == 'store_result' and event.get('command_kind') == 'DeliveryStarted' and
                     event.get('details', {}).get('outcome') == 'Committed']
            require(len(proof) == 1, 'message_expiry_started_proof_not_committed', str(len(proof)))
            await target.no_message(.04)
            await target.acknowledge(row)
            elapsed = time.monotonic_ns() - started
            require(elapsed > 2_000_000_000, 'message_expiry_puback_was_not_after_ttl')
            final = await until_state(broker, physically_drained, 'message_expiry_late_puback_did_not_finish')
            write_json(case / f'{label}-finished.sql.json', final)
            results.append({'retain': retain, 'delivered': row, 'wire': wire,
                            'started_commit': proof[0], 'elapsed_before_puback_ns': str(elapsed),
                            'pending_past_ttl': expired['counts'], 'final_counts': final['counts']})
            await close_peers(peers)
            peers = []
        broker.stop()
        writer_available(broker.db)
        return {'criterion': 'late-puback', 'status': 'PASS', 'ready': ready, 'cases': results,
                'process_status': broker.process.returncode}
    finally:
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        await close_peers(peers, cleanup=True)
        broker.cleanup()


async def malformed(binary, case):
    """Duplicate and truncated 0x02 properties are rejected before mutation."""
    from message_expiry_probe import MessageExpiryWire, publish_packet
    from retained_reception import business_state
    from tcp_reception import expect_rejection
    broker = Broker(binary, case)
    peer_wire = None
    results = []
    try:
        ready = broker.ready()
        properties = (
            ('duplicate-zero', b'\x02\x00\x00\x00\x00' * 2),
            ('duplicate-finite', b'\x02\x00\x00\x00\x02' * 2),
            ('missing-four-byte-value', b'\x02'),
            ('short-four-byte-value', b'\x02\x00\x00\x02'),
        )
        for qos in (0, 1, 2):
            for retain in (False, True):
                for name, props in properties:
                    label = f'malformed-{qos}-{int(retain)}-{name}'
                    peer_wire = MessageExpiryWire(broker.address, broker.observations, label)
                    peer_wire.session_connect(label, expiry=0, expected_present=False)
                    before = snapshot(broker, label + '-before')
                    marker = max(int(event['seq']) for event in broker.events())
                    sent = publish_packet('message-expiry/malformed/value', b'', 7 if qos else None,
                                          qos=qos, retain=retain, property_bytes=props)
                    peer_wire.send(sent)
                    expect_rejection(peer_wire)
                    peer_wire.close()
                    peer_wire = None
                    after = await until_state(broker, lambda state: physically_drained(state) and
                        all(row['client_id'] != label.encode().hex() for row in state['tables']['sessions']),
                        'message_expiry_malformed_connection_not_terminated')
                    require(business_state(after) == business_state(before),
                            'message_expiry_malformed_properties_partially_mutated')
                    commits = [event for event in broker.events() if int(event['seq']) > marker and
                               event['kind'] == 'store_result' and
                               event.get('command_kind') in ('Admission', 'RetainedQoS0') and
                               event.get('details', {}).get('outcome') == 'Committed']
                    require(not commits, 'message_expiry_malformed_properties_admitted')
                    results.append({'case': name, 'qos': qos, 'retain': retain, 'frame_hex': sent.hex(),
                                    'business_unchanged': True})
        broker.stop()
        writer_available(broker.db)
        return {'criterion': 'malformed', 'status': 'PASS', 'ready': ready, 'cases': results,
                'process_status': broker.process.returncode}
    finally:
        if peer_wire is not None:
            peer_wire.close()
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        broker.cleanup()


async def minimal(binary, case):
    """TTL2 retained QoS1 expires without any subscriber; timeless works next."""
    broker = Broker(binary, case)
    peers = []
    topic = 'message-expiry/minimal/value'
    expired, timeless = b'expiring-retained-with-no-subscriber', b'timeless-after-expiry'
    try:
        ready = broker.ready()
        source, source_tap = await peer('mqttium', broker, case, 'minimal-source', session_expiry=0)
        peers.append((source, source_tap))
        start = time.monotonic_ns()
        await source.publish(topic, expired, 1, retain=True, message_expiry=2)
        admitted = snapshot(broker, 'admitted')
        require(admitted['counts']['retained'] == 1 and admitted['counts']['payloads'] == 1,
                'message_expiry_retained_not_durable')
        await close_peers(peers)
        peers = []
        final = await until_state(broker, physically_drained, 'message_expiry_references_not_physically_reclaimed')
        elapsed = time.monotonic_ns() - start
        require(elapsed >= 1_900_000_000, 'message_expiry_retired_before_original_ttl')
        write_json(case / 'expired.sql.json', final)
        target, target_tap = await peer('paho', broker, case, 'minimal-target')
        peers.append((target, target_tap))
        await target.subscribe((Subscription('message-expiry/+/#', 1),))
        await target.no_message(.2)
        require(not target_tap.publications(expired), 'message_expiry_expired_retained_replayed')
        witness, witness_tap = await peer('mqttium', broker, case, 'minimal-witness', session_expiry=0)
        peers.append((witness, witness_tap))
        await witness.publish(topic, timeless, 1, retain=True)
        live = await target.receive(timeless, 1, False)
        wire = one_delivery(target_tap, timeless, 1, False, None)
        end = await until_state(broker, lambda state: state['counts']['retained'] == 1 and
            all(state['counts'][name] == 0 for name in ('obligations', 'outbound', 'inbound')),
            'message_expiry_timeless_cycle_not_finished')
        write_json(case / 'timeless-final.sql.json', end)
        await close_peers(peers)
        peers = []
        status = broker.stop()
        writer_available(broker.db)
        return {'criterion': 'minimal', 'status': 'PASS', 'ready': ready,
                'ttl_seconds': 2, 'elapsed_ns': str(elapsed), 'admitted_counts': admitted['counts'],
                'expired_counts': final['counts'], 'absence_scope': 'fresh wildcard after physical cleanup',
                'new_timeless_delivery': live, 'independent_wire': wire,
                'clients': [source.result, target.result, witness.result], 'process_status': status}
    finally:
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        await close_peers(peers, cleanup=True)
        broker.cleanup()


async def capacity(binary, case):
    """Thirty-two finite retained values expire and return a real quota slot."""
    from retained_reception import business_state
    broker = Broker(binary, case)
    peers = []
    try:
        ready = broker.ready()
        require(ready['details']['retained_topics'] == 32, 'message_expiry_retained_quota_changed')
        source, source_tap = await peer('mqttium', broker, case, 'capacity-source', session_expiry=0)
        peers.append((source, source_tap))
        started = time.monotonic_ns()
        for index in range(32):
            await source.publish(f'message-expiry/capacity/{index:02}', f'expiring-slot:{index:02}'.encode(),
                                 1, retain=True, message_expiry=2)
        full = snapshot(broker, 'capacity-full')
        require(full['counts']['retained'] == full['counts']['publications'] == full['counts']['payloads'] == 32
                and pending_drained(full) and int(full['tables']['meta'][0]['used_references'], 16) == 32,
                'message_expiry_capacity_was_not_full', repr(full['counts']))
        before = business_state(full)
        ack_cursor = len(source_tap.server_frames())
        refused = await source.publish_rejected('message-expiry/capacity/overflow', b'no-room-before-expiry',
                                                 reason=0x97, retain=True, message_expiry=2)
        negative = negative_puback(source_tap, refused['packet_id'], after=ack_cursor)
        after_refusal = snapshot(broker, 'capacity-after-refusal')
        require(business_state(after_refusal) == before, 'message_expiry_capacity_refusal_partially_mutated')
        expired = await until_state(broker, physically_drained, 'message_expiry_capacity_not_physically_reclaimed')
        elapsed = time.monotonic_ns() - started
        require(elapsed >= 1_900_000_000, 'message_expiry_capacity_retired_before_original_ttl')
        write_json(case / 'capacity-expired.sql.json', expired)
        payload = b'new-retained-value-fits-after-physical-expiry'
        await source.publish('message-expiry/capacity/overflow', payload, 1, retain=True)
        accepted = snapshot(broker, 'capacity-new-accepted')
        require(accepted['counts']['retained'] == accepted['counts']['publications'] ==
                accepted['counts']['payloads'] == 1, 'message_expiry_capacity_slot_not_returned')
        target, tap = await peer('paho', broker, case, 'capacity-target', session_expiry=0)
        peers.append((target, tap))
        await target.subscribe((Subscription('message-expiry/capacity/#', 1),))
        delivered = await target.receive(payload, 1, True)
        wire = one_delivery(tap, payload, 1, True, None)
        await target.no_message(.04)
        await target.unsubscribe(('message-expiry/capacity/#',))
        await source.publish('message-expiry/capacity/overflow', b'', 1, retain=True)
        final = await until_state(broker, physically_drained, 'message_expiry_capacity_witness_clear_leaked')
        write_json(case / 'capacity-final.sql.json', final)
        await close_peers(peers)
        peers = []
        broker.stop()
        writer_available(broker.db)
        return {'criterion': 'capacity', 'status': 'PASS', 'ready': ready, 'full_retained_topics': 32,
                'ttl_seconds': 2, 'elapsed_before_reclamation_ns': str(elapsed),
                'overflow': refused, 'negative_wire': {'header': negative.header,
                    'body_hex': negative.body.hex(), 'tap_frame_cursor': ack_cursor},
                'refusal_business_unchanged': True,
                'full_counts': full['counts'], 'expired_counts': expired['counts'],
                'new_accepted_counts': accepted['counts'], 'delivery': delivered, 'wire': wire,
                'final_counts': final['counts'], 'process_status': broker.process.returncode}
    finally:
        if broker.db.is_file():
            snapshot(broker, 'last-observed')
        await close_peers(peers, cleanup=True)
        broker.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary-source-head', required=True)
    functions = {'minimal': minimal, 'matrix': matrix, 'offline': offline,
                 'original-ttl': original_ttl, 'replay32': replay32, 'options': options,
                 'late-puback': late_puback, 'malformed': malformed, 'capacity': capacity}
    parser.add_argument('--case', choices=('normal', *functions), default='normal')
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    head = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD']).decode().strip()
    receipt = {'format': 'mqttiumd.message-expiry-system-reception', 'schema_version': 1,
               'status': 'FAIL', 'qualification': 'system-test-only', 'source_head': args.binary_source_head,
               'harness_source_head': head, 'client_identity': client_identity(),
               'binary': {'path': str(args.binary.resolve()),
               'sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest()}, 'cases': []}
    try:
        require(head == args.binary_source_head, 'message_expiry_source_mismatch')
        selected = tuple(functions) if args.case == 'normal' else (args.case,)
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
