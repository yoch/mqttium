#!/usr/bin/env python3
"""Product-level witnesses for retained Will authority, ordering and resources."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import ExitStack, contextmanager
import hashlib
import json
from pathlib import Path
import tempfile
import time

from external_mqttium import client_identity
from external_will import PahoWill
from external_wildcard import Subscription
from pubsub_probe import durable, session_key
from retained_probe import retained_publish_packet
from secure_reception import Peer, broker_with, client_context, latest, observed, pki_module
from session_reception import connect_bytes
from tcp_probe import operation_hex, packet, require, write_json
from tcp_reception import Broker
from will_boundaries import fact
from will_probe import WillWire
from will_reception import drained, no_local, public_sensor, triggers, until_state

TOPIC = 'atelier/capteur-1/status'


def retained_value(state, topic=TOPIC):
    record = next((row for row in state['tables']['retained'] if row['topic'] == topic.encode().hex()), None)
    if record is None:
        return None
    publication = next(row for row in state['tables']['publications'] if row['id'] == record['publication'])
    payload = next((row for row in state['tables']['payloads'] if row['id'] == publication['payload']), None)
    require(payload is not None, 'retained_will_current_payload_missing')
    return {**record, 'bytes': payload['bytes'], 'refs': int(payload['refs'], 16)}


@contextmanager
def wires(binary, case):
    broker = Broker(binary, case)
    with ExitStack() as resources:
        resources.callback(broker.cleanup)
        ready = broker.ready()['details']

        def open_wire(label, *, client=None, present=False, expiry=0):
            wire = WillWire(broker.address, broker.observations, label)
            resources.callback(wire.close)
            if client is not None:
                wire.session_connect(client, expected_present=present, expiry=expiry)
            return wire

        yield broker, open_wire, ready
        broker.stop()


def receive(wire, payload, *, retain, qos=1, topic=TOPIC):
    message = wire.next_publication(timeout=5)
    require((message.topic, message.payload, message.qos, message.retain) == (topic, payload, qos, retain),
            'retained_will_payload_qos_or_flag', repr(message.observation()))
    if qos:
        wire.acknowledge(message)
    return message.observation()


async def disconnect(broker, wire):
    before = latest(broker)
    wire.send(b'\xe0\x00')
    await fact(broker, 'connection_closed', after=before,
               predicate=lambda row: row['details']['code'] == 'client_disconnect')
    wire.close()


async def no_subscribers(binary, case):
    with wires(binary, case) as (broker, wire, _):
        sensor = wire('ephemeral-sensor')
        sensor.will_connect('sensor', TOPIC, retain=True, expiry=0, delay=None)
        installed = durable(broker.db)
        require(len(installed['tables']['wills']) == 1 and installed['tables']['wills'][0]['retain'] == 1,
                'retained_will_flag_lost_at_open')
        sensor.publish(1, TOPIC, b'online')
        before = retained_value(durable(broker.db))
        await fact(broker, 'will_installed')
        sensor.close()
        commit = await fact(broker, 'will_admitted')
        after = durable(broker.db)
        current = retained_value(after)
        require(commit['details']['targets'] == 0 and after['counts']['sessions'] == 0,
                'retained_will_zero_target_or_source_cleanup_witness')
        require(current is not None and current['bytes'] == b'offline'.hex() and
                current['publication'] != before['publication'] and current['producer_client_id'] == b'sensor'.hex(),
                'will_without_targets_missing_retained_value')
        require(after['counts']['wills'] == 0 and after['counts']['publications'] == after['counts']['payloads'] == 1
                and current['refs'] == 1, 'retained_will_zero_target_accounts')
        late = wire('late', client='late')
        late.subscribe_options(1, (Subscription('atelier/+/status', 1),))
        publication = receive(late, b'offline', retain=True)
        await drained(broker)
        write_json(case / 'after-will.sql.json', after)
        return {'admission': commit, 'replay': publication, 'resources': after['counts']}


async def routing(binary, case):
    with wires(binary, case) as (broker, wire, _):
        route = Subscription('atelier/+/status', 1, retain_handling=2)
        frozen = wire('absent', client='absent', expiry=0xFFFFFFFF)
        frozen.subscribe_options(1, (route,))
        await disconnect(broker, frozen)
        rap0 = wire('rap0', client='rap0')
        rap1 = wire('rap1', client='rap1')
        rap0.subscribe_options(1, (route,))
        rap1.subscribe_options(1, (Subscription(TOPIC, 0, retain_as_published=True, retain_handling=2), route))
        source = wire('source')
        source.will_connect('sensor', TOPIC, retain=True, expiry=0)
        source.publish(1, TOPIC, b'online')
        receive(rap0, b'online', retain=False)
        receive(rap1, b'online', retain=True)
        online = retained_value(durable(broker.db))
        source.close()
        commit = await fact(broker, 'will_admitted')
        image = durable(broker.db)
        offline = retained_value(image)
        require(offline and offline['bytes'] == b'offline'.hex() and commit['details']['targets'] == 3,
                'retained_will_fanout_or_value_wrong')
        old_id = online['publication']
        old_pub = next(row for row in image['tables']['publications'] if row['id'] == old_id)
        old_payload = next((row for row in image['tables']['payloads'] if row['id'] == old_pub['payload']), None)
        require(old_payload and old_payload['bytes'] == b'online'.hex()
                and any(row['publication'] == old_id for row in image['tables']['obligations']),
                'retained_will_replacement_freed_owed_payload')
        stop0 = receive(rap0, b'offline', retain=False)
        stop1 = receive(rap1, b'offline', retain=True)
        rap1.no_publication(.06, code='retained_will_duplicate_overlap')
        absent = wire('resumed', client='absent', present=True, expiry=0xFFFFFFFF)
        owed = [receive(absent, payload, retain=False) for payload in (b'online', b'offline')]
        await drained(broker)
        require(not any(row['id'] == old_id for row in durable(broker.db)['tables']['publications']),
                'retained_will_previous_value_still_charged')

        same_client = wire('new-incarnation', client='sensor')
        same_client.subscribe_options(1, (Subscription(TOPIC, 1, no_local=True),))
        same_client.no_publication(.08, code='retained_will_no_local_lost_after_source_cleanup')
        same_client.subscribe_options(2, (Subscription(TOPIC, 1, retain_handling=1),))
        same_client.no_publication(.08, code='retained_will_rh1_replayed_existing_filter')
        same_client.subscribe_options(3, (Subscription(TOPIC, 1),))
        replayed = receive(same_client, b'offline', retain=True)
        dashboard = wire('overlap', client='overlap')
        dashboard.subscribe_options(1, (Subscription(TOPIC, 0, retain_handling=1), Subscription('atelier/#', 1)))
        overlap = [receive(dashboard, b'offline', retain=True, qos=qos) for qos in (0, 1)]
        dashboard.no_publication(.08, code='retained_will_duplicate_replay')

        await disconnect(broker, same_client)
        clearing = wire('empty-will')
        clearing.will_connect('sensor', TOPIC, b'', retain=True, expiry=0)
        clearing.send(packet(0xE0, b'\x04\x00'))
        empty = [receive(rap0, b'', retain=False), receive(rap1, b'', retain=True),
                 receive(absent, b'', retain=False), receive(dashboard, b'', retain=False)]
        after = await drained(broker)
        require(after['counts']['retained'] == after['counts']['payloads'] == 0,
                'retained_will_empty_payload_failed_to_clear')
        fresh = wire('fresh', client='fresh')
        fresh.subscribe_options(1, (Subscription(TOPIC, 1),))
        fresh.no_publication(.08, code='retained_will_empty_payload_replayed')
        write_json(case / 'replacement.sql.json', image)
        write_json(case / 'cleared.sql.json', after)
        return {'live': [stop0, stop1], 'owed': owed, 'same_client_rh0': replayed,
                'overlap_replay': overlap, 'empty': empty, 'final': after['counts']}


async def pressure(binary, case):
    with wires(binary, case) as (broker, wire, ready):
        require(ready['retained_topics'] == 32 and ready['max_targets'] == 16, 'retained_will_quota_changed')
        sensor = wire('fill', client='filler')
        fill_topic = lambda index: f'fill/{index}'
        for index in range(31):
            sensor.publish(index + 1, fill_topic(index), b'filled')
        sensor.publish(32, TOPIC, b'online')
        replace = wire('replace')
        replace.will_connect('replacement', fill_topic(1), b'changed', retain=True, expiry=0)
        replace.close()
        first = await fact(broker, 'will_admitted')
        full = durable(broker.db)
        require(full['counts']['retained'] == full['counts']['payloads'] == 32
                and retained_value(full, fill_topic(1))['bytes'] == b'changed'.hex(),
                'retained_will_net_replacement_at_capacity_failed')

        extra = wire('extra')
        extra.will_connect('extra', fill_topic(31), b'added', retain=True, expiry=0)
        extra.close()
        pending = await until_state(broker, lambda state: any(row['state'] == 2 for row in state['tables']['wills']),
                                    'retained_will_33rd_pending_missing')
        await fact(broker, 'will_deferred', after=int(first['seq']))
        require(pending['counts']['retained'] == 32 and retained_value(pending, fill_topic(31)) is None
                and pending['counts']['obligations'] == 0, 'retained_will_33rd_partial_admission')
        sensor.publish(33, fill_topic(30), b'')
        second = await fact(broker, 'will_admitted', after=int(first['seq']))
        image = durable(broker.db)
        require(image['counts']['retained'] == 32 and retained_value(image, fill_topic(31))['bytes'] == b'added'.hex(),
                'retained_will_33rd_did_not_resume')

        for index in range(17):
            subscriber = wire(f'add-{index}', client=f'target-{index}', expiry=0xFFFFFFFF)
            filters = (TOPIC, 'atelier/+/status', 'atelier/#')
            subscriber.subscribe_options(1, (Subscription(filters[index % 3], 1, retain_handling=2),))
            await disconnect(broker, subscriber)
        dying = wire('losing-transport')
        dying.will_connect('presence', TOPIC, retain=True, expiry=0)
        dying.close()
        deferred = await fact(broker, 'will_deferred', after=int(second['seq']),
                              predicate=lambda row: row['details']['code'] == 'fanout_capacity')
        blocked = durable(broker.db)
        require(blocked['counts']['wills'] == 1 and blocked['counts']['obligations'] == 0
                and retained_value(blocked)['bytes'] == b'online'.hex(), 'retained_will_fanout_truncated')
        reconnect = wire('reconnect')
        # Reconnect without a new Will so only the runtime durable barrier can
        # hold it. The original Will's source session is already gone.
        mark = latest(broker)
        reconnect.send(connect_bytes('presence', expiry=0))
        sensor.send(b'\xc0\x00')
        require(sensor.wait(0xd0).body == b'', 'retained_will_barrier_blocked_independent_ping')
        control_topic = 'independent/heartbeat'
        sensor.send(retained_publish_packet(control_topic, b'independent', 34, retain=False))
        sensor.wait(0x40, 34)
        try:
            barrier = await fact(broker, 'will_connect_deferred', after=mark, timeout=.7)
        except Exception as error:
            early = reconnect.poll(.2)
            require(early is None or early.kind != 0x20 or early.body[1] != 0,
                    'retained_will_reconnect_overtook_pending_write')
            raise error
        early = reconnect.poll(.07)
        require(early is None, 'retained_will_reconnect_overtook_pending_write')
        # Remove exactly one eligibility and preserve the other sixteen.
        last = wire('unsubscribe', client='target-16', present=True, expiry=0xFFFFFFFF)
        last.unsubscribe(1, ['atelier/+/status'])
        receipt = reconnect.wait(0x20)
        require(receipt.body[:2] == b'\x00\x00', 'retained_will_reconnect_not_fresh')
        committed = await fact(broker, 'will_admitted', after=int(deferred['seq']))
        offline = retained_value(durable(broker.db))
        require(committed['details']['targets'] == 16 and operation_hex(committed['will']) == blocked['tables']['wills'][0]['id']
                and offline['bytes'] == b'offline'.hex(), 'retained_will_16_target_admission_wrong')

        # Drain owed Will deliveries before publishing online to keep the
        # independent last-reference checks unambiguous.
        deliveries = []
        for index in range(16):
            resumed = wire(f'target-{index}-resumed', client=f'target-{index}', present=True, expiry=0xFFFFFFFF)
            deliveries.append(receive(resumed, b'offline', retain=False))
            await disconnect(broker, resumed)
        await drained(broker)
        reconnect.publish(1, TOPIC, b'online-newer')
        for index in range(16):
            resumed = wire(f'target-{index}-newer', client=f'target-{index}', present=True, expiry=0xFFFFFFFF)
            receive(resumed, b'online-newer', retain=False)
            await disconnect(broker, resumed)
        final = await drained(broker)
        require(retained_value(final)['bytes'] == b'online-newer'.hex() and final['counts']['wills'] == 0,
                'retained_will_old_intent_overwrote_newer_online')
        for name, image in (('full', full), ('blocked', blocked), ('final', final)):
            write_json(case / f'{name}.sql.json', image)
        return {'deferred_33rd': pending['tables']['wills'], 'fanout17': deferred, 'barrier': barrier,
                'fanout16': committed, 'deliveries': deliveries, 'final': final['counts']}


async def secure_cycles(binary, case):
    with tempfile.TemporaryDirectory(prefix='retained-will-pki-') as temp, ExitStack() as stack:
        pki = Path(temp) / 'pki'
        pki_module.generate(pki)
        broker = broker_with(binary, case, pki)
        stack.callback(broker.cleanup)
        principals = {name: client_context(pki, name) for name in ('sensor', 'dashboard')}

        def peer(label, principal):
            wire = Peer(broker, label, principals[principal])
            stack.callback(wire.close)
            return wire

        dashboard = peer('start-dashboard', 'dashboard')
        dashboard.open('dashboard', expiry=0)
        dashboard.subscribe_options(1, (Subscription('atelier/+/status', 1, retain_as_published=True),))
        source = peer('protected-source', 'sensor')
        source.open('capteur-1', expiry=0, will=(TOPIC, b'takeover'), will_retain=True)
        for actor, client, topic in [('dashboard', 'capteur-1', TOPIC),
                                      ('sensor', 'capteur-1', 'atelier/capteur-10/status')]:
            bad = peer(f'{actor}-forbidden-{topic}', actor)
            bad.open(client, clean=True, will=(topic, b'foreign'), will_retain=True, reason=0x87)
            bad.close()
            source.ping()
            require(durable(broker.db)['counts']['wills'] == 1, 'unauthorized_retained_will_installed')
        replacement = peer('legitimate-takeover', 'sensor')
        replacement.open('capteur-1', expiry=0)
        message = dashboard.message(b'takeover')
        require(message.retain and message.qos == 1, 'retained_will_tls_takeover_wire')
        replacement.send(b'\xe0\x00')
        observed(broker, lambda: not any(row['client_id'] == b'capteur-1'.hex()
                 for row in durable(broker.db)['tables']['sessions']), 'retained_will_takeover_not_closed')

        rounds = []
        for index in range(6):
            live = peer(f'sensor-{index}', 'sensor')
            offline = f'offline-{index}'.encode()
            live.open('capteur-1', expiry=0, will=(TOPIC, offline), will_retain=True)
            live.publish(TOPIC, b'online', retain=True)
            dashboard.message(b'online')
            at = latest(broker)
            stimulus = time.monotonic_ns()
            live.close()
            delivered = dashboard.message(offline)
            received = time.monotonic_ns()
            expected = observed(broker, lambda: next((e for e in broker.events() if e['kind'] == 'will_admitted'
                                and int(e['seq']) > at), None), 'retained_will_tls_commit_missing')
            commit = next(e for e in broker.events() if e['kind'] == 'will_commit_observed'
                          and e['operation'] == expected['operation'])
            closure = next(e for e in broker.events() if int(e['seq']) > at and e['kind'] == 'connection_closed')
            detected = int(closure['details']['observed_monotonic_ns'])
            committed = int(commit['details']['monotonic_ns'])
            require(stimulus <= detected <= committed <= received and delivered.retain and delivered.qos == 1,
                    'retained_will_latency_boundaries_uncorrelated')
            late = peer(f'late-{index}', 'dashboard')
            late.open('dashboard-check', expiry=0)
            sub_at = time.monotonic_ns()
            late.subscribe_options(1, (Subscription('atelier/+/status', 1),))
            replay = late.message(offline)
            replay_at = time.monotonic_ns()
            require(replay.retain and replay.qos == 1, 'retained_will_tls_late_replay')
            late.send(b'\xe0\x00')
            late.close()
            final = observed(broker, lambda: drained_presence(broker), 'retained_will_cycle_not_drained')
            require(retained_value(final)['bytes'] == offline.hex() and final['counts']['payloads'] == 1,
                    'retained_will_cycle_state_grew')
            rss = next(int(line.split()[1]) for line in Path(f'/proc/{broker.process.pid}/status').read_text().splitlines()
                       if line.startswith('VmRSS:'))
            rounds.append({'operation': expected['operation'], 'loss_to_detection_ns': detected - stimulus,
                           'detection_to_commit_and_retained_ns': committed - detected,
                           'commit_to_live_ns': received - committed, 'late_subscribe_to_replay_ns': replay_at - sub_at,
                           'counts': final['counts'], 'references': retained_value(final)['refs'],
                           'sqlite_bytes': broker.db.stat().st_size, 'rss_kib': rss})
        paho = PahoWill(broker.address, 'dashboard-check', expiry=0, events_path=case / 'paho.client.jsonl')
        paho.client.tls_set_context(principals['dashboard'])
        try:
            await paho.open(False)
            await paho.subscribe((Subscription('atelier/+/status', 1),))
            paho_replay = await paho.receive(offline, 1, True)
        finally:
            await paho.disconnect()
        broker.stop()
        return {'cycles': rounds, 'paho_tls_replay': paho_replay, 'paho_client': paho.result,
                'pid': broker.process.pid, 'wait_status': broker.process.returncode}


def drained_presence(broker):
    image = durable(broker.db)
    return image if not any(image['counts'][name] for name in ('wills', 'obligations', 'outbound')) and image['counts']['sessions'] == 1 else None


async def main(args):
    args.out.mkdir(parents=True)
    binary = args.binary.resolve()
    result = {'source_revision': args.source, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'status': 'FAIL', 'cases': {}, 'scope': 'product system tests'}
    cases = {'no-targets': no_subscribers, 'routing': routing, 'pressure': pressure}
    try:
        if args.case in cases:
            result['cases'][args.case] = await cases[args.case](binary, args.out / args.case)
        else:
            result['clients'] = client_identity()
            for name, exercise in cases.items():
                result['cases'][name] = await exercise(binary, args.out / name)
            for source, dest in [('mqttium', 'paho'), ('paho', 'mqttium')]:
                name = f'{source}-to-{dest}'
                result['cases'][name] = await public_sensor(binary, args.out / name, source, dest, retained=True)
            result['cases']['triggers'] = await triggers(binary, args.out / 'triggers', retained=True)
            result['cases']['no-local-live'] = await no_local(binary, args.out / 'no-local-live', retained=True)
            result['cases']['mtls'] = await secure_cycles(binary, args.out / 'mtls')
        result['status'] = 'PASS'
        return 0
    except Exception as error:
        result['failure'] = {'code': getattr(error, 'code', str(error)), 'detail': str(error)[:512],
                             'type': type(error).__name__}
        return 1
    finally:
        write_json(args.out / 'receipt.json', result)
        print(json.dumps({key: result[key] for key in ('status', 'failure') if key in result}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--case', choices=['all', 'no-targets', 'routing', 'pressure'], default='all')
    raise SystemExit(asyncio.run(main(parser.parse_args())))
