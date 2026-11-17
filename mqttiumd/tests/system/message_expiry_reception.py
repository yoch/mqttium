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
from external_sessions import INFINITE
from external_wildcard import Subscription
from pubsub_probe import durable
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


async def close_peers(values):
    for client, tap in values:
        try:
            await client.disconnect()
        finally:
            tap.close()


def snapshot(broker, label):
    state = durable(broker.db)
    write_json(broker.case / f'{label}.sql.json', state)
    return state


def physically_drained(state):
    require(all(name in state['counts'] for name in BUSINESS), 'message_expiry_business_table_missing')
    return all(state['counts'][name] == 0 for name in BUSINESS)


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


def one_delivery(tap, payload, qos, retain, expiry):
    values = tap.publications(payload)
    require(len(values) == 1, 'message_expiry_wire_delivery_count', str(len(values)))
    value = values[0]
    require((value.qos, value.retain) == (qos, retain), 'message_expiry_wire_qos_or_retain')
    require(value.message_expiry_interval == expiry if expiry is None else
            value.message_expiry_interval is not None and 0 < value.message_expiry_interval <= expiry,
            'message_expiry_wire_interval_wrong', repr(value.observation()))
    return value.observation()


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
        await close_peers(peers)
        broker.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary-source-head', required=True)
    parser.add_argument('--case', choices=('minimal',), default='minimal')
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    head = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD']).decode().strip()
    receipt = {'format': 'mqttiumd.message-expiry-system-reception', 'schema_version': 1,
               'status': 'FAIL', 'qualification': 'system-test-only', 'source_head': args.binary_source_head,
               'harness_source_head': head, 'binary': {'path': str(args.binary.resolve()),
               'sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest()}, 'cases': []}
    try:
        require(head == args.binary_source_head, 'message_expiry_source_mismatch')
        receipt['cases'].append(asyncio.run(minimal(args.binary.resolve(), args.out / 'minimal')))
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
