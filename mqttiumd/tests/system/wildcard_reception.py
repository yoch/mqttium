#!/usr/bin/env python3
"""Finite hierarchical wildcard/No Local system tests, not oracle qualification."""
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
from external_wildcard import (INFINITE, MqttiumPeer, PahoPeer, Subscription, PacketTap,
                               MosquittoReceiver, mosquitto_identity, mosquitto_publish,
                               subscribe_packet)
from pubsub_probe import PubsubWire, decode_filter_ack, durable, session_key
from session_reception import SessionWire, EventCursor, session_row, session_snapshot
from tcp_probe import Observations, ReceiptFailure, packet, require, write_json
from tcp_reception import Broker, writer_available


async def peer(kind, broker, case, label, *, expiry=INFINITE, present=False, client_id=None):
    tap = PacketTap(broker.address, Observations(case / f'{label}.wire.jsonl'))
    constructor = MqttiumPeer if kind == 'mqttium' else PahoPeer
    client = constructor(tap.address, client_id or label, expiry=expiry, events_path=case / f'{label}.client.jsonl')
    try:
        await client.open(present)
    except BaseException:
        await client.disconnect();tap.close()
        raise
    return client, tap


async def close_peers(peers):
    for client, tap in peers:
        try:
            await client.disconnect()
        finally:
            tap.close()


def one_wire_delivery(tap, payload, qos, *, code='wildcard_duplicate_or_wrong_qos'):
    messages = tap.publications(payload)
    require(len(messages) == 1 and messages[0].qos == qos, code,
            f'count={len(messages)},qos={[message.qos for message in messages]}')
    return messages[0].observation()


async def demo(binary, case):
    broker = Broker(binary, case)
    peers, mosquitto, tap_mosquitto = [], None, None
    try:
        ready = broker.ready()
        subscriber, tap = await peer('paho', broker, case, 'demo-paho')
        peers.append((subscriber, tap))
        filters = (Subscription('atelier/#', 1), Subscription('atelier/+/temperature', 0),
                   Subscription('atelier/salle1/temperature', 1))
        await subscriber.subscribe(filters)
        publisher, publisher_tap = await peer('mqttium', broker, case, 'demo-mqttium')
        peers.append((publisher, publisher_tap))
        topic, first = 'atelier/salle1/temperature', b'mqttium-to-paho-overlap-once'
        await publisher.publish(topic, first, qos=2)
        got_first = await subscriber.receive(first, 1)
        await subscriber.no_message()
        first_wire = one_wire_delivery(tap, first, 1)
        second = b'mosquitto-to-paho'
        mosquitto_publisher = await mosquitto_publish(broker.address, 'demo-mosquitto-pub', topic, second, 1, case)
        got_second = await subscriber.receive(second, 1)
        second_wire = one_wire_delivery(tap, second, 1)
        tap_mosquitto = PacketTap(broker.address, Observations(case / 'demo-mosquitto.wire.jsonl'))
        mosquitto = MosquittoReceiver(tap_mosquitto.address, 'demo-mosquitto-sub',
                                      ('atelier/+/temperature',), qos=1, count=1, case=case)
        await tap_mosquitto.subscribed(1, (1,))
        third = b'mqttium-to-paho-and-mosquitto'
        await publisher.publish(topic, third, qos=1)
        got_third = await subscriber.receive(third, 1)
        mosquitto_result = await mosquitto.finish()
        require(mosquitto_result['messages'][0]['payload'] == third.decode()
                and mosquitto_result['messages'][0]['qos'] == 1, 'mosquitto_received_wrong_message')
        third_wire = one_wire_delivery(tap_mosquitto, third, 1)
        await close_peers(peers);peers=[]
        tap_mosquitto.close();tap_mosquitto=None
        final = durable(broker.db);write_json(case / 'durable.json', final)
        broker.stop();writer_available(broker.db)
        return {'criterion':'demo','status':'PASS','ready':ready,
                'filters':[asdict(value) for value in filters],
                'clients':[subscriber.result,publisher.result,mosquitto_publisher,mosquitto_result],
                'messages':[got_first,got_second,got_third],
                'independent_wire':[first_wire,second_wire,third_wire],
                'stop_status':broker.process.returncode}
    finally:
        if mosquitto is not None:mosquitto.close()
        await close_peers(peers)
        if tap_mosquitto is not None:tap_mosquitto.close()
        broker.cleanup()


async def matrix(binary, case):
    broker=Broker(binary,case); peers=[]; decisions=[]
    try:
        broker.ready()
        for index,(receiver_kind,publisher_kind) in enumerate((('mqttium','paho'),('paho','mqttium'))):
            receiver,tap=await peer(receiver_kind,broker,case,f'matrix-{receiver_kind}')
            source,source_tap=await peer(publisher_kind,broker,case,f'matrix-source-{index}')
            peers.extend(((receiver,tap),(source,source_tap)))
            for granted in (0,1):
                topic=f'matrix/{index}/temperature'
                values=(Subscription('matrix/#',granted),Subscription('matrix/+/temperature',granted),
                        Subscription(topic,granted))
                await receiver.subscribe(values)
                for published in (0,1,2):
                    payload=f'matrix:{index}:{published}:{granted}'.encode()
                    await source.publish(topic,payload,published)
                    effective=min(published,granted)
                    observed=await receiver.receive(payload,effective)
                    await receiver.no_message()
                    wire=one_wire_delivery(tap,payload,effective)
                    decisions.append({'publisher':publisher_kind,'receiver':receiver_kind,
                                      'published_qos':published,'granted_qos':granted,
                                      'effective_qos':effective,'message':observed,'wire':wire,
                                      'matching_filters':3,'recipient_occurrences':1})
            await source.publish('neighbor/elsewhere',b'neighbor-must-not-match',1)
            await receiver.no_message()
            await receiver.unsubscribe(tuple(value.filter for value in values))
            await until_state(broker,lambda state:all(state['counts'][name]==0 for name in ('obligations','outbound','inbound','publications','payloads')),
                              'wildcard_matrix_responsibility_not_finished')
            await close_peers(((receiver,tap),(source,source_tap)));peers=[]
        state=durable(broker.db)
        require(all(state['counts'][name]==0 for name in ('obligations','outbound','inbound','publications','payloads')),
                'wildcard_matrix_responsibility_not_finished')
        write_json(case/'durable-final.json',state);broker.stop()
        return {'criterion':'matrix','status':'PASS','cells':decisions,'cells_per_direction':6,
                'neighbor_excluded':True,'final':state,'stop_status':broker.process.returncode}
    finally:
        await close_peers(peers);broker.cleanup()


async def no_local(binary, case):
    broker=Broker(binary,case);peers=[];decisions=[]
    try:
        broker.ready()
        for kind in ('mqttium','paho'):
            owner,tap=await peer(kind,broker,case,'nl-'+kind)
            external,external_tap=await peer('paho' if kind=='mqttium' else 'mqttium',broker,case,'nl-source-'+kind)
            peers.extend(((owner,tap),(external,external_tap)))
            topic='local/'+kind+'/value'
            # Excluding the high-QoS contribution must leave the low-QoS local
            # match. No Local is neither a global self-delivery switch nor a
            # filter applied after maximum QoS has already been selected.
            values=(Subscription('local/#',1,True),Subscription(topic,0,False))
            await owner.subscribe(values)
            for qos in (0,1,2):
                own=f'self:{kind}:{qos}'.encode()
                await owner.publish(topic,own,qos)
                message=await owner.receive(own,0);await owner.no_message()
                wire=one_wire_delivery(tap,own,0,code='no_local_applied_after_aggregation')
                decisions.append({'implementation':kind,'case':'eligible_local_contribution',
                                  'published_qos':qos,'effective_qos':0,'message':message,'wire':wire})
                other=f'external:{kind}:{qos}'.encode()
                await external.publish(topic,other,qos)
                effective=min(qos,1)
                message=await owner.receive(other,effective);await owner.no_message()
                wire=one_wire_delivery(tap,other,effective)
                decisions.append({'implementation':kind,'case':'different_source_session',
                                  'published_qos':qos,'effective_qos':effective,'message':message,'wire':wire})
            await owner.unsubscribe((topic,))
            await owner.publish(topic,b'all-self-contributions-excluded',2)
            await owner.no_message()
            require(not tap.publications(b'all-self-contributions-excluded'),'no_local_self_delivery_not_excluded')
            # Replacing one literal changes its options atomically and creates
            # no second row. RAP/RH are persisted but all test publications are
            # non-retained, so these options cannot change their delivery.
            await owner.subscribe((Subscription('local/#',1,False,True,2),))
            state=durable(broker.db);target=session_key(state,owner.client_id)
            rows=[row for row in state['tables']['routes'] if row['session']==target and row['topic']==b'local/#'.hex()]
            require(len(rows)==1 and rows[0]['options']==40 and rows[0]['qos']==1,
                    'wildcard_replacement_options_not_durable')
            changed=b'replacement-restores-self-delivery'
            await owner.publish(topic,changed,1)
            await owner.receive(changed,1);one_wire_delivery(tap,changed,1)
            await owner.unsubscribe(('local/#',));await owner.no_message()
            await external.publish(topic,b'unsubscribed-literal',1);await owner.no_message()
            decisions.append({'implementation':kind,'case':'literal_replacement_and_unsubscribe',
                              'durable_filter':rows[0]})
            await close_peers(((owner,tap),(external,external_tap)));peers=[]
        broker.stop()
        return {'criterion':'no-local','status':'PASS','cases':decisions,'stop_status':broker.process.returncode}
    finally:
        await close_peers(peers);broker.cleanup()


async def filter_edges(binary, case):
    broker=Broker(binary,case);subscriber=source=None;rows=[]
    try:
        broker.ready()
        subscriber=PubsubWire(broker.address,broker.observations,'filter-table-target')
        source=PubsubWire(broker.address,broker.observations,'filter-table-source')
        subscriber.connect('filter-table-target',session_present=False)
        source.connect('filter-table-source',session_present=False)
        cases=(('atelier/#','atelier',True),('atelier/#','atelier/',True),
               ('atelier/+','atelier/',True),('atelier/+','atelier',False),
               ('atelier/+/temperature','atelier//temperature',True),
               ('atelier/+','atelier/a/b',False),('#','$SYS/load',False),
               ('+/#','$SYS/load',False),('$SYS/#','$SYS/load',True),
               ('atelier/#','atelier/$private/value',True),('+/+','/',True),
               ('/+/temperature','/x/temperature',True),('é/+/温度','é//温度',True),
               ('é/#','é/value',False),('Atelier/#','atelier/x',False))
        for index,(filter_name,topic,expected) in enumerate(cases):
            subscriber.subscribe(index+1,[(filter_name,1)])
            payload=f'edge:{index}'.encode()
            source.publish(index+1,topic,payload,qos=2)
            if expected:
                message=subscriber.next_publication();require(message.topic==topic and message.payload==payload,'wildcard_match_table_mismatch')
                subscriber.acknowledge(message)
            else:subscriber.no_publication(.1,code='wildcard_match_table_mismatch')
            subscriber.unsubscribe(100+index,[filter_name])
            rows.append({'filter':filter_name,'topic':topic,'expected_match':expected,'observed_match':expected})
        for index,filter_name in enumerate(('', 'a#','a/+suffix','a/#/later','a/++')):
            invalid=PubsubWire(broker.address,broker.observations,f'invalid-filter-{index}')
            try:
                invalid.connect(f'invalid-filter-{index}',session_present=False)
                body=b'\x00\x01\x00'+len(filter_name.encode()).to_bytes(2,'big')+filter_name.encode()+b'\x01'
                invalid.send(packet(0x82,body));refused=invalid.wait(0xe0)
                require(refused.body and refused.body[0]==0x82,'invalid_filter_not_protocol_error')
                rows.append({'invalid_filter':filter_name,'disconnect_reason':0x82})
            finally:invalid.close()
        subscriber.send(subscribe_packet(211,(Subscription('$share/group/a',1),)))
        _,reasons=decode_filter_ack(subscriber.wait(0x90,211))
        require(reasons==(0x9e,), 'shared_filter_not_explicitly_refused')
        # SUBSCRIBE mixed results remain in request order.
        subscriber.send(subscribe_packet(212,(Subscription('valid/#',2),Subscription('$share/group/a',1),Subscription('valid/+',0))))
        _,reasons=decode_filter_ack(subscriber.wait(0x90,212))
        require(reasons==(1,0x9e,0),'wildcard_filter_results_reordered')
        subscriber.unsubscribe(213,['valid/#','valid/+'])
        subscriber.close();subscriber=None;source.close();source=None
        broker.stop()
        return {'criterion':'filter-edges','status':'PASS','cases':rows,'mixed_reasons':list(reasons),
                'shared_reason':0x9e,'stop_status':broker.process.returncode}
    finally:
        if subscriber is not None:subscriber.close()
        if source is not None:source.close()
        broker.cleanup()


async def until_state(broker,predicate,reason,timeout=10):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        state=durable(broker.db)
        if predicate(state):return state
        require(broker.process.poll() is None,'wildcard_broker_exited')
        await asyncio.sleep(.01)
    raise ReceiptFailure(reason)


async def offline_and_expiry(binary,case):
    broker=Broker(binary,case);restarted=None;peers=[]
    try:
        broker.ready()
        target,tap=await peer('mqttium',broker,case,'offline-wildcard-before',expiry=5,client_id='offline-wildcard')
        peers.append((target,tap))
        values=(Subscription('persist/+/#',1,True),Subscription('persist/room/#',0,False))
        await target.subscribe(values)
        before=durable(broker.db);old=session_key(before,target.client_id)
        routes=[row for row in before['tables']['routes'] if row['session']==old]
        await close_peers(peers);peers=[]
        source,source_tap=await peer('paho',broker,case,'offline-source')
        peers.append((source,source_tap))
        payload=b'wildcard-offline-survives-broker-restart'
        await source.publish('persist/room/value',payload,2)
        owed=await until_state(broker,lambda state:state['counts']['obligations']==1,'wildcard_offline_obligation_missing')
        obligation=owed['tables']['obligations'][0]
        require(obligation['target']==old and obligation['qos']==1,'wildcard_offline_wrong_aggregation')
        await close_peers(peers);peers=[]
        killed=broker.stop(killed=True);writer_available(broker.db)
        restarted=Broker(binary,case,db=broker.db,label='restart');restarted.ready()
        recovered=durable(restarted.db)
        require([row for row in recovered['tables']['routes'] if row['session']==old]==routes,
                'wildcard_restart_lost_filter_options')
        target,tap=await peer('mqttium',restarted,case,'offline-wildcard-resumed',expiry=1,present=True,client_id='offline-wildcard')
        peers.append((target,tap))
        require(session_key(durable(restarted.db),target.client_id)==old,'wildcard_resume_changed_session')
        received=await target.receive(payload,1);one_wire_delivery(tap,payload,1)
        own=b'no-local-after-restart-uses-session-not-connection'
        await target.publish('persist/other/value',own,2);await target.no_message()
        require(not tap.publications(own),'no_local_lost_on_session_resume')
        await close_peers(peers);peers=[]
        expired=await until_state(restarted,lambda state:not any(row['id']==old for row in state['tables']['sessions']),
                                  'wildcard_finite_expiry_did_not_end_session')
        require(not any(row['session']==old for row in expired['tables']['routes']),'expired_wildcard_filters_survived')
        fresh,tap=await peer('paho',restarted,case,'offline-wildcard-fresh',expiry=0,present=False,client_id='offline-wildcard')
        peers.append((fresh,tap));new=session_key(durable(restarted.db),fresh.client_id)
        require(new!=old,'wildcard_expiry_reused_session_identity')
        await close_peers(peers);peers=[]
        restarted.stop();writer_available(restarted.db)
        write_json(case/'before.json',before);write_json(case/'owed.json',owed);write_json(case/'recovered.json',recovered)
        return {'criterion':'offline-expiry','status':'PASS','old_session':old,'new_session':new,
                'filters':routes,'original_obligation':obligation,'received':received,
                'same_file':str(broker.db),'killed_pid':broker.process.pid,'kill_status':killed,
                'stop_status':restarted.process.returncode}
    finally:
        await close_peers(peers);broker.cleanup()
        if restarted is not None:restarted.cleanup()


def business_state(state):
    return {**{name:state['tables'][name] for name in ('publications','payloads','obligations','inbound','outbound')},
            'accounts':{name:state['tables']['meta'][0][name] for name in ('used_payload_bytes','used_references','used_exchange_slots')}}


async def fanout_capacity(binary,case):
    broker=Broker(binary,case);source=None;witnesses=[];peers=[]
    try:
        broker.ready()
        for index in range(17):
            wire=PubsubWire(broker.address,broker.observations,f'fanout-setup-{index}')
            try:
                wire.connect(f'fanout-target-{index}',session_present=False)
                # The existing per-literal16-destination limit is preserved.
                literal='fanout/#' if index<16 else 'fanout/+/value'
                wire.subscribe(1,[(literal,1)])
            finally:wire.close()
        source=PubsubWire(broker.address,broker.observations,'fanout-source')
        source.connect('fanout-source',session_present=False)
        for index in (0,16):
            wire=PubsubWire(broker.address,broker.observations,f'fanout-online-{index}')
            wire.connect(f'fanout-target-{index}',session_present=True);witnesses.append(wire)
        before=durable(broker.db);baseline=business_state(before);mark=max(int(event['seq']) for event in broker.events())
        topic='fanout/room/value'
        source.publish_qos0(topic,b'over-limit-qos0');source.send(b'\xc0\x00');source.wait(0xd0)
        for wire in witnesses:wire.no_publication(.15,code='wildcard_fanout_truncated')
        dropped=[event for event in broker.events() if int(event['seq'])>mark and event['kind']=='qos0_dropped']
        require(any(event['details']['reason']=='capacity_exceeded' for event in dropped),
                'wildcard_qos0_overflow_not_observed')
        source.close();source=None
        rejected=[]
        from tcp_probe import publish_packet
        for identifier,qos in ((11,1),(12,2)):
            source=PubsubWire(broker.address,broker.observations,f'fanout-overflow-{qos}')
            source.connect('fanout-source',session_present=True)
            mark=max(int(event['seq']) for event in broker.events())
            source.send(publish_packet(topic,f'over-limit-qos{qos}'.encode(),identifier,qos=qos))
            disconnect=None
            try:
                response=source.poll(2)
                require(response is not None,'wildcard_overflow_did_not_refuse')
                require(response.kind==0xe0 and response.body and response.body[0]>=128,
                        'wildcard_fanout_truncated')
                disconnect=response.body[0]
                try:
                    ended=source.poll(2)
                    raise ReceiptFailure('wildcard_overflow_did_not_close',str(ended))
                except ReceiptFailure as error:
                    require(error.code=='peer_closed','wildcard_overflow_did_not_close',error.code)
            except ReceiptFailure as error:
                require(error.code=='peer_closed','wildcard_overflow_did_not_refuse',error.code)
            closures=[event for event in broker.events() if int(event['seq'])>mark
                      and event['kind']=='connection_closed' and event['details']['code']=='capacity_exceeded']
            require(len(closures)==1,'wildcard_capacity_close_cause_missing')
            rejected.append({'qos':qos,'clean_eof':True,'disconnect_reason':disconnect,
                             'product_close':closures[0],'positive_publish_ack':False})
            source.close();source=None
        source=PubsubWire(broker.address,broker.observations,'fanout-source-accepted')
        source.connect('fanout-source',session_present=True)
        after=durable(broker.db)
        require(business_state(after)==baseline,'wildcard_fanout_truncated')
        for wire in witnesses:wire.no_publication(.1,code='wildcard_fanout_truncated')
        witnesses[1].unsubscribe(2,['fanout/+/value']);witnesses[1].close();witnesses.pop()
        accepted=b'exactly-sixteen-destinations'
        source.publish(13,topic,accepted,qos=1)
        admitted=durable(broker.db)
        obligations=admitted['tables']['obligations']
        require(len(obligations)==16 and len({row['target'] for row in obligations})==16,
                'wildcard_sixteen_targets_not_admitted_atomically')
        message=witnesses[0].next_publication();require(message.payload==accepted,'wildcard_fanout_payload_mismatch')
        witnesses[0].acknowledge(message)
        zero_target=session_key(durable(broker.db),'fanout-target-0')
        await until_state(broker,lambda state:not any(row['target']==zero_target for row in state['tables']['obligations']),
                          'wildcard_target_finish_not_committed')
        witnesses[0].close();witnesses=[]
        deliveries=[]
        for index in range(1,16):
            receiver,tap=await peer('mqttium' if index%2 else 'paho',broker,case,f'fanout-drain-{index}',
                                    client_id=f'fanout-target-{index}',present=True)
            peers.append((receiver,tap))
            deliveries.append(await receiver.receive(accepted,1));one_wire_delivery(tap,accepted,1)
            target_id=session_key(durable(broker.db),receiver.client_id)
            await until_state(broker,lambda state:not any(row['target']==target_id for row in state['tables']['obligations']),
                              'wildcard_target_finish_not_committed')
            await close_peers(peers);peers=[]
        await until_state(broker,lambda state:state['counts']['obligations']==0 and state['counts']['outbound']==0,
                          'wildcard_fanout_not_durably_finished')
        # Seventeen raw destination matches become sixteen eligible targets
        # only after the source's own No Local contribution is removed.
        source.send(subscribe_packet(90,(Subscription('fanout/+/value',1,True),)))
        _,reasons=decode_filter_ack(source.wait(0x90,90));require(reasons==(1,),'wildcard_no_local_setup_refused')
        source.publish(14,topic,b'no-local-excludes-seventeenth-target',qos=2)
        selected=durable(broker.db);source_id=session_key(selected,'fanout-source')
        require(len(selected['tables']['obligations'])==16
                and not any(row['target']==source_id for row in selected['tables']['obligations']),
                'no_local_applied_after_destination_budget')
        source.no_publication(.1,code='no_local_applied_after_destination_budget')
        source.close();source=None
        write_json(case/'before-overflow.json',before);write_json(case/'after-refusals.json',after)
        write_json(case/'sixteen-admitted.json',admitted);write_json(case/'no-local-seventeen-raw.json',selected)
        broker.stop()
        return {'criterion':'fanout','status':'PASS','raw_targets':17,'allowed_targets':16,
                'per_literal_limit':16,'qos0_dropped':dropped,'rejections':rejected,
                'sixteen_original_obligations':obligations,'received_and_acknowledged':16,
                'remaining_responsibilities_deliberately_offline':16,
                'no_local_eligible_targets':16,'stop_status':broker.process.returncode}
    finally:
        if source is not None:source.close()
        for wire in witnesses:wire.close()
        await close_peers(peers);broker.cleanup()


def percentile(values,fraction):
    import math
    values=sorted(values)
    return values[max(0,math.ceil(len(values)*fraction)-1)]


async def routing_measure(binary,case):
    from receipt_reception import storage_sample
    from itertools import product
    decisions=[]
    for mode,size in ((mode,size) for mode in ('exact','wildcard') for size in (1,16,64,128)):
        directory=case/mode/str(size);broker=Broker(binary,directory);target=source=None
        try:
            ready=broker.ready()
            target=PubsubWire(broker.address,broker.observations,'measure-target')
            target.connect('measure-target',session_present=False)
            levels=('a','b','c','d','e','f','g')
            filters=['measure/'+'/'.join(level if literal else '+' for level,literal in zip(levels,bits))
                     for bits in product((False,True),repeat=7)][:size]
            if mode=='exact':
                filters=['measure/'+'/'.join(levels)]+[f'measure/nonmatching-{index}' for index in range(size-1)]
            for index in range(0,len(filters),8):target.subscribe(index//8+1,[(value,1) for value in filters[index:index+8]])
            source=PubsubWire(broker.address,broker.observations,'measure-source')
            source.connect('measure-source',session_present=False)
            state=durable(broker.db);require(state['counts']['routes']==size,'routing_measure_filter_count')
            initial=storage_sample(broker);samples=[]
            for index in range(16):
                payload=f'measure:{size}:{index}'.encode();start=time.monotonic_ns()
                source.publish(index+1,'measure/'+'/'.join(levels),payload,qos=1)
                acknowledged=time.monotonic_ns()
                message=target.next_publication();require(message.payload==payload and message.qos==1,'routing_measure_delivery_mismatch')
                target.acknowledge(message)
                samples.append({'number':index,'publisher_ack_ns':str(acknowledged-start),
                                'recipient_ack_submitted_ns':str(time.monotonic_ns()-start)})
            await until_state(broker,lambda state:all(state['counts'][name]==0 for name in ('obligations','outbound','publications','payloads')),
                              'routing_measure_not_durably_finished')
            target.no_publication(.1,code='routing_measure_duplicate_delivery')
            final=storage_sample(broker)
            committed=lambda kind:{json.dumps(event['operation'],sort_keys=True) for event in broker.events()
                if event['kind']=='store_result' and event.get('command_kind')==kind
                and event['details'].get('committed_now') is True and event['details'].get('outcome')=='Committed'}
            require(len(committed('Admission'))==16 and len(committed('Finish'))==16,
                    'routing_measure_commit_or_finish_count')
            values=[int(sample['publisher_ack_ns'])/1e6 for sample in samples]
            decisions.append({'mode':mode,'route_count':size,'distinct_targets':1,'messages_received':16,
                              'durably_finished':16,'samples':samples,'publisher_ack_ms':{
                                  'min':min(values),'median':percentile(values,.5),'p95_nearest_rank':percentile(values,.95),'max':max(values)},
                              'initial':initial,'final':final,'ready':ready,'pid':broker.process.pid,
                              'scope':'Exploratory complete TCP/durable admission latency; not isolated matcher CPU time or a performance qualification.'})
            target.close();target=None;source.close();source=None;broker.stop()
        finally:
            if target is not None:target.close()
            if source is not None:source.close()
            broker.cleanup()
    write_json(case/'measurements.json',decisions)
    return {'criterion':'routing-measure','status':'PASS','sizes':[1,16,64,128],
            'modes':['exact','wildcard'],'samples_per_size':16,'same_binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),
            'measurements':decisions,'qualification':'exploratory-system-measurement'}


async def pressure(binary,case):
    broker=Broker(binary,case)
    source=slow=independent=None
    try:
        ready=broker.ready()
        require(ready['details']['max_targets']==16 and ready['details']['max_subscriptions']==128
                and ready['details']['output_slots']==4 and ready['details']['write_ms']==250,
                'wildcard_pressure_changed_limits')
        slow=PubsubWire(broker.address,broker.observations,'wildcard-slow',receive_buffer=1024)
        slow.connect('wildcard-slow',session_present=False)
        slow.subscribe(1,[('pressure/slow/#',0),('pressure/slow/value',0)],expected=(0,0))
        independent=PubsubWire(broker.address,broker.observations,'wildcard-independent')
        independent.connect('wildcard-independent',session_present=False)
        independent.subscribe(1,[('pressure/control/+',0)],expected=(0,))
        source=PubsubWire(broker.address,broker.observations,'wildcard-pressure-source')
        source.connect('wildcard-pressure-source',session_present=False)
        mark=max(int(event['seq']) for event in broker.events());sent=0
        for index in range(64):
            source.publish_qos0('pressure/slow/value',f'{index:08}'.encode()+b's'*(32768-8));sent+=1
            if any(event['kind']=='qos0_dropped' and event['details']['reason'] in ('output_saturated','write_failed')
                   for event in broker.events() if int(event['seq'])>mark):break
        end=time.monotonic()+3;observed=[]
        while time.monotonic()<end:
            observed=[event for event in broker.events() if int(event['seq'])>mark and event['kind']=='qos0_dropped'
                      and event['details']['reason'] in ('output_saturated','write_failed')]
            if observed:break
            await asyncio.sleep(.01)
        require(bool(observed),'wildcard_output_pressure_not_reached')
        payload=b'independent-number:00000001'
        source.publish_qos0('pressure/control/value',payload)
        message=independent.next_publication()
        require(message.payload==payload and message.qos==0,'wildcard_pressure_blocked_independent_destination')
        independent.send(b'\xc0\x00');require(independent.wait(0xd0).body==b'','wildcard_pressure_blocked_ping')
        source.send(b'\xc0\x00');require(source.wait(0xd0).body==b'','wildcard_pressure_blocked_source_control')
        state=durable(broker.db)
        require(all(state['counts'][name]==0 for name in ('obligations','outbound','publications','payloads')),
                'wildcard_volatile_pressure_created_durable_work')
        for wire in (source,slow,independent):wire.close()
        source=slow=independent=None;broker.stop()
        return {'criterion':'pressure','status':'PASS','publications_bounded_at':sent,
                'payload_bytes':32768,'slow_receive_buffer':1024,'actual_pressure':observed,
                'independent_message':message.observation(),'independent_ping':True,'profile':ready['details'],
                'stop_status':broker.process.returncode}
    finally:
        for wire in (source,slow,independent):
            if wire is not None:wire.close()
        broker.cleanup()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--binary-source-head',required=True)
    parser.add_argument('--case',choices=('normal','demo','matrix','no-local','filter-edges','offline-expiry','fanout','routing-measure','pressure'),default='normal')
    args=parser.parse_args();args.out.mkdir(parents=True,exist_ok=False)
    source=Path(__file__).resolve().parents[3]
    head=subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD']).decode().strip()
    receipt={'format':'mqttiumd.wildcard-system-reception','schema_version':1,
             'status':'FAIL','qualification':'system-test-only','source_head':args.binary_source_head,
             'harness_source_head':head,'client_identity':client_identity(),'mosquitto':mosquitto_identity(),
             'binary':{'path':str(args.binary.resolve()),'sha256':hashlib.sha256(args.binary.read_bytes()).hexdigest()},'cases':[]}
    try:
        require(head==args.binary_source_head,'wildcard_source_mismatch')
        selected=('demo','matrix','no-local','filter-edges','offline-expiry','fanout','routing-measure','pressure') if args.case=='normal' else (args.case,)
        functions={'demo':demo,'matrix':matrix,'no-local':no_local,'filter-edges':filter_edges,'offline-expiry':offline_and_expiry,'fanout':fanout_capacity,'routing-measure':routing_measure,'pressure':pressure}
        for name in selected:
            receipt['cases'].append(asyncio.run(functions[name](args.binary.resolve(),args.out/name)))
        receipt['status']='PASS'
    except Exception as error:
        receipt['failure']={'code':getattr(error,'code',type(error).__name__),'detail':str(error)[:512]}
    finally:write_json(args.out/'receipt.json',receipt)
    print(json.dumps({'status':receipt['status'],'failure':receipt.get('failure'),'receipt':str(args.out/'receipt.json')}))
    return 0 if receipt['status']=='PASS' else 1


if __name__=='__main__':raise SystemExit(main())
