"""Same-source and A/B diagnostics. Fixed-work broker cells are not paced RTT."""
from __future__ import annotations
import argparse
import asyncio
from collections import deque
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import time

CELLS = [('callback', n, False) for n in (1, 2, 3, 8, 32)] + [
    ('callback', 8, True), ('iterator', 8, False), ('both', 8, False)]
RUNTIME = ['src/mqttium/api/'+p for p in ('_delivery.py', '_effects.py', 'async_client.py')]


def percentiles(values):
    if not values:
        return {'p50_us': None, 'p95_us': None}
    values = sorted(values)
    def q(p):
        x = p * (len(values)-1)
        i = int(x)
        return (values[i]+(values[min(i+1,len(values)-1)]-values[i])*(x-i))/1000
    return {'p50_us': q(.5), 'p95_us': q(.95)}


def hashes(root):
    return {p:hashlib.sha256((Path(root)/p).read_bytes()).hexdigest() for p in RUNTIME}


async def cell(kind, mode, count, asynchronous, batches, warmup, port):
    from mqttium.api import AsyncClient
    from mqttium.protocol.effects import EngineEffect, EffectKind
    from mqttium.types import Message
    first, tail, completion = [], [], []
    errors = []
    start = 0
    collecting = False
    seen = 0
    ordinal = 0
    done = None
    done_iter = None
    sub = AsyncClient(message_delivery='callback' if mode in ('publish','none') else mode,
                      max_pending_callbacks=64, max_pending_messages=64, keepalive=0)
    pub = None
    iterator_task = None
    topic = 'measure/' + str(os.getpid()) + '/' + mode
    payloads = [i.to_bytes(8,'big')+b'x'*56 for i in range(count)]

    def callback(message):
        nonlocal seen, ordinal
        assert message.payload == payloads[ordinal], (ordinal, message.payload)
        seen += 1
        ordinal += 1
        if collecting:
            (first if ordinal==1 else tail).append(time.perf_counter_ns()-start)
        if done is not None and ordinal==count:
            done.set_result(None)

    async def async_callback(message):
        await asyncio.sleep(0)
        callback(message)

    if mode not in ('iterator','none','publish'):
        sub.on_message = async_callback if asynchronous else callback
    sub._delivery.report_callback_error = lambda cb, exc: errors.append(repr(exc))
    effects = [EngineEffect(EffectKind.MESSAGE, Message(topic=topic,payload=p),
                            requires_delivery_mark=False) for p in payloads]
    try:
        if kind == 'network':
            pub = AsyncClient(message_delivery='callback',keepalive=0)
            await pub.connect('127.0.0.1',port,timeout=5)
            if mode != 'publish':
                await sub.connect('127.0.0.1',port,timeout=5)
                await sub.subscribe(topic,qos=1)
            if mode in ('iterator','both'):
                async def consume():
                    i = 0
                    async for message in sub.messages():
                        assert message.payload == payloads[i]
                        i += 1
                        if mode == 'iterator':
                            callback(message)
                        if i == count:
                            i = 0
                            done_iter.set_result(None)
                iterator_task = asyncio.create_task(consume())

        async def one():
            nonlocal start, ordinal, done, done_iter
            ordinal = 0
            if kind == 'network':
                done = asyncio.get_running_loop().create_future()
                done_iter = asyncio.get_running_loop().create_future()
            start = time.perf_counter_ns()
            if kind == 'micro':
                applied = sub._apply_message_effect_batch_inline(deque(effects),sub._connection_epoch)
                if mode == 'none':
                    assert applied == 0
                    for effect in effects:
                        await sub._apply_effect(effect,nowait=False,epoch=sub._connection_epoch)
                else:
                    assert applied == count
                await sub._callback_queue.join()
                if mode in ('iterator','both'):
                    for _ in range(count):
                        sub._messages.get_nowait()
            else:
                receipts=[pub.publish_nowait(topic,p,qos=1) for p in payloads]
                if mode != 'publish':
                    await done
                if collecting and mode != 'publish':
                    completion.append(time.perf_counter_ns()-start)
                for receipt in receipts:
                    await receipt.wait()
                if collecting and mode == 'publish':
                    completion.append(time.perf_counter_ns()-start)
                if mode in ('both','iterator'):
                    await done_iter
                await sub._callback_queue.join()
            if collecting and kind == 'micro':
                completion.append(time.perf_counter_ns()-start)

        for _ in range(warmup):
            await one()
        collecting=True
        cpu=time.process_time_ns(); wall=time.perf_counter_ns()
        for _ in range(batches):
            await one()
        wall=time.perf_counter_ns()-wall; cpu=time.process_time_ns()-cpu
        expected = 0 if mode in ('none','publish') or (mode=='iterator' and kind=='micro') else (batches+warmup)*count
        assert seen==expected, (seen,expected)
        assert not errors,errors
        assert sub.stats().delivery.callback_queued==0
        assert sub._delivery._callback_batch_reserved==0
        if pub:
            assert pub.stats().receipts.publish==0
        result={'cell':f"{mode}-{'async' if asynchronous else 'sync'}-{count}",
                'kind':kind,'batch_size':count,'throughput_msg_s':batches*count/(wall/1e9),
                'cpu_ns_per_msg':cpu/(batches*count),'first':percentiles(first),
                'tail':percentiles(tail),'completion':percentiles(completion),
                'callbacks_verified':seen,'measured_messages':batches*count,
                'wall_ns':wall,'stats':dataclasses.asdict(sub.stats())}
        return result
    finally:
        if iterator_task:
            iterator_task.cancel()
            await asyncio.gather(iterator_task,return_exceptions=True)
        if pub:
            await pub.disconnect()
        await sub.disconnect()
        await sub._shutdown_callback_worker(drain=False)
        await sub._callback_queue.join()


def worker(args):
    root=Path(args.worker).resolve()
    sys.path.insert(0,str(root/'src'))
    if hasattr(os,'sched_setaffinity'):
        os.sched_setaffinity(0,{args.cpu})
    cells=list(CELLS)+[('publish' if args.kind=='network' else 'none',8,False)]
    random.Random(20260910+args.seed).shuffle(cells)
    async def run():
        errors=[]
        asyncio.get_running_loop().set_exception_handler(lambda loop, ctx:errors.append(str(ctx)))
        rows=[]
        for item in cells:
            rows.append(await asyncio.wait_for(cell(args.kind,*item,args.batches,args.warmup,args.port),45))
        await asyncio.sleep(0)
        assert not errors,errors
        assert len(asyncio.all_tasks())==1, [t.get_name() for t in asyncio.all_tasks()]
        return rows
    print(json.dumps({'root':str(root),'runtime_sha256':hashes(root),'python':sys.version,
                      'cells':asyncio.run(run())},default=str))


def controller(args):
    out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
    for phase in ('AA','AB'):
        for pair in range(args.pairs):
            for label in (('A','B') if pair%2==0 else ('B','A')):
                root=args.base if phase=='AA' or label=='A' else args.candidate
                cmd=[sys.executable,__file__,'--worker',root,'--kind',args.kind,
                     '--cpu',str(args.cpu),'--seed',str(pair),'--batches',str(args.batches),
                     '--warmup',str(args.warmup),'--port',str(args.port)]
                run=subprocess.run(cmd,capture_output=True,text=True,timeout=240)
                prefix=out/f'{args.kind}-{phase}-{pair:02}-{label}'
                prefix.with_suffix('.stderr').write_text(run.stderr)
                prefix.with_suffix('.stdout').write_text(run.stdout)
                run.check_returncode()
                data=json.loads(run.stdout)
                assert data['runtime_sha256']==hashes(root)
                data.update(phase=phase,pair=pair,label=label)
                prefix.with_suffix('.json').write_text(json.dumps(data,indent=2)+'\n')
            print(f'{args.kind} {phase} pair {pair+1}/{args.pairs} complete',flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--worker');p.add_argument('--base');p.add_argument('--candidate');p.add_argument('--output')
    p.add_argument('--kind',choices=['micro','network'],default='micro')
    p.add_argument('--port',type=int,default=1883);p.add_argument('--cpu',type=int,default=0)
    p.add_argument('--seed',type=int,default=0);p.add_argument('--pairs',type=int,default=12)
    p.add_argument('--batches',type=int,default=4000);p.add_argument('--warmup',type=int,default=200)
    args=p.parse_args()
    if args.worker:worker(args)
    else:controller(args)
