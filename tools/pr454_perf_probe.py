"""Neutral PR454 same-host A/A -> A/B diagnostic. No candidate monkeypatching.

micro: real ApplicationDelivery, completion per fixed-size batch, not network throughput.
network: real Mosquitto, native QoS1 request/response, closed-loop fixed concurrency;
         not a matched-rate latency benchmark. Sequence IDs never use MQTT MID.
All reported latencies are instrumented. Every sample runs in a fresh subprocess.
"""
from __future__ import annotations
import argparse, asyncio, hashlib, json, os, platform, random, resource, statistics, struct, subprocess, sys, time
from collections import deque
from pathlib import Path

BASE='9ad1f01857306ac5079ffb1d073a59fdb60e1931'
HEAD='a27a5f7942c86d24495f375ced1c72f440e6982b'
ROUTES=['direct_sync','exact_sync','wildcard_sync','fallback_sync','exact_async','direct_async']

def quantile(x,p):
    s=sorted(x); k=(len(s)-1)*p; a=int(k); b=min(a+1,len(s)-1)
    return s[a]+(s[b]-s[a])*(k-a)

def treehash(path):
    def obj(kind,data): return hashlib.sha1(kind+b' '+str(len(data)).encode()+b'\0'+data).digest()
    entries=[]
    for p in path.iterdir():
        if p.name=='__pycache__' or p.suffix=='.pyc': continue
        isdir=p.is_dir(); data=treehash(p) if isdir else obj(b'blob',p.read_bytes())
        entries.append((p.name.encode()+(b'/' if isdir else b''), (b'40000' if isdir else b'100644')+b' '+p.name.encode()+b'\0'+data))
    return obj(b'tree',b''.join(v for k,v in sorted(entries)))

def configure(client,route,callback,topic):
    async def acb(m): callback(m)
    if route=='direct_sync': client.on_message=callback
    elif route=='direct_async': client.on_message=acb
    elif route=='exact_sync': client.message_callback_add(topic,callback)
    elif route=='wildcard_sync': client.message_callback_add(topic.rsplit('/',1)[0]+'/+',callback)
    elif route=='fallback_sync':
        client.on_message=callback; client.message_callback_add('never/match',lambda m: None)
    elif route=='exact_async': client.message_callback_add(topic,acb)
    else: raise ValueError(route)

async def micro_case(route,burst,n):
    from mqttium.api import AsyncClient
    from mqttium.types import Message
    from mqttium.protocol.effects import EngineEffect,EffectKind
    client=AsyncClient(message_delivery='callback',max_pending_callbacks=1024)
    count=0; begin=0; latency=[]; record=False; errors=[]
    loop=asyncio.get_running_loop(); previous=loop.get_exception_handler()
    loop.set_exception_handler(lambda l,c: errors.append(str(c)))
    def callback(m):
        nonlocal count
        count+=1
        if record: latency.append(time.perf_counter_ns()-begin)
    configure(client,route,callback,'bench/request')
    effects=deque(EngineEffect(EffectKind.MESSAGE,Message(topic='bench/request',payload=b'x'*64),requires_delivery_mark=False) for _ in range(burst))
    try:
        for _ in range(500):
            assert client._apply_message_effect_batch_inline(effects,client._connection_epoch)==burst
            await client._callback_queue.join()
        assert count==500*burst
        worker_created=int(client._callback_worker_task is not None)
        count=0; record=True; cpu0=time.process_time_ns(); t0=time.perf_counter_ns()
        for _ in range(n):
            begin=time.perf_counter_ns()
            assert client._apply_message_effect_batch_inline(effects,client._connection_epoch)==burst
            await client._callback_queue.join()
        elapsed=time.perf_counter_ns()-t0; cpu=time.process_time_ns()-cpu0; record=False
        assert count==n*burst and len(latency)==count and not errors,(count,errors)
        assert client.stats().delivery.callback_queued==0
        return dict(kind='micro',route=route,burst=burst,protocol=0,count=count,throughput=count*1e9/elapsed,cpu_ns_per_msg=cpu/count,p50_ns=quantile(latency,.5),p95_ns=quantile(latency,.95),p99_ns=quantile(latency,.99),worker_created=worker_created)
    finally:
        await client._shutdown_callback_worker(drain=False);loop.set_exception_handler(previous)

async def network_case(route,burst,protocol,n,port):
    from mqttium.api import AsyncClient
    from mqttium.enums import MQTTProtocolVersion,ConnectionState
    p=MQTTProtocolVersion(protocol)
    prefix=f'pr454/{os.getpid()}/{route}/{protocol}/{burst}'
    req,rep=prefix+'/req',prefix+'/rep'
    rx=AsyncClient(client_id=f'r{os.getpid()}',protocol=p,message_delivery='callback')
    tx=AsyncClient(client_id=f't{os.getpid()}',protocol=p,message_delivery='callback')
    event=asyncio.Event();expected=set();received=set();latency=[];errors=[];record=False;response_count=0
    loop=asyncio.get_running_loop();previous=loop.get_exception_handler()
    loop.set_exception_handler(lambda l,c: errors.append(str(c)))
    def echo(m):
        nonlocal response_count
        response_count+=1
        rx.publish_nowait(rep,m.payload,qos=1)
    def response(m):
        seq,ts=struct.unpack('!QQ',m.payload[:16])
        if seq not in expected or seq in received: errors.append(f'bad sequence {seq}')
        received.add(seq)
        if record: latency.append(time.perf_counter_ns()-ts)
        if received==expected: event.set()
    configure(rx,route,echo,req);tx.on_message=response
    receipts=[]
    try:
        await rx.connect('127.0.0.1',port);await tx.connect('127.0.0.1',port)
        await rx.subscribe(req,qos=1);await tx.subscribe(rep,qos=1)
        seq=0
        async def cycle():
            nonlocal seq,expected,received,receipts
            expected=set(range(seq,seq+burst));received=set();event.clear();receipts=[]
            for _ in range(burst):
                payload=struct.pack('!QQ',seq,time.perf_counter_ns())+b'x'*48
                receipts.append(tx.publish_nowait(req,payload,qos=1));seq+=1
            await event.wait()
        for _ in range(100): await cycle()
        record=True;latency.clear();response_count=0
        cpu0=time.process_time_ns();t0=time.perf_counter_ns()
        for _ in range(n): await cycle()
        elapsed=time.perf_counter_ns()-t0;cpu=time.process_time_ns()-cpu0;record=False
        assert len(latency)==n*burst and response_count==n*burst and not errors,(len(latency),response_count,errors)
        assert rx.state is ConnectionState.CONNECTED and tx.state is ConnectionState.CONNECTED
        worker_created=int(rx._callback_worker_task is not None)
        # Deliberately do not use ACK completion to pace this application RTT workload.
        return dict(kind='network',route=route,burst=burst,protocol=protocol,count=len(latency),throughput=len(latency)*1e9/elapsed,cpu_ns_per_msg=cpu/len(latency),p50_ns=quantile(latency,.5),p95_ns=quantile(latency,.95),p99_ns=quantile(latency,.99),worker_created=worker_created)
    finally:
        await tx.disconnect();await rx.disconnect();loop.set_exception_handler(previous)

async def worker(args):
    import mqttium
    root=Path(mqttium.__file__).parent
    digest=treehash(root).hex()
    expected={'A':'d4685d3e24167a5c71306acdf268629bc95f52c4','B':'0f55ffb86a2565af012e751e45155d36096f6dca'}[args.arm]
    assert digest==expected,(digest,expected)
    if hasattr(os,'sched_setaffinity'): os.sched_setaffinity(0,{args.cpu})
    if args.kind=='micro': cells=[(r,b,0) for r in ROUTES for b in (1,2,8)]
    else:
        cells=[(r,b,4) for r in ROUTES for b in (1,2,8)]
        cells += [(r,2,5) for r in ('direct_sync','exact_sync','wildcard_sync','fallback_sync','exact_async')]
    random.Random(args.seed).shuffle(cells)
    out=[]
    for r,b,p in cells:
        if args.kind=='micro': val=await asyncio.wait_for(micro_case(r,b,args.count),30)
        else: val=await asyncio.wait_for(network_case(r,b,p,args.count,args.port),30)
        out.append(val)
    return {'arm':args.arm,'source_tree':digest,'source':str(root),'cells':out,'python':sys.version,'platform':platform.platform(),'cpu':args.cpu,'usage':list(resource.getrusage(resource.RUSAGE_SELF))}

def campaign(args):
    dest=Path(args.output);dest.mkdir(parents=True,exist_ok=True)
    samples=[]
    paths={'A':str(Path(args.base).resolve()/'src'),'B':str(Path(args.candidate).resolve()/'src')}
    meta={'base':BASE,'head':HEAD,'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'platform':platform.platform(),'python':sys.version,'cpu':args.cpu,'paths':paths,'kind':args.kind,'count':args.count,'repeats':args.repeats,'fixed_concurrency':True,'same_host':True}
    (dest/'metadata.json').write_text(json.dumps(meta,indent=2))
    for phase in ('AA','AB'):
        for block in range(args.repeats):
            order=[0,1] if block%2==0 else [1,0]
            for slot in order:
                arm='B' if phase=='AB' and slot==1 else 'A'
                env=dict(os.environ,PYTHONPATH=paths[arm],PYTHONHASHSEED='0',PYTHONDONTWRITEBYTECODE='1')
                cmd=[sys.executable,str(Path(__file__).resolve()),'worker','--arm',arm,'--kind',args.kind,'--count',str(args.count),'--cpu',str(args.cpu),'--port',str(args.port),'--seed',str(block)]
                run=subprocess.run(cmd,env=env,capture_output=True,text=True,timeout=300)
                if run.returncode:
                    (dest/f'{phase}-{block}-{slot}-error.txt').write_text(run.stdout+run.stderr)
                    raise RuntimeError(run.stderr[-4000:])
                sample=json.loads(run.stdout);sample.update(phase=phase,block=block,slot=slot,position=order.index(slot))
                samples.append(sample);(dest/f'{phase}-{block}-{slot}.json').write_text(json.dumps(sample))
                print(phase,block,slot,arm,flush=True)
    (dest/'raw.json').write_text(json.dumps(samples,indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['worker','campaign']);p.add_argument('--kind',choices=['micro','network'],default='micro');p.add_argument('--arm',choices=['A','B'],default='A');p.add_argument('--count',type=int,default=8000);p.add_argument('--cpu',type=int,default=0);p.add_argument('--port',type=int,default=11883);p.add_argument('--seed',type=int,default=0);p.add_argument('--base');p.add_argument('--candidate');p.add_argument('--output');p.add_argument('--repeats',type=int,default=8);a=p.parse_args()
    if a.mode=='worker': print(json.dumps(asyncio.run(worker(a))))
    else: campaign(a)
