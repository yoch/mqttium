"""Disposable decision-grade supplements for scheduler PRs #285/#286."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    pos = q * (len(xs) - 1)
    lo = int(pos); hi = min(lo + 1, len(xs) - 1); f = pos - lo
    return xs[lo] * (1.0 - f) + xs[hi] * f


def jain(values: list[float]) -> float:
    s = sum(values); ss = sum(v*v for v in values)
    return (s*s)/(len(values)*ss) if values and ss else 1.0


def run_worker(root: Path, argv: list[str], timeout: float) -> dict[str, Any]:
    env = os.environ.copy(); env["PYTHONPATH"] = str(root.resolve()/"src")
    cp = subprocess.run([sys.executable, str(Path(__file__).resolve()), *argv], env=env,
                        capture_output=True, text=True, timeout=timeout, check=False)
    if cp.returncode:
        raise RuntimeError((cp.stderr or cp.stdout)[-4000:])
    lines = [x for x in cp.stdout.splitlines() if x.strip()]
    return json.loads(lines[-1])


def med_ratio(pairs: list[dict[str, Any]], key: str) -> float:
    return statistics.median(p["candidate"][key]/p["base"][key] for p in pairs if p["base"][key] > 0)


async def mixed_worker(a: argparse.Namespace) -> dict[str, Any]:
    from mqttium.api import AsyncClient, PublishMessage
    from mqttium.protocol.reconnect import ReconnectPolicy
    if a.cpu is not None:
        os.sched_setaffinity(0, {a.cpu})
    c = AsyncClient(client_id=f"mixed-{os.getpid()}-{time.time_ns()}", max_outbound_inflight=a.inflight,
                    max_pending_outbound_messages=max(a.inflight*4, a.producers*4, a.batch_size*2),
                    max_pending_outbound_bytes=64<<20, reconnect=ReconnectPolicy(enabled=False))
    await c.connect(a.host, a.port, timeout=a.timeout)
    payload = b"x"*64
    per: list[list[float]] = [[] for _ in range(a.producers)]
    finish = [0.0]*a.producers
    start = time.perf_counter(); cpu0 = time.process_time()

    async def producer(i: int) -> None:
        out = per[i]
        if i % 2 == 0:
            for _ in range(a.ops):
                t = time.perf_counter(); r = await c.publish(f"bench/mixed/s/{i}", payload, qos=1); await r.wait()
                out.append((time.perf_counter()-t)*1000)
        else:
            msgs = [PublishMessage(topic=f"bench/mixed/b/{i}", payload=payload, qos=1) for _ in range(a.batch_size)]
            for _ in range(a.ops):
                t = time.perf_counter(); r = await c.publish_many(msgs, chunk_size=a.batch_size); await r.wait()
                out.append((time.perf_counter()-t)*1000)
        finish[i] = time.perf_counter()-start

    await asyncio.wait_for(asyncio.gather(*(producer(i) for i in range(a.producers))), timeout=a.timeout)
    elapsed = time.perf_counter()-start; cpu = time.process_time()-cpu0
    singles = [x for i,p in enumerate(per) if i%2==0 for x in p]
    batches = [x for i,p in enumerate(per) if i%2 for x in p]
    means = [statistics.fmean(p) for p in per if p]
    result = {
        "rate_ops": (a.producers*a.ops)/max(elapsed,1e-9), "cpu_seconds": cpu,
        "single_p99_ms": pct(singles,.99), "single_p999_ms": pct(singles,.999), "single_max_ms": max(singles,default=0),
        "batch_p99_ms": pct(batches,.99), "batch_p999_ms": pct(batches,.999), "batch_max_ms": max(batches,default=0),
        "finish_p99_s": pct(finish,.99), "finish_max_s": max(finish,default=0),
        "producer_mean_jain": jain([1/max(x,1e-9) for x in means]),
    }
    await c.disconnect(); return result


def mixed_parent(a: argparse.Namespace) -> dict[str, Any]:
    roots={"base":a.base_root,"candidate":a.candidate_root}; rows=[]
    for producers in a.producer_values:
        pairs=[]
        for i in range(a.repeat):
            order=("base","candidate") if i%2==0 else ("candidate","base"); m={}
            for v in order:
                m[v]=run_worker(roots[v],["mixed-worker","--host",a.host,"--port",str(a.port),"--producers",str(producers),
                    "--inflight",str(a.inflight),"--batch-size",str(a.batch_size),"--ops",str(a.ops),"--timeout",str(a.timeout),"--cpu",str(a.cpu)],a.timeout+30)
            pairs.append({"order":list(order),**m})
        row={"producers":producers,"pairs":pairs}
        for k in ("rate_ops","cpu_seconds","single_p99_ms","single_p999_ms","single_max_ms","batch_p99_ms","batch_p999_ms","batch_max_ms","finish_max_s"):
            row[k+"_ratio"]=med_ratio(pairs,k)
        rows.append(row); print(f"mixed p={producers} rate={row['rate_ops_ratio']:.4f} single-p99={row['single_p99_ms_ratio']:.4f} batch-p99={row['batch_p99_ms_ratio']:.4f}",flush=True)
    return {"mode":"mixed_publish_batch","repeat":a.repeat,"rows":rows}


async def quantum_worker(a: argparse.Namespace) -> dict[str, Any]:
    if a.cpu is not None: os.sched_setaffinity(0,{a.cpu})
    import mqttium.api._writer as wm
    if a.quantum:
        if not hasattr(wm,"_WRITER_BATCH_MAX_BYTES"): raise RuntimeError("candidate has no byte quantum")
        wm._WRITER_BATCH_MAX_BYTES=a.quantum
    from mqttium.api import AsyncClient
    from mqttium.protocol.reconnect import ReconnectPolicy
    c=AsyncClient(client_id=f"quant-{os.getpid()}-{time.time_ns()}",max_outbound_bytes=1<<20,max_outbound_messages=4096,
                  max_outbound_inflight=20,reconnect=ReconnectPolicy(enabled=False))
    await c.connect(a.host,a.port,timeout=a.timeout)
    flood=b"f"*a.flood_bytes; probe=b"p"*64; lats=[]; start=time.perf_counter(); cpu0=time.process_time()
    async def f():
        for _ in range(a.flood_count): await c.publish("bench/q/f",flood,qos=0)
    async def p():
        await asyncio.sleep(0)
        for _ in range(a.probes):
            t=time.perf_counter(); r=await c.publish("bench/q/p",probe,qos=1); await r.wait(); lats.append((time.perf_counter()-t)*1000)
    await asyncio.wait_for(asyncio.gather(f(),p()),timeout=a.timeout); await asyncio.wait_for(c._write_pump.join(),timeout=a.timeout)
    elapsed=time.perf_counter()-start; cpu=time.process_time()-cpu0; st=c._write_pump.stats(); await c.disconnect()
    return {"flood_rate":a.flood_count/max(elapsed,1e-9),"cpu_seconds":cpu,"probe_p50_ms":pct(lats,.5),"probe_p95_ms":pct(lats,.95),
            "probe_p99_ms":pct(lats,.99),"probe_p999_ms":pct(lats,.999),"probe_max_ms":max(lats,default=0),"batches":st.batches,
            "avg_batch_items":st.batched_items/max(st.batches,1),"avg_batch_bytes":st.batched_bytes/max(st.batches,1)}


def quantum_parent(a: argparse.Namespace) -> dict[str, Any]:
    rows=[]
    for q in a.quantum_values:
        pairs=[]
        for i in range(a.repeat):
            order=("base","candidate") if i%2==0 else ("candidate","base"); m={}
            for v in order:
                root=a.base_root if v=="base" else a.candidate_root; qv=0 if v=="base" else q
                m[v]=run_worker(root,["quantum-worker","--host",a.host,"--port",str(a.port),"--quantum",str(qv),"--flood-count",str(a.flood_count),
                    "--flood-bytes",str(a.flood_bytes),"--probes",str(a.probes),"--timeout",str(a.timeout),"--cpu",str(a.cpu)],a.timeout+30)
            pairs.append({"order":list(order),**m})
        row={"quantum":q,"pairs":pairs}
        for k in ("flood_rate","cpu_seconds","probe_p50_ms","probe_p95_ms","probe_p99_ms","probe_p999_ms","probe_max_ms","batches"):
            row[k+"_ratio"]=med_ratio(pairs,k)
        rows.append(row); print(f"quantum={q} flood={row['flood_rate_ratio']:.4f} p99={row['probe_p99_ms_ratio']:.4f} p999={row['probe_p999_ms_ratio']:.4f}",flush=True)
    return {"mode":"quantum_frontier","repeat":a.repeat,"rows":rows}


def csv(s:str)->list[int]: return [int(x) for x in s.split(',') if x]

def main():
    p=argparse.ArgumentParser(); sub=p.add_subparsers(dest='mode',required=True)
    mw=sub.add_parser('mixed-worker'); mw.add_argument('--host',default='127.0.0.1'); mw.add_argument('--port',type=int,default=11883); mw.add_argument('--producers',type=int,required=True); mw.add_argument('--inflight',type=int,default=4); mw.add_argument('--batch-size',type=int,default=8); mw.add_argument('--ops',type=int,default=200); mw.add_argument('--timeout',type=float,default=120); mw.add_argument('--cpu',type=int,default=1)
    mp=sub.add_parser('mixed-parent'); mp.add_argument('--base-root',type=Path,required=True); mp.add_argument('--candidate-root',type=Path,required=True); mp.add_argument('--host',default='127.0.0.1'); mp.add_argument('--port',type=int,default=11883); mp.add_argument('--producer-values',type=csv,default=csv('4,16,64')); mp.add_argument('--inflight',type=int,default=4); mp.add_argument('--batch-size',type=int,default=8); mp.add_argument('--ops',type=int,default=200); mp.add_argument('--repeat',type=int,default=8); mp.add_argument('--timeout',type=float,default=120); mp.add_argument('--cpu',type=int,default=1); mp.add_argument('--output',type=Path,required=True)
    qw=sub.add_parser('quantum-worker'); qw.add_argument('--host',default='127.0.0.1'); qw.add_argument('--port',type=int,default=11883); qw.add_argument('--quantum',type=int,default=0); qw.add_argument('--flood-count',type=int,default=1000); qw.add_argument('--flood-bytes',type=int,default=32768); qw.add_argument('--probes',type=int,default=100); qw.add_argument('--timeout',type=float,default=120); qw.add_argument('--cpu',type=int,default=1)
    qp=sub.add_parser('quantum-parent'); qp.add_argument('--base-root',type=Path,required=True); qp.add_argument('--candidate-root',type=Path,required=True); qp.add_argument('--host',default='127.0.0.1'); qp.add_argument('--port',type=int,default=11883); qp.add_argument('--quantum-values',type=csv,default=csv('32768,65536,131072,262144')); qp.add_argument('--flood-count',type=int,default=1000); qp.add_argument('--flood-bytes',type=int,default=32768); qp.add_argument('--probes',type=int,default=100); qp.add_argument('--repeat',type=int,default=8); qp.add_argument('--timeout',type=float,default=120); qp.add_argument('--cpu',type=int,default=1); qp.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    if a.mode=='mixed-worker': r=asyncio.run(mixed_worker(a))
    elif a.mode=='mixed-parent': r=mixed_parent(a)
    elif a.mode=='quantum-worker': r=asyncio.run(quantum_worker(a))
    else: r=quantum_parent(a)
    if hasattr(a,'output'): a.output.parent.mkdir(parents=True,exist_ok=True); a.output.write_text(json.dumps(r,indent=2)+'\n')
    print(json.dumps(r))
if __name__=='__main__': main()
