import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

WORKER = r'''
import json
import time
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.packets import PubRecPacket, PubCompPacket

samples=[]
n=15000
for rep in range(13):
    e=ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv311, max_outbound_inflight=1))
    e.state=ConnectionState.CONNECTED
    start=time.perf_counter_ns()
    for _ in range(n):
        h=e.queue_publish('bench/q2', b'x'*32, qos=QoS.EXACTLY_ONCE)
        e.take_effects()
        mid=h.mid
        d=IncrementalDecoder(); d.feed(PubRecPacket(mid=mid).encode(MQTTProtocolVersion.MQTTv311)); e.handle_raw(d.next_packet()); e.take_effects()
        d=IncrementalDecoder(); d.feed(PubCompPacket(mid=mid).encode(MQTTProtocolVersion.MQTTv311)); e.handle_raw(d.next_packet()); e.take_effects()
    elapsed=time.perf_counter_ns()-start
    if rep>=2:
        samples.append(n*1e9/elapsed)
print(json.dumps(samples))
'''


def run(root: str) -> list[float]:
    env=os.environ.copy()
    env['PYTHONPATH']=str(Path(root).resolve()/'src')
    return json.loads(subprocess.check_output([sys.executable, '-c', WORKER], env=env, text=True))

base=run('/tmp/mqttium-base')
candidate=run('.')
ratios=[c/b for b,c in zip(base,candidate)]
result={'base':base,'candidate':candidate,'ratios':ratios,'median_ratio':statistics.median(ratios)}
print(json.dumps(result, indent=2))
if result['median_ratio'] < 1.03:
    raise SystemExit(f"candidate gain too small: {result['median_ratio']:.4f}x")
