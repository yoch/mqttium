"""Diagnostic-only wrapper. Does not edit either imported source checkout."""
import asyncio, ctypes, gc, importlib, json, os, pathlib, resource, sys, time

module = sys.argv[1]
sys.argv = [module] + sys.argv[2:]
config = json.loads(pathlib.Path(sys.argv[sys.argv.index('--config') + 1]).read_text())
variant = os.environ['DIAG_VARIANT'].removeprefix('profile_')
role = module.rsplit('.', 1)[-1]
out = pathlib.Path(os.environ['DIAG_CELL'])
src = pathlib.Path(config['client_path']).resolve()
sys.path.insert(0, str(src/'src'))
if variant == 'padding':
    padding = [bytearray(513+(i%31)*32) for i in range(2048)]
if variant == 'slack1':
    ctypes.CDLL(None).prctl(29, 1, 0, 0, 0)

import mqttium
m = importlib.import_module(module)
snapshots = []
try:
    audit = ctypes.CDLL(None)
    reset, dump = audit.diag_reset, audit.diag_dump
    dump.argtypes = [ctypes.c_char_p]
except AttributeError:
    audit = reset = dump = None

meta = {
    'pid': os.getpid(), 'role': role, 'variant': variant,
    'module': mqttium.__file__, 'sys_executable': sys.executable,
    'affinity': sorted(os.sched_getaffinity(0)),
    'pythonmalloc': os.environ.get('PYTHONMALLOC'),
    'config': config,
    'timerslack': pathlib.Path('/proc/self/timerslack_ns').read_text().strip(),
    'snapshots': snapshots,
}
(out / f'{role}-maps.txt').write_text(pathlib.Path('/proc/self/maps').read_text())

def sample(label):
    u=resource.getrusage(resource.RUSAGE_SELF)
    snapshots.append({'label':label, 'ns':time.monotonic_ns(), 'minflt':u.ru_minflt,
      'majflt':u.ru_majflt, 'nvcsw':u.ru_nvcsw,'nivcsw':u.ru_nivcsw,
      'utime':u.ru_utime,'stime':u.ru_stime,'maxrss':u.ru_maxrss,
      'allocated_blocks':sys.getallocatedblocks(),'gc_count':gc.get_count(),
      'gc_stats':gc.get_stats(),'affinity':sorted(os.sched_getaffinity(0))})
    if dump: dump(str(out / f'{role}-{label}-syscalls.json').encode())
    (out / f'{role}-diag.json').write_text(json.dumps(meta,indent=2))

original_snapshot = m.process_runtime_snapshot
snap_count = 0

def snapshot():
    global snap_count
    sample(f'snapshot{snap_count}')
    if role == 'rtt_initiator' and snap_count==0 and reset: reset()
    snap_count += 1
    return original_snapshot()
m.process_runtime_snapshot = snapshot

original_barrier = m.barrier_client_session
class Barrier:
    def __init__(self, wrapped): self.wrapped=wrapped
    def __getattr__(self, key): return getattr(self.wrapped,key)
    def wait(self, name):
        answer=self.wrapped.wait(name)
        if name=='T_MEASURE':
            sample('barrier_measure')
            if reset: reset()
        return answer
m.barrier_client_session = lambda *a,**kw: Barrier(original_barrier(*a,**kw))

if variant == 'timeout_inline' and role == 'rtt_initiator':
    async def recv_token(loop, sock, until, until_ns=None, recv_until_ns=None):
        bound = recv_until_ns if recv_until_ns is not None else until_ns
        remaining = ((int(bound)-time.monotonic_ns())/1e9 if bound is not None
                     else until-time.perf_counter())
        if remaining<=0: return None
        try:
            async with asyncio.timeout(min(.05,max(remaining,0.))):
                data=await loop.sock_recv(sock,64)
        except (asyncio.TimeoutError,OSError): return None
        return m.unpack_token(data)
    m._recv_token_async = recv_token
if variant in ('trace_fixed', 'trace_off') and role == 'rtt_initiator':
    original_commit_trace = m._commit_trace
    def commit_trace(state, seq, send_ns, receive_ns):
        if seq in state.get('trace_pending', {}):
            original_commit_trace(state, seq, send_ns, receive_ns)
    m._commit_trace = commit_trace
    if variant == 'trace_off':
        original_begin = m._begin_measure_instrumentation
        def begin(cfg, state, *args):
            # Keep PaceRecorder and identical offered tokens; only trace is disabled.
            c = dict(cfg, temporal_trace_max_points=-1)
            return original_begin(c, state, *args)
        m._begin_measure_instrumentation = begin
if variant == 'gc_off': gc.disable()

# Optional follow-up hypotheses; not executed by the first completed matrix.
# A persistent reader can remove per-token epoll ADD/DEL without changing the
# external emission calendar. Buffer bound is explicit; no unbounded spawn.
if variant in ('persistent_reader', 'both') and role == 'rtt_initiator':
    from collections import deque
    pending = deque()
    ready = asyncio.Event()
    registered = None
    def read_tokens(loop, sock):
        try:
            for _ in range(32):
                data = sock.recv(64)
                if len(pending) >= 4096:
                    raise RuntimeError('diagnostic token buffer overflow')
                pending.append(data)
        except BlockingIOError:
            pass
        finally:
            if pending:
                ready.set()
    async def recv_token(loop, sock, until, until_ns=None, recv_until_ns=None):
        global registered
        if registered is None:
            registered = (loop, sock.fileno())
            loop.add_reader(sock.fileno(), read_tokens, loop, sock)
        bound = recv_until_ns if recv_until_ns is not None else until_ns
        remaining = ((int(bound)-time.monotonic_ns())/1e9 if bound is not None
                     else until-time.perf_counter())
        if remaining <= 0:
            return None
        try:
            if not pending:
                ready.clear()
                async with asyncio.timeout(min(.05, remaining)):
                    await ready.wait()
        except (asyncio.TimeoutError, OSError):
            return None
        return m.unpack_token(pending.popleft())
    m._recv_token_async = recv_token
    original_drive = m._send_loop_async
    async def drive(*args, **kwargs):
        global registered
        try:
            return await original_drive(*args, **kwargs)
        finally:
            if registered is not None:
                registered[0].remove_reader(registered[1])
                registered = None
            if pending:
                raise RuntimeError('diagnostic leftover tokens between phases')
    m._send_loop_async = drive

# For the RTT roles, on_publish is never installed by the harness: all measured
# completions are response messages. Guard the experimental path to that case.
if variant in ('unused_completion_off', 'both'):
    from mqtt_client_bench.adapters.mqttium_async import MqttiumAsyncAdapter, FlowControlError
    original_publish = MqttiumAsyncAdapter.publish_nowait
    def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        if self.on_publish is not None or qos == 0:
            return original_publish(self,topic,payload,qos,retain,properties)
        assert self._client.on_publish is None
        try:
            self._client.publish_nowait(topic, payload, qos=qos, retain=retain, properties=properties)
        except FlowControlError:
            return None
        return self._alloc_mid()
    MqttiumAsyncAdapter.publish_nowait = publish

sample('startup')
try:
    rc=m.main()
finally:
    sample('exit')
raise SystemExit(rc)
