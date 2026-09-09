"""Diagnostic-only wrapper. Does not edit either imported source checkout."""
import asyncio, ctypes, gc, importlib, json, os, pathlib, resource, sys, time

module = sys.argv[1]
sys.argv = [module] + sys.argv[2:]
config = json.loads(pathlib.Path(sys.argv[sys.argv.index('--config') + 1]).read_text())
variant = os.environ['DIAG_VARIANT']
role = module.rsplit('.', 1)[-1]
out = pathlib.Path(os.environ['DIAG_CELL'])
src = pathlib.Path(config['client_path']).resolve()
sys.path.insert(0, str(src/'src'))
if variant == 'padding':
    padding = [bytearray(513+(i%31)*32) for i in range(2048)]
if variant == 'split_cpu' and role == 'responder':
    os.sched_setaffinity(0, {3})
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

if variant == 'no_wait_task' and role == 'rtt_initiator':
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
if variant == 'gc_off': gc.disable()

sample('startup')
try:
    rc=m.main()
finally:
    sample('exit')
raise SystemExit(rc)
