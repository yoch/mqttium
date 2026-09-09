"""Frozen finite diagnosis, not release-level A/B acceptance."""
import json, os, pathlib, random, shutil, subprocess, sys, time, traceback, tempfile
from mqtt_client_bench import harness
from mqtt_client_bench.scenarios import SCENARIO_BY_NAME,expand_scenario
from mqtt_client_bench.telemetry import allocate_cpuset
from mqtt_client_bench.metrics import comparison_value

root=pathlib.Path(os.environ['DIAG_ROOT']).resolve()
tools=pathlib.Path(__file__).resolve().parent
pid=int(os.environ['BROKER_PID'])
base=pathlib.Path('arm-a').resolve();candidate=pathlib.Path('arm-b').resolve()
real_popen=subprocess.Popen
cell=None;variant=None

def popen(cmd,*args,**kw):
    if isinstance(cmd,(list,tuple)) and '-m' in cmd:
        idx=cmd.index('-m')
        mod=cmd[idx+1]
        if mod in ('mqtt_client_bench.roles.rtt_initiator','mqtt_client_bench.roles.responder'):
            env=dict(kw.get('env') or os.environ)
            env.update(DIAG_CELL=str(cell),DIAG_VARIANT=variant)
            if variant.startswith('profile_'):
                env['LD_PRELOAD']=str(root/'sysaudit.so')
            if variant=='malloc': env['PYTHONMALLOC']='malloc'
            cmd=list(cmd[:idx])+[str(tools/'worker.py'),mod]+list(cmd[idx+2:])
            if variant=='memorytrace' and shutil.which('strace'):
                cmd=['strace','-f','--seccomp-bpf','-ttt','-T','-e','trace=memory','-o',str(cell/(mod.split('.')[-1]+'.memory.strace'))]+cmd
            if variant=='strace' and shutil.which('strace'):
                cmd=['strace','-f','-c','-w','-o',str(cell/(mod.split('.')[-1]+'.summary.strace'))]+cmd
            kw['env']=env
    return real_popen(cmd,*args,**kw)
subprocess.Popen=popen

cpus=allocate_cpuset(['sut','broker','loadgen','orch'],profile='standard')
(root/'cpusets.json').write_text(json.dumps(cpus))
def ids(s):
    out=[]
    for a in s.split(','):
        if '-' in a:
            lo,hi=map(int,a.split('-'));out.extend(range(lo,hi+1))
        else:out.append(int(a))
    return set(out)
os.sched_setaffinity(pid,ids(cpus['broker']))
os.sched_setaffinity(0,ids(cpus['orch']))

# Follow-up registered after discovery matrix: all timing arms un-interposed.
# Three shuffled fresh-process repetitions; counter profiles are separate.
variants=['plain','unused_completion_off','persistent_reader','both']
plan=[];rng=random.Random(4543943)
for rep in range(3):
    order=variants.copy();rng.shuffle(order)
    plan += [{'rep':rep,'variant':v,'duration_s':12} for v in order]
plan += [{'rep':0,'variant':'profile_'+v,'duration_s':12} for v in variants]
(root/'plan.json').write_text(json.dumps(plan,indent=2))
for index,spec in enumerate(plan):
    variant=spec['variant'];cell=root/'cells'/f'{index:02}-{variant}-{spec["rep"]}'
    cell.mkdir(parents=True)
    work=pathlib.Path(tempfile.mkdtemp(prefix='md-',dir='/tmp'))
    p=dict(expand_scenario(SCENARIO_BY_NAME['application_rtt_fixed_rate'],'standard')[0])
    p.update(target_rate=3942.,pacer_mode='external',version_ab_target_frozen=True,duration_s=spec['duration_s'])
    started=time.monotonic()
    try:
        r=harness.run_point(p,client='mqttium',client_path=str(candidate if variant=='candidate' else base),
            host='127.0.0.1',port=18894,tls_port=18884,profile='standard',work_dir=work,
            cpusets=cpus,load_profile=None,host_profile=None,managed_broker=False,
            external_broker_pid=pid,cross_client=True)
        r['diagnostic_variant']=variant
        (cell/'result.json').write_text(json.dumps(r))
        summary={'index':index,**spec,'seconds':time.monotonic()-started,'status':r.get('status'),
             'reasons':r.get('reasons'),**comparison_value(r,'application_rtt_fixed_rate')}
    except Exception as e:
        (cell/'error.txt').write_text(traceback.format_exc());summary={'index':index,**spec,'error':str(e)}
    finally:
        # Keep only regular files, never copy IPC sockets. Original full configs
        # and traces survive even when the run raises.
        (cell/'work').mkdir()
        for f in work.iterdir():
            if f.is_file(): shutil.copy2(f,cell/'work'/f.name)
        shutil.rmtree(work)
    print(json.dumps(summary),flush=True)
    with (root/'summary.jsonl').open('a') as f:f.write(json.dumps(summary)+'\n')

assert any((root/'cells').glob('*/result.json')), 'no measurement completed; inspect errors'
