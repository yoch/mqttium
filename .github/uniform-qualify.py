"""Exact-source qualification helper. Creates immutable objects, never moves refs."""
import ast
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import urllib.request

BASE = '9ad1f01857306ac5079ffb1d073a59fdb60e1931'
BASE_TREE = '271fd75325f24852a298845f6a7ef005e684cb46'
ROOT = Path(os.environ['JOB_ROOT']) / 'slot-b'
OUT = Path(os.environ['JOB_ROOT']) / 'evidence'
HERE = Path(__file__).resolve().parent
PATHS = ['CHANGELOG.md','benchmarks/hotpath_recon.py','benchmarks/uniform_callback_probe.py','docs/api-stability.md','docs/architecture.md','docs/implementation-guide.md','docs/migration.md','docs/reference/async-client.md','docs/reports/UNIFORM-CALLBACK-WORKER-2026-09-10.md','src/mqttium/api/_delivery.py','src/mqttium/api/async_client.py','tests/unit/test_callback_message_batches.py','tests/unit/test_direct_qos0_capture.py','tests/unit/test_effect_pump.py','tests/unit/test_hotpath_recon.py','tests/unit/test_sync_callback_contract.py','tests/unit/test_topic_callback_dispatch_contract.py','tests/unit/test_topic_callbacks.py','tests/unit/test_uniform_callback_probe.py','tests/unit/test_uniform_callbacks.py']

def run(*args, cwd=ROOT):
    p = subprocess.run(args, cwd=cwd, text=True, capture_output=True)
    if p.returncode:
        print(p.stdout, p.stderr, file=sys.stderr)
        p.check_returncode()
    return p.stdout.strip()

def sha(data):
    return hashlib.sha256(data).hexdigest()

def snapshots():
    return {p: {'sha256':sha((ROOT/p).read_bytes()),'blob':run('git','hash-object',p)} for p in PATHS}

def asts():
    return {p:sha(ast.dump(ast.parse((ROOT/p).read_text()),include_attributes=False).encode()) for p in PATHS if p.endswith('.py')}

def prepare():
    assert os.environ['GITHUB_REPOSITORY'] == 'yoch/mqttium'
    assert os.environ['GITHUB_REF'] == 'refs/heads/work/uniform-stage-20260910'
    OUT.mkdir(parents=True,exist_ok=True)
    packed = (HERE/'uniform.part1').read_bytes()+(HERE/'uniform.part2').read_bytes()
    assert sha(packed)=='d97a030c4efa647c0a021b8adce02d857c5e4294a7857fb8e64ab2553b810d7c'
    patch=gzip.decompress(packed)
    assert sha(patch)=='8b25a3e5c51a16e9c7331c370849d47e2ec66a4003e5e368cc1753fd99992b97'
    (OUT/'input.patch').write_bytes(patch)
    run('git','worktree','add','--detach',str(ROOT),BASE,cwd=HERE.parent)
    assert run('git','rev-parse','HEAD^{tree}')==BASE_TREE
    run('git','apply','--check',str(OUT/'input.patch'))
    run('git','apply','--index',str(OUT/'input.patch'))
    assert set(run('git','diff','--cached','--name-only').splitlines())==set(PATHS)
    # Keep one cell's closures and unconditional cleanup together. This is a
    # benchmark-only annotation, not a runtime or global complexity exemption.
    path=ROOT/'benchmarks/uniform_callback_probe.py'
    text=path.read_text()
    needle='async def cell(args, mode, size):\n'
    assert text.count(needle)==1
    text=text.replace(needle, '# Keep timing closures and all-path resource cleanup in one lexical cell.\nasync def cell(args, mode, size):  # noqa: C901\n')
    path.write_text(text)
    (OUT/'annotation.txt').write_text('Benchmark-only C901 annotation; all runtime/test/benchmark ASTs unchanged.\n')
    (OUT/'input-ast.json').write_text(json.dumps(asts(),indent=2)+'\n')
    (OUT/'input-sources.json').write_text(json.dumps(snapshots(),indent=2)+'\n')

def freeze():
    original=json.loads((OUT/'input-ast.json').read_text())
    run('ruff','format',*original)
    assert asts()==original,'formatter changed AST'
    run('git','add','--',*PATHS)
    tree=run('git','write-tree')
    record={'base':BASE,'root':tree,'staging':os.environ['GITHUB_SHA'],'files':snapshots(),'ast':asts(),'python':sys.version,'run':os.environ['GITHUB_RUN_ID']}
    token=os.environ['GH_TOKEN']
    def post(path,doc):
        req=urllib.request.Request('https://api.github.com/repos/yoch/mqttium/git/'+path,data=json.dumps(doc).encode(),method='POST',headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','Content-Type':'application/json','X-GitHub-Api-Version':'2022-11-28'})
        with urllib.request.urlopen(req,timeout=45) as response:
            return json.load(response)
    entries=[]
    for p in PATHS:
        blob=post('blobs',{'encoding':'base64','content':base64.b64encode((ROOT/p).read_bytes()).decode()})['sha']
        assert blob==record['files'][p]['blob']
        entries.append({'path':p,'mode':'100644','type':'blob','sha':blob})
    remote_tree=post('trees',{'base_tree':BASE_TREE,'tree':entries})['sha']
    assert remote_tree==tree
    commit=post('commits',{'tree':tree,'parents':[BASE],'message':'experiment: uniform bounded message callback ownership\n\nFast synchronous admission, ordinary FIFO entries and one serial consumer.\nRemove message pair-inline execution and physical batch reservations. Bound\nworker rounds by initial queue occupancy; controller-owned pending work,\ngeneration-safe shutdown/reopen and a stable per-message topic dispatcher.\nKeep strict callable-form contract, byte accounting and on_publish fast path.\n\nHistorical trade-offs and changed scheduling contract are explicit.\nNo release/merge approval. Exact-source qualification run '+record['run']+'.\n'})['sha']
    record['candidate']=commit
    run('git','fetch','--no-tags','origin',commit)
    run('git','checkout','--detach',commit)
    assert run('git','rev-parse','HEAD^{tree}')==tree
    record['src_tree']=run('git','rev-parse','HEAD:src')
    assert not run('git','status','--porcelain','--untracked-files=no')
    (OUT/'frozen.json').write_text(json.dumps(record,indent=2)+'\n')
    (OUT/'candidate.patch').write_text(run('git','diff','--binary',BASE,commit)+'\n')
    for p in PATHS:
        dest=OUT/'sources'/p
        dest.parent.mkdir(parents=True,exist_ok=True)
        dest.write_bytes((ROOT/p).read_bytes())
    print(json.dumps(record,indent=2))

def verify():
    record=json.loads((OUT/'frozen.json').read_text())
    assert snapshots()==record['files'] and asts()==record['ast']
    assert run('git','rev-parse','HEAD')==record['candidate']
    assert run('git','rev-parse','HEAD^{tree}')==record['root']
    assert not run('git','status','--porcelain','--untracked-files=no')
    (OUT/'verified.json').write_text(json.dumps(record,indent=2)+'\n')

{'prepare':prepare,'freeze':freeze,'verify':verify}[sys.argv[1]]()
