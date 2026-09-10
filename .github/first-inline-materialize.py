"""One-shot exact-source validation; creates Git objects, never updates refs."""
from __future__ import annotations
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
REPO = 'yoch/mqttium'
ROOT = Path(os.environ['CANDIDATE_ROOT'])
OUT = Path(os.environ['EVIDENCE_ROOT'])
HERE = Path(__file__).resolve().parent
MANIFEST = json.loads((HERE / 'first-inline-manifest.json').read_text())

def run(*cmd, cwd=ROOT, **kwargs):
    return subprocess.run(cmd, cwd=cwd, text=True, check=True, capture_output=True, **kwargs).stdout.strip()

def check(ok, message):
    if not ok:
        raise RuntimeError(message)

def sha(data):
    return hashlib.sha256(data).hexdigest()

def asts():
    return {p: sha(ast.dump(ast.parse((ROOT/p).read_text()), include_attributes=False).encode())
            for p in MANIFEST['ast_sha256']}

def snapshots():
    return {p: {'sha256': sha((ROOT/p).read_bytes()),
                'blob': run('git','hash-object',p)} for p in MANIFEST['paths']}

def prepare():
    check(os.environ['GITHUB_REPOSITORY'] == REPO, 'wrong repository')
    check(os.environ['GITHUB_REF'] == 'refs/heads/bench/first-inline-pi-smoke-9ad1f018', 'wrong branch')
    OUT.mkdir(parents=True, exist_ok=True)
    check(MANIFEST['base'] == BASE, 'wrong base')
    packed = b''.join((HERE / f'first-inline-candidate.part{i}').read_bytes() for i in (1, 2))
    check(sha(packed) == MANIFEST['payload_sha256'], 'payload checksum')
    patch = gzip.decompress(packed)
    check(sha(patch) == MANIFEST['patch_sha256'], 'patch checksum')
    (OUT/'input.patch').write_bytes(patch)
    run('git','worktree','add','--detach',str(ROOT),BASE, cwd=HERE.parent)
    check(run('git','rev-parse','HEAD^{tree}') == BASE_TREE, 'wrong base tree')
    run('git','apply','--check',str(OUT/'input.patch'))
    run('git','apply','--index',str(OUT/'input.patch'))
    check(set(run('git','diff','--cached','--name-only').splitlines()) == set(MANIFEST['paths']), 'unexpected paths')
    check(asts() == MANIFEST['ast_sha256'], 'input AST mismatch')
    (OUT/'input-sources.json').write_text(json.dumps(snapshots(),indent=2)+'\n')

def freeze():
    check(asts() == MANIFEST['ast_sha256'], 'formatter changed AST')
    run('git','add','--',*MANIFEST['paths'])
    tree=run('git','write-tree')
    env=os.environ | {'GIT_AUTHOR_NAME':'MQTTium validation','GIT_AUTHOR_EMAIL':'validation@example.invalid',
                      'GIT_COMMITTER_NAME':'MQTTium validation','GIT_COMMITTER_EMAIL':'validation@example.invalid'}
    commit=run('git','commit-tree',tree,'-p',os.environ['GITHUB_SHA'],env=env,
               input='fix: harden first-inline callback ownership; integration candidate\n')
    run('git','checkout','--detach',commit)
    check(not run('git','status','--porcelain','--untracked-files=no'), 'dirty frozen source')
    record={'bootstrap':os.environ['GITHUB_SHA'],'base':BASE,'tree':tree,
            'local_commit':commit,'files':snapshots(),'ast_sha256':asts(), 'python':sys.version,
            'run_id':os.environ['GITHUB_RUN_ID']}
    (OUT/'frozen.json').write_text(json.dumps(record,indent=2)+'\n')
    run('git','diff','--binary',BASE,commit, cwd=ROOT)
    (OUT/'candidate.patch').write_text(run('git','diff','--binary',BASE,commit)+'\n')
    for p in MANIFEST['paths']:
        dest=OUT/'sources'/p
        dest.parent.mkdir(parents=True,exist_ok=True)
        dest.write_bytes((ROOT/p).read_bytes())
    print(json.dumps(record,indent=2))

def publish():
    record=json.loads((OUT/'frozen.json').read_text())
    check(not run('git','status','--porcelain','--untracked-files=no'), 'tests changed tracked source')
    check(snapshots()==record['files'] and asts()==record['ast_sha256'],'source changed after freeze')
    check(run('git','rev-parse','HEAD^{tree}')==record['tree'],'tree changed after freeze')
    # Immutable Git objects only: no ref update, merge, branch or PR operation.
    token=os.environ['GH_TOKEN']
    def api(path,payload):
        request=urllib.request.Request('https://api.github.com/repos/'+REPO+'/git/'+path,
            data=json.dumps(payload).encode(),method='POST',headers={
                'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json',
                'Content-Type':'application/json','X-GitHub-Api-Version':'2022-11-28'})
        with urllib.request.urlopen(request,timeout=45) as response:
            return json.load(response)
    entries=[]
    for p in MANIFEST['paths']:
        blob=api('blobs',{'encoding':'base64','content':base64.b64encode((ROOT/p).read_bytes()).decode()})['sha']
        check(blob==record['files'][p]['blob'],'remote blob mismatch: '+p)
        entries.append({'path':p,'mode':'100644','type':'blob','sha':blob})
    tree=api('trees',{'base_tree':BASE_TREE,'tree':entries})['sha']
    check(tree==record['tree'],'remote tested tree mismatch')
    commit=api('commits',{'tree':tree,'parents':[record['bootstrap']], 'message':
        'fix: harden first-inline callback ownership and expose the tested runtime\n\n'
        'Retire cancelled callback workers including pre-start cancellation; protect\n'
        'replacement ownership and waiting producers. Reschedule the unconsumed effect\n'
        'suffix before relaying interrupted inline admission. Integrate 292 focused\n'
        'prototype/ownership cases and explicit cancellation/fairness limitations.\n\n'
        'Restore production workflows/tools. No main write, merge or release.\n'
        'The old Pi smoke is not performance qualification of this corrected tree.\n'
        'Validation run: '+record['run_id']+'; exact tested root: '+tree+'\n'})['sha']
    record['published_commit']=commit
    (OUT/'published.json').write_text(json.dumps(record,indent=2)+'\n')
    print('Immutable candidate commit:',commit,'tree:',tree)

{'prepare':prepare,'freeze':freeze,'publish':publish}[sys.argv[1]]()
