"""Build disposable harness worktrees; never modify a library or main branch."""
from __future__ import annotations
import argparse
import ast
import hashlib
import json
import subprocess
from pathlib import Path
from common import HARNESS, dump, git, sha256, verify_source

BLOBS = {
    'roles/rtt_initiator.py': 'aaee63a1f52ec232a1b049d08914510f0120c987',
    'roles/rtt_drive.py': 'e121fa91c85084c1b9858c676e97fdf08dff98b7',
    'temporal_trace.py': 'ab4253f16a11520451e604dd7e68b3297236f87c',
    'workloads.py': '3e1e168eda8510fe255b15003b94fe1512ee6767',
    'harness.py': '6bd3a243b09788e4b6e7dec47d1c66aac4ec8535',
}

def blob(data: bytes) -> str:
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()

def exactly_once(text: str, old: str, new: str) -> str:
    if text.count(old) != 1:
        raise RuntimeError('Patch context does not occur exactly once: ' + repr(old))
    return text.replace(old, new, 1)

def add_boundary_counters(text: str) -> str:
    text = exactly_once(text, 'import resource\n', 'import resource\nimport time\nimport os\n')
    text = exactly_once(text, '        "ru_nvcsw": int(usage.ru_nvcsw),',
        '        "diagnostic_pid": os.getpid(),\n'
        '        "diagnostic_perf_counter_ns": time.perf_counter_ns(),\n'
        '        "diagnostic_realtime_ns": time.time_ns(),\n'
        '        "ru_minflt": int(usage.ru_minflt),\n'
        '        "ru_majflt": int(usage.ru_majflt),\n'
        '        "ru_nvcsw": int(usage.ru_nvcsw),')
    text = exactly_once(text,
        'for key in ("ru_nvcsw", "ru_nivcsw", "ru_utime_s", "ru_stime_s"):',
        'for key in ("ru_nvcsw", "ru_nivcsw", "ru_utime_s", "ru_stime_s", "ru_minflt", "ru_majflt"):')
    ast.parse(text)
    return text

def fix_sampling(text: str) -> str:
    node = next(n for n in ast.parse(text).body if isinstance(n, ast.FunctionDef) and n.name == '_commit_trace')
    lines = text.splitlines(keepends=True)
    body = ''.join(lines[node.lineno - 1:node.end_lineno])
    old = '    pending = state.get("trace_pending") or {}\n    meta = pending.pop(seq, None) or {}\n'
    new = ('    pending = state.get("trace_pending")\n'
           '    if pending is None:\n        return\n'
           '    meta = pending.pop(seq, None)\n'
           '    if meta is None:\n        return\n')
    body = exactly_once(body, old, new)
    changed = ''.join(lines[:node.lineno - 1]) + body + ''.join(lines[node.end_lineno:])
    ast.parse(changed)
    return changed

def prepare(source: Path, root: Path, output: Path) -> dict:
    verify_source(source, HARNESS)
    src = source / 'src/mqtt_client_bench'
    for name, expected in BLOBS.items():
        if blob((src / name).read_bytes()) != expected:
            raise RuntimeError(f'Unexpected immutable blob: {name}')
    arms = {}
    for name in ('pristine', 'original', 'fixed'):
        dest = root / ('h-' + name)
        subprocess.run(['git', '-C', str(source), 'worktree', 'add', '--detach', '--no-checkout', str(dest), HARNESS], check=True)
        subprocess.run(['git', '-C', str(dest), 'sparse-checkout', 'set', '--no-cone',
                        '/src/', '/hosts/', '/mosquitto/', '/README.md', '/pyproject.toml'], check=True)
        subprocess.run(['git', '-C', str(dest), 'checkout', '--detach', HARNESS], check=True)
        if name != 'pristine':
            target = dest / 'src/mqtt_client_bench/roles/rtt_drive.py'
            target.write_text(add_boundary_counters(target.read_text()))
        if name == 'fixed':
            target = dest / 'src/mqtt_client_bench/roles/rtt_initiator.py'
            target.write_text(fix_sampling(target.read_text()))
        diff = subprocess.check_output(['git', '-C', str(dest), 'diff', '--binary'])
        (output / ('harness-' + name + '.patch')).write_bytes(diff)
        manifest = {str(p.relative_to(dest)): sha256(p) for p in sorted((dest / 'src').rglob('*.py'))}
        arms[name] = {'path': str(dest), 'upstream_sha': HARNESS,
                      'patch_sha256': hashlib.sha256(diff).hexdigest(), 'files': manifest}
    dump(output / 'harness-manifests.json', arms)
    return arms

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=True)
    prepare(a.source.resolve(), a.root.resolve(), a.output.resolve())
