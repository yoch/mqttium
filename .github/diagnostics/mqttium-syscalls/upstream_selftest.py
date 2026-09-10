"""Validate the small instrumentation patch against the full pinned sources."""
from __future__ import annotations
import argparse
import ast
import gc
import os
import resource
import time
from pathlib import Path


def load_function(path: Path, name: str, namespace: dict):
    tree = ast.parse(path.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), namespace)
    return namespace[name]


class Sampler:
    def __init__(self):
        self.rows = []
    def add(self, **kwargs):
        self.rows.append(kwargs)


def check(original: Path, fixed: Path):
    for root, n in ((original, 24), (fixed, 2)):
        commit = load_function(root / 'src/mqtt_client_bench/roles/rtt_initiator.py', '_commit_trace', {})
        sampler = Sampler()
        state = {'temporal_trace': sampler, 'trace_pending': {
            12: {'scheduled_deadline_ns': 5}, 24: {'scheduled_deadline_ns': 6}}}
        for seq in range(1, 25):
            commit(state, seq, 10, 20)
        assert len(sampler.rows) == n
        if n == 2:
            assert [r['sequence'] for r in sampler.rows] == [12, 24]
    namespace = {'gc': gc, 'resource': resource, 'time': time, 'os': os}
    path = fixed / 'src/mqtt_client_bench/roles/rtt_drive.py'
    snap = load_function(path, 'process_runtime_snapshot', namespace)
    delta = load_function(path, 'process_runtime_delta', namespace)
    first, second = snap(), snap()
    change = delta(first, second)
    for key in ('ru_minflt', 'ru_majflt', 'ru_nvcsw', 'ru_nivcsw'):
        assert key in first and key in change
    assert first['diagnostic_pid'] == os.getpid()
    print('Full pinned-source functions: trace reservation fix and boundary counters PASS')

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--original', type=Path, required=True)
    p.add_argument('--fixed', type=Path, required=True)
    a = p.parse_args()
    check(a.original, a.fixed)
