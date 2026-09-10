#!/usr/bin/env python3
"""Frozen, finite diagnostic campaign. Never repeat a cell based on its result."""
from __future__ import annotations
import argparse
import csv
import gzip
import json
import math
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from common import BASE, HEAD, HARNESS, BASE_SRC, HEAD_SRC, dump, sha256, trace_command, verify_source
from prepare_harness import prepare


def plan() -> list[dict]:
    rows = []
    def add(source='head', variant='fixed', mode='none', target='none'):
        rows.append({'cell': len(rows), 'source': source, 'variant': variant,
                     'mode': mode, 'target': target})
    add(variant='pristine')
    for variant in ('original', 'fixed', 'fixed', 'original') * 2:
        add(variant=variant)
    for mode in ('memory', 'flow'):
        for variant in ('original', 'fixed'):
            add(variant=variant, mode=mode, target='rtt_initiator')
    for target in ('responder', 'broker'):
        for mode in ('memory', 'flow'):
            add(mode=mode, target=target)
    for source in ('base', 'head', 'head', 'base'):
        add(source=source)
    add(variant='pristine')
    return rows


def quantile(xs: list[int], pct: float) -> float | None:
    if not xs:
        return None
    data = sorted(xs)
    return data[max(0, math.ceil(len(data) * pct) - 1)] / 1000


def summarize(out: Path, row: dict, status: int) -> dict:
    answer = {**row, 'exit_status': status, 'measurement_valid': False,
              'performance_qualified': False}
    if not (out / 'result.json').exists():
        return answer
    result = json.loads((out / 'result.json').read_text())
    init = next((w for w in result.get('workers', []) if w.get('role') == 'rtt_initiator'), {})
    lat = init.get('latencies_ns') or []
    completed = init.get('completed_in_window', 0)
    delta = init.get('runtime', {}).get('measure_delta', {})
    trace = init.get('temporal_trace') or {}
    answer.update(
        measurement_valid=result.get('status') == 'valid' and result.get('temporal_quality', {}).get('ok') is True,
        harness_status=result.get('status'), reasons=json.dumps(result.get('reasons', [])),
        temporal_reasons=json.dumps(result.get('temporal_quality', {}).get('reasons', [])),
        completed=completed, retained_latencies=len(lat),
        ordered_full_latency_stream=(len(lat) == completed),
        p50_us=quantile(lat, .5), p95_us=quantile(lat, .95), p99_us=quantile(lat, .99),
        slow_fraction=sum(290000 <= x <= 450000 for x in lat) / len(lat) if lat else None,
        ru_minflt=delta.get('ru_minflt'), ru_majflt=delta.get('ru_majflt'),
        minflt_per_response=delta.get('ru_minflt', 0) / completed if completed and 'ru_minflt' in delta else None,
        nvcsw_per_response=delta.get('ru_nvcsw', 0) / completed if completed else None,
        trace_count=trace.get('count'), trace_stride=trace.get('stride'),
    )
    for name in ('send_ns', 'receive_ns'):
        values = trace.get(name) or []
        answer[name + '_span_s'] = (max(values) - min(values)) / 1e9 if values else None
    return answer


def strace_preflight(binary: str, out: Path) -> None:
    (out / 'strace-version.txt').write_bytes(subprocess.check_output([binary, '-V'], stderr=subprocess.STDOUT))
    pidfile = out / 'preflight-pid'
    cmd = [sys.executable, '-c',
           'import os,time; from pathlib import Path; '
           f'Path({str(pidfile)!r}).write_text(str(os.getpid())); time.sleep(.1)']
    # Prove each exact filter is accepted, and -D keeps the real tracee as child.
    for mode in ('memory', 'flow'):
        pidfile.unlink(missing_ok=True)
        target = out / ('preflight-' + mode + '.strace')
        with (out / ('preflight-' + mode + '.log')).open('wb') as log:
            p = subprocess.Popen(trace_command(mode, target, cmd, binary), stdout=log, stderr=subprocess.STDOUT)
            p.wait(timeout=15)
        if p.returncode != 0 or not pidfile.exists() or int(pidfile.read_text()) != p.pid:
            raise RuntimeError('strace -D/ptrace preflight failed. No benchmark run. See preflight logs.')
        if not target.exists() or target.stat().st_size == 0:
            raise RuntimeError('strace produced no syscalls')
    dump(out / 'preflight.json', {'ptrace_launch': 'passed', 'tracee_pid_matches_popen': True,
        'follow_forks': False, 'seccomp_bpf': False, 'global_security_changes': False})


def tree_size(root: Path) -> int:
    total = 0
    for path in root.rglob('*'):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            # Writers use atomic rename while the guard samples the directory.
            pass
    return total


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--base', type=Path, required=True)
    parser.add_argument('--head', type=Path, required=True)
    parser.add_argument('--harness', type=Path, required=True)
    a = parser.parse_args()
    a.root = a.root.resolve()
    out = a.root / 'out'
    out.mkdir(exist_ok=True)
    os.umask(0o077)
    all_rows = plan()
    dump(out / 'plan.json', {'cells': all_rows, 'rate': 3942, 'duration_s': 12, 'warmup_s': 3,
        'drain_s': 6, 'effective_payload_bytes': 40, 'cell_timeout_s': 175,
        'max_raw_cell_bytes': 384 * 1024**2, 'max_total_bytes': 2 * 1024**3,
        'retry_policy': 'none; failures and invalid stimulus retained',
        'trace_scope': 'one main TID; pacer never ptraced', 'release_gate': False})
    if platform.system() != 'Linux' or not {0, 1, 2, 3} <= os.sched_getaffinity(0):
        raise RuntimeError('Linux and accessible CPUs0,1,2,3 are required')
    if os.environ.get('RUNNER_NAME') != 'rpi5':
        raise RuntimeError('This frozen campaign is for the rpi5 runner only')
    if shutil.disk_usage(a.root).free < 1024**3:
        raise RuntimeError('At least 1GiB of free temporary space required')
    binary = shutil.which('strace')
    if not binary or not shutil.which('mosquitto'):
        raise RuntimeError('strace and mosquitto must be executable')
    verify_source(a.base, BASE, BASE_SRC)
    verify_source(a.head, HEAD, HEAD_SRC)
    strace_preflight(binary, out)
    arms = prepare(a.harness.resolve(), a.root, out)
    selftest = subprocess.run([sys.executable, str(Path(__file__).with_name('upstream_selftest.py')),
                    '--original', arms['original']['path'], '--fixed', arms['fixed']['path']], check=True,
                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    (out / 'upstream-selftest.txt').write_text(selftest.stdout)
    summary = []
    failed = False
    active = None
    def interrupt(signum, _frame):
        if active is not None:
            try:
                os.killpg(active.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    try:
        for row in all_rows:
            cell = out / f'c{row["cell"]:02}'
            cell.mkdir()
            src = a.base if row['source'] == 'base' else a.head
            cmd = [sys.executable, str(Path(__file__).with_name('one_cell.py')),
                   '--source', str(src.resolve()), '--harness', arms[row['variant']]['path'],
                   '--output', str(cell), '--source-label', row['source'], '--variant', row['variant'],
                   '--trace-mode', row['mode'], '--target', row['target'], '--strace', binary]
            dump(cell / 'invocation.json', {'argv': cmd, 'started_at': time.time()})
            print(f'[{row["cell"]+1}/{len(all_rows)}] {row}', flush=True)
            with (cell / 'cell.log').open('wb') as log:
                active = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                start = time.monotonic()
                abort = None
                while active.poll() is None:
                    time.sleep(1)
                    rawsize = tree_size(cell)
                    if rawsize > 384 * 1024**2:
                        abort = 'raw_log_size_limit'
                    if time.monotonic() - start > 175:
                        abort = 'cell_timeout'
                    if abort:
                        os.killpg(active.pid, signal.SIGTERM)
                        try:
                            active.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            os.killpg(active.pid, signal.SIGKILL)
                            active.wait()
                        dump(cell / 'abort.json', {'reason': abort})
                        break
                status = active.returncode
                # Terminate leftover descendants ONLY in this owned process group,
                # including a detached -D tracer; never search/kill by name.
                try:
                    os.killpg(active.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                active = None
            if row['mode'] != 'none' and not any(cell.glob('*.strace')):
                status = status or 2
                dump(cell / 'missing-trace.json', {'error': 'requested trace missing'})
            failed |= status != 0
            summary.append(summarize(cell, row, status))
            for trace in cell.glob('*.strace'):
                with trace.open('rb') as f, gzip.open(str(trace) + '.gz', 'wb', compresslevel=1) as gz:
                    shutil.copyfileobj(f, gz)
                trace.unlink()
            dump(out / 'summary.json', summary)
            size = tree_size(out)
            if size > 2 * 1024**3 or shutil.disk_usage(a.root).free < 256 * 1024**2:
                dump(out / 'campaign-abort.json', {'reason': 'storage_guard'})
                failed = True
                break
            time.sleep(2)
    finally:
        if active is not None:
            try:
                os.killpg(active.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        dump(out / 'summary.json', summary)
        if summary:
            keys = list(dict.fromkeys(k for r in summary for k in r))
            with (out / 'summary.csv').open('w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(summary)
        for name, arm in arms.items():
            current = {str(p.relative_to(Path(arm['path']))): sha256(p)
                       for p in sorted((Path(arm['path']) / 'src').rglob('*.py'))}
            if current != arm['files']:
                failed = True
                dump(out / ('MUTATED-' + name + '.json'), current)
        verify_source(a.base, BASE, BASE_SRC)
        verify_source(a.head, HEAD, HEAD_SRC)
        dump(out / 'completion.json', {'cells_expected': len(all_rows), 'cells_finished': len(summary),
             'collection_errors': failed or len(summary) != len(all_rows),
             'performance_qualified': False, 'merge_authorized': False})
        dump(out / 'files-sha256.json', {str(p.relative_to(out)): sha256(p)
            for p in sorted(out.rglob('*')) if p.is_file() and p.name not in ('files-sha256.json', 'campaign.log')})
    return int(failed)

if __name__ == '__main__':
    raise SystemExit(main())
