#!/usr/bin/env python3
"""Read-only, bounded Linux /proc collector for explicitly supplied process IDs.

No sudo, no attach, no process memory/environment reads, and no system changes.
Start before a benchmark measurement and correlate monotonic_ns in the output.
This is counter collection, NOT a syscall trace. Run on a spare/observer core
when available; use identical observation in both diagnostic arms.
"""
from __future__ import annotations
import argparse
import json
import math
import os
from pathlib import Path
import platform
import time


def read(path: Path) -> str | None:
    try:
        return path.read_text()
    except (OSError, UnicodeError):
        return None


def stat_fields(text: str) -> dict:
    close = text.rfind(')')
    if close < 0:
        raise ValueError('invalid /proc stat: no comm terminator')
    fields = text[close + 2:].split()
    if len(fields) < 37:
        raise ValueError('truncated /proc stat')
    return {
        'state': fields[0], 'minflt': int(fields[7]), 'majflt': int(fields[9]),
        'utime_ticks': int(fields[11]), 'stime_ticks': int(fields[12]),
        'starttime_ticks': int(fields[19]), 'processor': int(fields[36]),
    }


def status_fields(text: str | None) -> dict:
    wanted = {'VmRSS', 'VmHWM', 'Threads', 'voluntary_ctxt_switches',
              'nonvoluntary_ctxt_switches'}
    output = {}
    for line in (text or '').splitlines():
        key, sep, value = line.partition(':')
        if sep and key in wanted:
            output[key] = int(value.split()[0])
    return output


def task_snapshot(root: Path) -> dict:
    value = read(root / 'stat')
    if value is None:
        return {'readable': False}
    try:
        out = stat_fields(value)
    except (ValueError, IndexError) as exc:
        return {'readable': False, 'error': str(exc)}
    out.update(status_fields(read(root / 'status')))
    sched = read(root / 'schedstat')
    if sched:
        parts = sched.split()
        if len(parts) >= 3:
            out['schedstat_run_ns'] = int(parts[0])
            out['schedstat_wait_ns'] = int(parts[1])
            out['schedstat_slices'] = int(parts[2])
    out['wchan'] = (read(root / 'wchan') or '').strip() or None
    out['readable'] = True
    return out


def pid_snapshot(pid: int) -> dict:
    root = Path('/proc') / str(pid)
    out = task_snapshot(root)
    out['pid'] = pid
    if not out['readable']:
        return out
    # Per-task counters avoid confusing main-thread switch counts with the
    # all-thread counters from getrusage(RUSAGE_SELF).
    try:
        tids = sorted(root.joinpath('task').iterdir(), key=lambda p: int(p.name))
    except OSError:
        tids = []
    out['threads'] = {p.name: task_snapshot(p) for p in tids}
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pid', type=int, action='append', required=True)
    parser.add_argument('--seconds', type=float, default=30.0)
    parser.add_argument('--hz', type=float, default=1.0)
    parser.add_argument('--cpu', type=int)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if platform.system() != 'Linux':
        parser.error('Linux is required')
    if not math.isfinite(args.seconds) or not 0 < args.seconds <= 600:
        parser.error('--seconds must be in (0, 600]')
    if not math.isfinite(args.hz) or not 0 < args.hz <= 10:
        parser.error('--hz must be in (0, 10]; default 1 avoids unnecessary interference')
    if any(pid <= 0 for pid in args.pid):
        parser.error('PIDs must be positive')
    if args.cpu is not None:
        os.sched_setaffinity(0, {args.cpu})
    args.output.mkdir(parents=True, exist_ok=False)
    provenance = {'kind': 'read_only_proc_counters', 'not_a_syscall_trace': True,
                  'python': platform.python_version(), 'kernel': platform.release(),
                  'clock_ticks': os.sysconf('SC_CLK_TCK'), 'page_size': os.sysconf('SC_PAGE_SIZE'),
                  'pids': args.pid, 'observer_affinity': sorted(os.sched_getaffinity(0)),
                  'seconds': args.seconds, 'hz': args.hz,
                  'aslr': read(Path('/proc/sys/kernel/randomize_va_space'))}
    (args.output / 'provenance.json').write_text(json.dumps(provenance, indent=2))
    for pid in args.pid:
        maps = read(Path(f'/proc/{pid}/maps'))
        (args.output / f'maps-{pid}-start.txt').write_text(maps or 'unreadable\n')
    start = time.monotonic()
    count = 0
    with (args.output / 'samples.jsonl').open('w') as stream:
        while True:
            now = time.monotonic()
            row = {'monotonic_ns': time.monotonic_ns(), 'realtime_ns': time.time_ns(),
                   'elapsed_s': now-start, 'processes': [pid_snapshot(pid) for pid in args.pid]}
            stream.write(json.dumps(row, separators=(',', ':'))+'\n')
            stream.flush()
            count += 1
            if now-start >= args.seconds:
                break
            deadline = min(start + args.seconds, start + count/args.hz)
            time.sleep(max(0, deadline-time.monotonic()))
    for pid in args.pid:
        maps = read(Path(f'/proc/{pid}/maps'))
        (args.output / f'maps-{pid}-end.txt').write_text(maps or 'unreadable\n')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
