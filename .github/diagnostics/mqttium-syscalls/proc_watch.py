"""One-Hz sidecar: only registered benchmark PIDs and their descendants.

No process environment or memory reads. Main-TID and per-TID switches have
separate fields. RSS and faults are counters, not a syscall trace.
"""
from __future__ import annotations
import argparse
import json
import os
import time
from pathlib import Path
from collect_linux import pid_snapshot, read


def descendants(pid: int, seen: set[int]) -> None:
    if pid in seen or len(seen) >= 128:
        return
    seen.add(pid)
    try:
        task_dirs = list(Path(f'/proc/{pid}/task').iterdir())
    except OSError:
        return
    for task in task_dirs:
        for child in (read(task / 'children') or '').split():
            descendants(int(child), seen)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--registry', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--stop', type=Path, required=True)
    p.add_argument('--seconds', type=float, default=180)
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)
    saved = set()
    start = time.monotonic()
    with (a.output / 'samples.jsonl').open('w') as stream:
        while time.monotonic() - start < a.seconds:
            roots = {}
            for line in (read(a.registry) or '').splitlines():
                try:
                    row = json.loads(line)
                    roots[int(row['pid'])] = row['role']
                except (ValueError, KeyError):
                    continue
            pids: set[int] = set()
            for pid in roots:
                descendants(pid, pids)
            records = []
            for pid in sorted(pids):
                snap = pid_snapshot(pid)
                snap['role'] = roots.get(pid, 'descendant')
                records.append(snap)
                if not snap.get('readable'):
                    continue
                # Capture initial and coarse follow-up maps/fds after imports.
                bucket = int((time.monotonic() - start) // 5)
                identity = (pid, snap['starttime_ticks'], bucket)
                if identity not in saved:
                    saved.add(identity)
                    folder = a.output / f'{pid}-{snap["starttime_ticks"]}-{bucket}'
                    folder.mkdir()
                    for name in ('maps', 'cmdline', 'comm'):
                        value = read(Path(f'/proc/{pid}/{name}'))
                        (folder / (name + '.txt')).write_text((value or 'unreadable').replace('\x00', ' '))
                    links = {}
                    try:
                        for fd in list(Path(f'/proc/{pid}/fd').iterdir())[:128]:
                            try:
                                links[fd.name] = os.readlink(fd)
                            except OSError:
                                pass
                    except OSError:
                        pass
                    (folder / 'fd.json').write_text(json.dumps(links, indent=2))
            stream.write(json.dumps({'monotonic_ns': time.monotonic_ns(),
                'perf_counter_ns': time.perf_counter_ns(), 'realtime_ns': time.time_ns(),
                'processes': records}, separators=(',', ':')) + '\n')
            stream.flush()
            if a.stop.exists():
                break
            time.sleep(1)

if __name__ == '__main__':
    main()
