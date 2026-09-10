"""Shared immutable experiment identities and bounded process helpers."""
from __future__ import annotations
import hashlib
import json
import os
import signal
import subprocess
from pathlib import Path

REPO = 'yoch/mqttium'
PR = 454
BASE = '9ad1f01857306ac5079ffb1d073a59fdb60e1931'
HEAD = '636de564dc2c92822a8361050cc24e369fe027d4'
HARNESS_REPO = 'yoch/mqtt-python-client-bench'
HARNESS = 'ec332e25003a6484727697c3018ce632a025a7f2'
BASE_SRC = 'd4685d3e24167a5c71306acdf268629bc95f52c4'
HEAD_SRC = '699b2dcd66ccae2d358e0c8aa740a7fe38933131'
CALLS = {
    'memory': 'brk,mmap,mremap,munmap,madvise',
    # Regex accepts both architecture variants without naming unsupported syscalls.
    'flow': '/^(read|readv|write|writev|recv.*|send.*|epoll_.*|poll|ppoll|select|pselect6|futex.*|clock_nanosleep|nanosleep|sched_yield|socket|socketpair|connect|accept|accept4|bind|listen|shutdown|close|fcntl|ioctl|getsockopt|setsockopt)$',
}

def dump(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    temp.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    temp.replace(path)

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()

def git(root: Path, *args: str) -> str:
    return subprocess.check_output(['git', '-C', str(root), *args], text=True).strip()

def verify_source(root: Path, sha: str, subtree: str | None = None) -> None:
    if git(root, 'rev-parse', 'HEAD') != sha:
        raise RuntimeError(f'Unexpected HEAD in {root}')
    if git(root, 'status', '--porcelain', '--untracked-files=no'):
        raise RuntimeError(f'Tracked changes in {root}; refusing measurement')
    if subtree and git(root, 'rev-parse', 'HEAD:src/mqttium') != subtree:
        raise RuntimeError(f'Unexpected runtime tree in {root}')

def trace_command(mode: str, path: Path, command: list[str], strace: str = 'strace') -> list[str]:
    # -D preserves Popen.pid as the actual tracee PID. Preflight proves this on
    # the runner. NO -f: the external pacer (child of initiator) stays untraced.
    # Main-TID trace only. No seccomp-bpf: it requires -f, which changes that scope.
    return [strace, '-D', '-I', '1', '-ttt', '-T', '-yy', '-xx', '-s', '96' if mode == 'flow' else '0',
            '-e', 'trace=' + CALLS[mode], '-o', str(path), *command]

def terminate_group(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
