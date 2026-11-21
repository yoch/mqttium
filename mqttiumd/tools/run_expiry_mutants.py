#!/usr/bin/env python3
"""Compile finite-expiry controls in one private, source-pinned worktree.

Usage from mqttiumd/: python tools/run_expiry_mutants.py NEW_OUTPUT_DIRECTORY
The caller must be clean. Targets stay outside the receipt directory; optional
MQTTIUMD_EXPIRY_MUTANT_TARGET_ROOT preserves an explicitly selected build cache.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def execute(command: list[str], cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, timeout=900)


def git(root: Path, *arguments: str) -> str:
    result = execute(['git', *arguments], root)
    if result.returncode:
        raise RuntimeError(f'git {arguments[0]} failed: {result.stdout}')
    return result.stdout.strip()


def test_binary(output: str, root: Path) -> dict[str, object]:
    found = re.findall(r'^\s*Running [^\n]*?\(([^\n)]+)\)\s*$', output, re.MULTILINE)
    if len(found) != 1:
        raise RuntimeError('one actual test binary must execute')
    path = Path(found[0])
    if not path.is_absolute():
        path = root / path
    return {'path': str(path), 'sha256': digest(path), 'size': path.stat().st_size}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    caller = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    if output == caller or caller in output.parents:
        raise SystemExit('output must be outside the checkout')
    if git(caller, 'status', '--porcelain', '--untracked-files=normal'):
        raise SystemExit('expiry mutations require a clean source tree')
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise SystemExit('output must be empty')
    source = git(caller, 'rev-parse', 'HEAD')
    tree = git(caller, 'rev-parse', 'HEAD^{tree}')
    catalog = caller / 'mqttiumd/tests/mutants/expiry/expected.json'
    entries = json.loads(catalog.read_text())['mutants']
    receipt: dict[str, object] = {
        'format': 'mqttiumd.finite-expiry-mutation-reception', 'schema_version': 1,
        'status': 'FAIL', 'qualification': 'implementation-test-only',
        'source_head': source, 'source_tree': tree,
        'expected_sha256': digest(catalog), 'mutants': [], 'restored': False,
    }
    start = time.monotonic()
    error: str | None = None
    with tempfile.TemporaryDirectory(prefix='mqttiumd-expiry-mutations-') as scratch:
        parent = Path(scratch)
        root = parent / 'source'
        cache = Path(os.environ.get('MQTTIUMD_EXPIRY_MUTANT_TARGET_ROOT', str(parent / 'target'))).resolve()
        if cache == output or output in cache.parents:
            raise SystemExit('Cargo cache must be outside the mutation receipt')
        env = dict(os.environ)
        env.update({'CARGO_BUILD_JOBS': '1', 'CARGO_INCREMENTAL': '0', 'CARGO_TARGET_DIR': str(cache)})
        env.pop('CARGO_ENCODED_RUSTFLAGS', None)
        env.pop('RUSTFLAGS', None)
        receipt['build'] = {'CARGO_BUILD_JOBS': 1, 'CARGO_INCREMENTAL': 0,
                            'target': str(cache), 'persistent_cache': 'MQTTIUMD_EXPIRY_MUTANT_TARGET_ROOT' in os.environ}
        git(caller, 'worktree', 'add', '--detach', str(root), source)
        try:
            if digest(root / 'mqttiumd/tests/mutants/expiry/expected.json') != receipt['expected_sha256']:
                raise RuntimeError('mutation catalog differs from the pinned source')
            for entry in entries:
                name = entry['name']
                row: dict[str, object] = {'name': name, 'reason': entry['reason'], 'detected': False}
                receipt['mutants'].append(row)
                command = ['cargo', 'test', '--manifest-path', 'mqttiumd/Cargo.toml', '--locked',
                           '-p', entry['package'], *entry['target'], entry['test'], '--', '--exact', '--nocapture']
                row['command'] = command
                row['source_sha256'] = digest(root / entry['source'])
                if git(root, 'status', '--porcelain', '--untracked-files=normal'):
                    raise RuntimeError('private tree was not restored before the next positive')
                positive = execute(command, root, env)
                (output / f'{name}.positive.log').write_text(positive.stdout)
                row['positive_exit'] = positive.returncode
                if positive.returncode or 'test result: ok. 1 passed;' not in positive.stdout:
                    raise RuntimeError(f'{name}: the exact positive did not pass')
                row['positive_binary'] = test_binary(positive.stdout, root)
                patch = root / 'mqttiumd/tests/mutants/expiry' / entry['patch']
                (output / entry['patch']).write_bytes(patch.read_bytes())
                row['patch_sha256'] = digest(patch)
                git(root, 'apply', '--check', str(patch))
                git(root, 'apply', str(patch))
                try:
                    mutation = execute(command, root, env)
                    (output / f'{name}.mutant.log').write_text(mutation.stdout)
                    row['mutant_exit'] = mutation.returncode
                    compiled = 'could not compile' not in mutation.stdout and 'error[E' not in mutation.stdout
                    row['detected'] = (mutation.returncode == 101 and compiled
                                       and entry['reason'] in mutation.stdout
                                       and 'test result: FAILED. 0 passed; 1 failed;' in mutation.stdout)
                    if compiled and 'test result: FAILED.' in mutation.stdout:
                        row['mutant_binary'] = test_binary(mutation.stdout, root)
                    if not row['detected']:
                        raise RuntimeError(f'{name}: required property was not detected')
                finally:
                    # This is the private tree, with no concurrent/user edits.
                    git(root, 'restore', '--', entry['source'])
                row['restored'] = not git(root, 'status', '--porcelain', '--untracked-files=normal')
                if not row['restored']:
                    raise RuntimeError(f'{name}: private tree or generated files were not restored')
                print(f"{name}: detected {entry['reason']}", flush=True)
            receipt['restored'] = not git(root, 'status', '--porcelain', '--untracked-files=normal')
            receipt['status'] = 'PASS' if receipt['restored'] and all(row['detected'] for row in receipt['mutants']) else 'FAIL'
        except (RuntimeError, OSError, subprocess.TimeoutExpired) as failure:
            error = str(failure)
            receipt['error'] = error
        finally:
            # Preserve the final status before removing our isolated source.
            receipt['tree_status_before_cleanup'] = git(root, 'status', '--porcelain', '--untracked-files=normal')
            git(caller, 'worktree', 'remove', '--force', str(root))
    receipt['elapsed_seconds'] = round(time.monotonic() - start, 3)
    (output / 'result.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    files = sorted(path for path in output.rglob('*') if path.is_file())
    (output / 'SHA256SUMS').write_text(''.join(f'{digest(path)}  {path.relative_to(output)}\n' for path in files))
    if error:
        print(error)
    print(json.dumps({'status': receipt['status'], 'source_head': source,
                      'receipt': str(output / 'result.json')}))
    return 0 if receipt['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
