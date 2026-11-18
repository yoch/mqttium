#!/usr/bin/env python3
"""Build isolated product mutations and require their exact business verdicts."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

CASES = (
    ('discard-retain-bit', 'mqttiumd/src/codec.rs',
     'retain: flags & 0x20 != 0,', 'retain: false,',
     'no-targets', 'retained_will_flag_lost_at_open'),
    ('skip-retained-without-targets', 'mqttiumd-store-sqlite/src/lib.rs',
     'if retained_mask.is_some() && length > 0 && !expired {',
     'if retained_mask.is_some() && length > 0 && !expired && (admit.will_source.is_none() || !targets.is_empty()) {',
     'no-targets', 'will_without_targets_missing_retained_value'),
    ('release-owed-previous-payload', 'mqttiumd-store-sqlite/src/retained.rs',
     'if refs == 0 {', 'if refs <= 1 {',
     'routing', 'retained_will_replacement_freed_owed_payload'),
    ('leave-committed-will-pending', 'mqttiumd-store-sqlite/src/will.rs',
     'DELETE FROM wills WHERE id=?1 AND state=2', 'UPDATE wills SET state=2 WHERE id=?1 AND state=2',
     'lost-admission', 'retained_will_lost_reply_left_pending_intent'),
    ('allow-reconnect-before-will', 'mqttiumd/src/runtime.rs',
     'waiting.awaiting = false;\n                    if pending {',
     'waiting.awaiting = false;\n                    if pending && false {',
     'pressure', 'retained_will_reconnect_overtook_pending_write'),
)


def main(args):
    daemon = Path(__file__).resolve().parents[1]
    repo = daemon.parent
    out = args.out.resolve()
    if out.is_relative_to(repo) or subprocess.check_output(['git', 'status', '--porcelain'], cwd=repo).strip():
        raise ValueError('use a clean checkout and an output directory outside it')
    out.mkdir(parents=True)
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
    env = dict(os.environ, CARGO_PROFILE_DEV_DEBUG='0', CARGO_INCREMENTAL='0')
    target = args.target.resolve()
    manifest = {'source_revision': sha, 'status': 'FAIL', 'mutants': []}

    def save():
        (out / 'receipt.json').write_text(json.dumps(manifest, indent=2) + '\n')

    with tempfile.TemporaryDirectory(prefix='mqttiumd-retained-will-mutant-') as temporary:
        worktree = Path(temporary) / 'repo'
        subprocess.run(['git', 'worktree', 'add', '--detach', str(worktree), sha], cwd=repo, check=True)
        try:
            for name, filename, original, mutated, case, reason in CASES:
                directory = out / name
                directory.mkdir()
                path = worktree / 'mqttiumd/crates' / filename
                source = path.read_text()
                if source.count(original) != 1:
                    raise ValueError(f'{name} injection site changed')
                changed = source.replace(original, mutated, 1)
                path.write_text(changed)
                try:
                    hooks = case == 'lost-admission'
                    build = ['cargo', 'build', '--locked', '-p', 'mqttiumd', '--target-dir', str(target)]
                    if hooks:
                        build += ['--features', 'test-hooks']
                    with (directory / 'build.log').open('w') as log:
                        subprocess.run(build, cwd=worktree / 'mqttiumd', env=env, stdout=log,
                                       stderr=subprocess.STDOUT, check=True, timeout=180)
                    binary = target / 'debug/mqttiumd'
                    args_cli = ['--binary', str(binary), '--out', str(directory / 'exercise'), '--case', case]
                    if hooks:
                        driver = 'will_boundaries.py'
                        args_cli += ['--binary-source-head', sha, '--retained']
                    else:
                        driver = 'retained_will_reception.py'
                        args_cli += ['--source', sha]
                    with (directory / 'exercise.log').open('w') as log:
                        status = subprocess.run([sys.executable, str(daemon / 'tests/system' / driver), *args_cli],
                                                stdout=log, stderr=subprocess.STDOUT, timeout=35).returncode
                    receipt = json.loads((directory / 'exercise/receipt.json').read_text())
                    verdict = receipt.get('failure', {}).get('code')
                    record = {'name': name, 'case': case, 'exit': status, 'expected': reason, 'observed': verdict,
                              'detected': status == 1 and verdict == reason,
                              'mutated_file_sha256': hashlib.sha256(changed.encode()).hexdigest(),
                              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                              'features': ['test-hooks'] if hooks else []}
                    manifest['mutants'].append(record)
                    save()
                    if not record['detected']:
                        raise ValueError(f'{name} expected {reason}, observed {verdict}')
                    print(f'{name} detected {verdict}', flush=True)
                finally:
                    path.write_text(source)
            manifest['status'] = 'PASS'
            save()
        finally:
            subprocess.run(['git', 'worktree', 'remove', str(worktree)], cwd=repo, check=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('out', type=Path)
    parser.add_argument('--target', type=Path, required=True)
    main(parser.parse_args())
