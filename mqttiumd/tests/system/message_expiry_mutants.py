#!/usr/bin/env python3
"""Compile six Message Expiry controls in a private, source-pinned worktree.

Run from mqttiumd/: python tests/system/message_expiry_mutants.py NEW_OUTPUT_DIRECTORY.
These are implementation tests, separate from the network reception and oracle
qualification. The exact positive target must pass before its mutant is built.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time


CATALOG = 'mqttiumd/tests/system/message_expiry_mutants.py'
PRODUCT = 'mqttiumd/crates/mqttiumd/src/'
SQLITE = 'mqttiumd/crates/mqttiumd-store-sqlite/src/'


@dataclass(frozen=True)
class Control:
    name: str
    source: str
    package: str
    target: tuple[str, ...]
    test: str
    expected_reason: str
    before: str
    after: str


# Every mutation changes production code, never a test or an assertion.
REPLAY_ACCOUNT = '''        let refs: Vec<u8> = conn
            .prepare_cached("SELECT refs FROM payloads WHERE id=?1")?'''
REPLAY_RESET = '''        // Mutation: replay incorrectly starts a new publication lifetime.
        if let Some(now) = observed {
            conn.prepare_cached("UPDATE publications SET expiry_tick=?2 WHERE id=?1 AND expiry_domain IS NOT NULL")?
                .execute(params![publication.to_be_bytes().as_slice(), u64_blob(now.tick).as_slice()])?;
        }
        let refs: Vec<u8> = conn
            .prepare_cached("SELECT refs FROM payloads WHERE id=?1")?'''
CLAIM_EXCLUSION = ('AND NOT EXISTS(SELECT 1 FROM outbound c '
                   'WHERE c.publication=o.publication AND c.idx=o.idx) ORDER BY o.idx LIMIT ?2')
STARTED_ELIGIBLE = ('AND NOT EXISTS(SELECT 1 FROM outbound c '
                    'WHERE c.publication=o.publication AND c.idx=o.idx AND c.started=0) '
                    'ORDER BY o.idx LIMIT ?2')
OFFLINE_SKIPPED = ('AND NOT EXISTS(SELECT 1 FROM outbound c '
                   'WHERE c.publication=o.publication AND c.idx=o.idx) '
                   'AND NOT EXISTS(SELECT 1 FROM sessions s '
                   'WHERE s.id=o.target AND s.detached_by IS NOT NULL) ORDER BY o.idx LIMIT ?2')
ELIGIBILITY = '''                        (Some(expiry), Some(now)) => matches!(
                            expiry
                                .state(now)
                                .map_err(|_| Abort::Read(ReadError::InvalidData))?,
                            ExpiryState::Fresh { .. }
                        ),'''
UNKNOWN_FRESH = ELIGIBILITY.replace('ExpiryState::Fresh { .. }',
                                    'ExpiryState::Fresh { .. } | ExpiryState::TemporalUncertain')
FIRST_WRITE_GATE = '''        if offset == 0
            && let Some(expiry) = expiry'''
RETAINED_BYPASS = '''        if offset == 0
            && bytes[0] & 1 == 0
            && let Some(expiry) = expiry'''
CONTROLS = (
    Control('replay-refreshes-ttl', SQLITE + 'retained.rs', 'mqttiumd-store-sqlite',
            ('--test', 'message_expiry'), 'thirty_two_timed_replays_keep_original_ttl',
            'replay_refreshed_expiry_origin', REPLAY_ACCOUNT, REPLAY_RESET),
    Control('stale-retained-emission', PRODUCT + 'net.rs', 'mqttiumd', ('--lib',),
            'net::tests::expired_retained_publication_never_reaches_the_first_socket_attempt',
            'expired_retained_reached_socket', FIRST_WRITE_GATE, RETAINED_BYPASS),
    Control('expiry-frees-started-payload', SQLITE + 'message_expiry.rs', 'mqttiumd-store-sqlite',
            ('--test', 'message_expiry'), 'claimed_copies_require_exact_never_started_guard',
            'started_claim_payload_released', CLAIM_EXCLUSION, STARTED_ELIGIBLE),
    Control('offline-expiry-ignored', SQLITE + 'message_expiry.rs', 'mqttiumd-store-sqlite',
            ('--test', 'message_expiry'), 'offline_expiry_removes_pending_copies',
            'offline_expiry_ignored', CLAIM_EXCLUSION, OFFLINE_SKIPPED),
    Control('outgoing-expiry-property-corrupt', PRODUCT + 'codec.rs', 'mqttiumd',
            ('--lib',),
            'codec::tests::publication_expiry_encoding_keeps_zero_present_and_exposes_only_its_value',
            'outgoing_message_expiry_remaining_wrong',
            '        bytes.extend_from_slice(&remaining.to_be_bytes());',
            '        bytes.extend_from_slice(&remaining.saturating_sub(1).to_be_bytes());'),
    Control('unknown-continuity-fresh', SQLITE + 'retained.rs', 'mqttiumd-store-sqlite',
            ('--test', 'message_expiry'), 'temporal_quarantine_and_observation_refusals',
            'unknown_origin_replayed_as_fresh', ELIGIBILITY, UNKNOWN_FRESH),
)


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


def precise_positive(output: str, status: int, test: str) -> bool:
    """A filter matching no tests cannot establish the positive baseline."""
    return (status == 0
            and re.search(r'(?m)^running 1 test$', output) is not None
            and re.search(r'(?m)^test ' + re.escape(test) + r' \.\.\. ok$', output) is not None
            and 'test result: ok. 1 passed; 0 failed;' in output)


def precise_failure(output: str, status: int, reason: str, test: str) -> bool:
    """Accept only the named executed test's exact semantic failure marker."""
    panic = re.search(r"(?m)^thread '" + re.escape(test)
                      + r"'(?: \(\d+\))? panicked at [^\n]+:\n", output)
    if panic is None:
        return False
    # Restrict the marker to this panic's message, before another test/thread.
    message = re.split(r"(?m)^(?:thread '|test |failures:|test result:)", output[panic.end():], maxsplit=1)[0]
    return (status == 101
            and 'could not compile' not in output and 'error[E' not in output
            and re.search(r'(?m)^running 1 test$', output) is not None
            and re.search(r'(?m)^test ' + re.escape(test) + r' \.\.\. FAILED$', output) is not None
            and 'test result: FAILED. 0 passed; 1 failed;' in output
            and re.search(r'(?m)^(?:assertion[^\n]*failed: )?' + re.escape(reason) + r'$', message) is not None
            and 'called `Result::unwrap()`' not in output
            and 'called `Option::unwrap()`' not in output)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    caller = Path(__file__).resolve().parents[3]
    output = args.output.resolve()
    if output == caller or caller in output.parents:
        raise SystemExit('output must be outside the checkout')
    if git(caller, 'status', '--porcelain', '--untracked-files=normal'):
        raise SystemExit('Message Expiry mutations require a clean source tree')
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise SystemExit('output must be empty')
    source = git(caller, 'rev-parse', 'HEAD')
    tree = git(caller, 'rev-parse', 'HEAD^{tree}')
    receipt: dict[str, object] = {
        'format': 'mqttiumd.message-expiry-mutation-reception', 'schema_version': 1,
        'status': 'FAIL', 'qualification': 'implementation-test-only',
        'source_head': source, 'source_tree': tree,
        'catalog_sha256': digest(Path(__file__).resolve()),
        'controls': [], 'restored': False,
    }
    start = time.monotonic()
    error: str | None = None
    with tempfile.TemporaryDirectory(prefix='mqttiumd-message-expiry-mutations-') as scratch:
        parent = Path(scratch)
        root = parent / 'source'
        cache = Path(os.environ.get('MQTTIUMD_MESSAGE_EXPIRY_MUTANT_TARGET_ROOT', str(parent / 'target'))).resolve()
        if cache == output or output in cache.parents or cache == caller or caller in cache.parents:
            raise SystemExit('Cargo cache must be outside the receipt and source checkout')
        env = dict(os.environ)
        env.update({'CARGO_BUILD_JOBS': '1', 'CARGO_INCREMENTAL': '0',
                    'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0',
                    'CARGO_TARGET_DIR': str(cache)})
        env.pop('CARGO_ENCODED_RUSTFLAGS', None)
        env.pop('RUSTFLAGS', None)
        receipt['build'] = {'jobs': 1, 'incremental': 0, 'dev_debug': 0, 'test_debug': 0,
                            'target': str(cache), 'requested_features': [],
                            'persistent_cache': 'MQTTIUMD_MESSAGE_EXPIRY_MUTANT_TARGET_ROOT' in os.environ}
        git(caller, 'worktree', 'add', '--detach', str(root), source)
        try:
            if digest(root / CATALOG) != receipt['catalog_sha256']:
                raise RuntimeError('mutation catalog differs from the pinned source')
            for control in CONTROLS:
                name = control.name
                row: dict[str, object] = {
                    'name': name, 'expected_reason': control.expected_reason,
                    'observed_reason': None, 'detected': False, 'source': control.source,
                    'test': control.test, 'package': control.package,
                    'positive_executed': False, 'mutant_executed': False, 'restored': False,
                    'scope': 'product-writer-or-codec' if control.package == 'mqttiumd' else 'sqlite-store-integration',
                }
                receipt['controls'].append(row)
                command = ['cargo', 'test', '--manifest-path', 'mqttiumd/Cargo.toml', '--locked',
                           '--no-default-features', '-p', control.package, *control.target,
                           control.test, '--', '--exact', '--nocapture']
                row['command'] = command
                path = root / control.source
                row['source_sha256'] = digest(path)
                if git(root, 'status', '--porcelain', '--untracked-files=normal'):
                    raise RuntimeError('private tree was not restored before the next positive')
                positive = execute(command, root, env)
                (output / f'{name}.positive.log').write_text(positive.stdout)
                row['positive_status'] = positive.returncode
                if not precise_positive(positive.stdout, positive.returncode, control.test):
                    raise RuntimeError(f'{name}: the exact positive did not pass')
                row['positive_binary'] = test_binary(positive.stdout, root)
                row['positive_executed'] = True
                original = path.read_text()
                if original.count(control.before) != 1:
                    raise RuntimeError(f'{name}: one exact mutation site required')
                modified = original.replace(control.before, control.after)
                patch = output / f'{name}.mutant'
                patch.write_text(''.join(difflib.unified_diff(
                    original.splitlines(keepends=True), modified.splitlines(keepends=True),
                    fromfile='a/' + control.source, tofile='b/' + control.source)))
                row['patch_sha256'] = digest(patch)
                git(root, 'apply', '--check', str(patch))
                git(root, 'apply', str(patch))
                row['mutated_source_sha256'] = digest(path)
                try:
                    mutation = execute(command, root, env)
                    (output / f'{name}.mutant.log').write_text(mutation.stdout)
                    row['mutant_status'] = mutation.returncode
                    row['detected'] = precise_failure(mutation.stdout, mutation.returncode, control.expected_reason, control.test)
                    if row['detected']:
                        row['observed_reason'] = control.expected_reason
                        row['mutant_binary'] = test_binary(mutation.stdout, root)
                        row['mutant_executed'] = True
                        if row['mutant_binary']['sha256'] == row['positive_binary']['sha256']:
                            row['detected'] = False
                            raise RuntimeError(f'{name}: mutant did not produce a distinct test binary')
                finally:
                    # This private tree has no concurrent or user edits.
                    git(root, 'restore', '--', control.source)
                    row['restored'] = (not git(root, 'status', '--porcelain', '--untracked-files=normal')
                                       and digest(path) == row['source_sha256'])
                if not row['detected']:
                    raise RuntimeError(f'{name}: required property was not detected')
                if not row['restored']:
                    raise RuntimeError(f'{name}: private tree or generated files were not restored')
                print(f'{name}: detected {control.expected_reason}', flush=True)
            receipt['restored'] = not git(root, 'status', '--porcelain', '--untracked-files=normal')
            receipt['status'] = 'PASS' if receipt['restored'] and all(row['detected'] for row in receipt['controls']) else 'FAIL'
        except (RuntimeError, OSError, subprocess.TimeoutExpired) as failure:
            error = str(failure)
            receipt['error'] = error
        finally:
            receipt['tree_status_before_cleanup'] = git(root, 'status', '--porcelain', '--untracked-files=normal')
            receipt['restored'] = not receipt['tree_status_before_cleanup']
            git(caller, 'worktree', 'remove', '--force', str(root))
    receipt['elapsed_seconds'] = round(time.monotonic() - start, 3)
    document = json.dumps(receipt, indent=2, sort_keys=True) + '\n'
    (output / 'result.json').write_text(document)
    (output / 'index.json').write_text(document)
    files = sorted(path for path in output.rglob('*') if path.is_file())
    (output / 'SHA256SUMS').write_text(''.join(f'{digest(path)}  {path.relative_to(output)}\n' for path in files))
    if error:
        print(error)
    print(json.dumps({'status': receipt['status'], 'source_head': source, 'receipt': str(output / 'result.json')}))
    return 0 if receipt['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
