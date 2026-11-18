#!/usr/bin/env python3
"""Compile five retained controls in a private, source-pinned worktree.

Run from mqttiumd/: python tests/system/retained_mutants.py NEW_OUTPUT_DIRECTORY.
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


CATALOG = 'mqttiumd/tests/system/retained_mutants.py'
CORE = 'mqttiumd/crates/mqttiumd-core/src/owner.rs'
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


CLEAR_SITE = '''    let inventory = snapshot(conn, meta, ReadBudget::SNAPSHOT)?;
    if new_length > 0'''
CLEAR_CANCEL = '''    // Mutation: clearing a retained topic wrongly cancels prior recipients.
    if new_length == 0 {
        let deliveries = {
            let mut statement = conn.prepare_cached(
                "SELECT o.publication,o.idx FROM obligations o JOIN publications p ON p.id=o.publication WHERE p.topic=?1 ORDER BY o.publication,o.idx",
            )?;
            let rows = statement.query_map([topic], |row| {
                Ok((reader::copy_blob(row, 0, 32)?, row.get::<_, i64>(1)?))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        for (publication, index) in deliveries {
            end_obligation(conn, meta, &publication, index).map_err(|_| Abort::Fault)?;
        }
    }
    let inventory = snapshot(conn, meta, ReadBudget::SNAPSHOT)?;
    if new_length > 0'''
RELEASE_SITE = '''    conn.prepare_cached("DELETE FROM inbound WHERE session = ?1 AND packet_id = ?2")?'''
RELEASE_REMUTATE = '''    // Mutation: an old PUBREL wrongly restores its original retained value.
    let (topic, source, payload, length, references) = conn.prepare_cached(
        "SELECT p.topic,p.source,p.payload,y.length,y.refs FROM publications p JOIN payloads y ON y.id=p.payload WHERE p.id=?1",
    )?.query_row([publication.as_slice()], |row| Ok((
        reader::copy_blob(row, 0, meta.limits.max_topic_bytes)?,
        reader::copy_blob(row, 1, 8)?, reader::copy_blob(row, 2, 32)?,
        reader::copy_blob(row, 3, 8)?, reader::copy_blob(row, 4, 8)?,
    )))?;
    retained::prepare_mutation(conn, meta, &topic, blob_u64(&length)?)?;
    let references = blob_u64(&references)?.checked_add(1).ok_or(Abort::Fault)?;
    conn.prepare_cached("UPDATE payloads SET refs=?2 WHERE id=?1")?
        .execute(params![payload, u64_blob(references).as_slice()])?;
    charge(meta, Accounts { references: References::new(1), ..Accounts::default() })?;
    retained::install(conn, &topic,
        PublicationId::from_be_slice(&publication).map_err(|_| Abort::Fault)?,
        SessionId::new(blob_u64(&source)?), None)?;
    conn.prepare_cached("DELETE FROM inbound WHERE session = ?1 AND packet_id = ?2")?'''
WORK_JOIN = 'LEFT JOIN payloads y ON y.id=p.payload LEFT JOIN outbound c'
WORK_CURRENT = ('LEFT JOIN payloads y ON y.id=CASE WHEN p.retained_version IS NOT NULL '
                'THEN COALESCE((SELECT current.payload FROM retained r JOIN publications current '
                'ON current.id=r.publication WHERE r.topic=p.topic),p.payload) ELSE p.payload END '
                'LEFT JOIN outbound c')
CONTROLS = (
    Control('retained-needs-live-target', CORE, 'mqttiumd-core', ('--lib',),
            'owner::tests::a_product_retained_publish_is_a_durable_admission_even_without_live_subscribers',
            'retained_publish_did_not_enter_durable_admission',
            '        let targets = selected.len();\n        if !fits(topic.len(), limits.max_topic_bytes)',
            '        let targets = selected.len();\n        let retain = &(*retain && targets != 0);\n        if !fits(topic.len(), limits.max_topic_bytes)'),
    Control('replay-rereads-current', SQLITE + 'pubsub.rs', 'mqttiumd-store-sqlite', ('--test', 'retained'),
            'retained_replacement_clear_and_replay_freeze', 'replay_reread_current_retained',
            WORK_JOIN, WORK_CURRENT),
    Control('clear-cancels-owed-payload', SQLITE + 'retained.rs', 'mqttiumd-store-sqlite', ('--test', 'retained'),
            'retained_replacement_clear_and_replay_freeze', 'clear_released_owed_payload',
            CLEAR_SITE, CLEAR_CANCEL),
    Control('late-pubrel-remutates', SQLITE + 'lib.rs', 'mqttiumd-store-sqlite', ('--test', 'retained'),
            'retained_replacement_clear_and_replay_freeze', 'old_retained_operation_remutated',
            RELEASE_SITE, RELEASE_REMUTATE),
    Control('retained-no-local-name-lost', SQLITE + 'retained.rs', 'mqttiumd-store-sqlite', ('--test', 'retained'),
            'quota_full_replacement_and_no_local_client_identity', 'no_local_forgot_producer_client_id',
            'change.options.no_local() && value.producer_client_id == client',
            'change.options.no_local() && value.producer_client_id != client'),
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


def precise_failure(output: str, status: int, reason: str) -> bool:
    # A failed compilation, filtered-out test, unwrap panic or unrelated error
    # cannot establish the intended semantic assertion.
    return (status == 101
            and 'could not compile' not in output and 'error[E' not in output
            and 'test result: FAILED. 0 passed; 1 failed;' in output
            and re.search(r'(?m)^(?:assertion[^\n]*failed: )?' + re.escape(reason) + r'$', output) is not None
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
        raise SystemExit('retained mutations require a clean source tree')
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise SystemExit('output must be empty')
    source = git(caller, 'rev-parse', 'HEAD')
    tree = git(caller, 'rev-parse', 'HEAD^{tree}')
    receipt: dict[str, object] = {
        'format': 'mqttiumd.retained-mutation-reception', 'schema_version': 1,
        'status': 'FAIL', 'qualification': 'implementation-test-only',
        'source_head': source, 'source_tree': tree,
        'catalog_sha256': digest(Path(__file__).resolve()),
        'controls': [], 'restored': False,
    }
    start = time.monotonic()
    error: str | None = None
    with tempfile.TemporaryDirectory(prefix='mqttiumd-retained-mutations-') as scratch:
        parent = Path(scratch)
        root = parent / 'source'
        cache = Path(os.environ.get('MQTTIUMD_RETAINED_MUTANT_TARGET_ROOT', str(parent / 'target'))).resolve()
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
                            'persistent_cache': 'MQTTIUMD_RETAINED_MUTANT_TARGET_ROOT' in os.environ}
        git(caller, 'worktree', 'add', '--detach', str(root), source)
        try:
            if digest(root / CATALOG) != receipt['catalog_sha256']:
                raise RuntimeError('mutation catalog differs from the pinned source')
            for control in CONTROLS:
                name = control.name
                row: dict[str, object] = {
                    'name': name, 'expected_reason': control.expected_reason,
                    'observed_reason': None, 'detected': False, 'source': control.source,
                    'scope': 'pure-core-transition' if control.package == 'mqttiumd-core' else 'sqlite-store-integration',
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
                if positive.returncode or 'test result: ok. 1 passed;' not in positive.stdout:
                    raise RuntimeError(f'{name}: the exact positive did not pass')
                row['positive_binary'] = test_binary(positive.stdout, root)
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
                try:
                    mutation = execute(command, root, env)
                    (output / f'{name}.mutant.log').write_text(mutation.stdout)
                    row['mutant_status'] = mutation.returncode
                    row['detected'] = precise_failure(mutation.stdout, mutation.returncode, control.expected_reason)
                    if row['detected']:
                        row['observed_reason'] = control.expected_reason
                        row['mutant_binary'] = test_binary(mutation.stdout, root)
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
