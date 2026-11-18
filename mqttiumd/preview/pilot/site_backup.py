"""Quiescent coordinated site backup with a fenced same-stream handoff."""
from __future__ import annotations

from contextlib import ExitStack, closing
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import uuid

from common import inventory, read_db, require, sha
from forward_store import backup as copy_outbox, snapshot
from segment_admin import copy_sqlite_file, publish_copy as copy_ledger, restore as restore_ledger
from segment_store import sync_directory
from site_ops import (BROKERS, CONSUMERS, DATABASES, PRODUCERS, Site, atomic_json, document, layout, locked,
                      policy_identity, private_directory, seal_site)


FORMAT = "mqttiumd.private-site-backup.v1"
IMAGE_LIMIT = 1024**3


def signature(path):
    fields = {}
    for suffix in ("", "-wal"):
        item = Path(str(path) + suffix)
        if item.exists() and item.stat().st_size:
            require(item.is_file() and not item.is_symlink() and item.stat().st_size <= IMAGE_LIMIT,
                    f"snapshot_size_or_path_invalid:{item.name}")
            fields[suffix or "db"] = {"bytes": item.stat().st_size, "sha256": sha(item)}
    require("db" in fields, f"missing_durable_source:{path.name}")
    return fields


def writer_path(site, role):
    suffix = ".writer-lock" if role in BROKERS else ".lock"
    return Path(str(site.db(role)) + suffix)


def stable_signatures(site):
    with ExitStack() as stack:
        for role in DATABASES:
            stack.enter_context(locked(writer_path(site, role)))
            site.assert_store(role)
        return {role: signature(site.db(role)) for role in DATABASES}


def stopped(site):
    require(site.status()["phase"] == "stopped", "site_must_stop_cleanly_before_snapshot")


def sqlite_image(source, target):
    with read_db(source) as db, closing(sqlite3.connect(target)) as out:
        db.backup(out)
        require(out.execute("PRAGMA quick_check").fetchone()[0] == "ok", "sqlite_snapshot_integrity")
    os.chmod(target, 0o600)
    with target.open("rb") as file:
        os.fsync(file.fileno())


def evidence(site):
    sources = {}
    for role in PRODUCERS:
        state = snapshot(site.db(role))
        sources[role] = {k: state[k] for k in ("stream", "enqueued", "confirmed", "pending", "unknown", "attempts")}
    ledgers = {}
    for role in CONSUMERS:
        info = snapshot(site.db(role))
        with read_db(site.db(role)) as db:
            rows = db.execute("SELECT sensor,hex(stream),archived FROM streams LIMIT 65").fetchall()
            require(len(rows) <= 64, "invalid_history_stream_budget")
        ledgers[role] = {"unique_applied": info["unique_count"], "registered_segments": info["segment_count"],
                         "archived": {f"{s}/{seq.lower()}": high for s, seq, high in rows}}
    return {"sources": sources, "ledgers": ledgers,
            "brokers": {role: inventory(site.db(role)) for role in BROKERS}}


def backup(site, target):
    target = target.absolute()
    private_directory(target.parent)
    require(not target.exists(), "backup_target_already_exists")
    with locked(site.root / "site.lock"):
        stopped(site)
        site.check()
        site.allow_run()
        before = evidence(site)
        manifest = {"format": FORMAT, "backup_id": uuid.uuid4().hex, "created_ns": time.time_ns(),
                    "provenance": document(site.package / "BUILD.json"),
                    "snapshot_kind": "quiescent-independent-sqlite-backups", "site": site.config,
                    "frontiers": before, "shutdown": site.status().get("shutdown"), "roles": {}}
        with tempfile.TemporaryDirectory(prefix=".site-backup-", dir=target.parent) as temporary:
            folder = Path(temporary) / "sealed"
            folder.mkdir(mode=0o700)
            for role in BROKERS:
                with locked(writer_path(site, role)):
                    path = folder / f"{role}.sqlite"
                    sqlite_image(site.db(role), path)
                manifest["roles"][role] = {"sqlite": signature(path)}
            for role in PRODUCERS:
                path = folder / f"{role}.sqlite"
                copy_outbox(site.db(role), path)
                with path.open("rb") as file:
                    os.fsync(file.fileno())
                manifest["roles"][role] = {"sqlite": signature(path)}
            for role in CONSUMERS:
                manifest["roles"][role] = {"ledger": copy_ledger(site.db(role), folder / role)}
            require(evidence(site) == before, "site_changed_during_snapshot")
            manifest["source_signatures"] = stable_signatures(site)
            policies = folder / "policies"
            policies.mkdir(mode=0o700)
            for lane in ("live", "history"):
                atomic_json(policies / (lane + ".json"), document(site.pki(lane) / "identities.json"), durable=True)
            atomic_json(folder / "MANIFEST.json", manifest, durable=True)
            folder.rename(target)
            sync_directory(target.parent)
        result = {"backup_id": manifest["backup_id"], "created_ns": manifest["created_ns"],
                  "manifest_sha256": sha(target / "MANIFEST.json"), "frontiers": before, "path": str(target)}
        atomic_json(site.runtime / "backup.json", result, durable=True)
        return result


def restore(bundle, root, credentials, package):
    root = root.absolute()
    private_directory(root.parent)
    private_directory(bundle)
    require(not root.exists(), "restore_site_requires_new_directory")
    require(not credentials.is_symlink(), "restore_credentials_alias")
    credentials = credentials.resolve(strict=True)
    manifest_path = bundle / "MANIFEST.json"
    manifest = document(manifest_path)
    require(manifest["format"] == FORMAT and set(manifest["roles"]) == set(DATABASES),
            "unsupported_site_backup")
    config = manifest["site"]
    require(policy_identity(credentials) == config["credentials"], "backup_credentials_do_not_match")
    for lane in ("live", "history"):
        require(document(bundle / "policies" / (lane + ".json")) ==
                document(credentials / lane / "identities.json"), "backup_policy_does_not_match")
    with tempfile.TemporaryDirectory(prefix=".site-restore-", dir=root.parent) as temporary:
        held = Path(temporary) / "restored"
        layout(held)
        for role in (*BROKERS, *PRODUCERS):
            name = role + ".sqlite"
            source = bundle / name
            expected = manifest["roles"][role]["sqlite"]
            require(set(expected) == {"db"} and signature(source) == expected, f"backup_sqlite_digest:{role}")
            target = held / "data" / name
            copy_sqlite_file(source, target, expected["db"]["bytes"])
            require(signature(target) == expected, f"backup_image_changed_during_restore:{role}")
        for role in CONSUMERS:
            require(document(bundle / role / "MANIFEST.json") == manifest["roles"][role]["ledger"],
                    f"backup_ledger_manifest:{role}")
            stage = Path(temporary) / role
            restore_ledger(bundle / role, stage)
            data = held / "data"
            (stage / "history.sqlite").rename(data / f"{role}.sqlite")
            (stage / "history.sqlite.segments").rename(data / f"{role}.sqlite.segments")
        sync_directory(held / "data")
        seal_site(held, config, credentials)
        staged = Site(held, package)
        staged.check()
        require(evidence(staged) == manifest["frontiers"], "restored_frontiers_diverge")
        marker = {"backup_id": manifest["backup_id"], "manifest_sha256": sha(manifest_path),
                  "source_signatures": manifest["source_signatures"], "target_signatures": stable_signatures(staged),
                  "activated": False}
        atomic_json(held / "restore.json", marker, durable=True)
        atomic_json(staged.runtime / "status.json", {"phase": "stopped", "restored_ns": time.time_ns()}, durable=True)
        held.rename(root)
        sync_directory(root.parent)
    return {"root": str(root), "backup_id": marker["backup_id"], "activated": False}


def handoff(source, target):
    require(source.root != target.root and source.config == target.config, "different_site_authorities")
    with ExitStack() as stack:
        for site in sorted((source, target), key=lambda s: s.root):
            stack.enter_context(locked(site.root / "site.lock"))
            stopped(site)
            site.check()
        path = target.root / "restore.json"
        copy = document(path)
        fence = {"backup_id": copy["backup_id"], "target": str(target.root),
                 "manifest_sha256": copy["manifest_sha256"]}
        retired = source.root / "retired.json"
        if retired.exists():
            require(document(retired) == fence, "source_already_fenced_for_other_site")
        if copy["activated"]:
            require(retired.exists() and copy["fenced_source"] == str(source.root), "invalid_activation_receipt")
            return fence
        require(stable_signatures(source) == copy["source_signatures"], "backup_stale_or_source_changed_reconcile")
        require(stable_signatures(target) == copy["target_signatures"], "restored_site_changed_reconcile")
        # After a crash between these two publications only the identical handoff
        # can finish. There is no point at which both supervisors can start.
        atomic_json(retired, fence, durable=True)
        atomic_json(path, {**copy, "activated": True, "fenced_source": str(source.root)}, durable=True)
    return fence
