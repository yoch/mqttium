#!/usr/bin/env python3
"""Seal current operator material around the exact qualified CI executable."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

from build_preview import copy_pilot_files


FORWARD_COMPONENTS = ("forward_store.py", "forward_client.py", "forward_reception.py", "common.py", "load.py")
SPLIT_COMPONENTS = (*FORWARD_COMPONENTS, "split_reception.py", "split_client.py", "segment_store.py")


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def package(args):
    root = Path(__file__).resolve().parents[1]
    if digest(args.archive) != args.archive_sha256:
        raise ValueError("frozen_archive_digest_mismatch")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=root).strip():
        raise ValueError("package_source_must_be_clean")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    args.out.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="mqttiumd-sealed-pilot-") as temp:
        extracted = Path(temp)
        with tarfile.open(args.archive) as archive:
            members = archive.getmembers()
            if len(members) > 512 or sum(m.size for m in members) > 256 * 1024 * 1024:
                raise ValueError("frozen_archive_budget")
            if any(m.issym() or m.islnk() or Path(m.name).is_absolute() or ".." in Path(m.name).parts for m in members):
                raise ValueError("frozen_archive_unsafe_member")
            archive.extractall(extracted, filter="data")
        roots = list(extracted.iterdir())
        if len(roots) != 1 or not roots[0].is_dir():
            raise ValueError("frozen_archive_root")
        original = roots[0]
        for row in (original / "SHA256SUMS").read_text().splitlines():
            expected, name = row.split("  ", 1)
            member = original / name
            if Path(name).is_absolute() or ".." in Path(name).parts or digest(member) != expected:
                raise ValueError("frozen_member_digest_mismatch")
        build = json.loads((original / "BUILD.json").read_text())
        if (build["source_sha"] != args.source or build["features"] != [] or build["instrumented"] is not False
                or build["machine"] != "x86_64" or not build["platform"].startswith("Linux-")
                or build["profile"] != "release"
                or digest(original / "mqttiumd") != args.binary_sha256
                or build["binary_sha256"] != args.binary_sha256):
            raise ValueError("qualified_normal_binary_identity_mismatch")
        name = f"mqttiumd-prepilot-{args.source[:8]}-{revision[:8]}-x86_64-linux"
        sealed = args.out / name
        shutil.copytree(original, sealed)
        for relative in ("pilot", "qualification", "preview"):
            path = sealed / relative
            if path.exists():
                shutil.rmtree(path)
        copy_pilot_files(root, sealed)
        for doc in ("README.md", "SECURE.md", "POLICY.md"):
            shutil.copy2(root / "preview" / doc, sealed / doc)
        metadata = {
            "format": "mqttiumd.prepilot.package.v1", "broker_source_sha": args.source,
            "broker_binary_sha256": args.binary_sha256, "package_source_sha": revision,
            "frozen_archive_sha256": args.archive_sha256,
            "build_record": "BUILD.json", "decision": "qualification/decision.md",
        }
        results = sealed / "qualification/forward-results.json"
        if results.is_file():
            observed = json.loads(results.read_text())
            files = {name: digest(sealed / "pilot" / name) for name in FORWARD_COMPONENTS}
            if observed["implementation"] != files or observed["binary_sha256"] != args.binary_sha256:
                raise ValueError("forward_evidence_component_mismatch")
            metadata["forward_application"] = {
                "measured_source_sha": observed["application_source_sha"], "component_sha256": files,
                "decision": "qualification/forward-decision.md", "results": "qualification/forward-results.json",
            }
            metadata["baseline_decision"] = metadata["decision"]
            metadata["decision"] = metadata["forward_application"]["decision"]
        split = sealed / "qualification/split-results.json"
        if split.is_file():
            observed = json.loads(split.read_text())
            files = {name: digest(sealed / "pilot" / name) for name in SPLIT_COMPONENTS}
            if observed["binary_sha256"] != args.binary_sha256 or files != observed["application_files"]:
                raise ValueError("split_evidence_component_mismatch")
            metadata["split_application"] = {
                "measured_source_sha": observed["application_source_sha"], "component_sha256": files,
                "decision": "qualification/split-decision.md", "results": "qualification/split-results.json",
            }
            metadata["forward_decision"] = metadata.get("decision")
            metadata["decision"] = metadata["split_application"]["decision"]
        site = sealed / "qualification/site-results.json"
        if site.is_file():
            received = json.loads(site.read_text())
            expected = received["component_sha256"]
            if received["broker_binary_sha256"] != args.binary_sha256 or any(
                Path(name).name != name or digest(sealed / "pilot" / name) != expected[name] for name in expected
            ):
                raise ValueError("site_evidence_component_mismatch")
            metadata["private_site"] = {
                "measured_source_sha": received["application_source"],
                "components": expected,
                "decision": "qualification/site-decision.md",
                "results": "qualification/site-results.json",
            }
            metadata["split_decision"] = metadata["decision"]
            metadata["decision"] = metadata["private_site"]["decision"]
        (sealed / "PACKAGE.json").write_text(json.dumps(metadata, indent=2) + "\n")
        sums = [f"{digest(f)}  {f.relative_to(sealed)}" for f in sorted(sealed.rglob("*"))
                if f.is_file() and f.name != "SHA256SUMS"]
        (sealed / "SHA256SUMS").write_text("\n".join(sums) + "\n")
        if digest(sealed / "mqttiumd") != args.binary_sha256:
            raise ValueError("packaging_changed_qualified_executable")
        result = args.out / (name + ".tar.gz")
        with tarfile.open(result, "w:gz") as archive:
            archive.add(sealed, arcname=name)
        result.with_suffix(result.suffix + ".sha256").write_text(f"{digest(result)}  {result.name}\n")
        print(json.dumps({"archive": str(result), "sha256": digest(result),
                          "broker_source_sha": args.source, "package_source_sha": revision,
                          "binary_sha256": args.binary_sha256}))


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("archive", type=Path)
    cli.add_argument("out", type=Path)
    cli.add_argument("--source", required=True)
    cli.add_argument("--archive-sha256", required=True)
    cli.add_argument("--binary-sha256", required=True)
    package(cli.parse_args())
