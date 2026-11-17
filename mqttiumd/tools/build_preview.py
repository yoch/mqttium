#!/usr/bin/env python3
"""Package a normal MQTTiumd executable; running it requires no build tools."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tarfile
import tempfile


def command(*args: str, cwd: Path | None = None) -> str:
    return subprocess.check_output(args, cwd=cwd, text=True, stderr=subprocess.STDOUT)


def build(out: Path, target: Path) -> Path:
    caller = Path(__file__).resolve().parents[2]
    if out.exists():
        raise ValueError("output directory must be new")
    if command("git", "status", "--porcelain", cwd=caller).strip():
        raise ValueError("packaging requires a clean source tree")
    source = command("git", "rev-parse", "HEAD", cwd=caller).strip()
    # A private detached source prevents an edit or concurrent commit during a
    # long build from changing what the recorded source SHA actually compiled.
    with tempfile.TemporaryDirectory(prefix="mqttiumd-preview-source-") as temporary:
        pin = Path(temporary) / "source"
        command("git", "worktree", "add", "--detach", str(pin), source, cwd=caller)
        try:
            return build_pinned(out, target, pin / "mqttiumd", source)
        finally:
            command("git", "worktree", "remove", "--force", str(pin), cwd=caller)


def build_pinned(out: Path, target: Path, root: Path, source: str) -> Path:
    repo = root.parent
    out.mkdir(parents=True)
    env = dict(os.environ, CARGO_INCREMENTAL="0")
    features = command("cargo", "tree", "-p", "mqttiumd", "--no-default-features", "--locked",
                       "--format", "{p} features={f}", cwd=root)
    if any(term in features for term in ("mqttiumd-lab", "kill-points", "test-hooks")):
        raise ValueError("normal dependency tree contains instrumentation")
    with (out / "build.log").open("w") as log:
        subprocess.run(["cargo", "build", "--release", "--locked", "-p", "mqttiumd",
                            "--no-default-features", "--target-dir", str(target)],
                       cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    binary = target / "release/mqttiumd"
    package = out / f"mqttiumd-preview-{source[:8]}-{platform.machine()}-linux"
    package.mkdir()
    shutil.copy2(binary, package / "mqttiumd")
    for name in ("README.md", "start.sh"):
        shutil.copy2(root / "preview" / name, package / name)
    (package / "NORMAL-FEATURES.txt").write_text(features)
    runtime = command("ldd", str(binary))
    if "not found" in runtime:
        raise ValueError("unresolved runtime dependency")
    (package / "RUNTIME.txt").write_text(runtime + "\n" + command("ldd", "--version"))
    metadata = json.loads(command("cargo", "metadata", "--locked", "--format-version", "1", cwd=root))
    # Resolve the declared product dependency closure (including target-only
    # packages), excluding laboratory/dev-only crates. License coverage can
    # therefore exceed the packages actually compiled on this platform.
    product = next(p for p in metadata["packages"] if p["name"] == "mqttiumd")
    nodes = {n["id"]: n for n in metadata["resolve"]["nodes"]}
    included: set[str] = set()
    pending = [product["id"]]
    while pending:
        key = pending.pop()
        if key in included:
            continue
        included.add(key)
        for dep in nodes[key]["deps"]:
            if any(k["kind"] != "dev" for k in dep["dep_kinds"]):
                pending.append(dep["pkg"])
    licenses = package / "licenses"
    licenses.mkdir()
    shutil.copy2(repo / "LICENSE", licenses / "mqttium-Apache-2.0.txt")
    shutil.copy2(repo / "NOTICE", licenses / "mqttium-NOTICE.txt")
    license_manifest = []
    for p in metadata["packages"]:
        if p["id"] not in included:
            continue
        entry = {"name": p["name"], "version": p["version"], "license": p["license"], "files": []}
        parent = Path(p["manifest_path"]).parent
        # Preserve actual upstream license/notice texts instead of only SPDX labels.
        for f in sorted(parent.iterdir()):
            if f.is_file() and f.name.upper().startswith(("LICENSE", "LICENCE", "COPYING", "NOTICE")):
                if f.stat().st_size > 1024 * 1024:
                    raise ValueError("unexpected license size")
                dest = f"{p['name']}-{p['version']}-{f.name}"
                shutil.copy2(f, licenses / dest)
                entry["files"].append(dest)
        license_manifest.append(entry)
    # Bundled SQLite explicitly places its implementation in the public domain.
    (licenses / "SQLite-public-domain.txt").write_text(
        "SQLite is in the public domain. See https://sqlite.org/copyright.html\n"
        "The SQLite amalgamation is compiled by the bundled libsqlite3-sys dependency.\n")
    (licenses / "dependencies.json").write_text(json.dumps(license_manifest, indent=2) + "\n")
    build_record = {
        "source_sha": source, "platform": platform.platform(), "machine": platform.machine(),
        "compiler": command("rustc", "-Vv"), "profile": "release", "features": [],
        "sqlite_schema": 7, "instrumented": False,
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "runtime_dependencies": runtime.splitlines(), "execution_verified": False,
        "execution_receipt": "See the separately identified archive extraction smoke and wildcard reception.",
    }
    (package / "BUILD.json").write_text(json.dumps(build_record, indent=2) + "\n")
    sums = []
    for f in sorted(package.rglob("*")):
        if f.is_file():
            sums.append(f"{hashlib.sha256(f.read_bytes()).hexdigest()}  {f.relative_to(package)}")
    (package / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    archive = out / f"{package.name}.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(package, arcname=package.name)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_name(archive.name + ".sha256").write_text(f"{digest}  {archive.name}\n")
    return archive


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", type=Path)
    parser.add_argument("--target-dir", type=Path, required=True)
    args = parser.parse_args()
    print(build(args.out.resolve(), args.target_dir.resolve()))


if __name__ == "__main__":
    main()
