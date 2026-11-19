#!/usr/bin/env python3
"""Package a normal MQTTiumd executable; running it requires no build tools."""
from __future__ import annotations
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tarfile
import tempfile

from preview_archive import MQTT311_FORMAT, RESTART_FORMAT, digest, elf_record


def command(*args: str, cwd: Path | None = None) -> str:
    return subprocess.check_output(args, cwd=cwd, text=True, stderr=subprocess.PIPE)


def copy_pilot_files(root: Path, package: Path) -> None:
    shutil.copy2(root / "preview/PILOT.md", package / "PILOT.md")
    shutil.copy2(root / "preview/FORWARD.md", package / "FORWARD.md")
    shutil.copy2(root / "preview/SPLIT.md", package / "SPLIT.md")
    shutil.copy2(root / "preview/SITE.md", package / "SITE.md")
    shutil.copy2(root / "tools/run_prepilot_reception.sh", package / "run-qualification.sh")
    shutil.copy2(root / "tools/run_forward_reception.sh", package / "run-forward-reception.sh")
    shutil.copy2(root / "tools/run_split_reception.sh", package / "run-split-reception.sh")
    shutil.copy2(root / "tools/run_site_reception.sh", package / "run-site-reception.sh")
    shutil.copytree(root / "preview/pilot", package / "pilot",
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    shutil.copytree(root / "docs/pilot", package / "qualification")
    # The existing socket helpers resolve these two modules relative to the
    # repository layout. Preserve that layout inside the standalone package.
    (package / "preview").mkdir()
    for name in ("demo-pki.py", "policy-demo.py"):
        shutil.copy2(root / "preview" / name, package / "preview" / name)
    # The receipt helper validates its source-pinned control window at import.
    # Keep that bounded public configuration beside the copied normal probes.
    (package / "contracts/v1").mkdir(parents=True, exist_ok=True)
    shutil.copy2(root / "contracts/v1/product-profile.json",
                 package / "contracts/v1/product-profile.json")
    # Ship the existing normal socket helpers used by charged restoration.
    # Follow only local Python imports, without copying proof laboratories.
    modules = ["policy_transition_reception"]
    copied = set()
    while modules:
        name = modules.pop()
        path = root / "tests/system" / (name + ".py")
        if name in copied or not path.is_file():
            continue
        copied.add(name)
        destination = package / "tests/system" / path.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules.append(node.module)
            elif isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)


def build(out: Path, target: Path, *, mqtt311: bool = False) -> Path:
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
            return build_pinned(out, target, pin / "mqttiumd", source, mqtt311=mqtt311)
        finally:
            command("git", "worktree", "remove", "--force", str(pin), cwd=caller)


def copy_restart_files(package: Path, packager: Path) -> None:
    preview = packager / "preview"
    shutil.copy2(preview / "DURABLE-RESTART.md", package / "README.md")
    for name in ("start-tls.sh", "demo-pki.py", "run-durable-restart.sh"):
        shutil.copy2(preview / name, package / name)
    pilot = package / "pilot"
    pilot.mkdir()
    for name in ("common.py", "durable_restart.py"):
        shutil.copy2(preview / "pilot" / name, pilot / name)
    (package / "requirements.txt").write_text("mqttium==1.3.0\npaho-mqtt==2.1.0\n")


def build_pinned(out: Path, target: Path, root: Path, source: str, *,
                 restart_packager: tuple[Path, str] | None = None, mqtt311: bool = False) -> Path:
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
    machine = platform.machine()
    name = f"mqttiumd-preview-{source[:8]}-{machine}-linux"
    if restart_packager or mqtt311:
        packager, revision = restart_packager or (root, source)
        elf = elf_record(binary)
        if platform.system() != "Linux" or elf["machine"] != machine:
            raise ValueError("preview_requires_native_linux_build")
        if mqtt311:
            name = f"mqttiumd-mqtt311-preview-{source[:8]}-{machine}-linux"
        else:
            name = f"mqttiumd-restart-preview-{source[:8]}-{revision[:8]}-{machine}-linux"
    package = out / name
    package.mkdir()
    shutil.copy2(binary, package / "mqttiumd")
    if restart_packager:
        copy_restart_files(package, packager)
    elif mqtt311:
        shutil.copy2(root / "preview/MQTT311.md", package / "README.md")
        for name in ("start-tls.sh", "demo-pki.py", "mqtt311-demo.py", "build-mosquitto-reference.sh", "rm-pubrel-probe.py",
                     "mosquitto-config.py", "mosquitto_inventory.py", "mosquitto_example.py", "cutover_demo.py", "CUTOVER.md"):
            shutil.copy2(root / "preview" / name, package / name)
        (package / "requirements.txt").write_text("mqttium==1.3.0\npaho-mqtt==2.1.0\n")
    else:
        for name in ("README.md", "start.sh", "demo.sh", "expiry-demo.sh", "will-demo.sh",
                     "SECURE.md", "start-tls.sh", "secure-demo.sh", "demo-pki.py", "identities.example.json",
                     "POLICY.md", "policy-demo.py", "identities.next.example.json", "retained-will-demo.py"):
            shutil.copy2(root / "preview" / name, package / name)
        copy_pilot_files(root, package)
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
        "build_options": ["--release", "--locked", "--no-default-features", "-p", "mqttiumd"],
        "build_environment": {key: os.environ.get(key) for key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_PROFILE_RELEASE_DEBUG", "CARGO_PROFILE_RELEASE_OPT_LEVEL", "CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS")},
        "sqlite_schema": 12, "instrumented": False,
        "listener_profiles": {"loopback_cleartext": {"sqlite_schema": 12, "tls": False},
            "private_mtls": {"sqlite_schema": 13, "tls_version": "TLS1.3",
                "client_certificate": "required", "tls_resumption": False, "early_data": False}},
        "will_profile": {"qos": 1, "retain": True, "delay_interval": 0,
                         "message_expiry": False, "intent_slots": 256, "payload_bytes": 8388608},
        "offline_policy": {"plan_format": "mqttiumd.policy-plan.v1", "sqlite_schema": 13,
                           "scope": "monotone", "tool": "mqttiumd policy"},
        "binary_sha256": digest(binary),
        "runtime_dependencies": runtime.splitlines(),
        "execution_verification": "Separate extraction receipt names this exact archive hash",
        "execution_receipt": "See the separately identified archive extraction smoke and Will/Message Expiry/retained receptions.",
    }
    if restart_packager or mqtt311:
        build_record["elf"] = elf
        build_record["lockfile_sha256"] = digest(root / "Cargo.lock")
        scope = "loopback mTLS MQTT 3.1.1 and MQTT 5 interoperability" if mqtt311 else "one loopback mTLS restart"
        build_record["execution_receipt"] = f"Separate reception for this archive covering {scope}"
        if mqtt311:
            build_record["mqtt_protocol_levels"] = [4, 5]
        seal = {
            "format": MQTT311_FORMAT if mqtt311 else RESTART_FORMAT, "machine": machine,
            "broker_source_sha": source,
            "broker_binary_sha256": build_record["binary_sha256"], "package_source_sha": revision,
            "reception": {"status": "separate_receipt", "scope": scope,
                          "binary_sha256": build_record["binary_sha256"]},
        }
        (package / "PACKAGE.json").write_text(json.dumps(seal, indent=2) + "\n")
    (package / "BUILD.json").write_text(json.dumps(build_record, indent=2) + "\n")
    sums = []
    for f in sorted(package.rglob("*")):
        if f.is_file():
            sums.append(f"{hashlib.sha256(f.read_bytes()).hexdigest()}  {f.relative_to(package)}")
    (package / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    archive = out / f"{package.name}.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(package, arcname=package.name)
    artifact_hash = digest(archive)
    archive.with_name(archive.name + ".sha256").write_text(f"{artifact_hash}  {archive.name}\n")
    return archive


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", type=Path)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--restart-broker-root", type=Path, help="mqttiumd directory of a clean pinned checkout")
    parser.add_argument("--broker-source", help="exact broker commit for the restart preview")
    parser.add_argument("--mqtt311", action="store_true", help="practical MQTT 3.1.1 and MQTT 5 preview")
    args = parser.parse_args()
    if (args.restart_broker_root is None) != (args.broker_source is None):
        parser.error("the restart preview needs both the broker checkout and its commit")
    if args.mqtt311 and args.restart_broker_root is not None:
        parser.error("choose either the MQTT 3.1.1 preview or the pinned restart preview")
    if args.restart_broker_root is None:
        print(build(args.out.resolve(), args.target_dir.resolve(), mqtt311=args.mqtt311))
        return
    packager = Path(__file__).resolve().parents[1]
    broker_root = args.restart_broker_root.resolve()
    for path in (packager, broker_root):
        if command("git", "status", "--porcelain", cwd=path).strip():
            raise ValueError("restart_preview_requires_clean_checkouts")
    source = command("git", "rev-parse", "HEAD", cwd=broker_root).strip()
    if source != args.broker_source:
        raise ValueError("restart_broker_checkout_mismatch")
    revision = command("git", "rev-parse", "HEAD", cwd=packager).strip()
    print(build_pinned(args.out.resolve(), args.target_dir.resolve(), broker_root, source,
                       restart_packager=(packager, revision)))


if __name__ == "__main__":
    main()
