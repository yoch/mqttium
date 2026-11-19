#!/usr/bin/env python3
"""Verify and extract a preview against an independently supplied SHA-256."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile


MIB = 1024**2
ELF_MACHINES = {62: "x86_64", 183: "aarch64"}
RESTART_FORMAT = "mqttiumd.durable-restart.preview.v1"
RESTART_SPACE = 128 * MIB


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def elf_machine(source):
    header = source.read(20)
    if len(header) != 20 or header[:7] != b"\x7fELF\x02\x01\x01":
        raise ValueError("unsupported_elf_header")
    machine = ELF_MACHINES.get(int.from_bytes(header[18:20], "little"))
    if machine is None:
        raise ValueError("unsupported_elf_machine")
    return machine


def elf_record(path):
    with path.open("rb") as source:
        machine = elf_machine(source)
    details = subprocess.check_output(
        ["readelf", "--wide", "--program-headers", "--version-info", str(path)],
        text=True, env=dict(os.environ, LC_ALL="C"),
    )
    interpreter = re.search(r"Requesting program interpreter: (\S+)\]", details)
    glibc = sorted(set(re.findall(r"Name: GLIBC_([0-9]+(?:\.[0-9]+)+)\b", details)),
                   key=lambda value: tuple(map(int, value.split("."))))
    if interpreter is None or not glibc:
        raise ValueError("missing_elf_runtime_requirements")
    return {"machine": machine, "interpreter": interpreter[1],
            "glibc_versions": glibc, "minimum_glibc_symbols": glibc[-1]}


@contextmanager
def checked_archive(path, expected_sha256):
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise ValueError("invalid_expected_archive_sha256")
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= 128 * MIB:
        raise ValueError("archive_must_be_a_bounded_regular_file")
    with path.open("rb") as source:
        if hashlib.file_digest(source, "sha256").hexdigest() != expected_sha256:
            raise ValueError("archive_sha256_mismatch")
        source.seek(0)
        with tarfile.open(fileobj=source, mode="r:gz") as package:
            members = {}
            roots = set()
            unpacked = 0
            for member in package:
                name = PurePosixPath(member.name)
                if (name.is_absolute() or not name.parts or ".." in name.parts
                        or name.as_posix() != member.name.rstrip("/")
                        or not (member.isfile() or member.isdir()) or member.name in members):
                    raise ValueError("unsafe_archive_entry")
                members[member.name] = member
                roots.add(name.parts[0])
                unpacked += member.size
                if len(members) > 512 or unpacked > 256 * MIB:
                    raise ValueError("archive_budget_exceeded")
            if len(roots) != 1:
                raise ValueError("archive_root_invalid")
            root = roots.pop()
            if root not in members or not members[root].isdir():
                raise ValueError("archive_root_invalid")
            yield package, root, unpacked


def archive_metadata(package, root):
    def record(name):
        member = package.getmember(f"{root}/{name}")
        if not member.isfile() or not 0 < member.size <= 64 * 1024:
            raise ValueError("invalid_archive_record")
        with package.extractfile(member) as source:
            return json.load(source)

    build = record("BUILD.json")
    seal = record("PACKAGE.json")
    binary = package.getmember(f"{root}/mqttiumd")
    if not binary.isfile() or binary.mode & 0o111 == 0:
        raise ValueError("invalid_archive_binary")
    with package.extractfile(binary) as source:
        machine = elf_machine(source)
        source.seek(0)
        fingerprint = hashlib.file_digest(source, "sha256").hexdigest()
    if (build["machine"] != machine or build["binary_sha256"] != fingerprint
            or seal["broker_binary_sha256"] != fingerprint or build["source_sha"] != seal["broker_source_sha"]
            or build["profile"] != "release" or build["instrumented"] is not False or build["features"] != []
            or build["build_options"] != ["--release", "--locked", "--no-default-features", "-p", "mqttiumd"]):
        raise ValueError("archive_binary_identity_mismatch")
    if seal["format"] == RESTART_FORMAT:
        if (build["elf"]["machine"] != machine or seal["machine"] != machine
                or seal["reception"]["binary_sha256"] != fingerprint
                or seal["reception"]["status"] != "separate_receipt"):
            raise ValueError("archive_reception_identity_mismatch")
    elif seal["format"] != "mqttiumd.prepilot.package.v1":
        raise ValueError("unsupported_preview_format")
    return build, seal


def require_native(machine, elf=None):
    if platform.system() != "Linux" or platform.machine() != machine:
        raise ValueError("native_architecture_mismatch")
    if elf is None:
        return
    libc, version = platform.libc_ver()
    minimum = tuple(map(int, elf["minimum_glibc_symbols"].split(".")))
    if (libc != "glibc" or re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", version) is None
            or tuple(map(int, version.split("."))) < minimum):
        raise ValueError("native_glibc_incompatible")
    if not Path(elf["interpreter"]).is_file():
        raise ValueError("native_interpreter_missing")


def check_members(root):
    manifest = {}
    for line in (root / "SHA256SUMS").read_text().splitlines():
        fingerprint, name = line.split("  ", 1)
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts or name in manifest:
            raise ValueError("invalid_member_manifest")
        manifest[name] = fingerprint
    files = {f.relative_to(root).as_posix(): f for f in root.rglob("*") if f.is_file()}
    del files["SHA256SUMS"]
    if files.keys() != manifest.keys() or any(digest(f) != manifest[name] for name, f in files.items()):
        raise ValueError("archive_member_digest_mismatch")
    return len(files)


def receive(archive, expected, out, *, restart=False):
    out = out.absolute()
    with checked_archive(archive, expected) as (package, name, unpacked):
        build, seal = archive_metadata(package, name)
        require_native(build["machine"], build.get("elf"))
        if restart and seal["format"] != RESTART_FORMAT:
            raise ValueError("restart_preview_required")
        if restart and shutil.disk_usage(out.parent).free < max(RESTART_SPACE, unpacked + 32 * MIB):
            raise ValueError("restart_scratch_space_insufficient")
        out.mkdir(mode=0o700)
        package.extractall(out, filter="data")
    root = out / name
    members = check_members(root)
    if "elf" in build and shutil.which("readelf") and elf_record(root / "mqttiumd") != build["elf"]:
        raise ValueError("delivered_elf_mismatch")
    observation = {
        "archive_sha256": expected, "binary_sha256": build["binary_sha256"],
        "broker_source_sha": build["source_sha"], "package_source_sha": seal["package_source_sha"],
        "verified_members": members, "elf": build.get("elf"), "machine": platform.machine(), "package_directory": name,
    }
    (out / "preflight.json").write_text(json.dumps(observation, indent=2) + "\n")
    if restart:
        subprocess.run([str(root / "run-durable-restart.sh"), str(out / "receipt"), expected],
                       env=dict(os.environ, PYTHON=sys.executable), check=True)
    return observation


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("archive", type=Path)
    cli.add_argument("--sha256", required=True, help="verified digest from the release record")
    cli.add_argument("--out", required=True, type=Path, help="new directory for extraction and receipt")
    cli.add_argument("--restart", action="store_true", help="run the packaged mTLS restart smoke")
    args = cli.parse_args()
    try:
        result = receive(args.archive, args.sha256, args.out, restart=args.restart)
    except (KeyError, OSError, ValueError, tarfile.TarError, subprocess.CalledProcessError) as error:
        print(json.dumps({"status": "REFUSED", "reason": str(error)}))
        return 2
    print(json.dumps({"status": "PASS", **result}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
