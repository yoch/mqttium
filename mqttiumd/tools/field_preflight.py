#!/usr/bin/env python3
"""Read-only field preflight for verified Linux previews."""
from __future__ import annotations

import argparse
import importlib.metadata
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import shutil
import sqlite3
import ssl
import stat
import subprocess
import tarfile
import time

from preview_archive import RESTART_FORMAT, archive_metadata, checked_archive


GIB = 1024**3
MIB = 1024**2
HORIZON_S = 72 * 3600
ROOT = "mqttiumd-prepilot-51be4079-91e53652-x86_64-linux"
ARCHIVE = "8ea28eda6be48e1ddfe07c623b51deff43d658ee7ac5d8b3edef012ac6f957a2"
BINARY = "9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119"
PACKAGE_SOURCE = "91e53652a7e4f2b95cabc34476048538e9a64ad7"
BROKER_SOURCE = "51be4079f7ee42f46500e993315eddc971f0a8d4"
SITE_SOURCE = "5f37e61735a6590675e7ed8b1bf103e4050c28bf"
PRIVATE_NETWORKS = tuple(ipaddress.ip_network(net) for net in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7"))
# Two cold stores, eight outboxes, two hot ledgers. Each segment has a directory and two files.
STORE_BYTES = 16 * GIB + (8 * 64 + 2 * 32) * MIB
STORE_INODES = 2 * 32768 * 3 + 4096


def read(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def command(*args, timeout=5):
    try:
        return subprocess.run(args, stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None


def output_json(result):
    try:
        return json.loads(result.stdout) if result is not None else None
    except (UnicodeError, ValueError):
        return None


def package_record(archive, expected_sha256=ARCHIVE):
    with checked_archive(archive, expected_sha256) as (package, root, _):
        build, seal = archive_metadata(package, root)
    if seal["format"] == RESTART_FORMAT:
        if build["source_sha"] != BROKER_SOURCE:
            raise ValueError("unexpected_restart_broker_source")
        return {"archive_sha256": expected_sha256, "binary_sha256": build["binary_sha256"],
                "broker_source": build["source_sha"], "package_source": seal["package_source_sha"],
                "elf_machine": build["machine"], **build["elf"], "scope": "durable_restart"}
    if (expected_sha256 != ARCHIVE or root != ROOT or build["source_sha"] != BROKER_SOURCE
            or build["binary_sha256"] != BINARY or seal["package_source_sha"] != PACKAGE_SOURCE
            or seal["private_site"]["measured_source_sha"] != SITE_SOURCE):
        raise ValueError("unexpected_sealed_binary_or_application")
    return {"archive_sha256": ARCHIVE, "binary_sha256": BINARY, "broker_source": BROKER_SOURCE,
            "package_source": PACKAGE_SOURCE, "site_source": SITE_SOURCE, "elf_machine": "x86_64",
            "minimum_glibc_symbols": "2.34", "components": seal["private_site"]["components"]}


def distribution_version(name):
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def host_record():
    cpu = read("/proc/cpuinfo") or ""
    cpu_model = next((line.split(":", 1)[1].strip() for line in cpu.splitlines()
                      if line.split(":", 1)[0].strip() == "model name"), None)
    board = read("/proc/device-tree/model") or read("/sys/class/dmi/id/product_name")
    virtual = command("systemd-detect-virt", "--vm")
    container = command("systemd-detect-virt", "--container")
    openssl = command("openssl", "version")
    mem = next((line.split()[1] for line in (read("/proc/meminfo") or "").splitlines()
                if line.startswith("MemTotal:")), None)
    supplies = [{"type": read(p / "type"), "online": read(p / "online")}
                for p in sorted(Path("/sys/class/power_supply").glob("*"))]
    power = command("vcgencmd", "get_throttled") if board and "Raspberry Pi" in board else None
    throttled = re.fullmatch(rb"throttled=(0x[0-9a-f]+)\s*", power.stdout) if power and power.returncode == 0 else None
    return {"kernel": platform.release(), "machine": platform.machine(), "os": platform.system(),
            "board": board.rstrip("\0") if board else None, "cpu_model": cpu_model,
            "available_cpus": len(os.sched_getaffinity(0)), "memory_kib": int(mem) if mem else None,
            "cpu_cgroup": read("/sys/fs/cgroup/cpu.max"), "memory_cgroup": read("/sys/fs/cgroup/memory.max"),
            "vm": virtual.stdout.decode().strip() if virtual else None,
            "container": container.stdout.decode().strip() if container else None,
            "glibc": list(platform.libc_ver()), "python": platform.python_version(), "openssl": ssl.OPENSSL_VERSION,
            "openssl_command": openssl.stdout.decode().strip() if openssl and openssl.returncode == 0 else None,
            "sqlite": sqlite3.sqlite_version, "paho": distribution_version("paho-mqtt"),
            "mqttium": distribution_version("mqttium"), "uid": os.geteuid(),
            "systemd": Path("/run/systemd/system").is_dir(), "smartctl_available": bool(shutil.which("smartctl")),
            "power_supplies": supplies, "rpi_throttled": int(throttled[1], 16) if throttled else None,
            "ups": "unverified", "ssh": "not_probed"}


def block_inventory():
    result = command("lsblk", "--bytes", "--json", "--list", "--output",
                     "KNAME,PKNAME,TYPE,SIZE,TRAN,MODEL,ROTA,MAJ:MIN")
    data = output_json(result)
    return data.get("blockdevices", []) if isinstance(data, dict) else []


def backing_disk(device, blocks):
    node = next((node for node in blocks if node["maj:min"] == device), None)
    if node is not None and node["type"] == "part":
        parent = node["pkname"]
        node = next((node for node in blocks if node["kname"] == parent), None)
    if node is None or node["type"] != "disk" or node["tran"] not in {"nvme", "sata", "ata", "sas", "usb", "mmc"}:
        return None
    if not node["model"] or not re.fullmatch(r"[A-Za-z0-9_-]+", node["kname"]):
        return None
    if re.search(r"virtual|virtio|qemu|vbox|vmware", node["model"], re.IGNORECASE):
        return None
    counter = read(f"/sys/class/block/{node['kname']}/stat")
    fields = counter.split() if counter is not None else []
    return {"model": node["model"].strip(), "transport": node["tran"], "size_bytes": node["size"],
            "rotational": bool(node["rota"]), "name": node["kname"], "device": node["maj:min"],
            "host_block_write_bytes": int(fields[6]) * 512 if len(fields) > 6 and fields[6].isdigit() else None}


def volume(root, blocks, vm):
    info = root.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError("expected_existing_directory")
    capacity = os.statvfs(root)
    device = f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}"
    result = command("findmnt", "--json", "--target", str(root), "--output", "SOURCE,FSTYPE,MAJ:MIN,OPTIONS")
    data = output_json(result)
    mounts = data.get("filesystems", []) if isinstance(data, dict) else []
    mount = mounts[0] if len(mounts) == 1 and mounts[0]["maj:min"] == device else {}
    options = mount.get("options", "").split(",")
    safe_keys = {"barrier", "commit", "data", "dax", "errors", "index", "metacopy"}
    safe_options = [option if option.partition("=")[0] in safe_keys else option.partition("=")[0]
                    for option in options if option]
    mapping = backing_disk(device, blocks) if vm == "none" else None
    return {"path": str(root), "device": device, "filesystem": mount.get("fstype"),
            "mount_options": safe_options,
            "private_writable": stat.S_IMODE(info.st_mode) == 0o700 and info.st_uid == os.geteuid()
            and os.access(root, os.W_OK | os.X_OK) and not capacity.f_flag & os.ST_RDONLY,
            "available_bytes": capacity.f_bavail * capacity.f_frsize, "available_inodes": capacity.f_favail,
            "disk": mapping}


def storage_budgets(volumes, overhead):
    by_fs = {}
    for role, volume in volumes.items():
        budget = by_fs.setdefault(volume["device"], {"roles": [], "required_bytes": 0, "required_inodes": 0,
                                                    "available_bytes": volume["available_bytes"],
                                                    "available_inodes": volume["available_inodes"]})
        budget["roles"].append(role)
        budget["required_bytes"] += STORE_BYTES + overhead
        budget["required_inodes"] += STORE_INODES
        for key in ("available_bytes", "available_inodes"):
            budget[key] = min(budget[key], volume[key])
    return by_fs


def smart_reading(disk):
    result = command("smartctl", "-a", "-j", f"/dev/{disk['name']}", timeout=12)
    data = output_json(result)
    if not isinstance(data, dict) or result.returncode & 7:
        return {"readable": False}
    nvme = data.get("nvme_smart_health_information_log", {})
    units = nvme.get("data_units_written")
    # NVMe reports units of 1000 logical 512-byte data units. This is not NAND wear.
    return {"readable": True, "passed": data.get("smart_status", {}).get("passed"),
            "nvme_host_data_written_bytes": units * 512000 if type(units) is int else None,
            "percentage_used": nvme.get("percentage_used"), "critical_warning": nvme.get("critical_warning")}


def write_headroom(disks, assessment):
    received = {}
    errors = []
    for device, disk in sorted(disks.items()):
        row = assessment.get(device, {})
        projected = row.get("projected_72h_write_bytes")
        remaining = row.get("available_write_budget_bytes")
        if (row.get("model") != disk["model"] or type(projected) is not int or projected <= 0
                or type(remaining) is not int or remaining < 0
                or not isinstance(row.get("evidence_sha256"), str)
                or not re.fullmatch(r"[a-f0-9]{64}", row["evidence_sha256"])):
            errors.append({"code": "write_budget_unverified", "disk": device})
            continue
        received[device] = {key: row[key] for key in ("model", "projected_72h_write_bytes",
                                                    "available_write_budget_bytes", "evidence_sha256")}
        if remaining < projected:
            errors.append({"code": "insufficient_write_budget", "disk": device})
    return received, errors


def private_address(address):
    return any(address.version == network.version and address in network for network in PRIVATE_NETWORKS)


def certificate_ok(pki, address):
    cert, ca, key = (pki / name for name in ("server.crt", "ca.crt", "server.key"))
    info = key.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.geteuid():
        return False
    verified = command("openssl", "verify", "-no-CApath", "-no-CAstore", "-CAfile", str(ca),
                       "-purpose", "sslserver", "-verify_ip", str(address), str(cert))
    expiry = command("openssl", "x509", "-in", str(cert), "-checkend", str(HORIZON_S), "-noout")
    pub_cert = command("openssl", "x509", "-in", str(cert), "-pubkey", "-noout")
    pub_key = command("openssl", "pkey", "-in", str(key), "-pubout")
    return (all(result is not None and result.returncode == 0 for result in (verified, expiry, pub_cert, pub_key))
            and bool(pub_cert.stdout) and pub_cert.stdout == pub_key.stdout)


def network_record(address, credentials, errors):
    if address is None:
        errors.append({"code": "private_lan_not_selected"})
        return {"address": None, "link": None, "server_certificate": {}, "peer_exchange": "not_executed"}
    if not private_address(address):
        errors.append({"code": "selected_address_not_private"})
        return {"address": str(address), "link": None, "server_certificate": {}, "peer_exchange": "not_executed"}
    result = command("ip", "-j", "address", "show")
    interfaces = output_json(result)
    link = next(({"name": iface["ifname"], "state": iface["operstate"]} for iface in interfaces
                 if "UP" in iface["flags"] and any(item.get("local") == str(address)
                                                  for item in iface.get("addr_info", []))), None) if isinstance(interfaces, list) else None
    if link is None:
        errors.append({"code": "selected_address_not_on_up_interface"})
    certificates = {}
    for lane in ("live", "history"):
        try:
            certificates[lane] = credentials is not None and certificate_ok(credentials / lane, address)
        except OSError:
            certificates[lane] = False
        if not certificates[lane]:
            errors.append({"code": "server_certificate_unverified", "lane": lane})
    return {"address": str(address), "link": link, "server_certificate": certificates, "peer_exchange": "not_executed"}


def assess(args):
    errors = []
    host = host_record()
    try:
        package = package_record(args.archive, args.archive_sha256 or ARCHIVE)
    except (ValueError, KeyError, tarfile.TarError, OSError) as error:
        package = None
        errors.append({"code": "sealed_package_unverified", "detail": type(error).__name__})
    if package is not None:
        if host["os"] != "Linux" or host["machine"] != package["elf_machine"]:
            errors.append({"code": "native_architecture_mismatch"})
        libc, version = host["glibc"]
        minimum = tuple(map(int, package["minimum_glibc_symbols"].split(".")))
        if (libc != "glibc" or not re.fullmatch(r"\d+(\.\d+)+", version)
                or tuple(map(int, version.split("."))) < minimum):
            errors.append({"code": "glibc_below_or_unknown_required_symbols"})
        if package.get("scope") == "durable_restart":
            errors.append({"code": "full_site_receipt_not_present"})
    if host["uid"] == 0:
        errors.append({"code": "requires_unprivileged_service_account"})
    if host["paho"] != "2.1.0":
        errors.append({"code": "paho_2_1_0_not_available"})
    if (host["rpi_throttled"] or 0) & 0xF:
        errors.append({"code": "rpi_current_power_or_thermal_limit"})
    blocks = block_inventory()
    volumes = {}
    for role, root in (("data", args.data_root), ("backup", args.backup_root),
                       ("restore", args.restore_root or args.backup_root)):
        try:
            detail = volume(root, blocks, host["vm"])
        except (OSError, ValueError) as error:
            errors.append({"code": "volume_unreadable", "role": role, "detail": type(error).__name__})
            continue
        volumes[role] = detail
        if not detail["private_writable"]:
            errors.append({"code": "volume_directory_not_private_and_writable", "role": role})
        if detail["disk"] is None:
            errors.append({"code": "physical_backing_device_unverified", "role": role})
        if {"nobarrier", "barrier=0"}.intersection(detail["mount_options"]):
            errors.append({"code": "durability_barriers_disabled", "role": role})
        if detail["filesystem"] is None:
            errors.append({"code": "mount_unverified", "role": role})
    budgets = storage_budgets(volumes, args.overhead_gib * GIB)
    for device, budget in budgets.items():
        for unit in ("bytes", "inodes"):
            if budget[f"available_{unit}"] < budget[f"required_{unit}"]:
                errors.append({"code": f"insufficient_free_{unit}", "filesystem_device": device})
    disks = {detail["disk"]["device"]: detail["disk"] for detail in volumes.values() if detail["disk"] is not None}
    assessment = {}
    if args.write_assessment is not None:
        try:
            if not stat.S_ISREG(args.write_assessment.stat().st_mode):
                raise ValueError("write_assessment_must_be_regular_file")
            with args.write_assessment.open("rb") as source:
                raw = source.read(65537)
            if len(raw) > 65536:
                raise ValueError("write_assessment_exceeds_budget")
            assessment = json.loads(raw)
            if not isinstance(assessment, dict) or any(not isinstance(row, dict) for row in assessment.values()):
                raise ValueError("expected_disk_assessment")
        except (ValueError, OSError):
            errors.append({"code": "write_assessment_invalid"})
            assessment = {}
    received, write_errors = write_headroom(disks, assessment)
    errors.extend(write_errors)
    if args.smart:
        for disk in disks.values():
            disk["smart"] = smart_reading(disk)
            smart = disk["smart"]
            if smart.get("passed") is False or smart.get("critical_warning") not in (None, 0):
                errors.append({"code": "smart_reported_fault", "disk": disk["device"]})
    network = network_record(args.lan_address, args.credentials, errors)
    return {"format": "mqttiumd.field-preflight.v1", "observed_unix_s": int(time.time()), "package": package,
            "host": host, "volumes": volumes, "capacity_by_filesystem": budgets,
            "operator_write_assessments": received,
            "network": network, "refusals": errors, "release_site_listener": "loopback_only",
            "qualification": "not_executed", "mode": "read_only",
            "preflight": "HOLD" if errors else "READY_FOR_STAGED_RECEPTION"}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--archive", required=True, type=Path)
    cli.add_argument("--archive-sha256", help="verified digest for a separately received restart preview")
    cli.add_argument("--data-root", required=True, type=Path)
    cli.add_argument("--backup-root", required=True, type=Path)
    cli.add_argument("--restore-root", type=Path, help="defaults to backup-root")
    cli.add_argument("--write-assessment", type=Path)
    cli.add_argument("--lan-address", type=ipaddress.ip_address)
    cli.add_argument("--credentials", type=Path, help="operator PKI root with live/ and history/")
    cli.add_argument("--smart", action="store_true", help="try passive SMART reads on directly mapped disks")
    cli.add_argument("--overhead-gib", type=int, default=4, help="headroom per simultaneous copy, default 4 GiB")
    args = cli.parse_args()
    if args.overhead_gib < 1:
        cli.error("--overhead-gib must be positive")
    result = assess(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 2 if result["refusals"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
