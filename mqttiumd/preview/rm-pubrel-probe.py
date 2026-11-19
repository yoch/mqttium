#!/usr/bin/env python3
"""Receive the two persistent QoS 2 exchanges with a smaller send window."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import runpy
import subprocess
from tempfile import TemporaryDirectory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("package", type=Path, help="verified, extracted preview")
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    package = args.package.resolve()
    demo = runpy.run_path(str(Path(__file__).with_name("mqtt311-demo.py")))
    build = json.loads((package / "BUILD.json").read_text())
    record = {"format": "mqttiumd.reduced-receive-maximum.v1", "machine": platform.machine(),
              "broker_source_sha": build["source_sha"], "binary_sha256": demo["sha"](package / "mqttiumd"),
              "starts": []}
    demo["require"](record["binary_sha256"] == build["binary_sha256"] and record["machine"] == build["machine"],
                    "binary_or_platform_mismatch")
    os.umask(0o077)
    with TemporaryDirectory(prefix="mqttiumd-rm-probe-") as scratch:
        out = Path(scratch)
        pki = out / "pki"
        subprocess.run(["python3", package / "demo-pki.py", pki], check=True, timeout=30)
        passed = demo["reduced_receive_maximum"](record, out, pki, package=package)
    record["status"] = "PASS" if passed else "FAIL"
    with args.receipt.open("x") as file:
        json.dump(record, file, indent=2)
        file.write("\n")
    print(f"{record['status']} {args.receipt}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
