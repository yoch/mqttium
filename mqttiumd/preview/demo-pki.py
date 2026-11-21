#!/usr/bin/env python3
"""Generate short-lived demonstration credentials in a new private directory."""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess


def openssl(*args: str | Path | int) -> bytes:
    return subprocess.run(
        ["openssl", *(str(arg) for arg in args)],
        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15,
    ).stdout


def demo_days() -> int:
    days = int(os.environ.get("MQTTIUMD_DEMO_PKI_DAYS", "2"))
    if not 2 <= days <= 7:
        raise ValueError("demo certificate lifetime must be between two and seven days")
    return days


def ca(root: Path, name: str) -> None:
    openssl("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
            "-nodes", "-days", demo_days(), "-subj", f"/CN={name}",
            "-addext", "basicConstraints=critical,CA:TRUE,pathlen:0",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign",
            "-keyout", root / f"{name}.key", "-out", root / f"{name}.crt")


def issue(root: Path, name: str, usage: str, *, issuer: str = "ca", san: str | None = None, days: int | None = None) -> str:
    key, csr, cert = (root / f"{name}.{suffix}" for suffix in ("key", "csr", "crt"))
    extensions = root / f"{name}.ext"
    extensions.write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n"
                          f"extendedKeyUsage={usage}\n" + (f"subjectAltName={san}\n" if san else ""))
    openssl("req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
            "-nodes", "-subj", f"/CN={name}", "-keyout", key, "-out", csr)
    openssl("x509", "-req", "-in", csr, "-CA", root / f"{issuer}.crt",
            "-CAkey", root / f"{issuer}.key", "-CAcreateserial", "-days", demo_days() if days is None else days,
            "-extfile", extensions, "-out", cert)
    csr.unlink()
    extensions.unlink()
    return hashlib.sha256(openssl("x509", "-in", cert, "-outform", "DER")).hexdigest()


def server_san(host: str) -> str:
    try:
        return f"IP:{ipaddress.ip_address(host)}"
    except ValueError:
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9.-]{0,252}", host):
            raise ValueError("expected the DNS name or IP address clients will verify") from None
        return f"DNS:{host}"


def generate(root: Path, host: str = "127.0.0.1") -> dict:
    san = server_san(host)
    saved = os.umask(0o077)
    try:
        root.mkdir(mode=0o700, parents=True, exist_ok=False)
        ca(root, "ca")
        issue(root, "server", "serverAuth", san=san)
        sensor = issue(root, "sensor", "clientAuth")
        dashboard = issue(root, "dashboard", "clientAuth")
        policy = {"version": 1, "principals": [
            {"name": "sensor-1", "certificates": [sensor], "client_ids": ["capteur-1"],
             "publish": ["atelier/capteur-1"], "subscribe": ["atelier/capteur-1/command"]},
            {"name": "dashboard", "certificates": [dashboard], "client_ids": ["dashboard", "dashboard-check"],
             "publish": [], "subscribe": ["atelier"]},
        ]}
        (root / "identities.json").write_text(json.dumps(policy, indent=2) + "\n")
        return policy
    finally:
        os.umask(saved)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    generate(args.directory, args.host)
    print(f"Created demonstration credentials in {args.directory} ({demo_days()} days)")
