#!/usr/bin/env python3
"""Generate a short-lived test CA and localhost server certificate with OpenSSL."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def _run(*arguments: str) -> None:
    subprocess.run(arguments, check=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--label", default="MQTTium CI")
    args = parser.parse_args()
    root = args.directory.resolve()
    root.mkdir(parents=True, exist_ok=True)
    (root / "ca.cnf").write_text(
        "\n".join(
            (
                "[req]",
                "distinguished_name = dn",
                "x509_extensions = v3_ca",
                "prompt = no",
                "[dn]",
                f"CN = {args.label} Test CA",
                "[v3_ca]",
                "basicConstraints = critical,CA:TRUE",
                "keyUsage = critical,keyCertSign,cRLSign",
                "subjectKeyIdentifier = hash",
                "",
            )
        ),
        encoding="utf-8",
    )
    (root / "server.ext").write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
        encoding="utf-8",
    )
    _run(
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-config",
        str(root / "ca.cnf"),
        "-keyout",
        str(root / "ca.key"),
        "-out",
        str(root / "ca.crt"),
    )
    _run(
        "openssl",
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-subj",
        "/CN=localhost",
        "-keyout",
        str(root / "server.key"),
        "-out",
        str(root / "server.csr"),
    )
    _run(
        "openssl",
        "x509",
        "-req",
        "-days",
        "1",
        "-in",
        str(root / "server.csr"),
        "-CA",
        str(root / "ca.crt"),
        "-CAkey",
        str(root / "ca.key"),
        "-CAcreateserial",
        "-extfile",
        str(root / "server.ext"),
        "-out",
        str(root / "server.crt"),
    )
    (root / "ca.key").chmod(0o600)
    (root / "server.key").chmod(0o600)
    (root / "ca.crt").chmod(0o644)
    (root / "server.crt").chmod(0o644)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
