#!/usr/bin/env python3
"""Wait until required TCP and Unix-domain endpoints are accepting connections."""

from __future__ import annotations

import argparse
import socket
import time
from pathlib import Path


def _tcp_ready(endpoint: str) -> bool:
    host, separator, port_text = endpoint.rpartition(":")
    if not separator or not host:
        raise ValueError(f"invalid TCP endpoint {endpoint!r}; expected HOST:PORT")
    try:
        with socket.create_connection((host, int(port_text)), timeout=0.25):
            return True
    except OSError:
        return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tcp", action="append", default=[], metavar="HOST:PORT")
    parser.add_argument("--unix", action="append", default=[], type=Path, metavar="PATH")
    parser.add_argument("--timeout", type=float, default=15.0)
    args = parser.parse_args()
    if not args.tcp and not args.unix:
        parser.error("at least one --tcp or --unix endpoint is required")

    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        tcp_ready = all(_tcp_ready(endpoint) for endpoint in args.tcp)
        unix_ready = all(path.is_socket() for path in args.unix)
        if tcp_ready and unix_ready:
            return 0
        time.sleep(0.2)
    missing_tcp = [endpoint for endpoint in args.tcp if not _tcp_ready(endpoint)]
    missing_unix = [str(path) for path in args.unix if not path.is_socket()]
    raise SystemExit(
        "endpoints did not become ready before the timeout: "
        f"tcp={missing_tcp or 'none'}, unix={missing_unix or 'none'}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
