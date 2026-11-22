#!/usr/bin/env python3
"""Provision and start the isolated loopback mTLS demonstration."""
import argparse
from pathlib import Path
import os
import tempfile

from common import PACKAGE, broker_argv, require
from forward_reception import require_binary
from split_reception import provision_planes


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest="command", required=True)
    commands.add_parser("provision").add_argument("directory", type=Path)
    broker = commands.add_parser("broker")
    broker.add_argument("directory", type=Path)
    broker.add_argument("lane", choices=("live", "history"))
    broker.add_argument("--port", type=int)
    args = cli.parse_args()
    os.umask(0o077)
    root = args.directory.resolve()
    if args.command == "provision":
        root.mkdir(mode=0o700)
        provision_planes(root, PACKAGE)
    else:
        require(root.is_dir() and root.stat().st_mode & 0o077 == 0, "private_pilot_directory_required")
        require_binary(PACKAGE / "mqttiumd", PACKAGE)
        run = Path(tempfile.mkdtemp(prefix=args.lane + "-", dir=root))
        # The broker exclusively creates the journal at startup.
        events = run / "events.jsonl"
        port = args.port if args.port is not None else (8883 if args.lane == "live" else 8884)
        argv = broker_argv(PACKAGE / "mqttiumd", root / (args.lane + ".sqlite"), events, root / args.lane,
                           listen=f"127.0.0.1:{port}")
        print(f"{args.lane} events {events}", flush=True)
        os.execv(argv[0], argv)


if __name__ == "__main__":
    main()
