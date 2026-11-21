#!/usr/bin/env python3
"""Prepare a private loopback example with fresh local certificates and two separate stores."""
import argparse
import os
from pathlib import Path
import subprocess
import sys


def reference_config(config, acl, pki, port, storage=None):
    persistence = ["persistence false"] if storage is None else [
        "persistence true", f"persistence_location {storage}/", "persistence_file mosquitto.db"]
    config.write_text("\n".join([
        f"listener {port} 127.0.0.1", *persistence, "allow_anonymous false",
        "require_certificate true", "use_identity_as_username true", "tls_version tlsv1.3",
        f"acl_file {acl}", f"cafile {pki / 'ca.crt'}", f"certfile {pki / 'server.crt'}",
        f"keyfile {pki / 'server.key'}", "log_type all", "",
    ]))


def prepare(site, pki, port):
    site.mkdir(mode=0o700)
    original = site / "mosquitto-data"
    original.mkdir(mode=0o700)
    acl = site / "access.acl"
    acl.write_text("user sensor\ntopic write atelier/#\nuser dashboard\ntopic read atelier/a\n"
                   "topic read atelier/capteur-1/#\ntopic write commande/point\n")
    conf = site / "mosquitto.conf"
    reference_config(conf, acl, pki, port, original)
    bindings = site / "bindings.txt"
    bindings.write_text(f"mosquitto_version 2.0.21\nclient sensor {pki}/sensor.crt capteur-1\n"
                        f"client dashboard {pki}/dashboard.crt dashboard\n")
    return conf, bindings


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("directory", type=Path)
    cli.add_argument("--port", type=int, default=8883, choices=range(1, 65536), metavar="PORT")
    args = cli.parse_args()
    os.umask(0o077)
    site = args.directory.resolve()
    pki = site / "pki"
    prepare(site, pki, args.port)
    subprocess.run([sys.executable, Path(__file__).with_name("demo-pki.py"), pki], check=True)
    print(site / "mosquitto.conf")
