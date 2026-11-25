#!/usr/bin/env python3
"""Measure real RSS, Private_Dirty, Threads, and FD count per idle MQTT 5 connection
for mqttiumd (plain & mTLS) vs Mosquitto (plain & mTLS)."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))
import run_arm64_bench as bench


def make_connect(cid: str) -> bytes:
    cid_b = cid.encode()
    vh = b"\x00\x04MQTT\x05\x02\x00\x3c\x00"
    pl = struct.pack("!H", len(cid_b)) + cid_b
    rem = len(vh) + len(pl)
    hdr = bytearray([0x10])
    x = rem
    while True:
        b = x % 128
        x //= 128
        if x > 0:
            b |= 0x80
        hdr.append(b)
        if x == 0:
            break
    return bytes(hdr) + vh + pl


def proc_metrics(pid: int) -> dict:
    status = {}
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if ":" in line:
                k, v = line.split(":", 1)
                status[k.strip()] = v.strip()
    smaps = {}
    try:
        with open(f"/proc/{pid}/smaps_rollup") as f:
            for line in f:
                if ":" in line:
                    k, v = line.split(":", 1)
                    smaps[k.strip()] = v.strip()
    except OSError:
        pass
    fds = len(os.listdir(f"/proc/{pid}/fd"))

    def kb(d: dict, k: str) -> int:
        val = d.get(k, "0 kB").split()[0]
        return int(val)

    return {
        "vm_rss_kb": kb(status, "VmRSS"),
        "rss_anon_kb": kb(status, "RssAnon"),
        "vm_data_kb": kb(status, "VmData"),
        "pss_kb": kb(smaps, "Pss"),
        "private_dirty_kb": kb(smaps, "Private_Dirty"),
        "threads": int(status.get("Threads", "0")),
        "fds": fds,
    }


def run_idle_test(
    label: str,
    cmd: list[str],
    port: int,
    counts: list[int],
    pki: Path | None,
    ready_ev: Path | None,
    log_path: Path,
) -> list[dict]:
    with log_path.open("wb") as logf:
        proc = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT)
    socks = []
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(f"{label} exited {proc.returncode}: {log_path.read_text()}")
            if ready_ev is not None:
                if ready_ev.exists() and b'"ready"' in ready_ev.read_bytes():
                    break
            else:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    pass
            time.sleep(0.02)
        time.sleep(0.15)
        base = proc_metrics(proc.pid)
        rows = [{"conns": 0, **base}]

        ctx = None
        if pki is not None:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.minimum_version = ssl.TLSVersion.TLSv1_3
            ctx.check_hostname = False
            ctx.load_verify_locations(cafile=str(pki / "ca.crt"))
            ctx.load_cert_chain(
                certfile=str(pki / "sensor.crt"), keyfile=str(pki / "sensor.key")
            )

        for target in counts:
            while len(socks) < target:
                idx = len(socks)
                raw = socket.create_connection(("127.0.0.1", port), timeout=5)
                raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                s = ctx.wrap_socket(raw, server_hostname="localhost") if ctx else raw
                s.sendall(make_connect(f"sensor-{idx}"))
                data = s.recv(64)
                if len(data) < 4 or data[0] != 0x20:
                    raise RuntimeError(f"{label} conn {idx}: bad CONNACK {data!r}")
                socks.append(s)
            time.sleep(0.35)
            m = proc_metrics(proc.pid)
            delta_rss = (m["vm_rss_kb"] - base["vm_rss_kb"]) * 1024 / target
            delta_dirty = (m["private_dirty_kb"] - base["private_dirty_kb"]) * 1024 / target
            delta_fd = (m["fds"] - base["fds"]) / target
            rows.append(
                {
                    "conns": target,
                    **m,
                    "bytes_per_conn_rss": round(delta_rss),
                    "bytes_per_conn_dirty": round(delta_dirty),
                    "fds_per_conn": round(delta_fd, 2),
                }
            )
        return rows
    finally:
        for s in socks:
            try:
                s.close()
            except OSError:
                pass
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default=str(REPO / "target/dist/mqttiumd"))
    ap.add_argument("--mosquitto", default="/usr/sbin/mosquitto")
    ap.add_argument("--out", default="/tmp/idle_rss_results.json")
    ap.add_argument("--plain-counts", default="128,256,400,512,768")
    ap.add_argument("--mtls-counts", default="128,256,400")
    ap.add_argument("--delivery-shards", type=int, default=0)
    ap.add_argument("--only-mqttiumd", action="store_true")
    args = ap.parse_args()

    plain_counts = [int(x) for x in args.plain_counts.split(",")]
    mtls_counts = [int(x) for x in args.mtls_counts.split(",")]
    shard_args = ["--delivery-shards", str(args.delivery_shards)] if args.delivery_shards > 0 else []
    tmp = Path(tempfile.mkdtemp(prefix="mqttiumd-idle-rss-"))
    try:
        pki = bench.make_pki(tmp, max(max(plain_counts), max(mtls_counts), 1024))
        results = {}
        if not args.only_mqttiumd:
            results["mosquitto_reference"] = bench.mosquitto_identity(args.mosquitto)

        p = bench.free_port()
        ev = tmp / "m_plain.ev"
        results["mqttiumd_plain"] = run_idle_test(
            "mqttiumd_plain",
            [
                args.binary,
                "--listen",
                f"127.0.0.1:{p}",
                "--events",
                str(ev),
                "--event-level",
                "ops",
                "--storage-mode",
                "volatile",
                "--max-connections",
                "1024",
                "--io-threads",
                "2",
                *shard_args,
            ],
            p,
            plain_counts,
            None,
            ev,
            tmp / "m_plain.log",
        )

        if not args.only_mqttiumd:
            p = bench.free_port()
            mconf = tmp / "mosq_plain.conf"
            mconf.write_text(
                f"listener {p} 127.0.0.1\nallow_anonymous true\npersistence false\nmax_connections -1\nlog_type error\n"
            )
            results["mosquitto_plain"] = run_idle_test(
                "mosquitto_plain",
                [args.mosquitto, "-c", str(mconf)],
                p,
                plain_counts,
                None,
                None,
                tmp / "mosq_plain.log",
            )

        p = bench.free_port()
        ev2 = tmp / "m_mtls.ev"
        results["mqttiumd_mtls"] = run_idle_test(
            "mqttiumd_mtls",
            [
                args.binary,
                "--listen",
                f"127.0.0.1:{p}",
                "--events",
                str(ev2),
                "--event-level",
                "ops",
                "--storage-mode",
                "volatile",
                "--max-connections",
                "1024",
                "--io-threads",
                "2",
                *shard_args,
                "--tls-cert",
                str(pki / "server.crt"),
                "--tls-key",
                str(pki / "server.key"),
                "--client-ca",
                str(pki / "ca.crt"),
                "--identities",
                str(pki / "identities.json"),
            ],
            p,
            mtls_counts,
            pki,
            ev2,
            tmp / "m_mtls.log",
        )

        if not args.only_mqttiumd:
            p = bench.free_port()
            mconf2 = tmp / "mosq_mtls.conf"
            mconf2.write_text(
                f"listener {p} 127.0.0.1\nallow_anonymous true\npersistence false\nmax_connections -1\n"
                f"cafile {pki / 'ca.crt'}\ncertfile {pki / 'server.crt'}\nkeyfile {pki / 'server.key'}\n"
                f"require_certificate true\ntls_version tlsv1.3\nlog_type error\n"
            )
            results["mosquitto_mtls"] = run_idle_test(
                "mosquitto_mtls",
                [args.mosquitto, "-c", str(mconf2)],
                p,
                mtls_counts,
                pki,
                None,
                tmp / "mosq_mtls.log",
            )

        out = Path(args.out)
        out.write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(results, indent=2))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
