# MQTTiumd durable restart preview

This Linux archive contains a normal release broker, a loopback mTLS
launcher and a fixture for a single durable restart. `BUILD.json` names
the ELF, its dynamic loader, glibc symbol requirements, broker revision
and build options. `PACKAGE.json` names the packaging revision and the
binary that the separate receipt must cover.

## Verify, extract and exercise

Use a native machine of the architecture in `BUILD.json`, a compatible
glibc runtime, OpenSSL and Python 3.12 or newer. Install the clients into
a private virtual environment.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install mqttium==1.3.0 paho-mqtt==2.1.0
```

From the downloaded GitHub artifact, use its verifier and the TAR SHA-256
from the separately published receipt or the verified workflow output.
The verifier checks the whole archive before extraction and refuses an
incompatible host.

```sh
.venv/bin/python preview_archive.py "$archive" --sha256 "$trusted_sha256" \
  --out ./restart-observation --restart
```

The new output directory contains the extracted archive, `preflight.json`
and `receipt/restart.json`. Leave at least 128 MiB available there for the
archive extraction and short fixture. The downloaded TAR and the Python
environment take separate space. The full site storage gate budgets three
concurrent copies of site state.

The fixture provisions short-lived demo certificates in a temporary private
directory. Paho creates a persistent QoS 1 subscription and disconnects.
MQTTium publishes while the subscriber is offline and waits for PUBACK.
The broker confirms its obligation in SQLite, then restarts on the same
file. A certificate claiming the other principal's ClientID receives reason
`0x87`. The legitimate Paho client receives Session Present and the exact
queued payload, then acknowledges it. The receipt records both running
executables, the file inode and the two event logs.

On Raspberry Pi 5, run the same command as an unprivileged operator with
the AArch64 TAR and its published digest. The receipt applies to the machine
where the command actually ran. The workload uses a fresh database, two
OS-assigned loopback ports and disposable PKI. It covers restart delivery.
LAN, physical storage endurance and whole-site recovery require separate
reception.
