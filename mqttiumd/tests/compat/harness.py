"""Start one broker per scenario and play a scripted exchange against it.

Two backends: the pinned Mosquitto reference and `mqttiumd`. Each call of
`broker()` starts a fresh process on a loopback port with fresh data, and
always stops and reaps it. Every wait has a deadline: a packet that does
not come is an observation, not a hang.
"""
from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import re
import select
import signal
import socket
import subprocess
import time

import mqttwire as wire

HOST = "127.0.0.1"
MOSQUITTO_VERSION = "2.1.4"
START_TIMEOUT = 15.0
STOP_TIMEOUT = 5.0
START_ATTEMPTS = 4
REFERENCE, BROKER = "reference", "mqttiumd"
BACKENDS = (REFERENCE, BROKER)
TAIL = 4000


class BrokerError(RuntimeError):
    """The broker could not be started: an infrastructure failure."""


def reference_binary(path: Path) -> Path:
    """The `mosquitto` executable named by a file, a build directory or
    the output directory of `tests/compat/build-reference.sh`."""
    candidates = [path, path / "src/mosquitto", path / f"mosquitto-{MOSQUITTO_VERSION}/src/mosquitto"]
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise BrokerError(f"no mosquitto executable at {path}")


def reference_version(binary: Path) -> str:
    """The version `mosquitto -h` prints."""
    done = subprocess.run([str(binary), "-h"], stdin=subprocess.DEVNULL, capture_output=True, text=True,
                          timeout=START_TIMEOUT, check=False)
    found = re.search(r"mosquitto version (\S+)", done.stdout + done.stderr)
    if not found:
        raise BrokerError(f"{binary} -h does not name a Mosquitto version")
    return found.group(1)


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind((HOST, 0))
        return probe.getsockname()[1]


def stop(child: subprocess.Popen) -> str:
    """Terminate, then kill, and always reap. Returns how it ended:
    `exited` (by itself, before being asked), `terminated` or `killed`."""
    if child.poll() is not None:
        return "exited"
    child.send_signal(signal.SIGTERM)
    try:
        child.wait(timeout=STOP_TIMEOUT)
        return "terminated"
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()
        return "killed"


def reference_config(directory: Path, port: int, extra) -> Path:
    lines = []
    if os.geteuid() == 0:
        # As root Mosquitto would switch to another account that cannot
        # read the scenario's files.
        lines.append("user root")
    lines += [f"listener {port} {HOST}", "allow_anonymous true", "persistence false", "log_dest stderr",
              "log_timestamp false", *extra]
    config = directory / "mosquitto.conf"
    config.write_text("\n".join(lines) + "\n")
    return config


def wait_ready(child: subprocess.Popen, log: Path, marker: str):
    """The match of `marker` in the log, or None when the process ended or
    the deadline passed first."""
    deadline = time.monotonic() + START_TIMEOUT
    while time.monotonic() < deadline:
        found = re.search(marker, log.read_text(errors="replace"))
        if found:
            return found
        if child.poll() is not None:
            return None
        time.sleep(0.005)
    return None


def accepts(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=1.0):
            return True
    except OSError:
        return False


def tail(path: Path) -> str:
    try:
        return path.read_text(errors="replace")[-TAIL:]
    except OSError:
        return ""


class Running:
    """One started broker: its port, and after the run how it ended."""

    def __init__(self, port: int):
        self.port = port
        self.ended = None       # exited | terminated | killed
        self.returncode = None
        self.log_tail = ""
        self.events_tail = ""

    def incidents(self) -> list:
        """The kinds of incident of this process, `classify.INCIDENTS`: a
        broker that ended before it was asked to (`exited`), that had to be
        killed (`killed`), or whose output mentions a panic (`panic`) did
        not merely answer differently. Several kinds can hold at once.
        """
        kinds = []
        if self.ended == "exited":
            kinds.append("exited")
        if self.ended == "killed":
            kinds.append("killed")
        if "panicked at" in self.log_tail:
            kinds.append("panic")
        return kinds

    def describe(self, kind: str) -> str:
        """One incident of `incidents()`, for a reader."""
        if kind == "exited":
            return f"exited by itself with status {self.returncode}"
        if kind == "killed":
            return f"did not stop within {STOP_TIMEOUT:.0f} s of SIGTERM and was killed"
        return "its output mentions a panic"


@contextmanager
def broker(backend: str, binary: Path, directory: Path, config=()):
    """Run one fresh broker; yields a `Running`.

    `binary` is the executable. `directory` must be new: it receives the
    configuration, the data and the log of this one process. `config` is
    extra configuration lines, for the reference only.
    """
    failure = "not started"
    for attempt in range(START_ATTEMPTS):
        work = directory / f"{backend}-{attempt}"
        work.mkdir(parents=True)
        log = work / "broker.log"
        events = work / "events.jsonl"
        if backend == REFERENCE:
            port = free_port()
            command = [str(binary), "-c", str(reference_config(work, port, config))]
            marker = r"mosquitto version \S+ running"
        elif backend == BROKER:
            port = None
            command = [str(binary), "--db", str(work / "broker.sqlite"), "--listen", f"{HOST}:0",
                       "--events", str(events)]
            marker = r"mqttiumd ready \S+:(\d+)"
        else:
            raise ValueError(backend)
        with log.open("wb") as output:
            child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=output,
                                     stderr=subprocess.STDOUT, cwd=work)
        running = None
        try:
            found = wait_ready(child, log, marker)
            if found and port is None:
                port = int(found.group(1))
            if found and accepts(port):
                running = Running(port)
                yield running
                return
            # For the reference, most often the port chosen above was
            # taken in the meantime.
            failure = tail(log)
        finally:
            ended = stop(child)
            if running is not None:
                running.ended, running.returncode = ended, child.returncode
                running.log_tail, running.events_tail = tail(log), tail(events)
    raise BrokerError(f"{backend} did not become ready: {failure}")


# --------------------------------------------------------------------------
# Scenario player
# --------------------------------------------------------------------------

class Connection:
    def __init__(self, sock: socket.socket, level: int):
        self.sock = sock
        self.level = level
        self.buffer = b""
        self.ended = None       # how the peer closed: `eof` or `reset`
        self.reset = False      # a read or a write met a connection reset
        self.broken = False     # the peer sent bytes that cannot be framed

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


def describe(data: bytes, level: int) -> dict:
    """Decode a packet for the transcript, or show its bytes."""
    try:
        return wire.decode(data, level)
    except wire.WireError as error:
        return {"type": "UNDECODABLE", "error": str(error), "hex": data.hex()}


ACKS = {"puback": wire.puback, "pubrec": wire.pubrec, "pubcomp": wire.pubcomp}
# How a read can find the connection closed by the peer.
CLOSED = ("eof", "reset")
# A step that asks for an order says so in its record; `normalize` sorts.
SORT = {False: {}, True: {"sort": True}}


class Player:
    """Plays the steps of one scenario against one broker."""

    def __init__(self, port: int):
        self.port = port
        self.connections: dict[str, Connection] = {}
        # Per connection name, the identifiers of the QoS 1 and QoS 2
        # PUBLISH packets the broker sent, in order of arrival.
        self.deliveries: dict[str, list[int]] = {}
        self.transcript: list[dict] = []

    def close_all(self) -> None:
        for connection in self.connections.values():
            connection.close()

    def play(self, steps) -> list[dict]:
        try:
            for step in steps:
                record = getattr(self, "do_" + step["op"])(step)
                if record is not None:
                    if "mark" in step:
                        record["mark"] = step["mark"]
                    self.transcript.append(record)
        finally:
            self.close_all()
        return self.transcript

    # -- reading ------------------------------------------------------------

    def note(self, name: str, packet: dict) -> None:
        if packet["type"] == "PUBLISH" and packet.get("qos"):
            self.deliveries.setdefault(name, []).append(packet["packet_id"])

    def read(self, name: str, *, count=None, deadline, until=None):
        """Collect packets until `count` of them, `until(packet)`, the end of
        the stream, or `deadline`.

        Returns `(packets, end)` with end one of `ok`, `timeout`, `eof` (the
        peer closed its end of the stream) and `reset` (a read or an earlier
        write on this connection met a connection reset). An end of stream
        seen first stays `eof`: what the peer's system answers to the
        client's later writes is not a reset by the peer.
        """
        connection = self.connections[name]
        got: list[dict] = []
        while True:
            if not connection.broken:
                try:
                    packets, connection.buffer = wire.split_packets(connection.buffer)
                except wire.WireError as error:
                    connection.broken = True
                    packets = []
                    got.append({"type": "UNDECODABLE", "error": str(error), "hex": connection.buffer.hex()})
                for index, data in enumerate(packets):
                    packet = describe(data, connection.level)
                    # What the packet took on the wire, fixed header included.
                    packet["wire_bytes"] = len(data)
                    self.note(name, packet)
                    got.append(packet)
                    if (count is not None and len(got) >= count) or (until is not None and until(packet)):
                        # Keep what follows for the next step.
                        connection.buffer = b"".join(packets[index + 1:]) + connection.buffer
                        return got, "ok"
            if connection.ended:
                return got, connection.ended
            now = time.monotonic()
            if now >= deadline:
                return got, "timeout"
            readable, _, _ = select.select([connection.sock], [], [], deadline - now)
            if not readable:
                continue
            try:
                chunk = connection.sock.recv(65536)
            except ConnectionResetError:
                connection.reset = True
                chunk = b""
            except OSError:
                chunk = b""
            if chunk:
                connection.buffer += chunk
            else:
                connection.ended = "reset" if connection.reset else "eof"

    def silent(self, name: str, *, window: float, limit: float):
        """Collect packets until none has come for `window` seconds.

        Returns `(packets, end)` with end `timeout` once the connection
        fell silent, `eof` or `reset` when the peer closed it, and
        `still-receiving` when packets kept coming for `limit` seconds. A
        window that starts again at each packet reads a slow answer to its
        end: one of a fixed length cut it wherever the machine was slower
        than the window.
        """
        got: list[dict] = []
        overall = time.monotonic() + limit
        while True:
            silent_at = time.monotonic() + window
            packets, end = self.read(name, count=1, deadline=min(silent_at, overall))
            got += packets
            if end == "timeout":
                return got, "timeout" if silent_at <= overall else "still-receiving"
            if end != "ok":
                return got, end

    # -- steps --------------------------------------------------------------

    def do_open(self, step):
        previous = self.connections.pop(step["conn"], None)
        if previous is not None:
            previous.close()
        try:
            sock = socket.create_connection((HOST, self.port), timeout=2.0)
        except OSError:
            return {"conn": step["conn"], "op": "open", "end": "refused"}
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.connections[step["conn"]] = Connection(sock, step["level"])
        return None

    def write(self, connection: Connection, data: bytes) -> None:
        try:
            connection.sock.settimeout(2.0)
            connection.sock.sendall(data)
        except ConnectionResetError:
            # The write took the error off the socket: the next read would
            # see an end of stream.
            connection.reset = True
        except OSError:
            # Whether a write to a closing socket fails depends on timing;
            # the next read of this connection shows the closure.
            pass

    def do_send(self, step):
        connection = self.connections.get(step["conn"])
        record = {"conn": step["conn"], "op": "send"}
        if "note" in step:
            record["note"] = step["note"]
        else:
            record["packet"] = describe(step["data"], connection.level if connection else 4)
        if connection is None:
            record["end"] = "not-open"
            return record
        self.write(connection, step["data"])
        return record

    def do_ack(self, step):
        connection = self.connections.get(step["conn"])
        record = {"conn": step["conn"], "op": "send"}
        if connection is None:
            return {**record, "end": "not-open"}
        try:
            packet_id = self.deliveries.get(step["conn"], [])[step["nth"]]
        except IndexError:
            # The PUBLISH this step answers never came; the read before it
            # shows that.
            return {**record, "end": "nothing-to-acknowledge", "kind": step["kind"]}
        data = ACKS[step["kind"]](packet_id, level=connection.level, reason=step["reason"])
        self.write(connection, data)
        return {**record, "packet": describe(data, connection.level)}

    def do_recv(self, step):
        record = {"conn": step["conn"], "op": "recv"}
        if step["conn"] not in self.connections:
            return {**record, "got": [], "end": "not-open"}
        got, end = self.read(step["conn"], count=step["count"], deadline=time.monotonic() + step["timeout"])
        return {**record, "got": got, "end": end, **SORT[bool(step["sort"])]}

    def do_quiet(self, step):
        """Record what arrives although nothing more is expected.

        With `ping`, a PINGREQ is sent first and everything up to its
        PINGRESP is read before the silent window starts, so that a slow
        broker is given the time of one round trip more. That PINGREQ and
        its PINGRESP belong to the harness and are not recorded. The step
        ends once nothing has come for `window` seconds (`quiet`), or
        after `limit` seconds of packets (`still-receiving`).
        """
        name = step["conn"]
        connection = self.connections.get(name)
        record = {"conn": name, "op": "quiet"}
        if connection is None:
            return {**record, "got": [], "end": "not-open"}
        got: list[dict] = []
        end = "timeout"
        if step["ping"] and not connection.ended:
            self.write(connection, wire.pingreq())
            got, end = self.read(name, deadline=time.monotonic() + step["timeout"],
                                 until=lambda packet: packet["type"] == "PINGRESP")
            if end == "ok":
                got.pop()
            elif end == "timeout":
                return {**record, "got": got, "end": "no-pingresp"}
        if end not in CLOSED:
            more, end = self.silent(name, window=step["window"], limit=step["limit"])
            got += more
        return {**record, "got": got, "end": "quiet" if end == "timeout" else end, **SORT[bool(step["sort"])]}

    def do_tally(self, step):
        """Record every packet that arrives, with what the step declares:
        `normalize.tally` reduces the record to its verdicts."""
        record = self.do_quiet({**step, "ping": True, "sort": False})
        declared = {key: step[key] for key in ("kind", "expected", "field", "sequence") if step.get(key) is not None}
        return {"conn": step["conn"], "op": "tally", **declared, "got": record["got"], "end": record["end"]}

    def do_measure(self, step):
        """Record every packet that arrives, with what the step declares:
        `normalize.measure` keeps what the protocol guarantees about the
        counted packets and leaves out how many came."""
        record = self.do_quiet({**step, "ping": True, "sort": False})
        declared = {key: step[key] for key in ("kind", "field", "sequence", "ordered")}
        return {"conn": step["conn"], "op": "measure", **declared, "got": record["got"], "end": record["end"]}

    def do_drain(self, step):
        """Read packets and answer each QoS 1 PUBLISH with its PUBACK at
        once, until `count` PUBLISH packets came (`ok`), nothing has come
        for `idle` seconds (`idle`), `timeout` seconds passed in all
        (`timeout`), or the peer closed. A PUBLISH at another QoS is
        recorded and not answered."""
        name = step["conn"]
        connection = self.connections.get(name)
        record = {"conn": name, "op": "drain", "expected": step["count"]}
        if connection is None:
            return {**record, "got": [], "acknowledged": 0, "end": "not-open"}
        got: list[dict] = []
        publishes = acknowledged = 0
        end = "ok"
        overall = time.monotonic() + step["timeout"]
        while publishes < step["count"]:
            idle_at = time.monotonic() + step["idle"]
            packets, end = self.read(name, count=1, deadline=min(idle_at, overall))
            got += packets
            for packet in packets:
                if packet["type"] != "PUBLISH":
                    continue
                publishes += 1
                if packet["qos"] == 1:
                    self.write(connection, wire.puback(packet["packet_id"], level=connection.level))
                    acknowledged += 1
            if end == "timeout":
                end = "idle" if idle_at <= overall else "timeout"
            if end != "ok":
                break
        return {**record, "got": got, "acknowledged": acknowledged, "end": end, **SORT[bool(step["sort"])]}

    def do_until_close(self, step):
        record = {"conn": step["conn"], "op": "until_close"}
        if step["conn"] not in self.connections:
            return {**record, "got": [], "end": "not-open"}
        got, end = self.read(step["conn"], deadline=time.monotonic() + step["timeout"])
        return {**record, "got": got, "end": end if end in CLOSED else "still-open"}

    def do_close(self, step):
        record = {"conn": step["conn"], "op": "close"}
        if step["wait_peer"] and step["conn"] in self.connections:
            got, _ = self.read(step["conn"], deadline=time.monotonic() + step["wait_peer"])
            if got:
                record["got"] = got
        connection = self.connections.pop(step["conn"], None)
        if connection is not None:
            connection.close()
        return record

    def do_sleep(self, step):
        time.sleep(step["seconds"])
        return {"op": "sleep", "seconds": step["seconds"]}


def run_scenario(scenario, backend: str, binary: Path, directory: Path):
    """Play `scenario` against a fresh broker.

    Returns `(transcript, running)`: the raw transcript (every packet
    received, in the order it came; `normalize` makes the comparable form) and the ended
    process, whose `incidents()` say whether it crashed or hung.
    """
    config = scenario.reference_config if backend == REFERENCE else ()
    with broker(backend, binary, directory, config) as running:
        transcript = Player(running.port).play(scenario.steps)
        # A broker that is gone before it is asked to stop did more than
        # answer differently: say so in the transcript too.
        time.sleep(0.02)
    if running.ended == "exited":
        transcript.append({"op": "broker", "end": "exited"})
    return transcript, running
