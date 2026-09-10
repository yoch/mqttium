#!/usr/bin/env python3
"""One fixed-rate diagnostic cell. Executes ONLY synthetic local traffic."""
from __future__ import annotations
import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from common import BASE, HEAD, BASE_SRC, HEAD_SRC, dump, trace_command, verify_source


def stop_process(p: subprocess.Popen | None) -> None:
    if p is None or p.poll() is not None:
        return
    p.terminate()
    try:
        p.wait(timeout=4)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=4)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--harness', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--source-label', choices=['base', 'head'], required=True)
    p.add_argument('--variant', choices=['pristine', 'original', 'fixed'], required=True)
    p.add_argument('--trace-mode', choices=['none', 'memory', 'flow'], default='none')
    p.add_argument('--target', choices=['none', 'rtt_initiator', 'responder', 'broker'], default='none')
    p.add_argument('--strace', default='strace')
    a = p.parse_args()
    a.source = a.source.resolve()
    a.harness = a.harness.resolve()
    a.output = a.output.resolve()
    a.output.mkdir(parents=True, exist_ok=True)
    if len(str(a.output / 'w' / 'barrier-00000000.sock').encode()) > 100:
        raise RuntimeError('Work path too long for AF_UNIX; use /tmp/ms-*')
    verify_source(a.source, BASE if a.source_label == 'base' else HEAD,
                  BASE_SRC if a.source_label == 'base' else HEAD_SRC)
    os.sched_setaffinity(0, {3})
    sys.path[:0] = [str(a.harness / 'src'), str(a.source / 'src')]
    from mqtt_client_bench import harness
    from mqtt_client_bench.scenarios import SCENARIO_BY_NAME, expand_scenario
    from mqtt_client_bench.quality import run_temporal_quality
    from mqtt_client_bench.workloads import encode_header

    # Imported original workload: do NOT silently add padding to telemetry256.
    assert len(encode_header(b'diag0000', 1, 1, 1, 1)) == 40
    point = dict(expand_scenario(SCENARIO_BY_NAME['application_rtt_fixed_rate'], 'standard')[0])
    point.update(target_rate=3942.0, pacer_mode='external', duration_s=12.0,
                 warmup_s=3.0, drain_s=6.0, temporal_trace_max_points=4096,
                 metric_sample_limit=50000, version_ab_target_frozen=True)
    assert point['protocol'] == 'MQTTv311' and point['qos_publish'] == 1
    cpus = {'sut': '0', 'broker': '1', 'loadgen': '2', 'orch': '3'}
    registry = a.output / 'pids.jsonl'
    handles = []
    children: list[subprocess.Popen] = []
    role_pids = {}
    watched = None
    broker = None
    original_spawn = harness._spawn_role

    def record(role: str, proc: subprocess.Popen, cmd: list[str], traced: bool) -> None:
        with registry.open('a') as f:
            f.write(json.dumps({'pid': proc.pid, 'role': role, 'traced_main_tid': traced,
                               'command': cmd, 'monotonic_ns': time.monotonic_ns(),
                               'realtime_ns': time.time_ns()}) + '\n')

    def spawn_role(script: str, config_path: str, cpuset: str | None = None) -> subprocess.Popen:
        role = Path(script).stem
        if role not in ('rtt_initiator', 'responder'):
            raise RuntimeError('Unexpected role: ' + role)
        cmd = ['taskset', '-c', cpuset or '3', sys.executable, '-m',
               'mqtt_client_bench.roles.' + role, '--config', config_path]
        traced = a.target == role and a.trace_mode != 'none'
        if traced:
            cmd = trace_command(a.trace_mode, a.output / (role + '.strace'), cmd, a.strace)
        env = os.environ.copy()
        env['PYTHONNOUSERSITE'] = '1'
        env['PYTHONPATH'] = os.pathsep.join([str(a.harness / 'src'), str(a.source / 'src')])
        log = (a.output / (role + '.log')).open('wb')
        handles.append(log)
        proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT)
        children.append(proc)
        role_pids[role] = proc.pid
        record(role, proc, cmd, traced)
        return proc

    def interrupted(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)

    try:
        # Dynamic isolated port. The owned broker must bind successfully; no
        # connection to an existing service is accepted if this process exits.
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        conf = a.output / 'mosquitto.conf'
        conf.write_text(f'listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n'
                        'set_tcp_nodelay true\nmax_inflight_messages 1000\n'
                        'max_queued_messages 100000\nlog_type error\n')
        cmd = ['taskset', '-c', '1', 'mosquitto', '-c', str(conf)]
        if a.target == 'broker':
            cmd = trace_command(a.trace_mode, a.output / 'broker.strace', cmd, a.strace)
        log = (a.output / 'broker.log').open('wb')
        handles.append(log)
        broker = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        record('broker', broker, cmd, a.target == 'broker')
        deadline = time.monotonic() + 10
        while True:
            if broker.poll() is not None:
                raise RuntimeError('Owned broker failed; see broker.log')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.2):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Owned broker startup timeout')
                time.sleep(.05)
        if Path(f'/proc/{broker.pid}/comm').read_text().strip() != 'mosquitto':
            raise RuntimeError('Broker PID is not Mosquitto (strace -D identity failed)')
        if a.variant != 'pristine':
            watch_cmd = [sys.executable, str(Path(__file__).with_name('proc_watch.py')),
                         '--registry', str(registry), '--output', str(a.output / 'proc'),
                         '--stop', str(a.output / 'stop-proc'), '--seconds', '175']
            log = (a.output / 'proc-watch.log').open('wb')
            handles.append(log)
            watched = subprocess.Popen(watch_cmd, stdout=log, stderr=subprocess.STDOUT)
        harness._spawn_role = spawn_role
        work = a.output / 'w'
        work.mkdir()
        dump(a.output / 'cell-config.json', {
            'library_sha': BASE if a.source_label == 'base' else HEAD,
            'harness_variant': a.variant, 'point': point, 'cpusets': cpus,
            'actual_payload_bytes': 40, 'traced_target': a.target,
            'trace_mode': a.trace_mode, 'trace_scope': 'target main TID only; pacer not traced',
            'performance_qualified': False, 'purpose': 'diagnosis, not a release gate',
            'collector': 'none' if a.variant == 'pristine' else '1Hz, CPU3',
            'strace_placement': 'CPU3, target re-pinned by taskset',
            'start_realtime_ns': time.time_ns(), 'start_perf_counter_ns': time.perf_counter_ns(),
        })
        result = harness.run_point(point, client='mqttium', client_path=str(a.source),
            host='127.0.0.1', port=port, tls_port=port, profile='standard', work_dir=work,
            cpusets=cpus, load_profile=None, host_profile=None, managed_broker=False,
            external_broker_pid=broker.pid, cross_client=True)
        # Keep the harness' original validity result, even if ptrace breaks pacing.
        result['temporal_quality'] = run_temporal_quality(result)
        result['syscall_diagnostic'] = {'performance_qualified': False, 'trace_mode': a.trace_mode,
            'target': a.target, 'variant': a.variant, 'actual_payload_bytes': 40}
        dump(a.output / 'result.json', result)
        roles = {w.get('role'): w for w in result.get('workers', [])}
        if set(roles) != {'rtt_initiator', 'responder'}:
            raise RuntimeError('Incomplete worker results; retained result.json')
        for role, worker in roles.items():
            if worker.get('native_async') is not True:
                raise RuntimeError(f'{role}: native-async path not used')
            module = Path(worker.get('client_module', '')).resolve()
            if not module.is_relative_to(a.source / 'src/mqttium'):
                raise RuntimeError(f'{role}: wrong imported library: {module}')
            if a.variant != 'pristine':
                runtime = worker.get('runtime', {})
                snap = runtime.get('measure_start', runtime.get('process_end', {}))
                expected_pid = role_pids[role]
                if snap.get('diagnostic_pid') != expected_pid:
                    raise RuntimeError(f'{role}: telemetry PID != actual target PID')
        return 0
    finally:
        harness._spawn_role = original_spawn
        for child in children:
            stop_process(child)
        stop_process(broker)
        (a.output / 'stop-proc').touch()
        if watched:
            try:
                watched.wait(timeout=3)
            except subprocess.TimeoutExpired:
                stop_process(watched)
        for handle in handles:
            handle.close()
        verify_source(a.source, BASE if a.source_label == 'base' else HEAD,
                      BASE_SRC if a.source_label == 'base' else HEAD_SRC)

if __name__ == '__main__':
    raise SystemExit(main())
