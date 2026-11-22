#!/bin/sh
# Run only the packaged executable and public Mosquitto clients on localhost.
set -eu
preview_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 - "$preview_dir" "${1:-$PWD/will-demo-data}" "${2:-}" <<'PY'
import errno, hashlib, json, os, pathlib, platform, pty, shutil
import signal, sqlite3, subprocess, sys, time

package = pathlib.Path(sys.argv[1]).resolve()
data = pathlib.Path(sys.argv[2]).resolve()
pki = pathlib.Path(sys.argv[3]).resolve() if sys.argv[3] else None
data.mkdir(parents=True, exist_ok=True)
if (data / 'broker.sqlite').exists():
    raise SystemExit('Use a new demo directory; an existing database is never reset.')
port = int(os.environ.get('MQTTIUMD_DEMO_PORT', '18887'))
if not 1 <= port <= 65535:
    raise SystemExit('Invalid demo port')
build = json.loads((package / 'BUILD.json').read_text())
result = {
    'format': 'mqttiumd.packaged-will-demo', 'status': 'RUNNING',
    'transport': 'mtls-tls13' if pki else 'loopback-cleartext',
    'source_sha': build['source_sha'], 'binary_sha256': hashlib.sha256((package / 'mqttiumd').read_bytes()).hexdigest(),
    'binary_features': build['features'], 'instrumented': build['instrumented'],
    'build_platform': build['platform'], 'build_machine': build['machine'],
    'execution_platform': platform.platform(), 'execution_machine': platform.machine(),
    'database': str(data / 'broker.sqlite'), 'address': f'127.0.0.1:{port}',
    'processes': [], 'commands': [], 'observations': [], 'clients': {},
}
children = []
broker = None


def require(value, code):
    if not value:
        raise AssertionError(code)


def save():
    (data / 'will-demo.json').write_text(json.dumps(result, indent=2) + '\n')


def pump():
    for child in children:
        while child['fd'] is not None:
            try:
                part = os.read(child['fd'], 65536)
            except BlockingIOError:
                break
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                part = b''
            if not part:
                os.close(child['fd'])
                child['fd'] = None
                break
            require(len(child['output']) + len(part) <= 1024 * 1024, 'demo_client_log_budget_exceeded')
            child['output'] += part
            with child['log'].open('ab') as stream:
                stream.write(part)


def output(child):
    pump()
    return child['output'].decode('utf8', errors='replace').replace('\r\n', '\n')


def spawn(name, argv, *, stdin=False, env=None):
    # PTY line buffering exposes the clients' actual CONNACK/SUBACK debug
    # receipts without requiring stdbuf or a private client API.
    master, slave = pty.openpty()
    try:
        process = subprocess.Popen(argv, stdin=subprocess.PIPE if stdin else subprocess.DEVNULL,
                                   stdout=slave, stderr=slave, env=env)
    finally:
        os.close(slave)
    os.set_blocking(master, False)
    row = {'name': name, 'pid': process.pid, 'status': None, 'argv': argv,
           'output': str(data / f'{name}.log')}
    child = {'process': process, 'fd': master, 'output': b'', 'log': data / f'{name}.log', 'row': row}
    children.append(child)
    result['processes'].append(row)
    result['commands'].append(argv)
    save()
    return child


def wait_for(predicate, code, seconds=10):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        pump()
        if broker is not None and broker['process'].poll() is not None:
            raise AssertionError('packaged_will_broker_exited')
        value = predicate()
        if value:
            return value
        time.sleep(.01)
    raise AssertionError(code)


def wait_status(child, expected, code):
    try:
        status = child['process'].wait(timeout=8)
    except subprocess.TimeoutExpired as error:
        raise AssertionError(code + '_timeout') from error
    child['row']['status'] = status
    pump()
    save()
    require(status == expected, code)


def event_files():
    return sorted(data.glob('logs/run.*/events.jsonl'))


def events():
    found = []
    for path in event_files():
        for line in path.read_text().splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if broker is not None and str(event.get('pid')) == str(broker['process'].pid):
                found.append(event)
    return found


def start():
    global broker
    launcher = [str(package / ('start-tls.sh' if pki else 'start.sh')), str(data)]
    if pki:
        launcher.append(str(pki))
    broker = spawn(f'broker-{sum(c["row"]["name"].startswith("broker-") for c in children)}',
                   launcher,
                   env=dict(os.environ, MQTTIUMD_LISTEN=f'127.0.0.1:{port}'))
    ready = wait_for(lambda: next((e for e in events() if e.get('kind') == 'ready'), None),
                     'packaged_will_broker_not_ready')
    if pki:
        require(ready['details']['profile'] == 'mtls-will-pubsub-v1', 'packaged_will_tls_profile_missing')
    result['observations'].append({'case': 'broker-ready', 'pid': broker['process'].pid,
                                   'ready': ready, 'events': [str(p) for p in event_files()]})


def stop():
    global broker
    if broker is not None:
        broker['process'].send_signal(signal.SIGINT)
        wait_status(broker, 0, 'packaged_will_broker_shutdown_failed')
        broker = None


def counts():
    with sqlite3.connect((data / 'broker.sqlite').as_uri() + '?mode=ro', uri=True) as db:
        require(db.execute('PRAGMA quick_check').fetchone()[0] == 'ok', 'packaged_will_sqlite_consistency')
        require(db.execute('PRAGMA user_version').fetchone()[0] == (13 if pki else 12), 'packaged_will_wrong_schema')
        values = {name: db.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
                  for name in ('sessions', 'routes', 'wills', 'retained', 'publications', 'payloads',
                               'obligations', 'outbound', 'inbound', 'operations')}
        values['will_states'] = dict(db.execute('SELECT state,count(*) FROM wills GROUP BY state'))
        if pki:
            values['session_owners'] = list(db.execute('SELECT hex(s.id), o.principal FROM sessions s '
                'JOIN security_session_owners o ON s.id=o.session ORDER BY s.id'))
        return values


common = ['-V', 'mqttv5', '-h', '127.0.0.1', '-p', str(port), '-k', '10']
status_topic = 'atelier/capteur-1/status'


def tls_args(identity):
    if pki is None:
        return []
    return ['--tls-version', 'tlsv1.3', '--cafile', str(pki / 'ca.crt'),
            '--cert', str(pki / f'{identity}.crt'), '--key', str(pki / f'{identity}.key')]


def dashboard(name):
    child = spawn(name, [clients['mosquitto_sub'], *common, *tls_args('dashboard'), '-d', '-c', '-x', '4294967295',
                        '-i', 'dashboard' if pki else 'will-demo-dashboard', '-q', '1', '-t', 'atelier/+/status',
                        '-F', '%t|%p|%q|%r'])
    wait_for(lambda: 'received suback' in output(child).lower(), 'packaged_will_dashboard_no_suback')
    return child


def sensor(name, client_id, will_payload):
    identity = ['-i', 'capteur-1', '-c', '-x', '4294967295'] if pki else ['-i', client_id]
    child = spawn(name, [clients['mosquitto_pub'], *common, *tls_args('sensor'), '-d', '-l', *identity,
                        '-q', '1', '-r', '-t', status_topic,
                        '--will-topic', status_topic, '--will-payload', will_payload,
                        '--will-qos', '1', '-D', 'will', 'will-delay-interval', '0'], stdin=True)
    # A line provides the publisher's actual workload while its stdin remains
    # open; its own process, not a pipeline wrapper, is the later SIGKILL target.
    child['process'].stdin.write(b'online\n')
    child['process'].stdin.flush()
    wait_for(lambda: 'received connack (0)' in output(child).lower(), 'packaged_will_sensor_no_connack')
    wait_for(lambda: 'received puback' in output(child).lower(), 'packaged_will_online_not_committed')
    return child


def lines(child, value):
    return [line for line in output(child).splitlines() if line == f'{status_topic}|{value}|1|0']


def terminate_sensor(child):
    pid = child['process'].pid
    os.kill(pid, signal.SIGKILL)
    wait_status(child, -signal.SIGKILL, 'packaged_will_wrong_sensor_signal')
    child['process'].stdin.close()
    result['observations'].append({'case': 'sensor-sigkill', 'pid': pid, 'wait_status': -signal.SIGKILL})


try:
    require(result['binary_features'] == [] and result['instrumented'] is False, 'packaged_will_not_normal')
    clients = {}
    for name in ('mosquitto_pub', 'mosquitto_sub'):
        resolved = shutil.which(name)
        require(resolved is not None, 'mosquitto_client_missing')
        clients[name] = resolved
        version = subprocess.run([resolved, '--help'], capture_output=True, text=True, timeout=5)
        require(version.returncode in (0, 1) and 'version' in version.stdout, 'mosquitto_client_identity_missing')
        result['clients'][name] = {'path': str(pathlib.Path(resolved).resolve()),
                                  'sha256': hashlib.sha256(pathlib.Path(resolved).read_bytes()).hexdigest(),
                                  'version': version.stdout.splitlines()[:3]}
    start()
    observer = dashboard('dashboard-live')
    first = sensor('sensor-killed', 'capteur-1', 'offline')
    wait_for(lambda: lines(observer, 'online'), 'packaged_will_online_not_received')
    installed = counts()
    require(installed['wills'] == 1, 'packaged_will_not_durably_installed')
    if pki:
        require({owner for _, owner in installed['session_owners']} == {'sensor-1', 'dashboard'},
                'packaged_will_session_owner_missing')
        attack = [clients['mosquitto_pub'], *common, *tls_args('dashboard'), '-d', '-i', 'capteur-1',
                  '-q', '1', '-t', status_topic, '-m', 'forbidden']
        result['commands'].append(attack)
        refused = subprocess.run(attack, capture_output=True, text=True, timeout=8)
        require(refused.returncode != 0 and 'not authorized' in (refused.stdout + refused.stderr).lower(),
                'packaged_will_foreign_client_id_not_refused')
        require(first['process'].poll() is None and counts()['session_owners'] == installed['session_owners'],
                'packaged_will_hostile_takeover_changed_session')
        result['observations'].append({'case': 'foreign-client-id-refused', 'exit': refused.returncode,
                                       'client': refused.stdout + refused.stderr})
        denied = spawn('wildcard-refused', [clients['mosquitto_sub'], *common, *tls_args('dashboard'),
                       '-d', '-i', 'dashboard-check', '-q', '1', '-t', '#'])
        refused_event = wait_for(lambda: next((event for event in events()
            if event['kind'] == 'authorization_refused' and event['details']['stage'] == 'subscribe'), None),
            'packaged_will_wildcard_refusal_missing')
        require(counts()['routes'] == installed['routes'], 'packaged_will_forbidden_route_created')
        if denied['process'].poll() is None:
            denied['process'].send_signal(signal.SIGINT)
        denied['row']['status'] = denied['process'].wait(timeout=8)
        result['observations'].append({'case': 'wildcard-refused', 'broker_event': refused_event})
    terminate_sensor(first)
    wait_for(lambda: lines(observer, 'offline'), 'packaged_will_offline_not_received')
    wait_for(lambda: counts()['wills'] == 0 and counts()['obligations'] == 0,
             'packaged_will_live_responsibility_not_finished')
    require(len(lines(observer, 'offline')) == 1, 'packaged_will_duplicate_admission_observed')
    result['observations'].append({'case': 'online-sigkill-offline', 'installed': installed,
                                  'received': [line for line in output(observer).splitlines() if line.startswith(status_topic+'|')]})

    normal = sensor('sensor-normal', 'capteur-normal', 'offline')
    wait_for(lambda: len(lines(observer, 'online')) == 2, 'packaged_normal_sensor_online_missing')
    normal['process'].stdin.close()  # EOF of the public CLI means a voluntary MQTT DISCONNECT.
    wait_status(normal, 0, 'packaged_normal_sensor_disconnect_failed')
    require('sending disconnect' in output(normal).lower(), 'packaged_normal_disconnect_not_observed')
    wait_for(lambda: counts()['wills'] == 0 and counts()['obligations'] == 0,
             'packaged_normal_will_not_cancelled')
    quiet_deadline = time.monotonic() + .3
    while time.monotonic() < quiet_deadline:
        pump()
        require(len(lines(observer, 'offline')) == 1, 'packaged_normal_disconnect_published_will')
        time.sleep(.01)
    result['observations'].append({'case': 'normal-disconnect-cancelled', 'counts': counts(),
                                  'additional_offline': len(lines(observer, 'offline')) - 1})

    # Keep the dashboard's persistent subscription while its transport is gone.
    observer['process'].send_signal(signal.SIGINT)
    wait_status(observer, 0, 'packaged_dashboard_disconnect_failed')
    absent = sensor('sensor-offline-target', 'capteur-pending', 'offline-pending')
    terminate_sensor(absent)
    before = wait_for(lambda: (lambda value: value if value['wills'] == 0 and value['obligations'] == 2 else None)(counts()),
                      'packaged_offline_will_obligation_missing')
    stop()
    start()
    recovered = dashboard('dashboard-after-restart')
    wait_for(lambda: lines(recovered, 'offline-pending'), 'packaged_offline_will_not_recovered')
    wait_for(lambda: counts()['obligations'] == 0 and counts()['outbound'] == 0,
             'packaged_recovered_will_not_finished')
    require(len(lines(recovered, 'offline-pending')) == 1, 'packaged_recovered_will_duplicate')
    result['observations'].append({'case': 'offline-responsibility-restart', 'before': before,
                                  'after': counts(), 'received_offline_pending': len(lines(recovered, 'offline-pending'))})

    argv = [clients['mosquitto_sub'], *common, *tls_args('dashboard'), '-i',
            'dashboard-check' if pki else 'will-demo-retained-check', '-q', '1',
            '-t', status_topic, '-C', '1', '-W', '5', '-F', '%t|%p|%q|%r']
    result['commands'].append(argv)
    retained = subprocess.run(argv, capture_output=True, text=True, timeout=8)
    require(retained.returncode == 0 and retained.stdout.strip() == f'{status_topic}|online|1|1',
            'packaged_nonretained_will_overwrote_online_retained')
    result['observations'].append({'case': 'retained-online-after-will-and-restart', 'stdout': retained.stdout,
                                  'status': retained.returncode})
    recovered['process'].send_signal(signal.SIGINT)
    wait_status(recovered, 0, 'packaged_recovered_dashboard_disconnect_failed')
    final = wait_for(lambda: (lambda value: value if value['wills'] == 0 and value['obligations'] == 0 and value['outbound'] == 0 else None)(counts()),
                     'packaged_will_resources_not_drained')
    require(final['retained'] == final['publications'] == final['payloads'] == 1,
            'packaged_will_references_not_exact')
    result['observations'].append({'case': 'drained', 'counts': final})
    stop()
    result['status'] = 'PASS'
    save()
    print('PASS: online → actual sensor SIGKILL → offline QoS1; normal DISCONNECT cancelled its Will.')
    print('PASS: an offline Will obligation survived broker restart; non-retained Will left retained online unchanged.')
    print('Receipt:', data / 'will-demo.json')
except BaseException as error:
    result['status'] = 'FAIL'
    result['failure'] = {'type': type(error).__name__, 'detail': str(error)[:256]}
    save()
    raise
finally:
    for child in children:
        process = child['process']
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
        if child['row']['status'] is None:
            child['row']['status'] = process.returncode
        if process.stdin is not None and not process.stdin.closed:
            process.stdin.close()
    pump()
    save()
PY
