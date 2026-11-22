#!/bin/sh
# Demonstrate expiry with the packaged normal executable and public clients.
set -eu
preview_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 - "$preview_dir" "${1:-$PWD/message-expiry-demo-data}" <<'PY'
import json, os, pathlib, signal, sqlite3, subprocess, sys, time

package = pathlib.Path(sys.argv[1])
data = pathlib.Path(sys.argv[2]).resolve()
data.mkdir(parents=True, exist_ok=True)
if (data / 'broker.sqlite').exists():
    raise SystemExit('Use a new demo directory; an existing database is never reset.')
port = int(os.environ.get('MQTTIUMD_DEMO_PORT', '18886'))
if not 1 <= port <= 65535:
    raise SystemExit('Invalid demo port')
env = dict(os.environ, MQTTIUMD_LISTEN=f'127.0.0.1:{port}')
broker = None
children = []
observations = []

def save():
    (data / 'expiry-demo.json').write_text(json.dumps({
        'format': 'mqttiumd.packaged-message-expiry-demo', 'status': 'RUNNING',
        'database': str(data / 'broker.sqlite'), 'port': port,
        'source_sha': json.loads((package / 'BUILD.json').read_text())['source_sha'],
        'processes': children, 'observations': observations,
    }, indent=2) + '\n')

def wait_for(predicate, seconds=8):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if broker is not None and broker.poll() is not None:
            raise RuntimeError(f'broker exited: {broker.returncode}')
        value = predicate()
        if value:
            return value
        time.sleep(.01)
    raise RuntimeError('named demo condition not reached within its budget')

def events():
    result = []
    for path in sorted(data.glob('logs/run.*/events.jsonl')):
        for line in path.read_text().splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(value.get('pid')) == str(broker.pid):
                result.append(value)
    return result

def start():
    global broker
    log = (data / 'demo-starts.log').open('ab')
    broker = subprocess.Popen([str(package / 'start.sh'), str(data)], env=env,
                              stdout=log, stderr=subprocess.STDOUT)
    log.close()
    wait_for(lambda: any(e.get('kind') == 'ready' for e in events()))
    anchor = next(e['details'] for e in events() if e.get('kind') == 'session_clock_anchor')
    children.append({'pid': broker.pid, 'ready': True, 'clock': anchor})
    save()
    return anchor

def stop():
    global broker
    if broker is not None:
        broker.send_signal(signal.SIGINT)
        status = broker.wait(timeout=5)
        children[-1]['status'] = status
        broker = None
        if status != 0:
            raise RuntimeError(f'broker stop failed: {status}')
        save()

def mqtt(argv, name, allowed=(0,)):
    with (data / 'commands.jsonl').open('a') as stream:
        stream.write(json.dumps({'argv': argv}) + '\n')
    result = subprocess.run(argv, capture_output=True, timeout=10, text=True)
    (data / (name + '.stdout')).write_text(result.stdout)
    (data / (name + '.stderr')).write_text(result.stderr)
    observations.append({'command': name, 'status': result.returncode,
                         'stdout': result.stdout, 'stderr': result.stderr})
    save()
    if result.returncode not in allowed:
        raise RuntimeError(f'{name} failed: {result.returncode}')
    return result

def publish(value, expiry=None):
    argv = ['mosquitto_pub', '-V', 'mqttv5', '-h', '127.0.0.1', '-p', str(port),
            '-i', 'expiry-demo-source', '-q', '1', '-r',
            '-t', 'atelier/salle1/temperature', '-m', value]
    if expiry is not None:
        argv += ['-D', 'publish', 'message-expiry-interval', str(expiry)]
    mqtt(argv, value)

def dashboard(name, expect=None):
    result = mqtt(['mosquitto_sub', '-V', 'mqttv5', '-h', '127.0.0.1', '-p', str(port),
                   '-i', name, '-q', '1', '-t', 'atelier/+/temperature', '-C', '1',
                   '-W', '1' if expect is None else '5', '-F', '%t|%p|%q|%r'],
                  name, (27,) if expect is None else (0,))
    if expect is None:
        if result.stdout or 'Timed out' not in result.stderr:
            raise RuntimeError('absence probe was not a completed bounded subscriber wait')
    elif result.stdout.strip() != f'atelier/salle1/temperature|{expect}|1|1':
        raise RuntimeError('new dashboard did not receive the exact expected state')

def counts():
    with sqlite3.connect((data / 'broker.sqlite').as_uri() + '?mode=ro', uri=True) as db:
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise RuntimeError('SQLite consistency failed')
        return {name: db.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
                for name in ('retained', 'publications', 'payloads', 'obligations', 'outbound', 'inbound')}

def empty():
    current = counts()
    return current if not any(current.values()) else None

try:
    for client in ('mosquitto_pub', 'mosquitto_sub'):
        mqtt([client, '--help'], client + '-version', (0, 1))
    first = start()
    publish('expires-in-this-process', 2)
    admitted = counts()
    if admitted['retained'] != 1:
        raise RuntimeError('retained admission was not observed before expiry')
    expired = wait_for(empty)
    observations.append({'case': 'same-process', 'mechanism': 'established-expiry',
                         'admitted': admitted, 'after': expired})
    dashboard('expiry-dashboard-after-deadline')
    publish('timeless-state')
    dashboard('expiry-dashboard-valid', 'timeless-state')

    publish('expires-during-interruption', 2)
    before_stop = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
    stop()
    time.sleep(2.2)
    after_stop = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
    second = start()
    if first['domain'] == second['domain']:
        after = wait_for(empty)
        mechanism = 'established-expiry-in-continuous-domain'
    else:
        wait_for(lambda: any(e.get('kind') == 'message_expiry_sweep' and
                            e.get('details', {}).get('quarantined', 0) > 0 for e in events()))
        after = counts()
        if after['retained'] != 1:
            raise RuntimeError('unrecognized origin was not preserved in bounded quarantine')
        mechanism = 'temporal-quarantine-not-proven-expiry'
    observations.append({'case': 'process-restart', 'mechanism': mechanism,
                         'first_clock': first, 'second_clock': second, 'after': after,
                         'independent_boottime_before_stop': str(before_stop),
                         'independent_boottime_after_stop': str(after_stop)})
    dashboard('expiry-dashboard-after-restart')
    publish('fresh-without-expiry')
    dashboard('expiry-dashboard-new-state', 'fresh-without-expiry')
    stop()
    save()
    result = json.loads((data / 'expiry-demo.json').read_text())
    result['status'] = 'PASS'
    (data / 'expiry-demo.json').write_text(json.dumps(result, indent=2) + '\n')
    print('PASS: expired state was not replayed; a new timeless state was received.')
    print('Restart outcome:', mechanism)
except BaseException as error:
    save()
    result = json.loads((data / 'expiry-demo.json').read_text())
    result['status'] = 'FAIL'
    result['failure'] = {'type': type(error).__name__, 'detail': str(error)[:256]}
    (data / 'expiry-demo.json').write_text(json.dumps(result, indent=2) + '\n')
    raise
finally:
    if broker is not None and broker.poll() is None:
        broker.send_signal(signal.SIGINT)
        try:
            broker.wait(timeout=5)
        except subprocess.TimeoutExpired:
            broker.kill()
            broker.wait(timeout=2)
PY
