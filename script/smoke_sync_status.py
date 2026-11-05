#!/usr/bin/env python3
"""Exercise status with two disposable peers; never opens production config."""
import argparse
import hashlib
import json
import os
import secrets
import socket
from pathlib import Path
import subprocess
import time
import urllib.request
import xml.etree.ElementTree as ET

import benchmark_bundles as bench


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--transport', choices=['tcp', 'quic'], default='quic')
    parser.add_argument('--bundles', action='store_true')
    parser.add_argument('--hold-seconds', type=int, default=0, help='Keep disposable GUIs open for inspection after tests')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=False)
    bench.EXE = args.binary.resolve()
    bench.TRANSPORT = args.transport
    source, destination = root / 'source', root / 'destination'
    source.mkdir()
    destination.mkdir()
    for i in range(2):
        (source / f'large-{i}.bin').write_bytes(os.urandom((8 << 20) + 123))
    for i in range(128):
        folder = source / f'group-{i // 32}'
        folder.mkdir(exist_ok=True)
        (folder / f'small-{i}').write_bytes(os.urandom(16384))
    for i in range(12):
        (source / f'empty-{i}').touch()
    # Avoid the OS's outbound ephemeral range, which can reuse a not-yet-bound
    # test listener port while the second peer is still starting.
    ports = []
    while len(ports) < 4:
        candidate = 15000 + secrets.randbelow(25000)
        if candidate in ports:
            continue
        try:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', candidate))
            ports.append(candidate)
        except OSError:
            pass
    homes = [root / 'sender', root / 'receiver']
    ida, ka = bench.setup(homes[0], source, ports[0], ports[2], None, None)
    idb, kb = bench.setup(homes[1], destination, ports[1], ports[3], ida, ports[0])
    bench.addpeer(homes[0], idb, ports[1])
    for home in homes:
        tree = ET.parse(home / 'config.xml')
        cfg = tree.getroot()
        for name, value in [('maxSendKbps', 1024), ('limitBandwidthInLan', 'true'),
                ('progressUpdateIntervalS', 1), ('reconnectionIntervalS', 5)]:
            bench.settext(cfg.find('options'), name, value)
        bench.settext(cfg.find('folder'), 'experimentalBatchedTransfers', str(args.bundles).lower())
        for device in cfg.findall('device'):
            bench.settext(device, 'numConnections', 3)
        tree.write(home / 'config.xml', encoding='utf-8', xml_declaration=True)

    def api(peer, path, method='GET', data=None):
        request = urllib.request.Request(f'http://127.0.0.1:{ports[2 + peer]}/rest/{path}',
            headers={'X-API-Key': [ka, kb][peer], 'Content-Type': 'application/json'},
            method=method, data=json.dumps(data).encode() if data is not None else None)
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = response.read()
            return json.loads(payload) if payload else None

    def wait(predicate, seconds=90):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                result = predicate()
                if result:
                    return result
            except (OSError, ValueError):
                pass
            time.sleep(.25)
        raise TimeoutError('Disposable sync status check; inspect console.log')

    processes, logs, samples = [], [], []
    start = time.monotonic()
    try:
        for home in homes:
            log = open(home / 'console.log', 'wb')
            logs.append(log)
            options = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}
            processes.append(subprocess.Popen([str(bench.EXE), 'serve', '--home', str(home), '--no-browser',
                '--no-tray', '--no-restart', '--no-upgrade', '--log-file=-'], env=bench.ENV,
                stdout=log, stderr=subprocess.STDOUT, **options))
        print(json.dumps({'work': str(root), 'guis': [f'http://127.0.0.1:{p}' for p in ports[2:]]}), flush=True)
        wait(lambda: api(1, 'system/status'))

        def sample():
            if any(process.poll() is not None for process in processes):
                raise RuntimeError('disposable peer exited')
            summary = api(1, 'db/status?folder=benchmark')
            local = api(1, f'db/completion?folder=benchmark&device={idb}')
            remote = api(0, f'db/completion?folder=benchmark&device={idb}')
            assert summary.get('updatedAt') and local.get('updatedAt') and remote.get('updatedAt')
            assert 0 <= local['completion'] <= 100 and 0 <= remote['completion'] <= 100
            assert summary['needBytes'] >= 0 and local['needBytes'] >= 0 and remote['needBytes'] >= 0
            samples.append({'seconds': round(time.monotonic() - start, 3), 'summary': summary,
                'localCompletion': local, 'remoteCompletion': remote})
            return (summary['needTotalItems'] == 0 and summary['localFiles'] == 142
                and remote['needItems'] == 0 and remote['needDeletes'] == 0 and remote['needBytes'] == 0)

        wait(sample)
        expected = {str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in source.rglob('*') if path.is_file() and '.stfolder' not in path.parts}
        for name, digest in expected.items():
            assert hashlib.sha256((destination / name).read_bytes()).hexdigest() == digest, name
        partial = [row for row in samples if 0 < row['localCompletion']['completion'] < 99]
        assert len(partial) >= 3, 'local completion did not report active partial transfers'
        assert any(0 < row['remoteCompletion']['completion'] < 99 for row in samples), 'remote progress missing'
        # Sequential API snapshots differ by at most one second of capped work.
        comparable = [row for row in partial if row['summary']['sequence'] == row['localCompletion']['sequence']]
        assert all(abs(row['summary']['needBytes'] - row['localCompletion']['needBytes']) < 2 << 20
            for row in comparable), 'local completion disagrees with the receiving folder between index commits'

        api(1, 'config/folders/benchmark', 'PATCH', {'paused': True})
        wait(lambda: api(0, f'db/completion?folder=benchmark&device={idb}')['remoteState'] == 'paused')
        for path in source.rglob('*'):
            if path.is_file() and '.stfolder' not in path.parts:
                path.unlink()
        api(0, 'db/scan?folder=benchmark', 'POST')
        pending = wait(lambda: (value if (value := api(0,
            f'db/completion?folder=benchmark&device={idb}'))['needDeletes'] >= 142 else None))
        assert pending['globalBytes'] == 0 and pending['needBytes'] == 0
        api(1, 'config/folders/benchmark', 'PATCH', {'paused': False})
        wait(lambda: (value := api(1, 'db/status?folder=benchmark'))['localFiles'] == 0
            and value['needTotalItems'] == 0)
        wait(lambda: api(0, f'db/completion?folder=benchmark&device={idb}')['needDeletes'] == 0)
        result = {'transport': args.transport, 'bundles': args.bundles, 'files_verified': len(expected),
            'partial_local_reports': len(partial), 'deletions_with_zero_global_bytes': pending['needDeletes'],
            'resumed_after_pause': True, 'converged_after_deletion': True, 'samples': samples}
        (root / 'results.json').write_text(json.dumps(result, indent=2))
        print(json.dumps({key: value for key, value in result.items() if key != 'samples'}), flush=True)
        if args.hold_seconds:
            time.sleep(args.hold_seconds)
    finally:
        (root / 'samples.json').write_text(json.dumps(samples, indent=2))
        for peer in range(2):
            try:
                api(peer, 'system/shutdown', 'POST')
            except OSError:
                pass
        for process in processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                if os.name == 'nt':
                    subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'], stdout=subprocess.DEVNULL)
                else:
                    process.kill()
                process.wait()
        for log in logs:
            log.close()


if __name__ == '__main__':
    main()
