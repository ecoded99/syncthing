# Copyright (C) 2026 The Syncthing Authors.
# SPDX-License-Identifier: MPL-2.0
"""Exercise bundle crash recovery and reconnect using disposable loopback peers."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request

import benchmark_bundles as bench


def request(port, key, path, method='POST', body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(f'http://127.0.0.1:{port}/rest/{path}', data=data,
                                 headers={'X-API-Key': key, 'Content-Type': 'application/json'}, method=method)
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.read()


def wait_for(check, timeout=180):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if check():
                return
        except (OSError, ValueError):
            pass
        time.sleep(.1)
    raise TimeoutError('recovery/convergence timed out')


def scenario(root, source, manifest, action):
    root.mkdir()
    destination = root / 'received'
    destination.mkdir()
    ports = [bench.port() for _ in range(4)]
    homes = [root / 'sender', root / 'receiver']
    first_id, first_key = bench.setup(homes[0], source, ports[0], ports[2], None, None)
    second_id, second_key = bench.setup(homes[1], destination, ports[1], ports[3], first_id, ports[0])
    bench.addpeer(homes[0], second_id, ports[1])
    keys = [first_key, second_key]
    for home in homes:
        tree = bench.ET.parse(home / 'config.xml')
        tree.getroot().find('folder/experimentalBatchedTransfers').text = 'true'
        tree.write(home / 'config.xml', encoding='utf-8', xml_declaration=True)
    processes = [None, None]
    logs = []

    def launch(index):
        log = open(homes[index] / 'console.log', 'ab')
        logs.append(log)
        options = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {'start_new_session': True}
        processes[index] = subprocess.Popen([str(bench.EXE), 'serve', '--home', str(homes[index]),
            '--no-browser', '--no-tray', '--no-restart', '--no-upgrade', '--log-file=-'],
            env=bench.ENV, stdout=log, stderr=subprocess.STDOUT, **options)

    def stop(index, force=False):
        process = processes[index]
        if process is None or process.poll() is not None:
            return
        if not force:
            try:
                request(ports[index + 2], keys[index], 'system/shutdown')
                process.wait(timeout=15)
                return
            except (OSError, subprocess.TimeoutExpired):
                pass
        if os.name == 'nt':
            subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'], check=False,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=15)

    def status():
        # A disconnected source legitimately produces transient pull errors.
        # Require them to clear after reconnect instead of failing mid-retry.
        return bench.api(ports[3], second_key, 'db/status?folder=benchmark')

    def complete():
        result = status()
        return result['localFiles'] == bench.N and result['needTotalItems'] == 0 and result['pullErrors'] == 0

    def bundle_count():
        stats = bench.api(ports[3], second_key, 'system/connections')
        return sum(c.get('batchRequestsSent', 0) for c in stats['connections'].values())

    def equal_file(name, digest):
        path = destination / name
        return path.exists() and hashlib.sha256(path.read_bytes()).hexdigest() == digest

    try:
        launch(0)
        launch(1)
        wait_for(lambda: 64 <= status()['localFiles'] < bench.N and bundle_count() > 0)
        active_bundles = bundle_count()
        index = 1 if action == 'receiver-crash' else 0
        stop(index, force=action == 'receiver-crash')
        time.sleep(1)
        launch(index)
        wait_for(complete)
        bench.verify(destination, manifest)
        bench.verify(source, manifest)

        # Existing-file edits must still use normal synchronization.
        name = next(iter(manifest))
        updated = os.urandom(bench.SIZE)
        (source / name).write_bytes(updated)
        os.utime(source / name, ns=(1700000010000000000, 1700000010000000000))
        request(ports[2], first_key, 'db/scan?folder=benchmark')
        wait_for(lambda: equal_file(name, hashlib.sha256(updated).hexdigest()))

        # Disabling the rollout control must allow continued ordinary transfers.
        request(ports[3], second_key, 'config/folders/benchmark', 'PATCH', {'experimentalBatchedTransfers': False})
        before_disabled = bundle_count()
        extra = os.urandom(bench.SIZE)
        (source / 'after-disable.bin').write_bytes(extra)
        request(ports[2], first_key, 'db/scan?folder=benchmark')
        wait_for(lambda: equal_file('after-disable.bin', hashlib.sha256(extra).hexdigest()))
        assert bundle_count() == before_disabled, 'disabled receiver still requested bundles'

        # Deletions must converge after the ordinary fallback/disable path.
        (source / 'after-disable.bin').unlink()
        request(ports[2], first_key, 'db/scan?folder=benchmark')
        wait_for(lambda: not (destination / 'after-disable.bin').exists())
        result = {'scenario': action, 'verified_files': bench.N, 'bundles_before_interruption': active_bundles,
                  'existing_edit': 'passed', 'disable_and_transfer': 'passed', 'deletion': 'passed'}
        bench.emit(result)
        return result
    finally:
        stop(0)
        stop(1)
        for log in logs:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--work', required=True, type=Path)
    parser.add_argument('--files', type=int, default=2048)
    args = parser.parse_args()
    if args.files < 256:
        parser.error('--files must be at least 256 to interrupt an active transfer')
    root = args.work.resolve()
    if root.exists():
        parser.error('--work must be a new directory')
    root.mkdir(parents=True)
    bench.EXE, bench.N = args.binary.resolve(), args.files
    results = []
    for action in ('receiver-crash', 'sender-reconnect'):
        source = root / f'{action}-source'
        source.mkdir()
        manifest = {}
        for i in range(bench.N):
            path = source / f'dir-{i // 64:03d}' / f'file-{i:05d}.bin'
            path.parent.mkdir(exist_ok=True)
            data = os.urandom(bench.SIZE)
            path.write_bytes(data)
            os.utime(path, ns=(1700000000000000000, 1700000000000000000))
            manifest[path.relative_to(source).as_posix()] = (hashlib.sha256(data).hexdigest(), path.stat().st_mtime_ns)
        results.append(scenario(root / action, source, manifest, action))
        (root / 'results.json').write_text(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
