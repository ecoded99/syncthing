#!/usr/bin/env python3
"""Verify folder relocation with two disposable peers and exact file checks."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import urllib.error
import urllib.request

import benchmark_bundles as bench


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--hold-seconds', type=int, default=0)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=False)
    bench.EXE = args.binary.resolve()
    source, destination, moved = [root / name for name in ('source', 'destination', 'moved folder')]
    source.mkdir()
    destination.mkdir()
    expected = os.urandom((1 << 20) + 123)
    (source / 'verified.bin').write_bytes(expected)
    (destination / '.stignore').write_text('ignored.txt\n')
    (destination / '.stversions').mkdir()
    (destination / '.stversions' / 'retained.txt').write_text('history')
    (destination / 'ignored.txt').write_text('local ignored data')
    ports = []
    while len(ports) < 4:
        port = bench.port()
        if port not in ports:
            ports.append(port)
    homes = [root / 'sender', root / 'receiver']
    ida, ka = bench.setup(homes[0], source, ports[0], ports[2], None, None)
    idb, kb = bench.setup(homes[1], destination, ports[1], ports[3], ida, ports[0])
    bench.addpeer(homes[0], idb, ports[1])

    def api(peer, path, method='GET', data=None):
        request = urllib.request.Request(f'http://127.0.0.1:{ports[2 + peer]}/rest/{path}',
            headers={'X-API-Key': [ka, kb][peer], 'Content-Type': 'application/json'},
            method=method, data=json.dumps(data).encode() if data is not None else None)
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = response.read()
            return json.loads(payload) if payload else None

    def wait(predicate):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                result = predicate()
                if result:
                    return result
            except (OSError, ValueError):
                pass
            time.sleep(.2)
        raise TimeoutError('Folder relocation check; inspect isolated console.log files')

    processes, logs = [None, None], []

    def start(peer):
        log = open(homes[peer] / 'console.log', 'ab')
        logs.append(log)
        options = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}
        processes[peer] = subprocess.Popen([str(bench.EXE), 'serve', '--home', str(homes[peer]),
            '--no-browser', '--no-tray', '--no-restart', '--no-upgrade', '--log-file=-'],
            env=bench.ENV, stdout=log, stderr=subprocess.STDOUT, **options)
        wait(lambda: api(peer, 'system/status'))

    folder = 'config/folders/benchmark'
    try:
        start(0)
        start(1)
        wait(lambda: (destination / 'verified.bin').is_file()
            and (destination / 'verified.bin').read_bytes() == expected)
        original = api(1, folder)
        try:
            api(1, folder, 'PATCH', {'path': str(moved)})
            raise AssertionError('active relocation accepted')
        except urllib.error.HTTPError as error:
            assert 'pause the folder' in error.read().decode()
        assert api(1, folder)['path'] == original['path']
        api(1, folder, 'PATCH', {'path': str(moved), 'paused': True})
        assert not moved.exists(), 'paused relocation touched the destination'
        assert (destination / 'verified.bin').read_bytes() == expected
        changed = api(1, folder)
        assert changed['paused'] and changed['devices'] == original['devices']
        # Resuming before moving must fail the existing marker/path check,
        # and must never turn missing files into deletions on the sender.
        api(1, folder, 'PATCH', {'paused': False})
        wait(lambda: api(1, 'db/status?folder=benchmark')['state'] == 'error')
        assert (source / 'verified.bin').read_bytes() == expected
        api(1, folder, 'PATCH', {'paused': True})
        moved.mkdir()
        api(1, folder, 'PATCH', {'paused': False})
        wait(lambda: api(1, 'db/status?folder=benchmark')['state'] == 'error')
        assert (source / 'verified.bin').read_bytes() == expected
        api(1, folder, 'PATCH', {'paused': True})
        moved.rmdir()
        assert destination.resolve().is_relative_to(root) and moved.resolve().is_relative_to(root)
        shutil.move(str(destination), str(moved))
        api(1, folder, 'PATCH', {'paused': False})
        wait(lambda: api(1, 'db/status?folder=benchmark')['state'] == 'idle')
        assert (moved / '.stfolder').exists()
        assert (moved / '.stignore').read_text() == 'ignored.txt\n'
        assert (moved / '.stversions' / 'retained.txt').read_text() == 'history'
        assert (moved / 'ignored.txt').read_text() == 'local ignored data'
        (source / 'after-move.txt').write_text('from sender')
        api(0, 'db/scan?folder=benchmark', 'POST')
        wait(lambda: (moved / 'after-move.txt').exists())
        (moved / 'from-moved.txt').write_text('from relocated peer')
        api(1, 'db/scan?folder=benchmark', 'POST')
        wait(lambda: (source / 'from-moved.txt').exists())
        assert (source / 'from-moved.txt').read_text() == 'from relocated peer'
        assert not (source / 'ignored.txt').exists()
        api(1, 'system/shutdown', 'POST')
        processes[1].wait(timeout=20)
        start(1)
        assert api(1, folder)['path'] == str(moved)
        assert api(1, 'system/status')['myID'] == idb
        for base in (source, moved):
            assert hashlib.sha256((base / 'verified.bin').read_bytes()).digest() == hashlib.sha256(expected).digest()
        result = {'active_change_rejected': True, 'saved_paused': True,
            'missing_destination_protected': True, 'missing_marker_protected': True, 'hidden_files_preserved': True,
            'bidirectional_sync_after_move': True, 'identity_and_path_survive_restart': True}
        (root / 'results.json').write_text(json.dumps(result, indent=2))
        print(json.dumps({'work': str(root), 'gui': f'http://127.0.0.1:{ports[3]}', **result}), flush=True)
        if args.hold_seconds:
            time.sleep(args.hold_seconds)
    finally:
        for peer in range(2):
            try:
                api(peer, 'system/shutdown', 'POST')
            except OSError:
                pass
        for process in processes:
            if process is None:
                continue
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()


if __name__ == '__main__':
    main()
