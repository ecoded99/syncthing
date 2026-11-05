// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
// Exercise the real controller's event wiring and polling, without a browser.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(path.join(__dirname,
    '../gui/default/syncthing/core/syncthingController.js'), 'utf8');
let now = 1000;
let requests = [];
const handlers = {};
const scope = {
    scanProgress: {}, connectionsTotal: {}, connections: {},
    model: {}, folders: {}, completion: {}, deviceStats: {},
    $on: (name, callback) => { handlers[name] = callback; },
    emitHTTPError: () => {},
};
const context = vm.createContext({
    $scope: scope, angular: {extend: Object.assign},
    $translate: {instant: text => text},
    Events: {FOLDER_SCAN_PROGRESS: 'scan', FOLDER_SUMMARY: 'summary', FOLDER_COMPLETION: 'completion'},
    performance: {now: () => now}, console: {log: () => {}},
    urlbase: '/rest', prevDate: 0, connectionStatsRequest: undefined,
    $http: {get: () => {
        const request = {
            success(callback) { this.respond = callback; return this; },
            error() { return this; },
            finally(callback) { this.finish = callback; return this; },
        };
        requests.push(request);
        return request;
    }},
});
function load(start, end) {
    const first = source.indexOf(start);
    const last = source.indexOf(end, first);
    assert(first >= 0 && last > first, 'controller section missing');
    vm.runInContext(source.slice(first, last), context);
}
load('function counterRate(', 'function refreshErrors(');
load('$scope.$on(Events.FOLDER_SCAN_PROGRESS', '// May be called through .error');
load('$scope.scanPercentage = function', '$scope.scanRate = function');
load('$scope.scanRemaining = function', '$scope.deviceStatus = function');
load('$scope.folderStatusText = function', '$scope.deviceClass = function');
load('function recalcLocalStateTotal()', 'function refreshCompletion(');
load('$scope.syncPercentage = function', '$scope.scanPercentage = function');
load('$scope.deviceStatus = function', '$scope.folderStatusIcon = function');
load('$scope.$on(Events.FOLDER_SUMMARY', '$scope.$on(Events.FOLDER_ERRORS');
context.syncProgressTracker = context.newSyncProgressTracker();
assert(source.includes('setInterval(refreshConnectionStats, 1000)'), 'one-second polling lost');

function poll(total, processing = {}, startedAt = 'same') {
    context.refreshConnectionStats();
    const request = requests.at(-1);
    context.refreshConnectionStats();
    assert.equal(requests.at(-1), request, 'overlapping request was issued');
    request.respond({
        total: {inBytesTotal: total, outBytesTotal: total}, processing,
        connections: {peer: {inBytesTotal: total, outBytesTotal: total, startedAt}},
    });
    request.finish();
}
const MiB = 1024 * 1024;
poll(10 * MiB, {writtenBytesTotal: 0, appliedFilesTotal: 0});
assert.equal(scope.connectionsTotal.inbps, 0, 'initial lifetime total became a rate');
now += 1000;
poll(110 * MiB, {writtenBytesTotal: 100 * MiB, appliedFilesTotal: 200});
assert.equal(scope.connectionsTotal.inbps, 100 * MiB, 'burst averaged over ten seconds');
assert.equal(scope.processingStats.writtenBps, 100 * MiB);
assert.equal(scope.processingStats.appliedFilesPerSecond, 200);
now += 500;
poll(160 * MiB, {writtenBytesTotal: 150 * MiB, appliedFilesTotal: 300});
assert.equal(scope.connectionsTotal.inbps, 100 * MiB, 'fractional sample interval lost');
now += 1000;
poll(160 * MiB, {writtenBytesTotal: 150 * MiB, appliedFilesTotal: 300});
assert.equal(scope.connectionsTotal.inbps, 0, 'idle retained stale rate');
now += 1000;
poll(1, {writtenBytesTotal: 1, appliedFilesTotal: 1}, 'reconnected');
assert.equal(scope.connectionsTotal.inbps, 0, 'counter reset became a rate');
assert.equal(scope.connections.peer.inbps, 0);
assert.equal(scope.processingStats.writtenBps, 0);
assert.equal(context.counterRate(123, 1, 0), 0);
assert.equal(context.counterRate(Infinity, 1, 1), 0);

function progress(data) { handlers.scan(null, {data: {folder: 'test', ...data}}); }
progress({current: 50, total: 100, rate: 10, discoveryComplete: false,
    phase: 'discovering', scope: 'paths', reason: 'watcher', entriesVisited: 123});
assert.equal(scope.scanProgress.test.entriesVisited, 123);
assert.equal(scope.scanProgress.test.scope, 'paths');
assert.equal(scope.scanPercentage('test'), undefined, 'discovery field lost in event handler');
assert.equal(scope.scanRemaining('test'), '');
progress({discoveryComplete: true, phase: 'hashing'});
assert.equal(scope.scanPercentage('test'), 50);
progress({phase: 'deletions', deletionChecks: 256});
assert.equal(scope.scanPercentage('test'), undefined, 'hash percentage shown as deletion progress');
assert.equal(scope.scanRemaining('test'), '');
scope.scanProgress = {};
progress({current: 50, total: 100, rate: 0});
assert.equal(scope.scanPercentage('test'), 50, 'older backend compatibility lost');
assert.equal(scope.scanRemaining('test'), '', 'zero-rate ETA was shown');
scope.folderStatus = () => 'scanning';
for (const [phase, label] of Object.entries({
    discovering: 'Checking Local Files', hashing: 'Hashing Changed Files',
    deletions: 'Checking Deletions', indexing: 'Updating Local Index',
})) {
    progress({phase});
    assert.equal(scope.folderStatusText({id: 'test'}), label);
}
console.log('GUI polling, rates, resets, scan event wiring and phases passed');

// ETA follows logical folder work, including zero-byte changes. Traffic on
// another connection or folder must never influence it.
now = 1000;
const tracker = context.newSyncProgressTracker();
const report = (bytes, items = 1, globalBytes = 100) =>
    tracker.update('test', {needBytes: bytes, needTotalItems: items, globalBytes, globalTotalItems: 10}, false);
report(100);
assert.equal(tracker.read('test').seconds, undefined, 'invented initial ETA');
now += 2000; report(80);
now += 2000; report(60);
assert.equal(tracker.read('test').seconds, 6);
assert.equal(tracker.read('test').bytesPerSecond, 10);
now += 16000;
assert.equal(tracker.read('test').phase, 'waiting');
assert.equal(tracker.read('test').seconds, undefined, 'stopped work retained ETA');
now += 6000;
assert.equal(tracker.read('test').phase, 'stale');
report(60);
report(90, 2);
assert.equal(tracker.read('test').seconds, undefined, 'growing queue retained old ETA');
now += 2000; report(70, 2);
now += 2000; report(50, 2);
assert.equal(tracker.read('test').seconds, 5);
now += 1000; report(50, 2, 200);
assert.equal(tracker.read('test').seconds, undefined, 'changed global scope retained ETA');
report(0, 10, 0);
now += 2000; report(0, 8, 0);
now += 2000; report(0, 6, 0);
assert.equal(tracker.read('test').phase, 'finishing');
assert.equal(tracker.read('test').basis, 'items');
assert.equal(tracker.read('test').seconds, 6);
report(0, 0, 0);
now += 60000;
assert.equal(tracker.read('test').phase, 'complete', 'idle folder needs no heartbeat');
assert.equal(tracker.read('test').seconds, undefined);
const slow = context.newSyncProgressTracker();
for (const [bytes, elapsed] of [[100, 0], [80, 5000], [60, 5000]]) {
    now += elapsed;
    slow.update('peer', {needBytes: bytes, needItems: 1, globalBytes: 100, globalItems: 1}, true);
}
assert.equal(slow.read('peer').seconds, 15, 'default five-second peer reports never produced an estimate');
const frequent = context.newSyncProgressTracker();
for (let i = 0; i <= 24; i++) {
    now += 250;
    frequent.update('peer', {needBytes: 100 - i * 2.5, needItems: 1, globalBytes: 100, globalItems: 1}, true);
}
assert.equal(frequent.read('peer').bytesPerSecond, 10, 'frequent reports evicted ETA warmup');
assert.equal(frequent.read('peer').seconds, 4);

scope.folders.test = {id: 'test', devices: [{deviceID: 'peer'}]};
scope.deviceFolders = () => ['test'];
const peer = {deviceID: 'peer'};
scope.connections.peer = {connected: true};
scope.completion.peer = {};
assert.equal(scope.deviceStatus(peer), 'checking', 'unknown completion looked like syncing');
context.recalcCompletion('peer');
assert.equal(scope.completion.peer._needItems, undefined, 'unreported completion looked complete');
const remote = {globalBytes: 0, globalItems: 2, needBytes: 0, needItems: 2, needDeletes: 0, remoteState: 'valid'};
handlers.completion(null, {data: {device: 'peer', folder: 'test', ...remote}});
assert.equal(scope.completion.peer._needItems, 2, 'empty files disappeared from aggregate');
assert.equal(scope.deviceStatus(peer), 'syncing', 'pending empty files were shown in sync');
assert.equal(scope.remoteDataPercentage('peer', 'test'), undefined, 'invented percentage for zero-byte work');
assert.equal(scope.syncActivity('test', 'peer').phase, 'finishing');
scope.folders.other = {id: 'other', devices: [{deviceID: 'peer'}]};
context.recalcCompletion('peer');
assert.equal(scope.completion.peer._total, undefined, 'partial folder coverage produced a whole-device percentage');
delete scope.folders.other;
handlers.completion(null, {data: {device: 'peer', folder: 'test', ...remote, needItems: 0, needDeletes: 2}});
assert.equal(scope.completion.peer._needItems, 2, 'deletions disappeared from aggregate');
handlers.completion(null, {data: {device: 'peer', folder: 'test', ...remote, remoteState: 'paused'}});
assert.equal(scope.deviceStatus(peer), 'folder-unavailable');
assert.equal(scope.syncActivity('test', 'peer').seconds, undefined);
handlers.completion(null, {data: {device: 'peer', folder: 'test', ...remote, remoteState: 'notSharing'}});
assert.equal(scope.syncActivity('test', 'peer').phase, 'notSharing');
scope.connections.peer.connected = false;
assert.equal(scope.syncActivity('test', 'peer').phase, 'disconnected');
scope.connections.peer.connected = true;
scope.folders.test.paused = true;
assert.equal(scope.deviceStatus(peer), 'folders-paused');
scope.folders.test.paused = false;

const summary = {folder: 'test', summary: {state: 'syncing', globalBytes: 100,
    needBytes: 50, needTotalItems: 1, updatedAt: '2026-11-05T10:00:02Z'}};
handlers.summary(null, {data: summary});
handlers.summary(null, {data: {folder: 'test', summary: {...summary.summary, needBytes: 90, updatedAt: '2026-11-05T10:00:01Z'}}});
assert.equal(scope.model.test.needBytes, 50, 'late HTTP summary overwrote a newer event');
scope.model.test.state = 'idle';
scope.model.test.stateChanged = '2026-11-05T10:00:04Z';
handlers.summary(null, {data: {folder: 'test', summary: {...summary.summary,
    updatedAt: '2026-11-05T10:00:03Z', stateChanged: '2026-11-05T10:00:01Z'}}});
assert.equal(scope.model.test.state, 'idle', 'summary overwrote a newer state event');
assert.equal(scope.syncPercentage('test'), 50);
scope.model.test.needBytes = 0;
assert.equal(scope.syncPercentage('test'), 100, 'data progress was replaced with arbitrary 95%');
assert.equal(scope.syncActivity('test').seconds, undefined, 'insufficient samples produced an ETA');
scope.model.test = undefined;
assert.equal(scope.syncPercentage('test'), undefined, 'missing status looked complete');

progress({current: 100, total: 100, rate: 10, discoveryComplete: true, phase: 'hashing'});
assert.equal(scope.scanRemaining('test'), '', 'completed hashing displayed zero-second ETA');
progress({current: 50, total: 100, rate: 10});
now += 16000;
assert.equal(scope.scanRemaining('test'), '', 'stale scan report retained ETA');
console.log('Sync stages, ETA warmup, stalls, stale reports, scope changes, empty files, deletions and snapshot order passed');

// Replay real isolated-peer API samples through the exact estimator used by
// the controller. Optional paths are emitted by smoke_sync_status.py.
for (const filename of process.argv.slice(2)) {
    const recording = JSON.parse(fs.readFileSync(filename, 'utf8'));
    const samples = recording.samples || recording;
    const replay = context.newSyncProgressTracker();
    const estimates = [];
    for (const row of samples) {
        now = row.seconds * 1000;
        replay.update('remote', row.remoteCompletion, true);
        const reading = replay.read('remote');
        if (reading.seconds && row.remoteCompletion.needBytes > 0) {
            estimates.push({at: row.seconds, seconds: reading.seconds});
        }
    }
    assert(estimates.length >= 3, 'recording yielded too few real ETA samples');
    const finish = samples.at(-1).seconds;
    const errors = estimates.map(estimate => Math.abs(estimate.seconds - (finish - estimate.at)) / Math.max(5, finish - estimate.at)).sort((a, b) => a - b);
    const medianError = errors[Math.floor(errors.length / 2)];
    assert(medianError < 0.5, `ETA median relative error ${(medianError * 100).toFixed(1)}%: ${filename}`);
    console.log(JSON.stringify({recording: filename, etaSamples: estimates.length, medianRelativeError: medianError}));
}
