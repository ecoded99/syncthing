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
    $on: (name, callback) => { handlers[name] = callback; },
    emitHTTPError: () => {},
};
const context = vm.createContext({
    $scope: scope, angular: {extend: Object.assign},
    $translate: {instant: text => text},
    Events: {FOLDER_SCAN_PROGRESS: 'scan'},
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
