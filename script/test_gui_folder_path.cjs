// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(path.join(__dirname,
    '../gui/default/syncthing/core/syncthingController.js'), 'utf8');
const copy = value => JSON.parse(JSON.stringify(value));
const scope = {};
let saved, closed, ignoreFailure;
const context = vm.createContext({
    $scope: scope, angular: {copy}, $q: {when: value => Promise.resolve(value)},
    saveFolderIgnoresExisting: () => ignoreFailure ? Promise.reject(new Error('ignore write failed')) : undefined,
    folderList: folders => Object.values(folders),
    hideModal: () => { closed = true; },
});
function load(start, end) {
    const first = source.indexOf(start);
    const last = source.indexOf(end, first);
    assert(first >= 0 && last > first);
    vm.runInContext(source.slice(first, last), context);
}
load('$scope.editingFolderExisting = function', '$scope.editingFolderNew = function');
load('$scope.saveFolder = function', '$scope.isFolderTabDisabled = function');
function setup(editing, newPath, paused = false) {
    saved = undefined;
    closed = false;
    ignoreFailure = false;
    scope.currentFolder = {id: 'shared', path: newPath, paused,
        _originalPath: 'old', _editPath: true, _editing: editing,
        devices: [{deviceID: 'peer'}], _guiVersioning: {selector: 'none'}};
    scope.currentSharing = {selected: {peer: true}, encryptionPasswords: {}};
    scope.myID = 'local';
    scope.folders = {};
    scope.config = {};
    scope.validateXattrFilter = () => {};
    scope.internalVersioningEnabled = () => false;
    scope.emitHTTPError = () => {};
    scope.saveConfig = () => { saved = copy(scope.config.folders[0]); return Promise.resolve(); };
}
(async () => {
    for (const [editing, newPath, paused, expectPaused] of [
        ['existing', 'new', false, true], ['existing', 'new', true, true],
        ['existing', 'old', false, false], ['existing', 'old', true, true],
        ['new', 'new', false, false],
    ]) {
        setup(editing, newPath, paused);
        await scope.saveFolder();
        assert.equal(saved.paused, expectPaused);
        assert.equal(saved.path, newPath);
        assert.equal(saved.id, 'shared');
        assert(saved.devices.some(device => device.deviceID === 'peer'));
        assert(!('_originalPath' in saved) && !('_editPath' in saved));
        assert(closed);
    }
    setup('existing', 'new');
    ignoreFailure = true;
    await scope.saveFolder();
    assert.equal(saved, undefined, 'changed path committed after ignore write failed');
    assert(!closed);
    assert(scope.currentSharing.selected.peer, 'retry lost device selections');
    console.log('Folder path editor checks passed.');
})().catch(error => { console.error(error); process.exitCode = 1; });
