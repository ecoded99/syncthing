// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
import AppKit
import Darwin

func runVersion(_ binary: URL) throws -> String {
    let task = Process()
    let output = Pipe()
    task.executableURL = binary
    task.arguments = ["--version"]
    task.standardOutput = output
    task.standardError = FileHandle.nullDevice
    try task.run()
    let text = String(decoding: output.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
    task.waitUntilExit()
    let fields = text.split(separator: " ")
    guard task.terminationStatus == 0, fields.count > 1, fields[1].hasPrefix("v") else {
        throw NSError(domain: "Syncthing", code: 1, userInfo: [NSLocalizedDescriptionKey: "Cannot read the Syncthing engine version."])
    }
    return String(fields[1])
}

// Keep the updatable engine outside the signed application bundle. A newer
// engine installed by Check for Updates must survive opening an older app.
func installEngine(_ bundled: URL, into directory: URL) throws -> URL {
    let manager = FileManager.default
    try manager.createDirectory(at: directory, withIntermediateDirectories: true)
    let destination = directory.appendingPathComponent("syncthing")
    let bundledVersion = try runVersion(bundled)
    if manager.fileExists(atPath: destination.path) {
        let installedVersion = try runVersion(destination)
        if installedVersion.compare(bundledVersion, options: .numeric) != .orderedAscending {
            return destination
        }
    }
    let temporary = directory.appendingPathComponent("syncthing-" + UUID().uuidString)
    defer { try? manager.removeItem(at: temporary) }
    try manager.copyItem(at: bundled, to: temporary)
    // Atomic replacement leaves an already running process's inode intact.
    guard rename(temporary.path, destination.path) == 0 else {
        throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno))
    }
    return destination
}

let bundledEngine = Bundle.main.resourceURL!.appendingPathComponent("syncthing")
if CommandLine.arguments.contains("--self-test") {
    do {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let engine = try installEngine(bundledEngine, into: directory)
        let version = try runVersion(engine)
        let before = try FileManager.default.attributesOfItem(atPath: engine.path)[.systemFileNumber] as! NSNumber
        _ = try installEngine(bundledEngine, into: directory)
        let after = try FileManager.default.attributesOfItem(atPath: engine.path)[.systemFileNumber] as! NSNumber
        guard before == after else { throw NSError(domain: "Syncthing", code: 2) }
        print("Launcher self-test passed: \(version); existing engine preserved")
        exit(0)
    } catch { fputs("Launcher self-test failed: \(error)\n", stderr); exit(1) }
}

final class AppDelegate: NSObject, NSApplicationDelegate {
    var engine: URL?
    var task: Process?
    var stopping = false

    func applicationDidFinishLaunching(_ notification: Notification) {
        let menu = NSMenu()
        let root = NSMenuItem()
        let applicationMenu = NSMenu()
        applicationMenu.addItem(withTitle: "Open Syncthing", action: #selector(openBrowser), keyEquivalent: "o").target = self
        applicationMenu.addItem(NSMenuItem.separator())
        applicationMenu.addItem(withTitle: "Quit Syncthing", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
        root.submenu = applicationMenu
        menu.addItem(root)
        NSApp.mainMenu = menu
        do {
            let support = try FileManager.default.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
            engine = try installEngine(bundledEngine, into: support.appendingPathComponent("Syncthing/bin"))
            let process = Process()
            process.executableURL = engine
            process.arguments = ["serve", "--log-file", support.appendingPathComponent("Syncthing/syncthing.log").path]
            process.standardOutput = FileHandle.nullDevice
            process.standardError = FileHandle.nullDevice
            process.terminationHandler = { process in
                DispatchQueue.main.async {
                    if self.stopping { NSApp.reply(toApplicationShouldTerminate: true) }
                    else {
                        if process.terminationStatus != 0 {
                            self.showError("The sync engine stopped. Check the Syncthing log in Library/Application Support/Syncthing for details.")
                        }
                        NSApp.terminate(nil)
                    }
                }
            }
            try process.run()
            task = process
        } catch {
            showError(error.localizedDescription)
            NSApp.terminate(nil)
        }
    }

    @objc func openBrowser() {
        guard let engine = engine else { return }
        let browser = Process()
        browser.executableURL = engine
        browser.arguments = ["browser"]
        try? browser.run()
    }

    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        openBrowser()
        return false
    }

    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        guard let process = task, process.isRunning else { return .terminateNow }
        stopping = true
        process.terminate() // The Syncthing monitor forwards SIGTERM for a clean shutdown.
        return .terminateLater
    }

    func showError(_ message: String) {
        let alert = NSAlert()
        alert.messageText = "Syncthing"
        alert.informativeText = message
        alert.runModal()
    }
}

let application = NSApplication.shared
let delegate = AppDelegate()
application.delegate = delegate
application.setActivationPolicy(.regular)
application.run()
