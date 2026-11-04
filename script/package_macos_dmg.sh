#!/bin/bash
# Copyright (C) 2026 The Syncthing Authors.
# SPDX-License-Identifier: MPL-2.0
set -euo pipefail
binary=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
version=$2
output=$3
[[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-ecoded\.[0-9]+$ ]]
test "$(uname -m)" = arm64
file "$binary" | grep -q arm64
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
app="$work/staging/Syncthing.app"
mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources"
cp "$binary" "$app/Contents/Resources/syncthing"
chmod 755 "$app/Contents/Resources/syncthing"
xcrun swiftc -O -target arm64-apple-macosx12.0 "$root/script/macos/Launcher.swift" -o "$app/Contents/MacOS/Syncthing"
python3 - "$app/Contents/Info.plist" "$version" <<'PY'
import plistlib,sys
with open(sys.argv[1], 'wb') as f:
    plistlib.dump({
        'CFBundleName':'Syncthing', 'CFBundleDisplayName':'Syncthing',
        'CFBundleIdentifier':'com.ecoded.syncthing', 'CFBundleExecutable':'Syncthing',
        'CFBundlePackageType':'APPL', 'CFBundleIconFile':'Syncthing.icns',
        'CFBundleShortVersionString':sys.argv[2][1:].split('-')[0],
        'CFBundleVersion':sys.argv[2][1:].replace('-ecoded.', '.'),
        'LSMinimumSystemVersion':'12.0', 'NSHighResolutionCapable':True,
    }, f)
PY
mkdir "$work/icon.iconset"
for size in 16 32 128 256 512; do
  sips -z "$size" "$size" "$root/assets/logo-512.png" --out "$work/icon.iconset/icon_${size}x${size}.png" >/dev/null
done
iconutil -c icns "$work/icon.iconset" -o "$app/Contents/Resources/Syncthing.icns"
cp "$root/LICENSE" "$app/Contents/Resources/LICENSE"
codesign --force --deep --sign - "$app"
codesign --verify --deep --strict "$app"
"$app/Contents/MacOS/Syncthing" --self-test
ln -s /Applications "$work/staging/Applications"
cat > "$work/staging/Read Me.txt" <<EOF
Syncthing $version for Apple Silicon

Drag Syncthing.app to Applications, then open it. The interface opens in your
browser. Use the Syncthing app menu to reopen it or quit the sync engine.
Existing Syncthing settings and folders are preserved. Quit other Syncthing
launchers before starting this app. Check for Updates updates the engine.

This application is ad-hoc signed, not Apple Developer ID signed or notarized.
macOS may require you to allow this downloaded app in System Settings > Privacy
& Security after the first launch attempt.
EOF
hdiutil create -quiet -volname "Syncthing $version" -srcfolder "$work/staging" -format UDZO "$output"
hdiutil verify "$output"
mkdir "$work/mount"
hdiutil attach -quiet -nobrowse -readonly -mountpoint "$work/mount" "$output"
trap 'hdiutil detach -quiet "$work/mount" || true; rm -rf "$work"' EXIT
test -L "$work/mount/Applications"
cmp "$binary" "$work/mount/Syncthing.app/Contents/Resources/syncthing"
codesign --verify --deep --strict "$work/mount/Syncthing.app"
"$work/mount/Syncthing.app/Contents/MacOS/Syncthing" --self-test
hdiutil detach -quiet "$work/mount"
trap 'rm -rf "$work"' EXIT
