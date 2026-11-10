Syncthing (ecoded) for Windows

Start Syncthing from the Start menu. It runs in the notification area and
opens the web interface. Use the tray menu to open the interface again
or quit Syncthing. Setup enables quiet startup at sign-in by default.

Existing installations
Quit any running Syncthing instance and disable its old startup entry
before switching to this installer. The new installation uses the same
default profile directory, %LOCALAPPDATA%\Syncthing, and keeps your device
ID, configuration, database and synced folders. If you used a custom
--home, update the shortcuts or keep using your existing launch command
with that path.

Updates and removal
The signed in-app updater still works. You can also quit from the tray
and run a newer installer over the same installation. To uninstall,
quit Syncthing and remove Syncthing (ecoded) from Settings > Apps.
The uninstaller removes the program and shortcuts; it leaves your profile
and synced files in place.

Optional: start before anyone signs in
Use Windows Task Scheduler for unattended PCs. This mode runs in the
background and has no tray icon.

1. Run setup with "Start Syncthing automatically when I sign in" unchecked
   (or rerun setup to clear it), then quit Syncthing.
2. As an administrator, open Task Scheduler and choose Create Task.
   Give the task a name such as Syncthing (ecoded) at boot. Set it to
   run as the Windows account that owns your Syncthing profile. Select
   "Run whether user is logged on or not" and leave "Run with highest
   privileges" unchecked.
3. Add an "At startup" trigger and a "Start a program" action. Browse to
   the installed syncthing.exe (normally in
   %LOCALAPPDATA%\Programs\SyncthingEcoded). Add these arguments:
   serve --no-browser --no-console --no-tray --home "C:\full\path\to\profile"
   Replace the example with the full path to that account's Syncthing
   profile. Use a local path and ensure the account can access its folders.
4. On the Settings tab, clear "Stop the task if it runs longer than".
   For a laptop, review the battery settings on the Conditions tab.
   Save the task and enter the account password in the Windows prompt.
   Run the task once to verify the web UI opens at http://127.0.0.1:8384
   (or at the address set in your profile).
5. Stop the task before reinstalling, and delete it before uninstalling.
