#ifndef AppVersion
  #error "Pass /DAppVersion=..."
#endif
#ifndef EnginePath
  #error "Pass /DEnginePath=..."
#endif

#define AppName "Syncthing (ecoded)"
#define StartupName "Syncthing (ecoded)"

[Setup]
AppId=SyncthingEcoded
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher=ecoded
AppPublisherURL=https://github.com/ecoded99/syncthing
DefaultDirName={localappdata}\Programs\SyncthingEcoded
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
MinVersion=10.0
SetupIconFile=..\..\assets\logo.ico
UninstallDisplayIcon={app}\syncthing.exe
CloseApplications=yes
RestartApplications=no
Compression=lzma2
SolidCompression=yes
WizardStyle=modern

[Tasks]
Name: "startup"; Description: "Start Syncthing automatically when I sign in"; GroupDescription: "Startup:"

[Files]
Source: "{#EnginePath}"; DestDir: "{app}"; DestName: "syncthing.exe"; Flags: ignoreversion
Source: "..\..\LICENSE"; DestDir: "{app}"
Source: "README.txt"; DestDir: "{app}"

[InstallDelete]
Type: files; Name: "{userstartup}\{#StartupName}.lnk"; Tasks: not startup

[Icons]
Name: "{userprograms}\{#AppName}"; Filename: "{app}\syncthing.exe"; Parameters: "serve --no-console"; WorkingDir: "{app}"
Name: "{userstartup}\{#StartupName}"; Filename: "{app}\syncthing.exe"; Parameters: "serve --no-console --no-browser"; WorkingDir: "{app}"; Tasks: startup

[Run]
Filename: "{app}\syncthing.exe"; Parameters: "serve --no-console"; WorkingDir: "{app}"; Description: "Launch Syncthing"; Flags: nowait postinstall skipifsilent
Filename: "{app}\README.txt"; Description: "View setup and unattended startup instructions"; Flags: shellexec postinstall skipifsilent unchecked
