[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $Installer,

    [Parameter(Mandatory)]
    [string] $Binary
)

$ErrorActionPreference = 'Stop'
$Installer = (Resolve-Path -LiteralPath $Installer).Path
$ExpectedHash = (Get-FileHash -LiteralPath $Binary).Hash
$UninstallKey = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\SyncthingEcoded_is1'
$Menu = Join-Path ([Environment]::GetFolderPath('Programs')) 'Syncthing (ecoded).lnk'
$Startup = Join-Path ([Environment]::GetFolderPath('Startup')) 'Syncthing (ecoded).lnk'
if ((Test-Path $UninstallKey) -or (Test-Path $Menu) -or (Test-Path $Startup)) {
    throw 'Run the smoke test in a Windows account without an existing Syncthing (ecoded) installation.'
}

$InstallDir = Join-Path ([IO.Path]::GetTempPath()) "Syncthing Setup Test $([guid]::NewGuid())"
$Installed = Join-Path $InstallDir 'syncthing.exe'
$Shell = New-Object -ComObject WScript.Shell

function Invoke-SetupProcess($File, $Arguments) {
    $process = Start-Process -FilePath $File -ArgumentList $Arguments -WindowStyle Hidden -Wait -PassThru
    if ($process.ExitCode -ne 0) { throw "$File returned $($process.ExitCode)" }
}

function Assert-Shortcut($Path, $Arguments) {
    if (-not (Test-Path -LiteralPath $Path)) { throw "Missing shortcut: $Path" }
    $link = $Shell.CreateShortcut($Path)
    if ($link.TargetPath -ne $Installed -or $link.Arguments -ne $Arguments) {
        throw "Incorrect target or arguments: $Path"
    }
}

try {
    $arguments = @('/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART', '/RESTARTEXITCODE=99', "/DIR=`"$InstallDir`"")
    Invoke-SetupProcess $Installer $arguments
    $uninstaller = Join-Path $InstallDir 'unins000.exe'
    if ((Get-FileHash -LiteralPath $Installed).Hash -ne $ExpectedHash -or -not (Test-Path $UninstallKey)) {
        throw 'Setup did not install the expected binary or register the uninstaller.'
    }
    Assert-Shortcut $Menu 'serve --no-console'
    Assert-Shortcut $Startup 'serve --no-console --no-browser'

    # Reinstall without startup, then put it back and verify uninstall cleans it up.
    Invoke-SetupProcess $Installer ($arguments + '/MERGETASKS=!startup')
    if (Test-Path $Startup) { throw 'Opting out left the startup shortcut behind.' }
    Invoke-SetupProcess $Installer ($arguments + '/MERGETASKS=startup')
    Assert-Shortcut $Startup 'serve --no-console --no-browser'

    $sentinel = Join-Path $InstallDir 'keep-user-data.txt'
    Set-Content -LiteralPath $sentinel -Value 'Keep this file'
    Invoke-SetupProcess $uninstaller @('/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART')
    if ((Test-Path $Installed) -or (Test-Path $UninstallKey) -or (Test-Path $Menu) -or (Test-Path $Startup)) {
        throw 'Uninstall left the application, registry entry, or shortcuts behind.'
    }
    if (-not (Test-Path $sentinel)) { throw 'Uninstall removed a file it did not install.' }
    Remove-Item -LiteralPath $sentinel
    Remove-Item -LiteralPath $InstallDir
    Write-Host 'Installer smoke test passed.'
} finally {
    $uninstaller = Join-Path $InstallDir 'unins000.exe'
    if (Test-Path -LiteralPath $uninstaller) {
        Invoke-SetupProcess $uninstaller @('/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART')
    }
}
