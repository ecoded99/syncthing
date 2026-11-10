[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $Binary,

    [Parameter(Mandatory)]
    [ValidatePattern('^v[0-9]+\.[0-9]+\.[0-9]+-ecoded\.[0-9]+$')]
    [string] $Version,

    [string] $OutputDirectory = (Get-Location).Path,

    [string] $Compiler
)

$ErrorActionPreference = 'Stop'
$Binary = (Resolve-Path -LiteralPath $Binary).Path
$OutputDirectory = (New-Item -ItemType Directory -Force -Path $OutputDirectory).FullName

if (-not $Compiler) {
    $Compiler = (Get-Command ISCC.exe -ErrorAction SilentlyContinue).Source
    if (-not $Compiler) {
        $Compiler = @(
            "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe"
            "$env:ProgramFiles\Inno Setup 6\ISCC.exe"
            "$env:LOCALAPPDATA\Programs\Inno Setup 6\ISCC.exe"
        ) | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
    }
}
if (-not $Compiler) {
    throw 'Inno Setup 6.3 or newer is required. Install it or pass -Compiler with the path to ISCC.exe.'
}

$reported = & $Binary --version
if ($LASTEXITCODE -ne 0 -or $reported -notmatch "^syncthing $([regex]::Escape($Version)) .* windows-amd64\)") {
    throw "Expected a Windows amd64 Syncthing binary for $Version; got: $reported"
}

& $Compiler /Qp "/DEnginePath=$Binary" "/DAppVersion=$Version" "/O$OutputDirectory" `
    "/Fsyncthing-windows-amd64-$Version-setup" "$PSScriptRoot\windows\installer.iss"
if ($LASTEXITCODE -ne 0) { throw "Inno Setup failed ($LASTEXITCODE)" }
