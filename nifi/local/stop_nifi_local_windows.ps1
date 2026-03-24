param(
    [string]$InstallRoot = "$PSScriptRoot"
)

$ErrorActionPreference = "Stop"

$envFile = Join-Path $InstallRoot ".env.nifi.local"
if (-not (Test-Path $envFile)) {
    throw "No existe .env.nifi.local."
}

Get-Content $envFile | ForEach-Object {
    if ($_ -match "^\s*#") { return }
    if ($_ -match "^\s*$") { return }
    $parts = $_.Split("=", 2)
    if ($parts.Count -eq 2) {
        [Environment]::SetEnvironmentVariable($parts[0], $parts[1], "Process")
    }
}

if (-not $env:NIFI_HOME) {
    throw "NIFI_HOME no definido en .env.nifi.local"
}

$stopBat = Join-Path $env:NIFI_HOME "bin\stop-nifi.bat"
if (-not (Test-Path $stopBat)) {
    throw "No se encuentra stop-nifi.bat en $stopBat"
}

Write-Host "Parando NiFi local..."
& $stopBat
