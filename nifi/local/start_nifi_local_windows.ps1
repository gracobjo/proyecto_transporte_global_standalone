param(
    [string]$InstallRoot = "$PSScriptRoot"
)

$ErrorActionPreference = "Stop"

$envFile = Join-Path $InstallRoot ".env.nifi.local"
if (-not (Test-Path $envFile)) {
    throw "No existe .env.nifi.local. Ejecuta primero install_nifi_local_windows.ps1"
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

$runBat = Join-Path $env:NIFI_HOME "bin\run-nifi.bat"
if (-not (Test-Path $runBat)) {
    throw "No se encuentra run-nifi.bat en $runBat"
}

$bootstrap = Join-Path $env:NIFI_HOME "conf\bootstrap.conf"
$txt = Get-Content $bootstrap -Raw
$txt = $txt -replace '(?m)^nifi\.bootstrap\.listen\.port=.*$', 'nifi.bootstrap.listen.port=9081'
$txt = $txt -replace '(?m)^java\.arg\.2=.*$', "java.arg.2=-Xms$($env:NIFI_JVM_HEAP_INIT)"
$txt = $txt -replace '(?m)^java\.arg\.3=.*$', "java.arg.3=-Xmx$($env:NIFI_JVM_HEAP_MAX)"
Set-Content -Path $bootstrap -Value $txt -Encoding UTF8

$nifiProps = Join-Path $env:NIFI_HOME "conf\nifi.properties"
$p = Get-Content $nifiProps -Raw
$p = $p -replace '(?m)^nifi\.web\.http\.port=.*$', "nifi.web.http.port=$($env:NIFI_WEB_HTTP_PORT)"
$p = $p -replace '(?m)^nifi\.web\.https\.port=.*$', 'nifi.web.https.port='
Set-Content -Path $nifiProps -Value $p -Encoding UTF8

Write-Host "Arrancando NiFi local..."
& $runBat
