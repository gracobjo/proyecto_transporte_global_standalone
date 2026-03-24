param(
    [string]$Version = "2.6.0",
    [string]$InstallRoot = "$PSScriptRoot",
    [switch]$SkipJavaCheck
)

$ErrorActionPreference = "Stop"

function Get-JavaMajor {
    try {
        $out = & java -version 2>&1
        $txt = ($out | Out-String)
        # Soporta salidas tipo: java version "21.0.7" u openjdk version "17.0.11"
        if ($txt -match '"([0-9]+)(\.[0-9]+)?') {
            return [int]$Matches[1]
        }
        return $null
    } catch {
        return $null
    }
}

if (-not $SkipJavaCheck) {
    $javaMajor = Get-JavaMajor
    if ($null -eq $javaMajor -or $javaMajor -lt 17) {
        throw "NiFi 2.6 requiere Java 17+. Instala JDK 17 y verifica con: java -version"
    }
}

New-Item -ItemType Directory -Force -Path $InstallRoot | Out-Null
$zipName = "nifi-$Version.zip"
$zipPath = Join-Path $InstallRoot $zipName
$targetDir = Join-Path $InstallRoot "nifi-$Version"

if (-not (Test-Path $targetDir)) {
    $url = "https://archive.apache.org/dist/nifi/$Version/nifi-$Version-bin.zip"
    Write-Host "Descargando $url ..."
    Invoke-WebRequest -Uri $url -OutFile $zipPath

    Write-Host "Descomprimiendo en $InstallRoot ..."
    Expand-Archive -Path $zipPath -DestinationPath $InstallRoot -Force

    $expanded = Join-Path $InstallRoot "nifi-$Version-bin"
    if (Test-Path $expanded) {
        Rename-Item -Path $expanded -NewName "nifi-$Version" -Force
    }
}

$nifiHome = $targetDir
[Environment]::SetEnvironmentVariable("NIFI_HOME", $nifiHome, "User")

$envFile = Join-Path $InstallRoot ".env.nifi.local"
@(
    "NIFI_VERSION=$Version"
    "NIFI_HOME=$nifiHome"
    "NIFI_WEB_HTTP_PORT=8081"
    "NIFI_JVM_HEAP_INIT=512m"
    "NIFI_JVM_HEAP_MAX=1g"
) | Set-Content -Path $envFile -Encoding UTF8

Write-Host ""
Write-Host "NiFi instalado localmente."
Write-Host "NIFI_HOME: $nifiHome"
Write-Host "Siguiente paso: .\start_nifi_local_windows.ps1"
