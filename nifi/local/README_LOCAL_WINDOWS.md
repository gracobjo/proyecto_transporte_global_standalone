# NiFi local en Windows (sin Docker)

Este proyecto puede ejecutar NiFi de forma local nativa en Windows.

## Requisitos

- JDK 17 o superior (`java -version`)
- PowerShell
- Acceso a internet para descargar binario de Apache NiFi

## Instalación (NiFi 2.6)

Desde la raíz del proyecto:

```powershell
powershell -ExecutionPolicy Bypass -File ".\nifi\local\install_nifi_local_windows.ps1"
```

Esto:

- descarga `nifi-2.6.0-bin.zip`,
- lo descomprime en `.\nifi\local\nifi-2.6.0`,
- crea `.\nifi\local\.env.nifi.local` con puertos y memoria.

## Arranque

```powershell
powershell -ExecutionPolicy Bypass -File ".\nifi\local\start_nifi_local_windows.ps1"
```

URL esperada:

- `http://localhost:8081/nifi`

## Parada

```powershell
powershell -ExecutionPolicy Bypass -File ".\nifi\local\stop_nifi_local_windows.ps1"
```

## Notas

- Se usa puerto HTTP `8081` para evitar colisiones con otros servicios.
- Si necesitas cambiar memoria o puerto, edita `.\nifi\local\.env.nifi.local`.
- El flujo JSON del proyecto está en `nifi/flow/simlog_kdd_canvas_import.json`.
