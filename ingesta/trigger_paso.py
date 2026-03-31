"""
Resolución del «paso» de simulación para ingesta periódica.

- Cada ventana de tiempo (p. ej. 15 min, o 2 min en demo) obtiene un **índice de slot**
  distinto → las posiciones GPS y la simulación (con semilla) **cambian** entre ciclos.
- OpenWeather se consulta en cada ejecución (datos reales actualizados).

Variables de entorno (ver `config.py`):
- `SIMLOG_INGESTA_INTERVAL_MINUTES` — duración de la ventana (15 producción, 2–5 presentación).
- `PASO_15MIN` — si está definido, **fija** el paso (Streamlit manual, pruebas).
- `SIMLOG_PASO_AUTOMATICO` — si es `0` y `PASO_15MIN` está definido, no recalcular (default: automático).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone


def intervalo_segundos() -> int:
    try:
        from config import SIMLOG_INGESTA_INTERVAL_MINUTES

        m = int(SIMLOG_INGESTA_INTERVAL_MINUTES)
    except Exception:
        m = int(os.environ.get("SIMLOG_INGESTA_INTERVAL_MINUTES", "15"))
    m = max(1, m)
    return m * 60


def paso_desde_reloj_utc(now_ts: float | None = None) -> int:
    """Índice de ventana entera según tiempo UTC (cambia cada intervalo)."""
    ts = now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp()
    return int(ts // intervalo_segundos())


def resolver_paso_ingesta_detalle() -> tuple[int, str]:
    """
    Devuelve (paso, modo):
    - modo `manual` si `PASO_15MIN` está definido (UI, Airflow, pruebas).
    - modo `auto` si no (cron/NiFi sin variable → índice global de ventana UTC).
    """
    manual = os.environ.get("PASO_15MIN")
    if manual is not None and str(manual).strip() != "":
        return int(manual), "manual"
    return paso_desde_reloj_utc(), "auto"


def resolver_paso_ingesta() -> int:
    """
    - `PASO_15MIN` en el entorno → valor entero (control manual del dashboard o Airflow).
    - Si no existe → `paso_desde_reloj_utc()` (cron/trigger sin variable).
    """
    return resolver_paso_ingesta_detalle()[0]


def semilla_simulacion(paso: int) -> int:
    """Semilla determinista por paso + día → incidentes/rutas distintos por ventana."""
    d = datetime.now(timezone.utc).date().toordinal()
    return paso * 1_000_003 + (d % 500_000)
