"""Ejecución de ingesta y procesamiento Spark desde UI o scripts."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

BASE = Path(__file__).resolve().parent.parent

# Spark en primer arranque puede superar fácilmente 2–10 minutos (JVM, GraphFrames…).
def _timeout_ingesta() -> int:
    return int(os.environ.get("SIMLOG_INGESTA_TIMEOUT_SEC", "300"))


def _timeout_spark() -> int:
    return int(os.environ.get("SIMLOG_SPARK_TIMEOUT_SEC", "1800"))


def _run_subprocess(
    cmd: List[str],
    *,
    cwd: str,
    env: dict,
    timeout: int,
    label: str = "comando",
) -> Tuple[int, str, str]:
    """
    Ejecuta un subproceso y **nunca** lanza TimeoutExpired (evita tumbar Streamlit).
    Código 124 = timeout (convención tipo `timeout` de shell).
    """
    try:
        p = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        return p.returncode, p.stdout or "", p.stderr or ""
    except subprocess.TimeoutExpired as e:
        out = e.stdout if isinstance(e.stdout, str) else (e.stdout or b"").decode(errors="replace")
        err = e.stderr if isinstance(e.stderr, str) else (e.stderr or b"").decode(errors="replace")
        msg = (
            f"[TIMEOUT {timeout}s] {label}\n"
            f"Comando: {' '.join(cmd)}\n"
            "Sugerencia: en el primer arranque Spark puede tardar mucho. "
            "Exporta SIMLOG_SPARK_TIMEOUT_SEC=3600 (o mayor) antes de streamlit.\n"
            f"{err}"
        )
        return 124, out or "", msg


def ejecutar_ingesta(
    paso_15min: Optional[int] = None,
    *,
    simular_incidencias: Optional[bool] = None,
) -> Tuple[int, str, str]:
    """
    Si `paso_15min` es None, no se define PASO_15MIN y la ingesta usa el paso automático
    por reloj (`ingesta/trigger_paso.py`) — útil para alinearse con cron/trigger.
    """
    env = os.environ.copy()
    if paso_15min is not None:
        env["PASO_15MIN"] = str(paso_15min)
    else:
        env.pop("PASO_15MIN", None)
    if simular_incidencias is None:
        env.pop("SIMLOG_SIMULAR_INCIDENCIAS", None)
    else:
        env["SIMLOG_SIMULAR_INCIDENCIAS"] = "1" if simular_incidencias else "0"
    t = _timeout_ingesta()
    return _run_subprocess(
        [sys.executable, "-m", "ingesta.ingesta_kdd"],
        cwd=str(BASE),
        env=env,
        timeout=t,
        label="ingesta KDD",
    )


def ejecutar_procesamiento() -> Tuple[int, str, str]:
    t = _timeout_spark()
    return _run_subprocess(
        [sys.executable, "-m", "procesamiento.procesamiento_grafos"],
        cwd=str(BASE),
        env=os.environ.copy(),
        timeout=t,
        label="procesamiento Spark (procesamiento_grafos)",
    )


def ejecutar_subfase_spark(subfase: str, paso_15min: int | None = None) -> Tuple[int, str, str]:
    """Subfases 3–5 parciales vía `procesamiento.fase_kdd_spark` (transformacion|mineria|interpretacion)."""
    env = os.environ.copy()
    if paso_15min is not None:
        env["PASO_15MIN"] = str(paso_15min)
    t = _timeout_spark()
    return _run_subprocess(
        [sys.executable, "-m", "procesamiento.fase_kdd_spark", "--fase", subfase],
        cwd=str(BASE),
        env=env,
        timeout=t,
        label=f"fase_kdd_spark --fase {subfase}",
    )


def ejecutar_fase_kdd(
    orden: int,
    paso_15min: int,
    *,
    simular_incidencias: Optional[bool] = None,
) -> Tuple[int, str, str]:
    """
    Ejecuta la fase KDD indicada (orden 1–5, alineado con `servicios.kdd_fases.FASES_KDD`).

    - 1–2: ingesta completa (mismo script; cubre selección + preprocesamiento).
    - 3–4: Spark parcial (`fase_kdd_spark`: transformación / minería).
    - 5: procesamiento completo (`procesamiento_grafos`) → Cassandra/Hive y dashboard.
    """
    if orden in (1, 2):
        return ejecutar_ingesta(paso_15min, simular_incidencias=simular_incidencias)
    if orden == 3:
        return ejecutar_subfase_spark("transformacion", paso_15min)
    if orden == 4:
        return ejecutar_subfase_spark("mineria", paso_15min)
    if orden == 5:
        return ejecutar_procesamiento()
    return 1, "", f"Fase KDD no soportada: orden={orden}"
