"""Ejecución de ingesta y procesamiento Spark desde UI o scripts."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Tuple

BASE = Path(__file__).resolve().parent.parent


def ejecutar_ingesta(paso_15min: int) -> Tuple[int, str, str]:
    env = os.environ.copy()
    env["PASO_15MIN"] = str(paso_15min)
    p = subprocess.run(
        [sys.executable, "-m", "ingesta.ingesta_kdd"],
        cwd=str(BASE),
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
    )
    return p.returncode, p.stdout or "", p.stderr or ""


def ejecutar_procesamiento() -> Tuple[int, str, str]:
    p = subprocess.run(
        [sys.executable, "-m", "procesamiento.procesamiento_grafos"],
        cwd=str(BASE),
        capture_output=True,
        text=True,
        timeout=600,
        env=os.environ.copy(),
    )
    return p.returncode, p.stdout or "", p.stderr or ""
