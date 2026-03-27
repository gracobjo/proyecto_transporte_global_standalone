#!/usr/bin/env python3
"""
Ejecucion standalone del flujo de ingesta DGT + simulacion + procesamiento.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from ingesta.ingesta_kdd import main as ejecutar_ingesta_kdd  # noqa: E402
from ingesta.trigger_paso import resolver_paso_ingesta  # noqa: E402


def _python_proyecto() -> str:
    venv = BASE / "venv_transporte" / "bin" / "python"
    if venv.is_file():
        return str(venv)
    return sys.executable


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecutar ingesta DGT y pipeline SIMLOG")
    parser.add_argument("--paso", type=int, default=None, help="Ventana temporal (15 min)")
    parser.add_argument("--cache-only", action="store_true", help="Usar solo cache local del XML DGT")
    parser.add_argument("--no-dgt", action="store_true", help="Desactivar la fuente DGT y dejar solo simulacion")
    parser.add_argument("--skip-processing", action="store_true", help="Ejecutar solo la ingesta")
    args = parser.parse_args()

    paso = args.paso if args.paso is not None else resolver_paso_ingesta()
    env = os.environ.copy()
    env["SIMLOG_INGESTA_CANAL"] = env.get("SIMLOG_INGESTA_CANAL", "script_python")
    env["SIMLOG_INGESTA_ORIGEN"] = env.get("SIMLOG_INGESTA_ORIGEN", "script_dgt_datex2")
    env["SIMLOG_INGESTA_EJECUTOR"] = env.get("SIMLOG_INGESTA_EJECUTOR", "scripts/ejecutar_ingesta_dgt.py")
    env["SIMLOG_USE_DGT"] = "0" if args.no_dgt else "1"
    env["SIMLOG_DGT_ONLY_CACHE"] = "1" if args.cache_only else "0"
    os.environ.update(env)

    payload = ejecutar_ingesta_kdd(paso)
    resumen = {
        "timestamp": payload.get("timestamp"),
        "paso_15min": paso,
        "dgt": payload.get("resumen_dgt", {}),
        "alertas_operativas": payload.get("alertas_operativas", []),
        "camiones": len(payload.get("camiones", [])),
    }
    print(json.dumps(resumen, indent=2, ensure_ascii=False))

    if args.skip_processing:
        return 0

    r = subprocess.run(
        [_python_proyecto(), str(BASE / "procesamiento" / "procesamiento_grafos.py")],
        cwd=str(BASE),
        env={**env, "SIMLOG_ENABLE_HIVE": "1"},
        text=True,
    )
    return int(r.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
