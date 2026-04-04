#!/usr/bin/env python3
"""
Pipeline completo por terminal: ingesta KDD → procesamiento Spark → notificación + log.

Requiere: Cassandra, HDFS accesible, Kafka opcional (ingesta publica igualmente).

Uso:
  cd ~/proyecto_transporte_global && source venv_transporte/bin/activate
  python scripts/ejecutar_pipeline_completo_cli.py
  PASO_15MIN=3 python scripts/ejecutar_pipeline_completo_cli.py --sin-notificar
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
VENV_PY = BASE / "venv_transporte" / "bin" / "python"


def _run(cmd: list[str], *, cwd: Path) -> int:
    r = subprocess.run(cmd, cwd=str(cwd))
    return int(r.returncode or 0)


def main() -> int:
    ap = argparse.ArgumentParser(description="SIMLOG: ingesta + Spark + notificación (CLI).")
    ap.add_argument(
        "--sin-notificar",
        action="store_true",
        help="No enviar Telegram/correo ni escribir reports/simlog_pipeline_runs.log.",
    )
    ap.add_argument(
        "--solo-procesamiento",
        action="store_true",
        help="Solo ejecutar procesamiento_grafos.py (sin ingesta).",
    )
    args = ap.parse_args()

    py = str(VENV_PY) if VENV_PY.is_file() else sys.executable
    cwd = BASE
    detalle_parts: list[str] = []

    if not args.solo_procesamiento:
        rc = _run([py, "-m", "ingesta.ingesta_kdd"], cwd=cwd)
        detalle_parts.append(f"ingesta rc={rc}")
        if rc != 0:
            if not args.sin_notificar:
                _run(
                    [
                        py,
                        str(BASE / "scripts" / "notificar_pipeline_simlog.py"),
                        "--origen",
                        "cli",
                        "--fallo",
                        "--mensaje",
                        "Fallo en ingesta (ingesta.ingesta_kdd).",
                    ],
                    cwd=cwd,
                )
            print("[PIPELINE_CLI] FALLO — ingesta", file=sys.stderr)
            return rc

    rc_spark = _run([py, str(BASE / "procesamiento" / "procesamiento_grafos.py")], cwd=cwd)
    detalle_parts.append(f"spark rc={rc_spark}")

    if not args.sin_notificar:
        notify_cmd = [
            py,
            str(BASE / "scripts" / "notificar_pipeline_simlog.py"),
            "--origen",
            "cli",
        ]
        notify_cmd.append("--exito" if rc_spark == 0 else "--fallo")
        notify_cmd.extend(["--mensaje", "; ".join(detalle_parts)])
        _run(notify_cmd, cwd=cwd)

    if rc_spark != 0:
        print("[PIPELINE_CLI] FALLO — procesamiento_grafos", file=sys.stderr)
        return rc_spark

    print("[PIPELINE_CLI] OK — ingesta + procesamiento (ver reports/simlog_pipeline_runs.log si notificaste)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
