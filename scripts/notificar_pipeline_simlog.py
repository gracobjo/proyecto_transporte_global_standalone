#!/usr/bin/env python3
"""
Notificación unificada tras el pipeline (ingesta + Spark), vía terminal o NiFi.

- Telegram + correo: `servicios.notificaciones_eventos.notificar_ejecucion_pipeline`
  (respeta SIMLOG_TELEGRAM_*, SIMLOG_SMTP_*, SIMLOG_NOTIFY_EMAIL_*).

- Log local: reports/simlog_pipeline_runs.log (una línea por ejecución).

Uso:
  venv/bin/python scripts/notificar_pipeline_simlog.py --origen cli --exito
  venv/bin/python scripts/notificar_pipeline_simlog.py --origen nifi --fallo --mensaje "spark exit 1"
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent


def _append_log(ok: bool, origen: str, msg: str) -> Path:
    log_dir = BASE / "reports"
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / "simlog_pipeline_runs.log"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"{ts}\t{origen}\t{'OK' if ok else 'FAIL'}\t{msg.replace(chr(9), ' ').replace(chr(10), ' ')[:800]}\n"
    with path.open("a", encoding="utf-8") as f:
        f.write(line)
    return path


def main() -> int:
    sys.path.insert(0, str(BASE))
    ap = argparse.ArgumentParser(description="SIMLOG: notificar pipeline (Telegram, correo, log).")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--exito", action="store_true", help="Marcar ejecución como correcta.")
    g.add_argument("--fallo", action="store_true", help="Marcar ejecución como fallida.")
    ap.add_argument("--origen", choices=("nifi", "cli"), required=True, help="Origen del evento.")
    ap.add_argument(
        "--mensaje",
        default="",
        help="Detalle opcional (aparece en Telegram/correo y en el log).",
    )
    args = ap.parse_args()
    ok = bool(args.exito)

    from servicios.notificaciones_eventos import notificar_ejecucion_pipeline

    titulo = f"Pipeline SIMLOG ({args.origen.upper()}) — {'éxito' if ok else 'fallo'}"
    notificar_ejecucion_pipeline(titulo, ok, args.mensaje)
    log_path = _append_log(ok, args.origen, args.mensaje or titulo)
    estado = "OK" if ok else "FALLO"
    print(f"[NOTIFY] {estado} origen={args.origen} · log: {log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
