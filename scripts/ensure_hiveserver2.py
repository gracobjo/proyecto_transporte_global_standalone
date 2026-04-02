#!/usr/bin/env python3
"""
Asegura HiveServer2 + metastore (idempotente). Pensado para cron o systemd oneshot.

  */10 * * * * cd /ruta/proyecto && ./venv_transporte/bin/python scripts/ensure_hiveserver2.py

Código de salida: 0 si el puerto JDBC (SIMLOG_PORT_HIVE, 10000) responde al terminar.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from servicios.gestion_servicios import (  # noqa: E402
    PORT_HIVE,
    _hive_hosts,
    ensure_hiveserver2,
    puerto_activo_en_hosts,
)


def main() -> int:
    msg = ensure_hiveserver2()
    print(msg, flush=True)
    ok = puerto_activo_en_hosts(_hive_hosts(), PORT_HIVE)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
