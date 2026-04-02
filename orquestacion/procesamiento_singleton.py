"""
Exclusión mutua para `procesamiento_grafos.main()`.

`simlog_maestro.ejecutar_procesamiento` y `simlog_kdd_05_interpretacion` (fase_kdd_spark
interpretacion) ejecutan el mismo trabajo pesado; en paralelo compiten por Spark, Hive/Metastore
y Cassandra. Un flock evita solapamiento en un mismo host.

Desactivar (solo depuración): ``SIMLOG_DISABLE_PROCESAMIENTO_LOCK=1``.

Ruta del lock: ``SIMLOG_PROCESAMIENTO_LOCK_PATH`` (por defecto ``/tmp/simlog_procesamiento_grafos.lock``).
"""
from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


def _env_yes(name: str) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    return v in ("1", "true", "yes", "on")


@contextmanager
def lock_procesamiento_grafos() -> Iterator[None]:
    if _env_yes("SIMLOG_DISABLE_PROCESAMIENTO_LOCK"):
        yield
        return

    path = (os.environ.get("SIMLOG_PROCESAMIENTO_LOCK_PATH") or "").strip() or "/tmp/simlog_procesamiento_grafos.lock"
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fp = open(p, "a+", encoding="utf-8")
    try:
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        fp.close()
