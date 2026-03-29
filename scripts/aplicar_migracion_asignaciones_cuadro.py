#!/usr/bin/env python3
"""
Crea la tabla `asignaciones_ruta_cuadro` con timeout de sesión largo.
Útil cuando `cqlsh -f ...` devuelve OperationTimedOut (acuerdo de esquema lento / carga).

Uso:
  cd ~/proyecto_transporte_global
  python3 scripts/aplicar_migracion_asignaciones_cuadro.py

Variables: CASSANDRA_HOST (por defecto 127.0.0.1), KEYSPACE en config (logistica_espana).
"""
from __future__ import annotations

import os
import sys

# Raíz del proyecto en sys.path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config import CASSANDRA_HOST, KEYSPACE


def main() -> int:
    try:
        from cassandra.cluster import Cluster
    except ImportError:
        print("Instala el driver: pip install cassandra-driver", file=sys.stderr)
        return 1

    host = (os.environ.get("CASSANDRA_HOST") or CASSANDRA_HOST or "127.0.0.1").split(",")[0].strip()
    cql = """
CREATE TABLE IF NOT EXISTS asignaciones_ruta_cuadro (
    dia DATE,
    id_camion TEXT,
    id_ruta TEXT,
    origen TEXT,
    destino TEXT,
    creado_en TIMESTAMP,
    PRIMARY KEY ((dia), id_camion)
) WITH CLUSTERING ORDER BY (id_camion ASC);
""".strip()

    cluster = Cluster([host], connect_timeout=60, control_connection_timeout=60)
    try:
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)
        session.execute(cql, timeout=180)
        print(f"OK: tabla asignaciones_ruta_cuadro en {KEYSPACE} (host={host})")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        print(
            "\nSi sigue fallando: nodetool status · revisar carga del nodo · "
            "conectar al host seed correcto (export CASSANDRA_HOST=...).",
            file=sys.stderr,
        )
        return 2
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
