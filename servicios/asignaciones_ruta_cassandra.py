"""
Persistencia en Cassandra de asignaciones origen→destino desde el cuadro de mando.
Tabla dedicada `asignaciones_ruta_cuadro` (no la modifica Spark) + UPSERT en `tracking_camiones`.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from config import CASSANDRA_HOST, KEYSPACE
from config_nodos import get_nodos


def persistir_asignacion_y_tracking(id_camion: str, origen: str, destino: str) -> Tuple[bool, str, str]:
    """
    Inserta fila en `asignaciones_ruta_cuadro` y actualiza `tracking_camiones`
    (posición inicial en el nodo origen, ruta y marca temporal).
    Devuelve (ok, error, id_ruta).
    """
    cid = (id_camion or "").strip()
    o, d = (origen or "").strip(), (destino or "").strip()
    if not cid:
        return False, "id_camión vacío", ""
    nodos = get_nodos()
    if o not in nodos or d not in nodos:
        return False, "Origen o destino no existen en la topología configurada.", ""
    if o == d:
        return False, "Origen y destino deben ser distintos.", ""

    id_ruta = uuid.uuid4().hex[:12]
    dia = datetime.now(timezone.utc).date()
    lat = float(nodos[o]["lat"])
    lon = float(nodos[o]["lon"])
    ts = datetime.now(timezone.utc)

    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        session.execute(
            """
            INSERT INTO asignaciones_ruta_cuadro (dia, id_camion, id_ruta, origen, destino, creado_en)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (dia, cid, id_ruta, o, d, ts),
        )
        session.execute(
            """
            INSERT INTO tracking_camiones (
                id_camion, lat, lon, ruta_origen, ruta_destino, ruta_sugerida,
                estado_ruta, motivo_retraso, ultima_posicion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (cid, lat, lon, o, d, [o, d], "En ruta", None, ts),
        )
        cluster.shutdown()
        return True, "", id_ruta
    except Exception as e:
        err = str(e)
        if "asignaciones_ruta_cuadro" in err.lower() or "does not exist" in err.lower():
            err += (
                " — Ejecuta la migración: `cqlsh -f cassandra/migracion_asignaciones_ruta_cuadro.cql` "
                "(o aplica el bloque del `esquema_logistica.cql`)."
            )
        return False, err, ""


def listar_asignaciones_dia(d: Optional[date] = None) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """Lee asignaciones del día indicado (UTC hoy por defecto)."""
    dia = d or datetime.now(timezone.utc).date()
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        rows = list(
            session.execute(
                """
                SELECT dia, id_camion, id_ruta, origen, destino, creado_en
                FROM asignaciones_ruta_cuadro
                WHERE dia = %s
                """,
                (dia,),
            )
        )
        cluster.shutdown()
        out = []
        for r in rows:
            out.append(
                {
                    "dia": str(r.dia),
                    "id_camion": r.id_camion,
                    "id_ruta": r.id_ruta,
                    "origen": r.origen,
                    "destino": r.destino,
                    "creado_en": r.creado_en,
                }
            )
        return True, "", out
    except Exception as e:
        return False, str(e), []


