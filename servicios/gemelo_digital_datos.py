"""
Carga de la red gemelo desde Hive (PyHive whitelist / beeline CSV) o generador local.
Tracking Cassandra con columnas estrictas para el gemelo.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from servicios.consultas_cuadro_mando import (
    ejecutar_cassandra_consulta,
    ejecutar_hive_consulta,
    ejecutar_hive_sql_seguro,
)
from servicios.gemelo_digital_grafo import generar_grafo


def _limpiar_col(c: str) -> str:
    c = c.strip()
    if "." in c:
        c = c.split(".")[-1]
    return c.lower()


def _tsv_texto_a_filas(texto: str) -> List[Dict[str, Any]]:
    lineas = [ln for ln in (texto or "").strip().split("\n") if ln.strip()]
    if not lineas:
        return []
    sep = "\t" if "\t" in lineas[0] else ","
    cab_raw = [c.strip() for c in lineas[0].split(sep)]
    cab = [_limpiar_col(c) for c in cab_raw]
    filas: List[Dict[str, Any]] = []
    for ln in lineas[1:]:
        partes = ln.split(sep)
        fila: Dict[str, Any] = {}
        for i, col in enumerate(cab):
            v = partes[i].strip() if i < len(partes) else None
            if v is None or v == "" or str(v).upper() == "NULL":
                fila[col] = None
            else:
                try:
                    if "." in v or "e" in v.lower():
                        fila[col] = float(v)
                    else:
                        fila[col] = int(v)
                except ValueError:
                    fila[col] = v
        filas.append(fila)
    return filas


def _normalizar_nodos(filas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in filas:
        out.append(
            {
                "id_nodo": str(r.get("id_nodo") or ""),
                "tipo": str(r.get("tipo") or ""),
                "lat": float(r["lat"]) if r.get("lat") is not None else 0.0,
                "lon": float(r["lon"]) if r.get("lon") is not None else 0.0,
                "id_capital_ref": str(r.get("id_capital_ref") or ""),
                "nombre": str(r.get("nombre") or ""),
            }
        )
    return out


def _normalizar_aristas(filas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in filas:
        out.append(
            {
                "src": str(r.get("src") or ""),
                "dst": str(r.get("dst") or ""),
                "distancia_km": float(r.get("distancia_km") or 0.0),
            }
        )
    return out


def cargar_red_gemelo() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    """
    Intenta Hive (tablas red_gemelo_*); si falla, usa el generador determinista en memoria.
    """
    ok_n, err_n, txt_n = ejecutar_hive_consulta("gemelo_red_nodos")
    ok_a, err_a, txt_a = ejecutar_hive_consulta("gemelo_red_aristas")
    if ok_n and ok_a and txt_n and txt_a:
        nodos = _normalizar_nodos(_tsv_texto_a_filas(txt_n))
        aristas = _normalizar_aristas(_tsv_texto_a_filas(txt_a))
        if nodos and aristas:
            return nodos, aristas, "hive"

    nodos_l, aristas_l = generar_grafo()
    return nodos_l, aristas_l, "generador_local"


def cargar_tracking_gemelo_cassandra() -> List[Dict[str, Any]]:
    """Solo id_camion, lat, lon, ultima_posicion (whitelist)."""
    ok, err, rows = ejecutar_cassandra_consulta("tracking_camiones_gemelo")
    if ok and rows:
        return rows
    try:
        from cassandra.cluster import Cluster

        from config import CASSANDRA_HOST, KEYSPACE

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        rows = list(
            session.execute(
                "SELECT id_camion, lat, lon, ultima_posicion FROM tracking_camiones"
            )
        )
        cluster.shutdown()
        out = []
        for r in rows:
            d = dict(r._asdict()) if hasattr(r, "_asdict") else dict(r)
            out.append(d)
        return out
    except Exception:
        return []


def cargar_transporte_ingesta_real_hive() -> List[Dict[str, Any]]:
    ok, err, txt = ejecutar_hive_consulta("transporte_ingesta_real_muestra")
    if not ok or not txt:
        return []
    return _tsv_texto_a_filas(txt)


def cargar_historial_tracking_hive(camion_id: str | None = None, limite: int = 100) -> List[Dict[str, Any]]:
    """
    Historial de tracking desde Hive (`tracking_camiones_historico` / `HIVE_TABLE_TRACKING_HIST`).
    Columnas alineadas con `persistencia_hive.TABLE_SCHEMAS`.
    """
    from config import HIVE_DB, HIVE_TABLE_TRACKING_HIST

    db = (HIVE_DB or "logistica_espana").strip()
    tabla = (HIVE_TABLE_TRACKING_HIST or "tracking_camiones_historico").strip()
    ts_col = "`timestamp`"
    lim = max(1, min(int(limite), 5000))

    sql = f"""
SELECT id_camion, origen, destino, nodo_actual, lat_actual, lon_actual, progreso_pct, distancia_total_km, {ts_col}
FROM {db}.{tabla}
""".strip()

    where_clauses: List[str] = []
    if camion_id:
        cid = str(camion_id).strip()
        if re.fullmatch(r"[A-Za-z0-9_.-]+", cid):
            where_clauses.append(f"id_camion = '{cid}'")
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    sql += f" ORDER BY {ts_col} DESC LIMIT {lim}"

    ok, _err, txt = ejecutar_hive_sql_seguro(sql)
    if not ok or not txt:
        return []
    return _tsv_texto_a_filas(txt)


def cargar_trayectorias_por_camion(historial: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Agrupa posiciones por camión ordenadas cronológicamente.
    
    Returns:
        Dict[str, List[Dict]] - {camion_id: [{lat, lon, timestamp, ...}, ...]}
    """
    trayectorias: Dict[str, List[Dict[str, Any]]] = {}
    
    for reg in historial:
        cid = str(reg.get("id_camion") or reg.get("camion_id") or "")
        if not cid:
            continue
        
        lat = reg.get("lat_actual") or reg.get("lat")
        lon = reg.get("lon_actual") or reg.get("lon")
        ts = reg.get("timestamp_posicion") or reg.get("timestamp") or reg.get("ts")
        
        if lat is not None and lon is not None:
            if cid not in trayectorias:
                trayectorias[cid] = []
            trayectorias[cid].append({
                "lat": float(lat),
                "lon": float(lon),
                "timestamp": str(ts) if ts else None,
                "nodo_actual": reg.get("nodo_actual"),
                "progreso_pct": reg.get("progreso_pct"),
                "origen": reg.get("origen"),
                "destino": reg.get("destino"),
            })
    
    for cid in trayectorias:
        trayectorias[cid].sort(key=lambda x: x["timestamp"] or "")
    
    return trayectorias
