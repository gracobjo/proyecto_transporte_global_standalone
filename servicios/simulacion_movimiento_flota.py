"""
Simulación de movimiento de camiones sobre la red (BFS + interpolación por distancia).

Actualiza `tracking_camiones` en Cassandra para que el mapa del cuadro de mando muestre
el desplazamiento hacia el destino; al completar la ruta se pueden generar alertas y correo.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from config import CASSANDRA_HOST, KEYSPACE
from config_nodos import get_nodos
from servicios.red_hibrida_rutas import bfs_ruta, construir_adyacencia


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def interpolar_posicion_sobre_ruta(
    path: List[str],
    nodos: Dict[str, Any],
    t: float,
) -> Tuple[float, float]:
    """
    `t` en [0, 1]: posición a lo largo de la polilínea por nodos (peso = distancia haversine por tramo).
    """
    t = max(0.0, min(1.0, float(t)))
    if not path:
        return 40.4, -3.7
    if len(path) == 1:
        n = nodos[path[0]]
        return float(n["lat"]), float(n["lon"])

    segment_lens: List[float] = []
    for i in range(len(path) - 1):
        a, b = path[i], path[i + 1]
        na, nb = nodos.get(a), nodos.get(b)
        if not na or not nb:
            segment_lens.append(0.0)
        else:
            segment_lens.append(
                haversine_km(float(na["lat"]), float(na["lon"]), float(nb["lat"]), float(nb["lon"]))
            )
    total = sum(segment_lens)
    if total <= 1e-9:
        n = nodos[path[0]]
        return float(n["lat"]), float(n["lon"])

    target_km = t * total
    cum = 0.0
    for i in range(len(path) - 1):
        ln = segment_lens[i]
        if cum + ln >= target_km - 1e-12 or i == len(path) - 2:
            frac = 0.0 if ln <= 1e-12 else (target_km - cum) / ln
            frac = max(0.0, min(1.0, frac))
            na, nb = nodos[path[i]], nodos[path[i + 1]]
            lat = float(na["lat"]) + frac * (float(nb["lat"]) - float(na["lat"]))
            lon = float(na["lon"]) + frac * (float(nb["lon"]) - float(na["lon"]))
            return lat, lon
        cum += ln
    n = nodos[path[-1]]
    return float(n["lat"]), float(n["lon"])


def ruta_bfs_minima(origen: str, destino: str) -> Optional[List[str]]:
    adj = construir_adyacencia()
    return bfs_ruta(adj, origen, destino, set(), set())


def escribir_tracking_camion(
    id_camion: str,
    lat: float,
    lon: float,
    ruta_origen: str,
    ruta_destino: str,
    ruta_sugerida: List[str],
    estado_ruta: str,
    motivo_retraso: Optional[str],
) -> Tuple[bool, str]:
    try:
        from cassandra.cluster import Cluster

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        ts = datetime.now(timezone.utc)
        session.execute(
            """
            INSERT INTO tracking_camiones (
                id_camion, lat, lon, ruta_origen, ruta_destino, ruta_sugerida,
                estado_ruta, motivo_retraso, ultima_posicion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                id_camion,
                float(lat),
                float(lon),
                ruta_origen,
                ruta_destino,
                ruta_sugerida,
                estado_ruta,
                motivo_retraso,
                ts,
            ),
        )
        cluster.shutdown()
        return True, ""
    except Exception as e:
        return False, str(e)


def iniciar_estado_simulacion_para_asignaciones(
    asignaciones: List[Dict[str, Any]],
    duracion_viaje_sec: float,
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    """
    Construye el dict por id de camión y escribe posición inicial en Cassandra.
    Devuelve (estado_sim, advertencias).
    """
    nodos = get_nodos()
    out: Dict[str, Dict[str, Any]] = {}
    warns: List[str] = []
    inicio = datetime.now(timezone.utc)

    for a in asignaciones:
        cid = str(a.get("camion") or "").strip()
        o = str(a.get("origen") or "").strip()
        d = str(a.get("destino") or "").strip()
        id_ruta = str(a.get("id_ruta") or "").strip()
        if not cid or not o or not d:
            continue
        path = ruta_bfs_minima(o, d)
        if not path:
            warns.append(f"{cid}: no hay ruta BFS entre {o} y {d}.")
            continue
        la, lo = interpolar_posicion_sobre_ruta(path, nodos, 0.0)
        ok, err = escribir_tracking_camion(
            cid,
            la,
            lo,
            o,
            d,
            path,
            "En ruta",
            None,
        )
        if not ok:
            warns.append(f"{cid}: {err}")
            continue
        out[cid] = {
            "inicio_utc": inicio,
            "duracion_sec": max(5.0, float(duracion_viaje_sec)),
            "path": path,
            "origen": o,
            "destino": d,
            "id_ruta": id_ruta,
            "notificado_fin": False,
        }
    return out, warns


def sincronizar_posiciones_desde_reloj(
    sim_por_camion: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Actualiza posiciones en Cassandra según tiempo transcurrido desde `inicio_utc`.
    Devuelve (estado_sim actualizado, eventos de finalización para UI/correo).
    """
    nodos = get_nodos()
    now = datetime.now(timezone.utc)
    completions: List[Dict[str, Any]] = []

    for cid, st in list(sim_por_camion.items()):
        if st.get("notificado_fin"):
            continue
        path = st.get("path") or []
        dur = float(st.get("duracion_sec") or 120.0)
        inicio: datetime = st["inicio_utc"]
        if inicio.tzinfo is None:
            inicio = inicio.replace(tzinfo=timezone.utc)
        elapsed = (now - inicio).total_seconds()
        t = min(1.0, max(0.0, elapsed / dur))

        la, lo = interpolar_posicion_sobre_ruta(path, nodos, t)
        o = str(st.get("origen") or "")
        d = str(st.get("destino") or "")
        id_ruta = str(st.get("id_ruta") or "")
        row_now = _fila_tracking_actual(cid)

        if t < 1.0:
            mot = row_now.get("motivo_retraso") if row_now else None
            escribir_tracking_camion(
                cid,
                la,
                lo,
                o,
                d,
                path,
                "En ruta",
                mot,
            )
        else:
            # Llegada: posición en destino
            if path:
                nd = nodos.get(path[-1])
                if nd:
                    la, lo = float(nd["lat"]), float(nd["lon"])
            escribir_tracking_camion(
                cid,
                la,
                lo,
                o,
                d,
                path,
                "Finalizada",
                row_now.get("motivo_retraso") if row_now else None,
            )
            llegada = datetime.now(timezone.utc)
            completions.append(
                {
                    "id_camion": cid,
                    "id_ruta": id_ruta,
                    "origen": o,
                    "destino": d,
                    "hora_salida": inicio,
                    "hora_llegada": llegada,
                    "motivo_retraso": (row_now or {}).get("motivo_retraso"),
                    "estado_antes": (row_now or {}).get("estado_ruta"),
                }
            )
            st["notificado_fin"] = True

    return sim_por_camion, completions


def _fila_tracking_actual(id_camion: str) -> Dict[str, Any]:
    from servicios.estado_y_datos import cargar_tracking_cassandra

    for row in cargar_tracking_cassandra():
        if str(row.get("id_camion")) == str(id_camion):
            return row
    return {}


def texto_alerta_ruta_finalizada(ev: Dict[str, Any]) -> str:
    sal = ev.get("hora_salida")
    ll = ev.get("hora_llegada")
    def _fmt(x: Any) -> str:
        if isinstance(x, datetime):
            return x.strftime("%Y-%m-%d %H:%M:%S UTC")
        return str(x or "—")

    inc = []
    if ev.get("motivo_retraso"):
        inc.append(f"Motivo / retraso: {ev['motivo_retraso']}")
    if ev.get("estado_antes") and str(ev["estado_antes"]).lower() not in ("en ruta", "finalizada"):
        inc.append(f"Estado previo: {ev['estado_antes']}")
    inc_txt = "\n".join(f"  - {x}" for x in inc) if inc else "  (Sin incidencias registradas en tracking.)"

    return (
        f"Ruta finalizada — SIMLOG\n\n"
        f"Camión: {ev.get('id_camion')}\n"
        f"Id. ruta: {ev.get('id_ruta') or '—'}\n"
        f"Trayecto: {ev.get('origen')} → {ev.get('destino')}\n"
        f"Hora de salida: {_fmt(sal)}\n"
        f"Hora de llegada: {_fmt(ll)}\n\n"
        f"Incidencias:\n{inc_txt}\n"
    )


def texto_toast_ruta_finalizada(ev: Dict[str, Any]) -> str:
    sal = ev.get("hora_salida")
    ll = ev.get("hora_llegada")
    def _fmt(x: Any) -> str:
        if isinstance(x, datetime):
            return x.strftime("%H:%M:%S")
        return str(x or "—")
    extra = ""
    if ev.get("motivo_retraso"):
        extra = f" · Incidencia: {ev['motivo_retraso'][:80]}"
    return (
        f"Ruta finalizada: {ev.get('id_camion')} ({ev.get('origen')} → {ev.get('destino')}). "
        f"Salida {_fmt(sal)} → Llegada {_fmt(ll)}{extra}"
    )
