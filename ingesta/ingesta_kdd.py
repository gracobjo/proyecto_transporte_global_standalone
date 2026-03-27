#!/usr/bin/env python3
"""
SIMLOG España — Fase I: Ingesta KDD
- Consulta climática API OpenWeather (5 Hubs)
- Simulación de incidentes (OK/Congestionado/Bloqueado)
- Interpolación GPS cada 15 min para 5 camiones
- Publicación a Kafka (transporte_status) y backup HDFS
"""
import json
import random
import math
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Ruta base del proyecto
BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

import requests
from config_nodos import RED, get_aristas, get_nodos
from config import (
    API_WEATHER_KEY,
    API_WEATHER_BASE,
    KAFKA_BOOTSTRAP,
    TOPIC_DGT_RAW,
    TOPIC_RAW,
    TOPIC_TRANSPORTE,
    HDFS_BACKUP_PATH,
)
from ingesta.ingesta_dgt_datex2 import fusionar_estados, obtener_incidencias_dgt
from ingesta.trigger_paso import resolver_paso_ingesta, semilla_simulacion

# Estados posibles
ESTADOS = ["OK", "Congestionado", "Bloqueado"]
MOTIVOS = {
    "OK": None,
    "Congestionado": ["Niebla", "Tráfico", "Lluvia"],
    "Bloqueado": ["Incendio", "Nieve", "Accidente"],
}


def _severity_for_state(estado: str) -> str:
    estado = (estado or "OK").strip().lower()
    if estado == "bloqueado":
        return "high"
    if estado == "congestionado":
        return "medium"
    return "low"


def _env_flag(key: str, default: bool = True) -> bool:
    """
    Interpreta un booleano desde entorno.
    Valores aceptados: 1/0, true/false, yes/no, on/off (case-insensitive).
    """
    v = (os.environ.get(key, "") or "").strip().lower()
    if not v:
        return default
    if v in ("1", "true", "yes", "si", "sí", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return default


def consulta_clima_hubs(api_key: str | None = None) -> dict:
    """
    Obtener clima actual de los 5 Hubs vía API OpenWeather.
    Si `api_key` viene rellena (p. ej. desde el dashboard), se usa como `appid`;
    si no, `API_WEATHER_KEY` de config / entorno.
    """
    appid = (api_key or "").strip() or (API_WEATHER_KEY or "").strip()
    hubs = RED["hubs"]
    nodos = get_nodos()
    clima: dict = {}
    if not appid:
        for hub in hubs:
            clima[hub] = {
                "descripcion": "Sin API key: define API_WEATHER_KEY o pasa api_key a consulta_clima_hubs().",
                "temp": None,
                "humedad": None,
                "viento": None,
            }
        return clima
    for hub in hubs:
        lat = nodos[hub]["lat"]
        lon = nodos[hub]["lon"]
        try:
            r = requests.get(
                API_WEATHER_BASE,
                params={
                    "lat": lat,
                    "lon": lon,
                    "appid": appid,
                    "units": "metric",
                    "lang": "es",
                },
                timeout=10,
            )
            if r.status_code == 200:
                d = r.json()
                clima[hub] = {
                    "descripcion": d.get("weather", [{}])[0].get("description", "N/A"),
                    "temp": d.get("main", {}).get("temp"),
                    "humedad": d.get("main", {}).get("humidity"),
                    "viento": d.get("wind", {}).get("speed"),
                }
            else:
                msg = "HTTP " + str(r.status_code)
                try:
                    err_j = r.json()
                    if isinstance(err_j, dict) and err_j.get("message"):
                        msg += f": {err_j['message']}"
                except Exception:
                    pass
                clima[hub] = {"descripcion": msg, "temp": None, "humedad": None, "viento": None}
        except Exception as e:
            clima[hub] = {"descripcion": f"Error: {e}", "temp": None, "humedad": None, "viento": None}
    return clima


def simular_incidentes_nodos() -> dict:
    """Estados aleatorios para nodos: OK, Congestionado, Bloqueado."""
    nodos = get_nodos()
    estados_nodos = {}
    for nid, datos in nodos.items():
        estado = random.choices(ESTADOS, weights=[0.7, 0.2, 0.1])[0]
        motivo = random.choice(MOTIVOS[estado]) if MOTIVOS[estado] else None
        estados_nodos[nid] = {
            "estado": estado,
            "motivo": motivo,
            "source": "simulacion",
            "severity": _severity_for_state(estado),
            "peso_pagerank": 1.0,
        }
    return estados_nodos


def simular_incidentes_aristas() -> dict:
    """Estados aleatorios para aristas."""
    aristas = get_aristas()
    estados_aristas = {}
    for src, dst, dist in aristas:
        key = f"{src}|{dst}"
        estado = random.choices(ESTADOS, weights=[0.75, 0.15, 0.1])[0]
        motivo = random.choice(MOTIVOS[estado]) if MOTIVOS[estado] else None
        estados_aristas[key] = {
            "estado": estado,
            "motivo": motivo,
            "distancia_km": dist,
            "source": "simulacion",
            "severity": _severity_for_state(estado),
        }
    return estados_aristas


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def interpolar_gps(lat1, lon1, lat2, lon2, factor):
    """Interpolar posición entre dos puntos. factor in [0,1]."""
    lat = lat1 + factor * (lat2 - lat1)
    lon = lon1 + factor * (lon2 - lon1)
    return round(lat, 6), round(lon, 6)


def generar_rutas_camiones(n=5):
    """Generar 5 rutas aleatorias para camiones (secuencia de nodos)."""
    aristas = get_aristas()
    nodos = get_nodos()
    rutas = []
    # Crear grafo simplificado para encontrar rutas
    vecinos = {}
    for src, dst, _ in aristas:
        vecinos.setdefault(src, []).append(dst)
        vecinos.setdefault(dst, []).append(src)

    todos = list(nodos.keys())
    for _ in range(n):
        orig = random.choice(todos)
        dest = random.choice([x for x in todos if x != orig])
        # BFS simple para ruta (máx 6 saltos)
        from collections import deque
        seen = {orig}
        q = deque([(orig, [orig])])
        ruta = [orig]
        while q:
            u, path = q.popleft()
            if u == dest:
                ruta = path
                break
            for v in vecinos.get(u, []):
                if v not in seen and len(path) < 6:
                    seen.add(v)
                    q.append((v, path + [v]))
        rutas.append(ruta)
    return rutas


def distancia_total_ruta_km(ruta):
    """Suma la distancia total de una ruta usando las aristas conocidas."""
    if not ruta or len(ruta) < 2:
        return 0.0
    nodos = get_nodos()
    distancias = {}
    for src, dst, dist in get_aristas():
        distancias[(src, dst)] = float(dist)
        distancias[(dst, src)] = float(dist)
    total = 0.0
    for src, dst in zip(ruta, ruta[1:]):
        tramo = distancias.get((src, dst))
        if tramo is None:
            n1 = nodos.get(src, {"lat": 40.4, "lon": -3.7})
            n2 = nodos.get(dst, {"lat": 40.4, "lon": -3.7})
            tramo = haversine_km(n1["lat"], n1["lon"], n2["lat"], n2["lon"])
        total += float(tramo)
    return round(total, 2)


def clima_hubs_a_lista(clima_hubs: dict, timestamp: str) -> list[dict]:
    """Convierte el mapa por hub en una lista homogénea para persistencia Hive."""
    out = []
    for hub, datos in (clima_hubs or {}).items():
        if not isinstance(datos, dict):
            continue
        out.append(
            {
                "ciudad": hub,
                "temperatura": datos.get("temp"),
                "humedad": datos.get("humedad"),
                "descripcion": datos.get("descripcion", ""),
                "visibilidad": datos.get("visibilidad"),
                "viento": datos.get("viento"),
                "timestamp": timestamp,
            }
        )
    return out


def interpolacion_gps_15min(rutas, paso_15min=0):
    """
    Calcular posición exacta (lat, lon) para cada camión cada 15 min.
    paso_15min: 0-3 indica el cuarto de hora (0=inicio, 3=fin del tramo).
    """
    nodos = get_nodos()
    posiciones = []
    for i, ruta in enumerate(rutas):
        if len(ruta) < 2:
            nodo = ruta[0] if ruta else list(nodos.keys())[0]
            d = nodos.get(nodo, {"lat": 40.4, "lon": -3.7})
            distancia_total = distancia_total_ruta_km(ruta)
            posiciones.append({
                "id_camion": f"camion_{i+1}",
                "id": f"camion_{i+1}",
                "lat": d["lat"],
                "lon": d["lon"],
                "posicion_actual": {"lat": d["lat"], "lon": d["lon"]},
                "ruta": ruta,
                "ruta_sugerida": ruta,
                "ruta_origen": ruta[0] if ruta else nodo,
                "ruta_destino": ruta[-1] if ruta else nodo,
                "indice_tramo": 0,
                "origen_tramo": nodo,
                "destino_tramo": nodo,
                "progreso": 0.0,
                "progreso_pct": 0.0,
                "distancia_total_km": distancia_total,
                "nodo_actual": nodo,
                "estado_ruta": "Sin movimiento",
                "motivo_retraso": "",
            })
            continue
        # Asumimos 4 pasos de 15 min por tramo (1 hora por arista)
        total_tramos = len(ruta) - 1
        paso_global = paso_15min % (total_tramos * 4)
        tramo = min(paso_global // 4, total_tramos - 1)
        progreso_tramo = (paso_global % 4) / 4.0
        n1, n2 = ruta[tramo], ruta[tramo + 1]
        d1 = nodos.get(n1, {"lat": 40.4, "lon": -3.7})
        d2 = nodos.get(n2, {"lat": 41.4, "lon": 2.17})
        lat, lon = interpolar_gps(d1["lat"], d1["lon"], d2["lat"], d2["lon"], progreso_tramo)
        progreso_global = ((tramo + progreso_tramo) / max(total_tramos, 1)) * 100.0
        distancia_total = distancia_total_ruta_km(ruta)
        nodo_actual = n1 if progreso_tramo < 0.5 else n2
        posiciones.append({
            "id_camion": f"camion_{i+1}",
            "id": f"camion_{i+1}",
            "lat": lat,
            "lon": lon,
            "posicion_actual": {"lat": lat, "lon": lon},
            "ruta": ruta,
            "ruta_sugerida": ruta,
            "ruta_origen": ruta[0],
            "ruta_destino": ruta[-1],
            "indice_tramo": tramo,
            "origen_tramo": n1,
            "destino_tramo": n2,
            "progreso": progreso_tramo,
            "progreso_pct": round(progreso_global, 2),
            "distancia_total_km": distancia_total,
            "nodo_actual": nodo_actual,
            "estado_ruta": "En ruta",
            "motivo_retraso": "",
        })
    return posiciones


def publicar_kafka(payload: dict) -> bool:
    """Enviar JSON del snapshot a topics raw/filtered y, si aplica, al raw DGT."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        producer.send(TOPIC_RAW, value=payload)
        producer.send(TOPIC_TRANSPORTE, value=payload)
        if payload.get("incidencias_dgt"):
            producer.send(
                TOPIC_DGT_RAW,
                value={
                    "timestamp": payload.get("timestamp"),
                    "origen_dgt": payload.get("resumen_dgt", {}).get("source_mode"),
                    "incidencias_dgt": payload.get("incidencias_dgt", []),
                },
            )
        producer.flush()
        producer.close()
        return True
    except Exception as e:
        print(f"[KAFKA] Error: {e}")
        return False


def _hdfs_cmd():
    """Ruta al ejecutable hdfs (HADOOP_HOME/bin/hdfs o 'hdfs' del PATH)."""
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        path = os.path.join(hadoop_home, "bin", "hdfs")
        if os.path.isfile(path) or os.path.isfile(path + ".cmd"):
            return path
    return "hdfs"


def guardar_hdfs(payload: dict) -> bool:
    """Guardar backup en HDFS."""
    try:
        import subprocess
        hdfs_bin = _hdfs_cmd()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        archivo = f"/tmp/transporte_{ts}.json"
        with open(archivo, "w") as f:
            json.dump(payload, f, default=str, indent=2)
        path_hdfs = f"{HDFS_BACKUP_PATH}/transporte_{ts}.json"
        subprocess.run(
            [hdfs_bin, "dfs", "-mkdir", "-p", HDFS_BACKUP_PATH],
            capture_output=True,
        )
        subprocess.run([hdfs_bin, "dfs", "-put", "-f", archivo, path_hdfs], capture_output=True)
        os.remove(archivo)
        return True
    except Exception as e:
        print(f"[HDFS] Error: {e}")
        return False


def evaluar_alerta_bloqueos(estados_nodos: dict) -> dict:
    bloqueados = [nid for nid, info in (estados_nodos or {}).items() if (info or {}).get("estado") == "Bloqueado"]
    total = len(estados_nodos or {})
    ratio = round((len(bloqueados) / total), 3) if total else 0.0
    if ratio >= 0.20 or len(bloqueados) >= 5:
        nivel = "critica"
    elif len(bloqueados) >= 3:
        nivel = "alta"
    else:
        nivel = "normal"
    return {
        "tipo_alerta": "bloqueo_red",
        "nivel": nivel,
        "bloqueados": len(bloqueados),
        "ratio_bloqueados": ratio,
        "nodos_bloqueados": bloqueados,
    }


def main(paso_15min=0):
    # Misma ventana temporal → misma semilla (reproducible); ventana distinta → incidentes/rutas distintos
    random.seed(semilla_simulacion(paso_15min))
    canal_ingesta = os.environ.get("SIMLOG_INGESTA_CANAL", "script_python").strip() or "script_python"
    origen_ingesta = os.environ.get("SIMLOG_INGESTA_ORIGEN", "cli_script").strip() or "cli_script"
    ejecutor_ingesta = os.environ.get("SIMLOG_INGESTA_EJECUTOR", "python -m ingesta.ingesta_kdd").strip()

    clima = consulta_clima_hubs()
    sim_incid = _env_flag("SIMLOG_SIMULAR_INCIDENCIAS", True)
    use_dgt = _env_flag("SIMLOG_USE_DGT", True)
    dgt_cache_only = _env_flag("SIMLOG_DGT_ONLY_CACHE", False)
    if sim_incid:
        estados_nodos = simular_incidentes_nodos()
        estados_aristas = simular_incidentes_aristas()
    else:
        # Incidencias desactivadas: estado operativo estable (todo OK).
        nodos = get_nodos()
        estados_nodos = {nid: {"estado": "OK", "motivo": None} for nid in nodos.keys()}
        estados_aristas = {
            f"{src}|{dst}": {"estado": "OK", "motivo": None, "distancia_km": dist}
            for (src, dst, dist) in get_aristas()
        }

    info_dgt = {"source_mode": "disabled", "error": None, "incidencias": [], "mapeo_nodos": {}}
    if use_dgt:
        info_dgt = obtener_incidencias_dgt(use_cache_only=dgt_cache_only, allow_cache_fallback=True)
        if info_dgt.get("mapeo_nodos"):
            estados_nodos = fusionar_estados(estados_nodos, info_dgt["mapeo_nodos"])

    rutas = generar_rutas_camiones(5)
    posiciones = interpolacion_gps_15min(rutas, paso_15min)
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    clima_lista = clima_hubs_a_lista(clima, timestamp)
    alerta_bloqueos = evaluar_alerta_bloqueos(estados_nodos)

    payload = {
        "origen": origen_ingesta,
        "canal_ingesta": canal_ingesta,
        "ejecutor_ingesta": ejecutor_ingesta,
        "timestamp": timestamp,
        "paso_15min": paso_15min,
        "simulacion_incidencias": sim_incid,
        "dgt_habilitado": use_dgt,
        "clima_hubs": clima,
        "clima": clima_lista,
        "incidencias_dgt": info_dgt.get("incidencias", []),
        "resumen_dgt": {
            "source_mode": info_dgt.get("source_mode", "disabled"),
            "error": info_dgt.get("error"),
            "incidencias_totales": len(info_dgt.get("incidencias", [])),
            "nodos_afectados": len(info_dgt.get("mapeo_nodos", {})),
        },
        "alertas_operativas": [alerta_bloqueos],
        "nodos_estado": {
            n: {
                "estado": v["estado"],
                "motivo": v.get("motivo"),
                "source": v.get("source", "simulacion"),
                "severity": v.get("severity", _severity_for_state(v["estado"])),
                "peso_pagerank": v.get("peso_pagerank", 1.0),
                "id_incidencia": v.get("id_incidencia"),
                "carretera": v.get("carretera"),
                "municipio": v.get("municipio"),
                "provincia": v.get("provincia"),
                "descripcion": v.get("descripcion"),
            }
            for n, v in estados_nodos.items()
        },
        "estados_nodos": {
            n: {
                "estado": v["estado"],
                "motivo": v.get("motivo"),
                "source": v.get("source", "simulacion"),
                "severity": v.get("severity", _severity_for_state(v["estado"])),
                "peso_pagerank": v.get("peso_pagerank", 1.0),
                "id_incidencia": v.get("id_incidencia"),
                "carretera": v.get("carretera"),
                "municipio": v.get("municipio"),
                "provincia": v.get("provincia"),
                "descripcion": v.get("descripcion"),
            }
            for n, v in estados_nodos.items()
        },
        "aristas_estado": estados_aristas,
        "estados_aristas": estados_aristas,
        "camiones": posiciones,
    }

    ok_kafka = publicar_kafka(payload)
    ok_hdfs = guardar_hdfs(payload)

    # Copia local para el dashboard (Spark / fase_kdd_spark leen `ultimo_payload.json`)
    work = BASE / "reports" / "kdd" / "work"
    work.mkdir(parents=True, exist_ok=True)
    try:
        (work / "ultimo_payload.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        meta = {
            "origen": origen_ingesta,
            "canal_ingesta": canal_ingesta,
            "ejecutor_ingesta": ejecutor_ingesta,
            "timestamp": payload["timestamp"],
            "paso_15min": paso_15min,
            "dgt_source_mode": payload["resumen_dgt"]["source_mode"],
            "dgt_incidencias_totales": payload["resumen_dgt"]["incidencias_totales"],
            "dgt_nodos_afectados": payload["resumen_dgt"]["nodos_afectados"],
            "ok_kafka": ok_kafka,
            "ok_hdfs": ok_hdfs,
        }
        (work / "ultima_ingesta_meta.json").write_text(
            json.dumps(meta, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError as e:
        print(f"[INGESTA] Aviso: no se pudo escribir reports/kdd/work: {e}")

    print(f"[INGESTA] Paso {paso_15min} | Kafka: {ok_kafka} | HDFS: {ok_hdfs}")
    return payload


if __name__ == "__main__":
    # Sin PASO_15MIN en el entorno → índice automático según reloj e intervalo (ver trigger_paso.py)
    main(resolver_paso_ingesta())
