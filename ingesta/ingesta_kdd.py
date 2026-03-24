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
    TOPIC_TRANSPORTE,
    HDFS_BACKUP_PATH,
)
from ingesta.trigger_paso import resolver_paso_ingesta, semilla_simulacion

# Estados posibles
ESTADOS = ["OK", "Congestionado", "Bloqueado"]
MOTIVOS = {
    "OK": None,
    "Congestionado": ["Niebla", "Tráfico", "Lluvia"],
    "Bloqueado": ["Incendio", "Nieve", "Accidente"],
}


def consulta_clima_hubs() -> dict:
    """Obtener clima actual de los 5 Hubs vía API OpenWeather."""
    hubs = RED["hubs"]
    nodos = get_nodos()
    clima = {}
    for hub in hubs:
        lat = nodos[hub]["lat"]
        lon = nodos[hub]["lon"]
        try:
            r = requests.get(
                API_WEATHER_BASE,
                params={
                    "lat": lat,
                    "lon": lon,
                    "appid": API_WEATHER_KEY,
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
                clima[hub] = {"descripcion": "N/A", "temp": None, "humedad": None, "viento": None}
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
        estados_nodos[nid] = {"estado": estado, "motivo": motivo}
    return estados_nodos


def simular_incidentes_aristas() -> dict:
    """Estados aleatorios para aristas."""
    aristas = get_aristas()
    estados_aristas = {}
    for src, dst, dist in aristas:
        key = f"{src}|{dst}"
        estado = random.choices(ESTADOS, weights=[0.75, 0.15, 0.1])[0]
        motivo = random.choice(MOTIVOS[estado]) if MOTIVOS[estado] else None
        estados_aristas[key] = {"estado": estado, "motivo": motivo, "distancia_km": dist}
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
            posiciones.append({
                "id_camion": f"camion_{i+1}",
                "lat": d["lat"],
                "lon": d["lon"],
                "ruta": ruta,
                "indice_tramo": 0,
                "progreso": 0.0,
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
        posiciones.append({
            "id_camion": f"camion_{i+1}",
            "lat": lat,
            "lon": lon,
            "ruta": ruta,
            "indice_tramo": tramo,
            "origen_tramo": n1,
            "destino_tramo": n2,
            "progreso": progreso_tramo,
        })
    return posiciones


def publicar_kafka(payload: dict) -> bool:
    """Enviar JSON a Kafka topic transporte_status."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        producer.send(TOPIC_TRANSPORTE, value=payload)
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


def main(paso_15min=0):
    # Misma ventana temporal → misma semilla (reproducible); ventana distinta → incidentes/rutas distintos
    random.seed(semilla_simulacion(paso_15min))

    clima = consulta_clima_hubs()
    estados_nodos = simular_incidentes_nodos()
    estados_aristas = simular_incidentes_aristas()
    rutas = generar_rutas_camiones(5)
    posiciones = interpolacion_gps_15min(rutas, paso_15min)

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "paso_15min": paso_15min,
        "clima_hubs": clima,
        "nodos_estado": {
            n: {"estado": v["estado"], "motivo": v["motivo"]}
            for n, v in estados_nodos.items()
        },
        "aristas_estado": estados_aristas,
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
            "timestamp": payload["timestamp"],
            "paso_15min": paso_15min,
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
