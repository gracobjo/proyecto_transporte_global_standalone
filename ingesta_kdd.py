"""
Sistema de Gemelo Digital Logístico - Fase I: Ingesta y Simulación (KDD)
Ruta base: ~/proyecto_transporte_global/
"""

import json
import math
import random
import time
import requests
from datetime import datetime
from typing import Dict, List, Tuple, Optional

from config_nodos import RED, HUBS, get_nodos, get_aristas
from config import HDFS_BACKUP_PATH as HDFS_BACKUP_PATH_CONFIG

API_KEY = "735df735c4780d78a550d6bb6b52dfd7"
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# Dos temas según PDF: raw (crudos) y filtered (filtrados)
KAFKA_TOPIC_RAW = "transporte_raw"
KAFKA_TOPIC_FILTERED = "transporte_filtered"
KAFKA_TOPIC = KAFKA_TOPIC_FILTERED  # Por defecto publicar en filtrado
# Escribir en HDFS_BACKUP_PATH para que Spark (procesamiento_grafos) pueda leer los JSON
HDFS_PATH = HDFS_BACKUP_PATH_CONFIG


def obtener_clima_hub(hub_name: str, lat: float, lon: float) -> Dict:
    """Consulta el clima actual de un hub via OpenWeatherMap API."""
    try:
        params = {
            "lat": lat,
            "lon": lon,
            "appid": API_KEY,
            "units": "metric"
        }
        resp = requests.get(WEATHER_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        return {
            "ciudad": hub_name,
            "temperatura": data["main"]["temp"],
            "humedad": data["main"]["humidity"],
            "descripcion": data["weather"][0]["description"],
            "visibilidad": data.get("visibility", 10000),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error consultando clima para {hub_name}: {e}")
        return {
            "ciudad": hub_name,
            "temperatura": None,
            "humedad": None,
            "descripcion": "error",
            "visibilidad": None,
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }


def obtener_clima_todos_hubs() -> List[Dict]:
    """Obtiene el clima de los 5 hubs principales."""
    climas = []
    for hub_name, coords in HUBS.items():
        clima = obtener_clima_hub(hub_name, coords["lat"], coords["lon"])
        climas.append(clima)
        time.sleep(0.2)
    return climas


def calcular_distancia_haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula distancia entre dos puntos usando fórmula de Haversine."""
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def simular_incidentes(nodos: Dict, aristas: List[Tuple]) -> Tuple[Dict, Dict]:
    """
    Genera estados aleatorios para nodos y aristas.
    Estados: OK (verde), Congestionado (amarillo), Bloqueado (rojo)
    """
    estados_nodos = {}
    estados_aristas = {}
    
    MOTIVOS = {
        "ok": ["Tráfico fluido", "Condiciones óptimas"],
        "congestionado": ["Niebla", "Tráfico denso", "Lluvia", "Obras"],
        "bloqueado": ["Incendio", "Nieve", "Avalancha", "Corte de carretera"]
    }
    
    for nombre in nodos:
        rand = random.random()
        if rand < 0.7:
            estado = "ok"
        elif rand < 0.9:
            estado = "congestionado"
        else:
            estado = "bloqueado"
        
        estados_nodos[nombre] = {
            "estado": estado,
            "motivo": random.choice(MOTIVOS[estado])
        }
    
    for src, dst, dist in aristas:
        edge_id = f"{src}|{dst}"
        rand = random.random()
        if rand < 0.7:
            estado = "ok"
        elif rand < 0.9:
            estado = "congestionado"
        else:
            estado = "bloqueado"
        
        estados_aristas[edge_id] = {
            "estado": estado,
            "distancia_km": dist,
            "motivo": random.choice(MOTIVOS[estado])
        }
    
    return estados_nodos, estados_aristas


def interpolar_posicion(
    lat_inicio: float, lon_inicio: float,
    lat_fin: float, lon_fin: float,
    paso: int, total_pasos: int
) -> Tuple[float, float]:
    """
    Interpola posición GPS en un punto específico de la ruta.
    paso: número de paso actual (0-indexed)
    total_pasos: número total de pasos (intervalos de 15 min)
    """
    if total_pasos == 0:
        return lat_inicio, lon_inicio
    
    lat = lat_inicio + (lat_fin - lat_inicio) * paso / total_pasos
    lon = lon_inicio + (lon_fin - lon_inicio) * paso / total_pasos
    return round(lat, 6), round(lon, 6)


def simular_camiones(
    nodos: Dict,
    aristas: List[Tuple],
    num_camiones: int = 5,
    pasos_por_ciclo: int = 4
) -> List[Dict]:
    """
    Simula 5 camiones en ruta con posiciones GPS interpoladas cada 15 min.
    Cada ciclo tiene 4 pasos (1 hora total = 4 * 15 min)
    """
    random.seed(datetime.now().hour * 60 + datetime.now().minute)
    
    all_nodos = list(nodos.keys())
    hubs = list(HUBS.keys())
    
    camiones = []
    arista_dict = {(src, dst): dist for src, dst, dist in aristas}
    
    for i in range(num_camiones):
        origen = random.choice(all_nodos)
        destino = random.choice([n for n in all_nodos if n != origen])
        
        ruta = [origen]
        visited = {origen}
        current = origen
        
        for _ in range(3):
            opciones = []
            for src, dst, dist in aristas:
                if src == current and dst not in visited:
                    opciones.append((dst, dist))
                elif dst == current and src not in visited:
                    opciones.append((src, dist))
            
            if not opciones:
                break
            
            next_node = min(opciones, key=lambda x: x[1])[0]
            ruta.append(next_node)
            visited.add(next_node)
            current = next_node
        
        ruta.append(destino)
        
        distancia_total = sum(
            arista_dict.get((ruta[j], ruta[j+1]), 
                          arista_dict.get((ruta[j+1], ruta[j]), 0))
            for j in range(len(ruta)-1)
        )
        
        total_pasos = pasos_por_ciclo * 4
        
        pos_actual = random.randint(0, total_pasos - 1)
        lat_actual, lon_actual = interpolar_posicion(
            nodos[ruta[0]]["lat"], nodos[ruta[0]]["lon"],
            nodos[ruta[-1]]["lat"], nodos[ruta[-1]]["lon"],
            pos_actual, total_pasos
        )
        
        progress = (pos_actual / total_pasos) * 100
        
        nodo_actual_idx = min(int(progress / (100 / (len(ruta)-1))), len(ruta)-2)
        nodo_actual = ruta[nodo_actual_idx]
        
        camiones.append({
            "id": f"CAM-{i+1:03d}",
            "ruta": ruta,
            "distancia_total_km": round(distancia_total, 2),
            "posicion_actual": {"lat": lat_actual, "lon": lon_actual},
            "nodo_actual": nodo_actual,
            "progreso_pct": round(progress, 1),
            "timestamp": datetime.now().isoformat()
        })
    
    return camiones


def crear_json_enriquecido(
    climas: List[Dict],
    estados_nodos: Dict,
    estados_aristas: Dict,
    camiones: List[Dict]
) -> Dict:
    """Crea el JSON enriquecido con todos los datos (raw)."""
    return {
        "timestamp": datetime.now().isoformat(),
        "clima": climas,
        "estados_nodos": estados_nodos,
        "estados_aristas": estados_aristas,
        "camiones": camiones,
        "intervalo_minutos": 15
    }


def filtrar_payload_para_kafka(raw_data: Dict) -> Dict:
    """
    Filtra el payload crudo para el tema 'Datos Filtrados':
    - Excluye nodos/aristas sin estado válido.
    - Excluye camiones con lat/lon nulos o fuera de España (lat 35-44, lon -10-5).
    - Excluye entradas de clima con error (temperatura None).
    """
    ts = raw_data.get("timestamp", datetime.now().isoformat())
    climas = raw_data.get("clima", [])
    estados_nodos = raw_data.get("estados_nodos", {})
    estados_aristas = raw_data.get("estados_aristas", {})
    camiones = raw_data.get("camiones", [])

    climas_ok = [c for c in climas if c.get("temperatura") is not None and c.get("ciudad")]
    nodos_ok = {k: v for k, v in estados_nodos.items() if k and v.get("estado")}
    aristas_ok = {k: v for k, v in estados_aristas.items() if k and "|" in k and v.get("estado")}

    def lat_lon_validos(c):
        lat = c.get("posicion_actual", {}).get("lat") if isinstance(c.get("posicion_actual"), dict) else c.get("lat")
        lon = c.get("posicion_actual", {}).get("lon") if isinstance(c.get("posicion_actual"), dict) else c.get("lon")
        if lat is None or lon is None:
            return False
        try:
            lat, lon = float(lat), float(lon)
            return 35 <= lat <= 44 and -10 <= lon <= 5
        except (TypeError, ValueError):
            return False

    camiones_ok = [c for c in camiones if (c.get("id") or c.get("id_camion")) and lat_lon_validos(c)]

    return {
        "timestamp": ts,
        "clima": climas_ok,
        "estados_nodos": nodos_ok,
        "estados_aristas": aristas_ok,
        "camiones": camiones_ok,
        "intervalo_minutos": 15
    }


def _ensure_hdfs_path():
    """Crea el directorio HDFS_BACKUP_PATH si no existe (para que Spark pueda leer después)."""
    try:
        import subprocess
        subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", HDFS_PATH],
            capture_output=True,
            timeout=15,
        )
    except Exception:
        pass


def guardar_en_hdfs(json_data: Dict, hdfs_path: str = HDFS_PATH) -> bool:
    """Guarda el JSON en HDFS como backup (ruta = HDFS_BACKUP_PATH para que Spark lo lea)."""
    try:
        import subprocess
        from datetime import datetime
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{hdfs_path}/transporte_{timestamp}.json"
        
        json_str = json.dumps(json_data, ensure_ascii=False, indent=2)
        
        cmd = f"echo '{json_str}' | hdfs dfs -put - {filename}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"Guardado en HDFS: {filename}")
            return True
        else:
            print(f"Error HDFS: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error guardando en HDFS: {e}")
        return False


def publicar_en_kafka(json_data: Dict, topic: str = KAFKA_TOPIC) -> bool:
    """Publica el JSON enriquecido en Kafka."""
    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
        
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        
        future = producer.send(topic, value=json_data)
        record_metadata = future.result(timeout=10)
        
        producer.flush()
        producer.close()
        
        print(f"Publicado en Kafka: {topic} [{record_metadata.partition}]")
        return True
        
    except Exception as e:
        print(f"Error conectando a Kafka: {e}")
        return False


def ejecutar_ingesta() -> Dict:
    """
    Ejecuta el pipeline completo de ingesta.
    Retorna el JSON enriquecido para procesamiento posterior.
    """
    print("=" * 60)
    print("FASE I: INGESTA Y SIMULACIÓN (KDD)")
    print("=" * 60)
    
    nodos = get_nodos()
    aristas = get_aristas()
    
    print("\n[1/4] Consultando clima de hubs...")
    climas = obtener_clima_todos_hubs()
    for c in climas:
        print(f"  - {c['ciudad']}: {c['temperatura']}°C, {c['descripcion']}")
    
    print("\n[2/4] Simulando incidentes...")
    estados_nodos, estados_aristas = simular_incidentes(nodos, aristas)
    
    print("\n[3/4] Simulando camiones con GPS...")
    camiones = simular_camiones(nodos, aristas, num_camiones=5, pasos_por_ciclo=4)
    for cam in camiones:
        print(f"  - {cam['id']}: {cam['nodo_actual']} ({cam['progreso_pct']}%)")
    
    print("\n[4/4] Creando JSON enriquecido...")
    json_enriquecido = crear_json_enriquecido(
        climas, estados_nodos, estados_aristas, camiones
    )
    
    print("\n[5/5] Persistiendo datos (raw + filtrado)...")
    # HDFS: copia raw en HDFS_BACKUP_PATH para auditoría y para que Spark lea desde ahí
    _ensure_hdfs_path()
    guardar_en_hdfs(json_enriquecido)
    publicar_en_kafka(json_enriquecido, topic=KAFKA_TOPIC_RAW)
    json_filtrado = filtrar_payload_para_kafka(json_enriquecido)
    publicar_en_kafka(json_filtrado, topic=KAFKA_TOPIC_FILTERED)
    
    print("\n" + "=" * 60)
    print("INGESTA COMPLETADA")
    print("=" * 60)
    
    return json_enriquecido


if __name__ == "__main__":
    resultado = ejecutar_ingesta()
    print("\nJSON generado (resumen):")
    print(f"  - Timestamp: {resultado['timestamp']}")
    print(f"  - Camiones: {len(resultado['camiones'])}")
    print(f"  - Nodos: {len(resultado['estados_nodos'])}")
    print(f"  - Aristas: {len(resultado['estados_aristas'])}")
