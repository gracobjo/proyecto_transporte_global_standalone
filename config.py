"""
SIMLOG España — configuración central
Paths de JARs y parámetros configurables
"""
import os
from pathlib import Path


def _cargar_env_local() -> None:
    """
    Carga variables desde `.env` en la raíz del proyecto si existen.
    No sobreescribe variables ya definidas en el entorno.
    """
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        val = v.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


_cargar_env_local()

BASE_PATH = os.path.expanduser("~/proyecto_transporte_global")


def _load_local_env() -> None:
    """Carga un `.env` local de forma simple, sin dependencias externas."""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                # Respeta variables ya definidas en el entorno.
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        # Si falla la lectura, la app sigue usando variables del entorno del sistema.
        pass


_load_local_env()

# --- Marca del proyecto (UI, documentación, NiFi, tags) ---
PROJECT_DISPLAY_NAME = os.environ.get("PROJECT_DISPLAY_NAME", "SIMLOG España")
PROJECT_SLUG = "simlog_es"
PROJECT_TAGLINE = "Monitorización y simulación logística en red (España)"
PROJECT_DESCRIPTION = (
    "Sistema Integrado de Monitorización y Simulación Logística: ingesta, minería de "
    "grafos, persistencia y visualización en el ciclo KDD."
)
NIFI_PROCESS_GROUP_NAME = "PG_SIMLOG_KDD"

# JARs - Configurables según instalación
JAR_GRAPHFRAMES = os.environ.get(
    "JAR_GRAPHFRAMES",
    os.path.join(BASE_PATH, "herramientas/graphframes-0.8.3-spark3.5-s_2.12.jar")
)
JAR_CASSANDRA = os.environ.get(
    "JAR_CASSANDRA",
    "/home/hadoop/.ivy2/cache/com.datastax.spark/spark-cassandra-connector_2.12/jars/spark-cassandra-connector_2.12-3.5.0.jar"
)
JAR_KAFKA = os.environ.get(
    "JAR_KAFKA",
    os.path.join(BASE_PATH, "spark-sql-kafka-0-10_2.12-3.5.1.jar.1")
)

# Fallback: si herramienta tiene extensión .1, usar la del proyecto
if not os.path.exists(JAR_GRAPHFRAMES):
    alt = os.path.join(BASE_PATH, "graphframes-0.8.3-spark3.5-s_2.12.jar.1")
    if os.path.exists(alt):
        JAR_GRAPHFRAMES = alt

# API Weather
# Soporta alias usados en NiFi/documentación y en la app principal.
API_WEATHER_KEY = (
    os.environ.get("API_WEATHER_KEY")
    or os.environ.get("OWM_API_KEY")
    or os.environ.get("OPENWEATHER_API_KEY")
    or ""
)
API_WEATHER_BASE = "https://api.openweathermap.org/data/2.5/weather"

# Kafka - Dos temas según PDF: Datos Crudos y Datos Filtrados
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = "transporte_raw"           # Datos crudos (auditoría)
TOPIC_FILTERED = "transporte_filtered" # Datos filtrados/validados para procesamiento
TOPIC_DGT_RAW = os.environ.get("SIMLOG_TOPIC_DGT_RAW", "transporte_dgt_raw")
TOPIC_TRANSPORTE = TOPIC_FILTERED      # Compatibilidad: consumidores usan filtrado

# DATEX2 / DGT
DGT_DATEX2_URL = os.environ.get(
    "SIMLOG_DGT_DATEX2_URL",
    "https://nap.dgt.es/datex2/v3/dgt/SituationPublication/datex2_v36.xml",
)
DGT_XML_CACHE_PATH = os.environ.get(
    "SIMLOG_DGT_XML_CACHE_PATH",
    os.path.join(BASE_PATH, "reports", "dgt_cache", "datex2_latest.xml"),
)
DGT_XML_META_PATH = os.environ.get(
    "SIMLOG_DGT_XML_META_PATH",
    os.path.join(BASE_PATH, "reports", "dgt_cache", "datex2_latest_meta.json"),
)
DGT_MAX_NODE_DISTANCE_KM = float(os.environ.get("SIMLOG_DGT_MAX_NODE_DISTANCE_KM", "100"))
TOPIC_DGT_RAW = os.environ.get("SIMLOG_TOPIC_DGT_RAW", "transporte_dgt_raw")

# Cassandra
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = "logistica_espana"

# HDFS (en Docker usar HDFS_NAMENODE=namenode:9000 para conectar al servicio)
HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "127.0.0.1:9000")
HDFS_BACKUP_PATH = os.environ.get("HDFS_BACKUP_PATH", "/user/hadoop/transporte_backup")

# DATEX2 / DGT
DATEX2_DGT_URL = os.environ.get(
    "DATEX2_DGT_URL",
    "https://nap.dgt.es/datex2/v3/dgt/SituationPublication/datex2_v36.xml",
)
DGT_CACHE_DIR = os.environ.get(
    "SIMLOG_DGT_CACHE_DIR",
    os.path.join(BASE_PATH, "reports", "dgt_cache"),
)
DGT_CACHE_FILE = os.environ.get(
    "SIMLOG_DGT_CACHE_FILE",
    os.path.join(DGT_CACHE_DIR, "datex2_latest.xml"),
)
DGT_CACHE_MAX_AGE_MINUTES = int(os.environ.get("SIMLOG_DGT_CACHE_MAX_AGE_MINUTES", "180"))
DGT_MAX_NODE_DISTANCE_KM = float(os.environ.get("SIMLOG_DGT_MAX_NODE_DISTANCE_KM", "100"))

# Hive (para histórico; en Docker usar HIVE_METASTORE_URIS si aplica)
# Debe coincidir con `procesamiento_grafos.py` (histórico Hive en logistica_espana).
HIVE_DB = os.environ.get("HIVE_DB", "logistica_espana")
HIVE_CONF_DIR = os.environ.get(
    "SIMLOG_HIVE_CONF_DIR",
    os.environ.get("HIVE_CONF_DIR", os.path.join(BASE_PATH, "hive", "conf")),
)
HIVE_METASTORE_PORT = int(os.environ.get("SIMLOG_PORT_HIVE_METASTORE", "9083"))
# Tabla Hive de histórico de ingesta/rutas (DDL propio; por defecto nombre pedido en integraciones gestor)
HIVE_TABLE_TRANSPORTE_HIST = os.environ.get(
    "SIMLOG_HIVE_TABLA_TRANSPORTE", "transporte_ingesta_completa"
)
# Histórico de nodos y maestro (Spark escribe sin sufijo; si tu clúster usa otro nombre, ajústalo aquí).
HIVE_TABLE_HISTORICO_NODOS = os.environ.get(
    "SIMLOG_HIVE_TABLE_HISTORICO_NODOS", "historico_nodos"
)
HIVE_TABLE_NODOS_MAESTRO = os.environ.get(
    "SIMLOG_HIVE_TABLE_NODOS_MAESTRO", "nodos_maestro"
)
HIVE_METASTORE_URIS = os.environ.get(
    "HIVE_METASTORE_URIS",
    f"thrift://127.0.0.1:{HIVE_METASTORE_PORT}",
)  # ej: thrift://hive-metastore:9083
HIVE_SERVER = os.environ.get("HIVE_SERVER", "127.0.0.1:10000")   # HiveServer2 para JDBC/beeline
# JDBC para beeline / clientes (cuadro de mando, integraciones)
HIVE_JDBC_URL = os.environ.get("HIVE_JDBC_URL", "jdbc:hive2://localhost:10000")
# Usuario Hive (beeline -n). Sin esto, HiveServer2 trata la sesión como «anonymous» y puede fallar con
# «User: hadoop is not allowed to impersonate anonymous» si no hay proxy user en core-site.xml.
_SIMLOG_HIVE_USER = os.environ.get("SIMLOG_HIVE_BEELINE_USER", "").strip()
HIVE_BEELINE_USER = (
    _SIMLOG_HIVE_USER
    if _SIMLOG_HIVE_USER
    else (os.environ.get("HADOOP_USER_NAME") or os.environ.get("USER") or "hadoop")
)

# NiFi (opcional; URL de la UI para documentación o integración)
# Importante: usar localhost por SNI/TLS del certificado por defecto de NiFi.
NIFI_URL = os.environ.get("NIFI_URL", "https://localhost:8443/nifi")

# Coste orientativo de retrasos (€/min) — rutas híbridas / dashboard
COSTE_EURO_MINUTO_RETASO = float(os.environ.get("SIMLOG_COSTE_RETASO_EUR_MIN", "2.5"))

# Ingesta periódica (cron / Airflow / systemd): ventana en minutos entre ejecuciones.
# 15 = producción; 2–5 para demos más rápidas. Usado con `ingesta/trigger_paso.py`.
SIMLOG_INGESTA_INTERVAL_MINUTES = int(os.environ.get("SIMLOG_INGESTA_INTERVAL_MINUTES", "15"))
