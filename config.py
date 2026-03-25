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
TOPIC_TRANSPORTE = TOPIC_FILTERED      # Compatibilidad: consumidores usan filtrado

# Cassandra
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = "logistica_espana"

# HDFS (en Docker usar HDFS_NAMENODE=namenode:9000 para conectar al servicio)
HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "127.0.0.1:9000")
HDFS_BACKUP_PATH = os.environ.get("HDFS_BACKUP_PATH", "/user/hadoop/transporte_backup")

# Hive (para histórico; en Docker usar HIVE_METASTORE_URIS si aplica)
HIVE_DB = os.environ.get("HIVE_DB", "logistica_db")
HIVE_METASTORE_URIS = os.environ.get("HIVE_METASTORE_URIS", "")  # ej: thrift://hive-metastore:9083
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
