#!/usr/bin/env bash
# Arranque rapido para demo en GitHub Codespaces:
# 1) crea/activa venv, 2) instala dependencias, 3) prepara .env,
# 4) arranca servicios Docker opcionales (cassandra/kafka por defecto),
# 5) genera snapshot de ingesta (opcional), 6) lanza Streamlit.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

log() {
  echo "[demo-codespaces] $*"
}

warn() {
  echo "[demo-codespaces][WARN] $*" >&2
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

pick_python() {
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
    return
  fi
  if command -v python >/dev/null 2>&1; then
    echo "python"
    return
  fi
  warn "No se encontro python ni python3 en PATH."
  exit 1
}

PYTHON_BIN="$(pick_python)"
VENV_DIR="${SIMLOG_VENV_DIR:-.venv}"
PORT="${PORT:-8501}"
DEMO_DOCKER="${SIMLOG_DEMO_DOCKER:-1}"
DEMO_DOCKER_SERVICES="${SIMLOG_DEMO_DOCKER_SERVICES:-cassandra kafka}"
DEMO_INIT_CASSANDRA="${SIMLOG_DEMO_INIT_CASSANDRA:-1}"
DEMO_INGESTA_EN_CONTENEDOR="${SIMLOG_DEMO_INGESTA_EN_CONTENEDOR:-1}"

if [[ ! -d "$VENV_DIR" ]]; then
  log "Creando entorno virtual en $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

log "Instalando dependencias de requirements.txt"
python -m pip install --upgrade pip >/dev/null
pip install -r requirements.txt

if [[ ! -f ".env" ]]; then
  if [[ -f ".env.example" ]]; then
    cp ".env.example" ".env"
    log "Creado .env desde .env.example"
  else
    warn "No existe .env.example; crea .env manualmente si necesitas API keys."
  fi
fi

# Defaults de demo seguros para Codespaces (si no vienen ya definidos).
export SIMLOG_ENABLE_HIVE="${SIMLOG_ENABLE_HIVE:-0}"
export SIMLOG_SPARK_TIMEOUT_SEC="${SIMLOG_SPARK_TIMEOUT_SEC:-2400}"
export SIMLOG_INGESTA_TIMEOUT_SEC="${SIMLOG_INGESTA_TIMEOUT_SEC:-600}"

if [[ "$DEMO_DOCKER" == "1" ]]; then
  if have_cmd docker; then
    log "Arrancando servicios Docker de demo: ${DEMO_DOCKER_SERVICES}"
    if ! docker compose up -d ${DEMO_DOCKER_SERVICES}; then
      warn "No se pudieron arrancar los servicios Docker (${DEMO_DOCKER_SERVICES}). Continuo en modo solo-UI."
    else
      # En Codespaces, el proceso Python corre en el host del workspace (no dentro de la red Docker).
      # Por eso, los hostnames de contenedores (p. ej. kafka/namenode) no siempre resuelven.
      # Ajustamos defaults para que ingesta/verificación usen puertos publicados en localhost.
      if [[ "$DEMO_DOCKER_SERVICES" == *"kafka"* ]]; then
        export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
      fi
      if [[ "$DEMO_DOCKER_SERVICES" == *"namenode"* ]]; then
        export HDFS_NAMENODE="${HDFS_NAMENODE:-localhost:9000}"
        # Forzamos WebHDFS al NameNode expuesto (evita redirects a datanode no resoluble desde el host).
        export SIMLOG_WEBHDFS_URL="${SIMLOG_WEBHDFS_URL:-http://localhost:9870}"
      fi
      if [[ "$DEMO_INIT_CASSANDRA" == "1" ]] && [[ "$DEMO_DOCKER_SERVICES" == *"cassandra"* ]]; then
        if [[ -f "cassandra/esquema_logistica.cql" ]]; then
          log "Inicializando esquema Cassandra (idempotente si ya existe)."
          if ! docker cp "cassandra/esquema_logistica.cql" cassandra:/tmp/esquema_logistica.cql >/dev/null 2>&1; then
            warn "No se pudo copiar esquema CQL al contenedor Cassandra."
          elif ! docker exec cassandra cqlsh -f /tmp/esquema_logistica.cql >/dev/null 2>&1; then
            warn "No se pudo aplicar el esquema en Cassandra (puede requerir mas tiempo de arranque)."
          fi
        else
          warn "No existe cassandra/esquema_logistica.cql; se omite inicializacion de tablas."
        fi
      fi
    fi
  else
    warn "Docker no esta disponible; continuo en modo solo-UI."
  fi
else
  log "SIMLOG_DEMO_DOCKER=0 -> se omite arranque de servicios Docker."
fi

if [[ "${SIMLOG_DEMO_SKIP_INGESTA:-0}" != "1" ]]; then
  log "Generando snapshot de ingesta (python -m ingesta.ingesta_kdd)"
  if [[ "$DEMO_DOCKER" == "1" ]] && have_cmd docker && [[ "$DEMO_INGESTA_EN_CONTENEDOR" == "1" ]]; then
    # Importante en Codespaces: Kafka se anuncia como `kafka:9092` (advertised listeners) y WebHDFS redirige a `datanode:*`.
    # Desde el host del workspace, esos hostnames no resuelven → timeouts. Ejecutar la ingesta dentro del compose lo evita.
    if docker compose config --services 2>/dev/null | grep -qx "app"; then
      log "Ingesta dentro de Docker (servicio app) para resolver kafka/namenode/datanode."
      if ! docker compose run --rm --no-deps app python -m ingesta.ingesta_kdd; then
        warn "La ingesta (en contenedor) fallo. Continuo con Streamlit para demo de UI."
      fi
    else
      warn "No existe el servicio 'app' en docker compose; ejecuto ingesta en el host (puede fallar en Codespaces)."
      if ! python -m ingesta.ingesta_kdd; then
        warn "La ingesta fallo. Continuo con Streamlit para demo de UI."
      fi
    fi
  else
    if ! python -m ingesta.ingesta_kdd; then
      warn "La ingesta fallo. Continuo con Streamlit para demo de UI."
    fi
  fi
else
  log "SIMLOG_DEMO_SKIP_INGESTA=1 -> se omite la ingesta inicial."
fi

log "Arrancando Streamlit en 0.0.0.0:${PORT}"
log "En Codespaces, abre el puerto ${PORT} cuando aparezca."
exec streamlit run app_visualizacion.py --server.address 0.0.0.0 --server.port "$PORT"
