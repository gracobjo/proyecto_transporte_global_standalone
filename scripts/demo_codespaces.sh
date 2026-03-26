#!/usr/bin/env bash
# Arranque rapido para demo en GitHub Codespaces:
# 1) crea/activa venv, 2) instala dependencias, 3) prepara .env,
# 4) genera un snapshot de ingesta (opcional), 5) lanza Streamlit.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

log() {
  echo "[demo-codespaces] $*"
}

warn() {
  echo "[demo-codespaces][WARN] $*" >&2
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

if [[ "${SIMLOG_DEMO_SKIP_INGESTA:-0}" != "1" ]]; then
  log "Generando snapshot de ingesta (python -m ingesta.ingesta_kdd)"
  if ! python -m ingesta.ingesta_kdd; then
    warn "La ingesta fallo. Continuo con Streamlit para demo de UI."
  fi
else
  log "SIMLOG_DEMO_SKIP_INGESTA=1 -> se omite la ingesta inicial."
fi

log "Arrancando Streamlit en 0.0.0.0:${PORT}"
log "En Codespaces, abre el puerto ${PORT} cuando aparezca."
exec streamlit run app_visualizacion.py --server.address 0.0.0.0 --server.port "$PORT"
