#!/usr/bin/env bash
# Reinicia NiFi (instalación bajo el repo), espera a la UI y ejecuta recreate + verificación API.
# Uso: SIMLOG_NIFI_HOME=/ruta/nifi-2.0.0 ./scripts/reiniciar_nifi_y_recrear_flujo.sh
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE"

find_nifi_home() {
  if [[ -n "${SIMLOG_NIFI_HOME:-}" ]] && [[ -x "${SIMLOG_NIFI_HOME}/bin/nifi.sh" ]]; then
    echo "${SIMLOG_NIFI_HOME}"
    return 0
  fi
  if [[ -n "${NIFI_HOME:-}" ]] && [[ -x "${NIFI_HOME}/bin/nifi.sh" ]]; then
    echo "${NIFI_HOME}"
    return 0
  fi
  if [[ -x "$BASE/nifi-2.0.0/bin/nifi.sh" ]]; then
    echo "$BASE/nifi-2.0.0"
    return 0
  fi
  local d
  d="$(ls -d "$BASE"/nifi-* 2>/dev/null | head -1 || true)"
  if [[ -n "$d" && -x "$d/bin/nifi.sh" ]]; then
    echo "$d"
    return 0
  fi
  echo "No se encontró bin/nifi.sh; define SIMLOG_NIFI_HOME." >&2
  return 1
}

NH="$(find_nifi_home)"
echo "[SIMLOG] NiFi home: $NH"
export NIFI_HOME="$NH"
export SIMLOG_NIFI_HOME="$NH"
"$NH/bin/nifi.sh" restart || {
  echo "[SIMLOG] restart falló; intentando stop + start..."
  "$NH/bin/nifi.sh" stop || true
  sleep 4
  "$NH/bin/nifi.sh" start
}
# `restart` a veces solo termina el JVM; comprobar que hay proceso antes de confiar en la espera Python.
sleep 3
if ! pgrep -f "org.apache.nifi.NiFi" >/dev/null 2>&1; then
  echo "[SIMLOG] No hay proceso NiFi tras restart; lanzando start…"
  "$NH/bin/nifi.sh" start
fi

echo "[SIMLOG] Esperando API HTTPS (hasta ~120 s)..."
for i in $(seq 1 40); do
  if curl -sk -o /dev/null -w "%{http_code}" "https://127.0.0.1:8443/nifi-api/flow/status" | grep -qE '^(200|401|403)$'; then
    echo "[SIMLOG] NiFi API responde."
    break
  fi
  sleep 3
  if [[ "$i" -eq 40 ]]; then
    echo "[SIMLOG] WARN: timeout esperando NiFi; continúa igualmente." >&2
  fi
done

if [[ -f "$BASE/venv_transporte/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$BASE/venv_transporte/bin/activate"
fi

echo "[SIMLOG] recreate_nifi_practice_flow.py"
python "$BASE/scripts/recreate_nifi_practice_flow.py"

echo "[SIMLOG] verificar_flujo_nifi_api.py"
python "$BASE/scripts/verificar_flujo_nifi_api.py"
echo "[SIMLOG] Hecho. Arranca el process group PG_SIMLOG_KDD en la UI si los procesadores quedaron en STOPPED."
