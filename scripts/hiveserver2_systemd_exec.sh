#!/usr/bin/env bash
# Ejecutable largo para systemd (HiveServer2 en primer plano).
# Instalación: ver orquestacion/systemd/simlog-hiveserver2.service
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export HIVE_CONF_DIR="${HIVE_CONF_DIR:-$ROOT/hive/conf}"
export HIVE_METASTORE_URIS="${HIVE_METASTORE_URIS:-thrift://127.0.0.1:9083}"
if [[ -z "${HIVE_HOME:-}" ]]; then
  for c in /home/hadoop/apache-hive-4.2.0-bin /home/hadoop/apache-hive-3.1.3-bin /opt/hive; do
    if [[ -x "$c/bin/hive" ]]; then
      export HIVE_HOME="$c"
      break
    fi
  done
fi
if [[ -z "${HIVE_HOME:-}" || ! -x "${HIVE_HOME}/bin/hive" ]]; then
  echo "hiveserver2_systemd_exec.sh: define HIVE_HOME o instala Hive" >&2
  exit 1
fi
exec "${HIVE_HOME}/bin/hive" --service hiveserver2
