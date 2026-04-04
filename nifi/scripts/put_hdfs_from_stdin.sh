#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$HOME/proyecto_transporte_global}"
HDFS_BACKUP_PATH="${HDFS_BACKUP_PATH:-/user/hadoop/transporte_backup}"
TS="$(date +%Y%m%d_%H%M%S)"
TMP_DIR="${PROJECT_ROOT}/logs/nifi_tmp"
TMP_FILE="${TMP_DIR}/simlog_${TS}.json"
HDFS_DIR="${HDFS_BACKUP_PATH}/nifi_raw/$(date +%Y/%m/%d/%H)"

mkdir -p "${TMP_DIR}"
cat > "${TMP_FILE}"

hdfs dfs -mkdir -p "${HDFS_DIR}"
hdfs dfs -put -f "${TMP_FILE}" "${HDFS_DIR}/"
# Copia plana para lecturas con patrón *.json (ingesta Python) y compatibilidad Spark batch.
hdfs dfs -mkdir -p "${HDFS_BACKUP_PATH}"
hdfs dfs -put -f "${TMP_FILE}" "${HDFS_BACKUP_PATH}/transporte_nifi_${TS}.json"

printf '{"status":"ok","hdfs_dir":"%s","tmp_file":"%s"}\n' "${HDFS_DIR}" "${TMP_FILE}"
