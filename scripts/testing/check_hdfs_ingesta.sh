#!/usr/bin/env bash
set -euo pipefail

HDFS_PATH="${HDFS_BACKUP_PATH:-/user/hadoop/transporte_backup}"
HDFS_BIN="${HDFS_BIN:-hdfs}"

echo "[HDFS] Listando JSON en ${HDFS_PATH}"
"${HDFS_BIN}" dfs -ls "${HDFS_PATH}"
