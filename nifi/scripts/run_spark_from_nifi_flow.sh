#!/usr/bin/env bash
# Disparado por NiFi (ExecuteStreamCommand) tras escribir el JSON en HDFS.
# Misma semántica que spark_submit_yarn.sh con PROJECT_ROOT fiable.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${PROJECT_ROOT:-$(cd "${HERE}/../.." && pwd)}"
export PROJECT_ROOT="$ROOT"
export SPARK_MASTER="${SPARK_MASTER:-local}"
export DEPLOY_MODE="${DEPLOY_MODE:-client}"
export CASSANDRA_HOST="${CASSANDRA_HOST:-127.0.0.1}"
exec bash "${ROOT}/nifi/scripts/spark_submit_yarn.sh"
