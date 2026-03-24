#!/usr/bin/env bash
# Ejemplo: ExecuteProcess en NiFi → spark-submit sobre YARN
# Ajusta SPARK_HOME, rutas y memoria según tu nodo (4GB RAM: usar client + memoria baja)

set -euo pipefail

export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export PROJECT_ROOT="${PROJECT_ROOT:-$HOME/proyecto_transporte_global}"
export SPARK_MASTER="${SPARK_MASTER:-yarn}"
export DEPLOY_MODE="${DEPLOY_MODE:-client}"

# En laboratorio sin YARN real, usar: SPARK_MASTER=local SPARK_LOCAL_IP=127.0.0.1
if [[ "${SPARK_MASTER}" == "local" ]]; then
  exec "${SPARK_HOME}/bin/spark-submit" \
    --master local \
    --conf spark.driver.memory=512m \
    --conf spark.driver.host=127.0.0.1 \
    --conf spark.driver.bindAddress=127.0.0.1 \
    --conf spark.cassandra.connection.host="${CASSANDRA_HOST:-127.0.0.1}" \
    "${PROJECT_ROOT}/procesamiento/procesamiento_grafos.py"
fi

exec "${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER}" \
  --deploy-mode "${DEPLOY_MODE}" \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  --conf spark.executor.instances=1 \
  --conf spark.cassandra.connection.host="${CASSANDRA_HOST:-127.0.0.1}" \
  --conf spark.yarn.submit.waitAppCompletion=true \
  "${PROJECT_ROOT}/procesamiento/procesamiento_grafos.py"
