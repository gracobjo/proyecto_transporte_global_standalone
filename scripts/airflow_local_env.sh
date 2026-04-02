#!/usr/bin/env bash
# Variables para Airflow en el host (sin Docker): ingesta vía DAG `simlog_maestro`.
#
# Uso típico:
#   source scripts/airflow_local_env.sh
#   ./scripts/run_airflow_local.sh
#
# SIMLOG_AIRFLOW_LOCAL=1 omite comprobaciones HDFS/Kafka/Cassandra y la tarea Spark
# (solo ejecuta la ingesta `python -m ingesta.ingesta_kdd`).

_AIRFLOW_ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
export SIMLOG_PROJECT_ROOT="$(cd "$_AIRFLOW_ENV_DIR/.." && pwd)"

export SIMLOG_AIRFLOW_LOCAL="${SIMLOG_AIRFLOW_LOCAL:-1}"
export SIMLOG_ENABLE_HIVE="${SIMLOG_ENABLE_HIVE:-0}"
export AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"

# Ingesta puede tardar (APIs clima, red); subir si hace falta
export SIMLOG_AIRFLOW_INGESTA_TIMEOUT_SEC="${SIMLOG_AIRFLOW_INGESTA_TIMEOUT_SEC:-300}"
