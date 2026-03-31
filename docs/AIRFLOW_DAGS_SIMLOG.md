# DAGs Airflow — SIMLOG (`simlog_*`)

Este documento describe qué hace cada DAG de SIMLOG y qué partes son **automáticas** vs **manuales**.

> Convención: los DAGs operativos del proyecto deben comenzar por `simlog_`.

---

## 1) Catálogo de DAGs `simlog_*`

### `simlog_maestro` (automático, cada N minutos)

- **Objetivo**: ejecutar el pipeline completo de forma periódica: **checks → ingesta → Spark**.
- **Programación**: `schedule=timedelta(minutes=SIMLOG_INGESTA_INTERVAL_MINUTES)` (15 por defecto).
- **Tareas típicas**:
  - verificar infraestructura mínima (HDFS/Kafka/Cassandra),
  - ejecutar ingesta (snapshot → Kafka/HDFS),
  - ejecutar procesamiento Spark (grafo → Cassandra y opcional Hive).
- **Scripts implicados**:
  - ingesta: `ingesta/ingesta_kdd.py`
  - spark: `procesamiento/procesamiento_grafos.py`
- **Cuándo usarlo**: cuando quieres ver el dashboard “siempre actualizado” sin pulsar botones.

### `simlog_kdd_00_infra` … `simlog_kdd_99_consulta_final` (manual por fases)

Estos DAGs implementan el ciclo KDD por fases y encadenan la siguiente fase mediante `TriggerDagRunOperator`.

> Todos llevan `schedule=None`: se disparan a demanda.

| DAG | Qué hace | Datos/efecto principal |
|-----|----------|------------------------|
| `simlog_kdd_00_infra` | checks/arranque/verificación previa | valida stack y precondiciones |
| `simlog_kdd_01_seleccion` | ingesta (selección) | snapshot en Kafka + backup en HDFS |
| `simlog_kdd_02_preprocesamiento` | preprocesamiento del snapshot | normalización/validación previa a Spark |
| `simlog_kdd_03_transformacion` | Spark fase 3 | transformaciones y modelo operativo |
| `simlog_kdd_04_mineria` | Spark fase 4 | métricas (criticidad/PageRank, etc.) |
| `simlog_kdd_05_interpretacion` | Spark fase 5 | persistencia final (Cassandra y opcional Hive) |
| `simlog_kdd_99_consulta_final` | validación/consultas finales | evidencia de resultados y checks post-run |

### Utilidades de servicios (manuales)

| DAG | Qué hace | Cuándo usar |
|-----|----------|------------|
| `simlog_arranque_servicios` | intenta iniciar servicios del stack en orden | tras reinicio de máquina / demo |
| `simlog_parar_servicios` | para servicios en orden inverso | parar demo / mantenimiento |
| `simlog_comprobar_servicios` | `comprobar_todos()` (puertos/endpoints) | diagnóstico rápido |

---

## 2) Automático vs manual (resumen)

- **Automático**:
  - `simlog_maestro`: planificado cada *N* minutos.
  - cron/NiFi pueden disparar ingesta, pero el ciclo “completo” (ingesta + Spark) se recomienda orquestarlo con Airflow.

- **Manual**:
  - `simlog_kdd_*`: fases KDD a demanda (útil para docencia, depuración y control fino).
  - `simlog_*_servicios`: operaciones de arranque/parada/check.

---

## 3) Dónde viven y cómo se despliegan

Recomendación: mantener los DAGs en `$AIRFLOW_HOME/dags/simlog/` (y otros proyectos en subcarpetas).

- **Carpeta**: `~/airflow/dags/simlog/`
- **DAG maestro**: puede importarse desde el repo (`orquestacion/dag_simlog_maestro.py`) mediante un wrapper en `dags/simlog/` para evitar problemas de symlinks.

---

## 4) Troubleshooting (si no aparecen en la UI)

- Confirmar que el `api-server` está levantado en el puerto configurado (típico `8088` en este proyecto).
- Ejecutar:
  - `airflow dags list-import-errors`
  - `airflow dags list | grep '^simlog_'`
- Si faltan DAGs:
  - suele ser por `dag_id` duplicado, ruta no escaneada, o `api-server` en un puerto distinto.

