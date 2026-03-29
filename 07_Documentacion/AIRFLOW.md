# Airflow: ejecucion del pipeline sin Streamlit

Esta guia deja documentada la operativa para ejecutar SIMLOG solo con Airflow, sin depender del front de Streamlit.

## Arranque de Airflow

En dos terminales separadas:

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate
export AIRFLOW_HOME=~/airflow
export AIRFLOW__API__BASE_URL=http://127.0.0.1:8088
export AIRFLOW__API__PORT=8088
airflow api-server -H 0.0.0.0 -p 8088
```

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate
export AIRFLOW_HOME=~/airflow
export AIRFLOW__API__BASE_URL=http://127.0.0.1:8088
export AIRFLOW__API__PORT=8088
airflow scheduler
```

URL: `http://localhost:8088`

En `~/airflow/airflow.cfg`, la seccion `[api]` debe tener `base_url` y `port` alineados con el api-server (8088). Si `base_url` queda vacio, Airflow 3 usa `http://localhost:8080/execution/` para el LocalExecutor; si en 8080 hay otro servicio (p. ej. Spark UI), las tareas se quedan en **cola** sin ejecutarse.

## DAGs operativos (pipeline backend)

Los DAGs de operacion viven bajo `~/airflow/dags/` en **subcarpetas** (Airflow 3 **DAG bundles**), para que en la UI queden agrupados por proyecto sin depender solo del nombre del DAG:

| Bundle (nombre en UI) | Carpeta | Contenido |
|----------------------|---------|-----------|
| `dags-folder` | `~/airflow/dags/` | Raíz: mismo path que `[core] dags_folder`. Airflow 3 usa este nombre por defecto; debe figurar en `dag_bundle_config_list` además de `simlog`/`smart_grid`, o las ejecuciones antiguas (o DagVersions en BD) que referencian `dags-folder` fallan con `Requested bundle 'dags-folder' is not configured`. |
| `simlog` | `~/airflow/dags/simlog/` | Arranque / parada / comprobación stack, fases KDD `simlog_kdd_*` |
| `smart_grid` | `~/airflow/dags/smart_grid/` | DAGs legacy del otro proyecto (`*_smart_grid`, fases KDD antiguas, etc.) |

Además, cada DAG lleva etiquetas **`proyecto_simlog`** o **`proyecto_smart_grid`** para **filtrar por tag** en el listado de DAGs.

La configuración está en `~/airflow/airflow.cfg`, clave `[dag_processor]` → `dag_bundle_config_list` (tres `LocalDagBundle`: raíz `dags-folder` + `simlog` + `smart_grid`).

El codigo Python de los DAGs sigue apoyandose en el repo (`servicios/gestion_servicios.py`, `orquestacion/kdd_ejecucion.py`, etc.).

### Gestion de servicios

- `dag_arranque_servicios_smart_grid`: arranca HDFS, Cassandra, Kafka, Spark, Hive, Airflow y NiFi.
- `dag_comprobar_servicios_simlog`: comprueba estado de todos los servicios y falla si alguno no responde.
- `dag_parar_servicios_smart_grid`: detiene el stack de forma ordenada.

### Fases KDD (un DAG por fase)

- `simlog_kdd_00_infra`
- `simlog_kdd_01_seleccion`
- `simlog_kdd_02_preprocesamiento`
- `simlog_kdd_03_transformacion`
- `simlog_kdd_04_mineria`
- `simlog_kdd_05_interpretacion`
- `simlog_kdd_99_consulta_final`

La cadena se dispara desde `simlog_kdd_00_infra` mediante `TriggerDagRunOperator`.

### Pipeline periodico

- `simlog_maestro` (`orquestacion/dag_simlog_maestro.py`): cada 15 minutos verifica servicios base y ejecuta `ingesta_kdd.py` → `procesamiento/procesamiento_grafos.py`.

## Flujo recomendado sin Streamlit

1. Trigger manual de `dag_arranque_servicios_smart_grid`.
2. Trigger manual de `simlog_kdd_00_infra` para ejecutar todo el ciclo KDD por fases.
3. Opcionalmente, despausar `simlog_maestro` para ciclo continuo cada 15 minutos.
4. Al terminar una demo, trigger de `dag_parar_servicios_smart_grid`.

## Comandos utiles

```bash
airflow dags list
airflow dags list-import-errors
airflow dags reserialize
airflow dags trigger simlog_kdd_00_infra
airflow dags trigger dag_arranque_servicios_smart_grid
```

## Troubleshooting rapido

- **`ValueError: Requested bundle 'dags-folder' is not configured`**: en `[dag_processor] dag_bundle_config_list` añade un bundle con `"name": "dags-folder"` y `"path"` igual a `[core] dags_folder` (normalmente `~/airflow/dags`). Reinicia **scheduler** y **api-server** (o `dag-processor` si lo usas separado).
- Tareas en **queued** sin arrancar: revisa `[api] base_url` y `port` (8088) y reinicia **api-server** y **scheduler**; o exporta `AIRFLOW__API__BASE_URL=http://127.0.0.1:8088` antes de lanzarlos.
- Si no ves DAGs nuevos: ejecuta `airflow dags reserialize` y refresca la UI.
- Si un DAG falla por rutas/entorno: verifica `AIRFLOW_HOME`, `HADOOP_HOME`, `KAFKA_HOME`, `SPARK_HOME`, `HIVE_HOME`, `NIFI_HOME`.
- Si `dag_comprobar_servicios_simlog` falla: revisa puertos (`9870`, `9092`, `9042`, `7077`, `10000`, `8088`, `8443`/`8080`) y levanta con el DAG de arranque.
- **Hive (10000) no arranca**: el script usa `HADOOP_CLASSPATH` vacío y escribe en `/tmp/hadoop/hiveserver2-daemon.log`. Si el log habla de Derby bloqueada, cierra procesos Hive viejos y revisa locks en el directorio del metastore (según `hive-site.xml`). Opcional: `SIMLOG_HIVE_MAX_WAIT_SEC`, `SIMLOG_HIVE_CONF_DIR`, `SIMLOG_HIVE_LOG`.
