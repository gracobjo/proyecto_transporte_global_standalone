# Airflow: ejecucion del pipeline sin Streamlit

Esta guia deja documentada la operativa para ejecutar SIMLOG solo con Airflow, sin depender del front de Streamlit.

## Arranque de Airflow

En dos terminales separadas:

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate
export AIRFLOW_HOME=~/airflow
airflow api-server -H 0.0.0.0 -p 8088
```

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate
export AIRFLOW_HOME=~/airflow
airflow scheduler
```

URL: `http://localhost:8088`

## DAGs operativos (pipeline backend)

Los DAGs de operacion viven en `~/airflow/dags` y se apoyan en codigo del repo (`servicios/gestion_servicios.py` y `orquestacion/kdd_ejecucion.py`).

### Gestion de servicios

- `dag_arranque_servicios_smart_grid`: arranca HDFS, Cassandra, Kafka, Spark, Hive, Airflow y NiFi.
- `dag_comprobar_servicios_smart_grid`: comprueba estado de todos los servicios y falla si alguno no responde.
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

- `dag_maestro_smart_grid`: cada 15 minutos verifica servicios base y ejecuta `ingesta_kdd.py` -> `procesamiento/procesamiento_grafos.py`.

## Flujo recomendado sin Streamlit

1. Trigger manual de `dag_arranque_servicios_smart_grid`.
2. Trigger manual de `simlog_kdd_00_infra` para ejecutar todo el ciclo KDD por fases.
3. Opcionalmente, despausar `dag_maestro_smart_grid` para ciclo continuo cada 15 minutos.
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

- Si no ves DAGs nuevos: ejecuta `airflow dags reserialize` y refresca la UI.
- Si un DAG falla por rutas/entorno: verifica `AIRFLOW_HOME`, `HADOOP_HOME`, `KAFKA_HOME`, `SPARK_HOME`, `HIVE_HOME`, `NIFI_HOME`.
- Si `dag_comprobar_servicios_smart_grid` falla: revisa puertos (`9870`, `9092`, `9042`, `7077`, `10000`, `8088`, `8443`/`8080`) y levanta con el DAG de arranque.
