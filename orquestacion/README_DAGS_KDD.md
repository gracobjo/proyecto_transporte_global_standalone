# Airflow — DAGs por fase KDD (SIMLOG)

## Archivo principal

| Fichero | Descripción |
|---------|-------------|
| **`dag_simlog_kdd_fases.py`** | **Siete DAGs** en un solo archivo: fases 0, 1–5 y 99. Cadena secuencial con `TriggerDagRunOperator`. |
| `dag_maestro.py` | Pipeline clásico cada 15 min (`dag_id=simlog_pipeline_maestro`): `python -m ingesta.ingesta_kdd` + Spark. |
| **`dag_simlog_maestro.py`** | Mismo espíritu que `dag_maestro.py`, con **`dag_id=simlog_maestro`** y prefijo `simlog_` en la UI; llama a `ingesta_kdd.py` en raíz + `procesamiento_grafos.py`. |
| `dag_arranque_servicios.py` | Solo arranque de servicios (manual). La fase 0 del flujo KDD también arranca servicios. |

## Secuencia

1. **`simlog_kdd_00_infra`** — Arranque HDFS/Cassandra/Kafka (ver `servicios_arranque.py`) + comprobación de puertos + **informe**.
2. **`simlog_kdd_01_seleccion`** — API OpenWeather (5 hubs) + JSON intermedio `reports/kdd/work/fase1_clima.json` + informe.
3. **`simlog_kdd_02_preprocesamiento`** — Simulación, GPS, Kafka, HDFS + `ultimo_payload.json` + informe.
4. **`simlog_kdd_03_transformacion`** — Spark: grafo GraphFrames + métricas (`fase3_metricas.json`) + informe.
5. **`simlog_kdd_04_mineria`** — Spark: PageRank (`fase4_pagerank.json`) + informe.
6. **`simlog_kdd_05_interpretacion`** — Spark: `procesamiento_grafos.main()` (Cassandra/Hive) + informe.
7. **`simlog_kdd_99_consulta_final`** — Conteos en Cassandra + informe (no detiene servicios).

## Informes (exportar a PDF)

Tras cada DAG se generan:

- `reports/kdd/<run_id>/informe_<fase>_<timestamp>.md`
- El mismo contenido en `.html` (imprimir desde el navegador: **Ctrl+P → Guardar como PDF**).

Con [Pandoc](https://pandoc.org/):

```bash
cd ~/proyecto_transporte_global/reports/kdd/<run_id>/
pandoc informe_*.md -o informe.pdf --pdf-engine=xelatex
```

> La carpeta `reports/kdd/` está en `.gitignore`.

## DAG maestro `simlog_maestro` (ingesta + Spark)

El fichero canónico está en el repo: **`orquestacion/dag_simlog_maestro.py`** (`dag_id=simlog_maestro`). No mantener una copia editada solo bajo `~/airflow/dags/`.

Despliegue recomendado (un symlink):

```bash
mkdir -p "$AIRFLOW_HOME/dags/simlog"
ln -sf /ruta/al/proyecto/orquestacion/dag_simlog_maestro.py \
       "$AIRFLOW_HOME/dags/simlog/dag_simlog_maestro.py"
```

Python resuelve el symlink: la raíz del proyecto se obtiene desde `orquestacion/`. Si en tu instalación copias el `.py` a otra ruta sin symlink, define **`SIMLOG_PROJECT_ROOT`** apuntando a la raíz del repositorio.

Hive en Spark: la tarea de procesamiento exporta **`SIMLOG_ENABLE_HIVE=1`**. Timeout configurable: **`SIMLOG_AIRFLOW_SPARK_TIMEOUT_SEC`** (por defecto 900).

> Otro DAG “maestro” en el mismo repo: **`dag_maestro.py`** (`dag_id=simlog_pipeline_maestro`), con `python -m ingesta.ingesta_kdd` y mismas convenciones; elige uno u otro según despliegue.

## Despliegue en Airflow

1. Copiar al directorio de DAGs (ej. `~/airflow/dags/simlog/`) al menos:
   - `dag_simlog_kdd_fases.py`
   - `kdd_ejecucion.py`, `kdd_informe.py`, `servicios_arranque.py`
2. Asegurar que el **PYTHONPATH** incluye la raíz del proyecto (`~/proyecto_transporte_global`) **o** instalar dependencias en el mismo entorno que el scheduler/worker.
3. Disparar manualmente **`simlog_kdd_00_infra`**; el resto se encadena solo.

## Encadenado

Cada DAG termina con `TriggerDagRunOperator` al siguiente (`wait_for_completion=False`). Si un DAG falla, el siguiente no se dispara hasta que lo ejecutes a mano.

## Ajuste del “paso” 15 minutos

Las tareas usan `data_interval_start` del DAG Run para calcular `PASO_15MIN`. En ejecución manual, puedes fijar `PASO_15MIN` en **Variables** de Airflow o en el entorno del worker si amplías las funciones.

## Por qué no ves una tarea Airflow separada «Spark lee HDFS → escribe Hive»

En la UI de Airflow cada **PythonOperator** es **una caja** en el grafo. Tanto la **fase 5** (`simlog_kdd_05_interpretacion`) como el **DAG maestro** (`simlog_pipeline_maestro`) lanzan **un solo proceso** que ejecuta el script de Spark (`fase_kdd_spark --fase interpretacion` o `procesamiento/procesamiento_grafos.py`). Dentro de ese proceso ocurre, en secuencia:

1. Lectura de JSON en **HDFS** (`HDFS_BACKUP_PATH`, backup de la ingesta).
2. Enriquecimiento / maestro en **Hive** (`nodos_maestro`, etc.) cuando `SIMLOG_ENABLE_HIVE=1` y Spark arranca con `enableHiveSupport()`.
3. Escritura en **Cassandra** y tablas históricas **Hive** (`historico_nodos`, …).

Eso **no** se divide en tres tareas Airflow salvo que alguien refactorice el DAG (por ejemplo con `BashOperator` solo para `hdfs dfs -ls` a modo de comprobación visible).

### Desalineaciones habituales

| Situación | Efecto |
|-----------|--------|
| Solo ejecutaste **`simlog_pipeline_maestro`** con el código antiguo | Llamaba a `procesamiento_grafos.py` **en la raíz** (legacy) **sin** `SIMLOG_ENABLE_HIVE` → Hive en Spark desactivado. **Corregido en el repo:** ahora invoca `procesamiento/procesamiento_grafos.py` con `SIMLOG_ENABLE_HIVE=1`. |
| Ejecutaste fases KDD pero **no la 5** | Cassandra/Hive del pipeline «completo» no se rellenan igual que en fase 5. |
| **No hay JSON** en `HDFS_BACKUP_PATH` | `main()` hace **fallback a simulación** en memoria; Hive/Cassandra se actualizan, pero no vienen de HDFS. |
| DAGs no copiados a `~/airflow/dags/` (o bundle `simlog` no configurado) | No verás `simlog_kdd_*` en la UI aunque existan en el repositorio. |

### Variable útil

- `SIMLOG_AIRFLOW_SPARK_TIMEOUT_SEC` (por defecto **900** en el DAG maestro): tiempo máximo del task de Spark.
