# Airflow — DAGs por fase KDD (SIMLOG)

## Archivo principal

| Fichero | Descripción |
|---------|-------------|
| **`dag_simlog_kdd_fases.py`** | **Siete DAGs** en un solo archivo: fases 0, 1–5 y 99. Cadena secuencial con `TriggerDagRunOperator`. |
| `dag_maestro.py` | Pipeline clásico cada 15 min (ingesta + Spark) — alternativa compacta. |
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
