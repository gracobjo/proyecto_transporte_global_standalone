# KDD Airflow: éxito por fase, artefactos, logs e integración con el front

Este documento describe qué significa que cada DAG `simlog_kdd_*` termine **en verde**, qué datos deja en disco y cómo reutilizarlos en la UI o en una API.

## Convenciones generales

| Concepto | Ubicación |
|----------|-----------|
| Informes por tarea | `reports/kdd/<run_id>/informe_<fase>_<timestamp>.md` y `.html` |
| Datos de trabajo compartidos | `reports/kdd/work/` (JSON que leen varias fases) |
| **Éxito Airflow** | El `PythonOperator` termina sin excepción → tarea **success** |
| **Éxito lógico** | Además, el JSON del informe (sección «2. Resultado») no contiene errores críticos según la fase (p. ej. `returncode: 0` en Spark) |
| `run_id` | En disparos manuales suele ser `manual__2026-...` (carpeta bajo `reports/kdd/`) |

La función `con_informe` en `orquestacion/kdd_ejecucion.py` envuelve cada fase: ejecuta la lógica, y **siempre** escribe Markdown + HTML con **datos de inicio** y **resultado** en JSON legible.

### Cómo ver o imprimir un informe

1. **En disco:** abre el `.md` en el IDE o el `.html` en el navegador (imprimir → PDF).
2. **Desde terminal:** `xdg-open reports/kdd/<run_id>/informe_05_interpretacion_*.html` (o ruta absoluta al proyecto).
3. **Airflow:** pestaña del DAG → instancia → **Log** del task (stdout incluye la ruta del informe devuelta por `con_informe`).
4. **Front existente:** pestaña **Pruebas** → tabla «Ejecuciones Airflow» usa `informe_99*.md` como evidencia de cadena completa (`servicios/pruebas_ingesta.py`).

### Logs Airflow (no son el informe, pero explican fallos)

- Directorio típico: `$AIRFLOW_HOME/logs/dag_id=simlog_kdd_XX/.../attempt=1.log`
- Ahí verás trazas Python, `RuntimeError` de Spark y la ruta devuelta al acabar bien.

---

## Por fase: qué hace, éxito y qué mirar

### Fase 0 — `simlog_kdd_00_infra` (`00_infra`)

| | |
|---|---|
| **Qué hace** | Arranca/verifica HDFS, Cassandra y Kafka (`servicios_arranque`) y comprueba puertos 9870, 9092, 9042. |
| **Éxito** | `todos_servicios_ok: true` en el resultado del informe (o al menos mensajes de arranque sin fallo irrecuperable). |
| **Artefactos** | Solo informe; no escribe en `work/`. |
| **Front / explotación** | Mostrar `puertos_ok` del JSON del informe como semáforo. |

### Fase 1 — `simlog_kdd_01_seleccion` (`01_seleccion`)

| | |
|---|---|
| **Qué hace** | Llama a la API de clima (`consulta_clima_hubs`) y guarda `reports/kdd/work/fase1_clima.json`. |
| **Éxito** | Fichero `fase1_clima.json` creado; informe con `hubs_con_clima` no vacío (salvo red caída). |
| **Artefactos** | `work/fase1_clima.json` |
| **Front** | Leer JSON para mapas/resumen de clima en la misma ejecución. |

### Fase 2 — `simlog_kdd_02_preprocesamiento` (`02_preprocesamiento`)

| | |
|---|---|
| **Qué hace** | Simula nodos/aristas/camiones, publica a Kafka, backup HDFS, escribe `work/ultimo_payload.json`. |
| **Éxito** | `kafka_ok` y `hdfs_ok` en resultado; payload presente. |
| **Artefactos** | `work/ultimo_payload.json` (también usado por ingesta estándar). |
| **Front** | Pestaña pipeline ya usa `leer_ultima_ingesta()` que lee estos paths; alinear `run_id` si muestras informe KDD. |

### Fase 3 — `simlog_kdd_03_transformacion` (`03_transformacion`)

| | |
|---|---|
| **Qué hace** | Spark: grafo GraphFrames + autosanación → `work/fase3_metricas.json`. |
| **Éxito** | `returncode: 0` en resultado anidado de Spark; métricas `vertices` / `edges` en el JSON del informe. |
| **Artefactos** | `work/fase3_metricas.json` |
| **Front** | Gráfico de tamaño de grafo; comparar con fases anteriores. |

### Fase 4 — `simlog_kdd_04_mineria` (`04_mineria`)

| | |
|---|---|
| **Qué hace** | Spark PageRank (subconjunto top) → `work/fase4_pagerank.json`. |
| **Éxito** | `returncode: 0`; lista `top_pagerank` en resultado. |
| **Artefactos** | `work/fase4_pagerank.json` |
| **Front** | Tabla o ranking de nodos críticos. |

### Fase 5 — `simlog_kdd_05_interpretacion` (`05_interpretacion`)

| | |
|---|---|
| **Qué hace** | `procesamiento_grafos.main()` — persistencia en **Cassandra** y **Hive** (con `SIMLOG_ENABLE_HIVE`). |
| **Éxito** | `returncode: 0`; existe `work/fase5_resumen.json`; tablas operativas con datos (validar en fase 99 o dashboard). |
| **Artefactos** | `work/fase5_resumen.json` + tablas en Cassandra/Hive. |
| **Front** | Mapa, cuadro de mando, pestaña «Resultados pipeline» (`obtener_snapshot_pipeline`). |

### Fase 99 — `simlog_kdd_99_consulta_final` (`99_consulta_final`)

| | |
|---|---|
| **Qué hace** | `SELECT COUNT(*)` en tablas clave de Cassandra (`nodos_estado`, `aristas_estado`, etc.). |
| **Éxito** | `cassandra_conteos` con números ≥ 0 (no `error:`). |
| **Artefactos** | Solo informe (resumen de conteos). |
| **Front** | La UI de **Pruebas** detecta `informe_99*.md` como **cadena KDD completa** (`listar_cadenas_kdd_airflow_recientes`). |

---

## Explotación en el front (resumen)

1. **Pestaña Pruebas (Streamlit):** sección **«Informes KDD por ejecución»** — selector de `run_id`, botones **Abrir** (`file://` al HTML) y **Descargar** del Markdown, tabla de rutas y catálogo de fases.
2. **Listar cadenas completas:** `listar_cadenas_kdd_airflow_recientes()` → rutas a `informe_99_*.md`.
3. **Último informe cualquiera:** `_latest_airflow_report()` en `pruebas_ingesta.py` (más reciente por mtime).
4. **Todos los informes de un `run_id`:** `servicios/kdd_informes_lectura.py` → `informes_por_run_id(run_id)`.
5. **Parsear contenido estructurado:** los informes ya incluyen JSON en bloques; para APIs es más estable leer los ficheros de `work/*.json` por timestamp de ejecución o copiar el dict al registrar la prueba.

6. **PDF:** instrucciones al pie de cada `.md` (pandoc o imprimir HTML).

---

## Variables útiles

- `SIMLOG_AIRFLOW_SPARK_TIMEOUT_SEC` — timeout de subprocesos Spark en fases 3–5 (por defecto alto en KDD).
- Rutas de proyecto: si Airflow corre en otro host, monta el mismo volumen para que `reports/kdd/` sea visible al Streamlit.
