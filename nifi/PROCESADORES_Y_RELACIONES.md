# NiFi — Process Group SIMLOG: procesadores, relaciones y criterio de diseño

Este documento describe **cómo está pensado** el flujo NiFi en el proyecto **SIMLOG España** (standalone), alineado con el mismo modelo de datos que **`ingesta/ingesta_kdd.py`**, **`config.py`** (Kafka raw/filtered, HDFS backup) y el proyecto de referencia tipo **Sentinel360 / PDF Big Data**: *ingesta → cola → almacén → Spark → Cassandra/Hive*.

No sustituye la UI de NiFi: sirve para **reconstruir** el grupo en cualquier entorno y justificar cada pieza.

---

## 1. Process Group: `PG_SIMLOG_KDD`

| Campo | Valor |
|--------|--------|
| **Nombre** | `PG_SIMLOG_KDD` (coincide con `NIFI_PROCESS_GROUP_NAME` en `config.py`) |
| **Objetivo** | Ingesta periódica (p. ej. cada 15 min): JSON enriquecido → **Kafka** (auditoría + filtrado) → **HDFS** (backup para Spark) → opcionalmente disparo de **Spark** (batch). |
| **Alternativa a Python** | Misma función que `python -m ingesta.ingesta_kdd`; NiFi aporta **UI**, **linaje** y **reintentos** sin cambiar el contrato del JSON. |

---

## 2. Dos enfoques (elige uno)

### A) **Flujo compacto (recomendado para ingesta estable)**

Un solo **ExecuteScript (Groovy)** genera el JSON **completo** (clima OpenWeather + simulación), igual que el script Python.

**Por qué:** evita `MergeContent` entre ramas (sincronización de N FlowFiles, `Minimum Number of Entries`, errores fáciles de cometer). Es el mismo criterio que en muchos proyectos Big Data: **un FlowFile = un snapshot de ingesta**.

**Archivo:** `groovy/GenerateFullPayloadSimlog.groovy` (sustituye al esqueleto anterior si solo generabas GPS sintético).

### B) **Flujo granular (didáctico / PDF)**

`GenerateFlowFile` → GPS sintético (`GenerateSyntheticPayload.groovy`) **en paralelo** con `InvokeHTTP` por hub → `MergeContent` → …

**Por qué:** enseña cada “caja” (KDD, datos externos, unión). **Coste:** más procesadores, más relaciones y más parámetros de `MergeContent` (ver notas al final de `flow/simlog_kdd_flow_spec.yaml`).

---

## 3. Flujo A — lista de procesadores y relaciones

Orden lógico de ejecución (todos en el mismo `PG_SIMLOG_KDD`).

| # | Procesador tipo | Nombre sugerido | Rol / por qué |
|---|-----------------|-----------------|---------------|
| 1 | **GenerateFlowFile** | `Timer_Ingesta_15min` | **Disparo temporal** (KDD “cada ventana de 15 min”). Sin entrada externa; genera un FlowFile vacío que arranca el pipeline. Alternativa a `Cron` externo o Airflow. |
| 2 | **UpdateAttribute** | `Set_Parametros_Ingesta` | Fija atributos usados por el script: `owm.api.key` (desde **Parameter Context**), `paso_15min` (opcional). **Por qué:** separar secretos y parámetros del código Groovy. |
| 3 | **ExecuteScript** | `Build_JSON_Ingesta_KDD` | **Script Engine: Groovy**, cuerpo = `groovy/GenerateFullPayloadSimlog.groovy`. **Por qué:** una sola pieza que replica la lógica de `ingesta_kdd.py` (HTTP + simulación) sin encadenar 5× `InvokeHTTP` + `Merge`. |
| 4 | **PublishKafka** | `Kafka_Topic_RAW` | Publica el JSON en **`transporte_raw`** (`TOPIC_RAW` en `config.py`). **Por qué:** trazabilidad / auditoría de datos crudos (PDF). |
| 5 | **PublishKafka** | `Kafka_Topic_Filtered` | Mismo contenido (o el que se considere “validado”) en **`transporte_filtered`**. **Por qué:** mismo esquema que `TOPIC_TRANSPORTE` = filtered para consumidores Spark/Streaming. |
| 6 | **PutHDFS** | `HDFS_Backup_JSON` | Escribe el JSON bajo **`HDFS_BACKUP_PATH`** (p. ej. `…/transporte_YYYYMMDD_HHMMSS.json`). **Por qué:** Spark lee HDFS si Kafka no está o para reprocesar — igual que `ingesta/ingesta_kdd.py`. |
| 7 | *(Opcional)* **ExecuteProcess** | `Spark_Submit_Procesamiento` | Lanza `spark-submit` / `nifi/scripts/spark_submit_yarn.sh` hacia `procesamiento_procesamiento_grafos`. **Por qué:** NiFi orquesta jobs; **no** ejecuta GraphFrames en línea. |

### Relaciones (conexiones)

```
[1 GenerateFlowFile]  --success-->  [2 UpdateAttribute]
[2 UpdateAttribute]   --success-->  [3 ExecuteScript]

[3 ExecuteScript] --success--+--> [4 PublishKafka RAW]
                             +--> [5 PublishKafka FILTERED]
                             +--> [6 PutHDFS]

[3 ExecuteScript] --failure-->  (LogAttribute / PutFile DLQ / notificación)

[6 PutHDFS] --success-->  [7 ExecuteProcess Spark]   (opcional)
```

**Notas:**

- En NiFi, **una misma relación `success` puede bifurcar a varios procesadores** sin `DuplicateFlowFile`: tres cables desde [3] a [4], [5] y [6].
- **Kafka** y **HDFS** pueden ir en paralelo; si uno falla, configura **retry** en el procesador o **funnel** + **PutFile** para no perder el FlowFile.
- **Spark (7)** no es obligatorio si ya usas **Airflow** (`dag_maestro`) o el dashboard para lanzar procesamiento.

---

## 4. Por qué cada tipo de procesador (criterio técnico)

| Tipo | Criterio |
|------|----------|
| **GenerateFlowFile** | Patrón estándar NiFi para “reloj”; no requiere cola externa. |
| **UpdateAttribute** | Evita embebidos secretos en Groovy; integración con **Parameter Providers** / variables de entorno. |
| **ExecuteScript (Groovy)** | Flexibilidad total para JSON + HTTP (OpenWeather) y mismas claves que Python (`nodos_estado`, `aristas_estado`, `clima_hubs`, `camiones`). |
| **PublishKafka** (×2) | Cumplimiento del modelo **raw vs filtered** del PDF y `config.py`. |
| **PutHDFS** | Contrato con Spark: ficheros en la ruta de backup. |
| **ExecuteProcess** | Spark/YARN es un **proceso batch**; NiFi no sustituye a Spark. |

---

## 5. Flujo B — referencia (ramas + Merge)

Si montas el flujo **desglosado** (GPS + `InvokeHTTP` por hub + `MergeContent`):

- **GenerateFlowFile** + **ExecuteScript** (`GenerateSyntheticPayload.groovy`) → **una** rama.
- **UpdateAttribute** + **InvokeHTTP** (×5 hubs) → **cinco** ramas o **una** rama con `RouteOnAttribute` + bucle.
- **MergeContent** con `Minimum Number of Entries` = número de ramas que deben confluir (p. ej. 6 si 5 hubs + 1 sintético).

**Problema típico:** la especificación anterior en YAML mezclaba conexiones sin garantizar que el mismo `Merge` recibiera exactamente los FlowFiles esperados. Por eso el flujo **A** es el recomendado para **ingesta funcional** con menos fricción.

Detalle histórico: `flow/simlog_kdd_flow_spec.yaml` incluye versión **v2** (compacta) y notas **v1** (granular).

---

## 6. Controller Services (según despliegue)

| Servicio | Uso |
|----------|-----|
| **SSLContextService** | Kafka/HDFS con TLS. |
| **Hadoop Configuration Resources** (PutHDFS) | `core-site.xml` + `hdfs-site.xml` del cluster (o Docker). |
| **Record Reader/Writer** | Solo si pasas a **MergeRecord** / **ValidateRecord**; el flujo A no lo exige. |

---

## 7. Parámetros (Parameter Context)

Alineados con `nifi/parameter-context.env.example` y `config.py`:

- `KAFKA_BOOTSTRAP`, `TOPIC_RAW`, `TOPIC_FILTERED`
- `HDFS_BACKUP_PATH` (o `HDFS_RAW_BASE` para subcarpeta `nifi_raw/`)
- `OWM_API_KEY` → mapear a atributo `owm.api.key` en UpdateAttribute

---

## 8. Relación con el otro proyecto (Sentinel360 / práctica)

| Concepto | Sentinel360 / PDF | SIMLOG NiFi |
|----------|-------------------|-------------|
| Datos crudos vs filtrados | Dos temas | Dos `PublishKafka` |
| Almacén distribuido | HDFS | `PutHDFS` |
| Cola | Kafka | Kafka |
| Procesamiento pesado | Spark | Fuera de NiFi (batch) o `ExecuteProcess` |
| Tiempo casi real | Cassandra | Spark → Cassandra (igual que Python) |

---

## 9. Archivos del repositorio

| Archivo | Contenido |
|---------|-----------|
| `groovy/GenerateFullPayloadSimlog.groovy` | Ingesta completa en un solo script (flujo A). |
| `groovy/GenerateSyntheticPayload.groovy` | Solo GPS/shell (flujo B + Merge). |
| `flow/simlog_kdd_flow_spec.yaml` | Especificación mecánica (v1 granular + v2 compacta). |
| `README_NIFI.md` | Visión general y seguridad. |

Con esto, **a vista del proyecto** queda definido el **grupo de procesadores**, los **tipos** elegidos y las **relaciones** para que la **ingesta sea funcional** y coherente con el stack Python que ya usáis.
