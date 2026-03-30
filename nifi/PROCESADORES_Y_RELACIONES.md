# NiFi — Process Group SIMLOG: procesadores, relaciones y criterio de diseño

Este documento describe **cómo está pensado** el flujo NiFi en el proyecto **SIMLOG España** (standalone), alineado con el mismo modelo de datos que **`ingesta/ingesta_kdd.py`**, **`config.py`** (Kafka raw/filtered, HDFS backup) y el proyecto de referencia tipo **Sentinel360 / PDF Big Data**: *ingesta → cola → almacén → Spark → Cassandra/Hive*.

No sustituye la UI de NiFi: sirve para **reconstruir** el grupo en cualquier entorno y justificar cada pieza.

---

## 1. Process Group: `PG_SIMLOG_KDD`

| Campo | Valor |
|--------|--------|
| **Nombre** | `PG_SIMLOG_KDD` (coincide con `NIFI_PROCESS_GROUP_NAME` en `config.py`) |
| **Objetivo** | Ingesta periódica (p. ej. cada 15 min): JSON enriquecido con **Open-Meteo (sin API key) o DATEX2 DGT como respaldo/realidad operativa** → **Kafka** (auditoría + filtrado) → **HDFS** (backup para Spark) → opcionalmente disparo de **Spark** (batch). |
| **Alternativa a Python** | Misma función que `python -m ingesta.ingesta_kdd`; NiFi aporta **UI**, **linaje** y **reintentos** sin cambiar el contrato del JSON. |

---

## 2. Dos enfoques (elige uno)

### A) **Flujo compacto (recomendado para ingesta estable)**

Un solo **ExecuteScript (Groovy)** puede generar el JSON **completo** (clima Open-Meteo + simulación), igual que el script Python.

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
| 2 | **UpdateAttribute** | `Set_Parametros_Ingesta` | Fija atributos: `dgt.url`, `paso_15min` (opcional). |
| 3 | **ExecuteScript** | `Build_JSON_Ingesta_KDD` | Groovy `GenerateSyntheticGpsForPractice.groovy`: genera snapshot sintético base y marca provenance (`simlog.provenance.stage=synthetic_payload`). |
| 4 | **InvokeHTTP** | `OpenMeteo_InvokeHTTP` | Petición GET multi-hub Open-Meteo; respuesta en atributo `owm.response` (nombre histórico). |
| 5 | **ExecuteScript** | `Merge_Weather_Into_Payload` | Une clima al JSON solo si la respuesta es válida; si no, deja pasar el payload para que continúe el respaldo DGT. |
| 6 | **InvokeHTTP** | `DGT_DATEX2_InvokeHTTP` | Descarga XML DATEX2 v3.6 de la DGT y lo deja en atributo `dgt.response.xml`. |
| 7 | **ExecuteScript** | `Merge_DGT_Into_Payload` | Fusiona incidencias DGT con prioridad sobre simulación, añade `incidencias_dgt`, `resumen_dgt`, `source`, `severity`, `peso_pagerank`, reconstruye `clima_hubs` alternativo si falta clima API y deja provenance `dgt_merged`. |
| 8 | **PublishKafka** | `Kafka_Topic_DGT_RAW` | Publica evidencia específica de la fuente DGT (`transporte_dgt_raw`). |
| 9 | **PublishKafka** | `Kafka_Topic_RAW` | Publica el snapshot completo en **`transporte_raw`**. |
| 10 | **PublishKafka** | `Kafka_Topic_Filtered` | Publica el snapshot enriquecido en **`transporte_filtered`**. |
| 11 | **PutHDFS** | `HDFS_Backup_JSON` | Escribe el JSON final en **`HDFS_BACKUP_PATH`**. |
| 12 | *(Opcional)* **ExecuteProcess** | `Spark_Submit_Procesamiento` | Lanza `spark-submit` sobre el snapshot ya enriquecido. |

### Relaciones (conexiones)

```
[1 GenerateFlowFile]  --success-->  [2 UpdateAttribute]
[2 UpdateAttribute]   --success-->  [3 ExecuteScript]

[3 ExecuteScript] --success--> [4 InvokeHTTP OpenMeteo]
[4 InvokeHTTP OpenMeteo] --Original--> [5 MergeWeather]
[5 MergeWeather] --success--> [6 InvokeHTTP DGT]
[6 InvokeHTTP DGT] --Original--> [7 MergeDGT]

[7 MergeDGT] --success--+--> [8 PublishKafka DGT_RAW]
                        +--> [9 PublishKafka RAW]
                        +--> [10 PublishKafka FILTERED]
                        +--> [11 PutHDFS]

[3/5/7 ExecuteScript] --failure-->  (LogAttribute / PutFile DLQ / notificación)

[10 PublishKafka FILTERED] --success-->  [12 ExecuteProcess Spark]   (opcional)
```

**Notas:**

- En NiFi, **una misma relación `success` puede bifurcar a varios procesadores** sin `DuplicateFlowFile`: el snapshot final sale desde [7] a Kafka/HDFS.
- **Kafka** y **HDFS** pueden ir en paralelo; si uno falla, configura **retry** en el procesador o **funnel** + **PutFile** para no perder el FlowFile.
- **Spark (7)** no es obligatorio si ya usas **Airflow** (`dag_maestro`) o el dashboard para lanzar procesamiento.
- Los atributos `simlog.provenance.sources`, `simlog.provenance.stage`, `simlog.provenance.dgt_mode`, `simlog.provenance.dgt_incidents` y `simlog.provenance.dgt_nodes_affected` permiten verificar en `Data Provenance` qué parte del payload procede de simulación, Open-Meteo (u OpenWeather legacy) o DGT.
- Configuración efectiva del fallback: `Set_Parametros_Ingesta` inyecta `dgt.url`; `Merge_Weather_Into_Payload.groovy` no bloquea la tubería ante error HTTP y `Merge_DGT_Into_Payload.groovy` rellena `clima_hubs` desde DATEX2 cuando no hay clima utilizable.

---

## 4. Por qué cada tipo de procesador (criterio técnico)

| Tipo | Criterio |
|------|----------|
| **GenerateFlowFile** | Patrón estándar NiFi para “reloj”; no requiere cola externa. |
| **UpdateAttribute** | Evita embebidos secretos en Groovy; integración con **Parameter Providers** / variables de entorno. |
| **ExecuteScript (Groovy)** | Flexibilidad total para JSON + merge de fuentes (`source`, `severity`, `peso_pagerank`) manteniendo el contrato del pipeline Python. |
| **PublishKafka** (×3) | Cumplimiento del modelo **dgt_raw + raw + filtered** y trazabilidad por fuente. |
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
- `TOPIC_DGT_RAW`, `DGT_DATEX2_URL`
- `HDFS_BACKUP_PATH` (o `HDFS_RAW_BASE` para subcarpeta `nifi_raw/`)
- (Opcional legacy OpenWeather) `OWM_API_KEY` → flujo antiguo con Group API; por defecto el proyecto usa Open-Meteo sin clave.

---

## 8. Relación con el otro proyecto (Sentinel360 / práctica)

| Concepto | Sentinel360 / PDF | SIMLOG NiFi |
|----------|-------------------|-------------|
| Datos crudos vs filtrados | Dos temas | `transporte_dgt_raw` + `transporte_raw` + `transporte_filtered` |
| Almacén distribuido | HDFS | `PutHDFS` |
| Cola | Kafka | Kafka |
| Procesamiento pesado | Spark | Fuera de NiFi (batch) o `ExecuteProcess` |
| Tiempo casi real | Cassandra | Spark → Cassandra (igual que Python) |

---

## 9. Archivos del repositorio

| Archivo | Contenido |
|---------|-----------|
| `groovy/GenerateSyntheticGpsForPractice.groovy` | Snapshot base sintético con provenance inicial. |
| `groovy/MergeOpenWeatherIntoPayload.groovy` | Añade clima y actualiza provenance. |
| `groovy/MergeDgtDatex2IntoPayload.groovy` | Añade incidencias DATEX2 DGT con prioridad y provenance final. |
| `flow/simlog_kdd_flow_spec.yaml` | Especificación mecánica (v1 granular + v2 compacta). |
| `README_NIFI.md` | Visión general y seguridad. |

Con esto, **a vista del proyecto** queda definido el **grupo de procesadores**, los **tipos** elegidos y las **relaciones** para que la **ingesta sea funcional** y coherente con el stack Python que ya usáis.
