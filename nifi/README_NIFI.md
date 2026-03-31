# Apache NiFi — Ingesta alternativa (SIMLOG España)

> **Documentación detallada (process group, procesadores, relaciones y criterio):**  
> **[`PROCESADORES_Y_RELACIONES.md`](PROCESADORES_Y_RELACIONES.md)** — incluye el **flujo compacto recomendado (v2)** y el **flujo granular (v1)**.

Este directorio describe el **grupo de procesadores** y el flujo de datos alineado con el PDF / práctica: **GPS sintético**, **Open-Meteo (InvokeHTTP; sin API key)**, **DGT DATEX2 (InvokeHTTP)**, **Kafka**, **HDFS (cliente Hadoop; cluster con YARN)**, **Spark** (job batch vía `spark-submit`), **Cassandra**, **Hive** (histórico analítico).

> **Nota:** NiFi no sustituye a Spark en memoria: orquesta **ingesta y disparo de jobs**. El procesamiento masivo (GraphFrames) sigue siendo `procesamiento/procesamiento_grafos.py` o un JAR equivalente.

## Ejecucion local en Windows (sin Docker)

Guia y scripts listos en:

- `nifi/local/README_LOCAL_WINDOWS.md`
- `nifi/local/install_nifi_local_windows.ps1`
- `nifi/local/start_nifi_local_windows.ps1`
- `nifi/local/stop_nifi_local_windows.ps1`

## Resumen de arquitectura (grupo completo usado en el proyecto)

```text
                    ┌─────────────────────────────────────────────────────────┐
                    │  Process Group: PG_SIMLOG_KDD                             │
                    └─────────────────────────────────────────────────────────┘
 [Timer 15 min]
      │
      ▼
[ExecuteScript Groovy]
(GPS sintético)
      │
      ▼
[InvokeHTTP Open-Meteo]
      │
      ▼
[ExecuteScript MergeWeather]
      │
      ▼
[InvokeHTTP DGT DATEX2]
      │
      ▼
[ExecuteScript MergeDGT]
      │
      ├──► [PublishKafka transporte_dgt_raw]
      ├──► [PublishKafka transporte_raw]
      └──► [PublishKafka transporte_filtered]
                 │
        ┌────────┴──────────┐
        ▼                   ▼
   [PutHDFS]       [ExecuteProcess spark-submit]
                            │
                            ▼
                  Cassandra + Hive (vía Spark)
```

## Parámetros del Parameter Context (recomendado)

Definir en **NiFi → Parameter Contexts** (o Variables del grupo):

| Parámetro | Ejemplo | Uso |
|-----------|---------|-----|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | PublishKafka |
| `TOPIC_DGT_RAW` | `transporte_dgt_raw` | Auditoría XML/merge DGT |
| `TOPIC_RAW` | `transporte_raw` | Datos crudos (auditoría) |
| `TOPIC_FILTERED` | `transporte_filtered` | Consumo Spark / pipeline |
| `SIMLOG_OPEN_METEO_MULTI_HUBS_URL` | *(opcional)* URL completa multi-hub; por defecto coincide con `OPEN_METEO_MULTI_HUBS_URL` en `config.py` | InvokeHTTP clima |
| `DGT_DATEX2_URL` | `https://nap.dgt.es/.../datex2_v36.xml` | InvokeHTTP DGT |
| `HDFS_RAW_BASE` | `/user/hadoop/transporte_backup/nifi_raw` | PutHDFS |
| `CASSANDRA_HOST` | `127.0.0.1` | ExecuteScript / Spark |
| `HIVE_JDBC_URL` | `jdbc:hive2://localhost:10000/logistica_analytics` | PutHiveQL |
| `SPARK_SUBMIT` | `/opt/spark/bin/spark-submit` | ExecuteProcess |
| `SPARK_MASTER` | `yarn` | Despliegue cluster |

Los valores por defecto del proyecto coinciden con `config.py`.

## Grupo de procesadores: `PG_SIMLOG_KDD`

Orden lógico y tipos de procesador **Apache NiFi 1.x / 2.x** (los nombres exactos pueden variar por versión).

> Montaje operativo paso a paso: `flow/MONTAJE_UI_NIFI.md`  
> Especificación declarativa: `flow/simlog_kdd_flow_spec.yaml`

### 1. Orquestación y datos sintéticos (GPS)

| # | Tipo de procesador | Rol KDD / PDF |
|---|-------------------|---------------|
| 1 | **GenerateFlowFile** | Disparo periódico (p. ej. cada 15 min) — *Selección / simulación* |
| 2 | **ExecuteScript** (Groovy) | Genera **JSON** con posiciones GPS sintéticas, ids de camión, `paso_15min` y atributos `simlog.provenance.*` — *Preprocesamiento* |
| | *Alternativa* | **ReplaceText** + plantilla en `assets/payload_template.json` (menos flexible) |

Script de referencia: `groovy/GenerateSyntheticPayload.groovy`.

### 2. Clima real (Open-Meteo)

| # | Tipo | Rol |
|---|------|-----|
| 3 | **InvokeHTTP** (`OpenMeteo_InvokeHTTP`) | `GET` → JSON **array** (una petición multi-hub). URL por defecto: misma cadena que `OPEN_METEO_MULTI_HUBS_URL` en `config.py` (5 hubs: Madrid … Sevilla). Cuerpo de respuesta en atributo `owm.response` (nombre histórico). |
| 4 | **ExecuteScript** | `groovy/MergeOpenWeatherIntoPayload.groovy` interpreta el array Open-Meteo y rellena `clima_hubs` con `source=openmeteo`. |

**OpenWeather (legacy):** el mismo script acepta respuestas **Group API** (`list[]`) si aún usas el flujo antiguo.

### 2.b DATEX2 DGT real

| # | Tipo | Rol |
|---|------|-----|
| 6 | **InvokeHTTP** | Descarga XML DATEX2 de la DGT y lo deja en atributo `dgt.response.xml` |
| 7 | **ExecuteScript** (Groovy) | Fusiona incidencias DGT con el payload, prioriza `source=dgt` sobre simulación y actualiza `severity` / `peso_pagerank` |

Este paso deja trazabilidad adicional en atributos:

- `simlog.provenance.stage=dgt_merged`
- `simlog.provenance.sources=simulacion,openmeteo,dgt` (o `openweather` si el merge vino de Group API legacy)
- `simlog.provenance.dgt_mode=live|disabled`
- `simlog.provenance.dgt_incidents=<n>`
- `simlog.provenance.dgt_nodes_affected=<n>`

### 3. Kafka (pub/sub)

| # | Tipo | Configuración clave |
|---|------|---------------------|
| 8 | **PublishKafka** | `Topic Name` = `${TOPIC_DGT_RAW}` para auditoría específica de la señal DGT |
| 9 | **PublishKafka** | `Topic Name` = `${TOPIC_RAW}` (snapshot enriquecido completo) |
| 10 | **PublishKafka** | `Topic Name` = `${TOPIC_FILTERED}` para consumidores del pipeline |

### 4. HDFS — datos crudos (cluster YARN)

| # | Tipo | Notas |
|---|------|--------|
| 11 | **PutHDFS** | `Directory` = `${HDFS_RAW_BASE}/${now():format('yyyy/MM/dd')}`; **Hadoop Configuration Resources** = `file:///etc/hadoop/conf/core-site.xml,file:///etc/hadoop/conf/hdfs-site.xml` |

**Importante:** YARN es el **manager de recursos** del cluster; **PutHDFS** usa el **cliente HDFS** (no “invoca YARN” directamente). El PDF suele pedir “datos en HDFS en un cluster YARN”: basta con que `hdfs-site.xml` apunte al NameNode del cluster gestionado por YARN.

### 5. Spark (batch + conector Cassandra / Hive)

NiFi no ejecuta Spark en línea; se usa **ExecuteProcess** o **ExecuteStreamCommand**:

| # | Tipo | Comando típico |
|---|------|----------------|
| 12 | **ExecuteProcess** | `bash` argumentos: `scripts/spark_submit_yarn.sh` (o `spark-submit` directo) |

El script debe lanzar:

- `--master yarn --deploy-mode cluster` (o `client` en entornos de práctica)
- Job PySpark: `procesamiento/procesamiento_grafos.py` **o** JAR con misma lógica.

Así, **después de Kafka/HDFS**, Spark escribe en **Cassandra** y **Hive** como ya define el proyecto.

**Logs y NiFi:** `nifi/scripts/spark_submit_yarn.sh` escribe **toda** la salida de `spark-submit` en `reports/nifi_spark/spark_submit_<fecha>_<pid>.log` (o en `SIMLOG_NIFI_SPARK_LOG_DIR` si lo defines). Así **stderr** del proceso solo muestra algo si el job **falla** (mensaje corto + últimas líneas del log), y la UI de NiFi deja de marcar líneas `INFO` de Spark como *warnings*.

### 6. Cassandra

| Opción | Descripción |
|--------|-------------|
| **A (recomendada)** | Solo **Spark** escribe en Cassandra (mismo esquema `cassandra/esquema_logistica.cql`). NiFi no inserta fila a fila. |
| **B** | **ExecuteScript** (Groovy/Java) con driver Datastax para inserts puntuales (no recomendado a gran escala). |
| **C** | `ExecuteProcess` → `cqlsh -f` con fichero generado (frágil). |

### 7. Hive — consultas históricas

Tablas definidas en `setup_hive.hql` (`logistica_analytics`):

- `clima_hist` (ORC, partición `dia`)
- `red_transporte_hist` (Parquet, partición `fecha`)
- `metricas_nodos_hist` (Parquet)

| # | Tipo | Uso |
|---|------|-----|
| 10 | **PutHiveQL** o **PutHive3** | `INSERT INTO logistica_analytics.clima_hist ...` si extraes campos con **EvaluateJsonPath** |
| | *Alternativa* | Spark escribe particiones en HDFS y **MSCK REPAIR** / particiones dinámicas |

En nodos con poca RAM, **priorizar** que Spark inserte en Hive en el mismo job que Cassandra.

## Relación con el script Python

| Componente | Script | NiFi |
|------------|--------|------|
| Ingesta | `ingesta/ingesta_kdd.py` | Flujo anterior (sustituye o complementa) |
| Procesamiento | `procesamiento/procesamiento_grafos.py` | Lanzado por `ExecuteProcess` / Airflow |

## Archivos de apoyo en este directorio

| Archivo | Descripción |
|---------|-------------|
| `flow/simlog_kdd_flow_spec.yaml` | Especificación legible de procesadores y conexiones |
| `flow/FLUJO_MINIMO_CLIMA.md` | Montaje mínimo operativo (Open-Meteo → Kafka + HDFS) |
| `groovy/GenerateSyntheticPayload.groovy` | GPS sintético + envelope JSON |
| `groovy/MergeDgtDatex2IntoPayload.groovy` | Fusión DATEX2 DGT con prioridad y provenance |
| `scripts/spark_submit_yarn.sh` | Ejemplo `spark-submit` con YARN |
| `assets/payload_template.json` | Plantilla JSON de referencia |
| `parameter-context.env.example` | Variables para Parameter Context |

## Importación en NiFi

1. Instalar NiFi en el mismo host o en uno con conectividad a Kafka/HDFS/Hive.
2. Copiar **Parameter Context** desde `parameter-context.env.example`.
3. Crear el **Process Group** vacío `PG_SIMLOG_KDD`.
4. Arrastrar procesadores según `flow/simlog_kdd_flow_spec.yaml` y esta guía.
5. Configurar **Controller Services** necesarios:
   - **JsonTreeReader** / **JsonRecordSetWriter** (si usas MergeRecord)
   - **SSLContextService** (si Kafka/HDFS con TLS)
   - **HiveConnectionPool** (PutHiveQL)

## Actualizar `Merge_DGT_Into_Payload` desde el repositorio

NiFi **no** lee el repositorio Git por sí solo: los cambios en `nifi/groovy/MergeDgtDatex2IntoPayload.groovy` hay que **reflejarlos en el procesador** del canvas (o en la ruta de fichero que use el nodo).

1. En el servidor donde está el código, actualizar el repo (`git pull` o copia desplegada) para que el fichero `nifi/groovy/MergeDgtDatex2IntoPayload.groovy` sea la versión deseada.
2. En la UI de NiFi, abrir el grupo `PG_SIMLOG_KDD` y el procesador **`Merge_DGT_Into_Payload`** (tipo **ExecuteScript**, motor **Groovy**).
3. Elegir **una** de estas dos formas de enlazar el script (no hace falta usar ambas):
   - **Script File:** en **Properties**, `Script File` = ruta **absoluta** al `.groovy` en ese host (debe ser el mismo fichero que en el repo; la especificación declarativa usa `${PROJECT_ROOT}/nifi/groovy/MergeDgtDatex2IntoPayload.groovy`, ver `flow/simlog_kdd_flow_spec.yaml`). Tras cambiar el fichero en disco, según la versión de NiFi puede bastar con **Apply** o con detener y arrancar el procesador para que cargue el contenido nuevo.
   - **Script Body:** dejar `Script File` vacío y pegar en la pestaña del script **todo** el contenido del fichero `MergeDgtDatex2IntoPayload.groovy` del proyecto.
4. **Apply** / guardar la configuración.
5. Si había FlowFiles atascados o en **failure**, vaciar las colas de las conexiones de salida de ese procesador y, si aplica, **Stop** → **Start** en el procesador antes de volver a disparar el flujo.

Tras actualizar, la relación **success** no debería mostrar el bulletin por comparación incorrecta entre listas en el fallback de clima DGT (error del tipo `Cannot compare java.util.ArrayList ...` en versiones anteriores del script).

**Comprobación en el repo:** `pytest tests/test_merge_dgt_nifi_groovy.py` (evita regresiones antes de volver a pegar el script en NiFi).

## Provenance y relaciones

Para comprobar el linaje en la UI de NiFi:

1. Abrir el Process Group `PG_SIMLOG_KDD`.
2. Revisar `Data Provenance` filtrando por `componentName`:
   - `Build_GPS_Sintetico`
   - `Merge_Weather_Into_Payload`
   - `Merge_DGT_Into_Payload`
   - `Kafka_Publish_FILTERED`
   - `HDFS_Backup_JSON`
3. Filtrar por atributos `simlog.provenance.*` para distinguir si un snapshot vino solo de simulación o quedó enriquecido con DGT.

Relaciones clave del flujo final:

- `Build_GPS_Sintetico.success -> OpenMeteo_InvokeHTTP`
- `OpenMeteo_InvokeHTTP.Original -> Merge_Weather_Into_Payload`
- `Merge_Weather_Into_Payload.success -> DGT_DATEX2_InvokeHTTP`
- `DGT_DATEX2_InvokeHTTP.Original -> Merge_DGT_Into_Payload`
- `Merge_DGT_Into_Payload.success -> Kafka_Publish_DGT_RAW | Kafka_Publish_RAW | Kafka_Publish_FILTERED | HDFS_Backup_JSON`
- `Kafka_Publish_FILTERED.success -> Spark_Submit_Procesamiento`

## Incidencias frecuentes

### Open-Meteo: fallos HTTP o `simlog.weather.available=false`

- Comprueba conectividad HTTPS a `api.open-meteo.com` y que la URL tenga **cinco** pares lat/lon en el mismo orden que el script (`MergeOpenWeatherIntoPayload.groovy`).
- Respuesta esperada: **array JSON** de longitud 5. Si cambias coordenadas, actualiza también el script o la variable `SIMLOG_OPEN_METEO_MULTI_HUBS_URL`.
- **OpenWeather (solo si vuelves a `SIMLOG_WEATHER_PROVIDER=openweather` en Python):** `401` indica `appid` inválido; revisa `OWM_API_KEY` en Parameter Context.

### Merge DGT: `Cannot compare java.util.ArrayList`

- Sustituir el script del procesador **`Merge_DGT_Into_Payload`** por la versión del repo (`nifi/groovy/MergeDgtDatex2IntoPayload.groovy`). Ver sección [Actualizar `Merge_DGT_Into_Payload` desde el repositorio](#actualizar-merge_dgt_into_payload-desde-el-repositorio).

## Seguridad

- Si usas OpenWeather en algún entorno legacy, no commitear `OWM_API_KEY` en texto plano; usar **Parameter Providers** o **NiFi sensitive parameters**.
