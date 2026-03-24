# Apache NiFi — Ingesta alternativa (SIMLOG España)

> **Documentación detallada (process group, procesadores, relaciones y criterio):**  
> **[`PROCESADORES_Y_RELACIONES.md`](PROCESADORES_Y_RELACIONES.md)** — incluye el **flujo compacto recomendado (v2)** y el **flujo granular (v1)**.

Este directorio describe el **grupo de procesadores** y el flujo de datos alineado con el PDF / práctica: **GPS sintético**, **OpenWeather (InvokeHTTP)**, **Kafka**, **HDFS (cliente Hadoop; cluster con YARN)**, **Spark** (job batch vía `spark-submit`), **Cassandra**, **Hive** (histórico analítico).

> **Nota:** NiFi no sustituye a Spark en memoria: orquesta **ingesta y disparo de jobs**. El procesamiento masivo (GraphFrames) sigue siendo `procesamiento/procesamiento_grafos.py` o un JAR equivalente.

## Ejecucion local en Windows (sin Docker)

Guia y scripts listos en:

- `nifi/local/README_LOCAL_WINDOWS.md`
- `nifi/local/install_nifi_local_windows.ps1`
- `nifi/local/start_nifi_local_windows.ps1`
- `nifi/local/stop_nifi_local_windows.ps1`

## Resumen de arquitectura

```text
                    ┌─────────────────────────────────────────────────────────┐
                    │  Process Group: PG_SIMLOG_KDD                             │
                    └─────────────────────────────────────────────────────────┘
 [Timer] ──► [GPS sintético] ──┬──► [InvokeHTTP clima] ──► [Merge] ──► [JSON final]
                               │              ▲
                               └──────────────┘
                    │                    │
                    ▼                    ▼
              [PublishKafka]      [PutHDFS raw]
              (transporte_raw)  (/user/hadoop/.../nifi_raw/)
                    │                    │
                    └────────┬───────────┘
                             ▼
                    [ExecuteProcess]  spark-submit --master yarn ...
                             │
                             ▼
                    Cassandra + Hive (vía Spark / PutHiveQL opcional)
```

## Parámetros del Parameter Context (recomendado)

Definir en **NiFi → Parameter Contexts** (o Variables del grupo):

| Parámetro | Ejemplo | Uso |
|-----------|---------|-----|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | PublishKafka |
| `TOPIC_RAW` | `transporte_raw` | Datos crudos (auditoría) |
| `TOPIC_FILTERED` | `transporte_filtered` | Consumo Spark / pipeline |
| `OWM_API_KEY` | *(tu clave)* | InvokeHTTP |
| `HDFS_RAW_BASE` | `/user/hadoop/transporte_backup/nifi_raw` | PutHDFS |
| `CASSANDRA_HOST` | `127.0.0.1` | ExecuteScript / Spark |
| `HIVE_JDBC_URL` | `jdbc:hive2://localhost:10000/logistica_analytics` | PutHiveQL |
| `SPARK_SUBMIT` | `/opt/spark/bin/spark-submit` | ExecuteProcess |
| `SPARK_MASTER` | `yarn` | Despliegue cluster |

Los valores por defecto del proyecto coinciden con `config.py`.

## Grupo de procesadores: `PG_SIMLOG_KDD`

Orden lógico y tipos de procesador **Apache NiFi 1.x / 2.x** (nombres pueden variar según versión; buscar en el palette el equivalente `PublishKafka_2_6`, etc.).

### 1. Orquestación y datos sintéticos (GPS)

| # | Tipo de procesador | Rol KDD / PDF |
|---|-------------------|---------------|
| 1 | **GenerateFlowFile** | Disparo periódico (p. ej. cada 15 min) — *Selección / simulación* |
| 2 | **ExecuteScript** (Groovy) | Genera **JSON** con posiciones GPS sintéticas, ids de camión, `paso_15min` — *Preprocesamiento* |
| | *Alternativa* | **ReplaceText** + plantilla en `assets/payload_template.json` (menos flexible) |

Script de referencia: `groovy/GenerateSyntheticPayload.groovy`.

### 2. Clima real (OpenWeather)

| # | Tipo | Rol |
|---|------|-----|
| 3 | **UpdateAttribute** | Atributos: `lat`, `lon`, `hub`, `owm_url` = `https://api.openweathermap.org/data/2.5/weather?lat=${lat}&lon=${lon}&appid=${OWM_API_KEY}&units=metric` |
| 4 | **InvokeHTTP** | `GET` → cuerpo JSON de OpenWeather (una llamada por FlowFile; duplicar rama o usar **RouteOnAttribute** + 5 flujos para los 5 hubs) |
| 5 | **MergeRecord** o **MergeContent** | Unir respuesta HTTP con el JSON sintético (binario `plain`, `Demarcator: \n---\n`) |

Para **5 hubs en paralelo**, crear **5 ramas** (Madrid, Barcelona, …) con `UpdateAttribute` fijo + `InvokeHTTP`, y **MergeContent** con `Minimum Number of Entries = 5`.

### 3. Kafka (pub/sub)

| # | Tipo | Configuración clave |
|---|------|---------------------|
| 6 | **PublishKafka** / **PublishKafka**_* | `Bootstrap Servers` = `${KAFKA_BOOTSTRAP`, `Topic Name` = `${TOPIC_RAW}` (crudo) |
| 7 | *(Opcional)* **PublishKafka** | Segundo topic `${TOPIC_FILTERED}` tras **ValidateRecord** / **JoltTransform** para “datos filtrados” (como en `config.py`) |

### 4. HDFS — datos crudos (cluster YARN)

| # | Tipo | Notas |
|---|------|--------|
| 8 | **PutHDFS** | `Directory` = `${HDFS_RAW_BASE}/${now():format('yyyy/MM/dd')}`; **Hadoop Configuration Resources** = `file:///etc/hadoop/conf/core-site.xml,file:///etc/hadoop/conf/hdfs-site.xml` |

**Importante:** YARN es el **manager de recursos** del cluster; **PutHDFS** usa el **cliente HDFS** (no “invoca YARN” directamente). El PDF suele pedir “datos en HDFS en un cluster YARN”: basta con que `hdfs-site.xml` apunte al NameNode del cluster gestionado por YARN.

### 5. Spark (batch + conector Cassandra / Hive)

NiFi no ejecuta Spark en línea; se usa **ExecuteProcess** o **ExecuteStreamCommand**:

| # | Tipo | Comando típico |
|---|------|----------------|
| 9 | **ExecuteProcess** | `bash` argumentos: `scripts/spark_submit_yarn.sh` (o `spark-submit` directo) |

El script debe lanzar:

- `--master yarn --deploy-mode cluster` (o `client` en entornos de práctica)
- Job PySpark: `procesamiento/procesamiento_grafos.py` **o** JAR con misma lógica.

Así, **después de Kafka/HDFS**, Spark escribe en **Cassandra** y **Hive** como ya define el proyecto.

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
| `flow/FLUJO_MINIMO_CLIMA.md` | Montaje mínimo operativo (OpenWeather -> Kafka + HDFS) |
| `groovy/GenerateSyntheticPayload.groovy` | GPS sintético + envelope JSON |
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

## Seguridad

- No commitear `OWM_API_KEY` en texto plano; usar **Parameter Providers** o **NiFi sensitive parameters**.
