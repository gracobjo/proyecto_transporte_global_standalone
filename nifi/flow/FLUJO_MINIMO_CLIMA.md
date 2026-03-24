# Flujo minimo NiFi para API clima (OpenWeather)

Objetivo: tener una ingesta funcional en NiFi que consulte clima real (5 hubs), publique en Kafka y guarde backup en HDFS.

Este flujo usa el script ya incluido en el repo: `nifi/groovy/GenerateFullPayloadSimlog.groovy`.

## Import rapido en canvas (JSON)

Se incluye un blueprint completo en:

- `nifi/flow/simlog_kdd_canvas_import.json`

Incluye procesadores, conexiones, Parameter Context, validacion y persistencia `raw/curated`.
Si tu version de NiFi no soporta importacion JSON directa en la UI, usa ese archivo como especificacion exacta para crear el Process Group manualmente (mismos IDs/nombres/relaciones).

## 1) Parameter Context (obligatorio)

En el Process Group `PG_SIMLOG_KDD`, crea/asigna estos parametros:

- `OWM_API_KEY` = tu clave OpenWeather (marcar como sensible)
- `KAFKA_BOOTSTRAP` = `localhost:9092`
- `TOPIC_RAW` = `transporte_raw`
- `TOPIC_FILTERED` = `transporte_filtered`
- `HDFS_BACKUP_PATH` = `/user/hadoop/transporte_backup`
- `HDFS_RAW_BASE` = `/user/hadoop/transporte_backup/nifi_raw`
- `HDFS_CURATED_BASE` = `/user/hadoop/transporte_backup/nifi_curated`
- `HADOOP_CONF_RESOURCES` = `/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml`
- `SPARK_SUBMIT` = `/opt/spark/bin/spark-submit`
- `SPARK_MASTER` = `yarn`
- `DEPLOY_MODE` = `client`
- `PROJECT_ROOT` = `/home/hadoop/proyecto_transporte_global`
- `CASSANDRA_HOST` = `127.0.0.1`

## 2) Procesadores (en este orden)

1. `GenerateFlowFile` (`Timer_Ingesta_15min`)
2. `UpdateAttribute` (`Set_Parametros_Ingesta`)
3. `ExecuteScript` (`Build_JSON_Ingesta_KDD`)
4. `PublishKafka` (`Kafka_Topic_RAW`)
5. `PublishKafka` (`Kafka_Topic_Filtered`)
6. `PutHDFS` (`HDFS_Raw_Backup`)
7. `PublishKafka` (`Kafka_Topic_Filtered`)
8. `PutHDFS` (`HDFS_Curated_Validated`)
9. `ExecuteProcess` (`Spark_Submit_Procesamiento`)
10. `LogAttribute` (`DLQ_Log`, recomendado para errores)
11. `PutFile` (`DLQ_Persist`, recomendado)

## 3) Configuracion clave de cada procesador

### 3.1 `GenerateFlowFile`

- `Scheduling Strategy`: `Timer driven`
- `Run Schedule`: `15 min` (para pruebas: `30 sec`)
- `Batch Size`: `1`
- `Data Format`: `Text`

### 3.2 `UpdateAttribute`

Agregar atributos:

- `owm.api.key` = `${OWM_API_KEY}`
- `paso_15min` = `0`

### 3.3 `ExecuteScript`

- `Script Engine`: `Groovy`
- `Script Body`: pegar el contenido de `nifi/groovy/GenerateFullPayloadSimlog.groovy`

Resultado esperado: FlowFile JSON con atributos:

- `mime.type=application/json`
- `filename=simlog_nifi_<timestamp>.json`

### 3.4 `ValidateRecord` (recomendado)

- `Record Reader`: `JsonTreeReader` (Controller Service)
- `Record Writer`: `JsonRecordSetWriter` (Controller Service)
- Ramas:
  - `valid` -> Kafka/HDFS
  - `invalid` y `failure` -> DLQ
### 3.5 `PublishKafka` (RAW)

- `Kafka Brokers` (o `Bootstrap Servers`, segun version): `${KAFKA_BOOTSTRAP}`
- `Topic Name`: `${TOPIC_RAW}`
- `Delivery Guarantee`: `Guarantee Replicated Delivery` (si aparece)

### 3.6 `PublishKafka` (FILTERED)

- Mismos brokers
- `Topic Name`: `${TOPIC_FILTERED}`

### 3.7 `PutHDFS` (RAW y CURATED)

- `Directory` RAW: `${HDFS_RAW_BASE}/${now():format('yyyy/MM/dd/HH')}`
- `Directory` CURATED: `${HDFS_CURATED_BASE}/${now():format('yyyy/MM/dd/HH')}`
- `Conflict Resolution Strategy`: `replace`
- `Hadoop Configuration Resources`: `${HADOOP_CONF_RESOURCES}`

### 3.8 `ExecuteProcess` (Spark -> Cassandra + Hive)

- `Command`: `bash`
- `Command Arguments`: `-lc "${PROJECT_ROOT}/nifi/scripts/spark_submit_yarn.sh"`

Este paso ejecuta `procesamiento/procesamiento_grafos.py`, que persiste en:

- Cassandra (`nodos_estado`, `aristas_estado`, `tracking_camiones`, `pagerank_nodos`)
- Hive (historico, si metastore disponible)

## 4) Conexiones (relationships)

- `GenerateFlowFile.success -> UpdateAttribute`
- `UpdateAttribute.success -> ExecuteScript`
- `ExecuteScript.success -> ValidateRecord`
- `ExecuteScript.failure -> DLQ_Log`
- `ValidateRecord.valid -> PublishKafka (RAW)`
- `ValidateRecord.valid -> PublishKafka (FILTERED)`
- `ValidateRecord.valid -> PutHDFS (RAW)`
- `ValidateRecord.valid -> PutHDFS (CURATED)`
- `ValidateRecord.invalid -> DLQ_Log`
- `ValidateRecord.failure -> DLQ_Log`
- `PublishKafka (RAW).failure -> DLQ_Log` (opcional)
- `PublishKafka (FILTERED).failure -> DLQ_Log` (opcional)
- `PutHDFS (RAW).failure -> DLQ_Log`
- `PutHDFS (CURATED).failure -> DLQ_Log`
- `PutHDFS (RAW).success -> ExecuteProcess (Spark)`
- `PutHDFS (CURATED).success -> ExecuteProcess (Spark)`
- `DLQ_Log.success -> DLQ_Persist.success`

## 5) Prueba rapida (smoke test)

1. Poner `GenerateFlowFile` a `30 sec`.
2. Arrancar primero: `DLQ_Log`, `PublishKafka`, `PutHDFS`.
3. Arrancar `ExecuteScript`, `UpdateAttribute`, `GenerateFlowFile`.
4. Verificar en NiFi:
   - No hay colas creciendo en `failure`.
   - `Provenance` muestra envios a Kafka y escritura HDFS.
5. Verificar fuera de NiFi:
   - Kafka topics `transporte_raw` y `transporte_filtered` reciben mensajes.
   - En HDFS aparecen JSON en `nifi_raw/yyyy/MM/dd/HH` y `nifi_curated/yyyy/MM/dd/HH`.
   - Tras Spark, hay datos en Cassandra y (si aplica) tablas Hive actualizadas.

## 6) Notas practicas

- Si `ExecuteScript` falla con `missing owm.api.key`, revisa `UpdateAttribute` y `Parameter Context`.
- Si OpenWeather rate limit falla, aumenta el `Run Schedule`.
- Para produccion, mantener `OWM_API_KEY` solo en Parameter Context sensible (no en ficheros de repo).
