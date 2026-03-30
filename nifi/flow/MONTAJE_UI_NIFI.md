# Montaje completo del Process Group en la UI de NiFi

Este montaje implementa lo que usa SIMLOG:

- Ingesta cada 15 min (GPS + clima Open-Meteo).
- Publicación en Kafka (`raw` + `filtered`).
- Persistencia en HDFS desde Kafka.
- Trigger de Spark para persistir en Cassandra + Hive.

Referencia principal: `nifi/flow/simlog_kdd_flow_spec.yaml` (v3.0).

## 1) Crear el grupo y parámetros

1. Crear Process Group: `PG_SIMLOG_KDD`.
2. Crear/importar Parameter Context con `nifi/parameter-context.env.example`.
3. Verificar al menos:
   - `KAFKA_BOOTSTRAP`, `TOPIC_RAW`, `TOPIC_FILTERED`
   - `HDFS_BACKUP_PATH`
   - `PROJECT_ROOT`, `SPARK_HOME`, `SPARK_MASTER`, `DEPLOY_MODE`

## 2) Añadir procesadores (en este orden)

### Ingesta

1. `GenerateFlowFile` → `Timer_Ingesta_15min`
   - Scheduling: `15 min`
2. `UpdateAttribute` → `Set_Parametros_Ingesta`
   - `simlog.interval.min = 15`
3. `ExecuteScript` → `Build_JSON_Ingesta_KDD`
   - Engine: `Groovy`
   - Script file: `${PROJECT_ROOT}/nifi/groovy/GenerateFullPayloadSimlog.groovy`
4. `PublishKafka` → `Kafka_Publish_RAW`
   - Brokers: `${KAFKA_BOOTSTRAP}`
   - Topic: `${TOPIC_RAW}`
5. `PublishKafka` → `Kafka_Publish_FILTERED`
   - Brokers: `${KAFKA_BOOTSTRAP}`
   - Topic: `${TOPIC_FILTERED}`

### Persistencia HDFS desde Kafka

6. `ConsumeKafka` → `Kafka_Consume_Filtered_for_HDFS`
   - Topic: `${TOPIC_FILTERED}`
   - Group ID: `nifi_hdfs_writer`
   - Scheduling: `30 sec`
7. `PutHDFS` → `HDFS_Backup_JSON`
   - Resources: `/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml`
   - Directory: `${HDFS_BACKUP_PATH}/${now():format('yyyy/MM/dd')}`

### Trigger Spark (Cassandra + Hive)

8. `ConsumeKafka` → `Kafka_Consume_Filtered_for_Spark`
   - Topic: `${TOPIC_FILTERED}`
   - Group ID: `nifi_spark_trigger`
   - Scheduling: `30 sec`
9. `ExecuteProcess` → `Spark_Submit_Procesamiento`
   - Command Path: `/bin/bash`
   - Arguments: `${PROJECT_ROOT}/nifi/scripts/spark_submit_yarn.sh`

### Errores / DLQ

10. `LogAttribute` → `Log_Fallos`
11. `PutFile` → `DLQ_Local`
   - Directory: `${PROJECT_ROOT}/logs/nifi_dlq`

## 3) Conexiones (relationships)

- `Timer_Ingesta_15min.success -> Set_Parametros_Ingesta`
- `Set_Parametros_Ingesta.success -> Build_JSON_Ingesta_KDD`
- `Build_JSON_Ingesta_KDD.success -> Kafka_Publish_RAW`
- `Build_JSON_Ingesta_KDD.success -> Kafka_Publish_FILTERED`
- `Kafka_Consume_Filtered_for_HDFS.success -> HDFS_Backup_JSON`
- `Kafka_Consume_Filtered_for_Spark.success -> Spark_Submit_Procesamiento`

Errores (`failure`) de los procesadores de negocio → `Log_Fallos` → `DLQ_Local`.

## 4) Orden de arranque recomendado

1. Arrancar consumidores: `Kafka_Consume_Filtered_for_HDFS`, `Kafka_Consume_Filtered_for_Spark`.
2. Arrancar salidas: `HDFS_Backup_JSON`, `Spark_Submit_Procesamiento`.
3. Arrancar publishers: `Kafka_Publish_RAW`, `Kafka_Publish_FILTERED`.
4. Arrancar ingesta: `Build_JSON_Ingesta_KDD`, `Set_Parametros_Ingesta`, `Timer_Ingesta_15min`.
5. Arrancar bloque de errores.

## 5) Validación end-to-end

1. En Provenance, comprobar un FlowFile completo cada 15 min.
2. Ver mensajes en Kafka (`${TOPIC_RAW}` y `${TOPIC_FILTERED}`).
3. Confirmar ficheros en HDFS bajo `${HDFS_BACKUP_PATH}`.
4. Confirmar escritura de Spark en Cassandra (`nodos_estado`, `tracking_camiones`, `pagerank_nodos`).
5. Confirmar tablas Hive accesibles por `beeline`.

## 6) Importante para no duplicar trabajo

Si este grupo NiFi dispara Spark (`ConsumeKafka + ExecuteProcess`), evita lanzar a la vez el mismo procesamiento desde Airflow o desde el dashboard para no generar escrituras duplicadas.
