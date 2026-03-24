# Montaje rápido del Process Group en la UI de NiFi

Orden sugerido (ajusta nombres de *relationship* a tu versión).

1. **Crear Process Group** `PG_SIMLOG_KDD`.
2. **Parameter Context** (menú derecho del canvas) → importar claves de `parameter-context.env.example`.
3. Añadir procesadores:
   - `GenerateFlowFile` (cada 15 min, 1 FlowFile).
   - `ExecuteScript` (Groovy) → pegar `groovy/GenerateSyntheticPayload.groovy`.
   - `DuplicateFlowFile` (opcional) si necesitas bifurcar hacia Merge.
   - `UpdateAttribute` → fija `owm_url` con `${OWM_API_KEY}` (clave sensible en parámetro).
   - `InvokeHTTP` → GET `${owm_url}`.
   - `MergeContent` → mínimo 2 entradas: rama sintética + rama HTTP (o usa un solo ExecuteScript que ya incorpore clima vía HTTP interno y simplifica el flujo).
   - `PublishKafka` → topic `${TOPIC_RAW}`.
   - `PutHDFS` → `${HDFS_RAW_BASE}/...`.
   - `ExecuteProcess` → `bash` + `nifi/scripts/spark_submit_yarn.sh`.
4. **Controller Services** (si aplica): pool Hive, SSL Kafka.
5. **Conectar** *success* de PutHDFS y PublishKafka hacia Spark, o solo una de las ramas para no duplicar jobs.
6. Validar con **NiFi** *Provenance* y comprobar topic en Kafka + fichero en HDFS.

## Simplificación para prácticas

Si **MergeContent** complica la demo: usar **un solo ExecuteScript Groovy** que:

1. Construya el JSON sintético.
2. Abra `HttpURLConnection` a OpenWeather para los 5 hubs.
3. Escriba el JSON final.

Así mantienes los requisitos del PDF (datos sintéticos + API real) con menos procesadores visibles; documenta en la memoria que la alternativa modular es la de `README_NIFI.md`.
