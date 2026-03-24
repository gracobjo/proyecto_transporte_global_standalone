# Diseño del sistema — SIMLOG

## Objetivo de diseño

SIMLOG implementa un ciclo KDD logístico de extremo a extremo con foco en:

- operación en modo standalone,
- resiliencia (componentes desacoplados por Kafka/HDFS),
- separación entre estado operativo (Cassandra) e histórico analítico (Hive),
- observabilidad y operación desde Streamlit/NiFi/Airflow.

## Arquitectura lógica

Capas principales:

1. **Ingesta**  
   NiFi y/o `ingesta/ingesta_kdd.py` generan snapshots cada 15 minutos (clima, estado de red, GPS camiones).
2. **Mensajería y backup**  
   Kafka (`transporte_raw`, `transporte_filtered`) y HDFS (`HDFS_BACKUP_PATH`).
3. **Procesamiento**  
   Spark (`procesamiento/procesamiento_grafos.py`) ejecuta limpieza, grafo, autosanación, rutas y PageRank.
4. **Persistencia**  
   Cassandra (operacional) y Hive (histórico).
5. **Orquestación y operación**  
   Airflow + NiFi + dashboard Streamlit.

## Decisiones de diseño

- **Persistencia dual**
  - Cassandra para baja latencia en consultas operativas.
  - Hive para histórico y analítica SQL.
- **Kafka con dos topics**
  - `raw` para trazabilidad.
  - `filtered` para consumo operativo.
- **Spark como capa de negocio**
  - Toda lógica de red se centraliza en Spark/GraphFrames.
- **NiFi + Airflow complementarios**
  - NiFi para ingestión/flujo visual y trigger.
  - Airflow para planificación y mantenimiento.

## Flujo operativo recomendado

1. Trigger (NiFi/Airflow) cada 15 min.
2. Ingesta produce snapshot.
3. Publicación en Kafka + backup HDFS.
4. Spark procesa y persiste en Cassandra/Hive.
5. Streamlit consume Cassandra y presenta estado.

## Restricciones y consideraciones

- Si se activa trigger en NiFi y en Airflow al mismo tiempo, puede haber duplicidad de procesamiento.
- Para evaluación estricta en cluster, usar `SPARK_MASTER=yarn`.
- La configuración de secretos se centraliza en `.env` + variables de entorno.
