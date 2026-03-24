# Checklist: requisitos del PDF "Proyecto Big Data.pdf"

Este documento coteja el enunciado **Proyecto Big Data.pdf** (raíz del repo) con el estado actual del proyecto. El PDF exige un ciclo KDD con stack Apache 2025-2026.

---

## Resumen ejecutivo (estado actual)

| Área | ¿Se cumple? | Comentario |
|------|-------------|------------|
| **Ingesta (NiFi + Kafka)** | Parcial | Ingesta Python operativa + especificación NiFi disponible; integración NiFi en runtime depende del entorno. |
| **Procesamiento (Spark)** | Sí | GraphFrames y limpieza previa a Cassandra; streaming 15 min disponible como script dedicado. |
| **Persistencia (HDFS, Cassandra, Hive)** | Sí | HDFS (raw + warehouse Hive), Cassandra, Hive con tablas históricas. |
| **Orquestación (Airflow)** | Parcial | DAG operativo por fases; el despliegue mensual/retrain depende de activar ese DAG en tu instalación. |
| **YARN** | Parcial | Soportado por configuración (`SPARK_MASTER=yarn`), pero en standalone suele ejecutarse `local`. |
| **Documentación** | Sí | README, AGENTS.md, docs de flujo y requisitos. |

---

## Requisitos técnicos del PDF (Stack Apache 2026)

| Requisito PDF | Versión pedida | En el proyecto | ¿Cumple? |
|---------------|----------------|----------------|----------|
| Ingesta | NiFi 2.6.0 + Kafka 3.9.1 (KRaft) | Kafka con `transporte_raw`/`transporte_filtered`; flujo NiFi documentado en `nifi/` | Parcial |
| Procesamiento | Spark 3.5.x (SQL, Structured Streaming, GraphFrames) | Spark 3.5, GraphFrames, limpieza previa a Cassandra, script de streaming 15 min | Sí / Parcial según modo |
| Orquestación | Airflow 2.10.x | DAG presente (versión según instalación) | Parcial |
| Almacenamiento | HDFS 3.4.2, Cassandra 5.0, Hive | HDFS, Cassandra, Hive (versiones según instalación) | Sí |
| Gestión recursos | YARN | Compatible por configuración, no obligatorio en standalone | Parcial |

---

## Fases KDD según el PDF

### Fase I: Ingesta y Selección (NiFi + Kafka)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| Fuentes externas | **NiFi** consumiendo API pública (OpenWeather, etc.) y logs GPS simulados | Ingesta Python operativa + flujo NiFi definido para montar en canvas | Parcial |
| Streaming | Publicar en Kafka con **dos temas**: "Datos Crudos" y "Datos Filtrados" | Implementado con `transporte_raw` y `transporte_filtered` | Sí |
| Registro | Copia "raw" en HDFS para auditoría | JSON de ingesta guardado en HDFS | Sí |

**Conclusión Fase I:** Se cumple en Kafka (raw/filtered). La parte NiFi se cumple cuando el flujo `nifi/` se despliega en runtime.

---

### Fase II: Preprocesamiento y Transformación (Spark)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| Limpieza | **Spark SQL** para normalizar, nulos y duplicados | Implementada vía `limpiar_datos_antes_cassandra` en pipeline Spark | Sí / Parcial |
| Enriquecimiento | Cruzar streaming Kafka con **datos maestros en Hive** | Implementado mediante `enriquecer_desde_hive()` cuando Hive está disponible | Parcial |
| Análisis de grafos | GraphFrames: nodos (almacenes), aristas (rutas), camino más corto o comunidades | GraphFrames con nodos/aristas, autosanación, ShortestPath, PageRank | Sí |

**Conclusión Fase II:** Grafos y limpieza operativos; enriquecimiento Hive disponible condicionado a servicio/metastore.

---

### Fase III: Minería y Acción (Streaming + ML)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| Ventanas de tiempo | **Structured Streaming** con ventanas de **15 minutos** (media de retrasos) | Script `procesamiento/streaming_ventanas_15min.py` disponible; no siempre activo en modo standalone | Parcial |
| Carga multicapa | Hive: agregados histórico; Cassandra: último estado por vehículo | Sí: Hive histórico, Cassandra estado actual (nodos, aristas, camiones, PageRank) | Sí |

**Conclusión Fase III:** Carga dual sí; streaming 15 min disponible y dependiente de despliegue.

---

### Fase IV: Orquestación (Airflow)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| DAG | Coordinar **re-entrenamiento mensual** del modelo de grafos y **limpieza de tablas temporales en HDFS** | DAG mensual definido (`dag_mensual_retrain_limpieza.py`) + DAG operativo por fases | Parcial / Sí según instalación |

**Conclusión Fase IV:** El diseño del DAG mensual existe; su cumplimiento final depende de despliegue y scheduler activos.

---

## Rúbrica de evaluación (resumen)

| Criterio | Excelente (10) según PDF | Estado actual |
|----------|--------------------------|----------------|
| Ingesta | NiFi y Kafka con back-pressure y manejo de errores | Kafka sí; NiFi no; back-pressure no explícito |
| Procesamiento Spark | GraphFrames, SQL, Streaming, optimización de joins | GraphFrames sí; limpieza en Python; no Structured Streaming |
| Persistencia | Cassandra + Hive según caso de uso | Sí (Cassandra estado actual, Hive histórico) |
| Orquestación Airflow | DAGs con reintentos, alertas, dependencias | DAG cada 15 min + DAG mensual (retrain + limpieza HDFS) |
| Documentación | Cada etapa KDD, diagramas, justificación | README, docs de flujo, AGENTS, este checklist |

---

## Implementado (alineado al PDF)

- **Kafka**: Dos temas `transporte_raw` y `transporte_filtered`. Ingesta publica en ambos; crear con `bash sql/crear_temas_kafka.sh`.
- **Structured Streaming**: `procesamiento/streaming_ventanas_15min.py` (ventanas 15 min sobre `transporte_filtered`).
- **Enriquecimiento Hive**: `enriquecer_desde_hive()` en procesamiento; tabla `nodos_maestro`; enriquece nodos con `hub`.
- **DAG mensual**: `orquestacion/dag_mensual_retrain_limpieza.py` (día 1 de cada mes: limpieza HDFS + re-entrenamiento grafos).
- **YARN**: `SPARK_MASTER=yarn` o `spark-submit --master yarn`; ver `docs/YARN_Y_SPARK.md`.

## Qué falta para alinearse al PDF al 100%

1. **NiFi en runtime**: importar y ejecutar efectivamente el flujo `nifi/flow/simlog_kdd_canvas_import.json`.
2. **Criterio de despliegue**: documentar explícitamente si la evaluación será en modo `standalone` o `yarn` para cerrar la rúbrica.

---

## Conclusión

Con la infraestructura actual:

- **Se cumple**: ciclo de datos (ingesta script → Kafka/HDFS → Spark → Cassandra + Hive), uso de GraphFrames, persistencia dual, orquestación con Airflow, documentación.
- **Implementado**: dos temas Kafka (raw/filtrado), Structured Streaming 15 min, enriquecimiento desde Hive, DAG mensual + limpieza HDFS, opción YARN.
- **Sigue pendiente para 100%**: validar NiFi en ejecución real y dejar evidencia de pruebas end-to-end en el entorno objetivo.
