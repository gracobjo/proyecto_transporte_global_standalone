# Checklist: requisitos del PDF "Proyecto Big Data.pdf"

Este documento coteja el enunciado **Proyecto Big Data.pdf** (raíz del repo) con el estado actual del proyecto. El PDF exige un ciclo KDD con stack Apache 2025-2026.

---

## Resumen ejecutivo

| Área | ¿Se cumple? | Comentario |
|------|-------------|------------|
| **Ingesta (NiFi + Kafka)** | Parcial | **NiFi no integrado**; script Python publica en dos temas Kafka (raw + filtered). |
| **Procesamiento (Spark)** | Sí | GraphFrames, limpieza, **Structured Streaming** (ventanas 15 min) y enriquecimiento desde Hive. |
| **Persistencia (HDFS, Cassandra, Hive)** | Sí | HDFS (raw + warehouse Hive), Cassandra, Hive con tablas históricas. |
| **Orquestación (Airflow)** | Parcial | Hay DAG (cada 15 min); el PDF pide DAG de **re-entrenamiento mensual** y limpieza HDFS. |
| **YARN** | No | Spark se ejecuta en modo `local`, no en cluster YARN. |
| **Documentación** | Sí | README, AGENTS.md, docs de flujo y requisitos. |

---

## Requisitos técnicos del PDF (Stack Apache 2026)

| Requisito PDF | Versión pedida | En el proyecto | ¿Cumple? |
|---------------|----------------|----------------|----------|
| Ingesta | NiFi 2.6.0 + Kafka 3.9.1 (KRaft) | Script Python + Kafka (NiFi no usado) | No (falta NiFi) |
| Procesamiento | Spark 3.5.x (SQL, Structured Streaming, GraphFrames) | Spark 3.5, GraphFrames, Spark SQL implícito; no Structured Streaming con ventanas | Parcial |
| Orquestación | Airflow 2.10.x | DAG presente (versión según instalación) | Parcial |
| Almacenamiento | HDFS 3.4.2, Cassandra 5.0, Hive | HDFS, Cassandra, Hive (versiones según instalación) | Sí |
| Gestión recursos | YARN | No; Spark en `local` | No |

---

## Fases KDD según el PDF

### Fase I: Ingesta y Selección (NiFi + Kafka)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| Fuentes externas | **NiFi** consumiendo API pública (OpenWeather, etc.) y logs GPS simulados | Script Python llama a OpenWeather y simula GPS; **no hay NiFi** | No |
| Streaming | Publicar en Kafka con **dos temas**: "Datos Crudos" y "Datos Filtrados" | Un solo tema `transporte_status` (datos ya enriquecidos) | Parcial |
| Registro | Copia "raw" en HDFS para auditoría | JSON de ingesta guardado en HDFS | Sí |

**Conclusión Fase I:** Sin NiFi y sin separación explícita de temas raw/filtrados, **no se cumple al 100%**. Para acercarse: integrar NiFi en la ingesta y usar dos temas Kafka (raw + filtrado).

---

### Fase II: Preprocesamiento y Transformación (Spark)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| Limpieza | **Spark SQL** para normalizar, nulos y duplicados | Limpieza en Python (`limpiar_datos_antes_cassandra`) antes de escribir; se podría hacer con Spark SQL | Parcial |
| Enriquecimiento | Cruzar streaming Kafka con **datos maestros en Hive** | No se cruza con tablas Hive; se usa `config_nodos` y payload de ingesta | Parcial / No |
| Análisis de grafos | GraphFrames: nodos (almacenes), aristas (rutas), camino más corto o comunidades | GraphFrames con nodos/aristas, autosanación, ShortestPath, PageRank | Sí |

**Conclusión Fase II:** Grafos sí; limpieza existe pero no está formulada como “Spark SQL”; enriquecimiento con datos maestros en Hive **no** implementado.

---

### Fase III: Minería y Acción (Streaming + ML)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| Ventanas de tiempo | **Structured Streaming** con ventanas de **15 minutos** (media de retrasos) | Procesamiento por lotes (lectura desde HDFS); ciclo cada 15 min pero no ventanas de ventana en Structured Streaming | Parcial / No |
| Carga multicapa | Hive: agregados histórico; Cassandra: último estado por vehículo | Sí: Hive histórico, Cassandra estado actual (nodos, aristas, camiones, PageRank) | Sí |

**Conclusión Fase III:** Carga dual Hive/Cassandra sí; **Structured Streaming con ventanas de 15 min** no está como en el enunciado.

---

### Fase IV: Orquestación (Airflow)

| Punto del PDF | Qué pide | Estado en el proyecto | ¿Cumple? |
|---------------|----------|------------------------|----------|
| DAG | Coordinar **re-entrenamiento mensual** del modelo de grafos y **limpieza de tablas temporales en HDFS** | DAG ejecuta Ingesta + Procesamiento **cada 15 min**; no hay tarea mensual de re-entrenamiento ni limpieza HDFS explícita | Parcial |

**Conclusión Fase IV:** Hay orquestación con Airflow, pero el objetivo del DAG (mensual + limpieza HDFS) no coincide con lo pedido.

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

## Qué falta para alinearse al PDF

1. **NiFi**: Integrar NiFi 2.6.0 para la ingesta (API + logs GPS) y que publique en Kafka (y opcionalmente escriba raw en HDFS). El script Python podría sustituirse o complementarse.
2. **Spark SQL** para limpieza: opcional (ya existe limpieza en Python antes de Cassandra). El resto (Kafka 2 temas, Streaming, Hive, DAG mensual, YARN) está implementado.

---

## Conclusión

Con la infraestructura actual **sin NiFi**:

- **Se cumple**: ciclo de datos (ingesta script → Kafka/HDFS → Spark → Cassandra + Hive), uso de GraphFrames, persistencia dual, orquestación con Airflow, documentación.
- **Implementado**: dos temas Kafka (raw/filtrado), Structured Streaming 15 min, enriquecimiento desde Hive, DAG mensual + limpieza HDFS, opción YARN.
- **Sigue faltando**: **NiFi** en la ingesta (el script Python cubre la lógica; NiFi sería la capa exigida por el enunciado).
