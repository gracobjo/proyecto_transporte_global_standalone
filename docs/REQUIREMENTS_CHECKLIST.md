# Checklist: requisitos del PDF "Proyecto Big Data.pdf"

Cotejo del enunciado con el estado operativo actual del proyecto SIMLOG (modo standalone).

---

## Resumen ejecutivo

| Área | ¿Se cumple? | Comentario |
|------|-------------|------------|
| **Ingesta (NiFi + Kafka)** | Sí | Ingesta Python operativa; NiFi con flujo documentado y trigger periódico; Kafka `transporte_raw` / `transporte_filtered`. |
| **Procesamiento (Spark)** | Sí | Spark 3.5 + GraphFrames, limpieza previa a persistencia, opción Hive enriquecimiento. |
| **Persistencia (HDFS, Cassandra, Hive)** | Sí | HDFS backup raw, Cassandra operativo, Hive histórico. |
| **Orquestación (Airflow)** | Sí | DAGs por fases KDD, maestro 15 min, gestión servicios; Airflow 3.x con api-server + scheduler; configurar `[api] base_url` al puerto real (p. ej. 8088). |
| **YARN** | Parcial | Soportado (`SPARK_MASTER=yarn`); despliegue habitual `local[*]`. |
| **Documentación** | Sí | Requisitos, diseño, casos de uso, diagramas Mermaid/PlantUML, Airflow, flujo de datos, **DASHBOARD_KDD_UI**. |
| **Operación stack** | Sí | `scripts/simlog_stack.py` (`start` / `status` / `stop`) y `servicios/gestion_servicios.py`. |
| **Dashboard KDD (UI)** | Sí | Pestaña Ciclo KDD: fases enlazadas a código/datos, OpenWeather en formulario, simulación por paso, topología única vs mapas en otras pestañas (`docs/DASHBOARD_KDD_UI.md`, RF/RNF en `FLUJO_DATOS_Y_REQUISITOS.md` §7–8). |

---

## Requisitos técnicos (stack)

| Requisito | Estado actual | ¿Cumple? |
|-----------|-----------------|----------|
| NiFi + Kafka (raw/filtered) | Integrado y documentado (`nifi/`) | Sí |
| Spark 3.5 + GraphFrames | `procesamiento/procesamiento_grafos.py` | Sí |
| HDFS + Cassandra + Hive | Rutas en `config.py` / esquemas | Sí |
| Airflow | DAGs en `~/airflow/dags` + `orquestacion/`; Execution API alineada al puerto del api-server | Sí |
| YARN como runtime principal | Opcional; no es el modo por defecto | Parcial |

---

## Fases KDD (estado)

### Fase I — Ingesta y selección

| Punto | Estado |
|------|--------|
| Fuentes (OpenWeather + GPS simulado) | Cubierto (`ingesta_kdd.py`, NiFi) |
| Kafka raw / filtered | Cubierto |
| Backup raw en HDFS | Cubierto (`HDFS_BACKUP_PATH`) |

**Conclusión:** Cumplida.

### Fase II — Preprocesamiento y transformación

| Punto | Estado |
|------|--------|
| Limpieza / normalización | Cubierto en pipeline Spark |
| Enriquecimiento desde Hive | Condicionado a Hive disponible (`SIMLOG_ENABLE_HIVE`) |
| Grafo, rutas, métricas | GraphFrames, ShortestPath, PageRank |

**Conclusión:** Cumplida.

### Fase III — Minería y acción

| Punto | Estado |
|------|--------|
| PageRank / criticidad | Cubierto |
| Persistencia dual Cassandra + Hive | Cubierto |
| Streaming 15 min | Script dedicado; operación principal batch periódico |

**Conclusión:** Cumplida en modo batch; streaming como variante.

### Fase IV — Orquestación

| Punto | Estado |
|------|--------|
| Ciclo periódico | Airflow (`dag_maestro_smart_grid`, `simlog_kdd_*`) + NiFi |
| Mantenimiento mensual | `dag_mensual_retrain_limpieza` (si está desplegado) |
| Arranque stack sin Streamlit | `python -u scripts/simlog_stack.py start` |

**Conclusión:** Cumplida según despliegue activo.

---

## Rúbrica (resumen)

| Criterio | Estado |
|----------|--------|
| Ingesta robusta | Kafka + HDFS + NiFi/ingesta |
| Procesamiento Spark | GraphFrames + persistencia |
| Persistencia | Cassandra + Hive |
| Orquestación | Airflow + scripts de stack |
| Documentación | Checklist, diseño, casos de uso, Mermaid, CU-09, diseño UI KDD |

---

## Brechas opcionales

1. Evidencias E2E automatizadas (reportes por run) para evaluación académica.
2. Si el tribunal exige YARN: documentar despliegue con `SPARK_MASTER=yarn`.
3. Unificar versión NiFi en documentación vs instalación local (`NIFI_HOME`).

---

## Conclusión

El proyecto cubre el ciclo KDD de extremo a extremo con stack Apache, orquestación Airflow, operación vía scripts de stack y documentación alineada al modo standalone actual.
