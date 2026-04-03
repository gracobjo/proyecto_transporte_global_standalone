# Checklist: requisitos del PDF "Proyecto Big Data.pdf"

Cotejo del enunciado con el estado operativo actual del proyecto SIMLOG (modo standalone).

---

## Resumen ejecutivo

| Área | ¿Se cumple? | Comentario |
|------|-------------|------------|
| **Ingesta (NiFi + Kafka)** | Sí | Ingesta Python operativa; NiFi con flujo documentado y trigger periódico; Kafka `transporte_dgt_raw` / `transporte_raw` / `transporte_filtered`. |
| **Procesamiento (Spark)** | Sí | Spark 3.5 + GraphFrames, limpieza previa a persistencia, opción Hive enriquecimiento. |
| **Persistencia (HDFS, Cassandra, Hive)** | Sí | HDFS backup raw, Cassandra operativo, Hive histórico. |
| **Orquestación (Airflow)** | Sí | DAGs por fases KDD, maestro 15 min, gestión servicios; Airflow 3.x con api-server + scheduler; configurar `[api] base_url` al puerto real (p. ej. 8088). |
| **Convención DAGs SIMLOG** | Sí | Los DAGs operativos del proyecto siguen prefijo `simlog_` y están documentados (`docs/AIRFLOW_DAGS_SIMLOG.md`). |
| **YARN** | Parcial | Soportado (`SPARK_MASTER=yarn`); despliegue habitual `local[*]`. |
| **Documentación** | Sí | Requisitos, diseño, casos de uso, diagramas Mermaid/PlantUML, Airflow, flujo de datos, **DASHBOARD_KDD_UI**. |
| **Operación stack** | Sí | `scripts/simlog_stack.py` (`start` / `status` / `stop`) y `servicios/gestion_servicios.py`. |
| **Cluster en Codespaces (perfil aislado)** | Sí | Guía y artefactos dedicados: `docker-compose.codespaces.yml`, `Dockerfile.codespaces`, `hadoop.codespaces.env`, `docs/CODESPACES_CLUSTER.md`. |
| **Dashboard KDD (UI)** | Sí | Pestaña Ciclo KDD: fases enlazadas a código/datos, OpenWeather en formulario cuando hay clave válida, respaldo DGT visible cuando no responde, simulación por paso, topología única vs mapas en otras pestañas (`docs/DASHBOARD_KDD_UI.md`, RF/RNF en `FLUJO_DATOS_Y_REQUISITOS.md` §7–8). |
| **Cuadro de mando extendido** | Sí | Consultas supervisadas Hive/Cassandra, SQL/CQL de lectura desde frontend, **contexto de modelo de datos** (descripción + columnas), **análisis asistido** sobre histórico Hive (estadísticas y proyección heurística), formateo tabular, informes PDF/plantillas; poda Hive `_P24H` en ventanas 24h para evitar timeouts. |
| **Navegación semántica UI** | Sí | Buscador semántico en cabecera con salto directo a pestañas/secciones. |
| **Swagger en catálogo de servicios** | Sí | API FastAPI incluida en resumen de servicios y enlaces de acceso (`/docs`, `/redoc`). |
| **FAQ IA operativa** | Sí | Microservicio FAQ local (`8091`) + panel Streamlit + KB JSON editable + Swagger. |
| **Integración DATEX2 DGT** | Sí | Feed XML real con caché, merge con simulación y trazabilidad por fuente. |

---

## Requisitos técnicos (stack)

| Requisito | Estado actual | ¿Cumple? |
|-----------|-----------------|----------|
| NiFi + Kafka (dgt_raw/raw/filtered) | Integrado y documentado (`nifi/`) | Sí |
| Spark 3.5 + GraphFrames | `procesamiento/procesamiento_grafos.py` | Sí |
| HDFS + Cassandra + Hive | Rutas en `config.py` / esquemas | Sí |
| Airflow | DAGs en `~/airflow/dags` + `orquestacion/`; Execution API alineada al puerto del api-server | Sí |
| YARN como runtime principal | Opcional; no es el modo por defecto | Parcial |
| Despliegue cloud didáctico (Codespaces) | Perfil separado y documentado para no interferir con stack principal | Sí |
| FAQ IA local | `servicios/api_faq_ia.py` + `servicios/ui_faq_ia.py` + `servicios/faq_knowledge_base.json` | Sí |

---

## Requisito específico: clúster en GitHub Codespaces

| Punto | Estado |
|------|--------|
| Composición del clúster | Cubierto (`docker-compose.codespaces.yml`) |
| Imagen Hadoop + Java 21 | Cubierto (`Dockerfile.codespaces`) |
| Variables de configuración | Cubierto (`hadoop.codespaces.env`) |
| Guía paso a paso (arranque, puertos, validación, parada) | Cubierto (`docs/CODESPACES_CLUSTER.md`) |
| Separación respecto al despliegue principal | Cubierto (perfil con nombres dedicados `*.codespaces.*`) |

**Conclusión:** Cumplido para entorno docente/demostrativo en Codespaces.

---

## Fases KDD (estado)

### Fase I — Ingesta y selección

| Punto | Estado |
|------|--------|
| Fuentes (OpenWeather + GPS simulado + DGT DATEX2) | Cubierto (`ingesta_kdd.py`, `ingesta_dgt_datex2.py`, NiFi); con clima alternativo DGT si OpenWeather falla |
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
| Ciclo periódico | Airflow (`simlog_maestro`, `simlog_kdd_*`) + NiFi |
| Mantenimiento mensual | `dag_mensual_retrain_limpieza` (si está desplegado) |
| Arranque stack sin Streamlit | `python -u scripts/simlog_stack.py start` |

**Conclusión:** Cumplida según despliegue activo.

---

## Rúbrica (resumen)

| Criterio | Estado |
|----------|--------|
| Ingesta robusta | Kafka + HDFS + NiFi/ingesta + respaldo DGT para continuidad del clima |
| Procesamiento Spark | GraphFrames + persistencia |
| Persistencia | Cassandra + Hive |
| Orquestación | Airflow + scripts de stack |
| Documentación | Checklist, diseño, casos de uso, Mermaid, CU-09 y FAQ IA documentada |
| Documentación operativa ampliada | Manual usuario/desarrollador + memoria actualizados con informes, buscador semántico, Swagger y FAQ IA |

---

## Brechas opcionales

1. Evidencias E2E automatizadas (reportes por run) para evaluación académica.
2. Si el tribunal exige YARN: documentar despliegue con `SPARK_MASTER=yarn`.
3. Sustituir la API key de OpenWeather por una válida si se quiere volver al modo de clima principal; mientras tanto, queda documentado el uso del respaldo DGT.
4. Ampliar la KB FAQ con troubleshooting adicional según incidencias reales de operación.

---

## Conclusión

El proyecto cubre el ciclo KDD de extremo a extremo con stack Apache, orquestación Airflow, operación vía scripts de stack y documentación alineada al modo standalone actual.
