# Alineación con Sentinel360 (referencia) — SIMLOG en modo **STANDALONE**

Este proyecto (**SIMLOG España**) toma como **referencia de funcionalidades** el repositorio público  
**[Proyecto-Big-Data-Sentinel-360](https://github.com/gracobjo/Proyecto-Big-Data-Sentinel-360)** (Sentinel360: monitorización logística, ciclo KDD, stack Apache).

La diferencia explícita es el **despliegue**: aquí el objetivo es un entorno **standalone** (una máquina o VM de desarrollo), sin depender de un **clúster multi-nodo**, **YARN** como orquestador obligatorio ni servicios distribuidos a escala de producción.

> **Persistencia operativa:** en **SIMLOG no se utiliza MongoDB**. El almacén de **estado en tiempo casi real** (nodos, aristas, tracking, PageRank, eventos) es **exclusivamente Apache Cassandra** (keyspace `logistica_espana`, tablas definidas en `cassandra/esquema_logistica.cql`).

---

## 1. Principios del modo standalone en SIMLOG

| Principio | Significado práctico |
|-----------|----------------------|
| **Un solo host** | HDFS NameNode, Kafka KRaft, Cassandra, Hive Metastore, Spark y Streamlit pueden convivir en localhost o una IP fija. |
| **Spark `local[*]`** (o `local[N]`) | Sin ResourceManager YARN; se limita memoria en `config` / scripts (`spark.driver.memory`, etc.). |
| **Persistencia operativa en Cassandra** | Equivalente funcional a colecciones “rápidas” (p. ej. MongoDB en Sentinel360): estado actual de nodos, aristas, tracking. |
| **Histórico en Hive** | Opcional; el warehouse suele residir en HDFS local. |
| **Orquestación Airflow** | Scheduler + api-server en la misma máquina; DAGs por fases KDD encadenados (`TriggerDagRunOperator`). |
| **Panel único Streamlit** | Concentrar visualización, cuadro de mando, rutas híbridas, gestión de servicios y enlaces a UIs. |

---

## 2. Mapa funcional Sentinel360 → SIMLOG (standalone)

| Área (Sentinel360) | Qué hace en el repo de referencia | Equivalente en SIMLOG | Estado |
|--------------------|-----------------------------------|------------------------|--------|
| **Marca / narrativa** | Sentinel360, visión 360° | SIMLOG España, branding en `BRANDING.md` | ✅ |
| **Configuración central** | `config.py` (SSOT) | `config.py`, `config_nodos.py` | ✅ |
| **Fase I — Ingesta** | NiFi + Kafka + HDFS raw | `ingesta/ingesta_kdd.py` + Kafka + HDFS; NiFi documentado en `nifi/` (opcional) | ✅ / parcial NiFi |
| **Temas Kafka** | raw / filtered | `TOPIC_RAW`, `TOPIC_FILTERED` / `transporte_filtered` | ✅ |
| **Fase II — Limpieza / enriquecimiento** | Spark SQL, maestros Hive | `limpiar_datos_antes_cassandra`, `enriquecer_desde_hive` en `procesamiento_grafos.py` | ✅ / Hive opcional |
| **Grafos** | GraphFrames, rutas | GraphFrames, autosanación, PageRank, `procesamiento_grafos.py` | ✅ |
| **Fase III — Streaming** | Ventanas 15 min, alertas Kafka | Scripts `procesamiento/streaming_*.py` (según despliegue); ingesta por pasos 15 min | Parcial / según entorno |
| **Estado operativo / near-real-time** | En Sentinel360 a veces MongoDB | **Solo Cassandra** en SIMLOG: `nodos_estado`, `aristas_estado`, `tracking_camiones`, `pagerank_nodos`, `eventos_historico` | ✅ |
| **Histórico analítico** | Hive Parquet | Hive (`historico_nodos`, `nodos_maestro`, etc.) | ✅ opcional |
| **Orquestación** | Airflow DAGs | `orquestacion/dag_simlog_kdd_fases.py`, `dag_maestro.py`, otros DAGs | ✅ |
| **API REST / OpenAPI** | — | `servicios/api_simlog.py` (FastAPI `/docs`) | ✅ |
| **Dashboard** | Streamlit presentación | `app_visualizacion.py` (pestañas KDD, cuadro de mando, rutas híbridas, servicios) | ✅ |
| **Grafana / Superset** | KPIs en Docker | No incluido por defecto; se puede añadir vía Docker opcional o enlazar MariaDB externo | ⚪ fuera del núcleo |
| **Clúster multi-nodo** | IPs fijas, YARN | **No** es objetivo; documentación `docs/YARN_Y_SPARK.md` para migración | ⚪ |

Leyenda: ✅ cubierto / equivalente · ⚪ no objetivo standalone · parcial = depende de instalación local.

---

## 3. Casos de uso alineados (presentación)

| Caso de uso (tipo Sentinel360) | Descripción breve | Dónde en SIMLOG |
|--------------------------------|-------------------|-----------------|
| **CU-01 Ingesta de contexto** | Clima + GPS + incidentes → Kafka/HDFS | `ingesta/ingesta_kdd.py`, Airflow fase 1–2 |
| **CU-02 Procesamiento de grafo** | Grafo, penalizaciones, PageRank | `procesamiento/procesamiento_grafos.py` |
| **CU-03 Consulta operativa** | Estado actual de red y flota | Cassandra + `app_visualizacion` mapa |
| **CU-04 Histórico** | Tendencias en SQL | Hive + beeline (cuadro de mando) |
| **CU-05 Orquestación** | DAGs por fase KDD | `orquestacion/` |
| **CU-06 Integración externa** | APIs documentadas | `servicios/api_simlog.py` |
| **CU-07 Planificación ante incidencias** | Rutas, retrasos, alternativas | `servicios/red_hibrida_rutas.py` + pestaña Streamlit |
| **CU-08 Operación de servicios** | Arranque / parada / UIs | `servicios/gestion_servicios.py`, sidebar |

---

## 4. Qué **no** se replica igual (por diseño standalone)

- **YARN** como planificador obligatorio: aquí Spark en local.
- **MongoDB**: **no forma parte de SIMLOG**; no está en dependencias ni en el diseño. Cualquier referencia en proyectos tipo Sentinel360 es solo comparativa; aquí el rol lo cubre **solo Cassandra**.
- **NiFi** en runtime: opcional; la ingesta principal es Python (documentación NiFi para paridad).
- **Grafana/Superset/MariaDB** empaquetados: no son dependencia del núcleo; se pueden añadir en una fase posterior.

---

## 5. Cómo “acercarse” más a Sentinel360 sin dejar el standalone

1. **NiFi**: importar el grupo documentado en `nifi/` y apuntar a los mismos topics que `ingesta_kdd.py`.
2. **Streaming estructurado**: ejecutar `procesamiento/streaming_*.py` con Kafka local si hay CPU/RAM.
3. **Dashboards**: exportar CSV desde Hive/Cassandra y cargar en Grafana/Superset en un `docker-compose` aparte (mismo host).
4. **Airflow**: mantener `AIRFLOW_HOME` local y un solo worker; no requiere cluster Celery.

---

## 6. Referencias cruzadas en este repositorio

| Documento | Contenido |
|-----------|-----------|
| [`README.md`](../README.md) | Visión general SIMLOG |
| [`README_SIMLOG.md`](../README_SIMLOG.md) | Despliegue operativo |
| [`docs/FLUJO_DATOS_Y_REQUISITOS.md`](FLUJO_DATOS_Y_REQUISITOS.md) | Flujo de datos y requisitos PDF |
| [`docs/REQUIREMENTS_CHECKLIST.md`](REQUIREMENTS_CHECKLIST.md) | Checklist vs enunciado |
| [`docs/AIRFLOW.md`](AIRFLOW.md) | Airflow local |
| [`orquestacion/README_DAGS_KDD.md`](../orquestacion/README_DAGS_KDD.md) | DAGs KDD encadenados |

---

*Documento generado para alinear el alcance funcional con el proyecto de referencia [Sentinel360](https://github.com/gracobjo/Proyecto-Big-Data-Sentinel-360), manteniendo SIMLOG como solución **standalone**.*
