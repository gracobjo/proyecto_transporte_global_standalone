# SIMLOG - Sistema Integrado de Monitorización y Simulación Logística

## 🎯 Objetivo
Plataforma de **simulación y monitorización** de red de transporte en España. Stack Apache (HDFS, Kafka, Spark 3.5 con GraphFrames, Hive, Cassandra) para ingesta, procesamiento con grafos, persistencia y visualización en tiempo cuasi real.

## 🧱 Arquitectura

| Módulo | Descripción |
|--------|-------------|
| **Ingesta (KDD)** | Obtención de clima por API, simulación de incidentes en nodos/aristas, simulación de camiones con posiciones GPS cada 15 min, y publicación a Kafka + backup en HDFS. |
| **Procesamiento (grafos)** | Construcción del grafo con GraphFrames, autosanación, rutas alternativas (ShortestPath), PageRank de nodos críticos, persistencia en Cassandra y Hive. |
| **Persistencia** | **Cassandra** (estado actual), **Hive** (histórico particionado). |
| **Dashboard** | Aplicación Streamlit + Folium: mapa de España con nodos/aristas, camiones en ruta, métricas PageRank. |
| **Orquestación** | DAG de Airflow cada 15 minutos. |
| **FAQ IA** | Microservicio FastAPI + panel Streamlit. |

## 📊 Flujo de Datos

```
Ingesta → Kafka + HDFS
    ↓
Procesamiento Spark
    ↓
Cassandra (estado actual) ← Dashboard lee aquí
    +
Hive (histórico)
```

## ⚙️ Tecnologías
- Python 3.x
- Apache Kafka
- Apache Spark 3.5 + GraphFrames
- Apache Hive
- Apache Cassandra
- HDFS
- Streamlit + Folium
- Airflow
- FastAPI

## 📌 Componentes

### Ingesta
- `ingesta_kdd.py` - Generación de snapshot cada 15 min
- `ingesta_dgt_datex2.py` - Ingesta de DGT
- `trigger_paso.py` - Trigger para pasos

### Procesamiento
- `procesamiento_grafos.py` - Grafo con GraphFrames
- `fase_kdd_spark.py` - Pipeline KDD
- `reconfiguracion_grafo.py` - Reconfiguración logística
- `streaming_*.py` - Procesamiento streaming

### Servicios
- `app_visualizacion.py` - Dashboard principal
- `cuadro_mando_ui.py` - Cuadro de mando
- `api_simlog.py` - API REST
- `api_faq_ia.py` - FAQ con IA

### Orquestación
- `dag_*.py` - DAGs de Airflow
- `kdd_ejecucion.py` - Pipeline KDD

## 📁 Archivos del Proyecto
- `config.py` - Configuración principal
- `config_nodos.py` - Configuración de nodos
- `docker-compose.yml` - Stack completo Docker

## 🔗 Relacionado
- [[Kafka]]
- [[Spark]]
- [[Cassandra]]
- [[Hive]]
- [[GraphFrames]]

## 🏷️ Tags
#proyecto #bigdata #simlog #transporte
