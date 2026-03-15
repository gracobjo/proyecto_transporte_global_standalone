# Sistema de Gemelo Digital Logístico - España

Sistema de **gemelo digital** para logística y transporte en España. Utiliza un stack Apache (HDFS, Kafka, Spark 3.5 con GraphFrames, Hive, Cassandra) para ingesta, procesamiento con grafos, persistencia y visualización en tiempo cuasi real.

---

## Funcionalidades del sistema

| Módulo | Descripción |
|--------|-------------|
| **Ingesta (KDD)** | Obtención de clima por API, simulación de incidentes en nodos/aristas, simulación de camiones con posiciones GPS cada 15 min, y publicación a Kafka + backup en HDFS. |
| **Procesamiento (grafos)** | Construcción del grafo con GraphFrames, autosanación (eliminar rutas bloqueadas, penalizar congestión/niebla/lluvia), rutas alternativas (ShortestPath), PageRank de nodos críticos, persistencia en Cassandra y Hive. |
| **Persistencia** | **Cassandra**: estado actual de nodos, aristas, tracking de camiones y PageRank. **Hive**: histórico particionado (eventos, clima, rutas alternativas, agregados diarios) para análisis. |
| **Dashboard** | Aplicación Streamlit + Folium: mapa de España con nodos/aristas coloreados por estado, camiones en ruta, rutas alternativas (línea azul), métricas PageRank y botón "Paso Siguiente (15 min)" que ejecuta ingesta + procesamiento. |
| **Orquestación** | DAG de Airflow que cada 15 minutos verifica HDFS/Kafka/Cassandra y ejecuta Ingesta → Procesamiento (sin solapamiento, pensado para entornos con ~4 GB RAM). |

---

## Ingesta de datos (`ingesta_kdd.py`)

La **ingesta** es la **Fase I** del pipeline (Knowledge Discovery in Data). Su objetivo es generar, cada 15 minutos, un **JSON enriquecido** con el estado del sistema (clima, incidentes, camiones) y enviarlo a Kafka y HDFS para que el procesamiento Spark lo consuma.

### Qué hace la ingesta

1. **Clima por API**  
   Consulta [OpenWeatherMap](https://openweathermap.org/) para los **5 hubs** (Madrid, Barcelona, Bilbao, Vigo, Sevilla). Obtiene temperatura, humedad, descripción y visibilidad. Los resultados se incluyen en el JSON como lista `clima`.

2. **Simulación de incidentes**  
   Asigna a cada **nodo** y cada **arista** un estado aleatorio:
   - **OK** (verde): tráfico fluido, condiciones óptimas.
   - **Congestionado** (amarillo): niebla, tráfico denso, lluvia, obras.
   - **Bloqueado** (rojo): incendio, nieve, avalancha, corte de carretera.  
   Cada elemento lleva un `motivo` asociado. Esto alimenta la lógica de autosanación en el procesamiento (eliminar bloqueados, penalizar congestión/niebla/lluvia).

3. **Simulación de camiones con GPS**  
   Simula **5 camiones** en rutas sobre la red (nodos definidos en `config_nodos.py`). Para cada camión:
   - Se construye una ruta (origen → varios saltos → destino).
   - Se calcula la posición GPS actual interpolando entre origen y destino en pasos de 15 min (4 pasos por ciclo = 1 h).
   - Se guarda: `id`, `ruta`, `distancia_total_km`, `posicion_actual` (lat/lon), `nodo_actual`, `progreso_pct` y `timestamp`.

4. **JSON enriquecido**  
   Se arma un único JSON con:
   - `timestamp`
   - `clima`: lista de climas por hub
   - `estados_nodos`: estado y motivo por nodo
   - `estados_aristas`: estado, distancia y motivo por arista
   - `camiones`: lista de los 5 camiones con posición y progreso
   - `intervalo_minutos`: 15

5. **Persistencia inmediata**  
   - **Kafka**: se publica el JSON en el topic `transporte_status` (configurable) para que Spark (o otro consumidor) lo procese.
   - **HDFS**: se guarda una copia en `/user/hadoop/transporte/ingesta/transporte_YYYYMMDD_HHMMSS.json` como respaldo.

### Qué incluye el script

- **Configuración**: uso de `config_nodos.py` para `RED`, `HUBS`, `get_nodos()`, `get_aristas()`.
- **API de clima**: `API_KEY` y `WEATHER_API_URL` (OpenWeatherMap); funciones `obtener_clima_hub` y `obtener_clima_todos_hubs`.
- **Cálculo de distancias**: `calcular_distancia_haversine` (no se usa en el flujo actual pero está disponible); las distancias de aristas vienen de `get_aristas()`.
- **Simulación**: `simular_incidentes`, `interpolar_posicion`, `simular_camiones`, `crear_json_enriquecido`.
- **Salidas**: `guardar_en_hdfs` (subprocess a `hdfs dfs -put`) y `publicar_en_kafka` (KafkaProducer).
- **Orquestación**: `ejecutar_ingesta()` ejecuta el flujo completo y devuelve el JSON enriquecido; si se ejecuta como `__main__`, imprime un resumen (timestamp, número de camiones, nodos, aristas).

El script está pensado para ejecutarse de forma periódica (p. ej. cada 15 min vía cron o DAG de Airflow) o desde el dashboard con el botón "Paso Siguiente (15 min)".

---

## Estructura del proyecto

| Archivo / carpeta | Descripción |
|-------------------|-------------|
| `config.py` | Rutas de JARs (GraphFrames, Cassandra, Kafka), API Weather, Kafka, Cassandra, HDFS, Hive. |
| `config_nodos.py` | Topología: 5 hubs, 25 secundarios, aristas (malla + estrella + conexiones entre secundarios). |
| `ingesta_kdd.py` | Ingesta y simulación (clima, incidentes, camiones); publica a Kafka y guarda en HDFS. |
| `ingesta/ingesta_kdd.py` | Copia/versión del mismo script (según despliegue). |
| `procesamiento/procesamiento_grafos.py` | Spark + GraphFrames: grafo, autosanación, rutas alternativas, PageRank, escritura Cassandra/Hive. |
| `procesamiento/analisis_grafos.py` | Análisis adicional sobre grafos. |
| `persistencia_hive.py` | Creación de tablas Hive y escritura del histórico (eventos, clima, rutas, agregados). |
| `app_visualizacion.py` | Dashboard Streamlit + Folium (mapa, nodos, aristas, camiones, rutas alternativas, PageRank). |
| `orquestacion/dag_maestro.py` | DAG Airflow: comprobación de servicios y ejecución Ingesta → Procesamiento. |
| `cassandra/` | Configuración y esquema CQL (`esquema_logistica.cql`) para nodos, aristas, tracking, PageRank. |
| `sql/` | Consultas SQL/HQL auxiliares. |
| `setup_hive.hql` | Script de inicialización de Hive. |
| `requirements.txt` | Dependencias Python (requests, kafka-python, cassandra-driver, streamlit, folium, pyspark, etc.). |

---

## Requisitos y arranque rápido

- **Python 3** con `venv` recomendado.
- **Servicios**: HDFS, Kafka (KRaft), Cassandra, Hive (opcional para histórico).
- **Spark 3.5** y JARs: GraphFrames, conector Spark-Cassandra, Kafka (según uso).

### 1. Entorno e ingesta

```bash
cd ~/proyecto_transporte_global
python3 -m venv venv_transporte
source venv_transporte/bin/activate
pip install -r requirements.txt
```

Configurar (opcional) la API key de OpenWeatherMap en `config.py` o por variable de entorno.

### 2. Topic Kafka

```bash
kafka-topics.sh --create --topic transporte_status --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
```

### 3. Cassandra

```bash
./cassandra/bin/cassandra
# Esperar 30–60 s
cqlsh -f cassandra/esquema_logistica.cql
```

### 4. Ejecución manual

```bash
# Ingesta (genera JSON, Kafka + HDFS)
python ingesta_kdd.py

# Procesamiento (Spark: grafo, autosanación, Cassandra/Hive)
python procesamiento/procesamiento_grafos.py
```

### 5. Dashboard

```bash
streamlit run app_visualizacion.py
```

En el dashboard, "Paso Siguiente (15 min)" ejecuta ingesta + procesamiento y actualiza el mapa con los datos de Cassandra.

---

## Documentación adicional

- **README_GEMELO_DIGITAL.md**: instrucciones de despliegue, arranque de servicios (Cassandra, Kafka), troubleshooting (Kafka KRaft, timeouts de Cassandra con 4 GB RAM), uso del DAG y del dashboard.

---

## Subir a GitHub

Repositorio remoto: **https://github.com/gracobjo/proyecto_transporte_global_standalone.git**

Con **Git** instalado (`sudo apt install git`), desde la raíz del proyecto:

```bash
git init
git add .
git commit -m "Documentación y código: gemelo digital logístico España"
git branch -M main
git remote add origin https://github.com/gracobjo/proyecto_transporte_global_standalone.git
git push -u origin main
```

También puedes ejecutar el script incluido:

```bash
chmod +x subir_github.sh
./subir_github.sh
```

**Nota:** El `.gitignore` excluye `venv_transporte/`, logs, JARs, archivos `.tar.gz`/`.zip` y datos locales. Antes del primer push en producción, considera usar variables de entorno para la API key de OpenWeatherMap en lugar de dejarla en `config.py` o `ingesta_kdd.py`.

---

## Licencia y uso

Proyecto de referencia para gemelo digital logístico. Ajustar `config.py` y `config_nodos.py` según entorno y topología deseada. No incluir API keys en el repositorio; usar variables de entorno o secretos en producción.
# proyecto_transporte_global_standalone
