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

## Responsabilidades de cada componente

Cada pieza del sistema tiene una función concreta. Ningún componente “le pasa datos” a otro de forma cruzada: la ingesta alimenta al procesamiento; el procesamiento escribe en Cassandra (estado actual) y, opcionalmente, en Hive (histórico).

| Componente | Responsabilidad única | Entrada | Salida |
|------------|------------------------|---------|--------|
| **Ingesta** (`ingesta_kdd.py`) | Generar el “snapshot” del sistema cada 15 min: clima real, estados simulados de nodos/aristas, posiciones de camiones. | API OpenWeatherMap, topología `config_nodos`. | Un JSON enriquecido → Kafka (topic `transporte_status`) y copia en HDFS. |
| **Kafka** | Cola de mensajes: recibe el JSON de la ingesta y lo deja disponible para el consumidor (Spark u otro). | Mensajes publicados por la ingesta. | Lectura por el procesamiento (o por cualquier consumidor del topic). |
| **HDFS** | Almacén de respaldo: guardar copias del JSON de ingesta por si Kafka no está o para reprocesar. | JSON escrito por la ingesta. | Archivos JSON que el procesamiento Spark puede leer como fuente alternativa. |
| **Procesamiento Spark** (`procesamiento_grafos.py`) | Construir el grafo, autosanación, rutas alternativas, PageRank, y persistir resultados. | JSON de ingesta desde HDFS (o datos simulados). Topología `config_nodos`. | Escritura en **Cassandra** (nodos, aristas, camiones, PageRank) y opcional en **Hive** (histórico). |
| **Cassandra** | Guardar **solo el estado actual** del gemelo digital para consultas rápidas y dashboard. | Datos escritos por Spark. | Lecturas por el dashboard y por `cqlsh` / aplicaciones. |
| **Hive** | Guardar **histórico** para análisis en el tiempo (eventos, clima, rutas, agregados). No alimenta a Cassandra. | Datos escritos por Spark o por `persistencia_hive.py`. | Consultas SQL (HiveQL) para reportes y tendencias. |
| **Dashboard** (`app_visualizacion.py`) | Mostrar en mapa el estado actual (nodos, aristas, camiones, PageRank). Puede lanzar “Paso Siguiente”. | Solo **Cassandra** (lectura). No lee de Hive. | Interfaz web (Streamlit + Folium). |
| **Airflow** (DAG) | Orquestar cada 15 min: comprobar servicios y ejecutar Ingesta → Procesamiento. | Configuración del DAG. | Ejecución programada de los scripts. |

Flujo de datos:

```
Ingesta → Kafka + HDFS
    ↓
Procesamiento Spark ← (lee HDFS o simula)
    ↓
Cassandra (estado actual)   ←  Dashboard lee aquí
    +
Hive (histórico)            ←  Consultas analíticas / reportes
```

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
| `orquestacion/dag_arranque_servicios.py` | DAG para **levantar** HDFS, Cassandra y Kafka (ejecución manual). |
| `cassandra/` | Configuración y esquema CQL (`esquema_logistica.cql`) para nodos, aristas, tracking, PageRank. |
| `sql/` | Consultas SQL/HQL auxiliares. |
| `setup_hive.hql` | Script de inicialización de Hive. |
| `requirements.txt` | Dependencias Python (requests, kafka-python, cassandra-driver, streamlit, folium, pyspark, etc.). |

---

## Visualización de datos en Cassandra

Los datos del gemelo digital se almacenan en el keyspace **`logistica_espana`**. Se pueden consultar por **línea de comandos** con `cqlsh` o visualizarlos en el **dashboard** Streamlit.

### Tablas y contenido

| Tabla | Descripción | Uso en visualización |
|-------|-------------|----------------------|
| **`nodos_estado`** | Estado de cada nodo (hub/ciudad): coordenadas, estado (OK/Congestionado/Bloqueado), motivo, clima, temperatura, humedad. | En el mapa: círculos por ciudad; color según estado; popup con motivo y PageRank. |
| **`aristas_estado`** | Estado de cada arista (ruta entre nodos): origen, destino, distancia, estado. | Líneas entre nodos; color según estado (verde/naranja/rojo). |
| **`tracking_camiones`** | Posición GPS de cada camión, origen/destino, ruta sugerida, estado, motivo. | Marcadores azules en el mapa; línea azul discontinua = ruta alternativa. |
| **`pagerank_nodos`** | PageRank por nodo (importancia en la red). | Tabla “PageRank - Nodos más críticos” en el dashboard; popup en cada nodo. |

### Acceso por línea de comandos (`cqlsh`)

Con Cassandra en marcha (`nc -z 127.0.0.1 9042`):

```bash
# Entrar al keyspace
cqlsh -e "USE logistica_espana;"

# Ver estado de nodos
cqlsh -e "USE logistica_espana; SELECT id_nodo, lat, lon, estado, motivo_retraso, clima_actual FROM nodos_estado LIMIT 20;"

# Ver aristas
cqlsh -e "USE logistica_espana; SELECT src, dst, distancia_km, estado FROM aristas_estado LIMIT 20;"

# Ver camiones
cqlsh -e "USE logistica_espana; SELECT id_camion, lat, lon, ruta_origen, ruta_destino, estado_ruta FROM tracking_camiones;"

# Ver PageRank
cqlsh -e "USE logistica_espana; SELECT id_nodo, pagerank FROM pagerank_nodos LIMIT 15;"
```

Modo interactivo: `cqlsh` → `USE logistica_espana;` → luego las consultas anteriores.

### Dashboard Streamlit (mapa e interfaz)

La app **`app_visualizacion.py`** se conecta a Cassandra (`CASSANDRA_HOST` y `KEYSPACE` en `config.py`) y:

1. **Carga los datos** con el driver `cassandra-driver`: ejecuta `SELECT` sobre `nodos_estado`, `aristas_estado`, `tracking_camiones` y `pagerank_nodos`.
2. **Construye un mapa Folium** centrado en España:
   - **Nodos**: `CircleMarker` por cada ciudad; color según `estado` (verde OK, naranja Congestionado, rojo Bloqueado); radio mayor en hubs; popup con estado, motivo y PageRank.
   - **Aristas**: `PolyLine` entre nodos; color según estado de la arista; tooltip con origen–destino y estado.
   - **Camiones**: marcador azul por camión; popup con id, ruta origen→destino y motivo de retraso.
   - **Rutas alternativas**: si un camión tiene `ruta_sugerida` con al menos 2 nodos, se dibuja una **línea azul discontinua** entre ellos.
3. **Muestra métricas**: “Paso simulación” (contador de pasos de 15 min) y tabla “PageRank - Nodos más críticos” (top 10).
4. **Leyenda**: colores por estado y significado de la línea azul discontinua.

Si **no hay datos en Cassandra**, la app usa nodos y camiones de ejemplo a partir de `config_nodos.py` para que el mapa sea visible igualmente.

**Arranque del dashboard:**

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate
streamlit run app_visualizacion.py
```

El botón **“Paso Siguiente (15 min)”** ejecuta la ingesta y el procesamiento Spark, actualiza Cassandra y recarga el mapa con los nuevos datos.

---

## Consultas en Hive (ejemplos)

Hive guarda el **histórico** en la base **`logistica_db`** (definida en `persistencia_hive.py`). Las tablas están particionadas por `anio_part` y `mes_part`. Puedes ejecutar HiveQL desde la consola `hive` o `beeline`, o desde Spark con `spark.sql()`.

### Tablas de histórico (logistica_db)

| Tabla | Contenido |
|-------|-----------|
| `eventos_historico` | Eventos por timestamp: tipo_evento, id_elemento, estado, motivo, pagerank, hub_asociado. |
| `clima_historico` | Clima por ciudad y fecha: temperatura, humedad, descripcion, visibilidad. |
| `tracking_camiones_historico` | Posiciones históricas de camiones: origen, destino, nodo_actual, progreso_pct. |
| `rutas_alternativas_historico` | Rutas originales vs alternativas: distancia, motivo_bloqueo, ahorro_km. |
| `agg_estadisticas_diarias` | Agregados diarios: tipo_evento, estado, motivo, contador, pct_total. |

### Cómo ejecutar consultas

**Consola Hive:**

```bash
hive
```

Dentro de Hive:

```sql
USE logistica_db;
SHOW TABLES;

-- Eventos por mes y estado
SELECT anio, mes, estado, COUNT(*) AS total
FROM eventos_historico
WHERE anio_part = 2025 AND mes_part = 3
GROUP BY anio, mes, estado
ORDER BY total DESC;

-- Clima histórico por ciudad
SELECT ciudad, anio, mes, AVG(temperatura) AS temp_media
FROM clima_historico
WHERE anio_part = 2025
GROUP BY ciudad, anio, mes;

-- Meses con más incidentes bloqueados
SELECT anio, mes, COUNT(*) AS eventos_bloqueados
FROM eventos_historico
WHERE estado = 'Bloqueado'
GROUP BY anio, mes
ORDER BY eventos_bloqueados DESC
LIMIT 10;
```

**Desde Spark (PySpark):**

```python
spark = SparkSession.builder.appName("HiveQuery").enableHiveSupport().getOrCreate()
spark.sql("USE logistica_db")
spark.sql("SELECT anio, mes, estado, COUNT(*) FROM eventos_historico GROUP BY anio, mes, estado").show()
```

Si las tablas no tienen datos, ejecuta antes el flujo que escribe en Hive (procesamiento Spark o `persistencia_hive.py`).

---

## Requisitos y arranque rápido

- **Python 3** con `venv` recomendado.
- **Servicios**: HDFS, Kafka (KRaft), Cassandra, Hive (opcional para histórico).
- **Spark 3.5** y JARs: GraphFrames, conector Spark-Cassandra, Kafka (según uso).

**Arranque de servicios con Airflow**: si tienes Airflow activo, puedes usar el DAG `dag_arranque_servicios` (ejecución manual) para levantar HDFS, Cassandra y Kafka antes de lanzar el pipeline. Configura `HADOOP_HOME` y `KAFKA_HOME` si no están en `/opt/hadoop` y `/opt/kafka`.

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

- **README_GEMELO_DIGITAL.md**: instrucciones de despliegue, arranque de servicios (Cassandra, Kafka), troubleshooting.
- **docs/AIRFLOW.md**: cómo arrancar Airflow (api-server + scheduler), qué hace cada DAG del proyecto y los que puedas tener en `~/airflow/dags`.
- **docs/FLUJO_DATOS_Y_REQUISITOS.md**: ejemplos GPS/clima/rutas, qué guarda Spark dónde, calidad de datos, consultas Hive.
- **docs/REQUIREMENTS_CHECKLIST.md**: cotejo con el PDF *Proyecto Big Data.pdf* (Fases I–IV, rúbrica).
- **docs/YARN_Y_SPARK.md**: cómo ejecutar Spark en YARN (`SPARK_MASTER=yarn`, `spark-submit --master yarn`).

### Cambios alineados al PDF (sin NiFi)

- **Kafka**: temas `transporte_raw` (crudos) y `transporte_filtered` (filtrados). Crear con `bash sql/crear_temas_kafka.sh`.
- **Structured Streaming**: `python procesamiento/streaming_ventanas_15min.py` (ventanas 15 min sobre `transporte_filtered`).
- **Enriquecimiento Hive**: tabla `nodos_maestro`; el procesamiento enriquece nodos con `hub` desde Hive.
- **DAG mensual**: `orquestacion/dag_mensual_retrain_limpieza.py` (re-entrenamiento + limpieza HDFS). Retención de backup: `DIAS_RETENCION_BACKUP` (por defecto 30); ruta: `HDFS_BACKUP_PATH`.
- **YARN**: `SPARK_MASTER=yarn` o `spark-submit --master yarn`.
- **HDFS**: La ingesta escribe el JSON raw en **`HDFS_BACKUP_PATH`** (misma ruta que lee Spark), así el procesamiento encuentra los ficheros sin configurar dos rutas.

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
