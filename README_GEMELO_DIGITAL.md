# Sistema de Gemelo Digital Logístico - España

Stack Apache 2026: HDFS, Kafka (KRaft), Spark 3.5 (GraphFrames), Hive, Cassandra, Airflow.

## Arranque de servicios

### Cassandra

```bash
~/proyecto_transporte_global/cassandra/bin/cassandra
```

O desde el proyecto:
```bash
cd ~/proyecto_transporte_global
./cassandra/bin/cassandra
```

Esperar 30-60 segundos. Para comprobar que está levantada:

```bash
nc -z 127.0.0.1 9042 && echo "Cassandra OK"
```

O con `cqlsh`:
```bash
cqlsh -e "DESCRIBE KEYSPACES;"
```

---

## Instrucciones de primer despliegue

### 1. Esquema Cassandra

```bash
cd ~/proyecto_transporte_global
cqlsh -f cassandra/esquema_logistica.cql
```

Verifica que las tablas usan tipos `float` y `timestamp` correctamente.

### 2. Configuración de JARs

Los paths de GraphFrames y Cassandra Connector son configurables vía variables de entorno:

```bash
export JAR_GRAPHFRAMES=/home/hadoop/proyecto_transporte_global/herramientas/graphframes-0.8.3-spark3.5-s_2.12.jar
export JAR_CASSANDRA=/home/hadoop/.ivy2/cache/com.datastax.spark/spark-cassandra-connector_2.12/jars/spark-cassandra-connector_2.12-3.5.0.jar
```

O edita `config.py` según tu instalación.

### 3. Crear topic Kafka

```bash
kafka-topics.sh --create --topic transporte_status --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
```

### 4. Dependencias Python

```bash
source venv_transporte/bin/activate
pip install -r requirements.txt
```

### 5. Ejecutar flujo manual

```bash
# Ingesta (paso 0)
PASO_15MIN=0 python ingesta/ingesta_kdd.py

# Procesamiento Spark (cierra con spark.stop() al final)
python procesamiento/procesamiento_grafos.py
```

### 5b. Verificación tras el procesamiento

Comprobar que Cassandra tiene datos (con Cassandra en marcha):

```bash
# Contar registros en cada tabla
cqlsh -e "USE logistica_espana; SELECT COUNT(*) FROM nodos_estado;"
cqlsh -e "USE logistica_espana; SELECT COUNT(*) FROM aristas_estado;"
cqlsh -e "USE logistica_espana; SELECT COUNT(*) FROM tracking_camiones;"
cqlsh -e "USE logistica_espana; SELECT COUNT(*) FROM pagerank_nodos;"

# Ver una muestra de nodos (estado, clima)
cqlsh -e "USE logistica_espana; SELECT id_nodo, tipo, estado, clima_actual, temperatura FROM nodos_estado LIMIT 10;"

# Ver camiones y rutas sugeridas
cqlsh -e "USE logistica_espana; SELECT id_camion, lat, lon, ruta_origen, ruta_destino, estado_ruta FROM tracking_camiones;"

# Ver PageRank (nodos más críticos)
cqlsh -e "USE logistica_espana; SELECT id_nodo, pagerank FROM pagerank_nodos LIMIT 10;"
```

O abrir el **dashboard Streamlit** (ver siguiente apartado): muestra el mapa con nodos, aristas y camiones a partir de estos datos.

### 6. Dashboard Streamlit

```bash
streamlit run app_visualizacion.py
```

El botón "Paso Siguiente (15 min)" dispara ingesta + procesamiento. Usa `st.session_state` para la línea de tiempo.

### 7. Airflow DAG

Copiar `orquestacion/dag_maestro.py` al directorio `dags/` de Airflow. El DAG:
- Verifica HDFS, Kafka, Cassandra
- Ejecuta Ingesta → Procesamiento (sin solapamiento, `max_active_runs=1`)

## Estructura

| Archivo | Descripción |
|---------|-------------|
| `config_nodos.py` | Topología: 5 hubs, 25 secundarios, aristas |
| `config.py` | JARs configurables, API keys |
| `ingesta/ingesta_kdd.py` | API clima, simulación, GPS 15min, Kafka+HDFS |
| `procesamiento/procesamiento_grafos.py` | GraphFrames, autosanación, Cassandra+Hive |
| `app_visualizacion.py` | Streamlit + Folium |
| `orquestacion/dag_maestro.py` | DAG Airflow cada 15 min |
| `cassandra/esquema_logistica.cql` | Esquema Cassandra |

## Restricciones (4GB RAM)

- Spark con `local[*]`, `spark.stop()` al final
- `max_active_runs=1` en Airflow

---

## Troubleshooting: Kafka KRaft

### Problema 1: `NoSuchFileException: server.properties`

**Causa:** Al ejecutar desde `/opt/kafka/bin`, el script busca `server.properties` en el directorio actual. El fichero está en `/opt/kafka/config/`.

**Solución:**
```bash
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
```

---

### Problema 2: `ConfigException: You must set node.id to the same value as broker.id`

**Causa:** En modo KRaft, `node.id` y `broker.id` deben ser iguales. El config tenía `node.id=1` y `broker.id=0`.

**Solución:** Editar `server.properties` y cambiar `broker.id=0` a `broker.id=1`.

---

### Problema 3: `RuntimeException: No readable meta.properties files found`

**Causa:** Kafka KRaft requiere `meta.properties` en el directorio de logs del broker (`log.dirs`). El controller usa `kraft-data/`, pero el broker usa `log.dirs` (ej. `/opt/kafka/logs`), donde faltaba el fichero.

**Solución:** Crear `/opt/kafka/logs/meta.properties` con el mismo `cluster.id` que en `kraft-data/meta.properties`:

```properties
version=1
node.id=1
cluster.id=VJj3m6mKTgWRMQ6MIpY08Q
```

Obtener el `cluster.id` con:
```bash
cat /opt/kafka/kraft-data/meta.properties
```

**Alternativa:** Si el cluster se formateó con otro config, arrancar con la config Kraft original:
```bash
/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
```

---

## Troubleshooting: Cassandra "Some operations timed out"

Si en `cassandra/logs/system.log` aparece:

```text
WARN  [ScheduledTasks:1] ... NoSpamLogger - Some operations timed out, details available at debug level (debug.log)
```

**Causa:** Con poco RAM (p. ej. 4 GB), Cassandra puede estar ocupada (compactions, flushes, redistribución de index summaries) y tarda más de 5 s en responder. El conector Spark usa por defecto 5 s y falla.

**Qué hacer:**

1. **Timeouts ya aumentados** en `procesamiento_grafos.py`: conexión 30 s, consulta y lectura 60 s. Vuelve a ejecutar el procesamiento.
2. **Ver el detalle** del timeout en Cassandra:
   ```bash
   tail -200 ~/proyecto_transporte_global/cassandra/logs/debug.log
   ```
3. **Ejecutar cuando el nodo esté más libre**: no lanzar ingesta + procesamiento justo después de arrancar muchos servicios; esperar 1–2 minutos tras levantar Cassandra.
4. **Si sigue fallando:** reducir carga de Cassandra (en `cassandra/conf/cassandra.yaml` bajar `compaction_throughput_mb_per_sec` o desactivar compactions automáticas temporalmente) o dar más heap a Cassandra si hay RAM disponible.
