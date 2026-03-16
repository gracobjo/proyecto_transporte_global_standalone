# Guía: seguir la práctica del PDF "Proyecto Big Data" con Docker

Esta guía enlaza el enunciado **Proyecto Big Data.pdf** con el stack Docker del proyecto para que puedas desarrollar la práctica en Windows 11.

---

## Correspondencia PDF ↔ Docker

| Requisito PDF (Stack Apache 2026) | En este Docker |
|-----------------------------------|----------------|
| **Fase I** – Ingesta (NiFi + Kafka, 2 temas, HDFS) | NiFi (puerto 8081), Kafka (KRaft), HDFS (namenode/datanode). Ingesta Python publica en Kafka y escribe en HDFS. |
| **Fase II** – Preprocesamiento (Spark SQL, Hive, GraphFrames) | Spark (master + worker), Hive (metastore + server). Procesamiento con GraphFrames y enriquecimiento desde Hive. |
| **Fase III** – Minería (Structured Streaming 15 min, Cassandra + Hive) | Cassandra (estado actual), Hive (histórico). Scripts de streaming con ventanas 15 min. |
| **Fase IV** – Orquestación (Airflow) | DAG en código (`orquestacion/`); Airflow no está en el `docker-compose` (opcional añadirlo). |
| **YARN** | ResourceManager + NodeManager en el compose; Spark puede usarse en `local` o apuntando a YARN. |

---

## Orden recomendado para la práctica

### 1. Levantar el stack

```powershell
cd C:\Users\chuwi\Documents\proyecto_transporte_global_standalone
docker-compose up -d
```

Espera 1–2 minutos a que Cassandra y Hive estén listos. Comprueba:

```powershell
docker-compose ps
```

---

### 2. Inicializar Cassandra (Fase III – persistencia)

Crear keyspace y tablas del gemelo digital:

```powershell
docker cp cassandra/esquema_logistica.cql cassandra:/tmp/esquema.cql
docker exec -it cassandra cqlsh -f /tmp/esquema.cql
```

Verificar:

```powershell
docker exec -it cassandra cqlsh -e "USE logistica_espana; DESCRIBE TABLES;"
```

---

### 3. Crear temas Kafka (Fase I – Datos Crudos y Datos Filtrados)

El PDF pide dos temas: **Datos Crudos** y **Datos Filtrados**. En el proyecto son `transporte_raw` y `transporte_filtered`:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic transporte_raw --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic transporte_filtered --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
```

Listar temas:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

### 4. (Opcional) Inicializar Hive y tabla maestra (Fase II – enriquecimiento)

Si el PDF pide datos maestros en Hive (p. ej. `nodos_maestro`), puedes crearlos desde el contenedor de Hive o desde un job Spark que use `enableHiveSupport()`. El procesamiento ya tiene `enriquecer_desde_hive()`. Para crear tablas Hive manualmente:

```powershell
docker exec -it hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "CREATE DATABASE IF NOT EXISTS logistica_espana; USE logistica_espana;"
```

---

### 5. Ejecutar la ingesta (Fase I)

La **ingesta** obtiene clima (API), simula nodos/aristas/camiones y publica en Kafka + HDFS. Desde el contenedor de la app:

```powershell
docker exec -it gemelo-app python -c "
import os
os.environ.setdefault('KAFKA_BOOTSTRAP', 'kafka:9092')
os.environ.setdefault('CASSANDRA_HOST', 'cassandra')
os.environ.setdefault('HDFS_NAMENODE', 'namenode:9000')
import sys
sys.path.insert(0, '/app')
from ingesta.ingesta_kdd import main
main(paso_15min=0)
"
```

O, si tu ingesta está en la raíz:

```powershell
docker exec -it gemelo-app python /app/ingesta/ingesta_kdd.py
```

La variable `API_WEATHER_KEY` debe estar definida en el `docker-compose` (o en `.env`) para que la consulta al tiempo funcione.

---

### 6. Ejecutar el procesamiento (Fases II y III)

Procesamiento con **Spark** (grafos, autosanación, persistencia en Cassandra y Hive). Dentro del contenedor la app usa Spark en `local` por defecto; para usar el cluster Spark del compose, configura `SPARK_MASTER=spark://spark-master:7077`:

```powershell
docker exec -e SPARK_MASTER=spark://spark-master:7077 -it gemelo-app python /app/procesamiento/procesamiento_grafos.py
```

O sin variable (Spark local dentro del contenedor):

```powershell
docker exec -it gemelo-app python /app/procesamiento/procesamiento_grafos.py
```

---

### 7. Dashboard (visualización)

Abre en el navegador:

- **Dashboard del gemelo digital:** http://localhost:8501

Desde el dashboard puedes usar **“Paso Siguiente (15 min)”** para lanzar ingesta + procesamiento y ver el mapa actualizado con datos de Cassandra.

---

### 8. Structured Streaming – ventanas 15 min (Fase III)

El PDF pide **Structured Streaming** con ventanas de 15 minutos. Script del proyecto:

```powershell
docker exec -it gemelo-app python /app/procesamiento/streaming_ventanas_15min.py
```

Para ejecutarlo contra el Kafka del compose, el script debe usar `KAFKA_BOOTSTRAP=kafka:9092` (ya configurado en el servicio `app`).

---

### 9. NiFi (Fase I – si el PDF exige NiFi)

- **URL NiFi:** http://localhost:8081/nifi  
- Puedes diseñar flujos que consuman API (p. ej. OpenWeather), generen o lean “logs GPS” y publiquen en Kafka (`transporte_raw` / `transporte_filtered`) o escriban en HDFS. La ingesta actual en Python puede coexistir o sustituirse según lo que pida el enunciado.

---

### 10. YARN (gestión de recursos según PDF)

El compose incluye **YARN** (resourcemanager, nodemanager). Para que el procesamiento Spark use YARN en lugar de `local` o del Spark standalone del compose:

- En el contenedor `gemelo-app` necesitas tener `HADOOP_CONF_DIR` o variables que apunten al resourcemanager (p. ej. desde un volumen o generadas a partir del servicio `resourcemanager`).
- Alternativa: ejecutar `spark-submit` desde un contenedor que tenga cliente Hadoop/Spark configurado para YARN, apuntando a `resourcemanager:8088`.

Mientras tanto, usar **Spark en local** o **Spark standalone** (spark-master:7077) cumple con la parte de “procesamiento con Spark” del enunciado.

---

### 11. Orquestación – DAG mensual (Fase IV)

El PDF pide un DAG de **re-entrenamiento mensual** y **limpieza de tablas temporales en HDFS**:

- Código: `orquestacion/dag_mensual_retrain_limpieza.py`
- **Airflow** no está en el `docker-compose`. Para seguir la práctica al 100% puedes:
  - Añadir un servicio Airflow al `docker-compose` y registrar ahí el DAG, o
  - Ejecutar manualmente el flujo del DAG (limpieza HDFS + re-entrenamiento) cuando lo pida el enunciado.

---

## Resumen de URLs útiles (Docker)

| Servicio        | URL / Acceso |
|-----------------|--------------|
| Dashboard app   | http://localhost:8501 |
| HDFS Namenode  | http://localhost:9870 |
| YARN RM        | http://localhost:8088 |
| NiFi           | http://localhost:8081/nifi |
| Spark Master   | http://localhost:18080 |
| Kafka          | `localhost:9092` (bootstrap) |

---

## Cotejo con el PDF

- **Checklist detallado** (qué pide el PDF y qué cubre el proyecto): ver **`docs/REQUIREMENTS_CHECKLIST.md`**.
- **Flujo de datos y requisitos**: **`docs/FLUJO_DATOS_Y_REQUISITOS.md`**.
- **YARN y Spark**: **`docs/YARN_Y_SPARK.md`**.

Con esta guía puedes seguir la práctica del **Proyecto Big Data.pdf** usando el mismo stack (HDFS, YARN, Kafka, NiFi, Spark, Cassandra, Hive) en Docker.
