# Ejecución con Docker (Windows 11)

Este proyecto incluye un **docker-compose** con todo el stack: **HDFS**, **YARN**, **Cassandra**, **NiFi**, **Spark**, **Kafka** y **Hive**, más la aplicación (dashboard Streamlit).

---

## Construir la imagen de la app

La imagen **gemelo-logistico** (servicio `app`) se construye con el `Dockerfile` de la raíz del proyecto. Incluye:

- **Python 3.11** y dependencias de `requirements.txt` (Streamlit, Folium, Kafka, Cassandra, PySpark, etc.).
- **Java** (`default-jre-headless`) y **cliente Hadoop** (binarios `hdfs`) para que la ingesta pueda escribir en HDFS desde el contenedor.
- **Entrypoint** (`entrypoint.sh`): al arrancar el contenedor genera `core-site.xml` con `HDFS_NAMENODE` para que el comando `hdfs dfs` se conecte al namenode del compose. En Windows el script se normaliza a finales de línea LF en el build para evitar el error *"exec /entrypoint.sh: no such file or directory"*.

### Comandos para construir

Desde la raíz del proyecto:

```powershell
# Construir solo la imagen de la app
docker-compose build app

# O construir con docker directamente (misma imagen)
docker build -t gemelo-logistico:latest .
```

Si cambias el `Dockerfile` o el código, vuelve a construir antes de levantar:

```powershell
docker-compose build app
docker-compose up -d app
```

### Nota para Windows

- El archivo **`entrypoint.sh`** debe usar finales de línea **LF** (Unix). El Dockerfile aplica `sed -i 's/\r$//'` al copiarlo para convertir CRLF → LF. El `.gitattributes` del repo fuerza `*.sh` a LF para que no se corrompan al clonar o editar en Windows.

### Resumen de lo implementado (Docker)

| Elemento | Descripción |
|----------|-------------|
| **Dockerfile** | Imagen Python 3.11 + Java + cliente Hadoop (`hdfs`), entrypoint que genera `core-site.xml` desde `HDFS_NAMENODE`, normalización CRLF para Windows. |
| **docker-compose.yml** | Stack: HDFS (namenode, datanode), YARN (resourcemanager, nodemanager, historyserver), Kafka (apache/kafka, KRaft), Cassandra, Hive (metastore + server), NiFi, Spark (master + worker), servicio `app` (Streamlit). |
| **entrypoint.sh** | Script de arranque que escribe la config HDFS y ejecuta el comando (p. ej. Streamlit). |
| **hadoop.env** | Variables para los contenedores Hadoop (fs.defaultFS, YARN, etc.). |
| **.dockerignore** | Excluye venv, `__pycache__`, logs y datos locales del contexto de build. |
| **.gitattributes** | Fuerza LF en `*.sh` para evitar errores en contenedores Linux. |
| **ingesta** | Usa `HADOOP_HOME/bin/hdfs` si existe para que el comando `hdfs` se encuentre dentro del contenedor. |

---

## Requisitos

- **Docker Desktop** para Windows 11 con WSL2 recomendado.
- Recursos: se recomienda **8 GB+ RAM** para levantar todos los servicios. Puedes subir solo los que necesites.

## Servicios incluidos

| Servicio        | Puerto(s)      | Descripción                    |
|-----------------|----------------|---------------------------------|
| **HDFS Namenode** | 9870, 9000    | HDFS + Web UI                  |
| **HDFS Datanode** | (interno)     | Almacenamiento HDFS           |
| **YARN ResourceManager** | 8088  | Orquestación de jobs          |
| **YARN NodeManager**     | (interno)     | Ejecución de contenedores     |
| **History Server**      | 8188          | Historial de jobs YARN        |
| **Kafka**       | 9092           | Mensajería (modo KRaft)       |
| **Cassandra**   | 9042           | Base de datos estado actual   |
| **Hive Metastore** | 9083        | Metadatos Hive                |
| **HiveServer2** | 10000          | Consultas Hive                |
| **NiFi**        | 8081           | Flujos de datos (UI en 8081 para evitar conflicto con 8080) |
| **Spark Master**| 18080, 7077    | Cluster Spark (UI en 18080)    |
| **Spark Worker**| (interno)      | Workers Spark                 |
| **App (Streamlit)** | 8501        | Dashboard del gemelo digital  |

## Uso rápido

### 1. Crear archivo de entorno (opcional)

Crea un `.env` en la raíz del proyecto si quieres pasar la API key de OpenWeatherMap:

```env
API_WEATHER_KEY=tu_api_key_aqui
```

### 2. Levantar todo el stack

```powershell
cd C:\Users\chuwi\Documents\proyecto_transporte_global_standalone
docker-compose up -d
```

Para ver logs en primer plano:

```powershell
docker-compose up
```

### 3. Inicializar Cassandra (una sola vez)

Cuando Cassandra esté en marcha (espera ~30–60 s), crea el keyspace y las tablas:

```powershell
docker cp cassandra/esquema_logistica.cql cassandra:/tmp/esquema.cql
docker exec -it cassandra cqlsh -f /tmp/esquema.cql
```

### 4. Crear temas en Kafka (opcional)

Si quieres los temas del proyecto (imagen **apache/kafka**):

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic transporte_raw --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic transporte_filtered --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
```

### 5. Acceder al dashboard

Abre en el navegador:

- **Dashboard**: http://localhost:8501

Otras UIs útiles:

- HDFS Namenode: http://localhost:9870
- YARN ResourceManager: http://localhost:8088
- NiFi: http://localhost:8081/nifi
- Spark Master: http://localhost:18080

## Levantar solo parte del stack

Si no quieres levantar todos los servicios (por RAM o tiempo):

```powershell
# Solo app + Cassandra + Kafka (mínimo para dashboard y datos en tiempo real)
docker-compose up -d cassandra kafka app

# App + Cassandra + Kafka + HDFS (para ingesta con backup HDFS)
docker-compose up -d namenode datanode cassandra kafka app
```

La app usa variables de entorno para conectar a cada servicio; si un servicio no está, esa parte fallará pero el resto puede seguir funcionando (por ejemplo, el mapa con datos por defecto si Cassandra no tiene tablas).

## Variables de entorno de la app

En `docker-compose.yml` la app ya tiene definidas:

- `KAFKA_BOOTSTRAP=kafka:9092`
- `CASSANDRA_HOST=cassandra`
- `HDFS_NAMENODE=namenode:9000`
- `HIVE_METASTORE_URIS=thrift://hive-metastore:9083`
- `HIVE_SERVER=hive-server:10000`
- `NIFI_URL=http://nifi:8080/nifi`
- `SPARK_MASTER=spark://spark-master:7077`

Puedes sobreescribirlas en un `.env` o con `environment` en tu propio compose.

## Solución de problemas

- **Cassandra tarda en arrancar**: espera 1–2 minutos y vuelve a ejecutar el `cqlsh -f` del esquema.
- **“Connection refused” a Kafka/Cassandra**: asegúrate de que los contenedores están `Up` con `docker-compose ps` y que la app depende de ellos en el compose.
- **HDFS desde la app**: la imagen de la app incluye cliente Hadoop; el `entrypoint.sh` genera `core-site.xml` con `HDFS_NAMENODE`. Si no usas HDFS, puedes dejar de levantar namenode/datanode.
- **Memoria**: si Docker Desktop se queda corto, aumenta la RAM en *Settings → Resources* o levanta menos servicios.

## Parar y eliminar volúmenes

```powershell
docker-compose down
# Eliminar datos (HDFS, Cassandra, Kafka, etc.)
docker-compose down -v
```

---

## Subir el proyecto a GitHub

Con los cambios documentados (Docker, Dockerfile, docker-compose, guías) ya en tu copia local:

```powershell
cd C:\Users\chuwi\Documents\proyecto_transporte_global_standalone

# Ver qué archivos han cambiado
git status

# Añadir los archivos que quieras subir (ejemplo: todo lo tocado para Docker)
git add Dockerfile docker-compose.yml entrypoint.sh hadoop.env .dockerignore .gitattributes
git add DOCKER.md GUIA_PRACTICA_DOCKER.md config.py ingesta/ingesta_kdd.py
git add README.md
# O añadir todo: git add .

# Crear un commit
git commit -m "Docker: stack completo (HDFS, YARN, Kafka, Cassandra, NiFi, Spark, Hive), guía práctica y construcción de imagen app"

# Si aún no tienes remote:
# git remote add origin https://github.com/TU_USUARIO/proyecto_transporte_global_standalone.git

# Subir a la rama main (o la que uses)
git push -u origin main
```

Si el repositorio ya existe en GitHub (por ejemplo `https://github.com/gracobjo/proyecto_transporte_global_standalone.git`), basta con `git push origin main` después del commit.
