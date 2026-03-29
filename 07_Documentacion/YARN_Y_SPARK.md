# Ejecutar Spark en YARN (según PDF Proyecto Big Data)

El enunciado exige **Gestión de Recursos: YARN**. Por defecto el proyecto usa `master("local")` para entornos con poca RAM. Para cumplir el requisito en un cluster con YARN:

---

## 1. Requisitos

- Hadoop con **YARN** activo (`start-yarn.sh` o servicios iniciados).
- Variables de entorno: `HADOOP_CONF_DIR` (o `YARN_CONF_DIR`) apuntando al `etc/hadoop` del cluster.

---

## 2. Procesamiento de grafos en YARN (modo cluster)

```bash
cd ~/proyecto_transporte_global
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop   # Ajustar a tu instalación

$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --num-executors 2 \
  --jars "$(python3 -c "
from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config import JAR_GRAPHFRAMES, JAR_CASSANDRA
print(f'{JAR_GRAPHFRAMES},{JAR_CASSANDRA}')
")" \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  procesamiento/procesamiento_grafos.py
```

**Modo cluster** (driver en el cluster):

```bash
spark-submit --master yarn --deploy-mode cluster \
  --driver-memory 512m --executor-memory 512m \
  --py-files config.py,config_nodos.py \
  procesamiento/procesamiento_grafos.py
```

En `deploy-mode cluster` los módulos locales (`config`, `config_nodos`) deben ir en `--py-files` o empaquetados.

---

## 3. Structured Streaming en YARN

```bash
spark-submit --master yarn --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  procesamiento/streaming_ventanas_15min.py --master yarn
```

El script acepta `--master yarn` para que la sesión Spark use YARN como master.

---

## 4. Forzar YARN desde el código (opcional)

Para que el job use YARN sin cambiar la línea de comandos, se puede leer una variable de entorno:

```python
# En crear_spark() de procesamiento_grafos.py
import os
master = os.environ.get("SPARK_MASTER", "local")
spark = SparkSession.builder.appName("...").master(master)...
```

Y ejecutar:

```bash
export SPARK_MASTER=yarn
python procesamiento/procesamiento_grafos.py
```

---

## 5. Comprobar YARN

```bash
yarn application -list
yarn logs -applicationId <application_id>
```

Si el cluster no tiene YARN o tiene poca capacidad, mantener `master("local")` es válido para desarrollo; en la memoria del proyecto se documenta la opción YARN para cumplir el requisito cuando el entorno lo permita.
