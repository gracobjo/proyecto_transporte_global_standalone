# Airflow: arranque y DAGs

Documentación de cómo arrancar Apache Airflow en este proyecto y qué hace cada DAG.

---

## Cómo arrancar Airflow

En este proyecto Airflow está instalado en el **entorno virtual** del proyecto. Hay que levantar **dos procesos**: el servidor de API (interfaz web + API para los workers) y el scheduler.

### Requisitos previos

- Variable de entorno `AIRFLOW_HOME` (por defecto suele ser `~/airflow`).
- Carpeta de DAGs configurada en `AIRFLOW_HOME/dags` o enlazada al proyecto (ver más abajo).

### 1. Activar entorno y variables

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate
export AIRFLOW_HOME=~/airflow
```

### 2. Arrancar el servidor de API (interfaz web + API)

En versiones recientes de Airflow el comando `webserver` fue sustituido por **`api-server`**:

```bash
airflow api-server -p 8080
```

O escuchando en todas las interfaces:

```bash
airflow api-server -H 0.0.0.0 -p 8080
```

**Importante:** Este proceso debe estar en marcha para que el **scheduler** pueda ejecutar tareas. Los workers del LocalExecutor se conectan al API en `http://localhost:8080`; si el api-server no está levantado, las tareas fallan con *Connection refused*.

### 3. Arrancar el scheduler (en otra terminal)

En una **segunda terminal**:

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate
export AIRFLOW_HOME=~/airflow
airflow scheduler
```

### 4. Acceder a la interfaz

- **URL:** http://localhost:8080  
- **Usuario:** el configurado al crear el usuario (por ejemplo `admin`).  
- Si es la primera vez: `airflow users create --role Admin --username admin --email admin@localhost --firstname Admin --lastname User --password <contraseña>`.

### Resumen de comandos

| Proceso      | Comando                      | Puerto |
|-------------|------------------------------|--------|
| API/Web     | `airflow api-server -p 8080` | 8080   |
| Scheduler   | `airflow scheduler`          | —      |

Ambos deben estar en ejecución para que los DAGs se ejecuten correctamente.

---

## DAGs en el repositorio (`orquestacion/`)

Estos DAGs están en la carpeta **`orquestacion/`** del proyecto. Para que Airflow los cargue puedes:

- Enlazar cada DAG en `~/airflow/dags/`, o  
- Configurar en `~/airflow/airflow.cfg`: `dags_folder = /home/hadoop/proyecto_transporte_global/orquestacion`

### 1. `dag_arranque_servicios` (`dag_arranque_servicios.py`)

- **Propósito:** Levantar los servicios necesarios antes del pipeline (HDFS, Cassandra, Kafka).
- **Ejecución:** Manual (Trigger DAG). No tiene schedule.
- **Tareas:**
  - **arrancar_hdfs:** Si el NameNode (puerto 9870) no responde, ejecuta `start-dfs.sh` (usa `HADOOP_HOME` si está definido).
  - **arrancar_cassandra:** Si el puerto 9042 no está en uso, lanza `cassandra/bin/cassandra` del proyecto en segundo plano.
  - **arrancar_kafka:** Si el puerto 9092 no está en uso, arranca el broker Kafka (usa `KAFKA_HOME`, por defecto `/opt/kafka`).
- Las tres tareas se ejecutan en paralelo. Si un servicio ya está activo, la tarea no hace nada.

### 2. `dag_maestro_transporte` (`dag_maestro.py`)

- **Propósito:** Pipeline principal cada 15 minutos: verificar servicios y ejecutar Ingesta → Procesamiento.
- **Schedule:** Cada 15 minutos (`timedelta(minutes=15)`).
- **Tareas:**
  - **verificar_hdfs:** Comprueba que HDFS responda (`hdfs dfs -ls /`).
  - **verificar_kafka:** Comprueba que Kafka esté escuchando en `localhost:9092`.
  - **verificar_cassandra:** Comprueba que Cassandra esté escuchando en `127.0.0.1:9042`.
  - **ejecutar_ingesta:** Ejecuta `ingesta_kdd.py` con `PASO_15MIN` según la hora.
  - **ejecutar_procesamiento:** Ejecuta `procesamiento_grafos.py` (Spark + GraphFrames).
- **Dependencias:** Las tres verificaciones en paralelo → ingesta → procesamiento. `max_active_runs=1` para no saturar RAM.

### 3. `dag_mensual_retrain_limpieza` (`dag_mensual_retrain_limpieza.py`)

- **Propósito:** Mantenimiento mensual: limpieza de HDFS y re-entrenamiento del modelo de grafos (alineado con requisitos del proyecto).
- **Schedule:** El día 1 de cada mes a las 00:00.
- **Tareas:**
  - **limpiar_hdfs_temporales:** Elimina en HDFS los JSON de backup en `HDFS_BACKUP_PATH` más antiguos que `DIAS_RETENCION_BACKUP` (por defecto 30 días).
  - **reentrenar_grafos:** Ejecuta `procesamiento/procesamiento_grafos.py` como batch completo (re-entrenamiento).
- **Configuración:** `DIAS_RETENCION_BACKUP` y `HDFS_BACKUP_PATH` por variable de entorno o en el DAG.

---

## DAGs en la instalación local (`~/airflow/dags`)

Si en tu máquina la carpeta de DAGs es `~/airflow/dags`, pueden existir DAGs adicionales (copias o variantes) que no están en el repositorio. Ejemplos típicos:

### `pipeline_transporte_global` (ej. `dag_transporte.py`)

- **Propósito:** Pipeline modular KDD: verificación de infraestructura (HDFS, YARN, Kafka, Cassandra), minería de grafos y generación de reporte.
- **Schedule:** Manual (`schedule=None`).
- **Tareas típicas:**
  - **verificar_hdfs:** Arranca HDFS si el puerto 9000 no responde.
  - **verificar_yarn:** Arranca YARN si el ResourceManager (8032) no está activo.
  - **verificar_kafka:** Comprueba Kafka en 9092 (falla si no está activo).
  - **verificar_cassandra:** Arranca Cassandra del proyecto si no está en marcha y espera al puerto 9042.
  - **ejecutar_mineria_grafos:** `spark-submit` con YARN del script de análisis de grafos (GraphFrames, Cassandra).
  - **generar_reporte_inteligente:** Exporta datos desde Cassandra a CSV y añade nivel de riesgo.
- **Dependencias:** [verificar_hdfs, verificar_yarn, verificar_kafka, verificar_cassandra] → ejecutar_mineria_grafos → generar_reporte_inteligente.

### `gemelo_digital_setup` (ej. `dag_setup_gemelo.py`)

- **Propósito:** Setup inicial (ejecutar una vez o bajo demanda): crear esquema Cassandra, esquema Hive y topic Kafka.
- **Tareas:** setup_cassandra → setup_hive y setup_kafka_topic (en paralelo).

### `dag_maestro_gemelo` (ej. `dag_maestro_gemelo.py`)

- **Propósito:** Similar al `dag_maestro_transporte` del repo: verificación de HDFS, Kafka, Cassandra y Hive, luego ingesta y procesamiento de grafos cada 15 minutos.

## Problemas típicos y solución rápida

- **No aparecen todos los DAGs en la UI o en `airflow dags list`**:
  1. Verifica que `AIRFLOW_HOME` apunta a `~/airflow` y que los ficheros `dag_*.py` están en `~/airflow/dags`.
  2. Asegúrate de que **no hay enlaces simbólicos recursivos** (por ejemplo `orquestacion -> /home/hadoop/proyecto_transporte_global/orquestacion` dentro de esa misma carpeta).
  3. Desde el proyecto, con el entorno virtual activado, vuelve a serializar los DAGs en la base de datos:

     ```bash
n### Arranque de Airflow con Docker (opcional)

En máquinas justas de recursos se puede levantar solo Airflow en Docker, manteniendo HDFS/Kafka/Cassandra en el host o en otros contenedores.

1. Construir y arrancar el servicio de Airflow:

   ```bash
   cd ~/proyecto_transporte_global
   docker compose -f docker-compose.airflow.yml up --build
   ```

2. Acceder a la interfaz web/API:

   - URL: http://localhost:8080
   - Usuario: `admin`
   - Contraseña: `admin` (creada automáticamente si no existe).

3. Detener el contenedor cuando no sea necesario:

   ```bash
   docker compose -f docker-compose.airflow.yml down
   ```

Los DAGs se leen desde la carpeta `orquestacion/` del proyecto, montada dentro del contenedor en `/opt/airflow/dags`.

     cd ~/proyecto_transporte_global
     source venv_transporte/bin/activate
     export AIRFLOW_HOME=~/airflow
     airflow dags reserialize
     ```

  4. Espera unos segundos, ejecuta `airflow dags list` y refresca la web de Airflow (F5).

- **Quiero ver los logs del api-server en la terminal**:
  - Lanza el servidor **sin `-D`** para que no vaya a segundo plano:

    ```bash
    airflow api-server -H 0.0.0.0 -p 8080
    ```

    Esa terminal quedará ocupada mostrando los logs hasta que pulses `Ctrl+C`.

---

## Enlaces rápidos

- Interfaz web: http://localhost:8080  
- DAGs del proyecto: `orquestacion/dag_*.py`  
- Configuración Airflow: `$AIRFLOW_HOME/airflow.cfg`  
- Logs: `$AIRFLOW_HOME/logs/`
