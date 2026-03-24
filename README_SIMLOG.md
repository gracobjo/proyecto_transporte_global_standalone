# SIMLOG España — Despliegue y operación

**SIMLOG** (*Sistema Integrado de Monitorización y Simulación Logística*): simulación de red de transporte, clima y riesgos en España con stack Apache 2026: HDFS, Kafka (KRaft), Spark 3.5 (GraphFrames), Hive, Cassandra, Airflow.

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
cd ~/proyecto_transporte_global && source venv_transporte/bin/activate
streamlit run app_visualizacion.py
```

La interfaz está organizada en **tres pestañas** alineadas con el ciclo KDD:

| Pestaña | Contenido |
|---------|-----------|
| **Ciclo KDD** | Las 5 fases (selección → interpretación) con actividades, entradas/salidas y scripts. |
| **Mapa y métricas** | Folium (nodos, aristas, camiones) y PageRank desde Cassandra. |
| **Verificación técnica** | Comprobaciones HDFS, Kafka y Cassandra (`config.py`: rutas y topic). |
| **Cuadro de mando** | Operaciones por fase KDD, consultas supervisadas a **Cassandra** (operativo) y **Hive** (histórico vía `beeline`), y **slides** de anticipación de retrasos (OpenWeather + nodos en Cassandra). |
| **Rutas híbridas** | Formulario origen/destino (hub o secundario), camino mínimo en **saltos** (BFS), retrasos y coste desde Cassandra + OpenWeather, vehículos afectados y **rutas alternativas** si cae un nodo/arista (clima/obras). |
| **Servicios** | Botones **Iniciar**, **Comprobar** y **Parar** (con confirmación) para HDFS, Kafka, Cassandra, Spark (master opcional), HiveServer2, Airflow (api-server) y NiFi. Puertos configurables con `SIMLOG_PORT_*`. |

En la **barra lateral**: estado de servicios, `paso_15min` (simulación), botones para ejecutar solo ingesta, solo Spark o **pipeline completo** (ingesta con el paso actual + procesamiento; al terminar bien, incrementa el paso). `st.session_state` guarda la línea temporal de ejecuciones.

**Cuadro de mando:** pestaña dedicada con tareas de supervisión por fase, botones de ingesta/Spark, selector de consultas CQL/Hive (solo plantillas aprobadas) y un carrusel de **slides** por hub que estima márgenes de retraso por clima (tormenta, nieve, niebla, lluvia, viento, visibilidad) y por estado de nodos (atascos, obras, bloqueos). Hive requiere HiveServer2; configurar `HIVE_JDBC_URL` y `HIVE_BEELINE_BIN` si tu instalación no es la estándar.

**Servicios / UIs:** en la pestaña **Servicios**, cada bloque incluye **Interfaz web** (URL, puerto, usuario y contraseña desde variables `SIMLOG_UI_*`). En la barra lateral, expander **Interfaces web del stack**. Plantilla: `simlog_ui.env.example`.

### 6a. API REST para otras aplicaciones (Swagger / OpenAPI)

Servicio **FastAPI** con documentación interactiva: topología, estado de infraestructura, datos en Cassandra, clima por hubs y fases KDD (solo lectura).

```bash
cd ~/proyecto_transporte_global && source venv_transporte/bin/activate
uvicorn servicios.api_simlog:app --host 0.0.0.0 --port 8090
```

- **Swagger UI:** http://localhost:8090/docs  
- **OpenAPI JSON:** http://localhost:8090/openapi.json  

Guía: [`servicios/README_SERVICIOS_API.md`](servicios/README_SERVICIOS_API.md). Código: `servicios/api_simlog.py`; lógica compartida con el dashboard: `servicios/estado_y_datos.py`.

### 6b. Apache NiFi (ingesta alternativa al script)

Incluye el **grupo de procesadores** documentado (GPS sintético, `InvokeHTTP` OpenWeather, Kafka, HDFS, disparo Spark/YARN, Hive/Cassandra alineados con el PDF):

- Guía principal: [`nifi/README_NIFI.md`](nifi/README_NIFI.md)
- Especificación de flujo: [`nifi/flow/simlog_kdd_flow_spec.yaml`](nifi/flow/simlog_kdd_flow_spec.yaml)
- Montaje en la UI: [`nifi/flow/MONTAJE_UI_NIFI.md`](nifi/flow/MONTAJE_UI_NIFI.md)

Parámetros de ejemplo: `nifi/parameter-context.env.example`. Script Groovy: `nifi/groovy/GenerateSyntheticPayload.groovy`. `spark-submit` vía NiFi: `nifi/scripts/spark_submit_yarn.sh` (ajustar `SPARK_MASTER=local` si no hay YARN).

### 7. Airflow — flujo por fases KDD (recomendado)

**Un DAG por fase KDD**, encadenados en secuencia (`TriggerDagRunOperator`): arranque de servicios (fase 0), selección, preprocesamiento, transformación, minería, interpretación y consulta final (fase 99). Tras cada fase se generan informes **Markdown/HTML** en `reports/kdd/<run_id>/` (exportables a PDF con Pandoc o impresión del HTML).

- Definición: `orquestacion/dag_simlog_kdd_fases.py` (DAGs `simlog_kdd_00_infra` … `simlog_kdd_99_consulta_final`)
- Lógica e informes: `orquestacion/kdd_ejecucion.py`, `orquestacion/kdd_informe.py`
- Guía detallada: [`orquestacion/README_DAGS_KDD.md`](orquestacion/README_DAGS_KDD.md)

Copiar al `dags/` de Airflow los ficheros de `orquestacion/` necesarios (como mínimo los anteriores más `servicios_arranque.py`) y asegurar **PYTHONPATH** hacia la raíz del proyecto si hace falta.

### 7b. Airflow — pipeline compacto (alternativa)

`orquestacion/dag_maestro.py`: DAG `simlog_pipeline_maestro` cada 15 min (verifica HDFS/Kafka/Cassandra y ejecuta Ingesta → Procesamiento, `max_active_runs=1`). Útil si no usas el flujo por fases KDD.

Si antes usabas el id `dag_maestro_transporte`, desactívalo en la UI y usa el DAG actualizado.

## Estructura

| Archivo | Descripción |
|---------|-------------|
| `config_nodos.py` | Topología: 5 hubs, 25 secundarios, aristas |
| `config.py` | Marca SIMLOG, JARs, API keys |
| `ingesta/ingesta_kdd.py` | API clima, simulación, GPS 15min, Kafka+HDFS |
| `procesamiento/procesamiento_grafos.py` | GraphFrames, autosanación, Cassandra+Hive |
| `app_visualizacion.py` | Streamlit + Folium |
| `servicios/cuadro_mando_ui.py` | Cuadro de mando (consultas + slides clima) |
| `servicios/consultas_cuadro_mando.py` | Plantillas CQL/Hive supervisadas |
| `servicios/clima_retrasos.py` | Heurísticas de retraso (OWM + Cassandra) |
| `servicios/gestion_servicios.py` | Lógica iniciar/comprobar/parar stack |
| `servicios/ui_gestion_servicios.py` | Panel Streamlit de servicios |
| `servicios/ui_servicios_web.py` | URLs y credenciales de consolas web |
| `servicios/red_hibrida_rutas.py` | BFS, retrasos, alternativas (red híbrida) |
| `servicios/ui_rutas_hibridas.py` | Formulario rutas en Streamlit |
| `simlog_ui.env.example` | Variables de ejemplo para UIs |
| `servicios/api_simlog.py` | API REST + Swagger (`/docs`) |
| `servicios/README_SERVICIOS_API.md` | Guía API OpenAPI |
| `nifi/` | NiFi: proceso alternativo de ingesta (Kafka, HDFS, Spark) |
| `orquestacion/dag_simlog_kdd_fases.py` | Airflow: DAGs por fase KDD (secuencia + informes) |
| `orquestacion/dag_maestro.py` | Airflow: pipeline compacto cada 15 min (alternativa) |
| `orquestacion/README_DAGS_KDD.md` | Guía despliegue y PDF de informes KDD |
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
