# Gestión de servicios (dashboard)

Pestaña **Servicios** en `app_visualizacion.py`: **Iniciar**, **Comprobar** y **Parar** para:

| Servicio | Comprobación típica | Inicio | Parada |
|----------|----------------------|--------|--------|
| HDFS | Puerto NameNode (9870) | `start-dfs.sh` | `stop-dfs.sh` |
| Kafka | 9092 | `kafka-server-start.sh` + config | `kafka-server-stop.sh` o pkill |
| Cassandra | 9042 | `cassandra/bin/cassandra` (proyecto) | `nodetool stopdaemon` / pkill |
| Spark | 7077 (master) | `sbin/start-master.sh` | `stop-master.sh` / workers |
| Hive | 10000 (HiveServer2) + 9083 (metastore) | `hive --service hiveserver2` | kill puerto |

### Hive: metastore vs HiveServer2

El panel marca **Hive** como activo solo si **ambos** puertos responden: JDBC **10000** (HiveServer2) y **9083** (metastore). Es habitual que el metastore siga arriba y HS2 se caiga (reinicio, Spark, etc.).

- **Reparación rápida (UI):** en la barra lateral aparece **«Reparar: levantar HiveServer2»** cuando 9083 está bien y 10000 no.
- **CLI:** `python scripts/simlog_stack.py ensure-hive` o `python scripts/ensure_hiveserver2.py`.
- **Persistente tras reboot:** unidad systemd `orquestacion/systemd/simlog-hiveserver2.service` + `scripts/hiveserver2_systemd_exec.sh` (instrucciones en el `.service`).
- **Cron (opcional):** cada 10 min `venv_transporte/bin/python scripts/ensure_hiveserver2.py` (sale en segundos si ya está bien).

Función Python: `servicios.gestion_servicios.ensure_hiveserver2()` (idempotente; equivale a **Iniciar** en Hive).
| Airflow | 8080 (api-server) | `airflow api-server` | kill puerto + scheduler |
| NiFi | 8443 / 8080 | `nifi.sh start` | `nifi.sh stop` |

## Variables de entorno

| Variable | Valor por defecto |
|----------|-------------------|
| `HADOOP_HOME` | `/opt/hadoop` |
| `KAFKA_HOME` | `/opt/kafka` |
| `SPARK_HOME` | `/opt/spark` |
| `HIVE_HOME` | `/opt/hive` |
| `NIFI_HOME` | `/opt/nifi` |
| `AIRFLOW_HOME` | `~/airflow` |
| `SIMLOG_PORT_HDFS` | …9870 |
| `SIMLOG_PORT_KAFKA` | …9092 |
| `SIMLOG_PORT_CASSANDRA` | …9042 |
| `SIMLOG_PORT_HIVE` | …10000 |
| `SIMLOG_PORT_SPARK_MASTER` | …7077 |
| `SIMLOG_PORT_AIRFLOW` | …8080 |
| `SIMLOG_PORT_NIFI_HTTPS` / `SIMLOG_PORT_NIFI_HTTP` | …8443 / 8080 |

**Spark:** los jobs del proyecto suelen usar `local[*]` sin daemon; el panel sirve para un cluster standalone opcional.

**Airflow:** el panel arranca solo **api-server**; el **scheduler** debe lanzarse aparte (`airflow scheduler`) si ejecutas DAGs.

**Parar:** requiere marcar la casilla de confirmación en la UI.

## Interfaces web (URL, puerto, credenciales)

En la pestaña **Servicios**, cada bloque incluye un expander **«Interfaz web · URL y credenciales»** con:

- **Puerto** de referencia (JDBC/CQL u otro, según el servicio).
- **Usuario** y **contraseña** leídos de variables de entorno `SIMLOG_UI_<SERVICIO>_USER` / `SIMLOG_UI_<SERVICIO>_PASSWORD`.
- Botón **«Abrir interfaz en nueva pestaña»** (`st.link_button`).

En la **barra lateral** hay un expander **«Interfaces web del stack»** con accesos rápidos a las mismas URLs.

Plantilla de variables: [`simlog_ui.env.example`](../simlog_ui.env.example) en la raíz del proyecto.

| Variable global | Uso |
|-----------------|-----|
| `SIMLOG_UI_HOST` | Host para URLs por defecto (sustituir si accedes desde otra máquina). |
| `SIMLOG_UI_REVEAL_SECRETS=1` | Muestra contraseñas en claro en el dashboard (**solo entornos de confianza**). |

Contraseñas **no** deben commitearse; usa `.env` local o un gestor de secretos.
