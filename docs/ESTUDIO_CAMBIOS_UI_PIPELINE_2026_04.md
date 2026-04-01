## Estudio técnico de cambios (UI + pipeline) — 2026-04

Documento orientado a **equipo de desarrollo** para revisar decisiones, alcance y validación de los cambios recientes en SIMLOG (modo standalone).

### Contexto / problema

En operación real se observó que:

- La UI (Streamlit) incluía acciones de **ingesta** y **Spark** en el **sidebar** que podían **bloquear el navegador** y “dejar el equipo sin reaccionar”.
- La métrica `paso_15min` se interpretaba mal cuando la ingesta se ejecutaba en modo **automático** (por reloj) y devolvía un índice grande (p. ej. `1972209`).
- NiFi mostraba *warnings* en `Spark_Submit_Procesamiento` por salida `INFO` de Spark enviada a `stderr`.
- HiveServer2 podía no arrancar por un **PID stale** en `hiveserver2.pid` aunque el puerto `10000` estuviese inactivo.

### Objetivos

- **UI segura y responsive**: evitar ejecuciones largas desde el sidebar; priorizar visualización y verificación.
- **Observabilidad**: métricas y mensajes que expliquen el “slot/ventana” de ingesta y el estado del pipeline.
- **Robustez de ingesta**: timeouts y fallbacks para no colgar procesos.
- **Operabilidad**: reducir ruido de NiFi, y documentar/automatizar recuperación de HiveServer2.

---

## Cambios principales por componente

### 1) Streamlit (`app_visualizacion.py`) — eliminación de ingesta manual del sidebar

**Decisión**: el sidebar se limita a **estado del stack**, arranque/parada y enlaces.  
**Motivo**: cualquier ejecución pesada en UI puede congelar el render o el navegador (especialmente Spark o operaciones lentas HDFS/Kafka).

**Acción**:
- Eliminados botones de ingesta/Spark/pipeline en el sidebar.
- Se mantiene la ejecución **manual** de fases KDD desde la pestaña **Ciclo KDD**, con controles explícitos para:
  - `paso_15min`
  - `simlog_simular_incidencias`

**Riesgo mitigado**:
- Se resolvió un conflicto Streamlit: un widget con `key="paso_15min"` no puede tener su `st.session_state` modificado después.  
  Se eliminó el segundo control (slider) y la escritura manual a `session_state.paso_15min` en la vista previa de ficheros.

Archivos tocados (UI):
- `app_visualizacion.py`
- `servicios/kdd_vista_ficheros.py`

---

### 2) Métrica de ingesta: `paso_15min_modo` (auto/manual)

**Problema**: `paso_15min` puede ser:
- **manual**: valor corto definido por `PASO_15MIN` (UI/pruebas)
- **auto**: índice global calculado por reloj UTC (sin `PASO_15MIN`)

**Implementación**:
- `ingesta/trigger_paso.py` incorpora `resolver_paso_ingesta_detalle() -> (paso:int, modo:str)` con `modo ∈ {"manual","auto"}`.
- El payload y `ultima_ingesta_meta.json` incluyen:
  - `paso_15min_modo`

**Consumo en UI**:
- `servicios/ui_pipeline_resultados.py` etiqueta la métrica según `paso_15min_modo`:
  - `auto` → “Slot ingesta (índice UTC)”
  - `manual` → “Paso (PASO_15MIN manual)”
- Además muestra la **ventana del día UTC** (0..N-1) calculada desde el `timestamp` e `SIMLOG_INGESTA_INTERVAL_MINUTES`.
- Compatibilidad: si el JSON no trae `paso_15min_modo`, se mantiene un fallback por heurística (umbral alto).

Archivos tocados:
- `ingesta/trigger_paso.py`
- `ingesta/ingesta_kdd.py`
- `ingesta_kdd.py` (shim raíz)
- `scripts/ejecutar_ingesta_dgt.py`
- `servicios/pipeline_verificacion.py`
- `servicios/ui_pipeline_resultados.py`
- `tests/test_ingestion.py`

---

### 3) Resultados del pipeline: sección Hive (histórico) en UI

**Motivo**: `pipeline_verificacion.py` ya disponía de `hive_resumen()` y `obtener_snapshot_pipeline(... incluir_hive=...)`, pero la pestaña de resultados no renderizaba Hive.

**Implementación** (`servicios/ui_pipeline_resultados.py`):
- Checkbox **“Incluir Hive”**: si está marcado, el botón “Actualizar comprobaciones” incluye consulta a HiveServer2.
- Botón **“Consultar Hive ahora”**: ejecuta solo `hive_resumen()` y actualiza el snapshot en sesión (sin repetir Kafka/HDFS/Cassandra).
- Render de:
  - `SHOW TABLES` (muestra truncada)
  - conteos whitelist (p. ej. `historico_nodos_*`, `nodos_maestro_*`)
  - hints de troubleshooting (refused, impersonación, alias de vistas)

---

### 4) Ingesta robusta (timeouts + fallbacks)

Objetivo: evitar bloqueos por IO y APIs inestables.

Cambios relevantes:
- Kafka: timeouts en describe/list para no colgar el proceso.
- HDFS: timeouts en CLI y **fallback WebHDFS** cuando `hdfs dfs` no responde.
- Clima: fallback por nodo en paralelo (ThreadPool) para no depender de un único endpoint lento.
- DGT DATEX2: parseo incremental (`iterparse`) + límites para XML grandes.

Archivos (ingesta/servicios/orquestación):
- `ingesta/ingesta_kdd.py`
- `ingesta/ingesta_dgt_datex2.py`
- `servicios/open_meteo_clima.py`
- `servicios/ejecucion_pipeline.py`
- `orquestacion/dag_simlog_maestro.py`
- `orquestacion/dag_ingesta_dgt.py`

---

### 5) Procesamiento Spark (Cassandra + Hive)

**Cassandra**:
- Se eliminó la dependencia de un jar “suelto” incompleto y se pasó a `spark.jars.packages` con el conector Maven:
  - `com.datastax.spark:spark-cassandra-connector_2.12:3.5.0`

**Hive**:
- Compatibilidad de esquemas: `coalesce(clima_actual, clima_desc)` y similares para evitar roturas por nombres de columnas cambiantes.

Archivos:
- `procesamiento/procesamiento_grafos.py`

---

### 6) Airflow — DAG de healthcheck del stack

Se añadió un DAG específico para comprobar servicios:
- `orquestacion/dag_comprobar_servicios_simlog.py` (`dag_id="dag_comprobar_servicios_simlog"`)

**Nota de despliegue**: requiere estar en el `dags_folder` efectivo de Airflow (symlink/copia).

---

### 7) NiFi — reducir warnings en `Spark_Submit_Procesamiento`

**Problema**: NiFi marca como *WARNING* lo que llega por `stderr` del proceso, aunque Spark escriba `INFO` por ahí.

**Solución**:
- `nifi/scripts/spark_submit_yarn.sh` redirige **stdout+stderr** de `spark-submit` a fichero:
  - `reports/nifi_spark/spark_submit_<timestamp>_<pid>.log` (o `SIMLOG_NIFI_SPARK_LOG_DIR`)
- `stderr` del wrapper solo escribe contenido si el job falla (resumen + tail del log).

Archivos:
- `nifi/scripts/spark_submit_yarn.sh`
- `nifi/README_NIFI.md`

---

### 8) HiveServer2 — PID stale y arranque JDBC 10000

**Síntoma**:
- Metastore activo (9083) pero JDBC 10000 inactivo
- Log: `HiveServer2 running as process <pid>. Stop it first.`

**Causa**:
- `HIVE_CONF_DIR/hiveserver2.pid` contiene un PID antiguo. Hive bloquea el arranque aunque el proceso ya no exista.

**Recuperación** (documentada):

```bash
rm -f ~/proyecto_transporte_global/hive/conf/hiveserver2.pid
python ~/proyecto_transporte_global/scripts/simlog_stack.py start
```

**Hardening en código**:
- `servicios/gestion_servicios.py` ahora limpia el PID stale en el **mismo directorio** que usa Hive (`HIVE_CONF_DIR/hiveserver2.pid`).

Docs actualizadas:
- `docs/AIRFLOW.md`
- `docs/MANUAL_USUARIO.md`

---

## Validación recomendada (checklist)

### UI (Streamlit)
- Abrir UI y navegar a:
  - **Ciclo KDD**: verificar que existen opciones `paso_15min` y `simlog_simular_incidencias` y que no hay error de `StreamlitAPIException` por `session_state`.
  - **Resultados pipeline**: pulsar “Actualizar comprobaciones”.
    - Comprobar que muestra `paso_15min_modo` correctamente (manual/auto).
    - Marcar “Incluir Hive” y repetir; o usar “Consultar Hive ahora”.

### Stack
- `python scripts/simlog_stack.py status` o `scripts/comprobar_stack.sh`
  - Confirmar `hive: OK — JDBC 10000 activo · Metastore 9083 activo`

### NiFi
- Ejecutar `Spark_Submit_Procesamiento` y comprobar que los bulletins amarillos disminuyen.
- Verificar que aparecen logs en `reports/nifi_spark/`.

### Tests
- `pytest tests/test_ingestion.py`

---

## Rollback / compatibilidad

- `paso_15min_modo` es **aditivo**: consumidores antiguos ignoran el campo.
- La UI mantiene fallback si el campo no existe.
- NiFi script cambia solo el destino de logs; si se desea revertir, basta con volver a redirección anterior.

---

## Pendientes / ideas (backlog)

- Unificar “acciones rápidas” que aún ejecuten ingesta/Spark desde UI en un disparo asíncrono vía Airflow/NiFi.
- Añadir un panel en UI para consultar “último log Spark de NiFi” (tail del fichero).
- Normalizar `paso` (auto) vs `paso_del_dia` como campos explícitos en el payload.

