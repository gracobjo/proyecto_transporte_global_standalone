# Flujo de datos, ejemplos y requisitos de la práctica

Documento que resume: (1) ejemplos concretos de datos (GPS, clima, rutas), (2) qué hace Spark y qué se guarda dónde, (3) cómo se gestionan nulos/duplicados/normalización antes de Cassandra, (4) Hive en la infraestructura, (5) checklist para contrastar con *Proyecto Big Data.pdf*.

---

## 1. Ejemplo de qué hace cada funcionalidad (datos GPS, clima, rutas)

### 1.1 Datos que genera la ingesta (ejemplo de un “snapshot” de 15 min)

La **ingesta** produce un único JSON que contiene tres bloques de datos: **clima**, **estados de nodos/aristas** (para rutas) y **camiones con GPS**.

**Ejemplo – Clima (datos de tiempo por hub):**

```json
"clima": [
  { "ciudad": "Madrid", "temperatura": 18.5, "humedad": 60, "descripcion": "clear sky", "visibilidad": 10000, "timestamp": "2025-03-15T12:00:00" },
  { "ciudad": "Barcelona", "temperatura": 16.2, "humedad": 72, "descripcion": "light rain", "visibilidad": 8000, "timestamp": "2025-03-15T12:00:00" }
]
```

**Ejemplo – Rutas (estados de aristas; se usan para decidir rutas válidas y pesos):**

```json
"estados_aristas": {
  "Madrid|Barcelona": { "estado": "ok", "distancia_km": 504.2, "motivo": "Tráfico fluido" },
  "Barcelona|Bilbao": { "estado": "congestionado", "distancia_km": 522.1, "motivo": "Lluvia" },
  "Bilbao|Vigo": { "estado": "bloqueado", "distancia_km": 403.0, "motivo": "Nieve" }
}
```

**Ejemplo – GPS de camiones (contrato canónico actual):**

```json
"camiones": [
  {
    "id_camion": "camion_1",
    "lat": 41.12,
    "lon": 1.85,
    "ruta": ["Madrid", "Toledo", "Cuenca", "Barcelona"],
    "ruta_origen": "Madrid",
    "ruta_destino": "Barcelona",
    "ruta_sugerida": ["Madrid", "Toledo", "Cuenca", "Barcelona"],
    "estado_ruta": "En ruta",
    "motivo_retraso": null
  }
]
```

- **GPS**: `id_camion`, `lat`, `lon`, `ruta`, `ruta_origen`, `ruta_destino`, `estado_ruta`.
- **Tiempo**: `clima` (temperatura, humedad, descripción, visibilidad por ciudad).
- **Rutas**: `estados_nodos` y `estados_aristas` (estado y motivo por nodo y por enlace).

Ese JSON se envía a **Kafka** (temas `transporte_raw` y `transporte_filtered`) y se guarda como copia **raw** en **HDFS** en la ruta **`HDFS_BACKUP_PATH`** (`/user/hadoop/transporte_backup`). Esa misma ruta es la que usa el procesamiento Spark para leer los JSON, de modo que no hace falta duplicar rutas: la ingesta escribe donde Spark lee.

---

## 2. Arranque de Spark: qué se guarda y dónde (HDFS vs Cassandra)

### 2.1 Origen de los datos para Spark

- **Entrada**: Spark lee el/los JSON de la **ingesta** desde **HDFS** (ruta configurada como `HDFS_BACKUP_PATH`, p. ej. `/user/hadoop/transporte_backup`). Si no hay ficheros, usa datos de simulación en memoria.
- Esos JSON son los **mismos datos lógicos** que luego verás en Cassandra (clima, nodos, aristas, camiones), pero en formato semiestructurado (JSON).

### 2.2 Qué hace Spark y dónde escribe

| Dónde | Qué se guarda |
|-------|----------------|
| **HDFS** | La ingesta ya ha guardado aquí el JSON “en bruto”. Spark **no** vuelve a escribir en HDFS en este flujo; solo **lee** desde aquí. El warehouse de Hive (si se usa) suele estar en HDFS, así que lo que Spark escribe en Hive queda almacenado en HDFS. |
| **Cassandra** (keyspace `logistica_espana`) | Spark **estructura** los datos y escribe en tablas: **nodos_estado**, **aristas_estado**, **tracking_camiones**, **pagerank_nodos**. Son los mismos datos que en el JSON (clima integrado en nodos, estados, camiones, etc.), pero normalizados y con tipos (float, timestamp, listas, etc.). |
| **Hive** (opcional) | Spark puede escribir tablas de **histórico** (p. ej. `logistica_espana.historico_nodos` o tablas en `logistica_db` si se usa `persistencia_hive.py`). Esas tablas viven en el warehouse de Hive (típicamente en HDFS). |

Resumen: **HDFS** = JSON de ingesta (y warehouse Hive). **Cassandra** = mismos datos ya procesados y estructurados en tablas para consulta en tiempo (casi) real.

---

## 3. Gestión de nulos, duplicados y normalización antes de Cassandra

Si hubiera datos que normalizar, nulos o duplicados, eso debe hacerse **en Spark** (o en una capa previa) **antes** de escribir en Cassandra. A continuación se muestra **cómo** hacerlo con ejemplos.

### 3.1 Dónde encaja en el flujo

- Spark lee el JSON desde HDFS (o simulación).
- Se aplican **limpieza y normalización** sobre DataFrames.
- Solo después se construyen los DataFrames finales que se escriben en Cassandra (y opcionalmente Hive).

### 3.2 Ejemplos de gestión (PySpark)

**Nulos – rellenar o eliminar:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Ejemplo: DataFrame con datos de camiones leídos del JSON
# Rellenar lat/lon nulos con un valor por defecto (ej. centro España)
df_camiones = df_camiones \
    .withColumn("lat", when(col("lat").isNull(), 40.4).otherwise(col("lat"))) \
    .withColumn("lon", when(col("lon").isNull(), -3.7).otherwise(col("lon")))

# O eliminar filas con nulos en campos clave
df_camiones = df_camiones.dropna(subset=["id_camion", "lat", "lon"])
```

**Duplicados – eliminar por clave:**

```python
# Eliminar duplicados por id_camion (quedarse con la última o la primera)
df_camiones = df_camiones.dropDuplicates(["id_camion"])

# O por varias columnas (ej. nodo + timestamp)
df_nodos = df_nodos.dropDuplicates(["id_nodo", "timestamp"])
```

**Normalización – estados y tipos:**

```python
# Unificar valores de estado (mayúsculas, tildes, variantes)
from pyspark.sql.functions import lower, trim, regexp_replace

df_nodos = df_nodos.withColumn(
    "estado",
    lower(trim(regexp_replace(col("estado"), "ó", "o")))
)
# Mapear a valores canónicos: ok, congestionado, bloqueado
from pyspark.sql.functions import when
df_nodos = df_nodos.withColumn(
    "estado",
    when(col("estado").isin("ok", "ok ", "fluido"), "OK")
    .when(col("estado").isin("congestionado", "congestion"), "Congestionado")
    .when(col("estado").isin("bloqueado", "blocked"), "Bloqueado")
    .otherwise("OK")
)
```

**Validación y filtrado antes de escribir:**

```python
# Solo escribir camiones con coordenadas válidas
df_camiones = df_camiones.filter(
    (col("lat").between(35, 44)) & (col("lon").between(-10, 5))
)
```

En este proyecto, la limpieza **sí está integrada** en `procesamiento_grafos.py` mediante la función **`limpiar_datos_antes_cassandra`**, que se ejecuta justo antes de `procesar_y_persistir`:

- **Nodos**: normaliza `estado` (OK, Congestionado, Bloqueado), rellena motivo vacío, descarta claves nulas.
- **Aristas**: normaliza estado y motivo, asegura `distancia_km` numérico, descarta claves inválidas.
- **Camiones**: rellena lat/lon nulos (40.4, -3.7), elimina duplicados por `id_camion`, filtra coordenadas fuera de España (35–44 lat, -10–5 lon).

Todo lo que se escribe en Cassandra (y en Hive si aplica) pasa por esta capa antes de persistir.

---

## 5. Hive en la infraestructura

- **Hive** tiene su propia base de datos (p. ej. `logistica_db` en `persistencia_hive.py`) y tablas (eventos, clima, camiones, rutas alternativas, agregados diarios), particionadas por año/mes.
- **Origen de los datos**: lo que Spark escribe en Hive (o lo que escribe `persistencia_hive.py`) viene del **mismo** flujo que alimenta Cassandra: datos de la ingesta (JSON en HDFS o simulación), procesados y opcionalmente limpiados en Spark.
- **Uso**: Hive sirve para consultas analíticas e histórico (por fechas); Cassandra para estado actual y dashboard. No hay flujo “Hive → Cassandra”; ambos se alimentan desde el procesamiento Spark.

---

## 6. Cumplimiento de requisitos (estado actual)

En el estado actual del proyecto:

| Requisito típico | Estado actual |
|------------------|---------------|
| **Ingesta de datos** | Sí: NiFi y/o script de ingesta (OpenWeather + simulación GPS) → Kafka + HDFS. |
| **Almacenamiento distribuido (HDFS)** | Sí: backup JSON; warehouse Hive en HDFS. |
| **Procesamiento Big Data (Spark)** | Sí: GraphFrames, autosanación, PageRank, persistencia. |
| **Almacenamiento NoSQL (Cassandra)** | Sí: keyspace operativo (nodos, aristas, camiones, PageRank). |
| **Almacenamiento analítico (Hive)** | Sí: histórico y consultas supervisadas. |
| **Cola de mensajes (Kafka)** | Sí: `transporte_raw` y `transporte_filtered`. |
| **Calidad de datos** | Sí: limpieza previa a persistencia en el pipeline Spark. |
| **Visualización** | Sí: Streamlit + mapa. |
| **Orquestación** | Sí: Airflow (DAGs) y/o trigger en NiFi; scripts `simlog_stack.py` para el stack. |
| **NiFi** | Sí: flujo documentado y alineado al proyecto (`nifi/`). |

Conclusión: la arquitectura actual cubre el ciclo completo KDD con stack Apache en modo standalone.  
Para trazabilidad de evaluación y brechas puntuales, consultar `docs/REQUIREMENTS_CHECKLIST.md`.

---

## 7. Requisitos funcionales — dashboard Ciclo KDD (Streamlit)

| ID | Requisito | Implementación |
|----|-----------|----------------|
| RF-KDD-01 | Asociar cada fase KDD a **scripts y stack** declarados en `servicios/kdd_fases.py` | Tarjeta por fase + caption stack/script |
| RF-KDD-02 | **Vista previa** de ficheros del repositorio citados (p. ej. `ingesta/ingesta_kdd.py`, `procesamiento_grafos.py`) | `servicios/kdd_vista_ficheros.py` — primeras y últimas 5 líneas |
| RF-KDD-03 | En fases 1–2, mostrar **GPS sintético** (`camiones`) y **clima** (`clima_hubs`) del último payload | Lectura de `reports/kdd/work/ultimo_payload.json` |
| RF-KDD-04 | Permitir **consulta en vivo** a OpenWeather con API key introducida en formulario (sin persistir en disco) | Formulario Streamlit + `consulta_clima_hubs(api_key=…)` |
| RF-KDD-05 | **Simular** otra ventana temporal: slider de paso, ingesta desde la tarjeta, **instantánea** y **diff** de payload | `st.session_state` + comparación JSON |
| RF-KDD-06 | En fases 3–5, documentar **reglas de negocio** del grafo sin repetir el mismo texto al cambiar de fase | `servicios/kdd_reglas_ui.py` — panel unificado |
| RF-KDD-07 | Mostrar **topología lógica** (Altair) como **una** figura para fases 3–5; distinguir de mapa geográfico | `servicios/kdd_vista_grafo.py` |
| RF-KDD-08 | Listado «Ver todas las fases» sin **duplicar** formularios/sliders (evitar colisión de `key` en Streamlit) | `mostrar_vista_previa=False` en lista |

## 8. Requisitos no funcionales — dashboard Ciclo KDD

| ID | Requisito | Notas |
|----|-----------|--------|
| RNF-KDD-01 | **Usabilidad**: una sola zona interactiva principal por tipo de control en la vista de fase activa | `widget_scope` (`kdd_principal` vs `kdd_lista_fN`) |
| RNF-KDD-02 | **Seguridad**: no almacenar API keys de prueba en ficheros del proyecto | Solo `st.session_state`; `.env` sigue siendo la vía para clave estable |
| RNF-KDD-03 | **Mantenibilidad**: módulos `servicios/kdd_*.py` desacoplados de la orquestación | Ingesta invocable por subproceso desde `ejecucion_pipeline` |
| RNF-KDD-04 | **Consistencia visual**: no sugerir tres grafos distintos cuando el layout topológico es el mismo | Título de gráfico y subtítulos alineados a «una red, varias fases» |

Diseño detallado: **`docs/DASHBOARD_KDD_UI.md`**.

---

## 9. Requisitos funcionales — Cuadro de mando (Hive histórico 24h)

| ID | Requisito | Implementación |
|----|-----------|----------------|
| RF-CM-HIVE-01 | En el **Cuadro de mando**, permitir consultas Hive supervisadas para analizar **incidencias** del histórico en una ventana temporal de **24 horas** | `servicios/consultas_cuadro_mando.py` (whitelist) + `render_consultas_hive()` en `servicios/cuadro_mando_ui.py` |
| RF-CM-HIVE-02 | Calcular un informe **Riesgo por hub (últimas 24h)** derivando un tipo de incidencia a partir de `estado`, `motivo_retraso` y `clima_actual` (clasificación: OK/Congestionado/Bloqueado + subtipos) | Query Hive `riesgo_hub_24h` (deriva severidad/tipo y agrega top 3 por hub) |
| RF-CM-HIVE-03 | Calcular un informe **Top causas (últimas 24h)** que agregue los tipos derivados de incidencia y devuelva el ranking de causas con su share y una **duración aproximada** sobre 24h | Query Hive `top_causas_24h` (top 10 por muestras) |

## 10. Requisitos no funcionales — Cuadro de mando (Hive histórico 24h)

| ID | Requisito | Notas |
|----|-----------|-------|
| RNF-CM-HIVE-01 | Las consultas Hive usadas en el UI deben ser agregadas y filtradas por ventana temporal (24h) para evitar bloqueos de la interfaz | SQL agregadas con `WHERE fecha_proceso >= ...` + límites/ranking |

Diseño detallado: **`docs/DIAGRAMAS_MERMAID.md`**, apartados de flujo UI/analítica y diagramas UML en `docs/uml/`.
