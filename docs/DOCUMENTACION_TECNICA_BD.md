# Documentación Técnica — SIMLOG

## Índice

1. [Arquitectura de Datos](#1-arquitectura-de-datos)
2. [Cassandra — Modelo de Datos](#2-cassandra--modelo-de-datos)
3. [Hive — Modelo de Datos](#3-hive--modelo-de-datos)
4. [Consultas Parametrizadas](#4-consultas-parametrizadas)
5. [Problemas Corregidos en Hive](#5-problemas-corrigidos-en-hive)
6. [Archivos de Configuración](#6-archivos-de-configuración)
7. [Checklist de Implementación](#7-checklist-de-implementación)

**Anexo:** [Hive — correcciones del cuadro de mando](HIVE_CUADRO_MANDO_CORRECCIONES.md) (configuración unificada, JOIN clima/transporte, normalización de texto, caché).

---

## 1. Arquitectura de Datos

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INGESTA (NiFi / Python)                          │
│  • GPS sintético generado por Groovy                                        │
│  • OpenWeather API → clima por hub                                         │
│  • DATEX2 DGT → incidencias de tráfico                                    │
└────────────────────────────┬────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MENSAJERÍA (Kafka)                                 │
│  Topics: transporte_raw, transporte_dgt_raw, transporte_filtered            │
└────────────────────────────┬────────────────────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│    HDFS        │  │   Cassandra     │  │     Hive        │
│  (Backup JSON)  │  │ (Tiempo Real)   │  │  (Histórico)    │
└─────────────────┘  └─────────────────┘  └─────────────────┘
                             │                   │
                             ▼                   ▼
                    ┌─────────────────┐  ┌─────────────────┐
                    │   Dashboard      │  │   Dashboard      │
                    │   Streamlit      │  │   Streamlit      │
                    └─────────────────┘  └─────────────────┘
```

---

## 2. Cassandra — Modelo de Datos

### Base de datos
- **Keyspace**: `logistica_espana`
- **Estratégia**: SimpleStrategy (replication_factor: 1)

### Tablas

#### 2.1 nodos_estado
Estado actual de cada nodo (hub) de la red.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_nodo` | TEXT | Clave primaria. Identificador del nodo |
| `lat` | FLOAT | Latitud |
| `lon` | FLOAT | Longitud |
| `tipo` | TEXT | Tipo: "hub", "capital", "secundario" |
| `estado` | TEXT | OK, Congestionado, Bloqueado |
| `motivo_retraso` | TEXT | Motivo del retraso/incidencia |
| `clima_actual` | TEXT | Descripción del clima actual |
| `temperatura` | FLOAT | Temperatura en °C |
| `humedad` | FLOAT | Humedad relativa (%) |
| `viento_velocidad` | FLOAT | Velocidad del viento |
| `source` | TEXT | Fuente de la información (DGT, simulación) |
| `severity` | TEXT | Severidad: low, medium, high, highest |
| `id_incidencia` | TEXT | ID de la incidencia DGT |
| `carretera` | TEXT | Carretera afectada |
| `municipio` | TEXT | Municipio de la incidencia |
| `provincia` | TEXT | Provincia de la incidencia |
| `descripcion_incidencia` | TEXT | Descripción textual |
| `ultima_actualizacion` | TIMESTAMP | Última actualización |

#### 2.2 aristas_estado
Estado de las conexiones entre nodos.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `src` | TEXT | Nodo origen (parte de la clave primaria) |
| `dst` | TEXT | Nodo destino (parte de la clave primaria) |
| `distancia_km` | FLOAT | Distancia en km |
| `estado` | TEXT | OK, Congestionado, Bloqueado |
| `peso_penalizado` | FLOAT | Peso con penalización por incidencias |

#### 2.3 tracking_camiones
Posición GPS actual y estado de los camiones.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_camion` | TEXT | Clave primaria. ID del camión |
| `lat` | FLOAT | Latitud actual |
| `lon` | FLOAT | Longitud actual |
| `ruta_origen` | TEXT | Nodo de origen de la ruta |
| `ruta_destino` | TEXT | Nodo de destino de la ruta |
| `ruta_sugerida` | LIST<TEXT> | Ruta calculada (lista de nodos) |
| `estado_ruta` | TEXT | Estado: "En ruta", "Bloqueado", etc. |
| `motivo_retraso` | TEXT | Motivo del retraso |
| `ultima_posicion` | TIMESTAMP | Timestamp de la última posición |

#### 2.4 pagerank_nodos
Criticidad de nodos según algoritmo PageRank.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_nodo` | TEXT | Clave primaria |
| `pagerank` | FLOAT | Puntuación PageRank |
| `peso_pagerank` | FLOAT | Peso ponderado |
| `source` | TEXT | Fuente del cálculo |
| `estado` | TEXT | Estado actual del nodo |
| `ultima_actualizacion` | TIMESTAMP | Última actualización |

#### 2.5 eventos_historico
Histórico de cambios de estado.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_evento` | UUID | Clave primaria |
| `tipo_entidad` | TEXT | Tipo: nodo, arista |
| `id_entidad` | TEXT | ID de la entidad |
| `estado_anterior` | TEXT | Estado anterior |
| `estado_nuevo` | TEXT | Estado nuevo |
| `motivo` | TEXT | Motivo del cambio |
| `lat` | FLOAT | Latitud |
| `lon` | FLOAT | Longitud |
| `timestamp_evento` | TIMESTAMP | Timestamp del evento |

**TTL**: 30 días (2592000 segundos)

#### 2.6 graph_anomalies
Anomalías detectadas por Graph AI.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id` | UUID | Clave primaria |
| `timestamp` | TIMESTAMP | Timestamp |
| `node_id` | TEXT | ID del nodo |
| `anomaly_score` | DOUBLE | Puntuación de anomalía |
| `metric_type` | TEXT | Tipo de métrica |
| `metric_value` | DOUBLE | Valor de la métrica |
| `ts_bucket` | BIGINT | Bucket temporal (15 min) |

---

## 3. Hive — Modelo de Datos

### Base de datos
- **Nombre**: `logistica_espana` (configurable via `HIVE_DB`)

### Almacenamiento y particiones
- Tablas externas en **Parquet** con compresión **Snappy**, particionadas por `anio_part` y `mes_part` (escritura vía Spark en `persistencia_hive.py`). Tras cada escritura se ejecuta `MSCK REPAIR TABLE` para registrar particiones nuevas en el metastore.
- Las consultas del cuadro de mando filtran por partición cuando aplica (`SIMLOG_HIVE_PARTITION_PRUNING`, fragmentos `_PM` / `_P7` en `consultas_cuadro_mando.py`) para reducir escaneos.
- **Migración desde CSV antiguo:** `export SIMLOG_HIVE_PARQUET_MIGRATION=1` antes de `inicializar_esquema_hive()` (o `DROP TABLE` manual) y eliminar ficheros huérfanos en HDFS si hace falta.

### Tablas Creadas por persistencia_hive.py

#### 3.1 eventos_historico
Histórico de eventos de nodos y aristas.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `timestamp` | STRING | ISO timestamp |
| `anio` | INT | Año |
| `mes` | INT | Mes |
| `dia` | INT | Día |
| `hora` | INT | Hora |
| `minuto` | INT | Minuto |
| `dia_semana` | STRING | Día de la semana |
| `tipo_evento` | STRING | nodo, arista |
| `id_elemento` | STRING | ID del nodo o "origen|destino" |
| `tipo_elemento` | STRING | Tipo de elemento |
| `estado` | STRING | Estado |
| `motivo` | STRING | Motivo |
| `pagerank` | DOUBLE | PageRank del nodo |
| `distancia_km` | DOUBLE | Distancia |
| `hub_asociado` | STRING | Hub más cercano |

#### 3.2 clima_historico
Histórico de datos climáticos por ciudad.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `timestamp` | STRING | ISO timestamp |
| `anio` | INT | Año |
| `mes` | INT | Mes |
| `dia` | INT | Día |
| `ciudad` | STRING | Nombre de la ciudad/hub |
| `temperatura` | DOUBLE | Temperatura en °C |
| `humedad` | INT | Humedad (%) |
| `descripcion` | STRING | Descripción del clima |
| `visibilidad` | INT | Visibilidad en metros |
| `estado_carretera` | STRING | Impacto en carretera |

#### 3.3 tracking_camiones_historico
Histórico de posiciones GPS de camiones.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `timestamp` | STRING | ISO timestamp |
| `anio` | INT | Año |
| `mes` | INT | Mes |
| `dia` | INT | Día |
| `id_camion` | STRING | ID del camión |
| `origen` | STRING | Nodo de origen |
| `destino` | STRING | Nodo de destino |
| `nodo_actual` | STRING | Nodo actual |
| `lat_actual` | DOUBLE | Latitud actual |
| `lon_actual` | DOUBLE | Longitud actual |
| `progreso_pct` | DOUBLE | Progreso (%) |
| `distancia_total_km` | DOUBLE | Distancia total |
| `tiene_ruta_alternativa` | BOOLEAN | Si tiene alternativa |
| `distancia_alternativa_km` | DOUBLE | Distancia alternativa |

#### 3.4 transporte_ingesta_completa
Tabla plana para UI con datos de ingestión.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `timestamp` | STRING | ISO timestamp |
| `anio` | INT | Año |
| `mes` | INT | Mes |
| `dia` | INT | Día |
| `hora` | INT | Hora |
| `minuto` | INT | Minuto |
| `id_camion` | STRING | ID del camión |
| `origen` | STRING | Nodo de origen |
| `destino` | STRING | Nodo de destino |
| `nodo_actual` | STRING | Nodo actual |
| `lat` | DOUBLE | Latitud |
| `lon` | DOUBLE | Longitud |
| `progreso_pct` | DOUBLE | Progreso (%) |
| `distancia_total_km` | DOUBLE | Distancia total |
| `estado_ruta` | STRING | Estado de la ruta |
| `motivo_retraso` | STRING | Motivo del retraso |
| `ruta` | STRING | Ruta como string "nodo1->nodo2->..." |
| `ruta_sugerida` | STRING | Ruta sugerida |
| `hub_actual` | STRING | Hub actual |

#### 3.5 rutas_alternativas_historico
Histórico de rutas alternativas calculadas.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `timestamp` | STRING | ISO timestamp |
| `anio` | INT | Año |
| `mes` | INT | Mes |
| `dia` | INT | Día |
| `origen` | STRING | Nodo de origen |
| `destino` | STRING | Nodo de destino |
| `ruta_original` | STRING | Ruta original |
| `ruta_alternativa` | STRING | Ruta alternativa |
| `distancia_original_km` | DOUBLE | Distancia original |
| `distancia_alternativa_km` | DOUBLE | Distancia alternativa |
| `motivo_bloqueo` | STRING | Motivo del bloqueo |
| `ahorro_km` | DOUBLE | Ahorro en km |

#### 3.6 agg_estadisticas_diarias
Agregaciones diarias de eventos.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `anio` | INT | Año |
| `mes` | INT | Mes |
| `dia` | INT | Día |
| `tipo_evento` | STRING | nodo, arista |
| `estado` | STRING | Estado |
| `motivo` | STRING | Motivo |
| `contador` | INT | Número de eventos |
| `pct_total` | DOUBLE | Porcentaje sobre el total |

---

## 4. Consultas Parametrizadas

### 4.1 Cassandra

#### Categorías y Consultas

| Categoría | Icono | Consultas |
|-----------|-------|----------|
| **Estado de Nodos** | 📍 | nodos_estado_resumen, nodos_hub_congestion, gestor_nodos_con_incidencias, gestor_nodos_madrid_barcelona, gestor_nodos_severidad_alta, gestor_nodos_clima_adverso |
| **Estado de Rutas** | 🛤️ | aristas_estado, gestor_aristas_bloqueadas, gestor_aristas_congestionadas |
| **Tracking Camiones** | 🚛 | tracking_camiones, tracking_camiones_gemelo, gestor_camiones_mapa, gestor_camiones_en_ruta, gestor_camiones_bloqueados, gestor_tracking_ruta_completa |
| **PageRank** | 📊 | pagerank_top, gestor_nodo_critico_pagerank, gestor_pagerank_nodos_criticos |
| **Eventos** | 📋 | eventos_recientes, gestor_eventos_cambios_estado |
| **Gestor** | 👤 | gestor_ciudades_trafico, gestor_incidencias_por_provincia |

**Total: 20 consultas Cassandra**

### 4.2 Hive

#### Categorías y Consultas

| Categoría | Icono | Consultas |
|-----------|-------|----------|
| **Diagnóstico** | 🔧 | diag_smoke_hive, tablas_bd, historico_nodos_muestra/conteo, nodos_maestro_muestra/conteo, gemelo_red_nodos, gemelo_red_aristas |
| **Eventos Histórico** | 📋 | eventos_historico_muestra, eventos_nodos_24h, eventos_bloqueos_24h, eventos_evolucion_dia |
| **Clima Histórico** | 🌤️ | clima_historico_muestra, clima_historico_hoy, clima_estado_carretera |
| **Tracking Camiones** | 🚛 | tracking_historico_muestra, tracking_camion_especifico, tracking_ultima_posicion |
| **Transporte Ingestado** | 📦 | transporte_ingesta_real_muestra, transporte_ingesta_hoy, transporte_retrasos_hoy, gestor_historial_rutas_camion |
| **Rutas Alternativas** | 🛤️ | rutas_alternativas_muestra, rutas_alternativas_bloqueos |
| **Agregaciones Diarias** | 📊 | agg_estadisticas_diarias, agg_ultima_semana |
| **Gestor** | 👤 | gestor_eventos_por_hub, gestor_clima_afecta_transporte, gestor_incidencias_resumen, gestor_pagerank_historico |

**Total: 32 consultas Hive** (aprox.; ver `HIVE_CONSULTAS` en `servicios/consultas_cuadro_mando.py`)

---

## 5. Problemas Corregidos en Hive

La documentación detallada de las **correcciones recientes** (configuración unificada de tablas, whitelist gemelo, JOIN clima/transporte, normalización de texto `_hive_txt_norm`, caché PyHive, UI) está en **[HIVE_CUADRO_MANDO_CORRECCIONES.md](HIVE_CUADRO_MANDO_CORRECCIONES.md)**.

### 5.1 Incidencias típicas (histórico)

| # | Problema | Causa / remedio |
|---|----------|-----------------|
| 1 | Base de datos distinta en ejemplos | Usar `HIVE_DB` (por defecto `logistica_espana`). |
| 2 | `` `timestamp` `` sin citar | En Hive 3+ es palabra reservada; usar `` `timestamp` `` en HiveQL. |
| 3 | Columnas inventadas (`camiones`, `fecha_proceso` en tablas históricas, etc.) | Alinear con `persistencia_hive.py` → `TABLE_SCHEMAS`. |
| 4 | Tabla de transporte distinta entre Spark y UI | Nombre unificado: `HIVE_TABLE_TRANSPORTE_HIST` / `SIMLOG_HIVE_TABLA_TRANSPORTE`. |
| 5 | JOIN clima–transporte vacío | No igualar solo `timestamp` STRING entre tablas; alinear por fecha y `ciudad`/`hub_actual` (ver anexo). |
| 6 | Comparaciones de texto rígidas (`Optimo`, `En ruta`) | Usar expresiones normalizadas en `consultas_cuadro_mando.py` (`_hive_txt_norm`). |

### 5.2 Tablas opcionales y gemelo

- **`historico_nodos`**, **`nodos_maestro`**: nombres configurables (`SIMLOG_HIVE_TABLE_HISTORICO_NODOS`, `SIMLOG_HIVE_TABLE_NODOS_MAESTRO`); deben existir en el clúster si se usan las consultas de diagnóstico.
- **`red_gemelo_nodos`**, **`red_gemelo_aristas`**: creadas con el flujo HDFS + DDL de `procesamiento/generar_red_gemelo_digital.py`; alimentan el gemelo digital y la API.

---

## 6. Archivos de Configuración

### 6.1 Archivos Principales

| Archivo | Descripción |
|---------|-------------|
| `config.py` | Configuración global del proyecto |
| `config_nodos.py` | Configuración de nodos y topología |
| `cassandra/esquema_logistica.cql` | Esquema de Cassandra |
| `sql/hive_schema.sql` | Esquema de referencia de Hive |
| `persistencia_hive.py` | Persistencia en Hive |
| `procesamiento/procesamiento_grafos.py` | Procesamiento con Spark |

### 6.2 Archivos de Consultas

| Archivo | Descripción |
|---------|-------------|
| `servicios/consultas_cuadro_mando.py` | Consultas Cassandra y Hive parametrizadas |
| `servicios/cuadro_mando_ui.py` | UI de consultas en Streamlit |
| `docs/HIVE_CUADRO_MANDO_CORRECCIONES.md` | Anexo: correcciones Hive del cuadro de mando y variables |

### 6.3 Variables de Entorno Relacionadas

| Variable | Descripción | Valor por defecto |
|----------|-------------|------------------|
| `CASSANDRA_HOST` | Host de Cassandra | 127.0.0.1 |
| `KEYSPACE` | Keyspace de Cassandra | logistica_espana |
| `HIVE_DB` | Base de datos Hive | logistica_espana |
| `HIVE_SERVER` | Host:puerto HiveServer2 | 127.0.0.1:10000 |
| `HIVE_JDBC_URL` | URL JDBC de Hive | jdbc:hive2://127.0.0.1:10000 |
| `SIMLOG_ENABLE_HIVE` | Habilitar persistencia Hive | 0 (deshabilitado) |
| `HIVE_QUERY_TIMEOUT_SEC` | Timeout de consultas Hive (PyHive) | 600 (por defecto en código) |
| `SIMLOG_HIVE_COMPAT_BASE` | Path HDFS para tablas Hive | /user/hadoop/simlog_hive |
| `SIMLOG_HIVE_PARQUET_MIGRATION` | Forzar `DROP`+recreate al pasar de CSV a Parquet particionado | 0 |
| `SIMLOG_HIVE_PARTITION_PRUNING` | Añadir filtros `anio_part`/`mes_part` en consultas Hive de la UI | 1 |
| `SIMLOG_HIVE_TABLA_TRANSPORTE` | Nombre de la tabla Hive de transporte plano | `transporte_ingesta_completa` |
| `SIMLOG_HIVE_TABLE_TRACKING_HIST` | Tabla de tracking histórico en Hive | `tracking_camiones_historico` |
| `SIMLOG_HIVE_TABLE_HISTORICO_NODOS` / `SIMLOG_HIVE_TABLE_NODOS_MAESTRO` | Diagnóstico cuadro de mando | `historico_nodos` / `nodos_maestro` |
| `SIMLOG_HIVE_TABLE_RED_GEMELO_NODOS` / `..._ARISTAS` | Red gemelo en Hive | `red_gemelo_nodos` / `red_gemelo_aristas` |

---

## 7. Checklist de Implementación

### 7.1 Prerequisites

- [ ] Cassandra corriendo en puerto 9042
- [ ] HiveServer2 corriendo en puerto 10000 (opcional)
- [ ] Python con dependencias instaladas (`requirements.txt`)
- [ ] Acceso a HDFS (para Hive)

### 7.2 Crear Esquema Cassandra

```bash
cqlsh -f cassandra/esquema_logistica.cql
```

### 7.3 Poblar Datos

#### Opción A: Script Python
```bash
PASO_15MIN=0 python ingesta/ingesta_kdd.py
```

#### Opción B: NiFi
Desplegar flow `nifi/flow/simlog_kdd_flow_spec.yaml`

### 7.4 Procesar con Spark

```bash
# Sin Hive
python procesamiento/procesamiento_grafos.py

# Con Hive (para histórico)
SIMLOG_ENABLE_HIVE=1 python procesamiento/procesamiento_grafos.py
```

### 7.5 Verificar Cassandra

```bash
cqlsh -e "USE logistica_espana; SELECT COUNT(*) FROM nodos_estado;"
cqlsh -e "USE logistica_espana; SELECT COUNT(*) FROM tracking_camiones;"
```

### 7.6 Verificar Hive (si está habilitado)

```bash
# Ver tablas creadas
beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN logistica_espana;"

# Verificar datos
beeline -u "jdbc:hive2://localhost:10000" -e "SELECT COUNT(*) FROM logistica_espana.eventos_historico;"
```

### 7.7 Iniciar Dashboard

```bash
streamlit run app_visualizacion.py
```

### 7.8 Verificar UI

- [ ] Pestaña "Consultas Cassandra" → Seleccionar categoría → Ejecutar consulta
- [ ] Pestaña "Consultas Hive" → Seleccionar categoría → Ejecutar consulta
- [ ] Gemelo Digital → Ver mapa con nodos, aristas y camiones
- [ ] Rutas Híbridas → Calcular ruta entre dos capitales

---

## 8. Notas de Desarrollo

### 8.1 Añadir Nueva Consulta Cassandra

1. Añadir en `CASSANDRA_CONSULTAS` en `servicios/consultas_cuadro_mando.py`
2. Asignar a una categoría en `CASSANDRA_CATEGORIAS`
3. La UI se actualiza automáticamente

### 8.2 Añadir Nueva Consulta Hive

1. Añadir en `HIVE_CONSULTAS` en `servicios/consultas_cuadro_mando.py`
2. Asignar a una categoría en `HIVE_CATEGORIAS`
3. Añadir en `requiere` dict si necesita tablas específicas
4. La UI se actualiza automáticamente

### 8.3 Añadir Nueva Categoría

1. Añadir en `HIVE_CATEGORIAS` o `CASSANDRA_CATEGORIAS`
2. La categoría aparece automáticamente en la UI

---

## 9. Glosario

| Término | Definición |
|---------|------------|
| **Cassandra** | Base de datos NoSQL para datos en tiempo real |
| **Hive** | Data warehouse para histórico y análisis |
| **PageRank** | Algoritmo para determinar criticidad de nodos |
| **Gemelo Digital** | Réplica virtual de la red de transporte |
| **OpenWeather** | API de datos meteorológicos |
| **DATEX2** | Estándar europeo para intercambio de información de tráfico |
| **NiFi** | Sistema de ingestión de datos |
| **Spark** | Motor de procesamiento distribuido |
| **Kafka** | Sistema de mensajería |
| **HDFS** | Sistema de archivos distribuido de Hadoop |

---

*Documento generado: Marzo 2026*
*Proyecto: SIMLOG - Sistema Integrado de Monitorización y Simulación Logística*
