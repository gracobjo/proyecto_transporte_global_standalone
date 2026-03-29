# Presentación del proyecto — SIMLOG España (standalone)

Guion breve para demo oral o defensa: **qué es**, **stack**, **novedades recientes** y **cómo mostrarlo**.

---

## 1. Elevator pitch (30 s)

**SIMLOG España** es una plataforma de **monitorización y simulación logística** que ejecuta un ciclo **KDD** completo: ingesta (clima, GPS simulado, incidencias **DATEX2 DGT**), colas **Kafka**, backup **HDFS**, procesamiento **Spark/GraphFrames**, estado en **Cassandra**, histórico en **Hive**, orquestación **Airflow**/**NiFi** y un **dashboard Streamlit** para operación y análisis.

---

## 2. Arquitectura en una lámina

| Capa | Tecnología | Rol |
|------|------------|-----|
| Ingesta | Python / NiFi | Snapshot JSON periódico |
| Mensajería | Kafka | Desacople |
| Lake | HDFS | Auditoría / replay |
| Proceso | Spark | Grafos, PageRank, persistencia |
| Tiempo real | Cassandra | Nodos, aristas, tracking, alertas |
| Analítica | Hive | Histórico particionado |
| UI | Streamlit + Folium | Cuadro de mando, mapas, KDD |

Detalle: `docs/DISENO_ARQUITECTURA.md`, diagramas Mermaid: `docs/DIAGRAMAS_MERMAID.md`.

---

## 3. Frontend (Streamlit) — qué destacar

1. **Ciclo KDD:** fases enlazadas a código y datos.
2. **Resultados pipeline:** comprobación HDFS / Kafka / Cassandra / Hive.
3. **Cuadro de mando:** consultas supervisadas, informes PDF, slides clima/retrasos, **flota por camión** (rutas persistidas, mapa, **simulación de movimiento** con alerta/correo al finalizar).
4. **Asistente flota:** lenguaje natural → SQL/CQL supervisado.
5. **Rutas híbridas / Gemelo digital / Servicios / Mapa / Verificación.**

Manual de usuario: `docs/MANUAL_USUARIO.md`.

---

## 4. Modelo de datos (referencia rápida)

- **Cassandra (`logistica_espana`):** `nodos_estado`, `aristas_estado`, `tracking_camiones`, `asignaciones_ruta_cuadro`, `pagerank_nodos`, `eventos_historico`, tablas de reconfiguración y `graph_anomalies`, etc. Esquema: `cassandra/esquema_logistica.cql`.
- **Hive:** base `logistica_espana` (configurable), tablas históricas escritas por Spark (`persistencia_hive.py`). Detalle: `docs/DOCUMENTACION_TECNICA_BD.md` §3.

---

## 5. Novedades operativas (última versión documentada)

| Tema | Descripción |
|------|-------------|
| Cuadro de mando — flota | Asignaciones `origen → destino`, persistencia en Cassandra, mapa con polilínea por ruta BFS |
| Simulación en mapa | Refresco periódico (p. ej. 5 s), duración de viaje configurable, `estado_ruta` → `Finalizada` |
| Alertas fin de ruta | Toast en UI + correo opcional (`SIMLOG_SMTP_*`) con ruta, camión, horas e incidencias |
| NiFi | Script `MergeDgtDatex2IntoPayload.groovy` sin comparación inválida de listas en fallback clima; prueba `pytest tests/test_merge_dgt_nifi_groovy.py` |
| NiFi / OpenWeather | **401** = API key inválida; actualizar `OWM_API_KEY` en Parameter Context y `.env` |

---

## 6. Demo sugerida (5–10 min)

1. Arrancar servicios mínimos (Cassandra + dashboard, o stack completo según entorno).
2. `streamlit run app_visualizacion.py` → **Cuadro de mando**.
3. Añadir ruta (camión + origen + destino) → **Iniciar simulación** → mostrar movimiento en mapa.
4. Opcional: pestaña **Ciclo KDD** / **Resultados pipeline** si hay ingesta Spark.
5. Mencionar **resiliencia**: si OpenWeather falla, respaldo meteorológico vía DGT en el payload (`source`, `fallback_activo`).

---

## 7. Documentos relacionados

| Documento | Uso |
|-----------|-----|
| `MEMORIA_PROYECTO_SIMLOG.md` | Memoria / informe consolidado |
| `MANUAL_USUARIO.md` | Uso de pestañas |
| `MANUAL_DESARROLLADOR.md` | Módulos y extensiones |
| `CASOS_DE_USO.md` | CU-01 … CU-19 |
| `DOCUMENTACION_TECNICA_BD.md` | Esquemas Cassandra/Hive |
| `nifi/README_NIFI.md` | Flujo NiFi y troubleshooting |

---

*Versión alineada con la rama documentada del repositorio standalone.*
