# Diagramas UML en Mermaid — SIMLOG

Diagramas equivalentes a los PlantUML de `docs/uml/` (casos de uso, componentes, secuencia). **Fuente recomendada** para visores que soporten Mermaid (GitHub, GitLab, VS Code, MkDocs con extensión).

---

## 1. Casos de uso (resumen)

```mermaid
flowchart TB
  subgraph Actores
    OP[Operador]
    AN[Analista]
    PL[Planificador]
    SCH[Sistema programador]
  end
  subgraph Casos_de_uso
    UC1[CU-01 Ciclo KDD]
    UC2[CU-02 Supervisar stack]
    UC3[CU-03 CLI simlog_stack]
    UC4[CU-04 Visualizar red]
    UC5[CU-05 Histórico Hive]
    UC6[CU-06 Rutas híbridas]
    UC7[CU-07 Airflow]
    UC8[CU-08 NiFi]
    UC9[CU-09 Explorar KDD UI]
    UC10[CU-10 Asistente de Flota]
    UC11[CU-11 Graph AI anomalías]
    UC12[CU-12 Cluster Codespaces]
  end
  OP --> UC1
  OP --> UC2
  OP --> UC3
  OP --> UC7
  OP --> UC9
  AN --> UC4
  AN --> UC5
  AN --> UC9
  AN --> UC10
  PL --> UC6
  SCH --> UC1
  SCH --> UC7
  SCH --> UC8
  SCH --> UC12
  OP --> UC11
  AN --> UC11
  OP --> UC12
```

---

## 2. Componentes y datos

```mermaid
flowchart LR
  U[Usuario]
  subgraph Clientes
    ST[Streamlit]
    N[NiFi UI]
    A[Airflow UI]
  end
  subgraph UI_KDD[servicios kdd_*]
    VF[kdd_vista_ficheros]
    VR[kdd_reglas_ui]
    VG[kdd_vista_grafo]
  end
  subgraph Pipeline
    I[ingesta_kdd]
    P[procesamiento_grafos]
  end
  subgraph Mensajería
    K[Kafka raw/filtered]
    H[HDFS backup]
  end
  subgraph Stores
    C[Cassandra]
    V[Hive]
  end
  U --> ST
  U --> N
  U --> A
  ST --> VF
  ST --> VR
  ST --> VG
  VF --> I
  VF --> H
  VG --> C
  N --> I
  A --> I
  A --> P
  I --> K
  I --> H
  P --> K
  P --> H
  P --> C
  P --> V
  ST --> C
```

---

## 3. Secuencia — ciclo KDD ~15 min

```mermaid
sequenceDiagram
  participant T as Trigger NiFi/Airflow
  participant I as Ingesta
  participant K as Kafka
  participant H as HDFS
  participant S as Spark
  participant C as Cassandra
  participant V as Hive
  participant D as Dashboard
  T->>I: Disparo periódico
  I->>K: Publica raw + filtered
  I->>H: Backup JSON
  T->>S: Ejecuta procesamiento
  S->>H: Lee snapshot
  S->>C: Estado operativo
  S->>V: Histórico
  D->>C: Lectura mapa / métricas
```

---

## 4. Secuencia — exploración KDD y OpenWeather (dashboard)

```mermaid
sequenceDiagram
  participant U as Analista / Operador
  participant ST as Streamlit Ciclo KDD
  participant W as ultimo_payload.json
  participant OW as OpenWeather API
  participant ING as consulta_clima_hubs
  U->>ST: Elige fase 1–2, ajusta paso / formulario API
  ST->>W: Lee camiones / clima_hubs
  W-->>ST: Fragmentos JSON
  U->>ST: Consultar clima en vivo
  ST->>ING: api_key opcional
  ING->>OW: GET weather
  OW-->>ING: JSON
  ING-->>ST: Tabla por hub
  Note over ST: Clave solo en session_state
```

---

## 5. Secuencia — arranque stack (CLI)

```mermaid
sequenceDiagram
  participant U as Operador
  participant X as simlog_stack.py
  participant G as gestion_servicios
  participant S as Servicios HDFS…NiFi
  U->>X: start
  X->>G: arrancar_todos_servicios
  loop Orden secuencial
    G->>S: iniciar servicio i
    S-->>G: resultado
  end
  G->>G: esperar_hiveserver2
  X-->>U: salida por servicio
```

---

## 6. Despliegue lógico (standalone)

```mermaid
flowchart TB
  subgraph Host
    subgraph Procesos
      NN[NameNode HDFS]
      BR[Kafka broker]
      CS[Cassandra]
      SP[Spark local / master]
      HS[HiveServer2]
      API[Airflow api-server]
      SCH[Airflow scheduler]
      NF[NiFi]
    end
    subgraph App
      PY[venv_transporte / Python]
    end
  end
  PY --> NN
  PY --> BR
  PY --> CS
  PY --> SP
  PY --> HS
  PY --> API
  PY --> SCH
  PY --> NF
```

---

## 6.b Despliegue lógico (perfil Codespaces aislado)

```mermaid
flowchart LR
  subgraph Codespaces
    CC[Codespace VM]
    subgraph Docker_profile[docker-compose.codespaces.yml]
      NN[NameNode]
      DN[DataNode]
      SM[Spark Master]
      SW[Spark Worker]
      KF[Kafka]
      JP[Jupyter]
    end
    DOC[docs/CODESPACES_CLUSTER.md]
  end

  CC --> NN
  CC --> DN
  CC --> SM
  CC --> SW
  CC --> KF
  CC --> JP
  DOC --> Docker_profile
```

---

## 7. Secuencia — Cuadro de mando (Hive) riesgo por hub 24h

```mermaid
sequenceDiagram
  participant G as Gestor de incidencias
  participant ST as Streamlit (Cuadro de mando)
  participant HV as Hive (histórico)

  G->>ST: Selecciona consulta (24h)\n("Riesgo por hub" / "Top causas")
  ST->>HV: Ejecuta consulta Hive derivada (whitelist)\n(clasifica por estado + motivo_retraso + clima_actual)
  ST->>HV: (JOIN) nodos_maestro para mapear a hub
  HV-->>ST: Devuelve resultados agregados\n(muestras, % y duración aprox)
  ST-->>G: Renderiza tabla/métricas
```

---

## Nota sobre PlantUML

Los ficheros `docs/uml/*.puml` se mantienen como referencia alternativa (actualizados en paralelo con CU-09 y módulos UI KDD); la documentación principal usa **Mermaid** en este archivo y en `DISENO_SISTEMA.md` y `CASOS_DE_USO.md`.

---

## 8. Secuencia — Asistente de Flota (lenguaje natural → SQL)

```mermaid
sequenceDiagram
  participant U as Usuario
  participant ST as Streamlit (Asistente flota)
  participant G as Gestor SQL (whitelist)
  participant C as Cassandra (CQL)
  participant HV as HiveServer2 (PyHive)

  U->>ST: Escribe pregunta (ej. “¿Dónde está el camión 1?”)
  ST->>G: resolver_intencion_gestor(pregunta)
  G-->>ST: (motor, sql, intención)
  alt Cassandra (tiempo real)
    ST->>C: ejecutar_consulta_asistente(CQL)
    C-->>ST: filas (DataFrame)
  else Hive (histórico)
    ST->>HV: ejecutar_hive_sql_seguro(HiveQL)
    HV-->>ST: TSV (parse a DataFrame)
  end
  ST-->>U: st.dataframe + toggle “Ver consulta SQL”
```

---

## 9. Componentes — Integración Asistente de Flota + Graph AI

```mermaid
flowchart LR
  subgraph UI
    ST[Streamlit UI]
  end

  subgraph Backend_SQL
    GSQL[Gestor consultas (whitelist)]
  end

  subgraph Datos
    CS[Cassandra keyspace]
    HV[Hive (histórico)]
  end

  subgraph Graph_AI
    FAPI[FastAPI Graph AI]
    NX[NetworkX (metrics + scoring)]
  end

  subgraph Orquestación
    DAG[Airflow DAG: simlog_graph_ai_anomalias]
  end

  ST --> GSQL
  GSQL --> CS
  GSQL --> HV

  DAG --> CS
  DAG --> FAPI
  FAPI --> NX
  FAPI --> CS
```

---

## 10. Secuencia — Graph AI análisis (Airflow → FastAPI → Cassandra)

```mermaid
sequenceDiagram
  participant AF as Airflow
  participant CS as Cassandra
  participant API as FastAPI Graph AI
  participant NX as NetworkX
  participant STORE as Cassandra graph_anomalies

  AF->>CS: fetch_graph (nodos_estado, aristas_estado)
  AF->>API: POST /analyze-graph (graph payload)
  API->>NX: build_nx_graph + compute_centralities
  API->>NX: detect_anomalies + anomaly_score
  API-->>AF: anomalías (por nodo) + métricas
  AF->>STORE: INSERT graph_anomalies
```

---

## 11. Diagrama (modelo conceptual) — Graph AI

```mermaid
classDiagram
  class AnalyzeGraphRequest{
    degree_z_threshold
    edge_z_threshold
    structural_change_threshold
    anomaly_score_threshold
  }
  class GraphPayload{
    nodes
    edges
    directed
  }
  class GraphProcessing{
    build_nx_graph()
    compute_centralities()
    detect_anomalies()
  }
  class FastAPI{
    POST /analyze-graph
    POST /compare-graphs
  }

  FastAPI --> GraphProcessing
  GraphProcessing --> GraphPayload
  FastAPI --> AnalyzeGraphRequest
```
