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
  end
  OP --> UC1
  OP --> UC2
  OP --> UC3
  OP --> UC7
  OP --> UC9
  AN --> UC4
  AN --> UC5
  AN --> UC9
  PL --> UC6
  SCH --> UC1
  SCH --> UC7
  SCH --> UC8
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

## Nota sobre PlantUML

Los ficheros `docs/uml/*.puml` se mantienen como referencia alternativa (actualizados en paralelo con CU-09 y módulos UI KDD); la documentación principal usa **Mermaid** en este archivo y en `DISENO_SISTEMA.md` y `CASOS_DE_USO.md`.
